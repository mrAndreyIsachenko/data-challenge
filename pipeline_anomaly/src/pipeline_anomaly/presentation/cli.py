from pathlib import Path

import typer

from pipeline_anomaly.application.use_cases.compute_aggregates import ComputeAggregates
from pipeline_anomaly.application.use_cases.detect_anomalies import DetectAnomalies
from pipeline_anomaly.application.use_cases.load_dataset import LoadSyntheticDataset
from pipeline_anomaly.application.use_cases.run_pipeline import RunPipeline
from pipeline_anomaly.domain.services.interfaces import AlertSink, DatasetGenerator
from pipeline_anomaly.infrastructure.alerting.http_sink import HttpAlertSink
from pipeline_anomaly.infrastructure.alerting.slack_sink import SlackAlertSink
from pipeline_anomaly.infrastructure.alerting.stdout_sink import StdOutAlertSink
from pipeline_anomaly.infrastructure.clients.clickhouse import ClickHouseFactory
from pipeline_anomaly.infrastructure.config import PipelineConfig
from pipeline_anomaly.infrastructure.data_quality.pandas_checker import PandasDataQualityChecker
from pipeline_anomaly.infrastructure.detectors.dbscan import DBSCANDetector
from pipeline_anomaly.infrastructure.detectors.hdbscan import HDBSCANDetector
from pipeline_anomaly.infrastructure.detectors.iqr import IQRDetector
from pipeline_anomaly.infrastructure.detectors.isolation_forest import IsolationForestDetector
from pipeline_anomaly.infrastructure.detectors.zscore import ZScoreDetector
from pipeline_anomaly.infrastructure.generators.synthetic_generator import SyntheticDatasetGenerator
from pipeline_anomaly.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from pipeline_anomaly.infrastructure.sources.kafka_file_source import KafkaBatchSource
from pipeline_anomaly.infrastructure.sources.web3_stub import Web3DatasetSource


def _build_pipeline(cfg: PipelineConfig) -> RunPipeline:
    factory = ClickHouseFactory(
        host=cfg.clickhouse.host,
        port=cfg.clickhouse.port,
        username=cfg.clickhouse.username,
        password=cfg.clickhouse.password,
        database=cfg.clickhouse.database,
    )
    repository = ClickHouseRepository(factory=factory)

    generator = _build_generator(cfg)
    quality_checker = None
    if cfg.quality.dedup_keys or cfg.quality.required_columns:
        quality_checker = PandasDataQualityChecker(
            dedup_keys=cfg.quality.dedup_keys,
            required_columns=cfg.quality.required_columns,
        )
    loader = LoadSyntheticDataset(generator=generator, writer=repository, quality_checker=quality_checker)
    aggregator = ComputeAggregates(writer=repository, windows=cfg.features.windows)

    detectors = [
        ZScoreDetector(threshold=cfg.anomaly_detection.zscore_threshold),
        IQRDetector(),
        IsolationForestDetector(
            contamination=cfg.anomaly_detection.isolation_forest.contamination,
            random_state=cfg.anomaly_detection.isolation_forest.random_state,
        ),
        DBSCANDetector(
            eps=cfg.anomaly_detection.dbscan.eps,
            min_samples=cfg.anomaly_detection.dbscan.min_samples,
        ),
    ]
    if cfg.anomaly_detection.hdbscan and cfg.anomaly_detection.hdbscan.enabled:
        detectors.append(
            HDBSCANDetector(
                min_cluster_size=cfg.anomaly_detection.hdbscan.min_cluster_size,
                min_samples=cfg.anomaly_detection.hdbscan.min_samples,
            )
        )
    detector = DetectAnomalies(
        writer=repository,
        detectors=detectors,
        threshold=cfg.alerting.threshold_score,
    )

    sink = _build_alert_sink(cfg)
    pipeline = RunPipeline(
        loader=loader,
        aggregator=aggregator,
        detector=detector,
        alert_sink=sink,
        alerts_enabled=cfg.alerting.enabled,
    )
    return pipeline


def _build_generator(cfg: PipelineConfig) -> DatasetGenerator:
    source_type = (cfg.source.type or "synthetic").lower()
    if source_type == "kafka":
        if not cfg.source.kafka:
            raise ValueError("kafka source requested but kafka config missing")
        return KafkaBatchSource(cfg.source.kafka)
    if source_type == "web3":
        if not cfg.source.web3:
            raise ValueError("web3 source requested but web3 config missing")
        return Web3DatasetSource(cfg.source.web3)
    return SyntheticDatasetGenerator(config=cfg.dataset)


def _build_alert_sink(cfg: PipelineConfig) -> AlertSink:
    sink_type = (cfg.alerting.sink or "stdout").lower()
    if sink_type == "http":
        return HttpAlertSink(url=cfg.alerting.webhook_url or "")
    if sink_type == "slack":
        return SlackAlertSink(
            webhook_url=cfg.alerting.webhook_url or "",
            channel=cfg.alerting.channel,
        )
    return StdOutAlertSink()


def run_pipeline(config: Path) -> None:
    cfg = PipelineConfig.load(config)
    pipeline = _build_pipeline(cfg)
    pipeline.execute()


def main(
    config: Path = typer.Option(
        Path("config/pipeline.yaml"),
        "--config",
        "-c",
        help="Path to pipeline config YAML",
        show_default=True,
    )
) -> None:
    if not config.exists():
        raise typer.BadParameter(f"config file not found: {config}")
    run_pipeline(config)


if __name__ == "__main__":
    typer.run(main)
