from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from pipeline_anomaly.infrastructure.generators.synthetic_generator import SyntheticDatasetConfig


@dataclass(slots=True)
class FeatureConfig:
    windows: tuple[str, ...]


@dataclass(slots=True)
class QualityConfig:
    dedup_keys: tuple[str, ...]
    required_columns: tuple[str, ...]


@dataclass(slots=True)
class KafkaSourceConfig:
    path: str
    batch_size: int


@dataclass(slots=True)
class Web3SourceConfig:
    rpc_url: str
    start_block: int
    end_block: int


@dataclass(slots=True)
class SourceConfig:
    type: str
    kafka: KafkaSourceConfig | None
    web3: Web3SourceConfig | None


@dataclass(slots=True)
class ClickHouseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str


@dataclass(slots=True)
class IsolationForestConfig:
    contamination: float
    random_state: int


@dataclass(slots=True)
class DBSCANConfig:
    eps: float
    min_samples: int


@dataclass(slots=True)
class HDBSCANConfig:
    enabled: bool
    min_cluster_size: int
    min_samples: int


@dataclass(slots=True)
class AnomalyDetectionConfig:
    zscore_threshold: float
    isolation_forest: IsolationForestConfig
    dbscan: DBSCANConfig
    hdbscan: HDBSCANConfig | None


@dataclass(slots=True)
class AlertingConfig:
    enabled: bool
    threshold_score: float
    sink: str
    webhook_url: str | None = None
    channel: str | None = None


@dataclass(slots=True)
class PipelineConfig:
    clickhouse: ClickHouseConfig
    source: SourceConfig
    dataset: SyntheticDatasetConfig
    features: FeatureConfig
    quality: QualityConfig
    anomaly_detection: AnomalyDetectionConfig
    alerting: AlertingConfig

    @classmethod
    def load(cls, path: Path) -> "PipelineConfig":
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
        source_raw = raw.get("source", {})
        return cls(
            clickhouse=ClickHouseConfig(**raw["clickhouse"]),
            source=SourceConfig(
                type=source_raw.get("type", "synthetic"),
                kafka=KafkaSourceConfig(**source_raw["kafka"]) if source_raw.get("kafka") else None,
                web3=Web3SourceConfig(**source_raw["web3"]) if source_raw.get("web3") else None,
            ),
            dataset=SyntheticDatasetConfig(**raw["dataset"]),
            features=FeatureConfig(windows=tuple(raw.get("features", {}).get("windows", ()))),
            quality=QualityConfig(
                dedup_keys=tuple(raw.get("quality", {}).get("dedup_keys", ())),
                required_columns=tuple(raw.get("quality", {}).get("required_columns", ())),
            ),
            anomaly_detection=AnomalyDetectionConfig(
                zscore_threshold=float(raw["anomaly_detection"]["zscore_threshold"]),
                isolation_forest=IsolationForestConfig(**raw["anomaly_detection"]["isolation_forest"]),
                dbscan=DBSCANConfig(**raw["anomaly_detection"]["dbscan"]),
                hdbscan=HDBSCANConfig(**raw["anomaly_detection"]["hdbscan"])
                if raw["anomaly_detection"].get("hdbscan")
                else None,
            ),
            alerting=AlertingConfig(**raw["alerting"]),
        )
