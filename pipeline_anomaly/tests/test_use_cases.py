from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from pipeline_anomaly.application.use_cases.compute_aggregates import ComputeAggregates
from pipeline_anomaly.application.use_cases.detect_anomalies import DetectAnomalies
from pipeline_anomaly.domain.models.aggregate import AggregateCollection
from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.services.interfaces import AnomalyDetector, ClickHouseWriter


class InMemoryWriter(ClickHouseWriter):
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame
        self.persisted_aggregates: AggregateCollection | None = None
        self.persisted_report: AnomalyReport | None = None

    def ensure_schema(self) -> None:  # pragma: no cover - not needed in tests
        pass

    def ingest_batch(self, batch) -> None:  # pragma: no cover - not used
        raise NotImplementedError

    def persist_aggregates(self, aggregates: AggregateCollection) -> None:
        self.persisted_aggregates = aggregates

    def persist_report(self, report: AnomalyReport) -> None:
        self.persisted_report = report

    def read_latest_window(self) -> pd.DataFrame:
        return self._frame.copy()


class FakeDetector(AnomalyDetector):
    def __init__(self, name: str, scores: list[float], severity_value: float) -> None:
        self.name = name
        self._scores = scores
        self._severity_value = severity_value

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        return pd.Series(self._scores)

    def severity(self, scores: pd.Series) -> float:
        assert scores.equals(pd.Series(self._scores))
        return self._severity_value


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                ["2024-01-01 00:00:00", "2024-01-01 00:04:00"],
            ),
            "entity_id": [1, 2],
            "chain_id": [1, 137],
            "block_number": [42, 43],
            "contract_address": ["0x1", "0x2"],
            "tx_hash": ["0xabc", "0xdef"],
            "value": [100.0, 200.0],
            "attribute": [0.1, 0.9],
            "gas_used": [21000, 42000],
            "calldata_size": [128, 2048],
        }
    )


def test_compute_aggregates_calculates_expected_metrics():
    writer = InMemoryWriter(frame=_sample_frame())
    aggregator = ComputeAggregates(writer=writer, windows=("5m",))

    result = aggregator.execute()

    assert writer.persisted_aggregates == result
    metrics = {row["metric"]: row for row in result.as_dict()}
    assert metrics["count"]["value"] == pytest.approx(2.0)
    assert metrics["mean_value"]["value"] == pytest.approx(150.0)
    assert metrics["high_calldata_ratio"]["value"] == pytest.approx(0.5)
    assert metrics["count_chain_1"]["value"] == pytest.approx(1.0)
    assert metrics["mean_value_chain_137"]["value"] == pytest.approx(200.0)
    assert metrics["count_last_5m"]["value"] == pytest.approx(2.0)
    assert datetime.fromisoformat(metrics["count_last_5m"]["window_end"]) == datetime.fromisoformat(
        metrics["mean_value_last_5m"]["window_end"]
    )


def test_detect_anomalies_builds_report_and_alerts_on_threshold():
    writer = InMemoryWriter(frame=_sample_frame())
    detectors = [
        FakeDetector("zscore", [0.0, 1.0], severity_value=0.2),
        FakeDetector("forest", [1.0, 1.0], severity_value=0.9),
    ]
    use_case = DetectAnomalies(writer=writer, detectors=detectors, threshold=0.8)

    report = use_case.execute()

    assert writer.persisted_report == report
    assert [anomaly.detector for anomaly in report.anomalies] == ["zscore", "forest"]
    assert report.anomalies[0].score == pytest.approx(0.5)
    assert report.anomalies[1].severity == pytest.approx(0.9)
    assert use_case.is_alert(report) is True
