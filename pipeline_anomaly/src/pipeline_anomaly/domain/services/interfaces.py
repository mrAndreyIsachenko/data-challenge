from __future__ import annotations

from typing import Protocol

import pandas as pd

from pipeline_anomaly.domain.models.aggregate import AggregateCollection
from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.models.batch import RecordBatch


class DatasetGenerator(Protocol):
    def batches(self) -> list[RecordBatch]:
        ...


class ClickHouseWriter(Protocol):
    def ensure_schema(self) -> None:
        ...

    def ingest_batch(self, batch: RecordBatch) -> None:
        ...

    def persist_aggregates(self, aggregates: AggregateCollection) -> None:
        ...

    def persist_report(self, report: AnomalyReport) -> None:
        ...

    def read_latest_window(self) -> pd.DataFrame:
        ...


class AnomalyDetector(Protocol):
    name: str

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        ...

    def severity(self, scores: pd.Series) -> float:
        ...


class AlertSink(Protocol):
    def send(self, report: AnomalyReport) -> None:
        ...


class DataQualityChecker(Protocol):
    def validate(self, batch: RecordBatch) -> RecordBatch:
        ...
