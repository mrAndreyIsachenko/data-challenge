from __future__ import annotations

from datetime import datetime

import pandas as pd

from pipeline_anomaly.domain.models.aggregate import AggregateCollection
from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.infrastructure.clients.clickhouse import ClickHouseFactory


class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory) -> None:
        self._factory = factory

    def ensure_schema(self) -> None:
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS events (
                event_time DateTime,
                entity_id UInt64,
                chain_id UInt16,
                block_number UInt64,
                contract_address String,
                tx_hash String,
                value Float64,
                attribute Float64,
                gas_used Float64,
                calldata_size UInt32
            ) ENGINE = MergeTree
            PARTITION BY toDate(event_time)
            ORDER BY (event_time, entity_id)
            TTL event_time + INTERVAL 45 DAY
            """,
            """
            CREATE TABLE IF NOT EXISTS aggregates (
                metric String,
                value Float64,
                window_start DateTime,
                window_end DateTime,
                extra Map(String, Float64)
            ) ENGINE = MergeTree
            ORDER BY (window_start, metric)
            """,
            """
            CREATE TABLE IF NOT EXISTS anomaly_reports (
                generated_at DateTime,
                window_start DateTime,
                window_end DateTime,
                detector String,
                score Float64,
                severity Float64,
                description String
            ) ENGINE = MergeTree
            ORDER BY (generated_at, detector)
            TTL generated_at + INTERVAL 90 DAY
            """,
        ]
        with self._factory.connect() as client:
            for ddl in ddl_statements:
                client.command(ddl)

    def ingest_batch(self, batch: RecordBatch) -> None:
        with self._factory.connect() as client:
            client.insert_df("events", batch.dataframe)

    def persist_aggregates(self, aggregates: AggregateCollection) -> None:
        rows = [
            (
                aggregate["metric"],
                aggregate["value"],
                datetime.fromisoformat(aggregate["window_start"]),
                datetime.fromisoformat(aggregate["window_end"]),
                aggregate["extra"],
            )
            for aggregate in aggregates.as_dict()
        ]
        with self._factory.connect() as client:
            client.insert(
                "aggregates",
                rows,
                column_names=["metric", "value", "window_start", "window_end", "extra"],
            )

    def persist_report(self, report: AnomalyReport) -> None:
        rows = [
            {
                "generated_at": report.generated_at,
                "window_start": report.window_start,
                "window_end": report.window_end,
                "detector": anomaly.detector,
                "score": anomaly.score,
                "severity": anomaly.severity,
                "description": anomaly.description,
            }
            for anomaly in report.anomalies
        ]
        with self._factory.connect() as client:
            client.insert(
                "anomaly_reports",
                [
                    (
                        row["generated_at"],
                        row["window_start"],
                        row["window_end"],
                        row["detector"],
                        row["score"],
                        row["severity"],
                        row["description"],
                    )
                    for row in rows
                ],
                column_names=[
                    "generated_at",
                    "window_start",
                    "window_end",
                    "detector",
                    "score",
                    "severity",
                    "description",
                ],
            )

    def read_latest_window(self) -> pd.DataFrame:
        with self._factory.connect() as client:
            query = """
            SELECT * FROM events
            WHERE event_time >= now() - INTERVAL 1 DAY
            ORDER BY event_time
            """
            result = client.query_df(query)
        if result.empty:
            raise RuntimeError("events table is empty")
        return result
