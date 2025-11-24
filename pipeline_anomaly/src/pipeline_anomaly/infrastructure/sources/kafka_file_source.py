from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pandas as pd
from loguru import logger

from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.domain.services.interfaces import DatasetGenerator
from pipeline_anomaly.infrastructure.config import KafkaSourceConfig


class KafkaBatchSource(DatasetGenerator):
    """Читает newline-json файл и имитирует консьюмера Kafka."""

    def __init__(self, config: KafkaSourceConfig) -> None:
        self._config = config
        self._path = Path(config.path)
        if not self._path.exists():
            raise FileNotFoundError(f"kafka mock file not found: {self._path}")

    def batches(self) -> list[RecordBatch]:
        return list(self._iter_batches())

    def _iter_batches(self) -> Iterator[RecordBatch]:
        current_rows: list[dict[str, object]] = []
        with self._path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue
                payload = json.loads(line)
                payload["event_time"] = pd.to_datetime(payload["event_time"], utc=True)
                current_rows.append(payload)
                if len(current_rows) >= self._config.batch_size:
                    yield self._to_batch(current_rows)
                    current_rows = []
        if current_rows:
            yield self._to_batch(current_rows)

    def _to_batch(self, rows: list[dict[str, object]]) -> RecordBatch:
        logger.info("kafka mock emitted {} rows", len(rows))
        dataframe = pd.DataFrame(rows)
        return RecordBatch(dataframe=dataframe)
