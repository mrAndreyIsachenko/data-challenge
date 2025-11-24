from __future__ import annotations

from loguru import logger

from pipeline_anomaly.domain.services.interfaces import ClickHouseWriter, DatasetGenerator, DataQualityChecker


class LoadSyntheticDataset:
    def __init__(
        self,
        generator: DatasetGenerator,
        writer: ClickHouseWriter,
        quality_checker: DataQualityChecker | None = None,
    ) -> None:
        self._generator = generator
        self._writer = writer
        self._quality_checker = quality_checker

    def execute(self) -> None:
        logger.info("ensure schema")
        self._writer.ensure_schema()
        for idx, batch in enumerate(self._generator.batches(), start=1):
            if self._quality_checker:
                batch = self._quality_checker.validate(batch)
            logger.info("ingesting batch {}/{} rows", idx, batch.size)
            self._writer.ingest_batch(batch)
