from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
from loguru import logger

from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.domain.services.interfaces import DataQualityChecker


class PandasDataQualityChecker(DataQualityChecker):
    """Простые проверки качества: заполненность и дедупликация по ключу."""

    def __init__(self, dedup_keys: Sequence[str], required_columns: Sequence[str]) -> None:
        self._dedup_keys = list(dedup_keys)
        self._required_columns = list(required_columns)

    def validate(self, batch: RecordBatch) -> RecordBatch:
        dataframe = batch.dataframe.copy()

        if self._required_columns:
            before = len(dataframe)
            dataframe = dataframe.dropna(subset=self._required_columns)
            removed = before - len(dataframe)
            if removed:
                logger.warning("quality: dropped {} rows with missing values", removed)

        if self._dedup_keys:
            before = len(dataframe)
            dataframe = dataframe.drop_duplicates(subset=self._dedup_keys, keep="first")
            removed = before - len(dataframe)
            if removed:
                logger.warning("quality: deduplicated {} rows using {}", removed, self._dedup_keys)

        if dataframe.empty:
            raise ValueError("_quality_checker removed all rows from batch")

        return RecordBatch(dataframe=dataframe)
