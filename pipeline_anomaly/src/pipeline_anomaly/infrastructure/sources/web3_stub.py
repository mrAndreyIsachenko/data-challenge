from __future__ import annotations

from loguru import logger

from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.domain.services.interfaces import DatasetGenerator
from pipeline_anomaly.infrastructure.config import Web3SourceConfig


class Web3DatasetSource(DatasetGenerator):
    """Заглушка для Web3-интеграции. Возвращает пустые батчи, но описывает контракт API."""

    def __init__(self, config: Web3SourceConfig) -> None:
        self._config = config

    def fetch_blocks(self, start_block: int, end_block: int) -> list[dict[str, object]]:
        logger.info(
            "web3 stub fetch: rpc_url=%s start_block=%s end_block=%s",
            self._config.rpc_url,
            start_block,
            end_block,
        )
        return []

    def batches(self) -> list[RecordBatch]:
        logger.warning(
            "web3 dataset source is a stub – returning no records. Configure a real Web3 connector to enable ingestion."
        )
        return []
