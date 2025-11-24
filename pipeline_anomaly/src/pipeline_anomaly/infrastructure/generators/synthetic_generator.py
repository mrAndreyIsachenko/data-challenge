from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

import numpy as np
import pandas as pd

from pipeline_anomaly.domain.models.batch import RecordBatch


@dataclass(slots=True)
class SyntheticDatasetConfig:
    row_count: int
    batch_size: int
    anomaly_ratio: float
    seed: int


class SyntheticDatasetGenerator:
    def __init__(self, config: SyntheticDatasetConfig) -> None:
        self._config = config
        self._random = np.random.default_rng(config.seed)

    def batches(self) -> list[RecordBatch]:
        return list(self._iter_batches())

    def _iter_batches(self) -> Iterator[RecordBatch]:
        remaining = self._config.row_count
        current_time = datetime.utcnow()
        block_number = int(current_time.timestamp())
        while remaining > 0:
            batch_size = min(self._config.batch_size, remaining)
            timestamps = [current_time - timedelta(seconds=i) for i in range(batch_size)]
            entity_ids = self._random.integers(1, 1_000_000, size=batch_size)
            chain_id_options = np.array([1, 10, 56, 137])
            chain_id_prob = np.array([0.45, 0.2, 0.15, 0.2])
            chain_ids = self._random.choice(chain_id_options, size=batch_size, p=chain_id_prob)
            contract_pool = [
                "0xA1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
                "0x1111111254eeb25477b68fb85ed929f73a960582",
                "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
                "0x00000000006c3852cbef3e08e8df289169ede581",
            ]
            contract_addresses = self._random.choice(contract_pool, size=batch_size)
            base_values = self._random.normal(loc=100.0, scale=15.0, size=batch_size)
            gas_used = self._random.integers(21_000, 800_000, size=batch_size)
            calldata_size = self._random.integers(64, 4096, size=batch_size)
            attribute = self._random.uniform(0, 1, size=batch_size)
            tx_hashes = self._generate_tx_hashes(batch_size)

            anomalies_count = max(1, int(batch_size * self._config.anomaly_ratio))
            anomaly_indices = self._random.choice(batch_size, size=anomalies_count, replace=False)
            base_values[anomaly_indices] *= self._random.uniform(2, 5, size=anomalies_count)
            gas_used[anomaly_indices] *= self._random.integers(2, 6, size=anomalies_count)
            calldata_size[anomaly_indices] *= self._random.integers(2, 5, size=anomalies_count)

            self._introduce_duplicates(tx_hashes)

            dataframe = pd.DataFrame(
                {
                    "event_time": timestamps,
                    "entity_id": entity_ids,
                    "chain_id": chain_ids,
                    "block_number": block_number,
                    "contract_address": contract_addresses,
                    "tx_hash": tx_hashes,
                    "value": base_values,
                    "attribute": attribute,
                    "gas_used": gas_used,
                    "calldata_size": calldata_size,
                }
            )
            yield RecordBatch(dataframe=dataframe)
            remaining -= batch_size
            current_time -= timedelta(seconds=batch_size)
            block_number -= batch_size

    def _generate_tx_hashes(self, size: int) -> list[str]:
        random_bytes = self._random.integers(0, 256, size=(size, 32), dtype=np.uint8)
        return ["0x" + "".join(f"{byte:02x}" for byte in row) for row in random_bytes]

    def _introduce_duplicates(self, tx_hashes: list[str]) -> None:
        if len(tx_hashes) < 2:
            return
        duplicate_count = max(1, int(len(tx_hashes) * 0.01))
        indices = self._random.choice(len(tx_hashes) - 1, size=duplicate_count, replace=False)
        for idx in indices:
            tx_hashes[idx] = tx_hashes[idx + 1]
