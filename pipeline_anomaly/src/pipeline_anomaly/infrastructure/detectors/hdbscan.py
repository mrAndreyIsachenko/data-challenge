from __future__ import annotations

import pandas as pd
import hdbscan

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class HDBSCANDetector(PandasDetector):
    def __init__(self, min_cluster_size: int, min_samples: int) -> None:
        super().__init__(name="hdbscan")
        self._model = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples)

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        if dataframe.empty:
            return pd.Series(dtype=int)
        features = dataframe[["value", "attribute", "gas_used", "calldata_size"]]
        labels = self._model.fit_predict(features)
        return pd.Series((labels == -1).astype(int))
