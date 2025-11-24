from __future__ import annotations

import pandas as pd

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class IQRDetector(PandasDetector):
    """Отлавливает выбросы по межквартильному размаху."""

    def __init__(self, multiplier: float = 1.5) -> None:
        super().__init__(name="iqr")
        self._multiplier = multiplier

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        q1 = dataframe["value"].quantile(0.25)
        q3 = dataframe["value"].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return pd.Series([0] * len(dataframe))
        lower = q1 - self._multiplier * iqr
        upper = q3 + self._multiplier * iqr
        mask = (dataframe["value"] < lower) | (dataframe["value"] > upper)
        return mask.astype(int)
