import pandas as pd

from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.infrastructure.data_quality.pandas_checker import PandasDataQualityChecker


def test_quality_checker_drops_duplicates_and_nulls():
    dataframe = pd.DataFrame(
        {
            "event_time": pd.date_range("2024-01-01", periods=4, freq="1min"),
            "tx_hash": ["0x1", "0x1", "0x2", None],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    batch = RecordBatch(dataframe=dataframe)

    checker = PandasDataQualityChecker(dedup_keys=["tx_hash"], required_columns=["tx_hash", "event_time"])
    cleaned = checker.validate(batch)

    assert len(cleaned.dataframe) == 2
    assert cleaned.dataframe["tx_hash"].tolist() == ["0x1", "0x2"]
