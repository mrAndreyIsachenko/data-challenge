from pipeline_anomaly.infrastructure.generators.synthetic_generator import (
    SyntheticDatasetConfig,
    SyntheticDatasetGenerator,
)


def test_batches_shape_and_schema():
    config = SyntheticDatasetConfig(row_count=1000, batch_size=200, anomaly_ratio=0.1, seed=42)
    generator = SyntheticDatasetGenerator(config)
    batches = generator.batches()
    assert sum(batch.size for batch in batches) == 1000

    required_columns = {
        "event_time",
        "entity_id",
        "chain_id",
        "block_number",
        "contract_address",
        "tx_hash",
        "value",
        "attribute",
        "gas_used",
        "calldata_size",
    }
    for batch in batches:
        assert required_columns.issubset(batch.dataframe.columns)
