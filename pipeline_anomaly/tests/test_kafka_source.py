from pathlib import Path

from pipeline_anomaly.infrastructure.config import KafkaSourceConfig
from pipeline_anomaly.infrastructure.sources.kafka_file_source import KafkaBatchSource


def test_kafka_source_reads_batches(tmp_path: Path) -> None:
    sample_file = Path(__file__).resolve().parents[1] / "data" / "kafka_mock" / "events.ndjson"
    config = KafkaSourceConfig(path=str(sample_file), batch_size=2)
    source = KafkaBatchSource(config)
    batches = source.batches()
    assert batches
    assert sum(batch.size for batch in batches) == 5
