"""Integration test: produce to Kafka and consume back.

Requires Docker Compose services running (make up).
Run with: pytest -m integration tests/integration/
"""

from __future__ import annotations

import json
import time
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture()
def unique_topic() -> str:
    """Generate a unique topic name to avoid test interference."""
    return f"test.roundtrip.{uuid.uuid4().hex[:8]}"


@pytest.mark.skipif(
    not pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed"),
    reason="confluent-kafka required",
)
class TestKafkaRoundtrip:
    """Produce a message and consume it back from Kafka."""

    def test_produce_and_consume_trade(self, unique_topic: str) -> None:
        from confluent_kafka import Producer, Consumer
        from confluent_kafka.admin import AdminClient, NewTopic

        bootstrap = "localhost:29092"

        # Create topic
        admin = AdminClient({"bootstrap.servers": bootstrap})
        topic = NewTopic(unique_topic, num_partitions=1, replication_factor=1)
        futures = admin.create_topics([topic])
        for _, f in futures.items():
            f.result(timeout=10)

        # Produce
        producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
        })

        test_message = {
            "e": "aggTrade",
            "E": int(time.time() * 1000),
            "s": "BTCUSDT",
            "a": 999999,
            "p": "50000.00",
            "q": "1.5",
            "f": 1,
            "l": 1,
            "T": int(time.time() * 1000),
            "m": False,
        }

        producer.produce(
            unique_topic,
            key="BTCUSDT",
            value=json.dumps(test_message).encode(),
        )
        producer.flush(timeout=10)

        # Consume
        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([unique_topic])

        msg = consumer.poll(timeout=10.0)

        assert msg is not None, "No message received within timeout"
        assert msg.error() is None, f"Consumer error: {msg.error()}"

        received = json.loads(msg.value().decode())
        assert received["s"] == "BTCUSDT"
        assert received["a"] == 999999
        assert received["p"] == "50000.00"

        consumer.close()

        # Cleanup topic
        admin.delete_topics([unique_topic])
