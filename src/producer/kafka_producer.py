"""Idempotent Kafka producer wrapper using confluent-kafka."""

from __future__ import annotations

from typing import TYPE_CHECKING

import orjson
import structlog
from confluent_kafka import KafkaError, KafkaException, Producer

from src.producer.metrics import ProducerMetrics

if TYPE_CHECKING:
    from src.producer.config import ProducerSettings

logger = structlog.get_logger()


class CryptoKafkaProducer:
    """Thread-safe, idempotent Kafka producer for crypto market data."""

    def __init__(self, settings: ProducerSettings) -> None:
        self._settings = settings
        self._metrics = ProducerMetrics()
        self._producer = Producer(self._build_config())
        logger.info(
            "kafka_producer_initialized",
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )

    def _build_config(self) -> dict[str, str | int | bool]:
        """Build confluent-kafka producer configuration."""
        return {
            "bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "security.protocol": self._settings.kafka_security_protocol,
            "acks": self._settings.kafka_acks,
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 2147483647,  # infinite retries for idempotent producer
            "linger.ms": 5,
            "batch.size": 32768,
            "compression.type": "lz4",
            "client.id": f"cryptostream-producer-{self._settings.env}",
        }

    def _delivery_callback(self, err: KafkaError | None, msg: object) -> None:
        """Called once per message when delivery succeeds or fails."""
        if err is not None:
            self._metrics.record_error()
            logger.error("kafka_delivery_failed", error=str(err))
        else:
            # msg is a confluent_kafka.Message but we type it loosely
            self._metrics.record_send(len(getattr(msg, "value", lambda: b"")()))

    def produce_trade(self, symbol: str, payload: dict[str, object]) -> None:
        """Publish a trade event to the trades topic."""
        self._produce(
            topic=self._settings.kafka_trades_topic,
            key=symbol,
            value=payload,
        )

    def produce_orderbook(self, symbol: str, payload: dict[str, object]) -> None:
        """Publish an order book update to the orderbook topic."""
        self._produce(
            topic=self._settings.kafka_orderbook_topic,
            key=symbol,
            value=payload,
        )

    def _produce(self, topic: str, key: str, value: dict[str, object]) -> None:
        """Serialize and send a message, triggering delivery callbacks."""
        try:
            serialized = orjson.dumps(value)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=serialized,
                callback=self._delivery_callback,
            )
            # Serve delivery callbacks without blocking
            self._producer.poll(0)
        except BufferError:
            logger.warning("kafka_buffer_full", topic=topic)
            self._producer.poll(1.0)
            # Retry once after draining
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=serialized,
                callback=self._delivery_callback,
            )
        except KafkaException as exc:
            self._metrics.record_error()
            logger.error("kafka_produce_error", error=str(exc), topic=topic)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages. Returns number of messages still in queue."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("kafka_flush_incomplete", remaining=remaining)
        return remaining

    def close(self) -> None:
        """Flush and clean up."""
        self.flush(timeout=30.0)
        logger.info("kafka_producer_closed")
