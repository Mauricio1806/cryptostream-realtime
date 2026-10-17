"""Benchmark the Kafka producer throughput.

Connects to Binance WebSocket and measures messages/second and bytes/second
produced to Kafka over a configurable duration.

Usage:
    python scripts/benchmark_producer.py --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import time

from src.producer.config import ProducerSettings
from src.producer.kafka_producer import CryptoKafkaProducer
from src.producer.binance_ws_client import BinanceWebSocketClient
from src.utils.logging import setup_logging

import structlog

logger = structlog.get_logger()


async def run_benchmark(duration: int) -> None:
    """Run the producer for a fixed duration and report metrics."""
    settings = ProducerSettings()
    producer = CryptoKafkaProducer(settings)

    logger.info("benchmark_start", duration_seconds=duration, symbols=settings.symbol_list)

    client = BinanceWebSocketClient(settings=settings, producer=producer)

    # Run in background
    task = asyncio.create_task(client.run())

    # Wait for the benchmark duration
    await asyncio.sleep(duration)

    # Stop the client
    client.stop()

    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        task.cancel()

    # Report results
    metrics = producer.metrics
    elapsed = duration

    logger.info(
        "benchmark_complete",
        duration_seconds=elapsed,
        total_messages=metrics.messages_sent,
        total_bytes=metrics.bytes_sent,
        total_errors=metrics.errors,
        avg_msgs_per_sec=round(metrics.messages_sent / elapsed, 1) if elapsed > 0 else 0,
        avg_mb_per_sec=round(metrics.bytes_sent / (1024 * 1024) / elapsed, 3) if elapsed > 0 else 0,
    )

    producer.close()

    print("\n" + "=" * 60)
    print(f"  Benchmark Results ({duration}s)")
    print("=" * 60)
    print(f"  Messages sent:    {metrics.messages_sent:,}")
    print(f"  Bytes sent:       {metrics.bytes_sent:,} ({metrics.bytes_sent / (1024*1024):.2f} MB)")
    print(f"  Errors:           {metrics.errors:,}")
    print(f"  Throughput:       {metrics.messages_sent / elapsed:,.1f} msg/s")
    print(f"  Bandwidth:        {metrics.bytes_sent / (1024*1024) / elapsed:.3f} MB/s")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark CryptoStream producer")
    parser.add_argument("--duration", type=int, default=60, help="Benchmark duration in seconds")
    parser.add_argument("--log-level", default="WARNING", help="Log level")
    args = parser.parse_args()

    setup_logging(args.log_level)
    asyncio.run(run_benchmark(args.duration))


if __name__ == "__main__":
    main()
