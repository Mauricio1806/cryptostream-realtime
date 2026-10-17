"""Integration test: Bronze → Silver transformation.

Requires a local Spark session and MinIO running (make up).
Run with: pytest -m integration tests/integration/
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture()
def test_path() -> str:
    """Unique S3 path for test isolation."""
    return f"s3a://cryptostream-dev-bronze/test-{uuid.uuid4().hex[:8]}/"


@pytest.mark.skipif(
    not pytest.importorskip("pyspark", reason="pyspark not installed"),
    reason="pyspark required",
)
class TestBronzeToSilverTrades:
    """Test the Bronze → Silver transformation logic for trades."""

    def test_trade_parsing_and_typing(self, test_path: str) -> None:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from src.streaming.jobs.silver_trades import _transform_trades

        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("test-silver-trades")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

        try:
            # Create a mock Bronze DataFrame
            trade_json = json.dumps({
                "e": "aggTrade",
                "E": 1696012345678,
                "s": "BTCUSDT",
                "a": 123456789,
                "p": "27150.50000000",
                "q": "0.01500000",
                "f": 100000001,
                "l": 100000003,
                "T": 1696012345677,
                "m": True,
                "_received_at": 1696012345680,
            })

            # Simulate Bronze row
            bronze_data = [(
                "crypto.trades.v1",  # kafka_topic
                0,                    # kafka_partition
                42,                   # kafka_offset
                1696012345678,        # kafka_timestamp
                "BTCUSDT",            # message_key
                trade_json,           # message_value
                "2023-09-29T12:00:00", # ingested_at
                "2023-09-29",         # dt
                "12",                 # hour
            )]

            columns = [
                "kafka_topic", "kafka_partition", "kafka_offset",
                "kafka_timestamp", "message_key", "message_value",
                "ingested_at", "dt", "hour",
            ]

            bronze_df = spark.createDataFrame(bronze_data, columns)

            # Apply transformation
            silver_df = _transform_trades(bronze_df)

            rows = silver_df.collect()
            assert len(rows) == 1

            row = rows[0]
            assert row["event_id"] == "BTCUSDT_123456789"
            assert row["symbol"] == "BTCUSDT"
            assert isinstance(row["price"], float)
            assert row["price"] == 27150.5
            assert isinstance(row["quantity"], float)
            assert row["quantity"] == 0.015
            assert row["is_buyer_maker"] is True

        finally:
            spark.stop()
