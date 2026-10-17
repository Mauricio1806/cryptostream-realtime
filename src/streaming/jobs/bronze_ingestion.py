"""Bronze layer: raw JSON from Kafka → S3 Delta Lake, partitioned by date/hour.

This is the rawest form of the data — no parsing, no dedup.
Just reliable, exactly-once landing of Kafka messages into durable storage.
"""

from __future__ import annotations

import os

from pyspark.sql import functions as F

from src.streaming.common.checkpoints import get_checkpoint_path
from src.streaming.common.spark_session import build_spark_session

# ── Configuration ──
KAFKA_SERVERS = os.getenv("CRYPTOSTREAM_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TRADES_TOPIC = os.getenv("CRYPTOSTREAM_KAFKA_TRADES_TOPIC", "crypto.trades.v1")
ORDERBOOK_TOPIC = os.getenv("CRYPTOSTREAM_KAFKA_ORDERBOOK_TOPIC", "crypto.orderbook.v1")
BRONZE_BUCKET = os.getenv("CRYPTOSTREAM_S3_BRONZE_BUCKET", "cryptostream-dev-bronze")


def run_bronze_ingestion() -> None:
    """Read from Kafka and write raw JSON to Bronze Delta tables."""
    spark = build_spark_session("cryptostream-bronze")

    # Subscribe to both topics
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", f"{TRADES_TOPIC},{ORDERBOOK_TOPIC}")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    # Add metadata columns for partitioning and lineage
    bronze_df = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("message_value"),
        F.current_timestamp().alias("ingested_at"),
        F.date_format(F.col("timestamp"), "yyyy-MM-dd").alias("dt"),
        F.date_format(F.col("timestamp"), "HH").alias("hour"),
    )

    # Write to Bronze as Delta, partitioned by date and hour
    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("kafka_topic", "dt", "hour")
        .option("checkpointLocation", get_checkpoint_path("bronze_ingestion"))
        .option("path", f"s3a://{BRONZE_BUCKET}/raw_events/")
        .trigger(processingTime="10 seconds")
        .queryName("bronze_ingestion")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_bronze_ingestion()
