"""Silver layer: parse, type-cast, deduplicate and upsert trade events.

Reads from the trades Kafka topic, applies the schema, deduplicates
by event_id using watermark + dropDuplicatesWithinWatermark, and
merges into a Delta table on S3.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.streaming.common.checkpoints import get_checkpoint_path
from src.streaming.common.delta_utils import upsert_to_delta
from src.streaming.common.schemas import TRADE_SCHEMA
from src.streaming.common.spark_session import build_spark_session

KAFKA_SERVERS = os.getenv("CRYPTOSTREAM_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TRADES_TOPIC = os.getenv("CRYPTOSTREAM_KAFKA_TRADES_TOPIC", "crypto.trades.v1")
SILVER_BUCKET = os.getenv("CRYPTOSTREAM_S3_SILVER_BUCKET", "cryptostream-dev-silver")
SILVER_PATH = f"s3a://{SILVER_BUCKET}/trades/"


def _transform_trades(raw_df: DataFrame) -> DataFrame:
    """Parse JSON, cast types, generate event_id, add ingestion timestamp."""
    parsed = raw_df.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_ts"),
    ).select("data.*", "kafka_ts")

    return parsed.select(
        F.concat_ws("_", F.col("s"), F.col("a").cast("string")).alias("event_id"),
        F.col("s").alias("symbol"),
        F.col("a").alias("agg_trade_id"),
        F.col("p").cast("double").alias("price"),
        F.col("q").cast("double").alias("quantity"),
        F.col("T").alias("trade_time"),
        F.col("E").alias("event_time"),
        F.col("m").alias("is_buyer_maker"),
        F.col("_received_at").alias("received_at"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(
            "ingested_at"
        ),
        # Watermark column: event time as a proper timestamp
        (F.col("T") / 1000).cast("timestamp").alias("event_ts"),
    )


def run_silver_trades() -> None:
    """Stream trades from Kafka → deduplicate → upsert into Silver Delta."""
    spark = build_spark_session("cryptostream-silver-trades")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TRADES_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    trades_df = _transform_trades(kafka_df)

    # Dedup within a 30-second watermark window
    deduped_df = (
        trades_df.withWatermark("event_ts", "30 seconds")
        .dropDuplicatesWithinWatermark(["event_id"])
        .drop("event_ts")
    )

    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Upsert each micro-batch into Delta."""
        upsert_to_delta(
            spark=spark,
            micro_batch=batch_df,
            target_path=SILVER_PATH,
            merge_keys=["event_id"],
        )

    query = (
        deduped_df.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", get_checkpoint_path("silver_trades"))
        .trigger(processingTime="15 seconds")
        .queryName("silver_trades")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_silver_trades()
