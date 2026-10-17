"""Silver layer: parse, flatten and upsert order book snapshots.

Extracts best bid/ask, top-5 depth, and spread from raw order book updates.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.common.checkpoints import get_checkpoint_path
from src.streaming.common.delta_utils import upsert_to_delta
from src.streaming.common.schemas import ORDERBOOK_SCHEMA
from src.streaming.common.spark_session import build_spark_session

KAFKA_SERVERS = os.getenv("CRYPTOSTREAM_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDERBOOK_TOPIC = os.getenv("CRYPTOSTREAM_KAFKA_ORDERBOOK_TOPIC", "crypto.orderbook.v1")
SILVER_BUCKET = os.getenv("CRYPTOSTREAM_S3_SILVER_BUCKET", "cryptostream-dev-silver")
SILVER_PATH = f"s3a://{SILVER_BUCKET}/orderbook/"


def _transform_orderbook(raw_df: DataFrame) -> DataFrame:
    """Parse JSON, extract top-of-book metrics."""
    parsed = raw_df.select(
        F.from_json(F.col("value").cast("string"), ORDERBOOK_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_ts"),
    ).select("data.*", "kafka_ts")

    return parsed.select(
        F.concat_ws("_", F.col("s"), F.col("lastUpdateId").cast("string")).alias("event_id"),
        F.col("s").alias("symbol"),
        F.col("lastUpdateId").alias("last_update_id"),
        # Best bid (first element)
        F.col("bids").getItem(0).getItem(0).cast("double").alias("best_bid_price"),
        F.col("bids").getItem(0).getItem(1).cast("double").alias("best_bid_qty"),
        # Best ask (first element)
        F.col("asks").getItem(0).getItem(0).cast("double").alias("best_ask_price"),
        F.col("asks").getItem(0).getItem(1).cast("double").alias("best_ask_qty"),
        # Top 5 bid depth (sum of quantities)
        F.aggregate(
            F.slice(F.col("bids"), 1, 5),
            F.lit(0.0),
            lambda acc, x: acc + x.getItem(1).cast("double"),
        ).alias("bid_depth_5"),
        # Top 5 ask depth
        F.aggregate(
            F.slice(F.col("asks"), 1, 5),
            F.lit(0.0),
            lambda acc, x: acc + x.getItem(1).cast("double"),
        ).alias("ask_depth_5"),
        # Spread
        (
            F.col("asks").getItem(0).getItem(0).cast("double")
            - F.col("bids").getItem(0).getItem(0).cast("double")
        ).alias("spread"),
        F.col("_received_at").alias("received_at"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(
            "ingested_at"
        ),
        # Watermark column
        (F.col("_received_at") / 1000).cast("timestamp").alias("event_ts"),
    )


def run_silver_orderbook() -> None:
    """Stream order book snapshots → deduplicate → upsert into Silver Delta."""
    spark = build_spark_session("cryptostream-silver-orderbook")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", ORDERBOOK_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    orderbook_df = _transform_orderbook(kafka_df)

    deduped_df = (
        orderbook_df.withWatermark("event_ts", "30 seconds")
        .dropDuplicatesWithinWatermark(["event_id"])
        .drop("event_ts")
    )

    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        upsert_to_delta(
            spark=spark,
            micro_batch=batch_df,
            target_path=SILVER_PATH,
            merge_keys=["event_id"],
        )

    query = (
        deduped_df.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", get_checkpoint_path("silver_orderbook"))
        .trigger(processingTime="15 seconds")
        .queryName("silver_orderbook")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_silver_orderbook()
