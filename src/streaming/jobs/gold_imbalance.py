"""Gold layer: order book imbalance score in 1-minute tumbling windows.

Imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
Range: -1 (all asks, bearish) to +1 (all bids, bullish).
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.common.checkpoints import get_checkpoint_path
from src.streaming.common.spark_session import build_spark_session

SILVER_BUCKET = os.getenv("CRYPTOSTREAM_S3_SILVER_BUCKET", "cryptostream-dev-silver")
GOLD_BUCKET = os.getenv("CRYPTOSTREAM_S3_GOLD_BUCKET", "cryptostream-dev-gold")
SILVER_ORDERBOOK_PATH = f"s3a://{SILVER_BUCKET}/orderbook/"


def _compute_imbalance(ob_df: DataFrame) -> DataFrame:
    """Compute average order book imbalance per 1-minute window."""
    return (
        ob_df.withWatermark("event_ts", "30 seconds")
        .groupBy(
            F.window(F.col("event_ts"), "1 minute"),
            F.col("symbol"),
        )
        .agg(
            F.avg(
                (F.col("bid_depth_5") - F.col("ask_depth_5"))
                / (F.col("bid_depth_5") + F.col("ask_depth_5"))
            ).alias("avg_imbalance"),
            F.avg("spread").alias("avg_spread"),
            F.avg("best_bid_price").alias("avg_best_bid"),
            F.avg("best_ask_price").alias("avg_best_ask"),
            F.avg("bid_depth_5").alias("avg_bid_depth"),
            F.avg("ask_depth_5").alias("avg_ask_depth"),
            F.count("*").alias("snapshot_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "avg_imbalance",
            "avg_spread",
            "avg_best_bid",
            "avg_best_ask",
            "avg_bid_depth",
            "avg_ask_depth",
            "snapshot_count",
        )
    )


def run_gold_imbalance() -> None:
    """Read Silver order book, compute imbalance, write to Gold Delta."""
    spark = build_spark_session("cryptostream-gold-imbalance")

    ob_df = (
        spark.readStream.format("delta")
        .load(SILVER_ORDERBOOK_PATH)
        .withColumn("event_ts", (F.col("received_at") / 1000).cast("timestamp"))
    )

    imbalance_df = _compute_imbalance(ob_df)
    output_path = f"s3a://{GOLD_BUCKET}/imbalance_1m/"

    query = (
        imbalance_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", get_checkpoint_path("gold_imbalance_1m"))
        .option("path", output_path)
        .trigger(processingTime="30 seconds")
        .queryName("gold_imbalance_1m")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_gold_imbalance()
