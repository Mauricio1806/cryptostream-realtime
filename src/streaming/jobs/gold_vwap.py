"""Gold layer: Volume-Weighted Average Price (VWAP) in 1m, 5m, 15m windows.

VWAP = Σ(price × quantity) / Σ(quantity) over each time window.
Reads from the Silver trades Delta table as a streaming source.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import window as W

from src.streaming.common.checkpoints import get_checkpoint_path
from src.streaming.common.spark_session import build_spark_session

SILVER_BUCKET = os.getenv("CRYPTOSTREAM_S3_SILVER_BUCKET", "cryptostream-dev-silver")
GOLD_BUCKET = os.getenv("CRYPTOSTREAM_S3_GOLD_BUCKET", "cryptostream-dev-gold")
SILVER_TRADES_PATH = f"s3a://{SILVER_BUCKET}/trades/"


def _compute_vwap(trades_df: DataFrame, window_duration: str) -> DataFrame:
    """Compute VWAP over a tumbling window.

    Args:
        trades_df: Streaming DataFrame with price, quantity, event_ts columns.
        window_duration: Spark window duration string (e.g. '1 minute').

    Returns:
        DataFrame with window_start, window_end, symbol, vwap, total_volume, trade_count.
    """
    return (
        trades_df.withWatermark("event_ts", "30 seconds")
        .groupBy(
            F.window(F.col("event_ts"), window_duration),
            F.col("symbol"),
        )
        .agg(
            (
                F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")
            ).alias("vwap"),
            F.sum("quantity").alias("total_volume"),
            F.sum(F.col("price") * F.col("quantity")).alias("total_notional"),
            F.count("*").alias("trade_count"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "vwap",
            "total_volume",
            "total_notional",
            "trade_count",
            "min_price",
            "max_price",
        )
    )


def run_gold_vwap() -> None:
    """Read Silver trades (streaming), compute VWAP windows, write to Gold Delta."""
    spark = build_spark_session("cryptostream-gold-vwap")

    # Read Silver trades as a streaming source
    trades_df = (
        spark.readStream.format("delta")
        .load(SILVER_TRADES_PATH)
        .withColumn("event_ts", (F.col("trade_time") / 1000).cast("timestamp"))
    )

    # Compute VWAP for each window granularity
    for label, duration in [("1m", "1 minute"), ("5m", "5 minutes"), ("15m", "15 minutes")]:
        vwap_df = _compute_vwap(trades_df, duration)
        output_path = f"s3a://{GOLD_BUCKET}/vwap_{label}/"

        query = (
            vwap_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", get_checkpoint_path(f"gold_vwap_{label}"))
            .option("path", output_path)
            .trigger(processingTime="30 seconds")
            .queryName(f"gold_vwap_{label}")
            .start()
        )

    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_gold_vwap()
