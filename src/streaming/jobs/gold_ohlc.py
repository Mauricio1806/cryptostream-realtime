"""Gold layer: OHLC (candlestick) aggregation in 1m, 5m, 15m windows.

Produces standard candlestick data: open, high, low, close, volume.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.common.checkpoints import get_checkpoint_path
from src.streaming.common.spark_session import build_spark_session

SILVER_BUCKET = os.getenv("CRYPTOSTREAM_S3_SILVER_BUCKET", "cryptostream-dev-silver")
GOLD_BUCKET = os.getenv("CRYPTOSTREAM_S3_GOLD_BUCKET", "cryptostream-dev-gold")
SILVER_TRADES_PATH = f"s3a://{SILVER_BUCKET}/trades/"


def _compute_ohlc(trades_df: DataFrame, window_duration: str) -> DataFrame:
    """Compute OHLC candles over a tumbling window.

    Open = price of the first trade in the window (by trade_time).
    Close = price of the last trade in the window.
    """
    return (
        trades_df.withWatermark("event_ts", "30 seconds")
        .groupBy(
            F.window(F.col("event_ts"), window_duration),
            F.col("symbol"),
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.sum(F.col("price") * F.col("quantity")).alias("quote_volume"),
            F.count("*").alias("trade_count"),
            F.sum(
                F.when(~F.col("is_buyer_maker"), F.col("quantity")).otherwise(0)
            ).alias("taker_buy_volume"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "taker_buy_volume",
        )
    )


def run_gold_ohlc() -> None:
    """Read Silver trades, compute OHLC candles, write to Gold Delta."""
    spark = build_spark_session("cryptostream-gold-ohlc")

    trades_df = (
        spark.readStream.format("delta")
        .load(SILVER_TRADES_PATH)
        .withColumn("event_ts", (F.col("trade_time") / 1000).cast("timestamp"))
    )

    for label, duration in [("1m", "1 minute"), ("5m", "5 minutes"), ("15m", "15 minutes")]:
        ohlc_df = _compute_ohlc(trades_df, duration)
        output_path = f"s3a://{GOLD_BUCKET}/ohlc_{label}/"

        query = (
            ohlc_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", get_checkpoint_path(f"gold_ohlc_{label}"))
            .option("path", output_path)
            .trigger(processingTime="30 seconds")
            .queryName(f"gold_ohlc_{label}")
            .start()
        )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_gold_ohlc()
