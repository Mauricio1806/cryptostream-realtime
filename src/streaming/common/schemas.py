"""PySpark StructType schemas for each stream."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Raw Kafka value schemas (JSON inside the Kafka message) ──

TRADE_SCHEMA = StructType(
    [
        StructField("e", StringType(), True),       # event type
        StructField("E", LongType(), True),         # event time
        StructField("s", StringType(), True),        # symbol
        StructField("a", LongType(), True),          # aggregate trade id
        StructField("p", StringType(), True),        # price
        StructField("q", StringType(), True),        # quantity
        StructField("f", LongType(), True),          # first trade id
        StructField("l", LongType(), True),          # last trade id
        StructField("T", LongType(), True),          # trade time
        StructField("m", BooleanType(), True),       # is buyer maker
        StructField("_received_at", LongType(), True),
    ]
)

ORDERBOOK_SCHEMA = StructType(
    [
        StructField("lastUpdateId", LongType(), True),
        StructField("s", StringType(), True),         # symbol (injected)
        StructField(
            "bids",
            ArrayType(ArrayType(StringType())),
            True,
        ),
        StructField(
            "asks",
            ArrayType(ArrayType(StringType())),
            True,
        ),
        StructField("_received_at", LongType(), True),
    ]
)

# ── Silver layer typed schemas ──

SILVER_TRADE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("agg_trade_id", LongType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", DoubleType(), False),
        StructField("trade_time", LongType(), False),
        StructField("event_time", LongType(), False),
        StructField("is_buyer_maker", BooleanType(), False),
        StructField("received_at", LongType(), False),
        StructField("ingested_at", StringType(), False),   # ISO timestamp
    ]
)

SILVER_ORDERBOOK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("last_update_id", LongType(), False),
        StructField("best_bid_price", DoubleType(), True),
        StructField("best_bid_qty", DoubleType(), True),
        StructField("best_ask_price", DoubleType(), True),
        StructField("best_ask_qty", DoubleType(), True),
        StructField("bid_depth_5", DoubleType(), True),   # sum qty top 5 bids
        StructField("ask_depth_5", DoubleType(), True),   # sum qty top 5 asks
        StructField("spread", DoubleType(), True),
        StructField("received_at", LongType(), False),
        StructField("ingested_at", StringType(), False),
    ]
)
