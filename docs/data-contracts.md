# Data Contracts

## Kafka Topics

### `crypto.trades.v1`

**Partitioning**: by symbol (message key)
**Retention**: 24 hours (dev), 7 days (prod)
**Compression**: LZ4

```json
{
  "e": "aggTrade",
  "E": 1696012345678,
  "s": "BTCUSDT",
  "a": 123456789,
  "p": "27150.50",
  "q": "0.01500",
  "f": 100000001,
  "l": 100000003,
  "T": 1696012345677,
  "m": true,
  "_received_at": 1696012345680
}
```

| Field | Type | Description |
|-------|------|-------------|
| `e` | string | Event type (always `aggTrade`) |
| `E` | long | Event time (epoch ms) |
| `s` | string | Trading pair symbol |
| `a` | long | Aggregate trade ID |
| `p` | string | Price (string to preserve precision) |
| `q` | string | Quantity |
| `f` | long | First trade ID in aggregate |
| `l` | long | Last trade ID in aggregate |
| `T` | long | Trade time (epoch ms) |
| `m` | boolean | Is the buyer the market maker? |
| `_received_at` | long | Producer receive timestamp (epoch ms) |

### `crypto.orderbook.v1`

**Partitioning**: by symbol (message key)

```json
{
  "lastUpdateId": 987654321,
  "s": "BTCUSDT",
  "bids": [["27150.00", "1.500"], ["27149.50", "0.800"]],
  "asks": [["27151.00", "2.100"], ["27151.50", "0.500"]],
  "_received_at": 1696012345680
}
```

## Delta Tables

### Bronze: `raw_events`

Partitioned by: `kafka_topic / dt / hour`

| Column | Type | Source |
|--------|------|--------|
| kafka_topic | string | Kafka metadata |
| kafka_partition | int | Kafka metadata |
| kafka_offset | long | Kafka metadata |
| kafka_timestamp | timestamp | Kafka metadata |
| message_key | string | Kafka key (symbol) |
| message_value | string | Raw JSON payload |
| ingested_at | timestamp | Processing time |
| dt | string | Date partition (yyyy-MM-dd) |
| hour | string | Hour partition (HH) |

### Silver: `trades`

| Column | Type | Source |
|--------|------|--------|
| event_id | string | `{symbol}_{agg_trade_id}` |
| symbol | string | `s` field |
| agg_trade_id | long | `a` field |
| price | double | `p` cast to double |
| quantity | double | `q` cast to double |
| trade_time | long | `T` field (epoch ms) |
| event_time | long | `E` field (epoch ms) |
| is_buyer_maker | boolean | `m` field |
| received_at | long | `_received_at` field |
| ingested_at | string | ISO timestamp |

### Silver: `orderbook`

| Column | Type | Source |
|--------|------|--------|
| event_id | string | `{symbol}_{lastUpdateId}` |
| symbol | string | `s` field |
| last_update_id | long | `lastUpdateId` field |
| best_bid_price | double | `bids[0][0]` |
| best_bid_qty | double | `bids[0][1]` |
| best_ask_price | double | `asks[0][0]` |
| best_ask_qty | double | `asks[0][1]` |
| bid_depth_5 | double | Sum of top 5 bid quantities |
| ask_depth_5 | double | Sum of top 5 ask quantities |
| spread | double | `best_ask_price - best_bid_price` |
| received_at | long | `_received_at` field |
| ingested_at | string | ISO timestamp |

### Gold: `vwap_{1m,5m,15m}`

| Column | Type |
|--------|------|
| window_start | timestamp |
| window_end | timestamp |
| symbol | string |
| vwap | double |
| total_volume | double |
| total_notional | double |
| trade_count | long |
| min_price | double |
| max_price | double |

### Gold: `ohlc_{1m,5m,15m}`

| Column | Type |
|--------|------|
| window_start | timestamp |
| window_end | timestamp |
| symbol | string |
| open | double |
| high | double |
| low | double |
| close | double |
| volume | double |
| quote_volume | double |
| trade_count | long |
| taker_buy_volume | double |

### Gold: `imbalance_1m`

| Column | Type |
|--------|------|
| window_start | timestamp |
| window_end | timestamp |
| symbol | string |
| avg_imbalance | double |
| avg_spread | double |
| avg_best_bid | double |
| avg_best_ask | double |
| avg_bid_depth | double |
| avg_ask_depth | double |
| snapshot_count | long |

## Schema Evolution Strategy

- **Backward compatible only**: new optional fields are added, never removed or renamed
- Topic version bump (`v1` → `v2`) for breaking changes
- Silver layer absorbs schema drift: new fields in Bronze JSON are ignored until Silver is updated
- Gold layer is recomputed from Silver on schema changes
