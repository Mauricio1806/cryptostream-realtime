# CryptoStream Realtime

![Python](https://img.shields.io/badge/python-3.12-blue)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/delta--lake-3.2-green)
![License](https://img.shields.io/badge/license-MIT-brightgreen)
![CI](https://img.shields.io/github/actions/workflow/status/mauricio-behrens/cryptostream-realtime/ci.yml?label=CI)

**Production-grade streaming data pipeline** that ingests real-time cryptocurrency market data from Binance WebSocket, processes it through Apache Kafka and Spark Structured Streaming, and lands it in a Delta Lake medallion architecture on AWS S3.

```
┌──────────────┐     ┌───────────┐     ┌──────────────────────────┐     ┌─────────────┐
│   Binance    │────▶│   Kafka   │────▶│  Spark Structured        │────▶│  S3 + Delta  │
│  WebSocket   │     │  (MSK)    │     │  Streaming (EMR)         │     │   Lake       │
│              │     │           │     │                          │     │              │
│ aggTrade     │     │ trades.v1 │     │ ┌────────┐ ┌──────────┐ │     │ ┌──────────┐ │
│ depth@100ms  │     │ orderb.v1 │     │ │ Bronze │▶│  Silver  │ │     │ │   Gold   │ │
│              │     │           │     │ │  raw   │ │  typed   │▶│────▶│ │ VWAP     │ │
│ BTC/ETH/SOL  │     │ 3 parts  │     │ │  JSON  │ │  deduped │ │     │ │ OHLC     │ │
└──────────────┘     └───────────┘     │ └────────┘ └──────────┘ │     │ │ Imbalance│ │
                                       └──────────────────────────┘     │ └──────────┘ │
                                                                        └──────┬──────┘
                                                                               │
                                                                    ┌──────────▼──────────┐
                                                                    │  Glue Catalog       │
                                                                    │  + Athena Queries   │
                                                                    └─────────────────────┘
```

## Why This Project Matters

This project demonstrates senior-level data engineering competencies that go beyond typical batch ETL:

- **Real-time streaming** with exactly-once semantics (Kafka idempotent producer + Spark checkpointing + Delta ACID transactions)
- **Microstructure analytics** — VWAP, OHLC candles, and order book imbalance are metrics used by quantitative trading desks
- **Medallion architecture** (Bronze → Silver → Gold) with proper data contracts, schema evolution strategy, and deduplication via watermarks
- **Production-ready infrastructure** — Terraform IaC, GitHub Actions CI/CD, structured logging, CloudWatch observability
- **Cost-conscious design** — EMR Serverless for pay-per-use compute, S3 lifecycle policies, Athena's scan-based pricing

## Quickstart

### Prerequisites

- Docker & Docker Compose v2+
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Make

### 1. Clone & install

```bash
git clone https://github.com/mauricio-behrens/cryptostream-realtime.git
cd cryptostream-realtime
cp .env.example .env
uv sync
```

### 2. Start infrastructure

```bash
make up          # Starts Kafka, Spark, MinIO, LocalStack
make bootstrap   # Creates Kafka topics + MinIO buckets
```

### 3. Run the producer

```bash
make run-producer
# Watch real-time trades flowing in → open http://localhost:8080 (Kafka UI)
```

### 4. Submit Spark jobs

```bash
make submit-bronze          # Raw Kafka → Bronze Delta
make submit-silver-trades   # Bronze → Silver (typed, deduped)
make submit-gold-vwap       # Silver → Gold VWAP aggregations
```

### 5. Explore the data

- **Kafka UI**: http://localhost:8080
- **Spark Master UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)

## AWS Services & Estimated Cost

| Service | Purpose | Est. Monthly Cost (dev) |
|---------|---------|------------------------|
| MSK (t3.small × 3) | Kafka cluster | ~$75 |
| EMR Serverless | Spark streaming jobs | ~$50-150 (usage-based) |
| S3 | Bronze/Silver/Gold storage | ~$5-20 |
| Glue Data Catalog | Schema registry + crawlers | ~$1 |
| Athena | Ad-hoc queries | ~$5 (per TB scanned) |
| MWAA (mw1.small) | Airflow for maintenance DAGs | ~$50 |
| **Total** | | **~$186-301/mo** |

> **Note**: Production costs scale with throughput and retention. The architecture supports scaling MSK brokers and EMR workers independently.

## Project Structure

```
cryptostream-realtime/
├── src/
│   ├── producer/          # Binance WS → Kafka (asyncio + confluent-kafka)
│   ├── streaming/         # Spark Structured Streaming jobs
│   │   ├── jobs/          # Bronze, Silver, Gold layer jobs
│   │   └── common/        # Shared Spark utilities
│   └── utils/             # Logging, AWS helpers
├── infra/terraform/       # IaC: VPC, MSK, S3, Glue, EMR, MWAA, IAM
├── airflow/dags/          # Delta maintenance DAGs
├── docker/                # Dockerfiles for producer + Spark
├── scripts/               # Bootstrap & helper scripts
├── tests/                 # Unit + integration tests
├── docs/                  # Architecture, data contracts, ADRs
└── .github/workflows/     # CI/CD pipelines
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Kafka not starting | Check Zookeeper health: `docker logs cs-zookeeper` |
| Producer can't connect to Binance | Ensure outbound WSS on port 9443 is not blocked |
| Spark job fails on Delta | Verify packages: `--packages io.delta:delta-spark_2.12:3.2.0` |
| MinIO buckets not found | Run `make bootstrap` after `make up` |
| Kafka UI blank | Wait 30s after `make up` for Kafka to initialize |

## Roadmap

1. **Schema Registry** — Confluent Schema Registry with Avro/Protobuf schemas for contract enforcement
2. **Real-time dashboard** — Grafana + Prometheus metrics from Spark + producer
3. **Alert system** — SNS notifications on order book imbalance spikes or spread anomalies
4. **Multi-exchange support** — Add Coinbase, Kraken, OKX WebSocket sources
5. **ML feature store** — Feast integration for serving VWAP/OHLC features to prediction models

## License

MIT
