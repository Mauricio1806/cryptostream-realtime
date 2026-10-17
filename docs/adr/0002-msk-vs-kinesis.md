# ADR-0002: Use Amazon MSK over Kinesis Data Streams

## Status

Accepted

## Date

2024-10-01

## Context

We need a managed streaming platform on AWS to buffer real-time cryptocurrency
market data between our WebSocket producer and Spark consumers.

The two AWS-native options are:

- **Amazon MSK** (Managed Streaming for Apache Kafka)
- **Amazon Kinesis Data Streams**

Our producer generates ~5,000 messages/second across trades and order book updates
for 3 trading pairs, with potential to scale to 20+ pairs.

## Decision

We will use **Amazon MSK** (Serverless or provisioned) as our streaming platform.

## Rationale

| Criterion | MSK (Kafka) | Kinesis Data Streams |
|-----------|-------------|----------------------|
| Protocol | Standard Apache Kafka (open source) | AWS-proprietary API |
| Partitioning | Topic partitions, consumer groups | Shards (fixed throughput per shard) |
| Retention | Configurable (hours to infinite) | Default 24h, max 365 days |
| Throughput scaling | Add partitions, no per-partition limit | Add shards ($$$), each limited to 1 MB/s in, 2 MB/s out |
| Consumer model | Consumer groups with offset tracking | Shard iterators, enhanced fan-out |
| Spark integration | `spark-sql-kafka` — mature, well-documented | `spark-streaming-kinesis-asl` — less commonly used |
| Local development | Docker (cp-kafka) — identical protocol | LocalStack emulation — not identical |
| Ecosystem | Schema Registry, Kafka Connect, ksqlDB, Kafka UI | Limited to AWS services |
| Cost model | Instance-based (provisioned) or capacity-based (serverless) | Per-shard-hour + per-PUT payload |

### Key factors

1. **Local dev parity**: With Docker Compose running `confluentinc/cp-kafka`, our
   local environment uses the exact same Kafka protocol as production MSK. Kinesis
   would require LocalStack emulation, which doesn't replicate all behaviors.

2. **Consumer groups**: Kafka consumer groups allow multiple independent consumers
   (Bronze ingestion, monitoring, replay) without provisioning additional shards.
   Kinesis enhanced fan-out costs extra per consumer.

3. **Spark integration**: `spark-sql-kafka` is the most battle-tested streaming
   source for Spark. It handles offset management, checkpointing, and exactly-once
   semantics out of the box.

4. **Portability**: Using standard Kafka means we can migrate to Confluent Cloud,
   Redpanda, or self-managed Kafka without changing application code.

5. **Ecosystem**: Kafka UI gives us free topic inspection. Schema Registry is
   available for future Avro/Protobuf enforcement. Neither has a Kinesis equivalent.

## Consequences

### Positive

- Identical protocol in dev (Docker) and prod (MSK)
- Mature Spark-Kafka integration with well-known offset semantics
- Consumer groups enable independent consumers without extra cost
- Full Kafka ecosystem (Schema Registry, Connect, ksqlDB) available for future use
- Portable — not locked to AWS

### Negative

- MSK provisioned clusters have a higher base cost (~$75/mo for 3× t3.small) vs
  Kinesis on-demand which starts at ~$15/mo for 1 shard
- MSK requires VPC networking (private subnets, security groups) — more Terraform
- MSK provisioned clusters need capacity planning; Kinesis on-demand auto-scales

### Mitigations

- **MSK Serverless** eliminates capacity planning and reduces cost for bursty
  workloads — evaluate once traffic patterns stabilize
- VPC complexity is absorbed by our Terraform modules (`modules/vpc/`, `modules/msk/`)
- CloudWatch metrics on MSK broker CPU and disk guide scaling decisions
