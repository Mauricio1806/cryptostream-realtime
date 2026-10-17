# ADR-0001: Use Delta Lake over Apache Iceberg

## Status

Accepted

## Date

2024-10-01

## Context

We need an open table format for our medallion architecture on S3. The two leading
contenders are **Delta Lake** and **Apache Iceberg**. Both provide ACID transactions,
time travel, and schema evolution on top of object storage.

Our pipeline is **Spark Structured Streaming** writing micro-batches every 10-30 seconds.
The write pattern is append-heavy for Bronze and upsert (MERGE) for Silver/Gold.

## Decision

We will use **Delta Lake 3.2** as the table format across all layers.

## Rationale

| Criterion | Delta Lake | Apache Iceberg |
|-----------|-----------|----------------|
| Spark integration | Native (delta-spark package, first-class `MERGE`) | Good but requires iceberg-spark-runtime JAR + catalog config |
| Streaming upserts | `foreachBatch` + `DeltaTable.merge()` is a documented, battle-tested pattern | Requires `MERGE INTO` via SparkSQL; less ergonomic from DataFrame API |
| Small-file compaction | Built-in `OPTIMIZE` with Z-ORDER | Requires external `rewriteDataFiles` action or Flink compaction |
| Vacuum / retention | `VACUUM` command with configurable retention | `expireSnapshots` + `deleteOrphanFiles` — two operations instead of one |
| Glue Catalog support | Native via `delta.enableIcebergCompatV2` symlink manifest or direct Glue integration | Native via `GlueCatalog` — slightly more mature |
| Community momentum | Strong (Databricks-backed, widely adopted in Spark shops) | Growing fast (multi-engine: Flink, Trino, Dremio) |
| EMR Serverless | Pre-installed Delta libraries since EMR 6.9 | Supported but requires explicit JAR packaging |

### Key factors

1. **MERGE ergonomics**: Our Silver layer does watermark-based deduplication via
   `DeltaTable.forName().alias("target").merge(batch.alias("source"), condition)`.
   This pattern is cleaner than Iceberg's SQL-only merge path.

2. **OPTIMIZE + VACUUM**: Streaming micro-batches produce many small files. Delta's
   single-command `OPTIMIZE` is simpler to schedule in Airflow than Iceberg's
   multi-step compaction.

3. **EMR Serverless**: Delta jars ship pre-installed on EMR, reducing job startup
   time and packaging complexity.

## Consequences

### Positive

- Simpler Spark job code for streaming upserts
- Single `OPTIMIZE` command for compaction, easy to schedule
- Pre-installed on EMR Serverless — no JAR management
- Time travel enables easy debugging of pipeline issues

### Negative

- Tighter coupling to Spark — if we ever adopt Flink or Trino as a query engine,
  Iceberg would offer better cross-engine support
- Delta's open-source governance is Databricks-driven; Iceberg has broader
  vendor-neutral backing (Apple, Netflix, AWS)

### Mitigations

- Delta Lake 3.x supports UniForm, which writes Iceberg-compatible metadata
  alongside Delta logs, enabling future migration
- Our Glue Catalog registration means Athena can query the tables regardless of
  the underlying format
