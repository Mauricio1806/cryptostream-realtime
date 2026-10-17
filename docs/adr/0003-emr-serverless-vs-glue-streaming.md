# ADR-0003: Use EMR Serverless over AWS Glue Streaming

## Status

Accepted

## Date

2024-10-01

## Context

We need a managed Spark runtime on AWS for our Structured Streaming jobs. The
pipeline runs 6 concurrent streaming queries (Bronze ingestion, 2 Silver jobs,
3 Gold aggregations) that must run continuously.

The two AWS-managed options are:

- **EMR Serverless**: auto-scaling Spark/Hive runtime, pay per vCPU-hour and GB-hour
- **AWS Glue Streaming**: managed ETL service with Spark under the hood, pay per DPU-hour

## Decision

We will use **EMR Serverless** as our Spark runtime for all streaming jobs.

## Rationale

| Criterion | EMR Serverless | AWS Glue Streaming |
|-----------|---------------|-------------------|
| Spark version control | Choose specific EMR release (6.15 = Spark 3.5) | Glue 4.0 ships Spark 3.3 (older) |
| Delta Lake support | Pre-installed Delta JARs, any version via `--packages` | Supported but requires manual JAR management |
| Spark configuration | Full `spark-defaults.conf` control | Limited — some settings locked by Glue |
| Debugging | Spark UI via EMR Studio, full driver/executor logs | Limited Spark UI, Glue console metrics |
| Cost (sustained) | ~$0.052/vCPU-hr + $0.0057/GB-hr | $0.44/DPU-hr (1 DPU = 4 vCPU + 16 GB) |
| Startup latency | Pre-initialized workers (< 30s with pre-init) | 1-3 minutes cold start |
| Auto-scaling | vCPU/memory based, sub-minute response | DPU-based, slower |
| Job packaging | Standard spark-submit with `--py-files` | Glue script + `--additional-python-modules` |
| Streaming checkpoints | Standard Spark checkpoint to S3 | Same, but Glue adds its own bookmark layer |
| IAM integration | Job execution role with fine-grained S3/Glue access | Glue service role — broader default perms |

### Key factors

1. **Cost for sustained workloads**: Our streaming jobs run 24/7. EMR Serverless
   charges ~$0.052/vCPU-hr vs Glue's ~$0.11/vCPU-hr (DPU translated). For 8 vCPUs
   running continuously, that's ~$300/mo vs ~$640/mo — EMR is roughly 50% cheaper.

2. **Spark version & configuration**: EMR Serverless lets us pin Spark 3.5 with
   Delta Lake 3.2 and full control over `spark.sql.shuffle.partitions`,
   `spark.serializer`, S3A committer settings, etc. Glue abstracts away many of
   these knobs.

3. **Debugging**: Spark UI is essential for diagnosing streaming lag, skewed
   partitions, and checkpoint issues. EMR Serverless provides the full Spark UI
   via EMR Studio; Glue's monitoring is more limited.

4. **Standard spark-submit**: Our local development uses `spark-submit` via Docker.
   EMR Serverless accepts the same `spark-submit` arguments, making the dev-to-prod
   transition seamless. Glue requires a different packaging and submission model.

5. **Pre-initialized capacity**: EMR Serverless supports pre-initialized workers
   that stay warm, reducing cold-start to under 30 seconds. Critical for streaming
   jobs that need to restart quickly after a failure.

## Consequences

### Positive

- ~50% lower cost for sustained 24/7 streaming workloads
- Full Spark configuration control and version pinning
- Standard spark-submit interface — same as local Docker dev
- Full Spark UI for debugging and performance tuning
- Pre-initialized workers for fast restart on failure

### Negative

- More operational surface — we manage EMR application lifecycle, VPC networking,
  and S3 log configuration ourselves
- No built-in job orchestration — we need MWAA (Airflow) for scheduling maintenance
  tasks, whereas Glue has built-in triggers and workflows
- Newer service — documentation and community examples are less abundant than Glue

### Mitigations

- Terraform modules (`modules/emr-serverless/`) encapsulate the operational
  complexity — teams interact via `terraform apply`, not AWS console
- MWAA is already in our stack for Delta OPTIMIZE/VACUUM scheduling, so Airflow
  orchestration is not additional complexity
- EMR Serverless is GA since 2022 and actively maintained; AWS investment is
  increasing (EMR Studio, Spark Connect support)
