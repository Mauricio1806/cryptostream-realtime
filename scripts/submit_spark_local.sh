#!/usr/bin/env bash
# Submit a Spark streaming job to the local Docker Spark cluster.
# Usage: ./scripts/submit_spark_local.sh <job_module>
# Example: ./scripts/submit_spark_local.sh src.streaming.jobs.bronze_ingestion

set -euo pipefail

JOB_MODULE="${1:?Usage: $0 <job_module> (e.g. src.streaming.jobs.bronze_ingestion)}"

SPARK_MASTER="${SPARK_MASTER:-spark://localhost:7077}"
PACKAGES="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"

# Delta Lake configuration
DELTA_CONF=(
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# S3A / MinIO configuration
S3A_CONF=(
    --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000"
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin"
    --conf "spark.hadoop.fs.s3a.secret.key=minioadmin"
    --conf "spark.hadoop.fs.s3a.path.style.access=true"
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
)

echo "==> Submitting job: ${JOB_MODULE}"
echo "    Spark Master: ${SPARK_MASTER}"

# Convert module path to file path
JOB_FILE="${JOB_MODULE//.//}.py"

spark-submit \
    --master "${SPARK_MASTER}" \
    --packages "${PACKAGES}" \
    "${DELTA_CONF[@]}" \
    "${S3A_CONF[@]}" \
    --conf "spark.sql.shuffle.partitions=8" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.adaptive.enabled=true" \
    --driver-memory 1g \
    --executor-memory 1g \
    "${JOB_FILE}"
