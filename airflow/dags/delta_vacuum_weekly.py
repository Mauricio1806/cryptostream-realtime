"""DAG: Weekly Delta Lake VACUUM on all tables.

Removes files no longer referenced by the Delta transaction log, reclaiming
storage. Default retention is 168 hours (7 days). Runs every Sunday at 05:00 UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3


TABLES = [
    {"bucket": "{{ var.value.s3_silver_bucket }}", "prefix": "trades/"},
    {"bucket": "{{ var.value.s3_silver_bucket }}", "prefix": "orderbook/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "vwap_1m/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "vwap_5m/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "vwap_15m/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "ohlc_1m/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "ohlc_5m/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "ohlc_15m/"},
    {"bucket": "{{ var.value.s3_gold_bucket }}", "prefix": "imbalance_1m/"},
]

RETENTION_HOURS = 168  # 7 days


def submit_vacuum_job(table: dict, **kwargs) -> str:
    """Submit an EMR Serverless job to run VACUUM on a Delta table."""
    client = boto3.client("emr-serverless")

    table_path = f"s3a://{table['bucket']}/{table['prefix']}"
    table_name = table["prefix"].rstrip("/")

    script = f"""
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("vacuum-{table_name}").getOrCreate()
dt = DeltaTable.forPath(spark, "{table_path}")
dt.vacuum({RETENTION_HOURS})
print(f"VACUUM complete for {table_path}, retention={RETENTION_HOURS}h")
spark.stop()
"""

    response = client.start_job_run(
        applicationId="{{ var.value.emr_application_id }}",
        executionRoleArn="{{ var.value.emr_job_role_arn }}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": "/tmp/vacuum_script.py",
                "sparkSubmitParameters": (
                    "--conf spark.jars.packages=io.delta:delta-spark_2.12:3.2.0 "
                    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
                    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "{{ var.value.s3_logs_bucket }}/emr-logs/"
                }
            }
        },
        name=f"vacuum-{table_name}",
    )

    job_run_id = response["jobRunId"]
    print(f"Submitted VACUUM job {job_run_id} for {table_path}")
    return job_run_id


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="delta_vacuum_weekly",
    description="Weekly VACUUM on Delta Lake tables to reclaim storage",
    schedule_interval="0 5 * * 0",  # Sunday 05:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["delta", "maintenance", "weekly"],
) as dag:
    for table in TABLES:
        table_name = table["prefix"].rstrip("/")
        PythonOperator(
            task_id=f"vacuum_{table_name}",
            python_callable=submit_vacuum_job,
            op_kwargs={"table": table},
        )
