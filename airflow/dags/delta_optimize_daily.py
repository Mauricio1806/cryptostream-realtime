"""DAG: Daily Delta Lake OPTIMIZE on Silver and Gold tables.

Compacts small files produced by streaming micro-batches into larger,
scan-efficient Parquet files. Runs daily at 03:00 UTC during low-traffic
hours.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

APPLICATION_ID = "{{ var.value.emr_application_id }}"
EXECUTION_ROLE = "{{ var.value.emr_job_role_arn }}"
S3_LOGS = "{{ var.value.s3_logs_bucket }}/emr-logs/"

TABLES_TO_OPTIMIZE = [
    "s3a://{{ var.value.s3_silver_bucket }}/trades/",
    "s3a://{{ var.value.s3_silver_bucket }}/orderbook/",
    "s3a://{{ var.value.s3_gold_bucket }}/vwap_1m/",
    "s3a://{{ var.value.s3_gold_bucket }}/vwap_5m/",
    "s3a://{{ var.value.s3_gold_bucket }}/vwap_15m/",
    "s3a://{{ var.value.s3_gold_bucket }}/ohlc_1m/",
    "s3a://{{ var.value.s3_gold_bucket }}/ohlc_5m/",
    "s3a://{{ var.value.s3_gold_bucket }}/ohlc_15m/",
    "s3a://{{ var.value.s3_gold_bucket }}/imbalance_1m/",
]

SPARK_PROPERTIES = {
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="delta_optimize_daily",
    description="Daily OPTIMIZE on Delta Lake Silver and Gold tables",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["delta", "maintenance", "daily"],
) as dag:
    for i, table_path in enumerate(TABLES_TO_OPTIMIZE):
        table_name = table_path.rstrip("/").split("/")[-1]

        optimize_script = f"""
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("optimize-{table_name}").getOrCreate()
dt = DeltaTable.forPath(spark, "{table_path}")
dt.optimize().executeCompaction()
print(f"OPTIMIZE complete for {table_path}")
spark.stop()
"""

        EmrServerlessStartJobOperator(
            task_id=f"optimize_{table_name}",
            application_id=APPLICATION_ID,
            execution_role_arn=EXECUTION_ROLE,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "command-runner.jar",
                    "entryPointArguments": ["spark-submit", "--conf", f"spark.pyspark.python.script={optimize_script}"],
                    "sparkSubmitParameters": " ".join(
                        f"--conf {k}={v}" for k, v in SPARK_PROPERTIES.items()
                    ),
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": S3_LOGS}
                }
            },
        )
