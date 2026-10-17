"""Delta Lake utility functions: upsert, optimize, vacuum."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert_to_delta(
    spark: SparkSession,
    micro_batch: DataFrame,
    target_path: str,
    merge_keys: list[str],
) -> None:
    """Merge (upsert) a micro-batch into a Delta table.

    If the table doesn't exist yet, it creates it from the first batch.

    Args:
        spark: Active SparkSession.
        micro_batch: The micro-batch DataFrame from Structured Streaming.
        target_path: S3A path to the Delta table.
        merge_keys: Columns to use as the merge condition.
    """
    if micro_batch.isEmpty():
        return

    if not DeltaTable.isDeltaTable(spark, target_path):
        # First batch — create the table
        micro_batch.write.format("delta").mode("overwrite").save(target_path)
        return

    delta_table = DeltaTable.forPath(spark, target_path)
    merge_condition = " AND ".join(
        [f"target.{key} = source.{key}" for key in merge_keys]
    )

    (
        delta_table.alias("target")
        .merge(micro_batch.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def optimize_table(spark: SparkSession, table_path: str) -> None:
    """Run OPTIMIZE on a Delta table to compact small files.

    Args:
        spark: Active SparkSession.
        table_path: S3A path to the Delta table.
    """
    if DeltaTable.isDeltaTable(spark, table_path):
        spark.sql(f"OPTIMIZE delta.`{table_path}`")


def vacuum_table(
    spark: SparkSession,
    table_path: str,
    retention_hours: int = 168,
) -> None:
    """Run VACUUM on a Delta table to remove old files.

    Args:
        spark: Active SparkSession.
        table_path: S3A path to the Delta table.
        retention_hours: Files older than this are deleted (default 7 days).
    """
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.vacuum(retention_hours)
