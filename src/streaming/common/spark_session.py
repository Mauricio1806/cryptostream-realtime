"""Spark session builder with Delta Lake and S3A configuration."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "cryptostream") -> SparkSession:
    """Build a SparkSession pre-configured for Delta Lake + S3A (MinIO in dev).

    Args:
        app_name: Application name shown in the Spark UI.

    Returns:
        A configured SparkSession.
    """
    env = os.getenv("CRYPTOSTREAM_ENV", "dev")
    s3_endpoint = os.getenv("CRYPTOSTREAM_S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("CRYPTOSTREAM_S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("CRYPTOSTREAM_S3_SECRET_KEY", "minioadmin")
    checkpoint_dir = os.getenv(
        "CRYPTOSTREAM_SPARK_CHECKPOINT_DIR",
        "s3a://cryptostream-dev-checkpoints/",
    )

    builder = (
        SparkSession.builder.appName(f"{app_name}-{env}")
        # Delta Lake
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Streaming
        .config(
            "spark.sql.streaming.checkpointLocation",
            checkpoint_dir,
        )
        .config("spark.sql.streaming.schemaInference", "false")
        # Performance
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    return builder.getOrCreate()
