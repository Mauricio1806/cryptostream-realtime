"""Checkpoint path helpers for Spark Structured Streaming."""

from __future__ import annotations

import os


def get_checkpoint_path(job_name: str) -> str:
    """Build the S3 checkpoint path for a given streaming job.

    Args:
        job_name: Logical job name (e.g. 'bronze_trades', 'silver_trades').

    Returns:
        Full S3A path like s3a://cryptostream-dev-checkpoints/bronze_trades/
    """
    base = os.getenv(
        "CRYPTOSTREAM_SPARK_CHECKPOINT_DIR",
        "s3a://cryptostream-dev-checkpoints",
    ).rstrip("/")
    return f"{base}/{job_name}/"
