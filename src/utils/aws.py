"""AWS client helpers with retry and timeout configuration."""

from __future__ import annotations

import boto3
import structlog
from botocore.config import Config

logger = structlog.get_logger()

_DEFAULT_CONFIG = Config(
    retries={"max_attempts": 3, "mode": "adaptive"},
    connect_timeout=5,
    read_timeout=30,
)


def get_s3_client(
    endpoint_url: str | None = None,
    region_name: str = "us-east-1",
) -> boto3.client:
    """Create an S3 client with retry and timeout defaults.

    Args:
        endpoint_url: Override for local development (MinIO, LocalStack).
        region_name: AWS region.

    Returns:
        A configured boto3 S3 client.
    """
    kwargs: dict[str, object] = {
        "service_name": "s3",
        "region_name": region_name,
        "config": _DEFAULT_CONFIG,
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(**kwargs)  # type: ignore[arg-type]


def get_glue_client(
    endpoint_url: str | None = None,
    region_name: str = "us-east-1",
) -> boto3.client:
    """Create a Glue client with retry and timeout defaults."""
    kwargs: dict[str, object] = {
        "service_name": "glue",
        "region_name": region_name,
        "config": _DEFAULT_CONFIG,
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(**kwargs)  # type: ignore[arg-type]


def get_athena_client(
    endpoint_url: str | None = None,
    region_name: str = "us-east-1",
) -> boto3.client:
    """Create an Athena client with retry and timeout defaults."""
    kwargs: dict[str, object] = {
        "service_name": "athena",
        "region_name": region_name,
        "config": _DEFAULT_CONFIG,
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(**kwargs)  # type: ignore[arg-type]
