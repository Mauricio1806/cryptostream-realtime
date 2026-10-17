"""Structured JSON logging via structlog."""

from __future__ import annotations

import logging
import sys
import uuid

import structlog


def setup_logging(level: str = "INFO") -> None:
    """Configure structlog with JSON output and a correlation ID processor.

    Args:
        level: Standard Python log level name (DEBUG, INFO, WARNING, ERROR).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            _add_correlation_id,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def _add_correlation_id(
    logger: object,
    method_name: str,
    event_dict: dict[str, object],
) -> dict[str, object]:
    """Add a correlation_id if one is not already bound."""
    if "correlation_id" not in event_dict:
        event_dict["correlation_id"] = str(uuid.uuid4())[:8]
    return event_dict
