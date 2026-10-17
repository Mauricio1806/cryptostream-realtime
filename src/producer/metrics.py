"""Lightweight in-process metrics for the producer."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger()


@dataclass
class ProducerMetrics:
    """Tracks throughput counters for logging and monitoring."""

    messages_sent: int = 0
    bytes_sent: int = 0
    errors: int = 0
    _last_report: float = field(default_factory=time.monotonic)
    _report_interval: float = 10.0  # seconds

    def record_send(self, byte_count: int) -> None:
        """Record a successful message send."""
        self.messages_sent += 1
        self.bytes_sent += byte_count
        self._maybe_report()

    def record_error(self) -> None:
        """Record a send failure."""
        self.errors += 1

    def _maybe_report(self) -> None:
        """Log metrics periodically."""
        now = time.monotonic()
        elapsed = now - self._last_report
        if elapsed >= self._report_interval:
            msgs_per_sec = self.messages_sent / elapsed if elapsed > 0 else 0
            kb_per_sec = (self.bytes_sent / 1024) / elapsed if elapsed > 0 else 0
            logger.info(
                "producer_metrics",
                msgs_per_sec=round(msgs_per_sec, 1),
                kb_per_sec=round(kb_per_sec, 2),
                total_messages=self.messages_sent,
                total_errors=self.errors,
            )
            # Reset counters for next window
            self.messages_sent = 0
            self.bytes_sent = 0
            self._last_report = now
