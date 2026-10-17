"""Shared fixtures for unit and integration tests."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import pytest


# ──────────────────────────────────────────────
# Raw Binance message fixtures
# ──────────────────────────────────────────────

@pytest.fixture()
def raw_agg_trade() -> dict[str, Any]:
    """Sample Binance aggTrade message as received from WebSocket."""
    return {
        "e": "aggTrade",
        "E": 1696012345678,
        "s": "BTCUSDT",
        "a": 123456789,
        "p": "27150.50000000",
        "q": "0.01500000",
        "f": 100000001,
        "l": 100000003,
        "T": 1696012345677,
        "m": True,
    }


@pytest.fixture()
def raw_depth_update() -> dict[str, Any]:
    """Sample Binance depthUpdate message as received from WebSocket."""
    return {
        "e": "depthUpdate",
        "E": 1696012345678,
        "s": "BTCUSDT",
        "U": 987654320,
        "u": 987654321,
        "b": [
            ["27150.00000000", "1.50000000"],
            ["27149.50000000", "0.80000000"],
            ["27149.00000000", "2.30000000"],
            ["27148.50000000", "0.50000000"],
            ["27148.00000000", "3.10000000"],
        ],
        "a": [
            ["27151.00000000", "2.10000000"],
            ["27151.50000000", "0.50000000"],
            ["27152.00000000", "1.80000000"],
            ["27152.50000000", "0.90000000"],
            ["27153.00000000", "4.20000000"],
        ],
    }


@pytest.fixture()
def combined_stream_trade(raw_agg_trade: dict) -> dict[str, Any]:
    """WebSocket message in combined stream format."""
    return {
        "stream": "btcusdt@aggTrade",
        "data": raw_agg_trade,
    }


@pytest.fixture()
def combined_stream_depth(raw_depth_update: dict) -> dict[str, Any]:
    """WebSocket message in combined stream format."""
    return {
        "stream": "btcusdt@depth@100ms",
        "data": raw_depth_update,
    }


# ──────────────────────────────────────────────
# Environment fixtures
# ──────────────────────────────────────────────

@pytest.fixture()
def env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set environment variables for testing."""
    monkeypatch.setenv("CRYPTOSTREAM_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("CRYPTOSTREAM_KAFKA_TRADES_TOPIC", "test.trades.v1")
    monkeypatch.setenv("CRYPTOSTREAM_KAFKA_ORDERBOOK_TOPIC", "test.orderbook.v1")
    monkeypatch.setenv("CRYPTOSTREAM_BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
    monkeypatch.setenv("CRYPTOSTREAM_BINANCE_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("CRYPTOSTREAM_BINANCE_STREAMS", "aggTrade,depth@100ms")
    monkeypatch.setenv("CRYPTOSTREAM_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("CRYPTOSTREAM_S3_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("CRYPTOSTREAM_S3_SECRET_KEY", "minioadmin")
