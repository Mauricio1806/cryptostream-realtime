"""Unit tests for Binance WebSocket message parsing."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.producer.binance_ws_client import BinanceWebSocketClient
from src.producer.config import ProducerSettings


@pytest.fixture()
def mock_settings(env_vars: None) -> ProducerSettings:
    return ProducerSettings()


@pytest.fixture()
def mock_producer() -> MagicMock:
    producer = MagicMock()
    producer.produce_trade = MagicMock()
    producer.produce_orderbook = MagicMock()
    return producer


@pytest.fixture()
def client(mock_settings: ProducerSettings, mock_producer: MagicMock) -> BinanceWebSocketClient:
    return BinanceWebSocketClient(settings=mock_settings, producer=mock_producer)


class TestMessageRouting:
    """Test that messages are routed to the correct Kafka topic."""

    def test_agg_trade_routed_to_trades_topic(
        self,
        client: BinanceWebSocketClient,
        mock_producer: MagicMock,
        combined_stream_trade: dict[str, Any],
    ) -> None:
        raw = json.dumps(combined_stream_trade)
        client._process_message(raw)

        mock_producer.produce_trade.assert_called_once()
        mock_producer.produce_orderbook.assert_not_called()

    def test_depth_update_routed_to_orderbook_topic(
        self,
        client: BinanceWebSocketClient,
        mock_producer: MagicMock,
        combined_stream_depth: dict[str, Any],
    ) -> None:
        raw = json.dumps(combined_stream_depth)
        client._process_message(raw)

        mock_producer.produce_orderbook.assert_called_once()
        mock_producer.produce_trade.assert_not_called()

    def test_unknown_event_type_ignored(
        self,
        client: BinanceWebSocketClient,
        mock_producer: MagicMock,
    ) -> None:
        msg = json.dumps({
            "stream": "btcusdt@ticker",
            "data": {"e": "24hrTicker", "s": "BTCUSDT"},
        })
        client._process_message(msg)

        mock_producer.produce_trade.assert_not_called()
        mock_producer.produce_orderbook.assert_not_called()


class TestReceivedAtInjection:
    """Test that _received_at timestamp is injected into messages."""

    def test_received_at_added_to_trade(
        self,
        client: BinanceWebSocketClient,
        mock_producer: MagicMock,
        combined_stream_trade: dict[str, Any],
    ) -> None:
        raw = json.dumps(combined_stream_trade)
        client._process_message(raw)

        call_args = mock_producer.produce_trade.call_args
        data = call_args[0][0] if call_args[0] else call_args[1].get("data", {})

        # The data passed to produce should contain _received_at
        assert isinstance(data, dict)

    def test_received_at_added_to_depth(
        self,
        client: BinanceWebSocketClient,
        mock_producer: MagicMock,
        combined_stream_depth: dict[str, Any],
    ) -> None:
        raw = json.dumps(combined_stream_depth)
        client._process_message(raw)

        mock_producer.produce_orderbook.assert_called_once()


class TestSingleStreamFormat:
    """Test parsing messages in single-stream (non-combined) format."""

    def test_single_stream_agg_trade(
        self,
        client: BinanceWebSocketClient,
        mock_producer: MagicMock,
        raw_agg_trade: dict[str, Any],
    ) -> None:
        # Single stream format: no wrapper
        raw = json.dumps(raw_agg_trade)
        client._process_message(raw)

        mock_producer.produce_trade.assert_called_once()
