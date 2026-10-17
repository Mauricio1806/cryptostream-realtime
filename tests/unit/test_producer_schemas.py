"""Unit tests for producer Pydantic schemas."""

from __future__ import annotations

from typing import Any

import pytest

from src.producer.schemas import AggTrade, DepthUpdate


class TestAggTrade:
    """Tests for AggTrade model."""

    def test_parse_valid_message(self, raw_agg_trade: dict[str, Any]) -> None:
        trade = AggTrade.model_validate(raw_agg_trade)

        assert trade.event_type == "aggTrade"
        assert trade.symbol == "BTCUSDT"
        assert trade.agg_trade_id == 123456789
        assert trade.price == "27150.50000000"
        assert trade.quantity == "0.01500000"
        assert trade.is_buyer_maker is True

    def test_event_id_property(self, raw_agg_trade: dict[str, Any]) -> None:
        trade = AggTrade.model_validate(raw_agg_trade)
        assert trade.event_id == "BTCUSDT_123456789"

    def test_event_time_is_epoch_ms(self, raw_agg_trade: dict[str, Any]) -> None:
        trade = AggTrade.model_validate(raw_agg_trade)
        assert trade.event_time == 1696012345678
        assert trade.event_time > 1_600_000_000_000  # After 2020

    def test_serialization_roundtrip(self, raw_agg_trade: dict[str, Any]) -> None:
        trade = AggTrade.model_validate(raw_agg_trade)
        data = trade.model_dump(by_alias=True)

        # Should be serializable back
        trade2 = AggTrade.model_validate(data)
        assert trade2.event_id == trade.event_id

    def test_missing_required_field_raises(self, raw_agg_trade: dict[str, Any]) -> None:
        del raw_agg_trade["s"]  # Remove symbol
        with pytest.raises(Exception):
            AggTrade.model_validate(raw_agg_trade)


class TestDepthUpdate:
    """Tests for DepthUpdate model."""

    def test_parse_valid_message(self, raw_depth_update: dict[str, Any]) -> None:
        depth = DepthUpdate.model_validate(raw_depth_update)

        assert depth.symbol == "BTCUSDT"
        assert depth.last_update_id == 987654321

    def test_bid_ask_levels(self, raw_depth_update: dict[str, Any]) -> None:
        depth = DepthUpdate.model_validate(raw_depth_update)

        bids = depth.bid_levels
        asks = depth.ask_levels

        assert len(bids) == 5
        assert len(asks) == 5
        assert bids[0].price == "27150.00000000"
        assert asks[0].price == "27151.00000000"

    def test_received_at_injected(self, raw_depth_update: dict[str, Any]) -> None:
        raw_depth_update["_received_at"] = 1696012345999
        depth = DepthUpdate.model_validate(raw_depth_update)

        assert depth.received_at == 1696012345999
