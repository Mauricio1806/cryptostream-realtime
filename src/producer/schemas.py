"""Pydantic models for Binance WebSocket messages."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AggTrade(BaseModel):
    """Binance Aggregate Trade Stream event.

    Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#aggregate-trade-streams
    """

    event_type: str = Field(alias="e")
    event_time: int = Field(alias="E")
    symbol: str = Field(alias="s")
    agg_trade_id: int = Field(alias="a")
    price: str = Field(alias="p")
    quantity: str = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time: int = Field(alias="T")
    is_buyer_maker: bool = Field(alias="m")

    @property
    def event_id(self) -> str:
        """Unique dedup key: symbol + aggregate trade id."""
        return f"{self.symbol}_{self.agg_trade_id}"


class DepthLevel(BaseModel):
    """A single price level in the order book."""

    price: str
    quantity: str


class DepthUpdate(BaseModel):
    """Binance Partial Book Depth Stream event.

    Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#partial-book-depth-streams
    """

    last_update_id: int = Field(alias="lastUpdateId")
    bids: list[list[str]]
    asks: list[list[str]]

    # These fields are injected by our producer, not from Binance directly
    symbol: str = ""
    received_at: int = 0

    @property
    def event_id(self) -> str:
        """Unique dedup key: symbol + last update id."""
        return f"{self.symbol}_{self.last_update_id}"

    @property
    def bid_levels(self) -> list[DepthLevel]:
        """Parse raw bid arrays into DepthLevel objects."""
        return [DepthLevel(price=b[0], quantity=b[1]) for b in self.bids]

    @property
    def ask_levels(self) -> list[DepthLevel]:
        """Parse raw ask arrays into DepthLevel objects."""
        return [DepthLevel(price=a[0], quantity=a[1]) for a in self.asks]
