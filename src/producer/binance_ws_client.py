"""Async WebSocket client for Binance market data streams."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import orjson
import structlog
import websockets
from websockets.exceptions import ConnectionClosed

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from src.producer.config import ProducerSettings
    from src.producer.kafka_producer import CryptoKafkaProducer

logger = structlog.get_logger()

# Exponential backoff constants
_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 60.0
_BACKOFF_FACTOR = 2.0


class BinanceWebSocketClient:
    """Connects to Binance combined streams and forwards messages to Kafka.

    Features:
        - Automatic reconnection with exponential backoff
        - Graceful shutdown via asyncio cancellation
        - Message routing: aggTrade → trades topic, depthUpdate → orderbook topic
    """

    def __init__(
        self,
        settings: ProducerSettings,
        kafka_producer: CryptoKafkaProducer,
    ) -> None:
        self._settings = settings
        self._kafka = kafka_producer
        self._ws_url = settings.build_ws_url()
        self._running = True
        self._backoff = _INITIAL_BACKOFF

    async def run(self) -> None:
        """Main loop — connect, consume, reconnect on failure."""
        logger.info("binance_ws_starting", url=self._ws_url)

        while self._running:
            try:
                await self._consume_stream()
                # If we exit cleanly, reset backoff
                self._backoff = _INITIAL_BACKOFF
            except ConnectionClosed as exc:
                logger.warning(
                    "binance_ws_disconnected",
                    code=exc.code,
                    reason=str(exc.reason),
                )
                await self._wait_backoff()
            except (OSError, asyncio.TimeoutError) as exc:
                logger.error("binance_ws_connection_error", error=str(exc))
                await self._wait_backoff()
            except asyncio.CancelledError:
                logger.info("binance_ws_cancelled")
                self._running = False
                break

        self._kafka.close()
        logger.info("binance_ws_stopped")

    async def _consume_stream(self) -> None:
        """Open a single WebSocket connection and process messages."""
        async for message in self._connect():
            self._process_message(message)

    async def _connect(self) -> AsyncIterator[str]:
        """Yield messages from the Binance WebSocket."""
        async with websockets.connect(
            self._ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=2**20,  # 1 MiB
        ) as ws:
            logger.info("binance_ws_connected")
            self._backoff = _INITIAL_BACKOFF
            while self._running:
                message = await ws.recv()
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                yield message

    def _process_message(self, raw: str) -> None:
        """Route a raw JSON message to the appropriate Kafka topic."""
        try:
            data: dict[str, object] = orjson.loads(raw)
        except orjson.JSONDecodeError:
            logger.warning("binance_ws_invalid_json", raw=raw[:200])
            return

        # Combined stream wraps the payload in {"stream": "...", "data": {...}}
        if "stream" in data:
            stream_name = str(data.get("stream", ""))
            payload = data.get("data", data)
            if not isinstance(payload, dict):
                return
        else:
            # Single stream mode — infer from event type
            stream_name = ""
            payload = data

        event_type = str(payload.get("e", ""))
        symbol = str(payload.get("s", "")).upper()

        # Inject receive timestamp
        payload["_received_at"] = int(time.time() * 1000)

        if event_type == "aggTrade" or "aggTrade" in stream_name:
            self._kafka.produce_trade(symbol=symbol, payload=payload)
        elif event_type == "depthUpdate" or "depth" in stream_name:
            # Depth updates don't always include "s"; extract from stream name
            if not symbol and "@" in stream_name:
                symbol = stream_name.split("@")[0].upper()
            payload["s"] = symbol
            self._kafka.produce_orderbook(symbol=symbol, payload=payload)
        else:
            logger.debug("binance_ws_unknown_event", event_type=event_type)

    async def _wait_backoff(self) -> None:
        """Sleep with exponential backoff before reconnecting."""
        logger.info("binance_ws_reconnecting_in", seconds=self._backoff)
        await asyncio.sleep(self._backoff)
        self._backoff = min(self._backoff * _BACKOFF_FACTOR, _MAX_BACKOFF)

    def stop(self) -> None:
        """Signal the client to shut down gracefully."""
        self._running = False
