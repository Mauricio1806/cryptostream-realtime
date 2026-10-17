"""CLI entry point for the CryptoStream producer."""

from __future__ import annotations

import asyncio
import signal

import typer

from src.producer.binance_ws_client import BinanceWebSocketClient
from src.producer.config import get_settings
from src.producer.kafka_producer import CryptoKafkaProducer
from src.utils.logging import setup_logging

app = typer.Typer(
    name="cryptostream-producer",
    help="Stream Binance market data into Kafka in real time.",
)


@app.command()
def run(
    symbols: str = typer.Option(
        None,
        "--symbols",
        "-s",
        help="Comma-separated trading pairs (e.g. BTCUSDT,ETHUSDT). Overrides env var.",
    ),
    streams: str = typer.Option(
        None,
        "--streams",
        help="Comma-separated stream types (e.g. aggTrade,depth@100ms). Overrides env var.",
    ),
    log_level: str = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Log level (DEBUG, INFO, WARNING, ERROR).",
    ),
) -> None:
    """Start the Binance WebSocket → Kafka producer."""
    settings = get_settings()

    # CLI overrides
    if symbols:
        settings.symbols = symbols
    if streams:
        settings.streams = streams
    if log_level:
        settings.log_level = log_level

    setup_logging(level=settings.log_level)

    kafka_producer = CryptoKafkaProducer(settings)
    ws_client = BinanceWebSocketClient(settings, kafka_producer)

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.new_event_loop()

    def _shutdown_handler() -> None:
        ws_client.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown_handler)

    try:
        loop.run_until_complete(ws_client.run())
    finally:
        loop.close()


if __name__ == "__main__":
    app()
