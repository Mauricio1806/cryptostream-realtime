"""Configuration loaded from environment variables via pydantic-settings."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class ProducerSettings(BaseSettings):
    """Producer configuration — reads from .env or environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="CRYPTOSTREAM_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Binance
    binance_ws_url: str = "wss://stream.binance.com:9443/ws"
    symbols: str = "BTCUSDT,ETHUSDT,SOLUSDT"
    streams: str = "aggTrade,depth@100ms"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_acks: str = "all"
    kafka_trades_topic: str = "crypto.trades.v1"
    kafka_orderbook_topic: str = "crypto.orderbook.v1"

    # General
    env: str = "dev"
    log_level: str = "INFO"

    @property
    def symbol_list(self) -> list[str]:
        """Parse comma-separated symbols into a list."""
        return [s.strip().lower() for s in self.symbols.split(",") if s.strip()]

    @property
    def stream_list(self) -> list[str]:
        """Parse comma-separated streams into a list."""
        return [s.strip() for s in self.streams.split(",") if s.strip()]

    def build_ws_url(self) -> str:
        """Build the combined WebSocket URL for all symbol/stream pairs.

        Example: wss://stream.binance.com:9443/ws/btcusdt@aggTrade/ethusdt@aggTrade/...
        """
        paths: list[str] = []
        for symbol in self.symbol_list:
            for stream in self.stream_list:
                if "@" in stream:
                    # e.g. depth@100ms → btcusdt@depth@100ms
                    stream_name = stream.split("@")[0]
                    stream_speed = stream.split("@")[1]
                    paths.append(f"{symbol}@{stream_name}@{stream_speed}")
                else:
                    paths.append(f"{symbol}@{stream}")
        return f"{self.binance_ws_url}/{'/'.join(paths)}"


def get_settings() -> ProducerSettings:
    """Factory for cached settings instance."""
    return ProducerSettings()
