FROM python:3.12-slim AS base

LABEL maintainer="mauricio.esquivel1806@gmail.com"
LABEL description="CryptoStream Realtime - Binance WebSocket Producer"

# System deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.4 /uv /usr/local/bin/uv

# Install dependencies (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen

# Copy source
COPY src/ src/

# Non-root user
RUN groupadd -r cryptostream && useradd -r -g cryptostream cryptostream
USER cryptostream

# Health check: verify the process is running
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD pgrep -f "cryptostream-producer" || exit 1

ENTRYPOINT ["uv", "run", "cryptostream-producer", "run"]
