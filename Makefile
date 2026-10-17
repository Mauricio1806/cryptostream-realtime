.PHONY: help up down restart logs status bootstrap run-producer \
       submit-bronze submit-silver-trades submit-silver-orderbook \
       submit-gold-vwap submit-gold-ohlc submit-gold-imbalance \
       lint format type-check test test-unit test-integration clean

SHELL := /bin/bash
DOCKER_COMPOSE := docker compose
SPARK_SUBMIT := docker exec cs-spark-master spark-submit
SPARK_PACKAGES := io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4

# ────────────────── Spark common flags ──────────────────
SPARK_CONF := \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
	--conf spark.hadoop.fs.s3a.access.key=minioadmin \
	--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
	--conf spark.hadoop.fs.s3a.path.style.access=true \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ────────────────── Docker ──────────────────
up: ## Start all services
	$(DOCKER_COMPOSE) up -d
	@echo "⏳ Waiting for Kafka to be healthy..."
	@timeout 90 bash -c 'until docker exec cs-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do sleep 2; done'
	@echo "✅ All services up"

down: ## Stop all services
	$(DOCKER_COMPOSE) down

restart: down up ## Restart all services

logs: ## Tail logs (use: make logs s=kafka)
	$(DOCKER_COMPOSE) logs -f $(s)

status: ## Show service status
	$(DOCKER_COMPOSE) ps

# ────────────────── Bootstrap ──────────────────
bootstrap: ## Create Kafka topics + MinIO buckets
	@echo "📦 Creating Kafka topics..."
	docker exec cs-kafka kafka-topics --create --bootstrap-server localhost:9092 \
		--topic crypto.trades.v1 --partitions 3 --replication-factor 1 --if-not-exists
	docker exec cs-kafka kafka-topics --create --bootstrap-server localhost:9092 \
		--topic crypto.orderbook.v1 --partitions 3 --replication-factor 1 --if-not-exists
	@echo "🪣 Creating MinIO buckets..."
	docker exec cs-minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
	docker exec cs-minio mc mb --ignore-existing local/cryptostream-dev-bronze
	docker exec cs-minio mc mb --ignore-existing local/cryptostream-dev-silver
	docker exec cs-minio mc mb --ignore-existing local/cryptostream-dev-gold
	docker exec cs-minio mc mb --ignore-existing local/cryptostream-dev-checkpoints
	@echo "✅ Bootstrap complete"

# ────────────────── Producer ──────────────────
run-producer: ## Run the Binance WebSocket producer locally
	uv run cryptostream-producer --symbols BTCUSDT,ETHUSDT,SOLUSDT

# ────────────────── Spark Jobs ──────────────────
submit-bronze: ## Submit bronze ingestion job
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		--packages $(SPARK_PACKAGES) $(SPARK_CONF) \
		/opt/spark-app/src/streaming/jobs/bronze_ingestion.py

submit-silver-trades: ## Submit silver trades job
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		--packages $(SPARK_PACKAGES) $(SPARK_CONF) \
		/opt/spark-app/src/streaming/jobs/silver_trades.py

submit-silver-orderbook: ## Submit silver order book job
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		--packages $(SPARK_PACKAGES) $(SPARK_CONF) \
		/opt/spark-app/src/streaming/jobs/silver_orderbook.py

submit-gold-vwap: ## Submit gold VWAP aggregation job
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		--packages $(SPARK_PACKAGES) $(SPARK_CONF) \
		/opt/spark-app/src/streaming/jobs/gold_vwap.py

submit-gold-ohlc: ## Submit gold OHLC candle job
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		--packages $(SPARK_PACKAGES) $(SPARK_CONF) \
		/opt/spark-app/src/streaming/jobs/gold_ohlc.py

submit-gold-imbalance: ## Submit gold order book imbalance job
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		--packages $(SPARK_PACKAGES) $(SPARK_CONF) \
		/opt/spark-app/src/streaming/jobs/gold_imbalance.py

# ────────────────── Quality ──────────────────
lint: ## Run ruff linter
	uv run ruff check src/ tests/

format: ## Format code with ruff
	uv run ruff format src/ tests/

type-check: ## Run mypy type checker
	uv run mypy src/

test: ## Run all tests
	uv run pytest

test-unit: ## Run unit tests only
	uv run pytest -m unit

test-integration: ## Run integration tests only
	uv run pytest -m integration

# ────────────────── Cleanup ──────────────────
clean: ## Remove caches and temp files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/ *.egg-info/
