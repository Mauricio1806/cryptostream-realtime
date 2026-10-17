#!/usr/bin/env bash
# Create Kafka topics for local development.
# Usage: ./scripts/seed_local_topics.sh

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cs-kafka}"
BOOTSTRAP="${BOOTSTRAP_SERVER:-localhost:29092}"

TOPICS=(
    "crypto.trades.v1:3:1"
    "crypto.orderbook.v1:3:1"
)

echo "==> Creating Kafka topics on ${BOOTSTRAP}..."

for topic_config in "${TOPICS[@]}"; do
    IFS=':' read -r topic partitions replication <<< "${topic_config}"

    echo "    Creating topic: ${topic} (partitions=${partitions}, replication=${replication})"
    docker exec "${KAFKA_CONTAINER}" kafka-topics \
        --create \
        --if-not-exists \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${replication}" \
        2>/dev/null || true
done

echo "==> Listing topics:"
docker exec "${KAFKA_CONTAINER}" kafka-topics \
    --list \
    --bootstrap-server "${BOOTSTRAP}"

echo "==> Done."
