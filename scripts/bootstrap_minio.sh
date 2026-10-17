#!/usr/bin/env bash
# Create MinIO buckets for local development (S3-compatible).
# Requires: mc (MinIO Client) — installed via `brew install minio/stable/mc` or Docker.
# Usage: ./scripts/bootstrap_minio.sh

set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
ALIAS="local"

BUCKETS=(
    "cryptostream-dev-bronze"
    "cryptostream-dev-silver"
    "cryptostream-dev-gold"
    "cryptostream-dev-checkpoints"
)

echo "==> Configuring MinIO alias '${ALIAS}'..."

# Try mc from Docker if not installed locally
if command -v mc &>/dev/null; then
    MC="mc"
else
    MC="docker run --rm --network host minio/mc:latest"
fi

${MC} alias set "${ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4

echo "==> Creating buckets..."
for bucket in "${BUCKETS[@]}"; do
    echo "    Creating: ${bucket}"
    ${MC} mb "${ALIAS}/${bucket}" --ignore-existing
done

echo "==> Listing buckets:"
${MC} ls "${ALIAS}/"

echo "==> Done."
