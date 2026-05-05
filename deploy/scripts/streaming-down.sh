#!/usr/bin/env bash
# TorqMind Streaming: Stop streaming stack
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-down.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.streaming.yml}"

cd "$REPO_ROOT"

echo "=== Stopping TorqMind Streaming Stack ==="
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile local-full down

echo "Streaming stack stopped. Volumes preserved."
echo "To remove volumes: docker compose -f $COMPOSE_FILE --env-file $ENV_FILE down -v"
