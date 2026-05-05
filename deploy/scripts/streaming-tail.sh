#!/usr/bin/env bash
# TorqMind Streaming: Tail CDC consumer and Debezium logs
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-tail.sh [service]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.streaming.yml}"
SERVICE="${1:-}"

cd "$REPO_ROOT"

if [[ -n "$SERVICE" ]]; then
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" logs -f --tail=100 "$SERVICE"
else
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" logs -f --tail=50 cdc-consumer debezium-connect
fi
