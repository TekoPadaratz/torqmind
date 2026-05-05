#!/usr/bin/env bash
# TorqMind Streaming: Start streaming stack
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-up.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.streaming.yml}"
PROFILE="${STREAMING_PROFILE:-local-full}"

if [[ ! -f "$REPO_ROOT/$COMPOSE_FILE" ]] && [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "ERROR: Compose file not found: $COMPOSE_FILE"
    exit 1
fi

echo "=== TorqMind Streaming Stack ==="
echo "ENV_FILE=$ENV_FILE"
echo "COMPOSE_FILE=$COMPOSE_FILE"
echo "PROFILE=$PROFILE"
echo ""

cd "$REPO_ROOT"

# Build and start
if [[ "$PROFILE" == "prod-lite" ]]; then
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build
else
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile "$PROFILE" up -d --build
fi

echo ""
echo "Waiting for services to be healthy..."

# Wait for Redpanda
for i in $(seq 1 60); do
    if docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T redpanda rpk cluster health 2>/dev/null | grep -q "HEALTHY"; then
        echo "  Redpanda: HEALTHY"
        break
    fi
    if [[ $i -eq 60 ]]; then
        echo "  Redpanda: TIMEOUT (check logs)"
        exit 1
    fi
    sleep 2
done

# Wait for Debezium Connect
for i in $(seq 1 90); do
    if docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T debezium-connect curl -fsS http://localhost:8083/connectors >/dev/null 2>&1; then
        echo "  Debezium Connect: READY"
        break
    fi
    if [[ $i -eq 90 ]]; then
        echo "  Debezium Connect: TIMEOUT (check logs)"
        exit 1
    fi
    sleep 2
done

echo ""
echo "Streaming stack is up. Next steps:"
echo "  1. Initialize ClickHouse schemas: ./deploy/scripts/streaming-init-clickhouse.sh"
echo "  2. Register Debezium connector:   ./deploy/scripts/streaming-register-debezium.sh"
echo "  3. Validate CDC:                  ./deploy/scripts/streaming-validate-cdc.sh"
