#!/usr/bin/env bash
# TorqMind Streaming: Initialize ClickHouse streaming schemas
# Requires ClickHouse to be running (in main compose or streaming compose).
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-init-clickhouse.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
CH_CONTAINER="${CH_CONTAINER:-}"

# Source env for CH connection info (don't print)
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# Determine ClickHouse container
if [[ -z "$CH_CONTAINER" ]]; then
    # Try main compose first
    CH_CONTAINER=$(docker compose -f "$REPO_ROOT/$COMPOSE_FILE" ps -q clickhouse 2>/dev/null || true)
    if [[ -z "$CH_CONTAINER" ]]; then
        # Try streaming compose
        CH_CONTAINER=$(docker compose -f "$REPO_ROOT/docker-compose.streaming.yml" ps -q clickhouse 2>/dev/null || true)
    fi
    if [[ -z "$CH_CONTAINER" ]]; then
        echo "ERROR: No ClickHouse container found. Is it running?"
        exit 1
    fi
fi

echo "=== TorqMind Streaming: ClickHouse Schema Init ==="

SQL_DIR="$REPO_ROOT/sql/clickhouse/streaming"

for sql_file in "$SQL_DIR"/0*.sql; do
    if [[ -f "$sql_file" ]]; then
        filename=$(basename "$sql_file")
        echo "  Applying: $filename"
        docker exec -i "$CH_CONTAINER" clickhouse-client --multiquery < "$sql_file"
    fi
done

echo ""
echo "Verifying databases..."
for db in torqmind_raw torqmind_current torqmind_ops; do
    count=$(docker exec "$CH_CONTAINER" clickhouse-client --query "SELECT count() FROM system.tables WHERE database = '$db'" 2>/dev/null || echo "0")
    echo "  $db: $count tables"
done

echo ""
echo "ClickHouse streaming schemas initialized successfully."
