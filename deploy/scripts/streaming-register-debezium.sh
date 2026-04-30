#!/usr/bin/env bash
# TorqMind Streaming: Register Debezium PostgreSQL CDC connector
# Reads connector config template, substitutes env vars, and registers via Connect REST API.
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-register-debezium.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
DEBEZIUM_URL="${DEBEZIUM_URL:-http://localhost:18083}"
CONNECTOR_TEMPLATE="$REPO_ROOT/deploy/debezium/connectors/torqmind-postgres-cdc.json"

# Source env (don't print)
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# Required env vars
: "${POSTGRES_HOST:=postgres}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_USER:=${PG_USER:-postgres}}"
: "${POSTGRES_PASSWORD:=${PG_PASSWORD:-postgres}}"
: "${POSTGRES_DB:=${PG_DATABASE:-torqmind}}"

echo "=== TorqMind Streaming: Register Debezium Connector ==="
echo "  Debezium Connect URL: $DEBEZIUM_URL"
echo "  PostgreSQL host: $POSTGRES_HOST:$POSTGRES_PORT"
echo "  Database: $POSTGRES_DB"
echo ""

# Wait for Debezium Connect to be ready
echo "Waiting for Debezium Connect..."
for i in $(seq 1 60); do
    if curl -fsS "$DEBEZIUM_URL/connectors" >/dev/null 2>&1; then
        echo "  Connect API: READY"
        break
    fi
    if [[ $i -eq 60 ]]; then
        echo "  ERROR: Debezium Connect not responding at $DEBEZIUM_URL"
        exit 1
    fi
    sleep 2
done

# Build connector config with env substitution
CONNECTOR_CONFIG=$(cat "$CONNECTOR_TEMPLATE" | \
    sed "s|\${POSTGRES_HOST:-postgres}|$POSTGRES_HOST|g" | \
    sed "s|\${POSTGRES_PORT:-5432}|$POSTGRES_PORT|g" | \
    sed "s|\${POSTGRES_USER}|$POSTGRES_USER|g" | \
    sed "s|\${POSTGRES_PASSWORD}|$POSTGRES_PASSWORD|g" | \
    sed "s|\${POSTGRES_DB:-torqmind}|$POSTGRES_DB|g")

CONNECTOR_NAME=$(echo "$CONNECTOR_CONFIG" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")

echo "  Connector name: $CONNECTOR_NAME"

# Check if connector already exists
EXISTING_STATUS=$(curl -sS -o /dev/null -w "%{http_code}" "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME" 2>/dev/null || echo "000")

if [[ "$EXISTING_STATUS" == "200" ]]; then
    echo "  Connector exists. Updating configuration..."
    CONFIG_ONLY=$(echo "$CONNECTOR_CONFIG" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['config']))")
    RESPONSE=$(curl -sS -X PUT \
        -H "Content-Type: application/json" \
        -d "$CONFIG_ONLY" \
        "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/config")
else
    echo "  Registering new connector..."
    RESPONSE=$(curl -sS -X POST \
        -H "Content-Type: application/json" \
        -d "$CONNECTOR_CONFIG" \
        "$DEBEZIUM_URL/connectors")
fi

echo ""
echo "Response:"
echo "$RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    # Remove password from output
    if 'config' in data:
        data['config'].pop('database.password', None)
    print(json.dumps(data, indent=2))
except:
    print(sys.stdin.read())
" 2>/dev/null || echo "$RESPONSE"

echo ""

# Check connector status
sleep 3
echo "Connector status:"
STATUS=$(curl -sS "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null || echo "{}")
echo "$STATUS" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    state = data.get('connector', {}).get('state', 'UNKNOWN')
    tasks = data.get('tasks', [])
    print(f'  Connector state: {state}')
    for t in tasks:
        print(f'  Task {t.get(\"id\", \"?\")}: {t.get(\"state\", \"UNKNOWN\")}')
except:
    print('  Unable to parse status')
" 2>/dev/null || echo "  Unable to fetch status"

echo ""
echo "Done. Use streaming-status.sh to monitor."
