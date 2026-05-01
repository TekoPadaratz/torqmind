#!/usr/bin/env bash
# TorqMind Streaming: Status overview
# Shows container status, Debezium connector state, topics, lag, and recent events.
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-status.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.streaming.yml}"
DEBEZIUM_URL="${DEBEZIUM_URL:-http://localhost:18083}"
CH_CONTAINER="${CH_CONTAINER:-}"

# Source env (don't print secrets)
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"

CH_AUTH_ARGS=(--user "$CLICKHOUSE_USER")
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
    CH_AUTH_ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

echo "=========================================="
echo " TorqMind Streaming Status"
echo "=========================================="
echo ""

# --- Containers ---
echo "--- Containers ---"
cd "$REPO_ROOT"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile local-full ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || \
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || \
    echo "  (unable to list containers)"
echo ""

# --- Debezium Connect ---
echo "--- Debezium Connect ---"
CONNECTORS=$(curl -sS "$DEBEZIUM_URL/connectors" 2>/dev/null || echo "[]")
echo "  Connectors: $CONNECTORS"

if [[ "$CONNECTORS" != "[]" ]] && [[ "$CONNECTORS" != "" ]]; then
    for conn in $(echo "$CONNECTORS" | python3 -c "import sys,json; [print(c) for c in json.load(sys.stdin)]" 2>/dev/null); do
        STATUS=$(curl -sS "$DEBEZIUM_URL/connectors/$conn/status" 2>/dev/null || echo "{}")
        echo "$STATUS" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    state = data.get('connector', {}).get('state', 'UNKNOWN')
    tasks = data.get('tasks', [])
    print(f'  {data.get(\"name\", \"?\")}: {state}')
    for t in tasks:
        tid = t.get('id', '?')
        tstate = t.get('state', '?')
        trace = t.get('trace', '')
        print(f'    Task {tid}: {tstate}')
        if trace:
            print(f'      Error: {trace[:200]}')
except:
    pass
" 2>/dev/null
    done
fi
echo ""

# --- Topics ---
echo "--- Redpanda Topics ---"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T redpanda rpk topic list 2>/dev/null || echo "  (unable to list topics)"
echo ""
echo "--- Canonical STG Topics ---"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T redpanda rpk topic list 2>/dev/null \
  | grep -E 'torqmind\.stg\.(comprovantes|itenscomprovantes|formas_pgto_comprovantes|turnos|produtos|grupoprodutos|funcionarios|usuarios|localvendas|contaspagar|contasreceber|entidades|clientes|filiais)' \
  || echo "  (no STG topics found yet)"
echo ""

# --- Consumer Group Lag ---
echo "--- Consumer Group Lag ---"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T redpanda rpk group describe torqmind-cdc-consumer 2>/dev/null || echo "  (group not found or no messages yet)"
echo ""

# --- ClickHouse Stats ---
echo "--- ClickHouse CDC Stats ---"
# Find CH container
if [[ -z "$CH_CONTAINER" ]]; then
    CH_CONTAINER=$(docker compose -f "$REPO_ROOT/docker-compose.yml" --env-file "$ENV_FILE" ps -q clickhouse 2>/dev/null || true)
    if [[ -z "$CH_CONTAINER" ]]; then
        CH_CONTAINER=$(docker compose -f "$REPO_ROOT/docker-compose.streaming.yml" --env-file "$ENV_FILE" ps -q clickhouse 2>/dev/null || true)
    fi
fi

if [[ -n "$CH_CONTAINER" ]]; then
    echo "  Raw events:"
    docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT count() as total_events FROM torqmind_raw.cdc_events" 2>/dev/null || echo "    (table not ready)"

    echo "  Events by table (top 15):"
    docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT table_schema, table_name, count() as events FROM torqmind_raw.cdc_events GROUP BY table_schema, table_name ORDER BY events DESC LIMIT 15" 2>/dev/null || echo "    (no data)"

    echo ""
    echo "  Current state tables:"
    docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT database, name, total_rows FROM system.tables WHERE database = 'torqmind_current' ORDER BY name" 2>/dev/null || echo "    (not ready)"

    echo ""
    echo "  CDC Table State (ops):"
    docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT table_schema, table_name, id_empresa, events_total, last_event_at FROM torqmind_ops.cdc_table_state FINAL ORDER BY last_event_at DESC LIMIT 15" 2>/dev/null || echo "    (no state data)"

    echo ""
    echo "  Recent Errors:"
    docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT created_at, error_type, table_name, substring(error_message, 1, 100) as msg FROM torqmind_ops.cdc_errors ORDER BY created_at DESC LIMIT 5" 2>/dev/null || echo "    (no errors)"
else
    echo "  ClickHouse container not found"
fi

echo ""
echo "=========================================="
