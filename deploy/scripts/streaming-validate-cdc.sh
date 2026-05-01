#!/usr/bin/env bash
# TorqMind Streaming: Validate CDC pipeline
# Checks that events are flowing from PostgreSQL through Redpanda to ClickHouse.
# Usage: ENV_FILE=/etc/torqmind/prod.env ID_EMPRESA=1 ./deploy/scripts/streaming-validate-cdc.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
ID_EMPRESA="${ID_EMPRESA:-1}"
SOURCE="${SOURCE:-${REALTIME_MARTS_SOURCE:-stg}}"
CH_CONTAINER="${CH_CONTAINER:-}"

# Source env
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

: "${POSTGRES_USER:=${PG_USER:-postgres}}"
: "${POSTGRES_DB:=${PG_DATABASE:-torqmind}}"
: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"

CH_AUTH_ARGS=(--user "$CLICKHOUSE_USER")
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
    CH_AUTH_ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

echo "=== TorqMind CDC Validation ==="
echo "  ID_EMPRESA=$ID_EMPRESA"
echo "  SOURCE=$SOURCE"
echo ""

errors=0
warnings=0

# Find ClickHouse container
if [[ -z "$CH_CONTAINER" ]]; then
    CH_CONTAINER=$(docker compose -f "$REPO_ROOT/$COMPOSE_FILE" --env-file "$ENV_FILE" ps -q clickhouse 2>/dev/null || true)
fi

if [[ -z "$CH_CONTAINER" ]]; then
    echo "ERROR: ClickHouse container not found"
    exit 1
fi

# --- Check raw events exist ---
echo "--- Raw Events ---"
RAW_COUNT=$(docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT count() FROM torqmind_raw.cdc_events WHERE id_empresa = $ID_EMPRESA" 2>/dev/null || echo "0")
echo "  Raw events for empresa $ID_EMPRESA: $RAW_COUNT"
if [[ "$RAW_COUNT" == "0" ]]; then
    echo "  WARNING: No raw events found. CDC may not be running or snapshot not started."
    ((warnings++))
fi

# --- Check current state ---
echo ""
echo "--- Current State ---"
if [[ "$SOURCE" == "stg" ]]; then
    TABLES=("stg_comprovantes" "stg_itenscomprovantes" "stg_formas_pgto_comprovantes" "stg_turnos" "stg_entidades" "stg_produtos" "stg_grupoprodutos" "stg_funcionarios" "stg_usuarios" "stg_localvendas" "stg_contaspagar" "stg_contasreceber")
else
    TABLES=("fact_venda" "fact_venda_item" "fact_pagamento_comprovante" "fact_caixa_turno" "fact_comprovante" "fact_financeiro" "fact_risco_evento" "dim_filial" "dim_produto" "dim_grupo_produto" "dim_funcionario" "dim_usuario_caixa" "dim_local_venda" "dim_cliente")
fi

for table in "${TABLES[@]}"; do
    count=$(docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT count() FROM torqmind_current.$table FINAL WHERE id_empresa = $ID_EMPRESA AND is_deleted = 0" 2>/dev/null || echo "-1")
    if [[ "$count" == "-1" ]]; then
        echo "  $table: TABLE NOT FOUND"
        ((errors++))
    elif [[ "$count" == "0" ]]; then
        echo "  $table: 0 (empty)"
        ((warnings++))
    else
        echo "  $table: $count"
    fi
done

# --- Compare with PostgreSQL source ---
echo ""
echo "--- PostgreSQL Source Comparison ---"
PG_CONTAINER=$(docker compose -f "$REPO_ROOT/$COMPOSE_FILE" --env-file "$ENV_FILE" ps -q postgres 2>/dev/null || true)

if [[ -n "$PG_CONTAINER" ]]; then
    if [[ "$SOURCE" == "stg" ]]; then
        PG_COUNT=$(docker exec "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM stg.comprovantes WHERE id_empresa = $ID_EMPRESA" 2>/dev/null || echo "?")
        CH_COUNT=$(docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT count() FROM torqmind_current.stg_comprovantes FINAL WHERE id_empresa = $ID_EMPRESA AND is_deleted = 0" 2>/dev/null || echo "?")
        echo "  PostgreSQL stg.comprovantes: $PG_COUNT"
        echo "  ClickHouse current.stg_comprovantes: $CH_COUNT"
    else
        PG_COUNT=$(docker exec "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM dw.fact_venda WHERE id_empresa = $ID_EMPRESA" 2>/dev/null || echo "?")
        CH_COUNT=$(docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT count() FROM torqmind_current.fact_venda FINAL WHERE id_empresa = $ID_EMPRESA AND is_deleted = 0" 2>/dev/null || echo "?")
        echo "  PostgreSQL dw.fact_venda: $PG_COUNT"
        echo "  ClickHouse current.fact_venda: $CH_COUNT"
    fi
    if [[ "$PG_COUNT" != "?" ]] && [[ "$CH_COUNT" != "?" ]] && [[ "$PG_COUNT" != "$CH_COUNT" ]]; then
        echo "  DIVERGENCE: counts differ (expected during initial sync)"
        ((warnings++))
    elif [[ "$PG_COUNT" == "$CH_COUNT" ]]; then
        echo "  MATCH: counts equal"
    fi
else
    echo "  PostgreSQL container not found, skipping comparison"
fi

# --- Ops state ---
echo ""
echo "--- CDC Operations State ---"
docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "
    SELECT table_schema, table_name, events_total, last_event_at
    FROM torqmind_ops.cdc_table_state FINAL
    WHERE id_empresa = $ID_EMPRESA
    ORDER BY last_event_at DESC
    LIMIT 15
" 2>/dev/null || echo "  (no ops data)"

echo ""
echo "--- Errors ---"
ERROR_COUNT=$(docker exec "$CH_CONTAINER" clickhouse-client "${CH_AUTH_ARGS[@]}" --query "SELECT count() FROM torqmind_ops.cdc_errors" 2>/dev/null || echo "0")
echo "  Total errors logged: $ERROR_COUNT"

echo ""
echo "=========================================="
echo "  RESULT: errors=$errors warnings=$warnings"
if [[ $errors -gt 0 ]]; then
    echo "  STATUS: FAIL"
    exit 1
fi
if [[ $warnings -gt 0 ]]; then
    echo "  STATUS: WARN (CDC may still be syncing)"
fi
echo "=========================================="
