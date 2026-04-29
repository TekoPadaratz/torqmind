#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

if [[ "$ALLOW_INSECURE_ENV" != "1" ]]; then
  tm_require_prod_runtime_env "$ENV_FILE"
fi

cd "$ROOT_DIR"

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

clickhouse_client_args=(clickhouse-client)
if [[ -n "${CLICKHOUSE_USER:-}" ]]; then
  clickhouse_client_args+=(--user "$CLICKHOUSE_USER")
fi
if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
  clickhouse_client_args+=(--password "$CLICKHOUSE_PASSWORD")
fi

ch() {
  compose exec -T clickhouse "${clickhouse_client_args[@]}" "$@"
}

validate_critical_sales_dw() {
  local venda item
  venda="$(ch --query "SELECT concat(toString(count()), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.fact_venda")"
  item="$(ch --query "SELECT concat(toString(count()), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.fact_venda_item")"
  if [[ "$venda" == "0|0" || "$item" == "0|0" ]]; then
    echo "ERROR: native torqmind_dw sales facts are empty after sync: fact_venda=${venda} fact_venda_item=${item}" >&2
    return 1
  fi
  echo "fact_venda count|max_data_key=${venda}"
  echo "fact_venda_item count|max_data_key=${item}"
}

validate_marts() {
  local mart_count mart_max item_max
  mart_count="$(ch --query "SELECT count() FROM torqmind_mart.agg_vendas_diaria")"
  mart_max="$(ch --query "SELECT coalesce(max(data_key), 0) FROM torqmind_mart.agg_vendas_diaria")"
  item_max="$(ch --query "SELECT coalesce(max(data_key), 0) FROM torqmind_dw.fact_venda_item")"

  if [[ "$mart_count" -le 0 ]]; then
    echo "ERROR: torqmind_mart.agg_vendas_diaria is empty after backfill." >&2
    return 1
  fi
  if [[ "$mart_max" -lt "$item_max" ]]; then
    echo "ERROR: torqmind_mart.agg_vendas_diaria max(data_key)=${mart_max} is behind torqmind_dw.fact_venda_item max(data_key)=${item_max}" >&2
    return 1
  fi

  echo "agg_vendas_diaria rows=${mart_count} max_data_key=${mart_max}"
}

echo "== validate PostgreSQL and ClickHouse =="
compose exec -T postgres sh -lc 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null'
compose exec -T clickhouse sh -lc 'wget -q -O - http://127.0.0.1:8123/ping | grep -q Ok'
echo "Services OK"

echo
echo "== sync native torqmind_dw from PostgreSQL dw =="
ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" ALLOW_INSECURE_ENV="$ALLOW_INSECURE_ENV" MODE=full \
  "$ROOT_DIR/deploy/scripts/prod-clickhouse-sync-dw.sh"

echo
echo "== validate native torqmind_dw critical sales facts =="
validate_critical_sales_dw

echo
echo "== recreate torqmind_mart tables =="
ch --query "DROP DATABASE IF EXISTS torqmind_mart SYNC"
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_design.sql"

echo
echo "== run native mart backfill =="
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase3_native_backfill.sql"

echo
echo "== create streaming materialized views =="
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_streaming_triggers.sql"

echo
echo "== validate tables =="
echo "torqmind_dw:"
ch --query "SHOW TABLES FROM torqmind_dw"
echo
echo "torqmind_mart:"
ch --query "SHOW TABLES FROM torqmind_mart"

echo
echo "== validate principal marts =="
validate_marts
ch --query "INSERT INTO torqmind_ops.sync_state SELECT 'mart_publication', 'full', 'ok', now64(6), null, toInt32(coalesce(max(data_key), 0)), toUInt64(count()), 'full_mart_refresh_completed', now64(6) FROM torqmind_mart.agg_vendas_diaria"

echo
echo "ClickHouse production initialization completed with native torqmind_dw."
