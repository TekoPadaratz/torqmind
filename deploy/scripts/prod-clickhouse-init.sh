#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
DW_WAIT_ATTEMPTS="${CLICKHOUSE_DW_WAIT_ATTEMPTS:-120}"
DW_WAIT_SLEEP="${CLICKHOUSE_DW_WAIT_SLEEP:-2}"
REPLICATION_WAIT_ATTEMPTS="${CLICKHOUSE_REPLICATION_WAIT_ATTEMPTS:-300}"
REPLICATION_WAIT_SLEEP="${CLICKHOUSE_REPLICATION_WAIT_SLEEP:-2}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

tm_require_prod_runtime_env "$ENV_FILE"

cd "$ROOT_DIR"

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
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

ch_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\'/\\\'}"
  printf "%s" "$value"
}

metric_pg() {
  local table="$1"
  pg "SELECT count(*)::bigint || '|' || COALESCE(max(data_key), 0)::bigint FROM dw.${table};"
}

metric_ch() {
  local table="$1"
  ch --query "SELECT concat(toString(count()), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.${table}"
}

wait_required_dw_tables() {
  local required_dw_table_sql="'dim_cliente','dim_filial','dim_funcionario','dim_grupo_produto','dim_local_venda','dim_produto','dim_usuario_caixa','fact_caixa_turno','fact_comprovante','fact_financeiro','fact_pagamento_comprovante','fact_risco_evento','fact_venda','fact_venda_item'"

  echo
  echo "== wait torqmind_dw required tables =="
  for ((attempt = 1; attempt <= DW_WAIT_ATTEMPTS; attempt++)); do
    count="$(ch --query "SELECT count() FROM system.tables WHERE database = 'torqmind_dw' AND name IN (${required_dw_table_sql})")"
    if [[ "$count" -ge 14 ]]; then
      echo "torqmind_dw ready with $count required tables"
      return 0
    fi
    if [[ "$attempt" -eq "$DW_WAIT_ATTEMPTS" ]]; then
      echo "Timed out waiting for torqmind_dw required tables; found $count/14" >&2
      ch --query "SHOW TABLES FROM torqmind_dw" || true
      return 1
    fi
    sleep "$DW_WAIT_SLEEP"
  done
}

wait_sales_replication() {
  echo
  echo "== wait torqmind_dw sales facts to match PostgreSQL =="
  local pg_venda pg_item ch_venda ch_item
  for ((attempt = 1; attempt <= REPLICATION_WAIT_ATTEMPTS; attempt++)); do
    pg_venda="$(metric_pg fact_venda)"
    pg_item="$(metric_pg fact_venda_item)"
    ch_venda="$(metric_ch fact_venda)"
    ch_item="$(metric_ch fact_venda_item)"

    if [[ "$pg_venda" == "$ch_venda" && "$pg_item" == "$ch_item" ]]; then
      echo "fact_venda matched PostgreSQL: $ch_venda"
      echo "fact_venda_item matched PostgreSQL: $ch_item"
      return 0
    fi

    if [[ "$attempt" -eq 1 || $((attempt % 10)) -eq 0 ]]; then
      echo "waiting replication attempt $attempt/$REPLICATION_WAIT_ATTEMPTS"
      echo "  pg dw.fact_venda=$pg_venda | ch torqmind_dw.fact_venda=$ch_venda"
      echo "  pg dw.fact_venda_item=$pg_item | ch torqmind_dw.fact_venda_item=$ch_item"
    fi

    if [[ "$attempt" -eq "$REPLICATION_WAIT_ATTEMPTS" ]]; then
      echo "Timed out waiting for torqmind_dw sales facts to match PostgreSQL." >&2
      echo "  pg dw.fact_venda=$pg_venda | ch torqmind_dw.fact_venda=$ch_venda" >&2
      echo "  pg dw.fact_venda_item=$pg_item | ch torqmind_dw.fact_venda_item=$ch_item" >&2
      return 1
    fi
    sleep "$REPLICATION_WAIT_SLEEP"
  done
}

echo "== validate postgres wal_level =="
wal_level="$(pg "SHOW wal_level")"
if [[ "$wal_level" != "logical" ]]; then
  echo "Postgres wal_level precisa ser logical para MaterializedPostgreSQL; atual: $wal_level" >&2
  exit 1
fi
echo "wal_level=$wal_level"

echo
echo "== validate clickhouse ping =="
compose exec -T clickhouse sh -lc 'wget -q -O - http://127.0.0.1:8123/ping | grep -q Ok'
echo "ClickHouse ping OK"

pg_host="$(ch_escape "${CLICKHOUSE_PG_HOST:-postgres}")"
pg_port="$(ch_escape "${CLICKHOUSE_PG_PORT:-5432}")"
pg_db="$(ch_escape "${POSTGRES_DB}")"
pg_user="$(ch_escape "${POSTGRES_USER}")"
pg_password="$(ch_escape "${POSTGRES_PASSWORD}")"

echo
echo "== recreate torqmind_dw MaterializedPostgreSQL =="
ch --multiquery --query "
SET allow_experimental_database_materialized_postgresql=1;
SET allow_experimental_materialized_postgresql_table=1;
DROP DATABASE IF EXISTS torqmind_dw SYNC;
CREATE DATABASE torqmind_dw
ENGINE = MaterializedPostgreSQL('${pg_host}:${pg_port}', '${pg_db}', '${pg_user}', '${pg_password}')
SETTINGS materialized_postgresql_schema = 'dw';
"

wait_required_dw_tables
wait_sales_replication

echo
echo "== recreate torqmind_mart tables =="
ch --query "DROP DATABASE IF EXISTS torqmind_mart SYNC"
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_design.sql"

echo
echo "== run native backfill =="
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
echo "== validate agg_vendas_diaria count =="
ch --query "SELECT count() FROM torqmind_mart.agg_vendas_diaria"

echo
echo "ClickHouse production initialization completed."
