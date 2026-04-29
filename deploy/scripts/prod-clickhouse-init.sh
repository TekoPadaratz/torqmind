#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

tm_require_prod_runtime_env "$ENV_FILE"

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

ch_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\'/\\\'}"
  printf "%s" "$value"
}

required_dw_table_sql="'dim_cliente','dim_filial','dim_funcionario','dim_grupo_produto','dim_local_venda','dim_produto','dim_usuario_caixa','fact_caixa_turno','fact_comprovante','fact_financeiro','fact_pagamento_comprovante','fact_risco_evento','fact_venda','fact_venda_item'"

echo "== validate postgres wal_level =="
wal_level="$(compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SHOW wal_level"')"
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
echo "== create torqmind_dw MaterializedPostgreSQL =="
ch --multiquery --query "
SET allow_experimental_database_materialized_postgresql=1;
SET allow_experimental_materialized_postgresql_table=1;
CREATE DATABASE IF NOT EXISTS torqmind_dw
ENGINE = MaterializedPostgreSQL('${pg_host}:${pg_port}', '${pg_db}', '${pg_user}', '${pg_password}')
SETTINGS materialized_postgresql_schema = 'dw';
"

echo
echo "== wait torqmind_dw required tables =="
for attempt in {1..120}; do
  count="$(ch --query "SELECT count() FROM system.tables WHERE database = 'torqmind_dw' AND name IN (${required_dw_table_sql})")"
  if [[ "$count" -ge 14 ]]; then
    echo "torqmind_dw ready with $count required tables"
    break
  fi
  if [[ "$attempt" -eq 120 ]]; then
    echo "Timed out waiting for torqmind_dw required tables; found $count/14" >&2
    ch --query "SHOW TABLES FROM torqmind_dw" || true
    exit 1
  fi
  sleep 2
done

echo
echo "== create torqmind_mart tables =="
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_design.sql"

echo
echo "== create streaming materialized views =="
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_streaming_triggers.sql"

echo
echo "== run native backfill =="
ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase3_native_backfill.sql"

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
