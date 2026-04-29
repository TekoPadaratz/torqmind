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

container_env() {
  local service="$1"
  local key="$2"
  compose exec -T "$service" sh -lc "printf '%s' \"\${${key}:-}\""
}

POSTGRES_DB="${POSTGRES_DB:-$(container_env postgres POSTGRES_DB)}"
POSTGRES_USER="${POSTGRES_USER:-$(container_env postgres POSTGRES_USER)}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-$(container_env postgres POSTGRES_PASSWORD)}"
CLICKHOUSE_PG_HOST="${CLICKHOUSE_PG_HOST:-postgres}"
CLICKHOUSE_PG_PORT="${CLICKHOUSE_PG_PORT:-5432}"

if [[ -z "$POSTGRES_DB" || -z "$POSTGRES_USER" || -z "$POSTGRES_PASSWORD" ]]; then
  echo "PostgreSQL credentials are unavailable for ClickHouse DW sync." >&2
  exit 1
fi

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

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
}

ch_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\'/\\\'}"
  printf "%s" "$value"
}

redact_secrets() {
  local text="$1"
  if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
    text="${text//${POSTGRES_PASSWORD}/***REDACTED***}"
  fi
  if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
    text="${text//${CLICKHOUSE_PASSWORD}/***REDACTED***}"
  fi
  printf "%s\n" "$text"
}

ch_sql_sensitive() {
  local sql="$1"
  local output
  if ! output="$(printf "%s\n" "$sql" | compose exec -T clickhouse "${clickhouse_client_args[@]}" --multiquery 2>&1)"; then
    redact_secrets "$output" >&2
    return 1
  fi
  if [[ -n "$output" ]]; then
    redact_secrets "$output"
  fi
}

pg_table_fn() {
  local table="$1"
  printf "postgresql('%s:%s', '%s', '%s', '%s', '%s', 'dw')" \
    "$(ch_escape "$CLICKHOUSE_PG_HOST")" \
    "$(ch_escape "$CLICKHOUSE_PG_PORT")" \
    "$(ch_escape "$POSTGRES_DB")" \
    "$(ch_escape "$table")" \
    "$(ch_escape "$POSTGRES_USER")" \
    "$(ch_escape "$POSTGRES_PASSWORD")"
}

required_tables=(
  dim_cliente
  dim_filial
  dim_funcionario
  dim_grupo_produto
  dim_local_venda
  dim_produto
  dim_usuario_caixa
  fact_caixa_turno
  fact_comprovante
  fact_financeiro
  fact_pagamento_comprovante
  fact_risco_evento
  fact_venda
  fact_venda_item
)

declare -A engines=(
  [dim_cliente]="ReplacingMergeTree(updated_at)"
  [dim_filial]="ReplacingMergeTree(updated_at)"
  [dim_funcionario]="ReplacingMergeTree(updated_at)"
  [dim_grupo_produto]="ReplacingMergeTree(updated_at)"
  [dim_local_venda]="ReplacingMergeTree(updated_at)"
  [dim_produto]="ReplacingMergeTree(updated_at)"
  [dim_usuario_caixa]="ReplacingMergeTree(updated_at)"
  [fact_caixa_turno]="ReplacingMergeTree(updated_at)"
  [fact_comprovante]="ReplacingMergeTree(updated_at)"
  [fact_financeiro]="ReplacingMergeTree(updated_at)"
  [fact_pagamento_comprovante]="MergeTree()"
  [fact_risco_evento]="MergeTree()"
  [fact_venda]="ReplacingMergeTree(updated_at)"
  [fact_venda_item]="ReplacingMergeTree(updated_at)"
)

declare -A order_by=(
  [dim_cliente]="(id_empresa, id_filial, id_cliente)"
  [dim_filial]="(id_empresa, id_filial)"
  [dim_funcionario]="(id_empresa, id_filial, id_funcionario)"
  [dim_grupo_produto]="(id_empresa, id_filial, id_grupo_produto)"
  [dim_local_venda]="(id_empresa, id_filial, id_local_venda)"
  [dim_produto]="(id_empresa, id_filial, id_produto)"
  [dim_usuario_caixa]="(id_empresa, id_filial, id_usuario)"
  [fact_caixa_turno]="(id_empresa, id_filial, id_turno)"
  [fact_comprovante]="(id_empresa, ifNull(data_key, 0), id_filial, id_db, id_comprovante)"
  [fact_financeiro]="(id_empresa, id_filial, ifNull(data_key_venc, 0), tipo_titulo, id_db, id_titulo)"
  [fact_pagamento_comprovante]="(id_empresa, data_key, id_filial, referencia)"
  [fact_risco_evento]="(id_empresa, data_key, id_filial, event_type, ifNull(id_db, 0), ifNull(id_comprovante, 0), ifNull(id_movprodutos, 0), id)"
  [fact_venda]="(id_empresa, ifNull(data_key, 0), id_filial, id_db, id_movprodutos, id_comprovante)"
  [fact_venda_item]="(id_empresa, ifNull(data_key, 0), id_filial, id_db, id_movprodutos, id_itensmovprodutos, id_comprovante, id_itemcomprovante)"
)

declare -A chunk_column=(
  [fact_comprovante]="data_key"
  [fact_pagamento_comprovante]="data_key"
  [fact_risco_evento]="data_key"
  [fact_venda]="data_key"
  [fact_venda_item]="data_key"
)

next_month() {
  local month_key="$1"
  local year=$((month_key / 100))
  local month=$((month_key % 100))
  if [[ "$month" -eq 12 ]]; then
    printf "%04d01" $((year + 1))
  else
    printf "%04d%02d" "$year" $((month + 1))
  fi
}

create_native_table() {
  local table="$1"
  local source
  source="$(pg_table_fn "$table")"
  ch_sql_sensitive "
CREATE TABLE torqmind_dw.${table}
ENGINE = ${engines[$table]}
ORDER BY ${order_by[$table]}
AS SELECT *
FROM ${source}
LIMIT 0;
"
}

load_table_full() {
  local table="$1"
  local source
  source="$(pg_table_fn "$table")"
  ch_sql_sensitive "INSERT INTO torqmind_dw.${table} SELECT * FROM ${source};"
}

load_table_chunked() {
  local table="$1"
  local column="$2"
  local source min_month max_month current_month start_key end_key
  source="$(pg_table_fn "$table")"

  ch_sql_sensitive "INSERT INTO torqmind_dw.${table} SELECT * FROM ${source} WHERE ${column} IS NULL OR ${column} < 19000101 OR ${column} > 99991231;"

  IFS='|' read -r min_month max_month < <(
    pg "SELECT COALESCE(min(${column}) / 100, 0)::bigint || '|' || COALESCE(max(${column}) / 100, 0)::bigint FROM dw.${table} WHERE ${column} BETWEEN 19000101 AND 99991231;"
  )
  if [[ "${min_month:-0}" -le 0 || "${max_month:-0}" -le 0 ]]; then
    return 0
  fi

  current_month="$min_month"
  while [[ "$current_month" -le "$max_month" ]]; do
    start_key=$((current_month * 100 + 1))
    end_key=$((current_month * 100 + 31))
    echo "  loading ${table} month=${current_month}"
    ch_sql_sensitive "INSERT INTO torqmind_dw.${table} SELECT * FROM ${source} WHERE ${column} BETWEEN ${start_key} AND ${end_key};"
    current_month="$(next_month "$current_month")"
  done
}

validate_table_count() {
  local table="$1"
  local pg_count ch_count
  pg_count="$(pg "SELECT count(*)::bigint FROM dw.${table};")"
  ch_count="$(ch --query "SELECT count() FROM torqmind_dw.${table}")"
  if [[ "$pg_count" != "$ch_count" ]]; then
    echo "ERROR: torqmind_dw.${table} count mismatch: postgres=${pg_count} clickhouse=${ch_count}" >&2
    return 1
  fi
  echo "  OK ${table}: rows=${ch_count}"
}

validate_sales_fact() {
  local table="$1"
  local pg_metric ch_metric
  pg_metric="$(pg "SELECT count(*)::bigint || '|' || COALESCE(max(data_key), 0)::bigint FROM dw.${table};")"
  ch_metric="$(ch --query "SELECT concat(toString(count()), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.${table}")"
  if [[ "$pg_metric" != "$ch_metric" ]]; then
    echo "ERROR: torqmind_dw.${table} count/max(data_key) mismatch: postgres=${pg_metric} clickhouse=${ch_metric}" >&2
    return 1
  fi
  echo "  OK ${table}: count|max_data_key=${ch_metric}"
}

drop_streaming_mvs() {
  local views=()
  mapfile -t views < <(ch --query "SELECT name FROM system.tables WHERE database = 'torqmind_mart' AND engine = 'MaterializedView' ORDER BY name" || true)
  if [[ "${#views[@]}" -eq 0 ]]; then
    return 0
  fi
  echo
  echo "== drop streaming materialized views before full DW refresh =="
  for view in "${views[@]}"; do
    echo "  dropping torqmind_mart.${view}"
    ch --query "DROP VIEW IF EXISTS torqmind_mart.${view}"
  done
}

echo "== validate PostgreSQL and ClickHouse connectivity =="
pg "SELECT 1" >/dev/null
compose exec -T clickhouse sh -lc 'wget -q -O - http://127.0.0.1:8123/ping | grep -q Ok'
echo "Connectivity OK"

drop_streaming_mvs

echo
echo "== recreate native torqmind_dw =="
ch --multiquery --query "DROP DATABASE IF EXISTS torqmind_dw SYNC; CREATE DATABASE torqmind_dw;"

echo
echo "== create native torqmind_dw tables =="
for table in "${required_tables[@]}"; do
  echo "  creating ${table}"
  create_native_table "$table"
done

echo
echo "== load PostgreSQL dw.* into native ClickHouse torqmind_dw =="
for table in "${required_tables[@]}"; do
  echo "  loading ${table}"
  if [[ -n "${chunk_column[$table]:-}" ]]; then
    load_table_chunked "$table" "${chunk_column[$table]}"
  else
    load_table_full "$table"
  fi
done

echo
echo "== validate native torqmind_dw row counts =="
for table in "${required_tables[@]}"; do
  validate_table_count "$table"
done

echo
echo "== validate critical sales facts =="
validate_sales_fact fact_venda
validate_sales_fact fact_venda_item

echo
echo "Native ClickHouse DW sync completed."
