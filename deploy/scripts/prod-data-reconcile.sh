#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"
ID_EMPRESA="${ID_EMPRESA:?missing ID_EMPRESA}"
ID_FILIAL="${ID_FILIAL:-}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

if [[ "$ALLOW_INSECURE_ENV" != "1" ]]; then
  tm_require_prod_runtime_env "$ENV_FILE"
fi

cd "$ROOT_DIR"

if ! [[ "$ID_EMPRESA" =~ ^[0-9]+$ ]]; then
  echo "ID_EMPRESA must be numeric" >&2
  exit 2
fi
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" && ! "$ID_FILIAL" =~ ^[0-9]+$ ]]; then
  echo "ID_FILIAL must be numeric or -1" >&2
  exit 2
fi

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

container_env() {
  local service="$1"
  local key="$2"
  compose exec -T "$service" sh -lc "printf '%s' \"\${${key}:-}\""
}

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
}

CLICKHOUSE_USER="${CLICKHOUSE_USER:-$(container_env clickhouse CLICKHOUSE_USER)}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-$(container_env clickhouse CLICKHOUSE_PASSWORD)}"

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

errors=0
warnings=0

error() {
  echo "ERROR: $*" >&2
  errors=$((errors + 1))
}

warn() {
  echo "WARN: $*" >&2
  warnings=$((warnings + 1))
}

ok() {
  echo "OK: $*"
}

pg_branch=""
pg_item_branch=""
ch_branch=""
scope_label="empresa=${ID_EMPRESA}"
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" ]]; then
  pg_branch=" AND id_filial = ${ID_FILIAL}"
  pg_item_branch=" AND i.id_filial = ${ID_FILIAL}"
  ch_branch=" AND id_filial = ${ID_FILIAL}"
  scope_label="${scope_label} filial=${ID_FILIAL}"
else
  scope_label="${scope_label} filial=todas"
fi

metric_pg_fact_venda() {
  pg "SELECT count(*)::bigint || '|' || COALESCE(to_char(min(data), 'YYYY-MM-DD'), '') || '|' || COALESCE(to_char(max(data), 'YYYY-MM-DD'), '') || '|' || COALESCE(min(data_key), 0)::bigint || '|' || COALESCE(max(data_key), 0)::bigint || '|' || COALESCE(max(updated_at)::text, '') FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"
}

metric_pg_fact_venda_item() {
  pg "SELECT count(*)::bigint || '|' || COALESCE(min(data_key), 0)::bigint || '|' || COALESCE(max(data_key), 0)::bigint || '|' || COALESCE(max(updated_at)::text, '') FROM dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"
}

metric_pg_fact_estoque() {
  pg "SELECT count(*)::bigint || '|' || COALESCE(max(data_key_ref), 0)::bigint || '|' || COALESCE(max(updated_at)::text, '') FROM dw.fact_estoque_atual WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"
}

metric_ch_fact_venda() {
  ch --query "SELECT concat(toString(count()), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(coalesce(max(updated_at), toDateTime(0)))) FROM torqmind_dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
}

metric_ch_fact_venda_item() {
  ch --query "SELECT concat(toString(count()), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(coalesce(max(updated_at), toDateTime(0)))) FROM torqmind_dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
}

metric_ch_fact_estoque() {
  ch --query "SELECT concat(toString(count()), '|', toString(coalesce(max(data_key_ref), 0)), '|', toString(coalesce(max(updated_at), toDateTime(0)))) FROM torqmind_dw.fact_estoque_atual WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
}

metric_ch_mart_sales() {
  ch --query "SELECT concat(toString(count()), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(round(coalesce(sum(faturamento), 0), 2)), '|', toString(coalesce(max(updated_at), toDateTime(0)))) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
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
  fact_estoque_atual
  fact_financeiro
  fact_pagamento_comprovante
  fact_risco_evento
  fact_venda
  fact_venda_item
)

echo "== TorqMind data reconciliation (${scope_label}) =="
echo

echo "required tables:"
dw_engine="$(ch --query "SELECT engine FROM system.databases WHERE name = 'torqmind_dw'" || true)"
if [[ -z "$dw_engine" ]]; then
  error "ClickHouse database torqmind_dw is missing"
elif [[ "$dw_engine" == "MaterializedPostgreSQL" ]]; then
  error "torqmind_dw is still using MaterializedPostgreSQL; production path must use native ClickHouse tables"
else
  ok "torqmind_dw database engine=${dw_engine}"
fi

for table in "${required_tables[@]}"; do
  table_count="$(ch --query "SELECT count() FROM system.tables WHERE database = 'torqmind_dw' AND name = '${table}'" || true)"
  if [[ "$table_count" != "1" ]]; then
    error "required table torqmind_dw.${table} is missing"
  else
    ok "torqmind_dw.${table} exists"
  fi
done

mart_exists="$(ch --query "SELECT count() FROM system.tables WHERE database = 'torqmind_mart' AND name = 'agg_vendas_diaria'" || true)"
if [[ "$mart_exists" != "1" ]]; then
  error "required mart torqmind_mart.agg_vendas_diaria is missing"
fi

stock_mart_exists="$(ch --query "SELECT count() FROM system.tables WHERE database = 'torqmind_mart' AND name = 'agg_estoque_posicao_atual'" || true)"
if [[ "$stock_mart_exists" != "1" ]]; then
  error "required mart torqmind_mart.agg_estoque_posicao_atual is missing"
fi

if [[ "${APP_CORS_ORIGINS:-}" == *"localhost"* || "${APP_CORS_ORIGINS:-}" == *"127.0.0.1"* || "${APP_CORS_ORIGIN_REGEX:-}" == *"localhost"* ]]; then
  warn "CORS still contains localhost entries"
fi

if [[ "$errors" -gt 0 ]]; then
  echo
  echo "Reconciliation finished with ${errors} critical error(s) before metrics."
  exit 1
fi

echo
pg_venda="$(metric_pg_fact_venda)"
pg_item="$(metric_pg_fact_venda_item)"
pg_estoque="$(metric_pg_fact_estoque)"
ch_venda="$(metric_ch_fact_venda)"
ch_item="$(metric_ch_fact_venda_item)"
ch_estoque="$(metric_ch_fact_estoque)"
ch_mart="$(metric_ch_mart_sales)"

IFS='|' read -r pg_venda_count pg_venda_min_data pg_venda_max_data pg_venda_min_key pg_venda_max_key pg_venda_updated <<< "$pg_venda"
IFS='|' read -r pg_item_count pg_item_min_key pg_item_max_key pg_item_updated <<< "$pg_item"
IFS='|' read -r pg_estoque_count pg_estoque_max_key pg_estoque_updated <<< "$pg_estoque"
IFS='|' read -r ch_venda_count ch_venda_min_key ch_venda_max_key ch_venda_updated <<< "$ch_venda"
IFS='|' read -r ch_item_count ch_item_min_key ch_item_max_key ch_item_updated <<< "$ch_item"
IFS='|' read -r ch_estoque_count ch_estoque_max_key ch_estoque_updated <<< "$ch_estoque"
IFS='|' read -r mart_count mart_min_key mart_max_key mart_sum_faturamento mart_updated <<< "$ch_mart"

echo "PostgreSQL dw.fact_venda:       rows=${pg_venda_count} min_data=${pg_venda_min_data:-null} max_data=${pg_venda_max_data:-null} min_data_key=${pg_venda_min_key} max_data_key=${pg_venda_max_key} max_updated_at=${pg_venda_updated:-null}"
echo "PostgreSQL dw.fact_venda_item:  rows=${pg_item_count} min_data_key=${pg_item_min_key} max_data_key=${pg_item_max_key} max_updated_at=${pg_item_updated:-null}"
echo "PostgreSQL dw.fact_estoque_atual: rows=${pg_estoque_count} max_data_key_ref=${pg_estoque_max_key} max_updated_at=${pg_estoque_updated:-null}"
echo "ClickHouse torqmind_dw.fact_venda:      rows=${ch_venda_count} min_data_key=${ch_venda_min_key} max_data_key=${ch_venda_max_key} max_updated_at=${ch_venda_updated:-null}"
echo "ClickHouse torqmind_dw.fact_venda_item: rows=${ch_item_count} min_data_key=${ch_item_min_key} max_data_key=${ch_item_max_key} max_updated_at=${ch_item_updated:-null}"
echo "ClickHouse torqmind_dw.fact_estoque_atual: rows=${ch_estoque_count} max_data_key_ref=${ch_estoque_max_key} max_updated_at=${ch_estoque_updated:-null}"
echo "ClickHouse torqmind_mart.agg_vendas_diaria: rows=${mart_count} min_data_key=${mart_min_key} max_data_key=${mart_max_key} sum_faturamento=${mart_sum_faturamento} max_updated_at=${mart_updated:-null}"

echo
echo "etl.watermark:"
watermark_exists="$(pg "SELECT to_regclass('etl.watermark') IS NOT NULL;")"
if [[ "$watermark_exists" == "t" ]]; then
  pg "SELECT dataset || '|' || COALESCE(updated_at::text, '') FROM etl.watermark WHERE id_empresa = ${ID_EMPRESA} AND dataset IN ('comprovantes','itenscomprovantes','comprovantespagamentos','comprovantes_sales_fact','itenscomprovantes_sales_fact') ORDER BY dataset;" || true
else
  echo "etl.watermark not found"
fi

echo
echo "checks:"

data_key_mismatch="$(pg "SELECT count(*)::bigint FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch} AND data IS NOT NULL AND data_key <> to_char(data, 'YYYYMMDD')::int;")"
echo "dw.fact_venda data_key mismatches=${data_key_mismatch}"
if [[ "$data_key_mismatch" != "0" ]]; then
  warn "dw.fact_venda has ${data_key_mismatch} data_key mismatch(es)"
fi

orphan_items="$(pg "SELECT count(*)::bigint FROM dw.fact_venda_item i LEFT JOIN dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_comprovante = i.id_comprovante WHERE i.id_empresa = ${ID_EMPRESA}${pg_item_branch} AND v.id_comprovante IS NULL;")"
echo "dw.fact_venda_item orphan items=${orphan_items}"
if [[ "$orphan_items" != "0" ]]; then
  warn "PostgreSQL dw.fact_venda_item has ${orphan_items} orphan item(s); this is data quality debt, not a ClickHouse rebuild trigger"
fi

if [[ "$pg_venda_count" != "$ch_venda_count" || "$pg_venda_max_key" != "$ch_venda_max_key" ]]; then
  error "torqmind_dw.fact_venda count/max(data_key) diverges from PostgreSQL"
else
  ok "torqmind_dw.fact_venda matches PostgreSQL count/max(data_key)"
fi

if [[ "$pg_item_count" != "$ch_item_count" || "$pg_item_max_key" != "$ch_item_max_key" ]]; then
  error "torqmind_dw.fact_venda_item count/max(data_key) diverges from PostgreSQL"
else
  ok "torqmind_dw.fact_venda_item matches PostgreSQL count/max(data_key)"
fi

if [[ "$pg_estoque_count" != "$ch_estoque_count" || "$pg_estoque_max_key" != "$ch_estoque_max_key" ]]; then
  error "torqmind_dw.fact_estoque_atual count/max(data_key_ref) diverges from PostgreSQL"
else
  ok "torqmind_dw.fact_estoque_atual matches PostgreSQL count/max(data_key_ref)"
fi

if (( mart_max_key < ch_item_max_key )); then
  error "torqmind_mart.agg_vendas_diaria max(data_key) is behind torqmind_dw.fact_venda_item"
else
  ok "torqmind_mart.agg_vendas_diaria covers the latest replicated sales item data_key"
fi

echo
echo "Reconciliation summary: errors=${errors} warnings=${warnings}"
if [[ "$errors" -gt 0 ]]; then
  exit 1
fi
exit 0
