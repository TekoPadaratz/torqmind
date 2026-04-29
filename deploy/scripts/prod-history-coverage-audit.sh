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

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
}

CLICKHOUSE_USER="${CLICKHOUSE_USER:-$(compose exec -T clickhouse sh -lc 'printf "%s" "${CLICKHOUSE_USER:-}"')}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-$(compose exec -T clickhouse sh -lc 'printf "%s" "${CLICKHOUSE_PASSWORD:-}"')}"
clickhouse_client_args=(clickhouse-client)
if [[ -n "$CLICKHOUSE_USER" ]]; then clickhouse_client_args+=(--user "$CLICKHOUSE_USER"); fi
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then clickhouse_client_args+=(--password "$CLICKHOUSE_PASSWORD"); fi

ch() {
  compose exec -T clickhouse "${clickhouse_client_args[@]}" "$@"
}

pg_branch=""
ch_branch=""
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" ]]; then
  pg_branch=" AND id_filial = ${ID_FILIAL}"
  ch_branch=" AND id_filial = ${ID_FILIAL}"
fi

echo "== TorqMind historical coverage audit =="
echo "empresa=${ID_EMPRESA} filial=${ID_FILIAL:-todas}"
echo

echo "PostgreSQL STG coverage (canonical sales sources only):"
pg "SELECT 'stg.comprovantes|' || count(*) || '|' || COALESCE(min((payload->>'DATA')::date)::text, '') || '|' || COALESCE(max((payload->>'DATA')::date)::text, '') FROM stg.comprovantes WHERE id_empresa = ${ID_EMPRESA};" || true
if [[ "$(pg "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'stg' AND table_name = 'itenscomprovantes' AND column_name = 'dt_evento');")" == "t" ]]; then
  pg "SELECT 'stg.itenscomprovantes|' || count(*) || '|' || COALESCE(min(dt_evento)::text, '') || '|' || COALESCE(max(dt_evento)::text, '') FROM stg.itenscomprovantes WHERE id_empresa = ${ID_EMPRESA};" || true
else
  pg "SELECT 'stg.itenscomprovantes|' || count(*) || '||' FROM stg.itenscomprovantes WHERE id_empresa = ${ID_EMPRESA};" || true
fi

echo
echo "PostgreSQL DW coverage:"
pg "SELECT 'dw.fact_venda|' || count(*) || '|' || COALESCE(min(data)::text, '') || '|' || COALESCE(max(data)::text, '') || '|' || COALESCE(min(data_key), 0) || '|' || COALESCE(max(data_key), 0) FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"
pg "SELECT 'dw.fact_venda_item|' || count(*) || '|' || COALESCE(min(data_key), 0) || '|' || COALESCE(max(data_key), 0) FROM dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"

echo
echo "ClickHouse DW/mart coverage:"
ch --query "SELECT concat('torqmind_dw.fact_venda|', toString(count()), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
ch --query "SELECT concat('torqmind_dw.fact_venda_item|', toString(count()), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
ch --query "SELECT concat('torqmind_mart.agg_vendas_diaria|', toString(count()), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(round(coalesce(sum(faturamento), 0), 2))) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"

echo
echo "Monthly counts PostgreSQL DW:"
pg "SELECT month_ref || '|fact_venda|' || count(*) FROM (SELECT to_char(data, 'YYYY-MM') AS month_ref FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch}) s GROUP BY month_ref ORDER BY month_ref;"
pg "SELECT month_ref || '|fact_venda_item|' || count(*) FROM (SELECT (data_key / 100)::text AS month_ref FROM dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${pg_branch}) s GROUP BY month_ref ORDER BY month_ref;"

echo
echo "Monthly counts ClickHouse mart:"
ch --query "SELECT concat(toString(intDiv(data_key, 100)), '|agg_vendas_diaria|', toString(count()), '|', toString(round(sum(faturamento), 2))) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = ${ID_EMPRESA}${ch_branch} GROUP BY intDiv(data_key, 100) ORDER BY intDiv(data_key, 100)"

echo
echo "Cancelado vs ativo PostgreSQL DW:"
pg "SELECT COALESCE(cancelado::text, 'null') || '|' || count(*) FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch} GROUP BY cancelado ORDER BY cancelado;"

echo
echo "Watermarks:"
pg "SELECT dataset || '|' || COALESCE(updated_at::text, '') FROM etl.watermark WHERE id_empresa = ${ID_EMPRESA} AND dataset IN ('comprovantes','itenscomprovantes','comprovantes_sales_fact','itenscomprovantes_sales_fact') ORDER BY dataset;" || true

echo
echo "Date parse checks:"
pg "SELECT 'dw.fact_venda_data_key_mismatch|' || count(*) FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch} AND data IS NOT NULL AND data_key <> to_char(data, 'YYYYMMDD')::int;"
