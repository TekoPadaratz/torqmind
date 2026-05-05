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

emit_gap_line() {
  local label="$1"
  python3 -c '
from datetime import date
import sys

label = sys.argv[1]
months = [line.split("|", 1)[0] for line in sys.stdin.read().splitlines() if line.strip()]
if not months:
    print(f"{label}.gaps|none")
    raise SystemExit(0)
months = sorted(set(months))
start = date.fromisoformat(months[0] + "-01")
end = date.fromisoformat(months[-1] + "-01")
present = set(months)
missing = []
cursor = start
while cursor <= end:
    month_ref = cursor.strftime("%Y-%m")
    if month_ref not in present:
        missing.append(month_ref)
    if cursor.month == 12:
        cursor = date(cursor.year + 1, 1, 1)
    else:
        cursor = date(cursor.year, cursor.month + 1, 1)
print(f"{label}.gaps|{','.join(missing) if missing else 'none'}")
' "$label"
}

print_monthly_section() {
  local label="$1"
  local output="$2"
  if [[ -n "$output" ]]; then
    printf '%s\n' "$output"
  fi
  printf '%s\n' "$output" | emit_gap_line "$label"
}

echo "== TorqMind historical coverage audit =="
echo "empresa=${ID_EMPRESA} filial=${ID_FILIAL:-todas}"
echo

echo "PostgreSQL STG coverage (canonical sales sources only):"
pg "SELECT 'stg.comprovantes|' || count(*) || '|' || COALESCE(min(etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)))::text, '') || '|' || COALESCE(max(etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)))::text, '') FROM stg.comprovantes WHERE id_empresa = ${ID_EMPRESA}${pg_branch};" || true
pg "SELECT 'stg.itenscomprovantes|' || count(*) || '|' || COALESCE(min(etl.business_date(dt_evento))::text, '') || '|' || COALESCE(max(etl.business_date(dt_evento))::text, '') FROM stg.itenscomprovantes WHERE id_empresa = ${ID_EMPRESA}${pg_branch};" || true

echo
echo "PostgreSQL DW coverage:"
pg "SELECT 'dw.fact_venda|' || count(*) || '|' || COALESCE(min(data::date)::text, '') || '|' || COALESCE(max(data::date)::text, '') || '|' || COALESCE(min(data_key), 0) || '|' || COALESCE(max(data_key), 0) FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"
pg "SELECT 'dw.fact_venda_item|' || count(*) || '|' || COALESCE(min(to_date(data_key::text, 'YYYYMMDD'))::text, '') || '|' || COALESCE(max(to_date(data_key::text, 'YYYYMMDD'))::text, '') || '|' || COALESCE(min(data_key), 0) || '|' || COALESCE(max(data_key), 0) FROM dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${pg_branch};"

echo
echo "ClickHouse DW/mart coverage:"
ch --query "SELECT concat('torqmind_dw.fact_venda|', toString(count()), '|', if(count() = 0, '', formatDateTime(parseDateTimeBestEffortOrNull(toString(min(data_key))), '%F')), '|', if(count() = 0, '', formatDateTime(parseDateTimeBestEffortOrNull(toString(max(data_key))), '%F')), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
ch --query "SELECT concat('torqmind_dw.fact_venda_item|', toString(count()), '|', if(count() = 0, '', formatDateTime(parseDateTimeBestEffortOrNull(toString(min(data_key))), '%F')), '|', if(count() = 0, '', formatDateTime(parseDateTimeBestEffortOrNull(toString(max(data_key))), '%F')), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0))) FROM torqmind_dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"
ch --query "SELECT concat('torqmind_mart.agg_vendas_diaria|', toString(count()), '|', if(count() = 0, '', formatDateTime(parseDateTimeBestEffortOrNull(toString(min(data_key))), '%F')), '|', if(count() = 0, '', formatDateTime(parseDateTimeBestEffortOrNull(toString(max(data_key))), '%F')), '|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(round(coalesce(sum(faturamento), 0), 2))) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = ${ID_EMPRESA}${ch_branch}"

echo
echo "Monthly counts PostgreSQL STG:"
monthly_stg_comprovantes="$(pg "SELECT month_ref || '|stg.comprovantes|' || COALESCE(min_dt::text, '') || '|' || COALESCE(max_dt::text, '') || '|' || cnt FROM (SELECT to_char(date_trunc('month', business_date)::date, 'YYYY-MM') AS month_ref, min(business_date)::date AS min_dt, max(business_date)::date AS max_dt, count(*)::bigint AS cnt FROM (SELECT etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)) AS business_date FROM stg.comprovantes WHERE id_empresa = ${ID_EMPRESA}${pg_branch}) s WHERE business_date IS NOT NULL GROUP BY 1) q ORDER BY month_ref;" || true)"
monthly_stg_itens="$(pg "SELECT month_ref || '|stg.itenscomprovantes|' || COALESCE(min_dt::text, '') || '|' || COALESCE(max_dt::text, '') || '|' || cnt FROM (SELECT to_char(date_trunc('month', business_date)::date, 'YYYY-MM') AS month_ref, min(business_date)::date AS min_dt, max(business_date)::date AS max_dt, count(*)::bigint AS cnt FROM (SELECT etl.business_date(dt_evento) AS business_date FROM stg.itenscomprovantes WHERE id_empresa = ${ID_EMPRESA}${pg_branch}) s WHERE business_date IS NOT NULL GROUP BY 1) q ORDER BY month_ref;" || true)"
print_monthly_section "stg.comprovantes" "$monthly_stg_comprovantes"
print_monthly_section "stg.itenscomprovantes" "$monthly_stg_itens"

echo
echo "Monthly counts PostgreSQL DW:"
monthly_dw_venda="$(pg "SELECT month_ref || '|dw.fact_venda|' || COALESCE(min_dt::text, '') || '|' || COALESCE(max_dt::text, '') || '|' || cnt FROM (SELECT to_char(date_trunc('month', data)::date, 'YYYY-MM') AS month_ref, min(data::date) AS min_dt, max(data::date) AS max_dt, count(*)::bigint AS cnt FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch} GROUP BY 1) q ORDER BY month_ref;")"
monthly_dw_venda_item="$(pg "SELECT month_ref || '|dw.fact_venda_item|' || COALESCE(min_dt::text, '') || '|' || COALESCE(max_dt::text, '') || '|' || cnt FROM (SELECT to_char(date_trunc('month', to_date(data_key::text, 'YYYYMMDD'))::date, 'YYYY-MM') AS month_ref, min(to_date(data_key::text, 'YYYYMMDD')) AS min_dt, max(to_date(data_key::text, 'YYYYMMDD')) AS max_dt, count(*)::bigint AS cnt FROM dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${pg_branch} GROUP BY 1) q ORDER BY month_ref;")"
print_monthly_section "dw.fact_venda" "$monthly_dw_venda"
print_monthly_section "dw.fact_venda_item" "$monthly_dw_venda_item"

echo
echo "Monthly counts ClickHouse DW:"
monthly_ch_dw_venda="$(ch --query "SELECT concat(concat(substring(toString(intDiv(data_key, 100)), 1, 4), '-', substring(toString(intDiv(data_key, 100)), 5, 2)), '|torqmind_dw.fact_venda|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(count())) FROM torqmind_dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${ch_branch} GROUP BY intDiv(data_key, 100) ORDER BY intDiv(data_key, 100)")"
monthly_ch_dw_venda_item="$(ch --query "SELECT concat(concat(substring(toString(intDiv(data_key, 100)), 1, 4), '-', substring(toString(intDiv(data_key, 100)), 5, 2)), '|torqmind_dw.fact_venda_item|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(count())) FROM torqmind_dw.fact_venda_item WHERE id_empresa = ${ID_EMPRESA}${ch_branch} GROUP BY intDiv(data_key, 100) ORDER BY intDiv(data_key, 100)")"
print_monthly_section "torqmind_dw.fact_venda" "$monthly_ch_dw_venda"
print_monthly_section "torqmind_dw.fact_venda_item" "$monthly_ch_dw_venda_item"

echo
echo "Monthly counts ClickHouse mart:"
monthly_ch_mart="$(ch --query "SELECT concat(concat(substring(toString(intDiv(data_key, 100)), 1, 4), '-', substring(toString(intDiv(data_key, 100)), 5, 2)), '|torqmind_mart.agg_vendas_diaria|', toString(coalesce(min(data_key), 0)), '|', toString(coalesce(max(data_key), 0)), '|', toString(count()), '|', toString(round(sum(faturamento), 2))) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = ${ID_EMPRESA}${ch_branch} GROUP BY intDiv(data_key, 100) ORDER BY intDiv(data_key, 100)")"
print_monthly_section "torqmind_mart.agg_vendas_diaria" "$monthly_ch_mart"

echo
echo "Cancelado vs ativo PostgreSQL DW:"
pg "SELECT COALESCE(cancelado::text, 'null') || '|' || count(*) FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch} GROUP BY cancelado ORDER BY cancelado;"

echo
echo "Watermarks:"
pg "SELECT dataset || '|' || COALESCE(updated_at::text, '') FROM etl.watermark WHERE id_empresa = ${ID_EMPRESA} AND dataset IN ('comprovantes','itenscomprovantes','comprovantes_sales_fact','itenscomprovantes_sales_fact') ORDER BY dataset;" || true

echo
echo "Date parse checks:"
pg "SELECT 'dw.fact_venda_data_key_mismatch|' || count(*) FROM dw.fact_venda WHERE id_empresa = ${ID_EMPRESA}${pg_branch} AND data IS NOT NULL AND data_key <> to_char(data, 'YYYYMMDD')::int;"
