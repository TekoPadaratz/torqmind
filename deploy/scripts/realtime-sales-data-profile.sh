#!/usr/bin/env bash
set -Eeuo pipefail

# TorqMind Realtime Sales Data Profile
# Compares STG (PostgreSQL) vs Current (ClickHouse) vs mart_rt for temporal correctness.
# 
# All dates/hours use America/Sao_Paulo (BRT/BRST) as the business timezone.
# STG stores dt_evento in UTC; this script converts to BRT for comparison.
#
# Usage:
#   ENV_FILE=.env.e2e.local ./deploy/scripts/realtime-sales-data-profile.sh [--id-empresa 1] [--date 2026-04-21] [--days 7]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
PROFILE_DATE=""
DAYS=7
TZ_BUSINESS="America/Sao_Paulo"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id-empresa) ID_EMPRESA="$2"; shift ;;
    --date) PROFILE_DATE="$2"; shift ;;
    --days) DAYS="$2"; shift ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
  shift
done

source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"
[[ -f "$ENV_FILE" ]] && tm_load_env_file "$ENV_FILE"
: "${CLICKHOUSE_USER:=torqmind}" "${CLICKHOUSE_PASSWORD:=}" "${POSTGRES_USER:=postgres}" "${POSTGRES_DB:=TORQMIND}"

ch() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
    clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=PrettyCompact --send_logs_level=error -q "$1"
}

pg() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
}

echo "============================================"
echo " TorqMind Sales Data Profile"
echo " id_empresa=$ID_EMPRESA tz=$TZ_BUSINESS days=$DAYS"
echo "============================================"
echo ""

echo "=== 1. Date range in sources ==="
pg "
SELECT 
  'stg.comprovantes' AS source,
  count(*) AS total,
  min(dt_evento AT TIME ZONE '$TZ_BUSINESS')::date AS min_date_brt,
  max(CASE WHEN dt_evento < '2027-01-01' THEN dt_evento AT TIME ZONE '$TZ_BUSINESS' END)::date AS max_date_brt,
  count(*) FILTER (WHERE dt_evento IS NULL) AS null_dt_evento
FROM stg.comprovantes WHERE id_empresa=$ID_EMPRESA;
"

ch "
SELECT 
  'current.stg_comprovantes' AS source,
  count() AS total,
  min(toDate(toTimezone(dt_evento, '$TZ_BUSINESS'))) AS min_date_brt,
  max(toDate(toTimezone(dt_evento, '$TZ_BUSINESS'))) AS max_date_brt
FROM torqmind_current.stg_comprovantes FINAL
WHERE id_empresa=$ID_EMPRESA AND is_deleted=0 AND dt_evento < '2027-01-01'
"

ch "
SELECT 
  'mart_rt.sales_daily_rt' AS source,
  count() AS total_rows,
  min(data_key) AS min_data_key,
  max(data_key) AS max_data_key
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key < 20270101
"

echo ""
echo "=== 2. Last $DAYS days - daily comparison (STG vs current vs mart_rt) ==="

# Determine profile date
if [[ -z "$PROFILE_DATE" ]]; then
  PROFILE_DATE="$(docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT max(dt_evento AT TIME ZONE '$TZ_BUSINESS')::date FROM stg.comprovantes WHERE id_empresa=$ID_EMPRESA AND dt_evento < '2027-01-01';")"
  PROFILE_DATE="$(echo "$PROFILE_DATE" | tr -d '[:space:]')"
fi
echo "  Profile end date (BRT): $PROFILE_DATE"
echo ""

pg "
WITH days AS (
  SELECT generate_series('$PROFILE_DATE'::date - interval '$(($DAYS - 1)) days', '$PROFILE_DATE'::date, '1 day')::date AS dia
)
SELECT 
  d.dia,
  count(c.*) AS stg_total,
  count(c.*) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL) AS stg_vendas,
  coalesce(sum(c.valor_total_shadow) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL), 0)::numeric(18,2) AS stg_fat
FROM days d
LEFT JOIN stg.comprovantes c 
  ON c.id_empresa=$ID_EMPRESA 
  AND (c.dt_evento AT TIME ZONE '$TZ_BUSINESS')::date = d.dia
  AND c.dt_evento < '2027-01-01'
GROUP BY d.dia ORDER BY d.dia;
"

START_KEY="$(date -d "$PROFILE_DATE - $(($DAYS - 1)) days" +%Y%m%d)"
END_KEY="$(date -d "$PROFILE_DATE" +%Y%m%d)"

ch "
SELECT 
  data_key,
  sum(qtd_vendas) AS mart_vendas,
  sum(faturamento) AS mart_fat,
  sum(qtd_canceladas) AS mart_canceladas
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key BETWEEN $START_KEY AND $END_KEY
GROUP BY data_key ORDER BY data_key
"

echo ""
echo "=== 3. Hourly distribution for $PROFILE_DATE (BRT) ==="
pg "
SELECT 
  extract(hour FROM c.dt_evento AT TIME ZONE '$TZ_BUSINESS')::int AS hora,
  count(*) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL) AS stg_vendas,
  coalesce(sum(c.valor_total_shadow) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL), 0)::numeric(18,2) AS stg_fat
FROM stg.comprovantes c
WHERE c.id_empresa=$ID_EMPRESA 
  AND (c.dt_evento AT TIME ZONE '$TZ_BUSINESS')::date = '$PROFILE_DATE'
  AND c.dt_evento < '2027-01-01'
GROUP BY 1 ORDER BY 1;
"

ch "
SELECT hora, sum(qtd_vendas) AS mart_vendas, sum(faturamento) AS mart_fat
FROM torqmind_mart_rt.sales_hourly_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d)
GROUP BY hora ORDER BY hora
"

echo ""
echo "=== 4. Filial breakdown for $PROFILE_DATE ==="
ch "
SELECT 
  id_filial,
  sum(qtd_vendas) AS vendas,
  sum(faturamento) AS fat
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d)
GROUP BY id_filial ORDER BY fat DESC
LIMIT 20
"

echo ""
echo "=== 5. Payment types ==="
ch "
SELECT 
  tipo_forma, label, sum(total) AS total, sum(qtd_transacoes) AS transacoes
FROM torqmind_mart_rt.payments_by_type_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d)
GROUP BY tipo_forma, label ORDER BY total DESC
LIMIT 15
"

echo ""
echo "=== 6. Cash overview (open shifts) ==="
ch "
SELECT id_filial, id_turno, is_aberto, abertura_ts, vendas_vinculadas, total_vendas
FROM torqmind_mart_rt.cash_overview_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND is_aberto=1
ORDER BY abertura_ts
LIMIT 10
"

echo ""
echo "=== 7. Fraud - recent cancellations ==="
ch "
SELECT data_key, sum(qtd_canceladas) AS canceladas, sum(valor_cancelado) AS valor
FROM torqmind_mart_rt.fraud_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key BETWEEN $START_KEY AND $END_KEY
GROUP BY data_key ORDER BY data_key
"

echo ""
echo "=== 8. Finance summary ==="
ch "
SELECT 
  tipo, sum(total_valor) AS valor, sum(qtd_titulos) AS titulos, sum(qtd_vencidos) AS vencidos
FROM torqmind_mart_rt.finance_overview_rt FINAL
WHERE id_empresa=$ID_EMPRESA
GROUP BY tipo
"

echo ""
echo "============================================"
echo " Profile complete"
echo "============================================"
