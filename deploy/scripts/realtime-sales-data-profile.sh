#!/usr/bin/env bash
set -Eeuo pipefail

# TorqMind Realtime Sales Data Profile
# Diagnoses STG Postgres vs current slim vs mart_rt.
#
# All dates/hours use America/Sao_Paulo (BRT/BRST) as the business timezone.
# STG stores dt_evento in UTC; this script converts to BRT for comparison.
#
# Usage:
#   ENV_FILE=.env.e2e.local COMPOSE_FILE=docker-compose.prod.yml \
#     bash deploy/scripts/realtime-sales-data-profile.sh [--id-empresa 1] [--date 2026-04-21] [--days 7] [--id-filial 14458]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
ID_FILIAL=""
PROFILE_DATE=""
DAYS=7
TZ_BUSINESS="America/Sao_Paulo"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id-empresa) ID_EMPRESA="$2"; shift ;;
    --id-filial) ID_FILIAL="$2"; shift ;;
    --date) PROFILE_DATE="$2"; shift ;;
    --days) DAYS="$2"; shift ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
  shift
done

if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi
: "${CLICKHOUSE_USER:=torqmind}" "${CLICKHOUSE_PASSWORD:=}" "${POSTGRES_USER:=${PG_USER:-postgres}}" "${POSTGRES_DB:=${PG_DATABASE:-TORQMIND}}"

FILIAL_PG=""
FILIAL_CH=""
[[ -n "$ID_FILIAL" ]] && FILIAL_PG="AND c.id_filial=$ID_FILIAL" && FILIAL_CH="AND id_filial=$ID_FILIAL"

compose() { docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"; }

ch() {
  compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=PrettyCompact --send_logs_level=error -q "$1"
}

pg() {
  compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
}

echo "============================================"
echo " TorqMind Sales Data Profile"
echo " id_empresa=$ID_EMPRESA id_filial=${ID_FILIAL:-all} tz=$TZ_BUSINESS days=$DAYS"
echo "============================================"
echo ""

echo "=== 1. Date range in sources ==="
pg "
SELECT 'stg.comprovantes' AS source, count(*) AS total,
  min(dt_evento AT TIME ZONE '$TZ_BUSINESS')::date AS min_date_brt,
  max(CASE WHEN dt_evento < '2027-01-01' THEN dt_evento AT TIME ZONE '$TZ_BUSINESS' END)::date AS max_date_brt
FROM stg.comprovantes WHERE id_empresa=$ID_EMPRESA $FILIAL_PG;
"

ch "
SELECT 'current.stg_comprovantes_slim' AS source, count() AS total,
  min(toDate(dt_evento_local)) AS min_date,
  max(toDate(dt_evento_local)) AS max_date
FROM torqmind_current.stg_comprovantes_slim
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND is_deleted=0 AND data_key < 20270101
"

ch "
SELECT 'mart_rt.sales_daily_rt' AS source, count() AS total_rows,
  min(data_key) AS min_data_key, max(data_key) AS max_data_key
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key < 20270101
"

echo ""
echo "=== 2. Last $DAYS days — daily comparison (STG vs slim vs mart_rt) ==="

# Determine profile date
if [[ -z "$PROFILE_DATE" ]]; then
  PROFILE_DATE="$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT max(dt_evento AT TIME ZONE '$TZ_BUSINESS')::date FROM stg.comprovantes WHERE id_empresa=$ID_EMPRESA AND dt_evento < '2027-01-01' $FILIAL_PG;")"
  PROFILE_DATE="$(echo "$PROFILE_DATE" | tr -d '[:space:]')"
fi
echo "  Profile end date (BRT): $PROFILE_DATE"
echo ""

pg "
WITH days AS (
  SELECT generate_series('$PROFILE_DATE'::date - interval '$(($DAYS - 1)) days', '$PROFILE_DATE'::date, '1 day')::date AS dia
)
SELECT d.dia, count(c.*) AS stg_total,
  count(c.*) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL) AS stg_vendas,
  coalesce(sum(c.valor_total_shadow) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL), 0)::numeric(18,2) AS stg_fat
FROM days d
LEFT JOIN stg.comprovantes c ON c.id_empresa=$ID_EMPRESA $FILIAL_PG
  AND (c.dt_evento AT TIME ZONE '$TZ_BUSINESS')::date = d.dia AND c.dt_evento < '2027-01-01'
GROUP BY d.dia ORDER BY d.dia;
"

START_KEY="$(date -d "$PROFILE_DATE - $(($DAYS - 1)) days" +%Y%m%d)"
END_KEY="$(date -d "$PROFILE_DATE" +%Y%m%d)"

ch "
SELECT data_key, count() AS slim_rows,
  countIf(cancelado=0) AS slim_vendas,
  sumIf(valor_total, cancelado=0) AS slim_fat
FROM torqmind_current.stg_comprovantes_slim
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key BETWEEN $START_KEY AND $END_KEY AND is_deleted=0
GROUP BY data_key ORDER BY data_key
"

ch "
SELECT data_key, sum(qtd_vendas) AS mart_vendas, sum(faturamento) AS mart_fat,
  sum(qtd_canceladas) AS mart_canceladas
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key BETWEEN $START_KEY AND $END_KEY
GROUP BY data_key ORDER BY data_key
"

echo ""
echo "=== 3. Hourly distribution for $PROFILE_DATE (BRT) ==="
pg "
SELECT extract(hour FROM c.dt_evento AT TIME ZONE '$TZ_BUSINESS')::int AS hora,
  count(*) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL) AS stg_vendas,
  coalesce(sum(c.valor_total_shadow) FILTER (WHERE c.cancelado_shadow = false OR c.cancelado_shadow IS NULL), 0)::numeric(18,2) AS stg_fat
FROM stg.comprovantes c
WHERE c.id_empresa=$ID_EMPRESA $FILIAL_PG
  AND (c.dt_evento AT TIME ZONE '$TZ_BUSINESS')::date = '$PROFILE_DATE' AND c.dt_evento < '2027-01-01'
GROUP BY 1 ORDER BY 1;
"

ch "
SELECT hora, count() AS slim_vendas, sum(valor_total) AS slim_fat
FROM torqmind_current.stg_comprovantes_slim
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d) AND is_deleted=0 AND cancelado=0
GROUP BY hora ORDER BY hora
"

ch "
SELECT hora, sum(qtd_vendas) AS mart_vendas, sum(faturamento) AS mart_fat
FROM torqmind_mart_rt.sales_hourly_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d)
GROUP BY hora ORDER BY hora
"

echo ""
echo "=== 4. Filial breakdown for $PROFILE_DATE ==="
ch "
SELECT id_filial, sum(qtd_vendas) AS vendas, sum(faturamento) AS fat
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d)
GROUP BY id_filial ORDER BY fat DESC LIMIT 20
"

echo ""
echo "=== 5. Payment types ==="
ch "
SELECT tipo_forma, label, sum(valor_total) AS total, sum(qtd_transacoes) AS transacoes
FROM torqmind_mart_rt.payments_by_type_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key=$(date -d "$PROFILE_DATE" +%Y%m%d)
GROUP BY tipo_forma, label ORDER BY total DESC LIMIT 15
"

echo ""
echo "=== 6. Cash overview (open shifts) ==="
ch "
SELECT id_filial, id_turno, is_aberto, abertura_ts, faturamento_turno, qtd_vendas_turno
FROM torqmind_mart_rt.cash_overview_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND is_aberto=1
ORDER BY abertura_ts LIMIT 10
"

echo ""
echo "=== 7. Fraud — daily cancellations ==="
ch "
SELECT data_key, sum(qtd_eventos) AS canceladas, sum(impacto_total) AS valor
FROM torqmind_mart_rt.fraud_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key BETWEEN $START_KEY AND $END_KEY
GROUP BY data_key ORDER BY data_key
"

echo ""
echo "=== 8. Finance summary ==="
ch "
SELECT tipo_titulo, faixa, sum(qtd_titulos) AS titulos, sum(valor_total) AS valor
FROM torqmind_mart_rt.finance_overview_rt FINAL
WHERE id_empresa=$ID_EMPRESA $FILIAL_CH
GROUP BY tipo_titulo, faixa ORDER BY tipo_titulo, faixa
"

echo ""
echo "=== 9. Gaps — days with STG data but no mart rows ==="
ch "
SELECT slim.data_key AS missing_in_mart
FROM (
  SELECT DISTINCT data_key FROM torqmind_current.stg_comprovantes_slim
  WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND is_deleted=0 AND cancelado=0
    AND data_key BETWEEN $START_KEY AND $END_KEY
) AS slim
LEFT JOIN (
  SELECT DISTINCT data_key FROM torqmind_mart_rt.sales_daily_rt FINAL
  WHERE id_empresa=$ID_EMPRESA $FILIAL_CH AND data_key BETWEEN $START_KEY AND $END_KEY
) AS mart ON slim.data_key = mart.data_key
WHERE mart.data_key = 0
ORDER BY slim.data_key
"

echo ""
echo "============================================"
echo " Profile complete"
echo "============================================"
