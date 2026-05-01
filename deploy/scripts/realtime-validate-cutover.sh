#!/usr/bin/env bash
set -Eeuo pipefail

# Blocking realtime cutover validation.
# Fails on ClickHouse/API connection errors, missing mart_rt tables, empty realtime
# marts when source/legacy has data, or metric divergence above tolerance.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
DECIMAL_TOLERANCE="${DECIMAL_TOLERANCE:-${TOLERANCE:-0.001}}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"

FAILURES=0
CHECKS=0

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

compose_prod() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

ch_query() {
  local sql="$1"
  local out
  if ! out="$(compose_prod exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=TabSeparated -q "$sql" 2>&1)"; then
    printf '__ERROR__:%s' "$out"
    return 0
  fi
  printf '%s' "$out"
}

normalize_number() {
  local value="$1"
  value="${value//[[:space:]]/}"
  case "$value" in
    ""|"\\N"|"NULL"|"nan"|"NaN") value="0" ;;
  esac
  printf '%s' "$value"
}

record_failure() {
  local label="$1"
  local detail="$2"
  printf '  %-48s %s\n' "$label" "$detail"
  FAILURES=$((FAILURES + 1))
}

require_clickhouse_connection() {
  CHECKS=$((CHECKS + 1))
  local result
  result="$(ch_query "SELECT 1")"
  if [[ "$(normalize_number "$result")" != "1" ]]; then
    record_failure "clickhouse.connection" "FAILED"
    return 1
  fi
  printf '  %-48s OK\n' "clickhouse.connection"
}

table_exists() {
  local full_table="$1"
  local db table result
  db="${full_table%%.*}"
  table="${full_table#*.}"
  result="$(ch_query "SELECT count() FROM system.tables WHERE database='$db' AND name='$table'")"
  [[ "$(normalize_number "$result")" == "1" ]]
}

validate_table_exists() {
  local rt_table="$1"
  CHECKS=$((CHECKS + 1))
  if ! table_exists "$rt_table"; then
    record_failure "$rt_table" "TABLE_MISSING"
    return 1
  fi
  printf '  %-48s OK\n' "$rt_table"
}

compare_metric() {
  local label="$1"
  local legacy_sql="$2"
  local rt_sql="$3"
  local mode="${4:-decimal}"

  CHECKS=$((CHECKS + 1))

  local legacy_val rt_val
  legacy_val="$(ch_query "$legacy_sql")"
  rt_val="$(ch_query "$rt_sql")"

  if [[ "$legacy_val" == __ERROR__* ]]; then
    record_failure "$label" "LEGACY_QUERY_FAILED"
    return
  fi
  if [[ "$rt_val" == __ERROR__* ]]; then
    record_failure "$label" "RT_QUERY_FAILED"
    return
  fi

  legacy_val="$(normalize_number "$legacy_val")"
  rt_val="$(normalize_number "$rt_val")"

  local status="OK"
  if [[ "$mode" == "count" ]]; then
    if [[ "$legacy_val" != "$rt_val" ]]; then
      status="DIVERGENT"
      if [[ "$legacy_val" != "0" && "$rt_val" == "0" ]]; then
        status="RT_EMPTY"
      fi
      FAILURES=$((FAILURES + 1))
    fi
  else
    local diff over_tolerance legacy_positive rt_zero
    diff="$(awk "BEGIN { l=$legacy_val+0; r=$rt_val+0; base=(l<0?-l:l); if(base<0.01) base=0.01; print ((l-r)<0 ? (r-l) : (l-r)) / base }")"
    over_tolerance="$(awk "BEGIN { print ($diff > $DECIMAL_TOLERANCE) ? 1 : 0 }")"
    legacy_positive="$(awk "BEGIN { print ($legacy_val+0 > 0) ? 1 : 0 }")"
    rt_zero="$(awk "BEGIN { print ($rt_val+0 == 0) ? 1 : 0 }")"
    if [[ "$legacy_positive" == "1" && "$rt_zero" == "1" ]]; then
      status="RT_EMPTY"
      FAILURES=$((FAILURES + 1))
    elif [[ "$over_tolerance" == "1" ]]; then
      status="DIVERGENT(delta=${diff})"
      FAILURES=$((FAILURES + 1))
    fi
  fi

  printf '  %-48s legacy=%-14s rt=%-14s [%s]\n' "$label" "$legacy_val" "$rt_val" "$status"
}

compare_grouped_sum() {
  local label="$1"
  local legacy_sql="$2"
  local rt_sql="$3"

  CHECKS=$((CHECKS + 1))

  local divergence_sql result
  divergence_sql="
    SELECT count()
    FROM (
      SELECT
        coalesce(l.k, r.k) AS k,
        toFloat64(coalesce(l.v, 0)) AS legacy_value,
        toFloat64(coalesce(r.v, 0)) AS rt_value
      FROM ($legacy_sql) AS l
      FULL OUTER JOIN ($rt_sql) AS r ON l.k = r.k
    )
    WHERE if(
      legacy_value = 0 AND rt_value = 0,
      0,
      abs(legacy_value - rt_value) / greatest(abs(legacy_value), 0.01)
    ) > $DECIMAL_TOLERANCE
  "
  result="$(ch_query "$divergence_sql")"
  if [[ "$result" == __ERROR__* ]]; then
    record_failure "$label" "GROUP_QUERY_FAILED"
    return
  fi
  result="$(normalize_number "$result")"
  if (( result > 0 )); then
    record_failure "$label" "DIVERGENT_GROUPS=$result"
    return
  fi
  printf '  %-48s OK\n' "$label"
}

validate_api_realtime() {
  CHECKS=$((CHECKS + 1))
  local result
  if ! result="$(compose_prod exec -T api env \
    USE_REALTIME_MARTS=true \
    REALTIME_MARTS_FALLBACK=false \
    USE_CLICKHOUSE=true \
    ID_EMPRESA="$ID_EMPRESA" \
    python - <<'PY' 2>&1
import os
from datetime import date, timedelta

from app.config import settings

assert settings.use_realtime_marts is True, "USE_REALTIME_MARTS is not effective"
assert settings.realtime_marts_fallback is False, "REALTIME_MARTS_FALLBACK must be false"

from app import repos_analytics

dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=30)
payload = getattr(repos_analytics, "dashboard_kpis")(
    "admin",
    int(os.environ["ID_EMPRESA"]),
    None,
    dt_ini,
    dt_fim,
)
assert isinstance(payload, dict), "dashboard_kpis did not return a dict"
print("API_REALTIME_OK")
PY
)"; then
    record_failure "api.realtime.facade" "FAILED"
    printf '%s\n' "$result" | sed 's/^/    /'
    return
  fi

  if [[ "$result" != *"API_REALTIME_OK"* ]]; then
    record_failure "api.realtime.facade" "FAILED"
    printf '%s\n' "$result" | sed 's/^/    /'
    return
  fi
  printf '  %-48s OK\n' "api.realtime.facade"
}

main() {
  log "=== Realtime Cutover Validation (BLOCKING) ==="
  log "id_empresa=$ID_EMPRESA decimal_tolerance=$DECIMAL_TOLERANCE env_file=$ENV_FILE"
  log ""

  log "ClickHouse connectivity:"
  require_clickhouse_connection || true
  log ""

  log "Required mart_rt tables:"
  local required_tables=(
    torqmind_mart_rt.dashboard_home_rt
    torqmind_mart_rt.sales_daily_rt
    torqmind_mart_rt.sales_hourly_rt
    torqmind_mart_rt.sales_products_rt
    torqmind_mart_rt.sales_groups_rt
    torqmind_mart_rt.payments_by_type_rt
    torqmind_mart_rt.cash_overview_rt
    torqmind_mart_rt.fraud_daily_rt
    torqmind_mart_rt.risk_recent_events_rt
    torqmind_mart_rt.finance_overview_rt
    torqmind_mart_rt.source_freshness
    torqmind_mart_rt.mart_publication_log
  )
  local table
  for table in "${required_tables[@]}"; do
    validate_table_exists "$table" || true
  done
  log ""

  log "Sales daily:"
  compare_metric "sales_daily.rows" \
    "SELECT count() FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_daily.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_daily.qtd_vendas" \
    "SELECT sum(vendas) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "Sales hourly:"
  compare_metric "sales_hourly.rows" \
    "SELECT count() FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_hourly.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_hourly.qtd_vendas" \
    "SELECT sum(vendas) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "Products and groups:"
  compare_metric "sales_products.rows" \
    "SELECT count() FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_products.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_products.qtd" \
    "SELECT sum(qtd) FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd) FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_groups.rows" \
    "SELECT count() FROM torqmind_mart.agg_grupos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_groups_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_groups.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_grupos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_groups_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  log "Payments by type:"
  compare_metric "payments.total" \
    "SELECT sum(total_valor) FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_grouped_sum "payments.grouped_by_label" \
    "SELECT concat(category, '|', label) AS k, sum(total_valor) AS v FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa=$ID_EMPRESA GROUP BY k" \
    "SELECT concat(category, '|', label) AS k, sum(valor_total) AS v FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA GROUP BY k"
  log ""

  log "Risk/Fraud:"
  compare_metric "risk_daily.count" \
    "SELECT sum(eventos_risco_total) FROM torqmind_mart.agg_risco_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_eventos) FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "risk_daily.impacto" \
    "SELECT sum(impacto_estimado_total) FROM torqmind_mart.agg_risco_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(impacto_total) FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "risk_recent_events.rows" \
    "SELECT count() FROM torqmind_mart.risco_eventos_recentes WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.risk_recent_events_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "Finance:"
  compare_metric "finance.count" \
    "SELECT count() FROM torqmind_current.fact_financeiro FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" \
    "SELECT sum(qtd_titulos) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "finance.valor_total" \
    "SELECT sum(coalesce(valor, 0)) FROM torqmind_current.fact_financeiro FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "finance.valor_pago" \
    "SELECT sum(coalesce(valor_pago, 0)) FROM torqmind_current.fact_financeiro FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" \
    "SELECT sum(valor_pago_total) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  log "API realtime facade:"
  validate_api_realtime
  log ""

  log "Mart RT publications:"
  ch_query "SELECT mart_name, max(published_at) AS last, sum(rows_written) FROM torqmind_mart_rt.mart_publication_log GROUP BY mart_name ORDER BY last DESC" || true
  log ""

  log "CDC state:"
  ch_query "SELECT table_name, events_total, last_event_at FROM torqmind_ops.cdc_table_state FINAL ORDER BY table_name" || true
  log ""

  log "============================================"
  log "CHECKS=$CHECKS  FAILURES=$FAILURES"
  if (( FAILURES > 0 )); then
    log "RESULT: FAILED - cutover BLOCKED."
    exit 1
  fi
  log "RESULT: PASSED - all checks within tolerance."
  log "============================================"
}

main "$@"
