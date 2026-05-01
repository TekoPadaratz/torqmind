#!/usr/bin/env bash
set -Eeuo pipefail

# Validate realtime cutover parity (BLOQUEANTE).
# Compares counts AND sums between torqmind_mart (legacy) and torqmind_mart_rt (realtime).
# Exit 1 on ANY divergence, missing table, or empty mart_rt when legacy has data.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
TOLERANCE="${TOLERANCE:-0.01}"  # 1% tolerance for decimal comparisons

# Load environment for ClickHouse credentials
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

CH_USER="${CLICKHOUSE_USER:-torqmind}"
CH_PASS="${CLICKHOUSE_PASSWORD:-}"
FAILURES=0
CHECKS=0

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

ch_query() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client \
    --user "$CH_USER" --password "$CH_PASS" \
    --format=TabSeparated -q "$1" 2>/dev/null || echo "__ERROR__"
}

# Check if table exists
table_exists() {
  local full_table="$1"
  local db table
  db="${full_table%%.*}"
  table="${full_table#*.}"
  local result
  result="$(ch_query "SELECT count() FROM system.tables WHERE database='$db' AND name='$table'")"
  [[ "$result" == "1" ]]
}

# Compare a numeric metric between legacy and realtime
compare_metric() {
  local label="$1"
  local legacy_sql="$2"
  local rt_sql="$3"
  local is_count="${4:-false}"

  CHECKS=$((CHECKS + 1))

  local legacy_val rt_val
  legacy_val="$(ch_query "$legacy_sql")"
  rt_val="$(ch_query "$rt_sql")"

  if [[ "$legacy_val" == "__ERROR__" ]]; then
    printf '  %-40s LEGACY_QUERY_FAILED\n' "$label"
    FAILURES=$((FAILURES + 1))
    return
  fi
  if [[ "$rt_val" == "__ERROR__" ]]; then
    printf '  %-40s RT_QUERY_FAILED\n' "$label"
    FAILURES=$((FAILURES + 1))
    return
  fi

  # Trim whitespace
  legacy_val="${legacy_val//[[:space:]]/}"
  rt_val="${rt_val//[[:space:]]/}"

  # Handle empty results
  legacy_val="${legacy_val:-0}"
  rt_val="${rt_val:-0}"

  local status="OK"
  if [[ "$is_count" == "true" ]]; then
    if [[ "$legacy_val" != "$rt_val" ]]; then
      status="DIVERGENT"
      FAILURES=$((FAILURES + 1))
    fi
    # Also flag if legacy has data but RT is empty
    if [[ "$legacy_val" != "0" && "$rt_val" == "0" ]]; then
      status="RT_EMPTY"
      # already counted above
    fi
  else
    # Decimal comparison with tolerance
    local diff
    diff="$(awk "BEGIN { l=$legacy_val+0; r=$rt_val+0; if(l==0 && r==0) print 0; else if(l==0) print 1; else { d=(l-r)/l; print (d<0?-d:d) } }")"
    local over_tolerance
    over_tolerance="$(awk "BEGIN { print ($diff > $TOLERANCE) ? 1 : 0 }")"
    if [[ "$over_tolerance" == "1" ]]; then
      status="DIVERGENT(delta=${diff})"
      FAILURES=$((FAILURES + 1))
    fi
    # If legacy > 0 and RT is 0
    local legacy_positive
    legacy_positive="$(awk "BEGIN { print ($legacy_val+0 > 0) ? 1 : 0 }")"
    local rt_zero
    rt_zero="$(awk "BEGIN { print ($rt_val+0 == 0) ? 1 : 0 }")"
    if [[ "$legacy_positive" == "1" && "$rt_zero" == "1" ]]; then
      status="RT_EMPTY"
      FAILURES=$((FAILURES + 1))
    fi
  fi

  printf '  %-40s legacy=%-12s rt=%-12s [%s]\n' "$label" "$legacy_val" "$rt_val" "$status"
}

validate_table_exists() {
  local rt_table="$1"
  local label="$2"
  CHECKS=$((CHECKS + 1))
  if ! table_exists "$rt_table"; then
    printf '  %-40s TABLE_MISSING\n' "$label"
    FAILURES=$((FAILURES + 1))
    return 1
  fi
  return 0
}

main() {
  log "=== Realtime Cutover Validation (BLOQUEANTE) ==="
  log "id_empresa=$ID_EMPRESA tolerance=$TOLERANCE"
  log ""

  # 1. Check mart_rt tables exist
  log "Checking mart_rt tables exist..."
  local required_tables=(
    "torqmind_mart_rt.sales_daily_rt"
    "torqmind_mart_rt.sales_hourly_rt"
    "torqmind_mart_rt.sales_products_rt"
    "torqmind_mart_rt.sales_groups_rt"
    "torqmind_mart_rt.payments_by_type_rt"
    "torqmind_mart_rt.cash_overview_rt"
    "torqmind_mart_rt.fraud_daily_rt"
    "torqmind_mart_rt.risk_recent_events_rt"
    "torqmind_mart_rt.finance_overview_rt"
    "torqmind_mart_rt.dashboard_home_rt"
    "torqmind_mart_rt.source_freshness"
    "torqmind_mart_rt.mart_publication_log"
  )
  for t in "${required_tables[@]}"; do
    validate_table_exists "$t" "$t" || true
  done
  log ""

  # 2. Sales daily: count + sum faturamento + qtd_vendas
  log "Sales Daily:"
  compare_metric "sales_daily.count" \
    "SELECT count() FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  compare_metric "sales_daily.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_daily.qtd_vendas" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  log ""

  # 3. Sales hourly: count + sum faturamento
  log "Sales Hourly:"
  compare_metric "sales_hourly.count" \
    "SELECT count() FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  compare_metric "sales_hourly.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  # 4. Sales products: count + sum faturamento/qtd
  log "Sales Products:"
  compare_metric "sales_products.count" \
    "SELECT count() FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  compare_metric "sales_products.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  # 5. Payments: sum valor_total
  log "Payments:"
  compare_metric "payments.valor_total" \
    "SELECT sum(valor_total) FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "payments.count" \
    "SELECT count() FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  log ""

  # 6. Fraud/Risk: count
  log "Fraud/Risk:"
  compare_metric "fraud_daily.count" \
    "SELECT count() FROM torqmind_mart.agg_risco_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  compare_metric "risk_events.count" \
    "SELECT count() FROM torqmind_mart.risk_events_latest WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.risk_recent_events_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "true"
  log ""

  # 7. Finance: sum valor
  log "Finance:"
  compare_metric "finance.valor_total" \
    "SELECT sum(valor_total) FROM torqmind_mart.finance_aging_daily WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  # 8. Mart publication check
  log "Mart RT publications:"
  ch_query "SELECT mart_name, max(published_at) AS last, sum(rows_written) FROM torqmind_mart_rt.mart_publication_log GROUP BY mart_name ORDER BY last DESC" || echo "  (no publications)"
  log ""

  # 9. CDC state
  log "CDC state:"
  ch_query "SELECT table_name, events_total, last_event_at FROM torqmind_ops.cdc_table_state FINAL ORDER BY table_name" || echo "  (unavailable)"
  log ""

  # Final result
  log "============================================"
  log "CHECKS=$CHECKS  FAILURES=$FAILURES"
  if (( FAILURES > 0 )); then
    log "RESULT: FAILED — $FAILURES check(s) divergent or missing."
    log "Cutover BLOCKED. Fix data or investigate before activating realtime."
    exit 1
  fi
  log "RESULT: PASSED — All checks within tolerance."
  log "============================================"
}

main "$@"
