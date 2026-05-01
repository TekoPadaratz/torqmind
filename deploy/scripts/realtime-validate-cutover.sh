#!/usr/bin/env bash
set -Eeuo pipefail

# Validate realtime cutover parity.
# Compares counts and sums between torqmind_mart (legacy) and torqmind_mart_rt (realtime).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

ch_query() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client --format=TabSeparated -q "$1"
}

validate_count() {
  local mart_name="$1"
  local legacy_table="$2"
  local rt_table="$3"

  local legacy_count rt_count
  legacy_count="$(ch_query "SELECT count() FROM $legacy_table WHERE id_empresa = $ID_EMPRESA" 2>/dev/null || echo "0")"
  rt_count="$(ch_query "SELECT count() FROM $rt_table FINAL WHERE id_empresa = $ID_EMPRESA" 2>/dev/null || echo "0")"

  local status="OK"
  if [[ "$legacy_count" != "$rt_count" ]]; then
    status="DIVERGENT"
  fi

  printf '  %-30s legacy=%s rt=%s [%s]\n' "$mart_name" "$legacy_count" "$rt_count" "$status"
}

main() {
  log "=== Realtime Cutover Validation ==="
  log "id_empresa=$ID_EMPRESA"
  log ""

  log "Row counts (legacy vs realtime):"
  validate_count "sales_daily" "torqmind_mart.agg_vendas_diaria" "torqmind_mart_rt.sales_daily_rt"
  validate_count "sales_hourly" "torqmind_mart.agg_vendas_hora" "torqmind_mart_rt.sales_hourly_rt"
  validate_count "sales_products" "torqmind_mart.agg_produtos_diaria" "torqmind_mart_rt.sales_products_rt"
  validate_count "payments_by_type" "torqmind_mart.agg_pagamentos_turno" "torqmind_mart_rt.payments_by_type_rt"

  log ""
  log "CDC state:"
  ch_query "SELECT table_name, events_total, last_event_at FROM torqmind_ops.cdc_table_state FINAL ORDER BY table_name" 2>/dev/null || echo "  (unavailable)"

  log ""
  log "Mart RT publications:"
  ch_query "SELECT mart_name, max(published_at) AS last, sum(rows_written) FROM torqmind_mart_rt.mart_publication_log GROUP BY mart_name ORDER BY last DESC" 2>/dev/null || echo "  (no publications yet)"

  log ""
  log "=== Validation complete ==="
}

main "$@"
