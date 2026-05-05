#!/usr/bin/env bash
set -Eeuo pipefail

# Initialize ClickHouse realtime mart tables (torqmind_mart_rt)
# Applies DDLs from sql/clickhouse/streaming/040_* and 041_*
# Requires: ENV_FILE with CLICKHOUSE_USER/CLICKHOUSE_PASSWORD

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"

# Load environment
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

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

apply_sql() {
  local file="$1"
  log "Applying $file ..."
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --multiquery < "$file"
}

main() {
  log "=== Init ClickHouse Mart RT ==="

  local sql_dir="$ROOT_DIR/sql/clickhouse/streaming"
  shopt -s nullglob
  local db_files=("$sql_dir"/040_*.sql)
  local table_files=("$sql_dir"/041_*.sql)
  shopt -u nullglob

  if (( ${#db_files[@]} == 0 )); then
    log "ERROR: Missing required 040_*.sql database DDL in $sql_dir"
    exit 1
  fi
  if (( ${#table_files[@]} == 0 )); then
    log "ERROR: Missing required 041_*.sql mart table DDL in $sql_dir"
    exit 1
  fi

  log "Checking ClickHouse connectivity..."
  local ping
  ping="$(docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=TabSeparated -q "SELECT 1" 2>/dev/null || true)"
  if [[ "${ping//[[:space:]]/}" != "1" ]]; then
    log "ERROR: ClickHouse connection failed for configured CLICKHOUSE_USER"
    exit 1
  fi

  local ddl_files=("${db_files[@]}" "${table_files[@]}")
  local f
  for f in "${ddl_files[@]}"; do
    apply_sql "$f"
  done

  # Verify the mandatory runtime contract, not just a loose table count.
  log "Verifying mart_rt tables..."
  local required_tables=(
    dashboard_home_rt
    sales_daily_rt
    sales_hourly_rt
    sales_products_rt
    sales_groups_rt
    payments_by_type_rt
    cash_overview_rt
    fraud_daily_rt
    risk_recent_events_rt
    finance_overview_rt
    source_freshness
    mart_publication_log
  )
  local missing=0
  local table count
  for table in "${required_tables[@]}"; do
    count="$(docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
      exec -T clickhouse clickhouse-client \
      --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
      --format=TabSeparated -q "SELECT count() FROM system.tables WHERE database = 'torqmind_mart_rt' AND name = '$table'" 2>/dev/null || echo "0")"
    count="${count//[[:space:]]/}"
    if [[ "$count" != "1" ]]; then
      log "ERROR: Missing torqmind_mart_rt.$table after DDL apply"
      missing=$((missing + 1))
    fi
  done

  if (( missing > 0 )); then
    log "ERROR: Mart RT init failed; $missing mandatory table(s) missing"
    exit 1
  fi

  log "=== Mart RT init complete (${#ddl_files[@]} files applied, ${#required_tables[@]} mandatory tables verified) ==="
}

main "$@"
