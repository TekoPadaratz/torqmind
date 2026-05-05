#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# realtime-reset-mart-rt.sh
# Reset ONLY torqmind_mart_rt tables. NEVER touches PostgreSQL STG,
# torqmind_current, torqmind_raw, torqmind_ops, or Redpanda.
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
MODE=""
DRY_RUN=false
YES=false

DDL_040="$ROOT_DIR/sql/clickhouse/streaming/040_mart_rt_database.sql"
DDL_041="$ROOT_DIR/sql/clickhouse/streaming/041_mart_rt_tables.sql"

MART_TABLES=(
  sales_daily_rt sales_hourly_rt sales_products_rt sales_groups_rt
  payments_by_type_rt dashboard_home_rt cash_overview_rt
  fraud_daily_rt risk_recent_events_rt finance_overview_rt
  customers_churn_rt source_freshness mart_publication_log
)

usage() {
  cat <<'EOF'
Usage: ENV_FILE=/etc/torqmind/prod.env [COMPOSE_FILE=docker-compose.prod.yml] \
  ./deploy/scripts/realtime-reset-mart-rt.sh <--truncate | --drop-recreate> [--yes] [--dry-run]

Options:
  --truncate       TRUNCATE TABLE ... SYNC on all mart_rt tables.
  --drop-recreate  DROP + re-CREATE all mart_rt tables from DDL (040/041).
  --yes            Non-interactive mode (skip confirmation prompt).
  --dry-run        Print SQL but do not execute.

Safety:
  - ONLY affects torqmind_mart_rt.
  - NEVER touches PostgreSQL STG, torqmind_current, torqmind_raw, torqmind_ops, Redpanda.
  - Idempotent: safe to re-run.
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --truncate)      MODE="truncate" ;;
    --drop-recreate) MODE="drop-recreate" ;;
    --yes)           YES=true ;;
    --dry-run)       DRY_RUN=true ;;
    --help|-h)       usage 0 ;;
    *)               echo "ERROR: unknown argument: $1" >&2; usage 2 ;;
  esac
  shift
done

if [[ -z "$MODE" ]]; then
  echo "ERROR: specify --truncate or --drop-recreate" >&2
  usage 2
fi

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"

ch_exec() {
  local sql="$1"
  if $DRY_RUN; then
    log "[DRY-RUN] $sql"
    return 0
  fi
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
    clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    -q "$sql"
}

ch_query() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
    clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=TabSeparated -q "$1"
}

show_counts() {
  local label="$1"
  log "=== Table counts ($label) ==="
  for t in "${MART_TABLES[@]}"; do
    local cnt
    cnt="$(ch_query "SELECT count() FROM torqmind_mart_rt.$t" 2>/dev/null || echo "N/A")"
    printf '  %-35s %s\n' "$t" "$cnt"
  done
}

# ---- Confirmation ----
if ! $YES && ! $DRY_RUN; then
  log "WARNING: This will $MODE all tables in torqmind_mart_rt."
  log "SAFETY: PostgreSQL STG, torqmind_current, torqmind_raw, torqmind_ops will NOT be touched."
  read -rp "Continue? [y/N] " confirm
  if [[ "$confirm" != [yY] ]]; then
    log "Aborted."
    exit 0
  fi
fi

log "Starting mart_rt reset: mode=$MODE dry_run=$DRY_RUN"
log "ENV_FILE=$ENV_FILE COMPOSE_FILE=$COMPOSE_FILE"

# Show before counts
show_counts "BEFORE" || true

if [[ "$MODE" == "truncate" ]]; then
  for t in "${MART_TABLES[@]}"; do
    log "TRUNCATE torqmind_mart_rt.$t"
    ch_exec "TRUNCATE TABLE IF EXISTS torqmind_mart_rt.$t SYNC" || true
  done
elif [[ "$MODE" == "drop-recreate" ]]; then
  # Drop individual tables
  for t in "${MART_TABLES[@]}"; do
    log "DROP TABLE torqmind_mart_rt.$t"
    ch_exec "DROP TABLE IF EXISTS torqmind_mart_rt.$t SYNC" || true
  done

  # Re-apply DDL
  if [[ ! -f "$DDL_040" ]]; then
    log "ERROR: DDL file not found: $DDL_040" >&2
    exit 1
  fi
  if [[ ! -f "$DDL_041" ]]; then
    log "ERROR: DDL file not found: $DDL_041" >&2
    exit 1
  fi

  log "Applying DDL: $DDL_040"
  if ! $DRY_RUN; then
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
      clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
      --multiquery < "$DDL_040"
  else
    log "[DRY-RUN] Would apply $DDL_040"
  fi

  log "Applying DDL: $DDL_041"
  if ! $DRY_RUN; then
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
      clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
      --multiquery < "$DDL_041"
  else
    log "[DRY-RUN] Would apply $DDL_041"
  fi
fi

# Verify all required tables exist
log "Verifying required tables..."
MISSING=0
for t in "${MART_TABLES[@]}"; do
  exists="$(ch_query "SELECT count() FROM system.tables WHERE database='torqmind_mart_rt' AND name='$t'" 2>/dev/null || echo "0")"
  exists="${exists//[[:space:]]/}"
  if [[ "$exists" != "1" ]] && ! $DRY_RUN; then
    log "ERROR: Required table torqmind_mart_rt.$t does not exist after DDL!" >&2
    MISSING=$((MISSING + 1))
  fi
done

if (( MISSING > 0 )); then
  log "FAILED: $MISSING required tables missing after reset."
  exit 1
fi

# Show after counts
show_counts "AFTER" || true

log "mart_rt reset complete: mode=$MODE"
