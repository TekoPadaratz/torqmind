#!/usr/bin/env bash
set -Eeuo pipefail

# Initialize ClickHouse realtime mart tables (torqmind_mart_rt)
# Applies DDLs from sql/clickhouse/streaming/040_* and 041_*
# Requires: ENV_FILE with CLICKHOUSE_USER/CLICKHOUSE_PASSWORD

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"

# Load environment
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

CH_USER="${CLICKHOUSE_USER:-torqmind}"
CH_PASS="${CLICKHOUSE_PASSWORD:-}"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

apply_sql() {
  local file="$1"
  log "Applying $file ..."
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client \
    --user "$CH_USER" --password "$CH_PASS" \
    --multiquery < "$file"
}

main() {
  log "=== Init ClickHouse Mart RT ==="

  local sql_dir="$ROOT_DIR/sql/clickhouse/streaming"
  local found=0

  for f in "$sql_dir"/040_*.sql "$sql_dir"/041_*.sql; do
    if [[ -f "$f" ]]; then
      apply_sql "$f"
      found=$((found + 1))
    fi
  done

  if (( found == 0 )); then
    log "ERROR: No 040_*.sql or 041_*.sql files found in $sql_dir"
    log "ERROR: Cannot init mart_rt without DDL files. Aborting."
    exit 1
  fi

  # Verify the database and key tables exist
  log "Verifying mart_rt tables..."
  local verify_sql="SELECT count() FROM system.tables WHERE database = 'torqmind_mart_rt'"
  local table_count
  table_count="$(docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client \
    --user "$CH_USER" --password "$CH_PASS" \
    --format=TabSeparated -q "$verify_sql" 2>/dev/null || echo "0")"

  if (( table_count < 10 )); then
    log "ERROR: Expected at least 10 tables in torqmind_mart_rt, found $table_count"
    exit 1
  fi

  log "=== Mart RT init complete ($found files applied, $table_count tables) ==="
}

main "$@"
