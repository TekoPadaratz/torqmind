#!/usr/bin/env bash
set -Eeuo pipefail

# Initialize ClickHouse realtime mart tables (torqmind_mart_rt)
# Applies DDLs from sql/clickhouse/streaming/040_* and 041_*

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

apply_sql() {
  local file="$1"
  log "Applying $file ..."
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" \
    exec -T clickhouse clickhouse-client --multiquery < "$file"
}

main() {
  log "=== Init ClickHouse Mart RT ==="

  local sql_dir="$ROOT_DIR/sql/clickhouse/streaming"
  for f in "$sql_dir"/040_*.sql "$sql_dir"/041_*.sql; do
    if [[ -f "$f" ]]; then
      apply_sql "$f"
    fi
  done

  log "=== Mart RT init complete ==="
}

main "$@"
