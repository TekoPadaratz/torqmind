#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# realtime-clickhouse-backfill-preflight.sh
# Pre-flight checks before mart rebuild: disk, health, parts, mutations, OOM.
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
MIN_DISK_FREE_GB="${MIN_DISK_FREE_GB:-5}"
MAX_PENDING_MUTATIONS="${MAX_PENDING_MUTATIONS:-50}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

ch_query() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
    clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=TabSeparated -q "$1" 2>/dev/null || echo "__ERROR__"
}

ch_query_pretty() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T clickhouse \
    clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    -q "$1" 2>/dev/null || echo "(query failed)"
}

FAILURES=0

check() {
  local label="$1" ok="$2" detail="${3:-}"
  if [[ "$ok" == "1" ]]; then
    printf '  %-45s OK %s\n' "$label" "$detail"
  else
    printf '  %-45s FAIL %s\n' "$label" "$detail"
    FAILURES=$((FAILURES + 1))
  fi
}

log "=== ClickHouse Backfill Pre-flight ==="
log "COMPOSE_FILE=$COMPOSE_FILE ENV_FILE=$ENV_FILE"
log ""

# 1. ClickHouse connectivity
result="$(ch_query "SELECT 1")"
result="${result//[[:space:]]/}"
check "clickhouse.connection" "$( [[ "$result" == "1" ]] && echo 1 || echo 0 )"

# 2. Disk free space
log ""
log "Disk space:"
disk_info="$(ch_query "SELECT
  path,
  formatReadableSize(free_space) AS free,
  formatReadableSize(total_space) AS total,
  round(free_space / greatest(total_space, 1) * 100, 1) AS pct_free,
  free_space
FROM system.disks")"
echo "$disk_info" | while IFS=$'\t' read -r path free total pct raw_free; do
  printf '  %-30s free=%s total=%s (%s%%)\n' "$path" "$free" "$total" "$pct"
done
free_bytes="$(echo "$disk_info" | head -1 | cut -f5)"
free_bytes="${free_bytes//[[:space:]]/}"
if [[ "$free_bytes" != "__ERROR__" && -n "$free_bytes" ]]; then
  free_gb=$((free_bytes / 1073741824))
  check "disk.free >= ${MIN_DISK_FREE_GB}GB" "$( (( free_gb >= MIN_DISK_FREE_GB )) && echo 1 || echo 0 )" "${free_gb}GB free"
else
  check "disk.free" "0" "could not read"
fi

# 3. Pending mutations
log ""
log "Pending mutations:"
mutations="$(ch_query "SELECT count() FROM system.mutations WHERE is_done = 0")"
mutations="${mutations//[[:space:]]/}"
if [[ "$mutations" != "__ERROR__" ]]; then
  check "mutations.pending <= $MAX_PENDING_MUTATIONS" \
    "$( (( mutations <= MAX_PENDING_MUTATIONS )) && echo 1 || echo 0 )" \
    "${mutations} pending"
  if (( mutations > 0 )); then
    ch_query_pretty "SELECT database, table, mutation_id, command, create_time
      FROM system.mutations WHERE is_done = 0
      ORDER BY create_time DESC LIMIT 10
      FORMAT PrettyCompact" || true
  fi
else
  check "mutations.pending" "0" "could not query"
fi

# 4. Parts count per database
log ""
log "Parts by database:"
ch_query_pretty "SELECT database, sum(rows) AS total_rows, count() AS parts,
    sum(bytes_on_disk) AS bytes_on_disk,
    formatReadableSize(sum(bytes_on_disk)) AS readable_size
  FROM system.parts
  WHERE database IN ('torqmind_current', 'torqmind_mart_rt', 'torqmind_raw', 'torqmind_ops')
    AND active = 1
  GROUP BY database
  ORDER BY database
  FORMAT PrettyCompact" || true

# 5. Mart_rt table parts detail
log ""
log "torqmind_mart_rt parts per table:"
ch_query_pretty "SELECT table, sum(rows) AS rows, count() AS parts,
    formatReadableSize(sum(bytes_on_disk)) AS size
  FROM system.parts
  WHERE database = 'torqmind_mart_rt' AND active = 1
  GROUP BY table ORDER BY table
  FORMAT PrettyCompact" || true

# 6. Recent OOM / memory errors
log ""
log "Recent memory errors (last 24h):"
oom_count="$(ch_query "SELECT count()
  FROM system.query_log
  WHERE type = 'ExceptionWhileProcessing'
    AND exception LIKE '%MEMORY_LIMIT_EXCEEDED%'
    AND event_time > now() - INTERVAL 24 HOUR")"
oom_count="${oom_count//[[:space:]]/}"
if [[ "$oom_count" != "__ERROR__" ]]; then
  check "memory.no_recent_OOM" "$( [[ "$oom_count" == "0" ]] && echo 1 || echo 0 )" "${oom_count} OOM errors in 24h"
else
  check "memory.no_recent_OOM" "1" "query_log not available (ok in some configs)"
fi

# 7. ClickHouse uptime
log ""
uptime_s="$(ch_query "SELECT uptime()")"
uptime_s="${uptime_s//[[:space:]]/}"
if [[ "$uptime_s" != "__ERROR__" && -n "$uptime_s" ]]; then
  uptime_h=$((uptime_s / 3600))
  check "clickhouse.uptime" "$( (( uptime_s > 60 )) && echo 1 || echo 0 )" "${uptime_h}h (${uptime_s}s)"
else
  check "clickhouse.uptime" "0" "could not read"
fi

# 8. Table row counts
log ""
log "Current mart_rt table counts:"
for t in sales_daily_rt sales_hourly_rt sales_products_rt sales_groups_rt \
         payments_by_type_rt dashboard_home_rt cash_overview_rt \
         fraud_daily_rt risk_recent_events_rt finance_overview_rt \
         source_freshness mart_publication_log; do
  cnt="$(ch_query "SELECT count() FROM torqmind_mart_rt.$t" 2>/dev/null || echo "N/A")"
  cnt="${cnt//[[:space:]]/}"
  printf '  %-35s %s\n' "$t" "$cnt"
done

# 9. Performance recommendation
log ""
log "=== Recommendations ==="
if [[ "$free_bytes" != "__ERROR__" && -n "$free_bytes" ]]; then
  free_gb_val=$((free_bytes / 1073741824))
  if (( free_gb_val < 10 )); then
    log "  WARNING: Low disk space (${free_gb_val}GB). Consider cleanup before rebuild."
    log "  Recommended: --drop-recreate (avoids mutation overhead)"
  fi
  if (( free_gb_val >= 20 )); then
    log "  Disk OK. Suggested batch_size=14, max_threads=4"
  else
    log "  Limited disk. Suggested batch_size=7, max_threads=2"
  fi
fi

log ""
log "=== Pre-flight Summary ==="
if (( FAILURES > 0 )); then
  log "RESULT: FAIL ($FAILURES issues)"
  log "Fix issues before proceeding with mart rebuild."
  exit 1
fi
log "RESULT: PASS - safe to proceed with mart rebuild."
exit 0
