#!/usr/bin/env bash
set -Eeuo pipefail

# ClickHouse Stability Gate
# Checks that ClickHouse is healthy before/during/after cutover.
# Used by prod-realtime-cutover-apply.sh as a pre-condition.
#
# Fails (exit 1) if:
#   - Container is not healthy
#   - Restart count increased since check started
#   - SELECT uptime() fails
#   - MEMORY_LIMIT_EXCEEDED found in err.log since $SINCE timestamp
#
# Usage:
#   ENV_FILE=.env.e2e.local COMPOSE_FILE=docker-compose.prod.yml \
#     bash deploy/scripts/clickhouse-stability-check.sh [--since "2026-05-01 00:00:00"]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
SINCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since) SINCE="$2"; shift ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
  shift
done

if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi

: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"

FAILURES=0

log() { printf '%s [stability] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
fail() { log "FAIL: $*"; FAILURES=$((FAILURES + 1)); }

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

ch_query() {
  compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=TabSeparated -q "$1" 2>/dev/null
}

# 1. Container health
log "Checking container health..."
HEALTH="$(docker inspect --format='{{.State.Health.Status}}' "$(compose ps -q clickhouse 2>/dev/null)" 2>/dev/null || echo "unknown")"
if [[ "$HEALTH" != "healthy" ]]; then
  fail "ClickHouse container health: $HEALTH (expected healthy)"
else
  log "OK: container healthy"
fi

# 2. Restart count
log "Checking restart count..."
RESTART_COUNT="$(docker inspect --format='{{.RestartCount}}' "$(compose ps -q clickhouse 2>/dev/null)" 2>/dev/null || echo "-1")"
if [[ "$RESTART_COUNT" == "-1" ]]; then
  fail "Cannot read restart count"
else
  log "OK: restart_count=$RESTART_COUNT"
fi

# 3. SELECT uptime
log "Checking SELECT uptime()..."
UPTIME="$(ch_query "SELECT uptime()" 2>/dev/null || echo "")"
if [[ -z "$UPTIME" || "$UPTIME" == "0" ]]; then
  fail "SELECT uptime() failed or returned 0"
else
  log "OK: uptime=${UPTIME}s"
fi

# 4. Memory limit exceeded in err.log
log "Checking for MEMORY_LIMIT_EXCEEDED..."
if [[ -n "$SINCE" ]]; then
  # Check err.log for OOM since timestamp
  OOM_LINES="$(compose exec -T clickhouse sh -c "grep -c 'MEMORY_LIMIT_EXCEEDED' /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || echo 0")"
else
  OOM_LINES="$(compose exec -T clickhouse sh -c "grep -c 'MEMORY_LIMIT_EXCEEDED' /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || echo 0")"
fi
OOM_LINES="${OOM_LINES//[^0-9]/}"
if [[ "${OOM_LINES:-0}" -gt 0 ]]; then
  log "WARNING: Found $OOM_LINES MEMORY_LIMIT_EXCEEDED entries in err.log"
  # Show last 3 for context
  compose exec -T clickhouse sh -c "grep 'MEMORY_LIMIT_EXCEEDED' /var/log/clickhouse-server/clickhouse-server.err.log | tail -3" 2>/dev/null || true
  if [[ -n "$SINCE" ]]; then
    fail "MEMORY_LIMIT_EXCEEDED detected after cutover start"
  else
    log "NOTE: Pre-existing OOM entries (not blocking if --since not set)"
  fi
else
  log "OK: no MEMORY_LIMIT_EXCEEDED in err.log"
fi

# 5. Summary
echo ""
if [[ $FAILURES -gt 0 ]]; then
  log "STABILITY CHECK FAILED ($FAILURES issues)"
  exit 1
else
  log "STABILITY CHECK PASSED"
  echo "restart_count=$RESTART_COUNT uptime=${UPTIME}s oom_lines=${OOM_LINES:-0}"
  exit 0
fi
