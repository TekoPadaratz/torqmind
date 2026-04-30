#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"
LOCK_FILE="${LOCK_FILE:-/tmp/torqmind-prod-etl-pipeline.lock}"
STATE_DIR="${STATE_DIR:-/var/tmp/torqmind-etl}"
RISK_INTERVAL_MINUTES="${RISK_INTERVAL_MINUTES:-30}"
PIPELINE_TIMEOUT_SECONDS="${PIPELINE_TIMEOUT_SECONDS:-90}"
PIPELINE_WARN_SECONDS="${PIPELINE_WARN_SECONDS:-30}"
PIPELINE_TRACK_LOG_TAIL_LINES="${PIPELINE_TRACK_LOG_TAIL_LINES:-120}"
PIPELINE_TRACK_LOG_MAX_BYTES="${PIPELINE_TRACK_LOG_MAX_BYTES:-12000}"
SKIP_BUSY_TENANTS="${SKIP_BUSY_TENANTS:-true}"
RISK_SKIP_BUSY_TENANTS="${RISK_SKIP_BUSY_TENANTS:-$SKIP_BUSY_TENANTS}"
RISK_STATE_FILE="$STATE_DIR/risk-last-success.epoch"
INCREMENTAL_SCRIPT="${INCREMENTAL_SCRIPT:-$ROOT_DIR/deploy/scripts/prod-etl-incremental.sh}"
CLICKHOUSE_SYNC_SCRIPT="${CLICKHOUSE_SYNC_SCRIPT:-$ROOT_DIR/deploy/scripts/prod-clickhouse-sync-dw.sh}"
CLICKHOUSE_REFRESH_MARTS_SCRIPT="${CLICKHOUSE_REFRESH_MARTS_SCRIPT:-$ROOT_DIR/deploy/scripts/prod-clickhouse-refresh-marts.sh}"
CLICKHOUSE_INCREMENTAL_ENABLED="${CLICKHOUSE_INCREMENTAL_ENABLED:-true}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOCK_FILE")" "$STATE_DIR"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  lock_age="unknown"
  if [[ -f "$LOCK_FILE" ]]; then
    lock_mtime="$(stat -c %Y "$LOCK_FILE" 2>/dev/null || echo 0)"
    now_epoch="$(date +%s)"
    if [[ "$lock_mtime" =~ ^[0-9]+$ && "$lock_mtime" -gt 0 ]]; then
      lock_age="$((now_epoch - lock_mtime))s"
    fi
  fi
  echo "$(date -Iseconds) TorqMind ETL pipeline skip: execução anterior ainda está ativa. Lock: $LOCK_FILE age=${lock_age}" >&2
  exit 0
fi

run_track() {
  local track="$1"
  local skip_busy="$2"
  local summary
  local status
  local start
  local elapsed
  local stdout_file
  local stderr_file

  start="$(date +%s)"
  stdout_file="$(mktemp)"
  stderr_file="$(mktemp)"
  echo "$(date -Iseconds) TorqMind ${track} ETL starting" >&2
  if timeout "${PIPELINE_TIMEOUT_SECONDS}s" \
    env \
    LOCK_DISABLED=true \
    LOCK_FILE="$LOCK_FILE" \
    TRACK="$track" \
    SKIP_BUSY_TENANTS="$skip_busy" \
    ENV_FILE="$ENV_FILE" \
    COMPOSE_FILE="$COMPOSE_FILE" \
    "$INCREMENTAL_SCRIPT" >"$stdout_file" 2>"$stderr_file"; then
    status=0
  else
    status=$?
  fi
  if [[ -s "$stderr_file" ]]; then
    tail -n "$PIPELINE_TRACK_LOG_TAIL_LINES" "$stderr_file" | tail -c "$PIPELINE_TRACK_LOG_MAX_BYTES" >&2
  fi
  summary="$(
    python3 - "$track" "$stdout_file" <<'PY'
import json
import sys

track = sys.argv[1]
path = sys.argv[2]
summary = None
try:
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("track") == track and "ok" in payload:
                summary = payload
except OSError:
    summary = None
if summary is None:
    summary = {
        "ok": False,
        "track": track,
        "failed": 1,
        "skipped": 0,
        "items": [],
        "error": "missing track summary",
    }
print(json.dumps(summary, ensure_ascii=False))
PY
  )"
  rm -f "$stdout_file" "$stderr_file"
  elapsed=$(( $(date +%s) - start ))
  if (( elapsed > PIPELINE_WARN_SECONDS )); then
    echo "$(date -Iseconds) WARN TorqMind ${track} ETL took ${elapsed}s (limit ${PIPELINE_WARN_SECONDS}s)" >&2
  else
    echo "$(date -Iseconds) TorqMind ${track} ETL finished in ${elapsed}s" >&2
  fi
  if (( status == 124 )); then
    printf '{"ok":false,"track":"%s","failed":1,"skipped":0,"items":[],"error":"timeout after %ss"}\n' "$track" "$PIPELINE_TIMEOUT_SECONDS"
    return 0
  fi
  if (( status != 0 )); then
    printf '{"ok":false,"track":"%s","failed":1,"skipped":0,"items":[],"error":"exit status %s"}\n' "$track" "$status"
    return 0
  fi
  printf '%s\n' "$summary"
}

run_with_timeout() {
  local label="$1"
  shift
  local start elapsed status
  start="$(date +%s)"
  echo "$(date -Iseconds) ${label} starting" >&2
  if timeout "${PIPELINE_TIMEOUT_SECONDS}s" "$@"; then
    status=0
  else
    status=$?
  fi
  elapsed=$(( $(date +%s) - start ))
  if (( elapsed > PIPELINE_WARN_SECONDS )); then
    echo "$(date -Iseconds) WARN ${label} took ${elapsed}s (limit ${PIPELINE_WARN_SECONDS}s)" >&2
  else
    echo "$(date -Iseconds) ${label} finished in ${elapsed}s" >&2
  fi
  return "$status"
}

track_succeeded() {
  python3 - "$1" <<'PY'
import json
import sys

try:
    payload = json.loads(sys.argv[1])
except json.JSONDecodeError:
    print("false")
    raise SystemExit(0)
print("true" if payload.get("ok") and int(payload.get("failed") or 0) == 0 else "false")
PY
}

track_has_changes() {
  python3 - "$1" <<'PY'
import json
import sys

try:
    payload = json.loads(sys.argv[1])
except json.JSONDecodeError:
    print("true")
    raise SystemExit(0)
for item in payload.get("items") or []:
    domains = item.get("phase_domains") or {}
    if any(bool(value) for value in domains.values()):
        print("true")
        raise SystemExit(0)
    clock = item.get("clock_meta") or {}
    if any(bool(value) for value in clock.values()):
        print("true")
        raise SystemExit(0)
print("false")
PY
}

is_clickhouse_incremental_enabled() {
  [[ "$CLICKHOUSE_INCREMENTAL_ENABLED" == "true" || "$CLICKHOUSE_INCREMENTAL_ENABLED" == "1" ]]
}

run_clickhouse_incremental_publication() {
  local stage_label="$1"
  if ! is_clickhouse_incremental_enabled; then
    echo "$(date -Iseconds) ClickHouse incremental publication disabled (${stage_label})." >&2
    return 0
  fi
  run_with_timeout "ClickHouse DW incremental sync (${stage_label})" \
    env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" ALLOW_INSECURE_ENV="$ALLOW_INSECURE_ENV" MODE=incremental "$CLICKHOUSE_SYNC_SCRIPT"
  run_with_timeout "ClickHouse mart incremental refresh (${stage_label})" \
    env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" ALLOW_INSECURE_ENV="$ALLOW_INSECURE_ENV" MODE=incremental "$CLICKHOUSE_REFRESH_MARTS_SCRIPT"
}

risk_is_due() {
  if [[ "${FORCE_RISK:-false}" == "true" || "${FORCE_RISK:-0}" == "1" ]]; then
    return 0
  fi
  if [[ ! "$RISK_INTERVAL_MINUTES" =~ ^-?[0-9]+$ ]]; then
    echo "RISK_INTERVAL_MINUTES inválido: $RISK_INTERVAL_MINUTES" >&2
    return 1
  fi
  if (( RISK_INTERVAL_MINUTES <= 0 )); then
    return 0
  fi
  if [[ ! -f "$RISK_STATE_FILE" ]]; then
    return 0
  fi

  local last_epoch
  local now_epoch
  last_epoch="$(cat "$RISK_STATE_FILE" 2>/dev/null || echo 0)"
  now_epoch="$(date +%s)"

  if [[ ! "$last_epoch" =~ ^[0-9]+$ ]]; then
    return 0
  fi

  (( now_epoch - last_epoch >= RISK_INTERVAL_MINUTES * 60 ))
}

risk_state_should_advance() {
  python3 - "$1" <<'PY'
import json
import sys

raw = sys.argv[1]
try:
    payload = json.loads(raw)
except json.JSONDecodeError:
    print("false")
    raise SystemExit(0)
ok = bool(payload.get("ok"))
failed = int(payload.get("failed") or 0)
skipped = int(payload.get("skipped") or 0)
print("true" if ok and failed == 0 and skipped == 0 else "false")
PY
}

echo "$(date -Iseconds) TorqMind ETL pipeline starting" >&2
operational_summary="$(run_track operational "$SKIP_BUSY_TENANTS")"
printf '%s\n' "$operational_summary"

if [[ "$(track_succeeded "$operational_summary")" == "true" ]]; then
  if [[ "$(track_has_changes "$operational_summary")" == "true" ]]; then
    echo "$(date -Iseconds) Operational track reported changes; publishing ClickHouse incremental." >&2
  else
    echo "$(date -Iseconds) Operational track did not report changes; validating DW deltas with ClickHouse incremental sync." >&2
  fi
  run_clickhouse_incremental_publication "after-operational"
else
  echo "$(date -Iseconds) ClickHouse incremental publication skipped after operational track: operational failed." >&2
fi

if risk_is_due; then
  echo "$(date -Iseconds) TorqMind risk track due." >&2
  risk_summary="$(run_track risk "$RISK_SKIP_BUSY_TENANTS")"
  printf '%s\n' "$risk_summary"
  if [[ "$(track_succeeded "$risk_summary")" == "true" ]]; then
    run_clickhouse_incremental_publication "after-risk"
  fi
  if [[ "$(risk_state_should_advance "$risk_summary")" == "true" ]]; then
    date +%s >"$RISK_STATE_FILE"
  else
    echo "$(date -Iseconds) TorqMind risk track terminou com falha/skip; janela não avançada." >&2
  fi
else
  echo "$(date -Iseconds) TorqMind risk track not due yet." >&2
fi

echo "$(date -Iseconds) TorqMind ETL pipeline finished" >&2
