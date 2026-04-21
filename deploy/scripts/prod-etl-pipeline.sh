#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
LOCK_FILE="${LOCK_FILE:-/tmp/torqmind-prod-etl-pipeline.lock}"
STATE_DIR="${STATE_DIR:-/var/tmp/torqmind-etl}"
RISK_INTERVAL_MINUTES="${RISK_INTERVAL_MINUTES:-30}"
SKIP_BUSY_TENANTS="${SKIP_BUSY_TENANTS:-true}"
RISK_SKIP_BUSY_TENANTS="${RISK_SKIP_BUSY_TENANTS:-$SKIP_BUSY_TENANTS}"
RISK_STATE_FILE="$STATE_DIR/risk-last-success.epoch"
INCREMENTAL_SCRIPT="${INCREMENTAL_SCRIPT:-$ROOT_DIR/deploy/scripts/prod-etl-incremental.sh}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOCK_FILE")" "$STATE_DIR"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Iseconds) TorqMind pipeline ETL já está em execução. Lock: $LOCK_FILE" >&2
  exit 0
fi

run_track() {
  local track="$1"
  local skip_busy="$2"
  local summary

  summary="$(
    LOCK_DISABLED=true \
    LOCK_FILE="$LOCK_FILE" \
    TRACK="$track" \
    SKIP_BUSY_TENANTS="$skip_busy" \
    ENV_FILE="$ENV_FILE" \
    COMPOSE_FILE="$COMPOSE_FILE" \
    "$INCREMENTAL_SCRIPT"
  )"
  printf '%s\n' "$summary"
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

if risk_is_due; then
  risk_summary="$(run_track risk "$RISK_SKIP_BUSY_TENANTS")"
  printf '%s\n' "$risk_summary"
  if [[ "$(risk_state_should_advance "$risk_summary")" == "true" ]]; then
    date +%s >"$RISK_STATE_FILE"
  else
    echo "$(date -Iseconds) TorqMind risk track terminou com falha/skip; janela não avançada." >&2
  fi
else
  echo "$(date -Iseconds) TorqMind risk track not due yet." >&2
fi

echo "$(date -Iseconds) TorqMind ETL pipeline finished" >&2
