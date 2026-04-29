#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
LOCK_FILE="${LOCK_FILE:-/tmp/torqmind-prod-etl-pipeline.lock}"
LOCK_DISABLED="${LOCK_DISABLED:-false}"
TRACK="${TRACK:-full}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  exit 1
fi

if [[ "$LOCK_DISABLED" != "true" && "$LOCK_DISABLED" != "1" ]]; then
  mkdir -p "$(dirname "$LOCK_FILE")"
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "$(date -Iseconds) TorqMind incremental ETL já está em execução. Lock: $LOCK_FILE" >&2
    exit 0
  fi
fi

cd "$ROOT_DIR"

args=(
  python
  -m
  app.cli.etl_incremental
  --track
  "$TRACK"
)

if [[ -n "${REF_DATE:-}" ]]; then
  args+=(--ref-date "$REF_DATE")
fi

if [[ -n "${TENANT_ID:-}" ]]; then
  args+=(--tenant-id "$TENANT_ID")
fi

if [[ "${FAIL_FAST:-false}" == "true" ]]; then
  args+=(--fail-fast)
fi

if [[ "${SKIP_BUSY_TENANTS:-false}" == "true" || "${SKIP_BUSY_TENANTS:-0}" == "1" ]]; then
  args+=(--skip-busy-tenants)
fi

emit_json_failure() {
  local error_code="$1"
  local message="$2"
  printf '%s' "$message" | python3 -c '
import json
import sys

track = sys.argv[1]
error_code = sys.argv[2]
message = sys.stdin.read()
print(
    json.dumps(
        {
            "ok": False,
            "track": track,
            "processed": 0,
            "failed": 1,
            "skipped": 0,
            "error": error_code,
            "message": message,
            "items": [],
        },
        ensure_ascii=False,
    )
)
' "$TRACK" "$error_code"
}

api_running="$(
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps --status running --services 2>/dev/null \
    | grep -Fx 'api' || true
)"
if [[ -z "$api_running" ]]; then
  message="Container api não está em execução para o trilho ${TRACK}."
  echo "$message" >&2
  emit_json_failure "api_container_not_running" "$message"
  exit 1
fi

echo "$(date -Iseconds) TorqMind ${TRACK} ETL starting" >&2
stdout_file="$(mktemp)"
stderr_file="$(mktemp)"
trap 'rm -f "$stdout_file" "$stderr_file"' EXIT

if docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api "${args[@]}" >"$stdout_file" 2>"$stderr_file"; then
  cat "$stdout_file"
else
  cat "$stderr_file" >&2
  failure_message="$(tr '\n' ' ' <"$stderr_file" | sed 's/[[:space:]]\\+/ /g; s/^ //; s/ $//')"
  if [[ -z "$failure_message" ]]; then
    failure_message="Falha ao executar o ETL ${TRACK} via docker compose exec."
  fi
  emit_json_failure "docker_exec_failed" "$failure_message"
  exit 1
fi
echo "$(date -Iseconds) TorqMind ${TRACK} ETL finished" >&2
