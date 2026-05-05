#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

cd "$ROOT_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  exit 1
fi

args=(
  python
  -m
  app.cli.platform_billing
  daily
  --as-of
  "${AS_OF:-}"
  --competence-month
  "${COMPETENCE_MONTH:-}"
  --months-ahead
  "${MONTHS_AHEAD:-0}"
)

if [[ -n "${TENANT_ID:-}" ]]; then
  args+=(--tenant-id "$TENANT_ID")
fi

docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api "${args[@]}"
