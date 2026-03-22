#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
LOCK_FILE="${LOCK_FILE:-/tmp/torqmind-prod-etl-incremental.lock}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  exit 1
fi

mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Iseconds) TorqMind incremental ETL já está em execução. Lock: $LOCK_FILE"
  exit 0
fi

cd "$ROOT_DIR"

args=(
  python
  -m
  app.cli.etl_incremental
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

echo "$(date -Iseconds) TorqMind incremental ETL starting"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api "${args[@]}"
echo "$(date -Iseconds) TorqMind incremental ETL finished"
