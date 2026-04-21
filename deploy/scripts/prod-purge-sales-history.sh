#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
LOCK_FILE="${LOCK_FILE:-/tmp/torqmind-prod-purge-sales-history.lock}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  exit 1
fi

mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Iseconds) TorqMind purge diário já está em execução. Lock: $LOCK_FILE"
  exit 0
fi

cd "$ROOT_DIR"

args=(
  python
  -m
  app.cli.purge_sales_history
)

if [[ -n "${REF_DATE:-}" ]]; then
  args+=(--ref-date "$REF_DATE")
fi

if [[ -n "${TENANT_ID:-}" ]]; then
  args+=(--tenant-id "$TENANT_ID")
fi

echo "$(date -Iseconds) TorqMind sales history purge starting"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api "${args[@]}"
echo "$(date -Iseconds) TorqMind sales history purge finished"
