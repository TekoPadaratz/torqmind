#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
TENANT_ID="${TENANT_ID:?missing TENANT_ID}"
DATE="${DATE:?missing DATE}"
BRANCH_ID="${BRANCH_ID:-}"
GROUP_NAME="${GROUP_NAME:-${GROUP:-COMBUSTIVEIS}}"
DETAIL_LIMIT="${DETAIL_LIMIT:-10}"

cd "$ROOT_DIR"

args=(
  -f docker-compose.prod.yml
  --env-file "$ENV_FILE"
  exec
  -T
  api
  python
  -m
  app.cli.reconcile_sales
  --tenant-id "$TENANT_ID"
  --date "$DATE"
  --group "$GROUP_NAME"
  --detail-limit "$DETAIL_LIMIT"
)

if [[ -n "$BRANCH_ID" ]]; then
  args+=(--branch-id "$BRANCH_ID")
fi

docker compose "${args[@]}"
