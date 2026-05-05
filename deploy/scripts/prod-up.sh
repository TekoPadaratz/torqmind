#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
SKIP_MIGRATE="${SKIP_MIGRATE:-false}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

tm_require_prod_runtime_env "$ENV_FILE"

cd "$ROOT_DIR"
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" up -d --build

if [[ "$SKIP_MIGRATE" != "true" && "$SKIP_MIGRATE" != "1" ]]; then
  "$ROOT_DIR/deploy/scripts/prod-migrate.sh"
fi
