#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

cd "$ROOT_DIR"
tm_require_prod_seed_env "$ENV_FILE"

COMPOSE_FILE="$COMPOSE_FILE" ./deploy/scripts/prod-migrate.sh
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api env SEED_MODE=master-only python -m app.cli.seed
