#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"

cd "$ROOT_DIR"
./deploy/scripts/prod-migrate.sh
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" exec -T api env SEED_MODE=master-only python -m app.cli.seed
