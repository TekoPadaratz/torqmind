#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"

cd "$ROOT_DIR"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  exit 1
fi
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" exec -T api python -m app.cli.migrate "$@"
