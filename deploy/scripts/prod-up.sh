#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  echo "Crie /etc/torqmind/prod.env a partir de .env.production.example antes de subir a stack."
  exit 1
fi

cd "$ROOT_DIR"
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" up -d --build
