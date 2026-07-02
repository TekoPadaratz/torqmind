#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
RUN_INTEGRATION="${RUN_INTEGRATION:-false}"
SERVICE="${SERVICE:-api}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
PYTEST_ARGS=("$@")
PYTEST_TARGETS=(".")
if ((${#PYTEST_ARGS[@]} > 0)); then
  PYTEST_TARGETS=("${PYTEST_ARGS[@]}")
fi

cd "$ROOT_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2
  exit 1
fi

docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build "$POSTGRES_SERVICE" "$SERVICE"

docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T "$SERVICE" python -m app.cli.migrate

docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T "$SERVICE" sh -lc \
  'python -m pip show pytest >/dev/null 2>&1 || python -m pip install --no-cache-dir pytest'

if [[ "$RUN_INTEGRATION" == "true" || "$RUN_INTEGRATION" == "1" ]]; then
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T "$SERVICE" \
    env REFRESH_LEGACY_PG_MARTS=true python -m pytest "${PYTEST_TARGETS[@]}" -q --run-db-integration
else
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T "$SERVICE" \
    python -m pytest "${PYTEST_TARGETS[@]}" -q
fi
