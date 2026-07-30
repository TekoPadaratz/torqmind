#!/usr/bin/env bash
# Recria api/web/nginx de PRODUÇÃO com isolamento obrigatório.
# Uso:
#   ./deploy/scripts/prod-app-up.sh
#   ./deploy/scripts/prod-app-up.sh api
#   SERVICES="api" ./deploy/scripts/prod-app-up.sh
#
# NUNCA passe homolog.app.env aqui — o script recusa.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
SERVICES="${SERVICES:-${*:-api web nginx}}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

export ROOT_DIR
tm_require_prod_runtime_env "$ENV_FILE"

cd "$ROOT_DIR"
echo "=== TorqMind PROD app up ==="
echo "ENV_FILE=$ENV_FILE"
echo "SERVICES=$SERVICES"
echo "Compose: docker-compose.app.yml -p torqmind"

# shellcheck disable=SC2086
tm_compose_prod_app up -d --force-recreate --no-deps $SERVICES

echo "=== Guard check (container env) ==="
for i in $(seq 1 30); do
  st="$(docker inspect -f '{{.State.Health.Status}}' torqmind-api 2>/dev/null || echo starting)"
  if [[ "$st" == "healthy" ]]; then
    break
  fi
  if [[ "$st" == "unhealthy" ]]; then
    echo "API unhealthy" >&2
    docker logs torqmind-api --tail 40 >&2 || true
    exit 1
  fi
  sleep 2
done

docker exec torqmind-api python - <<'PY'
import os, sys
app_env = os.environ.get("APP_ENV")
stack = os.environ.get("TORQMIND_STACK")
url = os.environ.get("DATABASE_URL") or ""
print(f"APP_ENV={app_env} TORQMIND_STACK={stack}")
print(f"DATABASE_URL_db={url.rsplit('/',1)[-1].split('?',1)[0]}")
assert app_env == "prod", app_env
assert stack == "prod", stack
assert "homolog" not in url.lower(), url
print("GUARD_OK")
PY

echo "=== Health ==="
curl -fsS -m 10 http://127.0.0.1/api/health
echo
echo "PROD app up OK"
