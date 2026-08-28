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

# Recreate de api/web troca IP no Docker; nginx sem reload → 502 até restart.
needs_nginx_refresh=false
for s in $SERVICES; do
  if [[ "$s" == "api" || "$s" == "web" ]]; then
    needs_nginx_refresh=true
    break
  fi
done
if [[ "$needs_nginx_refresh" == true ]]; then
  echo "=== Refresh nginx (upstream DNS após recreate api/web) ==="
  tm_compose_prod_app up -d --no-deps --force-recreate nginx
fi

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
health_code="000"
for i in $(seq 1 20); do
  health_code="$(curl -sS -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1/api/health 2>/dev/null || echo 000)"
  if [[ "$health_code" == "200" ]]; then
    break
  fi
  sleep 2
done
if [[ "$health_code" != "200" ]]; then
  echo "Health via nginx falhou (HTTP $health_code). Tentando restart isolado do nginx..." >&2
  docker restart torqmind-nginx
  sleep 4
  health_code="$(curl -sS -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1/api/health 2>/dev/null || echo 000)"
fi
if [[ "$health_code" != "200" ]]; then
  echo "FALHA CRÍTICA: produção indisponível (nginx/api health HTTP $health_code)." >&2
  docker logs torqmind-nginx --tail 30 >&2 || true
  exit 1
fi
curl -fsS -m 10 http://127.0.0.1/api/health
echo
echo "PROD app up OK"
