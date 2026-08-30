#!/usr/bin/env bash
# Deploy api + web em homolog e produção via SSH.
# Uso: BRANCH=cursor/foo ./deploy/scripts/cloud-deploy-app.sh homolog
set -euo pipefail

TARGET="${1:-homolog}"
BRANCH="${BRANCH:-master}"
REPO_DIR="/home/tm/torqmind"
RUN_MIGRATE="${RUN_MIGRATE:-1}"

ssh_cmd() {
  if [ -n "${TM_SSH_PASSWORD:-}" ] && command -v sshpass >/dev/null 2>&1; then
    SSHPASS="${TM_SSH_PASSWORD}" sshpass -e ssh -o StrictHostKeyChecking=accept-new "$@"
  else
    ssh "$@"
  fi
}

case "${TARGET}" in
  homolog)
    PROJECT="torqmind-homolog"
    COMPOSE_FILE="docker-compose.homolog.yml"
    ENV_FILE="/etc/torqmind/homolog.app.env"
    SERVICES="api web"
  ;;
  prod)
    PROJECT="torqmind"
    COMPOSE_FILE="docker-compose.app.yml"
    ENV_FILE="/etc/torqmind/prod.app.env"
    SERVICES="api web"
  ;;
  *)
    echo "Uso: $0 homolog|prod" >&2
    exit 1
  ;;
esac

REMOTE=$(cat <<EOF
set -euo pipefail
cd ${REPO_DIR}
git fetch origin
git checkout ${BRANCH}
git pull --ff-only origin ${BRANCH}
docker ps --format '{{.ID}}\t{{.Names}}\t{{.Status}}' | grep torqmind || true
docker compose ls
if [ "${RUN_MIGRATE}" = "1" ]; then
  ENV_FILE=${ENV_FILE} ./deploy/scripts/prod-migrate.sh
fi
docker compose -p ${PROJECT} -f ${COMPOSE_FILE} --env-file ${ENV_FILE} build ${SERVICES}
docker compose -p ${PROJECT} -f ${COMPOSE_FILE} --env-file ${ENV_FILE} up -d --no-deps ${SERVICES}
docker compose -p ${PROJECT} -f ${COMPOSE_FILE} --env-file ${ENV_FILE} ps ${SERVICES}
EOF
)

echo "=== Deploy ${TARGET} api+web (branch ${BRANCH}) ==="
ssh_cmd torqmind-prod "bash -s" <<< "${REMOTE}"
echo "=== OK: ${TARGET} ==="

