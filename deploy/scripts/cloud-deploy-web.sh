#!/usr/bin/env bash
# Deploy do serviço web em homolog e produção via SSH.
# Uso: BRANCH=cursor/commission-config-group-codes-3837 ./deploy/scripts/cloud-deploy-web.sh homolog
#      BRANCH=master ./deploy/scripts/cloud-deploy-web.sh prod
set -euo pipefail

TARGET="${1:-homolog}"
BRANCH="${BRANCH:-master}"
REPO_DIR="/home/tm/torqmind"

ssh_cmd() {
  if [ -n "${TM_SSH_PASSWORD:-}" ] && command -v sshpass >/dev/null 2>&1; then
    sshpass -e ssh -o StrictHostKeyChecking=accept-new "$@"
  else
    ssh "$@"
  fi
}

export SSHPASS="${TM_SSH_PASSWORD:-}"

case "${TARGET}" in
  homolog)
    PROJECT="torqmind-homolog"
    COMPOSE_FILE="docker-compose.homolog.yml"
    ENV_FILE="/etc/torqmind/homolog.app.env"
  ;;
  prod)
    PROJECT="torqmind"
    COMPOSE_FILE="docker-compose.app.yml"
    ENV_FILE="/etc/torqmind/prod.app.env"
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
docker ps --format '{{.ID}}\t{{.Names}}\t{{.Status}}'
docker compose ls
docker compose -p ${PROJECT} -f ${COMPOSE_FILE} --env-file ${ENV_FILE} build web
docker compose -p ${PROJECT} -f ${COMPOSE_FILE} --env-file ${ENV_FILE} up -d --no-deps web
docker compose -p ${PROJECT} -f ${COMPOSE_FILE} --env-file ${ENV_FILE} ps web
EOF
)

echo "=== Deploy web ${TARGET} (branch ${BRANCH}) ==="
ssh_cmd torqmind-prod "bash -s" <<< "${REMOTE}"
echo "=== OK: ${TARGET} ==="
