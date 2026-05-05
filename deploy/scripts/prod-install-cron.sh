#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
PIPELINE_LOG="${PIPELINE_LOG:-/home/deploy/logs/torqmind-etl-pipeline.log}"
RISK_INTERVAL_MINUTES="${RISK_INTERVAL_MINUTES:-30}"
RISK_TRACK_MODE="${RISK_TRACK_MODE:-auto}"
OPERATIONAL_INTERVAL_MINUTES="${OPERATIONAL_INTERVAL_MINUTES:-2}"
PIPELINE_TIMEOUT_SECONDS="${PIPELINE_TIMEOUT_SECONDS:-240}"
PIPELINE_WARN_SECONDS="${PIPELINE_WARN_SECONDS:-120}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo de ambiente não encontrado em $ENV_FILE"
  exit 1
fi

mkdir -p "$(dirname "$PIPELINE_LOG")"

if ! [[ "$OPERATIONAL_INTERVAL_MINUTES" =~ ^[0-9]+$ ]] || [[ "$OPERATIONAL_INTERVAL_MINUTES" -lt 1 || "$OPERATIONAL_INTERVAL_MINUTES" -gt 59 ]]; then
  echo "OPERATIONAL_INTERVAL_MINUTES deve estar entre 1 e 59"
  exit 2
fi

pipeline_line="*/$OPERATIONAL_INTERVAL_MINUTES * * * * cd $ROOT_DIR && ENV_FILE=$ENV_FILE COMPOSE_FILE=$COMPOSE_FILE RISK_INTERVAL_MINUTES=$RISK_INTERVAL_MINUTES RISK_TRACK_MODE=$RISK_TRACK_MODE PIPELINE_TIMEOUT_SECONDS=$PIPELINE_TIMEOUT_SECONDS PIPELINE_WARN_SECONDS=$PIPELINE_WARN_SECONDS $ROOT_DIR/deploy/scripts/prod-etl-pipeline.sh >> $PIPELINE_LOG 2>&1"

existing_cron="$(crontab -l 2>/dev/null || true)"
filtered_cron="$(printf '%s\n' "$existing_cron" | grep -v 'TorqMind ETL schedule' | grep -v 'prod-etl-operational.sh' | grep -v 'prod-etl-risk.sh' | grep -v 'prod-etl-pipeline.sh' || true)"

{
  printf '%s\n' "$filtered_cron"
  echo "# TorqMind ETL schedule"
  echo "$pipeline_line"
} | sed '/^[[:space:]]*$/N;/^\n$/D' | crontab -

echo "Cron TorqMind instalado/atualizado com sucesso."
echo "  pipeline único: */${OPERATIONAL_INTERVAL_MINUTES} * * * *"
echo "  risk sequencial após operacional: modo ${RISK_TRACK_MODE} (intervalo ${RISK_INTERVAL_MINUTES} min quando habilitado)"
if [[ "${RISK_TRACK_MODE,,}" == "auto" ]]; then
  echo "  auto: desabilita risk em realtime STG sem refresh legado de marts PostgreSQL"
fi
echo "  lock anti-overlap e timeout de ${PIPELINE_TIMEOUT_SECONDS}s protegendo o ciclo"
echo "  log: ${PIPELINE_LOG}"
echo "Garanta também no host:"
echo "  sudo systemctl enable --now docker"
echo "  sudo systemctl enable --now cron"
