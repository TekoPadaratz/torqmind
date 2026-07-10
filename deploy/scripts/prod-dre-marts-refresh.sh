#!/usr/bin/env bash
# deploy/scripts/prod-dre-marts-refresh.sh
#
# Wrapper de AUTOMACAO para o refresh dos marts de DRE/Lucro (ClickHouse).
#
# O prod-profit-marts-refresh.sh precisa de DUAS fontes de credencial:
#   - Postgres (le dw em 172.30.0.8): POSTGRES_* em prod.app.env  -> psql roda na .10
#   - ClickHouse (escreve marts em 172.30.0.9): CLICKHOUSE_PASSWORD em prod.analytics.env
# Nenhum env sozinho tem os dois, e a .9 nao tem psql. Este wrapper roda na .10
# (que tem psql + os dois envs), combina as credenciais e aponta o ClickHouse
# para a VM analytics (.9) via HTTP.
#
# Uso: ./deploy/scripts/prod-dre-marts-refresh.sh [--full]
set -uo pipefail

cd "$(dirname "$0")/../.." || exit 1

APP_ENV="${APP_ENV:-/etc/torqmind/prod.app.env}"
ANALYTICS_ENV="${ANALYTICS_ENV:-/etc/torqmind/prod.analytics.env}"

set -a
# shellcheck disable=SC1090
[[ -f "$APP_ENV" ]] && source "$APP_ENV"
# shellcheck disable=SC1090
[[ -f "$ANALYTICS_ENV" ]] && source "$ANALYTICS_ENV"
set +a

# ClickHouse fica na VM analytics; alcancado por HTTP a partir da .10.
export CLICKHOUSE_HOST="${CLICKHOUSE_REMOTE_HOST:-172.30.0.9}"

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] DRE marts refresh (ClickHouse=${CLICKHOUSE_HOST})"

# ENV_FILE=/dev/null: as credenciais ja estao no ambiente (nao re-sourcear).
ENV_FILE=/dev/null ID_EMPRESA="${ID_EMPRESA:-1}" \
  ./deploy/scripts/prod-profit-marts-refresh.sh "$@" < /dev/null

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] DRE marts refresh done."
