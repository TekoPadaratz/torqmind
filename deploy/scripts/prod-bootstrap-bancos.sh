#!/usr/bin/env bash
# Bootstrap CONTASBANCARIA + BANCOSPADRAO + MOVBANCOS (Xpert → STG PG) e refresh liquidez.
# Depois rode: ENV_FILE=/etc/torqmind/prod.app.env ./deploy/scripts/prod-bi-hotpath-ch-publish.sh
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
SOURCE_ENV="${SOURCE_ENV:-$ROOT_DIR/config/source-explorer.env}"
FROM_DATE="${FROM_DATE:-2025-01-06}"
ID_EMPRESA="${ID_EMPRESA:-1}"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
# shellcheck disable=SC1090
source "$SOURCE_ENV"
set +a

export PG_HOST="${POSTGRES_HOST:-172.30.0.8}"
export PG_PORT="${POSTGRES_PORT:-5432}"
export PG_USER="${POSTGRES_USER:?}"
export PG_PASSWORD="${POSTGRES_PASSWORD:?}"
PG_DATABASE="${POSTGRES_DB:-torqmind}"

cd "$ROOT_DIR"
PATH="$ROOT_DIR/.venv/bin:$PATH" python -u tools/bootstrap_bancos_from_xpert.py \
  --id-empresa "$ID_EMPRESA" \
  --pg-database "$PG_DATABASE" \
  --from-date "$FROM_DATE"
