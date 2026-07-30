#!/usr/bin/env bash
# Publica títulos financeiros STG PostgreSQL → mart ClickHouse.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_APP="${ENV_FILE:-/etc/torqmind/homolog.app.env}"
ENV_CH="${CLUSTER_ENV:-/etc/torqmind/prod.analytics.env}"
ENV_PG="${PG_ENV_FILE:-/etc/torqmind/prod.pg.env}"
EMPRESA="${ID_EMPRESA:-1}"
ROLE="${ROLE:-platform_master}"
DAYS="${DAYS:-180}"

set -a
# shellcheck disable=SC1090
source "$ENV_APP"
# shellcheck disable=SC1090
source "$ENV_CH"
if [[ -f "$ENV_PG" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_PG"
fi
set +a

export APP_ENV="${APP_ENV:-homolog}"
export PG_HOST="${PG_HOST:-172.30.0.8}"
export PG_PORT="${PG_PORT:-5432}"
export PG_USER="${POSTGRES_USER:-torqmind}"
export PG_PASSWORD="${POSTGRES_PASSWORD:-}"
export PG_DATABASE="${POSTGRES_DB:-torqmind}"
export POSTGRES_DB="$PG_DATABASE"
unset DATABASE_URL
export PYTHONPATH="$ROOT/apps/api${PYTHONPATH:+:$PYTHONPATH}"

cd "$ROOT"
.venv/bin/python - <<PY
from app.services.finance_titles import publish_finance_titles
import json
n = publish_finance_titles("${ROLE}", int("${EMPRESA}"), days=int("${DAYS}"))
print(json.dumps({"ok": True, "inserted": n, "id_empresa": int("${EMPRESA}")}))
PY
