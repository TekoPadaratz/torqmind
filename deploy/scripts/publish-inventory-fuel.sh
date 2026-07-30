#!/usr/bin/env bash
# Publica mart de estoque combustível (PG STG → CH write).
# Homolog API é RO no CH — usa prod.analytics.env para escrita.
# Mash lê STG de produção (fonte viva), não torqmind_homolog.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_APP="${ENV_FILE:-/etc/torqmind/homolog.app.env}"
ENV_CH="${CLUSTER_ENV:-/etc/torqmind/prod.analytics.env}"
ENV_PG="${PG_ENV_FILE:-/etc/torqmind/prod.pg.env}"
EMPRESA="${ID_EMPRESA:-1}"
ROLE="${ROLE:-platform_master}"
DAYS="${DAYS:-120}"

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
export POSTGRES_HOST="$PG_HOST"
export POSTGRES_USER="$PG_USER"
export POSTGRES_PASSWORD="$PG_PASSWORD"
# homolog.app.env define DATABASE_URL→torqmind_homolog; db.py prioriza DATABASE_URL.
unset DATABASE_URL
export PYTHONPATH="$ROOT/apps/api${PYTHONPATH:+:$PYTHONPATH}"

cd "$ROOT"
.venv/bin/python - <<PY
from app.services.inventory_fuel import publish_inventory_fuel_bundle
from app.db import get_conn
import json
with get_conn(role="${ROLE}", tenant_id=int("${EMPRESA}"), branch_id=None) as conn:
    db = conn.execute("SELECT current_database() AS db").fetchone()["db"]
out = publish_inventory_fuel_bundle("${ROLE}", int("${EMPRESA}"), days=int("${DAYS}"))
print(json.dumps({"ok": True, **out, "id_empresa": int("${EMPRESA}"), "pg": db}))
PY
