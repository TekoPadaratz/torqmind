#!/usr/bin/env bash
# Reconcile TURNOS (Xpert → STG → ClickHouse) on the App VM.
#
# Heals the "turno operacional = Turno não resolvido / Caixa geral falso" class
# of bug: the agent watermark for turnos is temporal (MAX DATA/DATATURNO/
# DATAFECHAMENTO). After a successful batch the local cursor advances; any
# ID_TURNOS that failed the first delivery (or never entered pending) and is
# already closed + outside the agent revisit window disappears from the
# incremental forever. Comprovantes/aferições then reference a turno that never
# landed in stg.turnos.
#
# This server-side safety net:
#   1) finds orphan FK refs (comprovantes + aferições) missing from stg.turnos
#   2) finds Xpert→STG gaps in the operational window (even without refs yet)
#   3) upserts PG + optionally CH (don't wait for Debezium)
#
# Idempotent; safe on a schedule. Does NOT recreate api/web/nginx.
#
# Usage:
#   ENV_FILE=/etc/torqmind/prod.app.env \
#   SQLSERVER_ENV_FILE=config/source-explorer.env \
#   ID_EMPRESA=1 ./deploy/scripts/prod-reconcile-turnos.sh
#   # preview: DRY_RUN=1
#
# Suggested cron (App VM, every 2h, flock to avoid overlap):
#   15 */2 * * * cd /home/tm/torqmind && flock -n /tmp/torqmind-reconcile-turnos.lock \
#     env ENV_FILE=/etc/torqmind/prod.app.env SQLSERVER_ENV_FILE=config/source-explorer.env \
#     ID_EMPRESA=1 ./deploy/scripts/prod-reconcile-turnos.sh \
#     >> /home/tm/logs/reconcile-turnos-cron.log 2>&1
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
SQLSERVER_ENV_FILE="${SQLSERVER_ENV_FILE:-config/source-explorer.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
SINCE_DAYS="${SINCE_DAYS:-60}"
ALSO_CLICKHOUSE="${ALSO_CLICKHOUSE:-1}"
DRY_RUN="${DRY_RUN:-0}"

cd "$ROOT_DIR"

if ! [[ "$ID_EMPRESA" =~ ^[0-9]+$ ]]; then
  echo "ID_EMPRESA must be numeric" >&2
  exit 2
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ENV_FILE not found: $ENV_FILE" >&2
  exit 2
fi
if [[ ! -f "$SQLSERVER_ENV_FILE" ]]; then
  echo "SQLSERVER_ENV_FILE not found: $SQLSERVER_ENV_FILE" >&2
  exit 2
fi

# Load PG_* from prod env without polluting NGINX_* for other stacks.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

PYBIN="python3"
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYBIN="$ROOT_DIR/.venv/bin/python"
fi

ARGS=(scripts/reconcile_turnos_sync.py
  --sqlserver-env-file "$SQLSERVER_ENV_FILE"
  --id-empresa "$ID_EMPRESA"
  --since-days "$SINCE_DAYS"
  --all-comprovantes)
if [[ "$ALSO_CLICKHOUSE" == "1" ]]; then
  ARGS+=(--also-clickhouse)
fi
if [[ "$DRY_RUN" == "1" ]]; then
  ARGS+=(--dry-run)
fi

echo "[reconcile-turnos] $(date -Is) id_empresa=$ID_EMPRESA since_days=$SINCE_DAYS also_ch=$ALSO_CLICKHOUSE dry_run=$DRY_RUN"
exec "$PYBIN" "${ARGS[@]}"
