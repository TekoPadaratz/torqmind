#!/usr/bin/env bash
# Reconcile CONTASRECEBER (Xpert -> STG -> DW -> mart) on the App VM.
#
# Heals the "title paid directly in CONTASRECEBER still shown as overdue" class
# of bug: a direct payment sets DTAPGTO/VLRPAGO without bumping DATAREPL, the
# agent watermark can be poisoned, and the agent's open-only revisit window does
# not re-read paid titles. This server-side safety net re-reads open + recently
# paid titles from Xpert and refreshes the delinquency mart via the canonical
# ETL functions. Idempotent; safe to run on a schedule.
#
# Requires the App VM to reach the Xpert SQL Server (same host that runs the
# xpert_source_explorer tool). NO secrets here — they come from env files.
#
# Usage:
#   ENV_FILE=/etc/torqmind/prod.app.env \
#   SQLSERVER_ENV_FILE=config/source-explorer.env \
#   ID_EMPRESA=1 ./deploy/scripts/prod-reconcile-contasreceber.sh
#   # preview only: add DRY_RUN=1
#
# Suggested cron (App VM, e.g. twice a day, with a lock to avoid overlap):
#   0 7,19 * * * cd /home/tm/torqmind && flock -n /tmp/torqmind-reconcile-cr.lock \
#     env ENV_FILE=/etc/torqmind/prod.app.env SQLSERVER_ENV_FILE=config/source-explorer.env \
#     ID_EMPRESA=1 ./deploy/scripts/prod-reconcile-contasreceber.sh \
#     >> /home/tm/logs/reconcile-contasreceber-cron.log 2>&1
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
SQLSERVER_ENV_FILE="${SQLSERVER_ENV_FILE:-config/source-explorer.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
PAID_DAYS="${PAID_DAYS:-120}"
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

# Load PostgreSQL connection from the prod env (POSTGRES_*/PG_*).
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

PYBIN="python3"
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYBIN="$ROOT_DIR/.venv/bin/python"
fi

ARGS=(scripts/fix_contasreceber_sync.py
  --sqlserver-env-file "$SQLSERVER_ENV_FILE"
  --id-empresa "$ID_EMPRESA"
  --paid-days "$PAID_DAYS")
if [[ "$DRY_RUN" == "1" ]]; then
  ARGS+=(--dry-run)
fi

echo "[reconcile-contasreceber] $(date -Is) id_empresa=$ID_EMPRESA paid_days=$PAID_DAYS dry_run=$DRY_RUN"
exec "$PYBIN" "${ARGS[@]}"
