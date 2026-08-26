#!/usr/bin/env bash
# Reconcile CONTASRECEBER (Xpert -> STG -> DW -> CH titles/overview) on the App VM.
#
# Heals the "title paid directly in CONTASRECEBER still shown as overdue" class
# of bug: a direct payment sets DTAPGTO/VLRPAGO without bumping DATAREPL, the
# agent watermark can miss the payment, and phantoms stay open in finance KPIs
# until STG is healed AND finance_overview_rt is rebuilt.
#
# Mirrors prod-reconcile-contaspagar.sh: heal + publish titles + refresh overview.
#
# Usage:
#   ENV_FILE=/etc/torqmind/prod.app.env \
#   SQLSERVER_ENV_FILE=config/source-explorer.env \
#   ID_EMPRESA=1 ./deploy/scripts/prod-reconcile-contasreceber.sh
#   # preview only: add DRY_RUN=1
#
# Suggested cron (App VM, several times/day — baixas do dia não podem esperar 19h):
#   20 6,10,14,18 * * * cd /home/tm/torqmind && flock -n /tmp/torqmind-reconcile-cr.lock \
#     env ENV_FILE=/etc/torqmind/prod.app.env SQLSERVER_ENV_FILE=config/source-explorer.env \
#     ID_EMPRESA=1 ./deploy/scripts/prod-reconcile-contasreceber.sh \
#     >> /home/tm/logs/reconcile-contasreceber-cron.log 2>&1
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
SQLSERVER_ENV_FILE="${SQLSERVER_ENV_FILE:-config/source-explorer.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
PAID_DAYS="${PAID_DAYS:-180}"
DRY_RUN="${DRY_RUN:-0}"
SKIP_PUBLISH="${SKIP_PUBLISH:-0}"

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
"$PYBIN" "${ARGS[@]}"

if [[ "$DRY_RUN" == "1" || "$SKIP_PUBLISH" == "1" ]]; then
  echo "[reconcile-contasreceber] skip publish (dry_run=$DRY_RUN skip_publish=$SKIP_PUBLISH)"
  exit 0
fi

echo "[reconcile-contasreceber] publish mart_finance_titles_rt"
ENV_FILE="$ENV_FILE" ID_EMPRESA="$ID_EMPRESA" DAYS="$PAID_DAYS" \
  ./deploy/scripts/publish-finance-titles.sh

echo "[reconcile-contasreceber] wait CDC STG→CH then refresh finance_overview_rt"
sleep 12
_REFRESH_PY='import os
from torqmind_cdc_consumer.mart_builder import MartBuilder
mb = MartBuilder(
    clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
    clickhouse_user=os.environ.get("CLICKHOUSE_USER", "torqmind"),
    clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
)
client = mb._get_client()
try:
    res = mb._refresh_finance_overview_stg(client, id_empresa=1, id_filial=None)
    print({"ok": True, "result": str(res)})
finally:
    client.close()
'
if docker ps --format '{{.Names}}' | grep -qx torqmind-cdc-consumer; then
  docker exec torqmind-cdc-consumer python -c "$_REFRESH_PY"
elif ssh -o BatchMode=yes -o ConnectTimeout=8 tm@172.30.0.9 \
    'docker ps --format "{{.Names}}" | grep -qx torqmind-cdc-consumer'; then
  ssh -o BatchMode=yes -o ConnectTimeout=60 tm@172.30.0.9 \
    "docker exec torqmind-cdc-consumer python -c $(printf '%q' "$_REFRESH_PY")"
else
  echo "WARN: cdc-consumer unreachable — overview sobe no próximo CDC tick."
fi

echo "[reconcile-contasreceber] DONE $(date -Is)"
