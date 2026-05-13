#!/usr/bin/env bash
set -euo pipefail
# Refresh PostgreSQL materialized views that are not covered by the realtime
# ClickHouse pipeline. Run every 15-30 minutes or @reboot.
#
# These MVs back the goals/leaderboard and risk-employee screens that still
# read from PG while no realtime equivalent exists in torqmind_mart_rt.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.app.yml}"
LOG_FILE="${LOG_FILE:-/home/tm/logs/refresh-pg-marts.log}"
LOCK_FILE="/tmp/torqmind-refresh-pg-marts.lock"

mkdir -p "$(dirname "$LOG_FILE")"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Iseconds) Refresh PG marts already running" >> "$LOG_FILE"
  exit 0
fi

compose() {
  docker compose -f "$ROOT_DIR/$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

# Wait for API container to be healthy (important for @reboot)
for i in $(seq 1 60); do
  status="$(compose ps --status running --services 2>/dev/null | grep -Fx 'api' || true)"
  if [[ -n "$status" ]]; then
    break
  fi
  sleep 5
done

if [[ -z "${status:-}" ]]; then
  echo "$(date -Iseconds) ERROR: API container not running after 5 min" >> "$LOG_FILE"
  exit 1
fi

echo "$(date -Iseconds) Starting PG mart refresh..." >> "$LOG_FILE"

compose exec -T api python3 -c "
from app.db import get_conn
import time

# Sales MVs (used by goals/leaderboard, sales overview fallback)
mvs = [
    'mart.agg_vendas_diaria',
    'mart.insights_base_diaria',
    'mart.agg_funcionarios_diaria',
    'mart.agg_vendas_hora',
    'mart.agg_grupos_diaria',
    'mart.agg_risco_diaria',
    'mart.risco_top_funcionarios_diaria',
    'mart.risco_turno_local_diaria',
]
with get_conn(role='MASTER', tenant_id=1) as conn:
    for mv in mvs:
        t0 = time.time()
        try:
            conn.execute(f'REFRESH MATERIALIZED VIEW {mv}')
            elapsed = time.time() - t0
            r = conn.execute(f'SELECT count(*) AS cnt FROM {mv}').fetchone()
            print(f'{mv}: {r[\"cnt\"]} rows ({elapsed:.1f}s)')
        except Exception as e:
            elapsed = time.time() - t0
            print(f'{mv}: ERROR ({elapsed:.1f}s) - {e}')
" >> "$LOG_FILE" 2>&1

echo "$(date -Iseconds) PG mart refresh complete" >> "$LOG_FILE"
