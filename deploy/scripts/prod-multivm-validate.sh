#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YES=false
DRY_RUN=false
ID_EMPRESA="${ID_EMPRESA:-1}"
CRITICAL_DATA_KEY="${CRITICAL_DATA_KEY:-20260430}"
FRESHNESS_MAX_SECONDS="${FRESHNESS_MAX_SECONDS:-300}"
CDC_LAG_MAX_MESSAGES="${CDC_LAG_MAX_MESSAGES:-1000}"

# shellcheck source=deploy/scripts/lib/multivm.sh
source "$ROOT_DIR/deploy/scripts/lib/multivm.sh"

usage() {
  cat <<'EOF'
Usage:
  CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh [--yes] [--dry-run]

Blocking production validation. It fails on broken infrastructure, fallback=true,
Debezium not RUNNING, stale freshness, CDC lag above threshold, data_key=0, or
missing critical data_key.
EOF
}

while [[ $# -gt 0 ]]; do
  if tm_mv_parse_common_flag "$1"; then
    shift
    continue
  fi
  case "$1" in
    --id-empresa)
      [[ $# -ge 2 ]] || tm_mv_die "--id-empresa requires a value"
      ID_EMPRESA="$2"
      shift 2
      ;;
    --critical-data-key)
      [[ $# -ge 2 ]] || tm_mv_die "--critical-data-key requires a value"
      CRITICAL_DATA_KEY="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      tm_mv_die "unknown argument: $1"
      ;;
  esac
done

tm_mv_load_cluster_env

FAILURES=0
CHECKS=0

record_ok() {
  CHECKS=$((CHECKS + 1))
  printf '  %-42s OK\n' "$1"
}

record_fail() {
  CHECKS=$((CHECKS + 1))
  FAILURES=$((FAILURES + 1))
  printf '  %-42s FAIL %s\n' "$1" "${2:-}"
}

run_check() {
  local label="$1"
  local role="$2"
  local cmd="$3"
  if tm_mv_ssh "$role" "$cmd"; then
    record_ok "$label"
  else
    record_fail "$label"
  fi
}

PG_ENV="$(tm_mv_env_file_for_role pg)"
ANALYTICS_ENV="$(tm_mv_env_file_for_role analytics)"
APP_ENV="$(tm_mv_env_file_for_role app)"

pg_cmd() {
  local sql="$1"
  cat <<EOF
set -euo pipefail
set -a
source $(tm_mv_quote "$PG_ENV")
set +a
cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
docker compose -f docker-compose.pg.yml --env-file $(tm_mv_quote "$PG_ENV") exec -T postgres \
  psql -U "\$POSTGRES_USER" -d "\$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc $(tm_mv_quote "$sql")
EOF
}

ch_cmd() {
  local sql="$1"
  cat <<EOF
set -euo pipefail
set -a
source $(tm_mv_quote "$ANALYTICS_ENV")
set +a
cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
docker compose -f docker-compose.analytics.yml --env-file $(tm_mv_quote "$ANALYTICS_ENV") exec -T clickhouse \
  clickhouse-client --user "\$CLICKHOUSE_USER" --password "\$CLICKHOUSE_PASSWORD" --format=TabSeparated -q $(tm_mv_quote "$sql")
EOF
}

tm_mv_log "validating PostgreSQL"
run_check "pg.health" pg "
  set -euo pipefail
  set -a; source $(tm_mv_quote "$PG_ENV"); set +a
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.pg.yml --env-file $(tm_mv_quote "$PG_ENV") exec -T postgres pg_isready -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\"
"
run_check "pg.logical_replication" pg "$(pg_cmd "SELECT CASE WHEN current_setting('wal_level') = 'logical' THEN 1 ELSE 0 END;") | grep -qx 1"
run_check "pg.dw.schemas" pg "$(pg_cmd "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('stg','dw','mart','app','auth');") | awk '{exit (\$1 >= 4 ? 0 : 1)}'"
run_check "pg.stg.counts" pg "$(pg_cmd "SELECT COALESCE((SELECT count(*) FROM stg.comprovantes),0);") | awk '{exit (\$1 >= 0 ? 0 : 1)}'"
run_check "pg.dw.counts" pg "$(pg_cmd "SELECT COALESCE((SELECT count(*) FROM dw.fact_comprovante),0);") | awk '{exit (\$1 >= 0 ? 0 : 1)}'"

tm_mv_log "validating Analytics/Streaming"
run_check "clickhouse.health" analytics "$(ch_cmd "SELECT 1") | grep -qx 1"
run_check "clickhouse.databases" analytics "$(ch_cmd "SELECT count() FROM system.databases WHERE name IN ('torqmind_raw','torqmind_current','torqmind_mart_rt','torqmind_ops');") | awk '{exit (\$1 == 4 ? 0 : 1)}'"
run_check "redpanda.health" analytics "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.analytics.yml --env-file $(tm_mv_quote "$ANALYTICS_ENV") exec -T redpanda rpk cluster health | grep -qi '^Healthy:[[:space:]]*true'
"
run_check "debezium.running" analytics "
  status=\$(curl -fsS http://127.0.0.1:18083/connectors/torqmind-postgres-cdc/status)
  STATUS_JSON=\"\$status\" python3 - <<'PY'
import json, os
data = json.loads(os.environ['STATUS_JSON'])
connector = data.get('connector', {}).get('state')
tasks = [task.get('state') for task in data.get('tasks', [])]
if connector != 'RUNNING' or not tasks or any(state != 'RUNNING' for state in tasks):
    raise SystemExit(1)
PY
"
run_check "cdc-consumer.running" analytics "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.analytics.yml --env-file $(tm_mv_quote "$ANALYTICS_ENV") ps --status running --services | grep -Fx cdc-consumer
"
run_check "cdc.lag.not_stuck" analytics "
  value=\$($(ch_cmd "SELECT ifNull(max(lag), 0) FROM torqmind_ops.cdc_lag WHERE measured_at >= now() - INTERVAL 15 MINUTE;"))
  value=\"\${value//[[:space:]]/}\"
  [[ -z \"\$value\" ]] && value=0
  awk \"BEGIN { exit ((\$value + 0) <= $CDC_LAG_MAX_MESSAGES ? 0 : 1) }\"
"
run_check "freshness.ok" analytics "
  freshness=\$($(ch_cmd "SELECT count() AS rows, countIf(status = 'stale' OR lag_seconds > $FRESHNESS_MAX_SECONDS) AS stale_rows FROM torqmind_mart_rt.source_freshness FINAL WHERE id_empresa = $ID_EMPRESA;"))
  rows=\"\$(printf '%s' \"\$freshness\" | cut -f1 | tr -d '[:space:]')\"
  stale=\"\$(printf '%s' \"\$freshness\" | cut -f2 | tr -d '[:space:]')\"
  [[ \"\$rows\" -gt 0 && \"\$stale\" == \"0\" ]]
"
run_check "mart.no_data_key_zero" analytics "
  zero=\$($(ch_cmd "SELECT sum(rows) FROM (SELECT count() AS rows FROM torqmind_mart_rt.sales_daily_rt WHERE id_empresa=$ID_EMPRESA AND data_key=0 UNION ALL SELECT count() FROM torqmind_mart_rt.sales_hourly_rt WHERE id_empresa=$ID_EMPRESA AND data_key=0 UNION ALL SELECT count() FROM torqmind_mart_rt.sales_products_rt WHERE id_empresa=$ID_EMPRESA AND data_key=0 UNION ALL SELECT count() FROM torqmind_mart_rt.sales_groups_rt WHERE id_empresa=$ID_EMPRESA AND data_key=0);"))
  zero=\"\${zero//[[:space:]]/}\"
  [[ \"\$zero\" == \"0\" ]]
"
run_check "mart.critical_data_key" analytics "
  rows=\$($(ch_cmd "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA AND data_key=$CRITICAL_DATA_KEY;"))
  rows=\"\${rows//[[:space:]]/}\"
  [[ \"\$rows\" -gt 0 ]]
"

tm_mv_log "validating App/Web/Nginx"
run_check "api.fallback_false" app "
  set -euo pipefail
  set -a; source $(tm_mv_quote "$APP_ENV"); set +a
  case \"\${REALTIME_MARTS_FALLBACK,,}\" in true|1|yes) echo 'REALTIME_MARTS_FALLBACK=true is forbidden' >&2; exit 1;; esac
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") exec -T api python - <<'PY'
from app.config import settings
assert settings.realtime_marts_fallback is False
assert settings.pg_host not in {'postgres', 'localhost', '127.0.0.1'}
assert settings.clickhouse_host not in {'clickhouse', 'localhost', '127.0.0.1'}
print('ok')
PY
"
run_check "api.health" app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") exec -T api python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5).read()\"
"
run_check "web.health" app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") exec -T web wget -q -O - http://127.0.0.1:3000/ >/dev/null
"
run_check "nginx.health" app "curl -fsS http://127.0.0.1/health >/dev/null"
run_check "product.screen.smoke" app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  ENV_FILE=$(tm_mv_quote "$APP_ENV") COMPOSE_FILE=docker-compose.app.yml PRODUCT_SMOKE_REQUIRE_MULTIVM=true PRODUCT_SMOKE_CHECK_PAGES=true ./deploy/scripts/realtime-product-screen-smoke.sh
"

tm_mv_log "CHECKS=$CHECKS FAILURES=$FAILURES"
if (( FAILURES > 0 )); then
  tm_mv_log "RESULT: FAIL"
  exit 1
fi

tm_mv_log "RESULT: PASS"
