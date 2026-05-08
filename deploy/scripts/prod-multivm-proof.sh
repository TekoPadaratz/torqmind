#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YES=true
DRY_RUN=false
OUTPUT_FILE="${OUTPUT_FILE:-}"
ID_EMPRESA="${ID_EMPRESA:-1}"

# shellcheck source=deploy/scripts/lib/multivm.sh
source "$ROOT_DIR/deploy/scripts/lib/multivm.sh"

usage() {
  cat <<'EOF'
Usage:
  CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh [--output proof.json] [--dry-run]

Generates a JSON proof pack with PASS/FAIL. It never prints secrets.
EOF
}

while [[ $# -gt 0 ]]; do
  if tm_mv_parse_common_flag "$1"; then
    shift
    continue
  fi
  case "$1" in
    --output)
      [[ $# -ge 2 ]] || tm_mv_die "--output requires a path"
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --id-empresa)
      [[ $# -ge 2 ]] || tm_mv_die "--id-empresa requires a value"
      ID_EMPRESA="$2"
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

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
ITEMS_FILE="$TMP_DIR/items.tsv"
RESULT="PASS"

PG_ENV="$(tm_mv_env_file_for_role pg)"
ANALYTICS_ENV="$(tm_mv_env_file_for_role analytics)"
APP_ENV="$(tm_mv_env_file_for_role app)"

collect() {
  local key="$1"
  local role="$2"
  local cmd="$3"
  local status="ok"
  local output_file="$TMP_DIR/${key}.out"

  tm_mv_log "collecting $key"
  if [[ "$DRY_RUN" == "true" ]]; then
    printf 'DRY_RUN\n' >"$output_file"
    status="dry_run"
  elif ! tm_mv_ssh "$role" "$cmd" >"$output_file" 2>&1; then
    status="fail"
    RESULT="FAIL"
  fi
  printf '%s\t%s\t%s\t%s\n' "$key" "$role" "$status" "$output_file" >>"$ITEMS_FILE"
}

pg_json_query() {
  local sql="$1"
  cat <<EOF
set -euo pipefail
set -a; source $(tm_mv_quote "$PG_ENV"); set +a
cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
docker compose -f docker-compose.pg.yml --env-file $(tm_mv_quote "$PG_ENV") exec -T postgres \
  psql -U "\$POSTGRES_USER" -d "\$POSTGRES_DB" -XAt -c $(tm_mv_quote "$sql")
EOF
}

ch_json_query() {
  local sql="$1"
  cat <<EOF
set -euo pipefail
set -a; source $(tm_mv_quote "$ANALYTICS_ENV"); set +a
cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
docker compose -f docker-compose.analytics.yml --env-file $(tm_mv_quote "$ANALYTICS_ENV") exec -T clickhouse \
  clickhouse-client --user "\$CLICKHOUSE_USER" --password "\$CLICKHOUSE_PASSWORD" --format=JSONEachRow -q $(tm_mv_quote "$sql")
EOF
}

for role in $(tm_mv_for_each_role); do
  collect "containers_${role}" "$role" "
    cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
    docker compose -f $(tm_mv_quote "$(tm_mv_compose_file_for_role "$role")") --env-file $(tm_mv_quote "$(tm_mv_env_file_for_role "$role")") ps --format json
  "
done

collect "env_flags" app "
  set -euo pipefail
  set -a; source $(tm_mv_quote "$APP_ENV"); set +a
  python3 - <<'PY'
import json, os
keys = [
    'APP_ENV', 'PG_HOST', 'PG_PORT', 'POSTGRES_DB', 'CLICKHOUSE_HOST',
    'CLICKHOUSE_PORT', 'USE_CLICKHOUSE', 'USE_REALTIME_MARTS',
    'REALTIME_MARTS_SOURCE', 'REALTIME_MARTS_FALLBACK',
    'REFRESH_LEGACY_PG_MARTS',
]
print(json.dumps({key: os.environ.get(key, '') for key in keys}, sort_keys=True))
PY
"

collect "migrations" pg "$(pg_json_query "SELECT json_build_object('applied', count(*), 'latest', max(filename)) FROM app.schema_migrations;")"
collect "stg_counts" pg "$(pg_json_query "SELECT json_build_object('stg_comprovantes', (SELECT count(*) FROM stg.comprovantes), 'stg_itenscomprovantes', (SELECT count(*) FROM stg.itenscomprovantes), 'stg_formas_pgto_comprovantes', (SELECT count(*) FROM stg.formas_pgto_comprovantes));")"
collect "dw_counts" pg "$(pg_json_query "SELECT json_build_object('dw_fact_comprovante', (SELECT count(*) FROM dw.fact_comprovante), 'dw_fact_venda', (SELECT count(*) FROM dw.fact_venda), 'dw_fact_venda_item', (SELECT count(*) FROM dw.fact_venda_item));")"

collect "clickhouse_counts" analytics "$(ch_json_query "SELECT database, name, total_rows FROM system.tables WHERE database IN ('torqmind_raw','torqmind_current','torqmind_mart_rt','torqmind_ops') ORDER BY database, name;")"
collect "mart_counts" analytics "$(ch_json_query "SELECT 'sales_daily_rt' AS table, count() AS rows FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA UNION ALL SELECT 'dashboard_home_rt', count() FROM torqmind_mart_rt.dashboard_home_rt FINAL WHERE id_empresa=$ID_EMPRESA UNION ALL SELECT 'payments_by_type_rt', count() FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA;")"
collect "debezium_status" analytics "curl -fsS http://127.0.0.1:18083/connectors/torqmind-postgres-cdc/status"
collect "cdc_lag" analytics "$(ch_json_query "SELECT ifNull(max(lag), 0) AS max_lag_messages FROM torqmind_ops.cdc_lag WHERE measured_at >= now() - INTERVAL 15 MINUTE;")"
collect "freshness" analytics "$(ch_json_query "SELECT id_empresa, domain, lag_seconds, status, checked_at FROM torqmind_mart_rt.source_freshness FINAL WHERE id_empresa=$ID_EMPRESA ORDER BY domain;")"

collect "etl_cron_last_run" app "
  log_file=/home/${TORQMIND_SSH_USER}/logs/prod-etl-incremental-cron.log
  export TM_CRON_LOG_FILE=\"\$log_file\"
  python3 - <<'PY'
import json, os, pathlib
path = pathlib.Path(os.environ['TM_CRON_LOG_FILE'])
print(json.dumps({
    'path': str(path),
    'exists': path.exists(),
    'last_modified_epoch': path.stat().st_mtime if path.exists() else None,
    'tail': path.read_text(errors='replace').splitlines()[-10:] if path.exists() else [],
}))
PY
"

collect "api_endpoint" app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") exec -T api python -c \"import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5).read().decode())\"
"
collect "web_health" app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") exec -T web wget -q -O - http://127.0.0.1:3000/ | head -c 500
"
collect "nginx_health" app "curl -fsS http://127.0.0.1/health"
collect "product_screen_smoke" app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  ENV_FILE=$(tm_mv_quote "$APP_ENV") COMPOSE_FILE=docker-compose.app.yml ./deploy/scripts/realtime-product-screen-smoke.sh
"

COMMIT="$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
BRANCH="$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DEFAULT_PROOF_DIR="${PROOF_DIR:-$ROOT_DIR/tmp}"
mkdir -p "$DEFAULT_PROOF_DIR"
OUTPUT_FILE="${OUTPUT_FILE:-$DEFAULT_PROOF_DIR/prod-multivm-proof-$(date -u +%Y%m%d_%H%M%S).json}"

PROOF_COMMIT="$COMMIT" \
PROOF_BRANCH="$BRANCH" \
PROOF_GENERATED_AT="$GENERATED_AT" \
PROOF_RESULT="$RESULT" \
PROOF_ITEMS_FILE="$ITEMS_FILE" \
PROOF_OUTPUT_FILE="$OUTPUT_FILE" \
PROOF_PG_HOST="$TORQMIND_PG_HOST" \
PROOF_ANALYTICS_HOST="$TORQMIND_ANALYTICS_HOST" \
PROOF_APP_HOST="$TORQMIND_APP_HOST" \
PROOF_ID_EMPRESA="$ID_EMPRESA" \
python3 - <<'PY'
import json
import os
from pathlib import Path

items = {}
items_file = Path(os.environ["PROOF_ITEMS_FILE"])
if items_file.exists():
    for line in items_file.read_text().splitlines():
        key, role, status, output_file = line.split("\t", 3)
        text = Path(output_file).read_text(errors="replace") if Path(output_file).exists() else ""
        parsed = None
        stripped = text.strip()
        if stripped:
            try:
                parsed = json.loads(stripped)
            except Exception:
                lines = []
                for raw in stripped.splitlines():
                    try:
                        lines.append(json.loads(raw))
                    except Exception:
                        lines = []
                        break
                parsed = lines or None
        items[key] = {
            "role": role,
            "status": status,
            "json": parsed,
            "text": None if parsed is not None else stripped[-4000:],
        }

proof = {
    "proof": "torqmind-production-multi-vm",
    "generated_at": os.environ["PROOF_GENERATED_AT"],
    "commit": os.environ["PROOF_COMMIT"],
    "branch": os.environ["PROOF_BRANCH"],
    "id_empresa": int(os.environ["PROOF_ID_EMPRESA"]),
    "hosts": {
        "postgres": os.environ["PROOF_PG_HOST"],
        "analytics": os.environ["PROOF_ANALYTICS_HOST"],
        "app": os.environ["PROOF_APP_HOST"],
    },
    "containers": {
        "postgres": items.get("containers_pg"),
        "analytics": items.get("containers_analytics"),
        "app": items.get("containers_app"),
    },
    "env_flags": items.get("env_flags"),
    "migrations": items.get("migrations"),
    "stg_counts": items.get("stg_counts"),
    "dw_counts": items.get("dw_counts"),
    "clickhouse_counts": items.get("clickhouse_counts"),
    "marts_counts": items.get("mart_counts"),
    "debezium_status": items.get("debezium_status"),
    "cdc_lag": items.get("cdc_lag"),
    "freshness": items.get("freshness"),
    "etl_cron_last_run": items.get("etl_cron_last_run"),
    "api_endpoints": items.get("api_endpoint"),
    "web_health": items.get("web_health"),
    "nginx_health": items.get("nginx_health"),
    "product_screen_smoke": items.get("product_screen_smoke"),
    "checks": items,
    "result": os.environ["PROOF_RESULT"],
}

output = Path(os.environ["PROOF_OUTPUT_FILE"])
output.write_text(json.dumps(proof, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
print(str(output))
PY

tm_mv_log "proof written to $OUTPUT_FILE (result=$RESULT)"
if [[ "$RESULT" != "PASS" ]]; then
  exit 1
fi
