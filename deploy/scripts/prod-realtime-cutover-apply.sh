#!/usr/bin/env bash
set -Eeuo pipefail

# TorqMind Realtime Cutover Apply
# One-command script to prepare and activate the realtime event-driven pipeline.
#
# Flow:
# 1. Preflight checks
# 2. Validate env and compose
# 3. Build API/Web/Consumer
# 4. Migrate PostgreSQL
# 5. Init ClickHouse streaming schemas (raw/current/ops/mart_rt)
# 6. Prepare PostgreSQL publication/slot
# 7. Start Redpanda/Debezium/Consumer
# 8. Register Debezium connector
# 9. Backfill mart_rt from current data
# 10. Validate parity
# 11. Set USE_REALTIME_MARTS=true
# 12. Rebuild API/Web
# 13. Smoke endpoints
# 14. Report

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROD_COMPOSE_FILE="docker-compose.prod.yml"
STREAMING_COMPOSE_FILE="docker-compose.streaming.yml"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
LOG_DIR="${LOG_DIR:-/home/deploy/logs}"

DRY_RUN=0
ASSUME_YES=0
FROM_DATE="2025-01-01"
ID_EMPRESA=1
ID_FILIAL=""
WITH_BACKFILL=0
VALIDATE_ONLY=0
ROLLBACK_TO_LEGACY=0
SOURCE="stg"

source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh [flags]

Flags:
  --yes                  Skip confirmations
  --dry-run              Print actions without executing
  --from-date YYYY-MM-DD Backfill start date (default 2025-01-01)
  --id-empresa <id>      Tenant (default 1)
  --id-filial <id>       Audit filial
  --with-backfill        Run mart_rt backfill from current data
  --validate-only        Only validate parity, don't cutover
  --rollback-to-legacy   Disable realtime marts and revert to legacy
  --source stg|dw        Realtime source (default stg)
  --help

Examples:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --yes --with-backfill
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --validate-only
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy
  ENV_FILE=.env.production.example ./deploy/scripts/prod-realtime-cutover-apply.sh --dry-run --with-backfill
EOF
}

log() {
  printf '%s [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" "$2"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --yes) ASSUME_YES=1 ;;
      --dry-run) DRY_RUN=1 ;;
      --from-date)
        [[ $# -ge 2 ]] || { echo "ERROR: --from-date requires a value" >&2; exit 2; }
        FROM_DATE="$2"; shift ;;
      --id-empresa)
        [[ $# -ge 2 ]] || { echo "ERROR: --id-empresa requires a value" >&2; exit 2; }
        ID_EMPRESA="$2"; shift ;;
      --id-filial)
        [[ $# -ge 2 ]] || { echo "ERROR: --id-filial requires a value" >&2; exit 2; }
        ID_FILIAL="$2"; shift ;;
      --with-backfill) WITH_BACKFILL=1 ;;
      --validate-only) VALIDATE_ONLY=1 ;;
      --rollback-to-legacy) ROLLBACK_TO_LEGACY=1 ;;
      --source)
        [[ $# -ge 2 ]] || { echo "ERROR: --source requires stg or dw" >&2; exit 2; }
        SOURCE="$2"; shift ;;
      --help|-h) usage; exit 0 ;;
      *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
  done
  SOURCE="$(printf '%s' "$SOURCE" | tr '[:upper:]' '[:lower:]')"
  [[ "$SOURCE" == "stg" || "$SOURCE" == "dw" ]] || { echo "ERROR: --source must be stg or dw" >&2; exit 2; }
}

run() {
  log INFO "RUN $*"
  if (( DRY_RUN )); then return 0; fi
  "$@"
}

compose_prod() {
  docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

compose_streaming() {
  docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

ch_query() {
  local sql="$1"
  local out
  if ! out="$(compose_prod exec -T clickhouse clickhouse-client \
    --user "${CLICKHOUSE_USER:-torqmind}" --password "${CLICKHOUSE_PASSWORD:-}" \
    --format=TabSeparated -q "$sql" 2>/dev/null)"; then
    printf '__ERROR__'
    return 0
  fi
  printf '%s' "${out//[[:space:]]/}"
}

pg_scalar() {
  local sql="$1"
  local out
  if ! out="$(compose_prod exec -T postgres psql \
    -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-TORQMIND}" \
    -tAc "$sql" 2>/dev/null)"; then
    printf '__ERROR__'
    return 0
  fi
  printf '%s' "${out//[[:space:]]/}"
}

wait_for_ch_positive() {
  local label="$1"
  local sql="$2"
  local max_wait="${3:-120}"
  local interval=5
  local elapsed=0

  while (( elapsed < max_wait )); do
    local value
    value="$(ch_query "$sql")"
    if [[ "$value" == "__ERROR__" ]]; then
      log WARN "$label query failed, retrying (${elapsed}s/${max_wait}s)"
    elif (( value > 0 )); then
      log INFO "$label=$value after ${elapsed}s"
      return 0
    else
      log INFO "Waiting for $label... (${elapsed}s/${max_wait}s)"
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  log ERROR "$label did not become positive within ${max_wait}s"
  return 1
}

step_preflight() {
  log INFO "=== STEP 1: Preflight ==="
  [[ -f "$ROOT_DIR/docker-compose.prod.yml" ]] || { echo "ERROR: not in repo root" >&2; exit 1; }
  [[ -f "$ENV_FILE" ]] || { echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2; exit 1; }
  tm_load_env_file "$ENV_FILE"
  : "${CLICKHOUSE_USER:=torqmind}"
  : "${CLICKHOUSE_PASSWORD:=}"
  : "${POSTGRES_USER:=postgres}"
  : "${POSTGRES_DB:=TORQMIND}"
  command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found" >&2; exit 1; }
  run docker compose version
  run find "$ROOT_DIR/deploy/scripts" -maxdepth 1 -type f -name '*.sh' -exec chmod +x {} +
  log INFO "preflight=OK env_file=$ENV_FILE from_date=$FROM_DATE id_empresa=$ID_EMPRESA source=$SOURCE"
}

step_validate_compose() {
  log INFO "=== STEP 2: Validate Compose ==="
  run compose_prod config --quiet
  run compose_streaming config --quiet
  log INFO "compose_validation=OK"
}

step_build() {
  log INFO "=== STEP 3: Build services ==="
  run compose_prod build api web
  run compose_streaming build cdc-consumer
  log INFO "build=OK"
}

step_migrate() {
  log INFO "=== STEP 4: PostgreSQL migration ==="
  run env ENV_FILE="$ENV_FILE" "$ROOT_DIR/deploy/scripts/prod-migrate.sh"
  log INFO "migrate=OK"
}

step_init_clickhouse_streaming() {
  log INFO "=== STEP 5: Init ClickHouse streaming schemas ==="
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-init-clickhouse.sh"
  # Also init mart_rt
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-init-mart-rt.sh"
  log INFO "clickhouse_streaming_init=OK"
}

step_prepare_postgres() {
  log INFO "=== STEP 6: Prepare PostgreSQL publication/slot ==="
  if [[ -f "$ROOT_DIR/deploy/scripts/streaming-prepare-postgres.sh" ]]; then
    run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-prepare-postgres.sh"
  else
    log WARN "streaming-prepare-postgres.sh not found; assuming publication/slot already exist"
  fi
  log INFO "postgres_preparation=OK"
}

step_start_streaming() {
  log INFO "=== STEP 7: Start streaming stack ==="
  run compose_streaming up -d --wait
  log INFO "streaming_stack=UP"
}

step_register_debezium() {
  log INFO "=== STEP 8: Register Debezium connector ==="
  run env ENV_FILE="$ENV_FILE" "$ROOT_DIR/deploy/scripts/streaming-register-debezium.sh"
  log INFO "debezium_connector=REGISTERED"
}

step_backfill() {
  if (( ! WITH_BACKFILL )); then
    log INFO "=== STEP 9: Backfill SKIPPED ==="
    return 0
  fi
  log INFO "=== STEP 9: Backfill mart_rt ==="

  # Wait for Debezium to be RUNNING and data to flow (condition-based, not sleep)
  log INFO "Waiting for Debezium connector to be RUNNING and initial data to flow..."
  if (( ! DRY_RUN )); then
    local max_wait=120
    local elapsed=0
    local interval=5

    # Wait for Debezium connector RUNNING
    while (( elapsed < max_wait )); do
      local status
      status="$(compose_streaming exec -T debezium-connect curl -sf http://localhost:8083/connectors/torqmind-postgres-cdc/status 2>/dev/null | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || echo "UNKNOWN")"
      if [[ "$status" == "RUNNING" ]]; then
        log INFO "Debezium connector RUNNING after ${elapsed}s"
        break
      fi
      log INFO "  Debezium status=$status, waiting... (${elapsed}s/${max_wait}s)"
      sleep "$interval"
      elapsed=$((elapsed + interval))
    done
    if (( elapsed >= max_wait )); then
      log ERROR "Debezium connector did not reach RUNNING state within ${max_wait}s"
      exit 1
    fi

    # Wait for at least some events in raw
    elapsed=0
    while (( elapsed < max_wait )); do
      local raw_count
      raw_count="$(compose_prod exec -T clickhouse clickhouse-client \
        --user "${CLICKHOUSE_USER:-torqmind}" --password "${CLICKHOUSE_PASSWORD:-}" \
        --format=TabSeparated -q "SELECT count() FROM torqmind_raw.cdc_events" 2>/dev/null || echo "0")"
      raw_count="${raw_count//[[:space:]]/}"
      if (( raw_count > 0 )); then
        log INFO "Raw events present: $raw_count rows after ${elapsed}s"
        break
      fi
      log INFO "  Waiting for CDC events in raw... (${elapsed}s/${max_wait}s)"
      sleep "$interval"
      elapsed=$((elapsed + interval))
    done
    if (( elapsed >= max_wait )); then
      log WARN "No raw CDC events after ${max_wait}s — proceeding with backfill from current"
    fi

    # Wait for current tables to have source data
    elapsed=0
    local current_source_table="fact_venda"
    if [[ "$SOURCE" == "stg" ]]; then
      current_source_table="stg_comprovantes"
    fi
    while (( elapsed < max_wait )); do
      local current_count
      current_count="$(compose_prod exec -T clickhouse clickhouse-client \
        --user "${CLICKHOUSE_USER:-torqmind}" --password "${CLICKHOUSE_PASSWORD:-}" \
        --format=TabSeparated -q "SELECT count() FROM torqmind_current.${current_source_table} FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null || echo "0")"
      current_count="${current_count//[[:space:]]/}"
      if (( current_count > 0 )); then
        log INFO "Current ${current_source_table} has data: $current_count rows"
        break
      fi
      log INFO "  Waiting for current.${current_source_table} data... (${elapsed}s/${max_wait}s)"
      sleep "$interval"
      elapsed=$((elapsed + interval))
    done
    if (( elapsed >= max_wait )); then
      log ERROR "No data in torqmind_current.${current_source_table} after ${max_wait}s. Cannot backfill."
      exit 1
    fi
  fi

  # Run mart builder backfill via the CDC consumer CLI
  local backfill_command="backfill"
  if [[ "$SOURCE" == "stg" ]]; then
    backfill_command="backfill-stg"
  fi
  local backfill_cmd=(
    docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE"
    exec -T cdc-consumer env REALTIME_MARTS_SOURCE="$SOURCE"
    python -m torqmind_cdc_consumer.cli "$backfill_command"
    --from-date "$FROM_DATE" --id-empresa "$ID_EMPRESA"
  )
  if [[ "$backfill_command" == "backfill" ]]; then
    backfill_cmd+=(--source "$SOURCE")
  fi
  if [[ -n "$ID_FILIAL" ]]; then
    backfill_cmd+=(--id-filial "$ID_FILIAL")
  fi
  run "${backfill_cmd[@]}"

  # Verify backfill produced rows
  if (( ! DRY_RUN )); then
    local mart_rows
    mart_rows="$(compose_prod exec -T clickhouse clickhouse-client \
      --user "${CLICKHOUSE_USER:-torqmind}" --password "${CLICKHOUSE_PASSWORD:-}" \
      --format=TabSeparated -q "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" 2>/dev/null || echo "0")"
    mart_rows="${mart_rows//[[:space:]]/}"
    if (( mart_rows == 0 )); then
      log ERROR "Backfill produced 0 rows in sales_daily_rt. Aborting cutover."
      exit 1
    fi
    log INFO "Backfill verified: sales_daily_rt has $mart_rows rows"
  fi

  log INFO "backfill=OK"
}

step_verify_streaming_readiness() {
  log INFO "=== STEP 9B: Verify streaming data readiness ==="
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would verify Redpanda/Debezium/CDC/current/mart_rt readiness"
    return 0
  fi

  local running_services
  running_services="$(compose_streaming ps --status=running --services)"
  local service
  for service in redpanda debezium-connect cdc-consumer; do
    if ! grep -qx "$service" <<<"$running_services"; then
      log ERROR "Streaming service $service is not RUNNING"
      compose_streaming ps || true
      exit 1
    fi
  done
  log INFO "streaming_services=RUNNING"

  local max_wait=120
  local elapsed=0
  local interval=5
  while (( elapsed < max_wait )); do
    local status
    status="$(compose_streaming exec -T debezium-connect curl -sf http://localhost:8083/connectors/torqmind-postgres-cdc/status 2>/dev/null | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || echo "UNKNOWN")"
    if [[ "$status" == "RUNNING" ]]; then
      log INFO "debezium_connector=RUNNING"
      break
    fi
    log INFO "Waiting for Debezium connector RUNNING (status=$status, ${elapsed}s/${max_wait}s)"
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done
  if (( elapsed >= max_wait )); then
    log ERROR "Debezium connector did not reach RUNNING within ${max_wait}s"
    compose_streaming logs debezium-connect --tail=80 || true
    exit 1
  fi

  local source_schema current_prefix
  local -a cdc_tables
  if [[ "$SOURCE" == "stg" ]]; then
    source_schema="stg"
    current_prefix="stg_"
    cdc_tables=(
      comprovantes
      itenscomprovantes
      formas_pgto_comprovantes
      turnos
      entidades
      produtos
      grupoprodutos
      funcionarios
      usuarios
      localvendas
      contaspagar
      contasreceber
    )
  else
    source_schema="dw"
    current_prefix=""
    cdc_tables=(
      fact_venda
      fact_venda_item
      fact_pagamento_comprovante
      fact_comprovante
      fact_caixa_turno
      fact_financeiro
      fact_risco_evento
      dim_filial
      dim_produto
      dim_grupo_produto
      dim_funcionario
      dim_usuario_caixa
      dim_cliente
    )
  fi

  local table source_count
  for table in "${cdc_tables[@]}"; do
    source_count="$(pg_scalar "SELECT count(*) FROM ${source_schema}.${table} WHERE id_empresa=${ID_EMPRESA}")"
    if [[ "$source_count" == "__ERROR__" ]]; then
      log ERROR "Could not read PostgreSQL source count for ${source_schema}.${table}"
      exit 1
    fi
    if (( source_count == 0 )); then
      log INFO "source ${source_schema}.${table}=0; raw/current positive check not required"
      continue
    fi
    wait_for_ch_positive "raw.${table}" \
      "SELECT count() FROM torqmind_raw.cdc_events WHERE table_schema='${source_schema}' AND table_name='${table}' AND id_empresa=${ID_EMPRESA}" \
      120
    wait_for_ch_positive "current.${table}" \
      "SELECT count() FROM torqmind_current.${current_prefix}${table} FINAL WHERE id_empresa=${ID_EMPRESA} AND is_deleted=0" \
      120
  done

  wait_for_ch_positive "mart_rt.sales_daily_rt" \
    "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=${ID_EMPRESA}" \
    120
  log INFO "streaming_data_readiness=OK"
}

step_validate_parity() {
  log INFO "=== STEP 10: Validate parity (BLOQUEANTE) ==="
  if [[ ! -f "$ROOT_DIR/deploy/scripts/realtime-validate-cutover.sh" ]]; then
    log ERROR "realtime-validate-cutover.sh not found — cannot validate. Aborting."
    exit 1
  fi
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" "$ROOT_DIR/deploy/scripts/realtime-validate-cutover.sh" --source "$SOURCE"
  log INFO "parity_validation=PASSED"
}

step_activate_realtime() {
  if (( VALIDATE_ONLY )); then
    log INFO "=== STEP 11: Activate SKIPPED (validate-only mode) ==="
    return 0
  fi
  log INFO "=== STEP 11: Activate USE_REALTIME_MARTS=true ==="
  # Set the flag in the environment file
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would set USE_REALTIME_MARTS=true REALTIME_MARTS_SOURCE=$SOURCE REALTIME_MARTS_FALLBACK=false in $ENV_FILE"
    return 0
  fi

  # First set fallback=false for clean validation (no masking)
  if grep -q "^REALTIME_MARTS_FALLBACK=" "$ENV_FILE" 2>/dev/null; then
    sed -i 's/^REALTIME_MARTS_FALLBACK=.*/REALTIME_MARTS_FALLBACK=false/' "$ENV_FILE"
  else
    echo "REALTIME_MARTS_FALLBACK=false" >> "$ENV_FILE"
  fi

  if grep -q "^USE_REALTIME_MARTS=" "$ENV_FILE" 2>/dev/null; then
    sed -i 's/^USE_REALTIME_MARTS=.*/USE_REALTIME_MARTS=true/' "$ENV_FILE"
  else
    echo "USE_REALTIME_MARTS=true" >> "$ENV_FILE"
  fi
  if grep -q "^REALTIME_MARTS_SOURCE=" "$ENV_FILE" 2>/dev/null; then
    sed -i "s/^REALTIME_MARTS_SOURCE=.*/REALTIME_MARTS_SOURCE=$SOURCE/" "$ENV_FILE"
  else
    echo "REALTIME_MARTS_SOURCE=$SOURCE" >> "$ENV_FILE"
  fi
  log INFO "realtime_marts=ACTIVATED source=$SOURCE fallback=false"
}

step_rollback_to_legacy() {
  log INFO "=== ROLLBACK: Disabling realtime marts ==="
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would set USE_REALTIME_MARTS=false in $ENV_FILE"
    return 0
  fi
  if grep -q "^USE_REALTIME_MARTS=" "$ENV_FILE" 2>/dev/null; then
    sed -i 's/^USE_REALTIME_MARTS=.*/USE_REALTIME_MARTS=false/' "$ENV_FILE"
  else
    echo "USE_REALTIME_MARTS=false" >> "$ENV_FILE"
  fi
  # Restart API to pick up the change
  run compose_prod up -d --no-deps --force-recreate api
  log INFO "rollback=DONE use_realtime_marts=false"
}

step_rebuild_api() {
  if (( VALIDATE_ONLY )); then
    log INFO "=== STEP 12: Rebuild API SKIPPED (validate-only mode) ==="
    return 0
  fi
  log INFO "=== STEP 12: Rebuild API/Web with realtime flag ==="
  run compose_prod up -d --no-deps --force-recreate api web
  log INFO "api_web_rebuild=OK"
}

step_smoke() {
  if (( VALIDATE_ONLY )); then
    log INFO "=== STEP 13: Smoke SKIPPED (validate-only mode) ==="
    return 0
  fi
  log INFO "=== STEP 13: Smoke endpoints (fallback=false) ==="
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would test API health and BI endpoints with fallback=false"
    return 0
  fi
  # Wait for API to become healthy
  local retries=0
  while (( retries < 30 )); do
    if compose_prod exec -T api python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" 2>/dev/null; then
      break
    fi
    retries=$((retries + 1))
    sleep 2
  done
  if (( retries >= 30 )); then
    log ERROR "API did not become healthy within 60s"
    exit 1
  fi
  log INFO "api_health=OK"

  # Smoke test: call a BI endpoint that goes through realtime path
  # This validates that with USE_REALTIME_MARTS=true and FALLBACK=false, the API serves data
  local smoke_result
  smoke_result="$(compose_prod exec -T api env \
    USE_REALTIME_MARTS=true \
    REALTIME_MARTS_SOURCE="$SOURCE" \
    REALTIME_MARTS_FALLBACK=false \
    USE_CLICKHOUSE=true \
    ID_EMPRESA="$ID_EMPRESA" \
    python - <<'PY' 2>&1 || echo "SMOKE_FAILED"
import inspect
import os
from datetime import date, timedelta

from app.config import settings

assert settings.use_realtime_marts is True, "USE_REALTIME_MARTS is not active"
assert settings.realtime_marts_fallback is False, "fallback must be disabled"
assert settings.realtime_marts_source == os.environ["REALTIME_MARTS_SOURCE"], "source flag must be active"

from app import repos_analytics
from app import repos_mart_realtime as rt

for fn_name in sorted(rt.REALTIME_FUNCTIONS):
    fn = getattr(rt, fn_name)
    sig = inspect.signature(fn)
    params = list(sig.parameters.keys())
    if fn_name != "streaming_health":
        assert params[0] == "role", f"{fn_name}: first param should be role, got {params[0]}"
        assert params[1] == "id_empresa", f"{fn_name}: second param should be id_empresa, got {params[1]}"
        assert params[2] == "id_filial", f"{fn_name}: third param should be id_filial, got {params[2]}"

dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=30)
payload = getattr(repos_analytics, "dashboard_kpis")(
    "admin",
    int(os.environ["ID_EMPRESA"]),
    None,
    dt_ini,
    dt_fim,
)
assert isinstance(payload, dict), "facade did not return dashboard_kpis payload"
print("SMOKE_OK")
PY
)"

  if [[ "$smoke_result" != *"SMOKE_OK"* ]]; then
    log ERROR "Smoke test FAILED: $smoke_result"
    log ERROR "Realtime path not working with fallback=false. Rolling back."
    step_rollback_to_legacy
    exit 1
  fi
  log INFO "smoke_test=PASSED (realtime path validated with fallback=false)"
}

step_report() {
  log INFO "============================================"
  if (( DRY_RUN == 1 )); then
    log INFO "  REALTIME CUTOVER DRY-RUN COMPLETE"
  else
    log INFO "  REALTIME CUTOVER COMPLETE"
  fi
  log INFO "============================================"
  log INFO "env_file=$ENV_FILE"
  log INFO "from_date=$FROM_DATE"
  log INFO "id_empresa=$ID_EMPRESA"
  log INFO "with_backfill=$WITH_BACKFILL"
  log INFO "source=$SOURCE"
  log INFO "validate_only=$VALIDATE_ONLY"
  log INFO "dry_run=$DRY_RUN"
  log INFO ""
  log INFO "Next steps:"
  log INFO "  - Monitor Redpanda Console: http://localhost:18080"
  log INFO "  - Check CDC lag: make streaming-status"
  log INFO "  - Validate marts: make realtime-validate"
  log INFO "  - Rollback: ENV_FILE=$ENV_FILE ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy"
  log INFO "============================================"
}

# ===== MAIN =====
parse_args "$@"

if (( ROLLBACK_TO_LEGACY )); then
  step_preflight
  step_rollback_to_legacy
  exit 0
fi

step_preflight
step_validate_compose
step_build
step_migrate
step_init_clickhouse_streaming
step_prepare_postgres
step_start_streaming
step_register_debezium
step_backfill
step_verify_streaming_readiness
step_validate_parity
step_activate_realtime
step_rebuild_api
step_smoke
step_report
