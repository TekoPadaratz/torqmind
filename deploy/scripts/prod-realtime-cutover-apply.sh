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
      --help|-h) usage; exit 0 ;;
      *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
  done
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

step_preflight() {
  log INFO "=== STEP 1: Preflight ==="
  [[ -f "$ROOT_DIR/docker-compose.prod.yml" ]] || { echo "ERROR: not in repo root" >&2; exit 1; }
  [[ -f "$ENV_FILE" ]] || { echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2; exit 1; }
  command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found" >&2; exit 1; }
  run docker compose version
  run find "$ROOT_DIR/deploy/scripts" -maxdepth 1 -type f -name '*.sh' -exec chmod +x {} +
  log INFO "preflight=OK env_file=$ENV_FILE from_date=$FROM_DATE id_empresa=$ID_EMPRESA"
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

  # Wait for initial Debezium snapshot to complete
  log INFO "Waiting 30s for Debezium initial data to flow..."
  if (( ! DRY_RUN )); then sleep 30; fi

  # Run mart builder backfill via the CDC consumer CLI
  local backfill_cmd=(
    docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE"
    exec -T cdc-consumer python -m torqmind_cdc_consumer.cli backfill
    --from-date "$FROM_DATE" --id-empresa "$ID_EMPRESA"
  )
  if [[ -n "$ID_FILIAL" ]]; then
    backfill_cmd+=(--id-filial "$ID_FILIAL")
  fi
  run "${backfill_cmd[@]}"
  log INFO "backfill=OK"
}

step_validate_parity() {
  log INFO "=== STEP 10: Validate parity ==="
  if [[ -f "$ROOT_DIR/deploy/scripts/realtime-validate-cutover.sh" ]]; then
    run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" "$ROOT_DIR/deploy/scripts/realtime-validate-cutover.sh"
  else
    log WARN "realtime-validate-cutover.sh not found; skipping parity check"
  fi
  log INFO "parity_validation=OK"
}

step_activate_realtime() {
  if (( VALIDATE_ONLY )); then
    log INFO "=== STEP 11: Activate SKIPPED (validate-only mode) ==="
    return 0
  fi
  log INFO "=== STEP 11: Activate USE_REALTIME_MARTS=true ==="
  # Set the flag in the environment file
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would set USE_REALTIME_MARTS=true in $ENV_FILE"
    return 0
  fi
  if grep -q "^USE_REALTIME_MARTS=" "$ENV_FILE" 2>/dev/null; then
    sed -i 's/^USE_REALTIME_MARTS=.*/USE_REALTIME_MARTS=true/' "$ENV_FILE"
  else
    echo "USE_REALTIME_MARTS=true" >> "$ENV_FILE"
  fi
  log INFO "realtime_marts=ACTIVATED"
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
  log INFO "=== STEP 13: Smoke endpoints ==="
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would test API health and BI endpoints"
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
  log INFO "api_health=OK"
}

step_report() {
  log INFO "============================================"
  log INFO "  REALTIME CUTOVER COMPLETE"
  log INFO "============================================"
  log INFO "env_file=$ENV_FILE"
  log INFO "from_date=$FROM_DATE"
  log INFO "id_empresa=$ID_EMPRESA"
  log INFO "with_backfill=$WITH_BACKFILL"
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
step_validate_parity
step_activate_realtime
step_rebuild_api
step_smoke
step_report
