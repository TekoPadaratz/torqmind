#!/usr/bin/env bash
set -Eeuo pipefail

# TorqMind Realtime Cutover Apply
# One-command script to prepare and activate the realtime event-driven pipeline.
#
# Modes:
#   (default)       Full cutover: build → migrate → init → stream → bootstrap → backfill → validate → activate
#   --validate-only Read-only validation of an already-prepared environment
#   --dry-run       Print actions without executing any mutations
#   --rollback-to-legacy  Disable realtime, re-enable legacy (does NOT restart cron)
#
# Flow (full cutover):
#   1. Preflight checks
#   2. Validate env and compose
#   3. Neutralize legacy ETL cron
#   4. Build API/Web/Consumer
#   5. Migrate PostgreSQL
#   6. Init ClickHouse streaming schemas (raw/current/ops/mart_rt)
#   7. Prepare PostgreSQL publication/slot
#   8. Start Redpanda/Debezium/Consumer
#   9. Register Debezium connector
#  10. Bootstrap STG (realtime-bootstrap-stg.sh)
#  11. MartBuilder backfill
#  12. Validate parity (BLOQUEANTE)
#  13. Activate realtime env vars
#  14. Rebuild API/Web
#  15. Smoke endpoints (fallback=false)
#  16. Report

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROD_COMPOSE_FILE="docker-compose.prod.yml"
STREAMING_COMPOSE_FILE="docker-compose.streaming.yml"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
LOG_DIR="${LOG_DIR:-/home/deploy/logs}"

DRY_RUN=0
ASSUME_YES=0
FROM_DATE="2025-01-01"
ID_EMPRESA=1
ID_FILIAL=""            # audit/smoke/validation filial (does NOT limit backfill)
BACKFILL_ID_FILIAL=""   # optional: limit backfill to specific filial
ALL_FILIAIS=0           # explicit flag: backfill all filiais (default behavior)
WITH_BACKFILL=0
VALIDATE_ONLY=0
ROLLBACK_TO_LEGACY=0
SOURCE="stg"
BOOTSTRAP_STG=1  # default: enabled for source=stg + with-backfill
SKIP_BOOTSTRAP_STG=0
KILL_LEGACY_ETL=0
MART_ONLY=0
SKIP_SLIM=0
RESET_MART_RT=""
BACKFILL_BATCH_SIZE=""
BACKFILL_MAX_THREADS=""
BACKFILL_MAX_MEMORY_GB=""

source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh [flags]

Flags:
  --yes                       Skip confirmations
  --dry-run                   Print actions without executing
  --from-date YYYY-MM-DD      Backfill start date (default 2025-01-01)
  --id-empresa <id>           Tenant (default 1)
  --id-filial <id>            Audit/smoke filial (does NOT limit backfill)
  --backfill-id-filial <id>   Limit backfill to specific filial (default: all)
  --all-filiais               Explicit: backfill covers all filiais (default)
  --with-backfill             Run bootstrap + mart_rt backfill from current data
  --validate-only             ONLY validate already-prepared environment (non-mutating)
  --rollback-to-legacy        Disable realtime marts and revert to legacy
  --source stg|dw             Realtime source (default stg)
  --skip-bootstrap-stg        Skip STG bootstrap even with --with-backfill
  --kill-legacy-etl           Force-kill running legacy ETL processes
  --help

  NOTE: --id-filial is for audit/validation/smoke only. It does NOT scope
  the bootstrap or MartBuilder backfill. Use --backfill-id-filial to limit
  the backfill to a specific branch. For full production cutover, do NOT
  pass --backfill-id-filial (all filiais are processed by default).

Validate-only mode:
  Does NOT build, migrate, start services, register connectors, or alter env.
  Only validates: containers, connector, raw/current data, mart_rt, API fallback=false.

Examples:
  # Full cutover (all filiais)
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --yes --with-backfill --all-filiais

  # Cutover with backfill limited to one filial (testing)
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --with-backfill --backfill-id-filial 14458

  # Validate existing environment (audit focused on specific filial)
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --validate-only --id-filial 14458

  # Dry-run
  ENV_FILE=.env.production.example ./deploy/scripts/prod-realtime-cutover-apply.sh --dry-run --with-backfill --id-filial 14458 --all-filiais

  # Rollback
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy
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
      --backfill-id-filial)
        [[ $# -ge 2 ]] || { echo "ERROR: --backfill-id-filial requires a value" >&2; exit 2; }
        BACKFILL_ID_FILIAL="$2"; shift ;;
      --all-filiais) ALL_FILIAIS=1 ;;
      --with-backfill) WITH_BACKFILL=1 ;;
      --validate-only) VALIDATE_ONLY=1 ;;
      --rollback-to-legacy) ROLLBACK_TO_LEGACY=1 ;;
      --source)
        [[ $# -ge 2 ]] || { echo "ERROR: --source requires stg or dw" >&2; exit 2; }
        SOURCE="$2"; shift ;;
      --skip-bootstrap-stg) SKIP_BOOTSTRAP_STG=1 ;;
      --kill-legacy-etl) KILL_LEGACY_ETL=1 ;;
      --mart-only) MART_ONLY=1 ;;
      --skip-slim) SKIP_SLIM=1 ;;
      --reset-mart-rt) RESET_MART_RT="truncate" ;;
      --drop-recreate-mart-rt) RESET_MART_RT="drop-recreate" ;;
      --backfill-batch-size)
        [[ $# -ge 2 ]] || { echo "ERROR: --backfill-batch-size requires a value" >&2; exit 2; }
        BACKFILL_BATCH_SIZE="$2"; shift ;;
      --backfill-max-threads)
        [[ $# -ge 2 ]] || { echo "ERROR: --backfill-max-threads requires a value" >&2; exit 2; }
        BACKFILL_MAX_THREADS="$2"; shift ;;
      --backfill-max-memory-gb)
        [[ $# -ge 2 ]] || { echo "ERROR: --backfill-max-memory-gb requires a value" >&2; exit 2; }
        BACKFILL_MAX_MEMORY_GB="$2"; shift ;;
      --help|-h) usage; exit 0 ;;
      *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
  done
  SOURCE="$(printf '%s' "$SOURCE" | tr '[:upper:]' '[:lower:]')"
  [[ "$SOURCE" == "stg" || "$SOURCE" == "dw" ]] || { echo "ERROR: --source must be stg or dw" >&2; exit 2; }

  # Validate --backfill-id-filial is numeric if set
  if [[ -n "$BACKFILL_ID_FILIAL" ]] && ! [[ "$BACKFILL_ID_FILIAL" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --backfill-id-filial must be numeric, got: $BACKFILL_ID_FILIAL" >&2
    exit 2
  fi

  # Conflict: --all-filiais and --backfill-id-filial cannot be used together
  if (( ALL_FILIAIS )) && [[ -n "$BACKFILL_ID_FILIAL" ]]; then
    echo "ERROR: --all-filiais and --backfill-id-filial are mutually exclusive" >&2
    exit 2
  fi

  # Resolve bootstrap default: enabled for source=stg + with-backfill, unless explicitly skipped
  if [[ "$SOURCE" == "stg" ]] && (( WITH_BACKFILL )) && (( ! SKIP_BOOTSTRAP_STG )); then
    BOOTSTRAP_STG=1
  else
    BOOTSTRAP_STG=0
  fi
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
  local out err_out
  local err_file="/tmp/ch_query_err_$$.txt"
  if ! out="$(compose_prod exec -T clickhouse clickhouse-client \
    --user "${CLICKHOUSE_USER:-torqmind}" --password "${CLICKHOUSE_PASSWORD:-}" \
    --format=TabSeparated --send_logs_level=error -q "$sql" 2>"$err_file")"; then
    local err_msg
    err_msg="$(head -3 "$err_file" 2>/dev/null || echo 'unknown error')"
    rm -f "$err_file"
    log ERROR "ClickHouse query failed: $err_msg"
    log ERROR "  SQL: $sql"
    printf '__ERROR__'
    return 0
  fi
  rm -f "$err_file"
  printf '%s' "${out//[[:space:]]/}"
}

pg_scalar() {
  local sql="$1"
  local out
  local err_file="/tmp/pg_query_err_$$.txt"
  if ! out="$(compose_prod exec -T postgres psql \
    -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-TORQMIND}" \
    -tAc "$sql" 2>"$err_file")"; then
    local err_msg
    err_msg="$(head -3 "$err_file" 2>/dev/null || echo 'unknown error')"
    rm -f "$err_file"
    log ERROR "PostgreSQL query failed: $err_msg"
    log ERROR "  SQL: $sql"
    printf '__ERROR__'
    return 0
  fi
  rm -f "$err_file"
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

# --- Safe env file update (atomic, backup, sudo-aware) ---

tm_env_set() {
  # Usage: tm_env_set KEY VALUE
  # Safely updates a key=value in ENV_FILE with backup and atomic write.
  local key="$1"
  local value="$2"
  local env_file="$ENV_FILE"

  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would set $key=<REDACTED> in $env_file"
    return 0
  fi

  # Backup with timestamp
  local backup="${env_file}.bak.$(date +%Y%m%d_%H%M%S)"
  local tmpfile="${env_file}.tmp.$$"

  local use_sudo=0
  if [[ ! -w "$env_file" ]]; then
    if command -v sudo >/dev/null 2>&1; then
      use_sudo=1
    else
      log ERROR "Cannot write to $env_file and sudo not available"
      return 1
    fi
  fi

  # Create backup
  if (( use_sudo )); then
    sudo cp -p "$env_file" "$backup"
  else
    cp -p "$env_file" "$backup"
  fi

  # Create temp file with the update
  if grep -q "^${key}=" "$env_file" 2>/dev/null; then
    sed "s|^${key}=.*|${key}=${value}|" "$env_file" > "$tmpfile"
  else
    cp "$env_file" "$tmpfile"
    printf '%s=%s\n' "$key" "$value" >> "$tmpfile"
  fi

  # Atomic move (preserving permissions)
  if (( use_sudo )); then
    sudo cp -p "$env_file" "$tmpfile.perms"
    sudo mv "$tmpfile" "$env_file"
    sudo chmod --reference="$tmpfile.perms" "$env_file" 2>/dev/null || true
    sudo rm -f "$tmpfile.perms"
  else
    mv "$tmpfile" "$env_file"
  fi

  # Validate the write
  local actual
  actual="$(grep "^${key}=" "$env_file" | cut -d= -f2-)"
  if [[ "$actual" != "$value" ]]; then
    log ERROR "Failed to verify $key in $env_file after write"
    # Restore backup
    if (( use_sudo )); then
      sudo mv "$backup" "$env_file"
    else
      mv "$backup" "$env_file"
    fi
    return 1
  fi

  log INFO "env_set: $key updated in $env_file (backup: $backup)"
}

# --- Legacy ETL neutralization ---

LEGACY_ETL_PATTERNS=(
  "prod-etl-pipeline.sh"
  "prod-etl-incremental.sh"
  "prod-homologation-apply.sh"
  "prod-rebuild-derived-from-stg.sh"
  "etl_incremental"
  "etl_orchestrator"
)

# Disable legacy ETL cron entries using awk (safe with any path characters).
# Reads crontab from stdin, writes modified crontab to stdout.
# Only comments lines that are NOT already commented and match a pattern.
_comment_legacy_cron_lines() {
  local ts="$1"
  shift
  local patterns=("$@")

  # Build a single awk-compatible regex from patterns (escape dots for literal match)
  local awk_regex=""
  for p in "${patterns[@]}"; do
    local escaped="${p//./[.]}"
    if [[ -n "$awk_regex" ]]; then
      awk_regex="${awk_regex}|${escaped}"
    else
      awk_regex="$escaped"
    fi
  done

  awk -v tag="# TORQMIND_LEGACY_ETL_DISABLED_${ts} " -v re="$awk_regex" \
    '{ if ($0 ~ re && $0 !~ /^[[:space:]]*#/) { print tag $0 } else { print } }'
}

step_neutralize_legacy_etl() {
  log INFO "=== STEP 3: Neutralize legacy ETL ==="

  local ts
  ts="$(date +%Y%m%d_%H%M%S)"
  local crontab_backup="${LOG_DIR}/crontab_backup_${ts}.txt"

  # 1. Disable cron jobs related to legacy ETL
  if ! crontab -l > "$crontab_backup" 2>/dev/null; then
    log INFO "No crontab installed (OK)"
    # Still check for running processes below
  else
    # Detect active legacy entries (not already commented)
    local has_legacy=0
    for pattern in "${LEGACY_ETL_PATTERNS[@]}"; do
      if grep -v '^\s*#' "$crontab_backup" | grep -qF "$pattern"; then
        has_legacy=1
        break
      fi
    done

    if (( has_legacy )); then
      if (( DRY_RUN )); then
        log INFO "DRY-RUN: would comment legacy cron entries:"
        for pattern in "${LEGACY_ETL_PATTERNS[@]}"; do
          grep -v '^\s*#' "$crontab_backup" | grep -F "$pattern" | while IFS= read -r line; do
            log INFO "  [would disable] $line"
          done
        done
      else
        log INFO "Legacy ETL cron entries found. Commenting out..."
        local new_crontab="/tmp/crontab_new_$$.txt"
        _comment_legacy_cron_lines "$ts" "${LEGACY_ETL_PATTERNS[@]}" \
          < "$crontab_backup" > "$new_crontab"

        crontab "$new_crontab"
        rm -f "$new_crontab"
        log INFO "Legacy cron entries disabled (backup: $crontab_backup)"

        # Validate no active legacy remains
        local post_cron="/tmp/crontab_verify_$$.txt"
        crontab -l > "$post_cron" 2>/dev/null || true
        for pattern in "${LEGACY_ETL_PATTERNS[@]}"; do
          if grep -v '^\s*#' "$post_cron" 2>/dev/null | grep -qF "$pattern"; then
            log ERROR "Legacy cron entry still active after neutralization: $pattern"
            rm -f "$post_cron"
            exit 1
          fi
        done
        rm -f "$post_cron"
        log INFO "Verified: no active legacy ETL cron entries remain"
      fi
    else
      log INFO "No active legacy ETL cron entries found"
    fi
  fi

  # 2. Check for running legacy ETL processes
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would check for running legacy ETL processes"
    log INFO "legacy_etl=DRY_RUN_OK"
    return 0
  fi

  local running_legacy=()
  for pattern in "${LEGACY_ETL_PATTERNS[@]}"; do
    local pids
    pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
    if [[ -n "$pids" ]]; then
      running_legacy+=("$pattern:$pids")
    fi
  done

  if (( ${#running_legacy[@]} > 0 )); then
    log WARN "Running legacy ETL processes detected: ${running_legacy[*]}"
    if (( KILL_LEGACY_ETL )); then
      for entry in "${running_legacy[@]}"; do
        local pids="${entry#*:}"
        log INFO "Killing legacy process: $entry"
        # shellcheck disable=SC2086
        kill $pids 2>/dev/null || true
      done
      sleep 2
      # Verify they're gone
      for pattern in "${LEGACY_ETL_PATTERNS[@]}"; do
        if pgrep -f "$pattern" >/dev/null 2>&1; then
          log ERROR "Could not kill legacy process matching: $pattern"
          exit 1
        fi
      done
      log INFO "Legacy ETL processes terminated"
    else
      log ERROR "Legacy ETL is still running. Use --kill-legacy-etl to force stop, or wait for completion."
      log ERROR "Cannot proceed with cutover while legacy ETL is active."
      exit 1
    fi
  else
    log INFO "No legacy ETL processes running"
  fi

  # 3. Document residual cron role
  log INFO "NOTE: After cutover, only health/reconcile/ops cron jobs should remain."
  log INFO "  Legacy cron (STG→DW→ClickHouse) is NO LONGER the BI motor."
  log INFO "  Realtime pipeline: Agent→STG→Debezium→Redpanda→CDC→MartBuilder→API"
  log INFO "legacy_etl=NEUTRALIZED"
}

# --- Steps ---

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
  # Resolve display labels for filial scope
  local audit_filial_label="${ID_FILIAL:-nenhuma}"
  local backfill_filial_label="${BACKFILL_ID_FILIAL:-todas}"
  log INFO "preflight=OK env_file=$ENV_FILE from_date=$FROM_DATE id_empresa=$ID_EMPRESA source=$SOURCE"
  log INFO "audit_filial=$audit_filial_label backfill_filial=$backfill_filial_label"
}

step_validate_compose() {
  log INFO "=== STEP 2: Validate Compose ==="
  run compose_prod config --quiet
  run compose_streaming config --quiet
  log INFO "compose_validation=OK"
}

step_build() {
  log INFO "=== STEP 4: Build services ==="
  run compose_prod build api web
  run compose_streaming build cdc-consumer
  log INFO "build=OK"
}

step_migrate() {
  log INFO "=== STEP 5: PostgreSQL migration ==="
  run env ENV_FILE="$ENV_FILE" "$ROOT_DIR/deploy/scripts/prod-migrate.sh"
  log INFO "migrate=OK"
}

step_init_clickhouse_streaming() {
  log INFO "=== STEP 6: Init ClickHouse streaming schemas ==="
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-init-clickhouse.sh"
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-init-mart-rt.sh"
  log INFO "clickhouse_streaming_init=OK"
}

step_prepare_postgres() {
  log INFO "=== STEP 7: Prepare PostgreSQL publication/slot ==="
  if [[ -f "$ROOT_DIR/deploy/scripts/streaming-prepare-postgres.sh" ]]; then
    run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-prepare-postgres.sh"
  else
    log WARN "streaming-prepare-postgres.sh not found; assuming publication/slot already exist"
  fi
  log INFO "postgres_preparation=OK"
}

step_start_streaming() {
  log INFO "=== STEP 8: Start streaming stack ==="
  run compose_streaming up -d --wait
  log INFO "streaming_stack=UP"
}

step_register_debezium() {
  log INFO "=== STEP 9: Register Debezium connector ==="
  run env ENV_FILE="$ENV_FILE" "$ROOT_DIR/deploy/scripts/streaming-register-debezium.sh"
  log INFO "debezium_connector=REGISTERED"
}

step_bootstrap_stg() {
  if (( ! BOOTSTRAP_STG )); then
    log INFO "=== STEP 10: Bootstrap STG SKIPPED ==="
    return 0
  fi
  log INFO "=== STEP 10: Bootstrap STG (historical data → ClickHouse current) ==="

  local bootstrap_args=(
    --id-empresa "$ID_EMPRESA"
    --from-date "$FROM_DATE"
    --skip-mart-backfill  # mart backfill is done separately in step 11
  )

  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" \
    "$ROOT_DIR/deploy/scripts/realtime-bootstrap-stg.sh" "${bootstrap_args[@]}"

  log INFO "bootstrap_stg=OK"
}

step_backfill() {
  if (( ! WITH_BACKFILL )); then
    log INFO "=== STEP 11: Backfill SKIPPED ==="
    return 0
  fi
  log INFO "=== STEP 11: MartBuilder backfill ==="

  # Wait for Debezium to be RUNNING
  if (( ! DRY_RUN )); then
    log INFO "Waiting for Debezium connector to be RUNNING..."
    local max_wait=120
    local elapsed=0
    local interval=5

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

    # Verify current has data (bootstrap should have populated it)
    local current_source_table="stg_comprovantes"
    [[ "$SOURCE" == "dw" ]] && current_source_table="fact_venda"
    local current_count
    current_count="$(ch_query "SELECT count() FROM torqmind_current.${current_source_table} FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0")"
    if [[ "$current_count" == "0" || "$current_count" == "__ERROR__" ]]; then
      log ERROR "No data in torqmind_current.${current_source_table}. Bootstrap may have failed."
      exit 1
    fi
    log INFO "Current ${current_source_table} has data: $current_count rows"
  fi

  # Reset mart_rt if requested
  if [[ -n "$RESET_MART_RT" ]]; then
    log INFO "Resetting mart_rt ($RESET_MART_RT) before backfill..."
    local reset_flags="--$RESET_MART_RT --yes"
    run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" \
      bash "$ROOT_DIR/deploy/scripts/realtime-reset-mart-rt.sh" $reset_flags
  fi

  # Run mart builder backfill
  local backfill_command="backfill"
  [[ "$SOURCE" == "stg" ]] && backfill_command="backfill-stg"
  local backfill_cmd=(
    docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE"
    exec -T cdc-consumer env REALTIME_MARTS_SOURCE="$SOURCE"
    python -m torqmind_cdc_consumer.cli "$backfill_command"
    --from-date "$FROM_DATE" --id-empresa "$ID_EMPRESA"
  )
  [[ "$backfill_command" == "backfill" ]] && backfill_cmd+=(--source "$SOURCE")
  [[ -n "$BACKFILL_ID_FILIAL" ]] && backfill_cmd+=(--id-filial "$BACKFILL_ID_FILIAL")
  # mart-only / skip-slim flags
  if (( MART_ONLY || SKIP_SLIM )); then
    backfill_cmd+=(--mart-only)
    # skip batch deletes when mart was just reset
    [[ -n "$RESET_MART_RT" ]] && backfill_cmd+=(--skip-batch-deletes)
  fi
  # Performance tuning
  [[ -n "$BACKFILL_BATCH_SIZE" ]] && backfill_cmd+=(--batch-size "$BACKFILL_BATCH_SIZE")
  [[ -n "$BACKFILL_MAX_THREADS" ]] && backfill_cmd+=(--max-threads "$BACKFILL_MAX_THREADS")
  [[ -n "$BACKFILL_MAX_MEMORY_GB" ]] && backfill_cmd+=(--max-memory-gb "$BACKFILL_MAX_MEMORY_GB")
  run "${backfill_cmd[@]}"

  # Verify backfill produced rows
  if (( ! DRY_RUN )); then
    local mart_rows
    mart_rows="$(ch_query "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA")"
    if [[ "$mart_rows" == "0" || "$mart_rows" == "__ERROR__" ]]; then
      log ERROR "Backfill produced 0 rows in sales_daily_rt. Aborting cutover."
      exit 1
    fi
    log INFO "Backfill verified: sales_daily_rt has $mart_rows rows"
  fi

  log INFO "backfill=OK"
}

step_verify_streaming_readiness() {
  log INFO "=== STEP 12: Verify streaming data readiness ==="
  if (( DRY_RUN )); then
    log INFO "DRY-RUN: would verify Redpanda/Debezium/CDC/current/mart_rt readiness"
    return 0
  fi

  local running_services
  running_services="$(compose_streaming ps --status=running --services 2>/dev/null || echo "")"
  local service
  for service in redpanda debezium-connect cdc-consumer; do
    if ! grep -qx "$service" <<<"$running_services"; then
      log ERROR "Streaming service $service is not RUNNING"
      compose_streaming ps || true
      exit 1
    fi
  done
  log INFO "streaming_services=RUNNING"

  # Verify Debezium connector
  local max_wait=60
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
    exit 1
  fi

  # Verify mart_rt has data
  wait_for_ch_positive "mart_rt.sales_daily_rt" \
    "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=${ID_EMPRESA}" \
    120
  log INFO "streaming_data_readiness=OK"
}

step_validate_parity() {
  log INFO "=== STEP 13: Validate parity (BLOQUEANTE) ==="
  if [[ ! -f "$ROOT_DIR/deploy/scripts/realtime-validate-cutover.sh" ]]; then
    log ERROR "realtime-validate-cutover.sh not found — cannot validate. Aborting."
    exit 1
  fi
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" \
    "$ROOT_DIR/deploy/scripts/realtime-validate-cutover.sh" --source "$SOURCE"
  log INFO "parity_validation=PASSED"
}

step_activate_realtime() {
  log INFO "=== STEP 14: Activate USE_REALTIME_MARTS=true ==="
  tm_env_set "USE_REALTIME_MARTS" "true"
  tm_env_set "REALTIME_MARTS_SOURCE" "$SOURCE"
  tm_env_set "REALTIME_MARTS_FALLBACK" "false"
  log INFO "realtime_marts=ACTIVATED source=$SOURCE fallback=false"
}

step_rollback_to_legacy() {
  log INFO "=== ROLLBACK: Disabling realtime marts ==="
  tm_env_set "USE_REALTIME_MARTS" "false"
  # Restart API to pick up the change
  run compose_prod up -d --no-deps --force-recreate api
  log INFO "rollback=DONE use_realtime_marts=false"
  log INFO "NOTE: Legacy cron is NOT automatically restored. Re-enable manually if needed."
}

step_rebuild_api() {
  log INFO "=== STEP 15: Rebuild API/Web with realtime flag ==="
  run compose_prod up -d --no-deps --force-recreate api web
  log INFO "api_web_rebuild=OK"
}

step_smoke() {
  log INFO "=== STEP 16: Smoke endpoints (fallback=false) ==="
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

  # Smoke test: validates realtime path with fallback=false
  local smoke_result
  smoke_result="$(compose_prod exec -T api env \
    USE_REALTIME_MARTS=true \
    REALTIME_MARTS_SOURCE="$SOURCE" \
    REALTIME_MARTS_FALLBACK=false \
    USE_CLICKHOUSE=true \
    ID_EMPRESA="$ID_EMPRESA" \
    python - <<'PY' 2>&1 || echo "SMOKE_FAILED"
import inspect, os
from datetime import date, timedelta
from app.config import settings
assert settings.use_realtime_marts is True, "USE_REALTIME_MARTS is not active"
assert settings.realtime_marts_fallback is False, "fallback must be disabled"
from app import repos_analytics, repos_mart_realtime as rt
dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=30)
payload = getattr(repos_analytics, "dashboard_kpis")(
    "admin", int(os.environ["ID_EMPRESA"]), None, dt_ini, dt_fim,
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
  elif (( VALIDATE_ONLY == 1 )); then
    log INFO "  REALTIME CUTOVER VALIDATION COMPLETE"
  else
    log INFO "  REALTIME CUTOVER COMPLETE"
  fi
  log INFO "============================================"
  log INFO "env_file=$ENV_FILE"
  log INFO "from_date=$FROM_DATE"
  log INFO "id_empresa=$ID_EMPRESA"
  log INFO "audit_filial=${ID_FILIAL:-nenhuma}"
  log INFO "backfill_filial=${BACKFILL_ID_FILIAL:-todas}"
  log INFO "with_backfill=$WITH_BACKFILL"
  log INFO "bootstrap_stg=$BOOTSTRAP_STG"
  log INFO "source=$SOURCE"
  log INFO "validate_only=$VALIDATE_ONLY"
  log INFO "dry_run=$DRY_RUN"
  log INFO ""
  log INFO "Pipeline: Agent→STG→Debezium→Redpanda→CDC Consumer→ClickHouse→MartBuilder→API"
  log INFO "Legacy ETL cron is NOT the BI motor. Only health/reconcile/ops remain."
  log INFO ""
  log INFO "Next steps:"
  log INFO "  - Monitor Redpanda Console: http://localhost:18080"
  log INFO "  - Check CDC lag: make streaming-status"
  log INFO "  - Validate marts: make realtime-validate"
  log INFO "  - Rollback: ENV_FILE=$ENV_FILE ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy"
  log INFO "============================================"
}

# ===== VALIDATE-ONLY MODE =====
# Non-mutating: only reads environment state.
run_validate_only() {
  log INFO "=== VALIDATE-ONLY MODE (non-mutating) ==="
  log INFO "This mode does NOT build, migrate, start services, register connectors, or alter env."

  step_preflight

  # Check compose files are valid (read-only)
  log INFO "Checking compose file validity..."
  compose_prod config --quiet || { log ERROR "docker-compose.prod.yml is invalid"; exit 1; }
  compose_streaming config --quiet || { log ERROR "docker-compose.streaming.yml is invalid"; exit 1; }
  log INFO "compose_validation=OK"

  # Validate containers are running
  log INFO "Checking streaming services..."
  local running_services
  running_services="$(compose_streaming ps --status=running --services 2>/dev/null || echo "")"
  for service in redpanda debezium-connect cdc-consumer; do
    if ! grep -qx "$service" <<<"$running_services"; then
      log ERROR "Streaming service $service is not RUNNING"
      exit 1
    fi
  done
  log INFO "streaming_services=RUNNING"

  # Debezium connector
  local dbz_status
  dbz_status="$(compose_streaming exec -T debezium-connect curl -sf http://localhost:8083/connectors/torqmind-postgres-cdc/status 2>/dev/null | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || echo "UNKNOWN")"
  if [[ "$dbz_status" != "RUNNING" ]]; then
    log ERROR "Debezium connector is $dbz_status (expected RUNNING)"
    exit 1
  fi
  log INFO "debezium_connector=RUNNING"

  # Check current has data
  local current_count
  current_count="$(ch_query "SELECT count() FROM torqmind_current.stg_comprovantes FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0")"
  log INFO "current.stg_comprovantes=$current_count"
  if [[ "$current_count" == "0" || "$current_count" == "__ERROR__" ]]; then
    log ERROR "No data in torqmind_current.stg_comprovantes"
    exit 1
  fi

  # Check mart_rt
  local mart_count
  mart_count="$(ch_query "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA")"
  log INFO "mart_rt.sales_daily_rt=$mart_count"
  if [[ "$mart_count" == "0" || "$mart_count" == "__ERROR__" ]]; then
    log ERROR "No data in torqmind_mart_rt.sales_daily_rt"
    exit 1
  fi

  # Validate parity
  step_validate_parity

  # API smoke with fallback=false (read-only test)
  step_smoke_validate_only

  step_report
  log INFO "VALIDATION PASSED. Environment is ready for cutover activation."
}

step_smoke_validate_only() {
  log INFO "Validating API realtime path (read-only)..."
  local smoke_result
  smoke_result="$(compose_prod exec -T api env \
    USE_REALTIME_MARTS=true \
    REALTIME_MARTS_SOURCE="$SOURCE" \
    REALTIME_MARTS_FALLBACK=false \
    USE_CLICKHOUSE=true \
    ID_EMPRESA="$ID_EMPRESA" \
    python -c "
import os
from datetime import date, timedelta
from app.config import settings
from app import repos_analytics
dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=30)
payload = repos_analytics.dashboard_kpis('admin', int(os.environ['ID_EMPRESA']), None, dt_ini, dt_fim)
assert isinstance(payload, dict), 'no payload'
print('VALIDATE_OK')
" 2>&1 || echo "VALIDATE_FAILED")"

  if [[ "$smoke_result" == *"VALIDATE_OK"* ]]; then
    log INFO "api_realtime_path=OK (fallback=false)"
  else
    log ERROR "API realtime path failed: $smoke_result"
    exit 1
  fi
}

# ===== MAIN =====
parse_args "$@"

if (( ROLLBACK_TO_LEGACY )); then
  step_preflight
  step_rollback_to_legacy
  exit 0
fi

if (( VALIDATE_ONLY )); then
  run_validate_only
  exit 0
fi

# Full cutover flow
step_preflight
step_validate_compose
step_neutralize_legacy_etl
step_build
step_migrate
step_init_clickhouse_streaming
step_prepare_postgres
step_start_streaming
step_register_debezium
step_bootstrap_stg
step_backfill
step_verify_streaming_readiness
step_validate_parity
step_activate_realtime
step_rebuild_api
step_smoke
step_report
