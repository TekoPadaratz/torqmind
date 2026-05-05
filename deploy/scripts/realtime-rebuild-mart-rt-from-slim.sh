#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# realtime-rebuild-mart-rt-from-slim.sh
# Full operational script: preflight → reset mart_rt → backfill mart-only → validate
#
# tmux-friendly. Safe to re-run after interruption.
# NEVER touches PostgreSQL STG, torqmind_current, or torqmind_raw.
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
STREAMING_COMPOSE_FILE="${STREAMING_COMPOSE_FILE:-docker-compose.streaming.yml}"
LOG_DIR="${LOG_DIR:-/home/deploy/logs}"

# Defaults
ID_EMPRESA=1
ID_FILIAL=""
FROM_DATE="2025-01-01"
TO_DATE=""
BATCH_SIZE=7
MAX_THREADS=2
MAX_MEMORY_GB=3
RESET_MODE=""  # truncate or drop-recreate
DRY_RUN=false
YES=false
ENABLE_AFTER_PASS=false
SKIP_PREFLIGHT=false
STOP_WRITERS=true

usage() {
  cat <<'EOF'
Usage:
  ENV_FILE=/etc/torqmind/prod.env \
  COMPOSE_FILE=docker-compose.prod.yml \
  STREAMING_COMPOSE_FILE=docker-compose.streaming.yml \
  ./deploy/scripts/realtime-rebuild-mart-rt-from-slim.sh \
    --yes --drop-recreate \
    --from-date 2025-01-01 --id-empresa 1 \
    --batch-size 14 --max-threads 4 --max-memory-gb 8

Options:
  --id-empresa N         Tenant ID (default: 1)
  --id-filial N          Branch ID (optional; all if omitted)
  --from-date YYYY-MM-DD Start date (default: 2025-01-01)
  --to-date YYYY-MM-DD   End date (optional)
  --batch-size N         Data_keys per batch (default: 7)
  --max-threads N        ClickHouse max_threads (default: 2)
  --max-memory-gb N      ClickHouse max_memory_usage GB (default: 3)
  --truncate             TRUNCATE mart_rt tables before rebuild
  --drop-recreate        DROP + re-CREATE mart_rt tables (recommended)
  --yes                  Non-interactive
  --dry-run              Print commands without executing
  --enable-after-pass    If validate PASS, enable realtime
  --skip-preflight       Skip ClickHouse preflight checks
  --no-stop-writers      Don't stop cdc-consumer before rebuild

Safety:
  - ONLY rebuilds torqmind_mart_rt from existing slim tables.
  - NEVER touches PostgreSQL STG, torqmind_current, torqmind_raw, Redpanda.
  - Idempotent: safe to re-run with --drop-recreate.

Recommended tmux session:
  tmux new -s torqmind-mart-rebuild
  # run this script
  # Ctrl+B, D to detach
  # tmux attach -t torqmind-mart-rebuild to resume
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id-empresa)        ID_EMPRESA="$2"; shift ;;
    --id-filial)         ID_FILIAL="$2"; shift ;;
    --from-date)         FROM_DATE="$2"; shift ;;
    --to-date)           TO_DATE="$2"; shift ;;
    --batch-size)        BATCH_SIZE="$2"; shift ;;
    --max-threads)       MAX_THREADS="$2"; shift ;;
    --max-memory-gb)     MAX_MEMORY_GB="$2"; shift ;;
    --truncate)          RESET_MODE="truncate" ;;
    --drop-recreate)     RESET_MODE="drop-recreate" ;;
    --yes)               YES=true ;;
    --dry-run)           DRY_RUN=true ;;
    --enable-after-pass) ENABLE_AFTER_PASS=true ;;
    --skip-preflight)    SKIP_PREFLIGHT=true ;;
    --no-stop-writers)   STOP_WRITERS=false ;;
    --help|-h)           usage 0 ;;
    *)                   echo "ERROR: unknown argument: $1" >&2; usage 2 ;;
  esac
  shift
done

if [[ -z "$RESET_MODE" ]]; then
  echo "ERROR: specify --truncate or --drop-recreate" >&2
  usage 2
fi

log() { printf '%s [rebuild] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

mkdir -p "$LOG_DIR" 2>/dev/null || true
LOGFILE="$LOG_DIR/realtime-mart-rebuild-$(date +%Y%m%d_%H%M%S).log"

# All stdout+stderr go to both terminal and LOGFILE
exec > >(tee -a "$LOGFILE") 2>&1

VALIDATE_RESULT="NOT_RUN"

log "=== TorqMind Mart RT Rebuild from Slim ==="
log "ENV_FILE=$ENV_FILE"
log "COMPOSE_FILE=$COMPOSE_FILE"
log "STREAMING_COMPOSE_FILE=$STREAMING_COMPOSE_FILE"
log "id_empresa=$ID_EMPRESA id_filial=${ID_FILIAL:-all}"
log "from_date=$FROM_DATE to_date=${TO_DATE:-now}"
log "batch_size=$BATCH_SIZE max_threads=$MAX_THREADS max_memory_gb=$MAX_MEMORY_GB"
log "reset_mode=$RESET_MODE dry_run=$DRY_RUN"
log ""

# ---- Step 1: Pre-flight ----
if ! $SKIP_PREFLIGHT; then
  log "Step 1/6: ClickHouse pre-flight checks..."
  if $DRY_RUN; then
    log "[DRY-RUN] Would run: $SCRIPT_DIR/realtime-clickhouse-backfill-preflight.sh"
  else
    ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" \
      bash "$SCRIPT_DIR/realtime-clickhouse-backfill-preflight.sh" || {
        log "ERROR: Pre-flight checks failed. Fix issues before rebuild."
        exit 1
      }
  fi
else
  log "Step 1/6: Pre-flight SKIPPED (--skip-preflight)"
fi
log ""

# ---- Step 2: Stop writers ----
if $STOP_WRITERS; then
  log "Step 2/6: Stopping CDC consumer..."
  if $DRY_RUN; then
    log "[DRY-RUN] Would stop cdc-consumer"
  else
    docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE" \
      stop cdc-consumer 2>/dev/null || log "cdc-consumer not running (ok)"
  fi
else
  log "Step 2/6: Writer stop SKIPPED (--no-stop-writers)"
fi
log ""

# ---- Step 3: Reset mart_rt ----
log "Step 3/6: Resetting mart_rt ($RESET_MODE)..."
RESET_FLAGS="--$RESET_MODE"
$YES && RESET_FLAGS="$RESET_FLAGS --yes"
$DRY_RUN && RESET_FLAGS="$RESET_FLAGS --dry-run"

ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" \
  bash "$SCRIPT_DIR/realtime-reset-mart-rt.sh" $RESET_FLAGS || {
    log "ERROR: mart_rt reset failed."
    exit 1
  }
log ""

# ---- Step 4: Rebuild marts from slim ----
log "Step 4/6: Running mart-only backfill..."

BACKFILL_CMD="python -m torqmind_cdc_consumer.cli backfill-stg \
  --mart-only \
  --from-date $FROM_DATE \
  --id-empresa $ID_EMPRESA \
  --batch-size $BATCH_SIZE \
  --max-threads $MAX_THREADS \
  --max-memory-gb $MAX_MEMORY_GB \
  --skip-batch-deletes"

[[ -n "$TO_DATE" ]] && BACKFILL_CMD="$BACKFILL_CMD --to-date $TO_DATE"
[[ -n "$ID_FILIAL" ]] && BACKFILL_CMD="$BACKFILL_CMD --id-filial $ID_FILIAL"

if $DRY_RUN; then
  log "[DRY-RUN] Would run: $BACKFILL_CMD"
else
  T0=$(date +%s)
  docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE" \
    run --rm --entrypoint python cdc-consumer -m torqmind_cdc_consumer.cli backfill-stg \
      --mart-only \
      --from-date "$FROM_DATE" \
      --id-empresa "$ID_EMPRESA" \
      --batch-size "$BATCH_SIZE" \
      --max-threads "$MAX_THREADS" \
      --max-memory-gb "$MAX_MEMORY_GB" \
      --skip-batch-deletes \
      ${TO_DATE:+--to-date "$TO_DATE"} \
      ${ID_FILIAL:+--id-filial "$ID_FILIAL"} || {
      log "ERROR: Mart backfill failed."
      log "System is in safe state: mart_rt was reset but not populated."
      log "Realtime remains disabled (fallback=true)."
      log "To retry: re-run this script with same arguments."
      exit 1
    }
  T1=$(date +%s)
  ELAPSED=$((T1 - T0))
  log "Backfill completed in ${ELAPSED}s"
fi
log ""

# ---- Step 5: Validate ----
log "Step 5/6: Validating mart_rt..."
if $DRY_RUN; then
  log "[DRY-RUN] Would run validate-cutover and stability-check"
  VALIDATE_RESULT="DRY_RUN"
else
  # Run validate-cutover (blocking)
  if ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" \
    bash "$SCRIPT_DIR/realtime-validate-cutover.sh" --source stg; then
    VALIDATE_RESULT="PASS"
    log "Validation PASSED."
  else
    VALIDATE_RESULT="FAIL"
    log "ERROR: Validate cutover FAILED."
    log "Realtime will NOT be enabled."
  fi

  # Run stability check (non-blocking, informational only)
  ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" \
    bash "$SCRIPT_DIR/clickhouse-stability-check.sh" 2>&1 || {
      log "WARNING: ClickHouse stability check did not pass (non-blocking)."
    }
fi
log ""

# ---- Step 6: Post-rebuild report ----
log "Step 6/6: Post-rebuild report..."
if ! $DRY_RUN; then
  ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" \
    bash "$SCRIPT_DIR/realtime-clickhouse-backfill-preflight.sh" 2>&1 || true
fi

# Save proof JSON
PROOF_FILE="$LOG_DIR/rebuild-proof-$(date +%Y%m%d_%H%M%S).json"
if ! $DRY_RUN; then
  cat > "$PROOF_FILE" <<PROOF
{
  "proof": "realtime-rebuild-mart-rt-from-slim",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "result": "$VALIDATE_RESULT",
  "id_empresa": $ID_EMPRESA,
  "from_date": "$FROM_DATE",
  "to_date": "${TO_DATE:-null}",
  "batch_size": $BATCH_SIZE,
  "max_threads": $MAX_THREADS,
  "max_memory_gb": $MAX_MEMORY_GB,
  "reset_mode": "$RESET_MODE",
  "elapsed_seconds": ${ELAPSED:-0}
}
PROOF
  log "Proof saved: $PROOF_FILE"
fi

# ---- Restart CDC consumer ----
if $STOP_WRITERS && ! $DRY_RUN; then
  log "Restarting CDC consumer..."
  docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE" \
    start cdc-consumer 2>/dev/null || log "Could not restart cdc-consumer"
fi

# ---- Optional: Enable realtime (only on PASS) ----
if $ENABLE_AFTER_PASS && [[ "$VALIDATE_RESULT" == "PASS" ]]; then
  log "Enabling realtime (--enable-after-pass + validate PASS)..."
  # Backup current env
  cp "$ENV_FILE" "${ENV_FILE}.bak.$(date +%Y%m%d_%H%M%S)"
  # Apply realtime settings
  sed -i 's/^USE_REALTIME_MARTS=.*/USE_REALTIME_MARTS=true/' "$ENV_FILE"
  grep -q '^USE_REALTIME_MARTS=' "$ENV_FILE" || echo 'USE_REALTIME_MARTS=true' >> "$ENV_FILE"
  sed -i 's/^REALTIME_MARTS_SOURCE=.*/REALTIME_MARTS_SOURCE=stg/' "$ENV_FILE"
  grep -q '^REALTIME_MARTS_SOURCE=' "$ENV_FILE" || echo 'REALTIME_MARTS_SOURCE=stg' >> "$ENV_FILE"
  sed -i 's/^REALTIME_MARTS_FALLBACK=.*/REALTIME_MARTS_FALLBACK=false/' "$ENV_FILE"
  grep -q '^REALTIME_MARTS_FALLBACK=' "$ENV_FILE" || echo 'REALTIME_MARTS_FALLBACK=false' >> "$ENV_FILE"
  # Recreate api, web, nginx to pick up new env
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --force-recreate api web nginx
  log "Realtime ENABLED. Backup: ${ENV_FILE}.bak.*"
elif $ENABLE_AFTER_PASS && [[ "$VALIDATE_RESULT" != "PASS" ]]; then
  log "--enable-after-pass requested but validate did not PASS. Realtime NOT enabled."
else
  log "Realtime NOT enabled automatically."
  log "Validate manually, then set USE_REALTIME_MARTS=true if satisfied."
fi

log ""
if [[ "$VALIDATE_RESULT" == "FAIL" ]]; then
  log "=== Rebuild FAILED (validation did not pass) ==="
  log "Log: $LOGFILE"
  exit 1
fi
log "=== Rebuild Complete (result=$VALIDATE_RESULT) ==="
log "Log: $LOGFILE"
