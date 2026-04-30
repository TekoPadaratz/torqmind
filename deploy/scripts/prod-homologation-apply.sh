#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROD_COMPOSE_FILE="docker-compose.prod.yml"
STREAMING_COMPOSE_FILE="docker-compose.streaming.yml"
LOG_DIR="${LOG_DIR:-/home/deploy/logs}"

ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ASSUME_YES=0
DRY_RUN=0
SKIP_BUILD=0
SKIP_MIGRATE=0
REBUILD_DW_FROM_STG=0
SKIP_DERIVED_REBUILD=0
INCLUDE_DIMENSIONS=0
ALLOW_DW_ONLY=0
FULL_CLICKHOUSE=0
SKIP_CLICKHOUSE=0
WITH_STREAMING=0
STREAMING_NON_BLOCKING=0
SKIP_CRON=0
SKIP_AUDITS=0
ID_EMPRESA=1
ID_FILIAL=14458
FROM_DATE="${FROM_DATE:-2025-01-01}"
TO_DATE="${TO_DATE:-}"
STREAMING_PROFILE="${STREAMING_PROFILE:-prod-lite}"
STREAMING_VALIDATE_TIMEOUT_SECONDS="${STREAMING_VALIDATE_TIMEOUT_SECONDS:-300}"
PIPELINE_LOG="${PIPELINE_LOG:-/home/deploy/logs/torqmind-etl-pipeline.log}"

CURRENT_STEP_ID="init"
CURRENT_STEP_LABEL="bootstrap"
LAST_ERROR_LINE=""
LAST_ERROR_COMMAND=""
FINALIZED=0
CRON_STOPPED=0
CRON_RESTARTED=0
APP_ENV_EFFECTIVE="unknown"
BRANCH_NAME="unknown"
COMMIT_SHORT="unknown"
LOG_FILE=""
CH_CONTAINER_ID=""

BUILD_STATUS="PENDING"
MIGRATE_STATUS="PENDING"
DERIVED_REBUILD_STATUS="SKIPPED"
API_STATUS="PENDING"
WEB_STATUS="PENDING"
CLICKHOUSE_STATUS="PENDING"
RECONCILE_STATUS="PENDING"
SEMANTIC_AUDIT_STATUS="PENDING"
HISTORY_AUDIT_STATUS="SKIPPED"
ORPHANS_REPORT_STATUS="SKIPPED"
STREAMING_STATUS="SKIPPED"
SNAPSHOT_CACHE_STATUS="PENDING"
POST_BOOT_STATUS="PENDING"
CRON_STATUS="PENDING"

WARNINGS=()

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh [flags]

Flags:
  --yes
  --dry-run
  --skip-build
  --skip-migrate
  --rebuild-dw-from-stg
  --skip-derived-rebuild
  --include-dimensions
  --from-date <YYYY-MM-DD>
  --to-date <YYYY-MM-DD>
  --allow-dw-only
  --full-clickhouse
  --skip-clickhouse
  --with-streaming
  --skip-streaming
  --no-streaming
  --streaming-non-blocking
  --skip-cron
  --skip-audits
  --id-empresa <id>
  --id-filial <id>
  --help

Examples:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --from-date 2025-01-01
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --include-dimensions --from-date 2025-01-01
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --allow-dw-only --skip-clickhouse --from-date 2025-01-01
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse --with-streaming
  ENV_FILE=.env.production.example ./deploy/scripts/prod-homologation-apply.sh --dry-run --full-clickhouse --with-streaming --id-empresa 1 --id-filial 14458
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --yes)
        ASSUME_YES=1
        ;;
      --dry-run)
        DRY_RUN=1
        ;;
      --skip-build)
        SKIP_BUILD=1
        ;;
      --skip-migrate)
        SKIP_MIGRATE=1
        ;;
      --rebuild-dw-from-stg)
        REBUILD_DW_FROM_STG=1
        ;;
      --skip-derived-rebuild)
        SKIP_DERIVED_REBUILD=1
        ;;
      --include-dimensions)
        INCLUDE_DIMENSIONS=1
        ;;
      --from-date)
        [[ $# -ge 2 ]] || { echo "ERROR: --from-date requires a value" >&2; exit 2; }
        FROM_DATE="$2"
        shift
        ;;
      --to-date)
        [[ $# -ge 2 ]] || { echo "ERROR: --to-date requires a value" >&2; exit 2; }
        TO_DATE="$2"
        shift
        ;;
      --full-clickhouse)
        FULL_CLICKHOUSE=1
        ;;
      --skip-clickhouse)
        SKIP_CLICKHOUSE=1
        ;;
      --allow-dw-only)
        ALLOW_DW_ONLY=1
        ;;
      --with-streaming)
        WITH_STREAMING=1
        ;;
      --skip-streaming|--no-streaming)
        WITH_STREAMING=0
        ;;
      --streaming-non-blocking)
        STREAMING_NON_BLOCKING=1
        ;;
      --skip-cron)
        SKIP_CRON=1
        ;;
      --skip-audits)
        SKIP_AUDITS=1
        ;;
      --id-empresa)
        [[ $# -ge 2 ]] || { echo "ERROR: --id-empresa requires a value" >&2; exit 2; }
        ID_EMPRESA="$2"
        shift
        ;;
      --id-filial)
        [[ $# -ge 2 ]] || { echo "ERROR: --id-filial requires a value" >&2; exit 2; }
        ID_FILIAL="$2"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "ERROR: unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
    shift
  done

  if [[ -n "$ID_EMPRESA" && ! "$ID_EMPRESA" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --id-empresa must be numeric" >&2
    exit 2
  fi
  if [[ -n "$ID_FILIAL" && ! "$ID_FILIAL" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --id-filial must be numeric" >&2
    exit 2
  fi
  if (( FULL_CLICKHOUSE )) && (( SKIP_CLICKHOUSE )); then
    echo "ERROR: --full-clickhouse and --skip-clickhouse cannot be used together" >&2
    exit 2
  fi
  if (( REBUILD_DW_FROM_STG )) && (( SKIP_DERIVED_REBUILD )); then
    echo "ERROR: --rebuild-dw-from-stg and --skip-derived-rebuild cannot be used together" >&2
    exit 2
  fi
  if (( REBUILD_DW_FROM_STG )) && (( SKIP_MIGRATE )); then
    echo "ERROR: --rebuild-dw-from-stg cannot be combined with --skip-migrate" >&2
    exit 2
  fi
  if (( INCLUDE_DIMENSIONS )) && (( ! REBUILD_DW_FROM_STG )); then
    echo "ERROR: --include-dimensions requires --rebuild-dw-from-stg" >&2
    exit 2
  fi
  if (( ALLOW_DW_ONLY )) && (( ! REBUILD_DW_FROM_STG )); then
    echo "ERROR: --allow-dw-only requires --rebuild-dw-from-stg" >&2
    exit 2
  fi
  if (( REBUILD_DW_FROM_STG )) && (( SKIP_CLICKHOUSE )) && (( ! ALLOW_DW_ONLY )); then
    echo "ERROR: --rebuild-dw-from-stg cannot be combined with --skip-clickhouse unless --allow-dw-only is used" >&2
    exit 2
  fi
  if (( REBUILD_DW_FROM_STG )) && (( ! SKIP_CLICKHOUSE )); then
    FULL_CLICKHOUSE=1
  fi
}

resolve_path() {
  local raw_path="$1"
  if [[ "$raw_path" = /* ]]; then
    printf '%s\n' "$raw_path"
    return 0
  fi
  if [[ -f "$raw_path" ]]; then
    printf '%s\n' "$raw_path"
    return 0
  fi
  if [[ -f "$ROOT_DIR/$raw_path" ]]; then
    printf '%s\n' "$ROOT_DIR/$raw_path"
    return 0
  fi
  printf '%s\n' "$raw_path"
}

init_logging() {
  local ts
  local target_log_dir
  ts="$(date +%Y%m%d_%H%M%S)"
  target_log_dir="$LOG_DIR"

  if ! mkdir -p "$target_log_dir" 2>/dev/null; then
    if (( DRY_RUN )); then
      target_log_dir="${TMPDIR:-/tmp}/torqmind-logs"
      mkdir -p "$target_log_dir"
      echo "WARN: $LOG_DIR nao esta gravavel neste host; dry-run usando fallback em $target_log_dir" >&2
    else
      echo "ERROR: nao foi possivel criar $target_log_dir" >&2
      exit 1
    fi
  fi

  LOG_DIR="$target_log_dir"
  LOG_FILE="$LOG_DIR/torqmind-homologation-apply-${ts}.log"
  touch "$LOG_FILE"
  exec > >(tee -a "$LOG_FILE") 2>&1
}

log() {
  local level="$1"
  shift
  printf '%s [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$level" "$*"
}

record_warning() {
  WARNINGS+=("$*")
  log WARN "$*"
}

sanitize_text() {
  local text="$*"
  local key secret
  for key in API_JWT_SECRET JWT_SECRET_KEY POSTGRES_PASSWORD PG_PASSWORD CLICKHOUSE_PASSWORD PLATFORM_MASTER_PASSWORD SEED_PASSWORD CHANNEL_BOOTSTRAP_PASSWORD DATABASE_URL; do
    secret="${!key:-}"
    if [[ -n "$secret" ]]; then
      text="${text//${secret}/***REDACTED***}"
    fi
  done
  printf '%s' "$text"
}

cmd_to_string() {
  local cmd
  printf -v cmd '%q ' "$@"
  sanitize_text "${cmd% }"
}

run() {
  local cmd
  cmd="$(cmd_to_string "$@")"
  log INFO "RUN $cmd"
  if (( DRY_RUN )); then
    return 0
  fi
  "$@"
}

run_soft() {
  local cmd rc
  cmd="$(cmd_to_string "$@")"
  log INFO "RUN_SOFT $cmd"
  if (( DRY_RUN )); then
    return 0
  fi
  set +e
  "$@"
  rc=$?
  set -e
  return "$rc"
}

require_file() {
  local file_path="$1"
  if [[ ! -f "$file_path" ]]; then
    log ERROR "Arquivo obrigatorio nao encontrado: $file_path"
    return 1
  fi
}

require_dir() {
  local dir_path="$1"
  if [[ ! -d "$dir_path" ]]; then
    log ERROR "Diretorio obrigatorio nao encontrado: $dir_path"
    return 1
  fi
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log ERROR "Comando obrigatorio ausente: $cmd"
    return 1
  fi
}

confirm() {
  if (( ASSUME_YES )) || (( DRY_RUN )); then
    log INFO "Confirmacao interativa pulada"
    return 0
  fi

  local answer
  printf 'Continuar com o homologation apply em %s? [y/N]: ' "$BRANCH_NAME" >/dev/tty
  read -r answer </dev/tty || {
    log ERROR "Falha ao ler confirmacao interativa"
    exit 130
  }

  case "${answer,,}" in
    y|yes|s|sim)
      log INFO "Confirmacao recebida"
      ;;
    *)
      log ERROR "Operacao cancelada pelo usuario"
      exit 130
      ;;
  esac
}

load_env() {
  tm_load_env_file "$ENV_FILE"
}

compose_prod() {
  docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

mark_status() {
  local value="$1"
  if (( DRY_RUN )); then
    printf 'DRY-RUN\n'
  else
    printf '%s\n' "$value"
  fi
}

is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|y|Y|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_prod_like_env() {
  case "${1,,}" in
    prod|production|homolog|homologation|staging)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_weak_secret() {
  local value="$1"
  local lower
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  lower="${value,,}"

  if tm_is_insecure_secret "$value" || tm_is_placeholder "$value"; then
    return 0
  fi

  case "$lower" in
    ""|default|password|admin|secret|changeme|change_me|replace_me|postgres|clickhouse|torqmind|jwt)
      return 0
      ;;
  esac

  if [[ ${#value} -lt 12 ]]; then
    return 0
  fi

  return 1
}

security_gate() {
  local message="$1"
  if (( DRY_RUN )); then
    record_warning "DRY-RUN: bloqueio de seguranca que ocorreria em execucao real: $message"
    return 0
  fi
  log ERROR "$message"
  return 1
}

run_step() {
  CURRENT_STEP_ID="$1"
  CURRENT_STEP_LABEL="$2"
  shift 2
  log INFO "===== STEP ${CURRENT_STEP_ID}: ${CURRENT_STEP_LABEL} ====="
  "$@"
  log INFO "===== STEP ${CURRENT_STEP_ID}: OK ====="
}

print_follow_up_commands() {
  echo "next_commands:"
  echo "  docker compose -f $PROD_COMPOSE_FILE --env-file $ENV_FILE ps"
  echo "  tail -f $LOG_FILE"
  echo "  tail -f $PIPELINE_LOG"
  if (( WITH_STREAMING )); then
    echo "  ENV_FILE=$ENV_FILE ./deploy/scripts/streaming-status.sh"
  fi
}

print_failure_hints() {
  echo "diagnostics:"
  echo "  docker compose -f $PROD_COMPOSE_FILE --env-file $ENV_FILE ps"
  echo "  docker compose -f $PROD_COMPOSE_FILE --env-file $ENV_FILE logs --tail=200 api web nginx"
  echo "  docker compose -f $PROD_COMPOSE_FILE --env-file $ENV_FILE logs --tail=200 postgres clickhouse"
  if (( WITH_STREAMING )); then
    echo "  docker compose -f $STREAMING_COMPOSE_FILE --env-file $ENV_FILE ps"
    echo "  ENV_FILE=$ENV_FILE ./deploy/scripts/streaming-status.sh"
  fi
  echo "rollback_basic: checkout do commit estavel anterior e reexecucao do mesmo script; streaming continua paralelo e pode ser omitido com --no-streaming"
}

final_report() {
  local result_label="$1"

  echo
  echo "=========================================="
  if [[ "$result_label" == "OK" ]]; then
    echo "HOMOLOGATION APPLY: OK"
  else
    echo "HOMOLOGATION APPLY: FAILED at step ${CURRENT_STEP_ID}"
  fi
  echo "LOG: $LOG_FILE"
  echo "commit: $COMMIT_SHORT"
  echo "branch: $BRANCH_NAME"
  echo "api_status: $API_STATUS"
  echo "web_status: $WEB_STATUS"
  echo "derived_rebuild_status: $DERIVED_REBUILD_STATUS"
  echo "clickhouse_status: $CLICKHOUSE_STATUS"
  echo "reconcile_status: $RECONCILE_STATUS"
  echo "semantic_audit_status: $SEMANTIC_AUDIT_STATUS"
  echo "history_audit_status: $HISTORY_AUDIT_STATUS"
  echo "orphans_report_status: $ORPHANS_REPORT_STATUS"
  echo "streaming_status: $STREAMING_STATUS"
  echo "snapshot_cache_status: $SNAPSHOT_CACHE_STATUS"
  echo "post_boot_status: $POST_BOOT_STATUS"
  echo "cron_status: $CRON_STATUS"
  echo "pipeline_log: $PIPELINE_LOG"
  if [[ ${#WARNINGS[@]} -gt 0 ]]; then
    echo "warnings:"
    local warning
    for warning in "${WARNINGS[@]}"; do
      echo "  - $warning"
    done
  fi
  if [[ "$result_label" == "OK" ]]; then
    print_follow_up_commands
  else
    print_failure_hints
  fi
  echo "=========================================="
}

finalize() {
  local exit_code="$1"
  if (( FINALIZED )); then
    return
  fi
  FINALIZED=1

  if (( exit_code == 0 )); then
    final_report "OK"
    return
  fi

  if [[ -n "$LAST_ERROR_LINE" ]]; then
    log ERROR "Falha capturada em linha $LAST_ERROR_LINE: $(sanitize_text "$LAST_ERROR_COMMAND")"
  fi
  if (( ! SKIP_CRON )) && (( CRON_STOPPED )) && (( ! CRON_RESTARTED )); then
    CRON_STATUS="STOPPED_AFTER_FAILURE"
    record_warning "cron permanece parado; nao foi religado automaticamente depois da falha"
  fi
  final_report "FAILED"
}

trap 'LAST_ERROR_LINE=$LINENO; LAST_ERROR_COMMAND=$BASH_COMMAND' ERR
trap 'finalize "$?"' EXIT

step_preflight_local() {
  local git_root
  require_dir "$ROOT_DIR/deploy/scripts"
  require_file "$ROOT_DIR/$PROD_COMPOSE_FILE"
  if (( WITH_STREAMING )); then
    require_file "$ROOT_DIR/$STREAMING_COMPOSE_FILE"
  fi

  require_cmd git
  require_cmd docker
  require_cmd python3
  require_cmd tee
  require_cmd find
  if (( ! SKIP_CRON )) && (( ! DRY_RUN )); then
    require_cmd sudo
    require_cmd systemctl
    if ! run_soft sudo -n true; then
      log ERROR "Este fluxo exige sudo nao interativo para controlar o cron. Use --skip-cron ou ajuste sudoers."
      return 1
    fi
  fi
  if (( WITH_STREAMING )); then
    require_cmd timeout
  fi

  ENV_FILE="$(resolve_path "$ENV_FILE")"
  tm_require_env_file "$ENV_FILE"
  load_env

  git_root="$(git -C "$ROOT_DIR" rev-parse --show-toplevel)"
  if [[ "$git_root" != "$ROOT_DIR" ]]; then
    log ERROR "Repositorio invalido: esperado $ROOT_DIR, obtido $git_root"
    return 1
  fi

  require_file "$ROOT_DIR/deploy/scripts/prod-migrate.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-clickhouse-init.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-clickhouse-sync-dw.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-clickhouse-refresh-marts.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-data-reconcile.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-semantic-marts-audit.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-post-boot-check.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-install-cron.sh"
  require_file "$ROOT_DIR/deploy/scripts/prod-rebuild-derived-from-stg.sh"
  if (( WITH_STREAMING )); then
    require_file "$ROOT_DIR/deploy/scripts/streaming-init-clickhouse.sh"
    require_file "$ROOT_DIR/deploy/scripts/streaming-prepare-postgres.sh"
    require_file "$ROOT_DIR/deploy/scripts/streaming-up.sh"
    require_file "$ROOT_DIR/deploy/scripts/streaming-register-debezium.sh"
    require_file "$ROOT_DIR/deploy/scripts/streaming-validate-cdc.sh"
    require_file "$ROOT_DIR/deploy/scripts/streaming-status.sh"
  fi

  run docker compose version
  run docker info --format '{{.ServerVersion}}'
  run find "$ROOT_DIR/deploy/scripts" -maxdepth 1 -type f -name '*.sh' -exec chmod +x {} +
  run find "$ROOT_DIR/deploy/scripts/lib" -maxdepth 1 -type f -name '*.sh' -exec chmod +x {} +

  BRANCH_NAME="$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD)"
  COMMIT_SHORT="$(git -C "$ROOT_DIR" rev-parse --short HEAD)"

  log INFO "repo_root=$ROOT_DIR"
  log INFO "env_file=$ENV_FILE"
  log INFO "branch=$BRANCH_NAME"
  log INFO "commit=$COMMIT_SHORT"
  log INFO "dry_run=$DRY_RUN skip_build=$SKIP_BUILD skip_migrate=$SKIP_MIGRATE rebuild_dw_from_stg=$REBUILD_DW_FROM_STG include_dimensions=$INCLUDE_DIMENSIONS allow_dw_only=$ALLOW_DW_ONLY full_clickhouse=$FULL_CLICKHOUSE skip_clickhouse=$SKIP_CLICKHOUSE with_streaming=$WITH_STREAMING skip_cron=$SKIP_CRON skip_audits=$SKIP_AUDITS"
  if (( REBUILD_DW_FROM_STG )); then
    log INFO "derived_rebuild_window from_date=$FROM_DATE to_date=${TO_DATE:-aberto} id_empresa=$ID_EMPRESA id_filial=${ID_FILIAL:-todas} include_dimensions=$INCLUDE_DIMENSIONS allow_dw_only=$ALLOW_DW_ONLY"
  fi
}

step_security_env() {
  local jwt_secret postgres_secret clickhouse_user clickhouse_password

  APP_ENV_EFFECTIVE="${APP_ENV:-${TORQMIND_ENV:-${ENVIRONMENT:-unknown}}}"
  log INFO "effective_env=$APP_ENV_EFFECTIVE"

  if ! is_prod_like_env "$APP_ENV_EFFECTIVE"; then
    record_warning "O marcador de ambiente ($APP_ENV_EFFECTIVE) nao esta em prod/homolog/staging; revise o env antes de homologar"
    return 0
  fi

  if ! tm_require_prod_runtime_env "$ENV_FILE"; then
    security_gate "tm_require_prod_runtime_env reprovou o arquivo de ambiente"
  fi

  jwt_secret="${API_JWT_SECRET:-${JWT_SECRET_KEY:-}}"
  postgres_secret="${POSTGRES_PASSWORD:-${PG_PASSWORD:-}}"
  clickhouse_user="${CLICKHOUSE_USER:-}"
  clickhouse_password="${CLICKHOUSE_PASSWORD:-}"

  if [[ -z "$jwt_secret" || ${#jwt_secret} -lt 32 ]]; then
    security_gate "API_JWT_SECRET precisa existir e ter pelo menos 32 caracteres em ambiente prod-like"
  fi
  if is_weak_secret "$postgres_secret"; then
    security_gate "POSTGRES_PASSWORD/PG_PASSWORD esta ausente, placeholder ou fraco para ambiente prod-like"
  fi
  if [[ -z "${clickhouse_user//[[:space:]]/}" || "${clickhouse_user,,}" == "default" ]]; then
    security_gate "CLICKHOUSE_USER nao pode ser vazio nem default em ambiente prod-like"
  fi
  if is_weak_secret "$clickhouse_password"; then
    security_gate "CLICKHOUSE_PASSWORD esta ausente, placeholder ou fraco para ambiente prod-like"
  fi
  if ! is_true "${INGEST_REQUIRE_KEY:-}"; then
    security_gate "INGEST_REQUIRE_KEY precisa ser true em ambiente prod-like"
  fi

  log INFO "Security env precheck finalizado"
}

step_stop_cron() {
  if (( SKIP_CRON )); then
    CRON_STATUS="SKIPPED"
    return 0
  fi

  if ! run_soft sudo systemctl stop cron; then
    record_warning "sudo systemctl stop cron retornou erro; vou verificar o estado real do servico"
  fi

  if (( DRY_RUN )); then
    CRON_STOPPED=1
    CRON_STATUS="DRY-RUN"
    return 0
  fi

  if sudo systemctl is-active --quiet cron; then
    log ERROR "cron segue ativo apos a tentativa de stop"
    return 1
  fi

  CRON_STOPPED=1
  CRON_STATUS="STOPPED"
  log INFO "cron parado com seguranca"
}

step_validate_compose() {
  run docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" config --quiet
  if (( WITH_STREAMING )); then
    run docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE" config --quiet
  fi
}

step_build_runtime() {
  if (( SKIP_BUILD )); then
    BUILD_STATUS="SKIPPED"
    return 0
  fi

  run docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" build api web
  run docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" up -d --no-deps --force-recreate api web
  run docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" up -d --no-deps --force-recreate nginx

  BUILD_STATUS="$(mark_status RECREATED)"
}

step_migrate() {
  if (( SKIP_MIGRATE )); then
    MIGRATE_STATUS="SKIPPED"
    return 0
  fi

  run env ENV_FILE="$ENV_FILE" "$ROOT_DIR/deploy/scripts/prod-migrate.sh"
  MIGRATE_STATUS="$(mark_status OK)"
}

step_derived_rebuild() {
  if (( ! REBUILD_DW_FROM_STG )) || (( SKIP_DERIVED_REBUILD )); then
    DERIVED_REBUILD_STATUS="SKIPPED"
    return 0
  fi

  local cmd=(env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" FROM_DATE="$FROM_DATE")
  if [[ -n "$ID_FILIAL" ]]; then
    cmd+=(ID_FILIAL="$ID_FILIAL")
  fi
  if [[ -n "$TO_DATE" ]]; then
    cmd+=(TO_DATE="$TO_DATE")
  fi
  cmd+=("$ROOT_DIR/deploy/scripts/prod-rebuild-derived-from-stg.sh")
  if (( ASSUME_YES )); then
    cmd+=(--yes)
  fi
  if (( DRY_RUN )); then
    cmd+=(--dry-run)
  fi
  if (( INCLUDE_DIMENSIONS )); then
    cmd+=(--include-dimensions)
  fi

  run "${cmd[@]}"
  DERIVED_REBUILD_STATUS="$(mark_status OK)"
}

step_clickhouse() {
  if (( SKIP_CLICKHOUSE )); then
    CLICKHOUSE_STATUS="SKIPPED"
    return 0
  fi

  if (( FULL_CLICKHOUSE )); then
    run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/prod-clickhouse-init.sh"
    CLICKHOUSE_STATUS="$(mark_status FULL_OK)"
    return 0
  fi

  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" MODE=incremental "$ROOT_DIR/deploy/scripts/prod-clickhouse-sync-dw.sh"
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" MODE=incremental "$ROOT_DIR/deploy/scripts/prod-clickhouse-refresh-marts.sh"
  CLICKHOUSE_STATUS="$(mark_status INCREMENTAL_OK)"
}

step_audits() {
  if (( SKIP_AUDITS )); then
    RECONCILE_STATUS="SKIPPED"
    SEMANTIC_AUDIT_STATUS="SKIPPED"
    HISTORY_AUDIT_STATUS="SKIPPED"
    ORPHANS_REPORT_STATUS="SKIPPED"
    return 0
  fi

  if (( REBUILD_DW_FROM_STG )) && (( ALLOW_DW_ONLY )) && (( SKIP_CLICKHOUSE )); then
    RECONCILE_STATUS="SKIPPED"
    SEMANTIC_AUDIT_STATUS="SKIPPED"
    HISTORY_AUDIT_STATUS="SKIPPED"
    ORPHANS_REPORT_STATUS="SKIPPED"
    record_warning "Audits dependentes de ClickHouse foram pulados porque --allow-dw-only manteve ClickHouse sem republicacao"
    return 0
  fi

  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" ID_FILIAL="$ID_FILIAL" "$ROOT_DIR/deploy/scripts/prod-data-reconcile.sh"
  RECONCILE_STATUS="$(mark_status OK)"

  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" ID_FILIAL="$ID_FILIAL" "$ROOT_DIR/deploy/scripts/prod-semantic-marts-audit.sh"
  SEMANTIC_AUDIT_STATUS="$(mark_status OK)"

  if [[ -f "$ROOT_DIR/deploy/scripts/prod-history-coverage-audit.sh" ]]; then
    if run_soft env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" ID_FILIAL="$ID_FILIAL" "$ROOT_DIR/deploy/scripts/prod-history-coverage-audit.sh"; then
      HISTORY_AUDIT_STATUS="$(mark_status OK)"
    else
      HISTORY_AUDIT_STATUS="WARN"
      record_warning "prod-history-coverage-audit.sh falhou; trate como auditoria complementar antes de religar novos ciclos manuais"
    fi
  fi

  if [[ -f "$ROOT_DIR/deploy/scripts/prod-sales-orphans-report.sh" ]]; then
    if run_soft env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" ID_FILIAL="$ID_FILIAL" "$ROOT_DIR/deploy/scripts/prod-sales-orphans-report.sh"; then
      ORPHANS_REPORT_STATUS="$(mark_status WARN)"
    else
      ORPHANS_REPORT_STATUS="WARN"
      record_warning "prod-sales-orphans-report.sh nao concluiu; os itens orfaos continuam sendo sinal WARN, nao rollback automatico"
    fi
  fi
}

run_streaming_sequence() {
  CH_CONTAINER_ID="$(docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" ps -q clickhouse 2>/dev/null || true)"
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-init-clickhouse.sh"
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" "$ROOT_DIR/deploy/scripts/streaming-prepare-postgres.sh"
  run env ENV_FILE="$ENV_FILE" STREAMING_PROFILE="$STREAMING_PROFILE" "$ROOT_DIR/deploy/scripts/streaming-up.sh"
  run env ENV_FILE="$ENV_FILE" "$ROOT_DIR/deploy/scripts/streaming-register-debezium.sh"
  run timeout "${STREAMING_VALIDATE_TIMEOUT_SECONDS}s" env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" ID_EMPRESA="$ID_EMPRESA" CH_CONTAINER="$CH_CONTAINER_ID" "$ROOT_DIR/deploy/scripts/streaming-validate-cdc.sh"
  run env ENV_FILE="$ENV_FILE" CH_CONTAINER="$CH_CONTAINER_ID" "$ROOT_DIR/deploy/scripts/streaming-status.sh"
}

step_streaming() {
  if (( ! WITH_STREAMING )); then
    STREAMING_STATUS="SKIPPED"
    return 0
  fi

  if run_streaming_sequence; then
    STREAMING_STATUS="$(mark_status OK)"
    return 0
  fi

  if (( STREAMING_NON_BLOCKING )); then
    STREAMING_STATUS="WARN"
    record_warning "Streaming falhou, mas o fluxo segue porque --streaming-non-blocking foi solicitado. A API atual nao foi cortada para o streaming."
    run_soft env ENV_FILE="$ENV_FILE" CH_CONTAINER="$CH_CONTAINER_ID" "$ROOT_DIR/deploy/scripts/streaming-status.sh" || true
    return 0
  fi

  return 1
}

step_snapshot_cache() {
  if run_soft docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
DO $$
BEGIN
  IF to_regclass('app.snapshot_cache') IS NOT NULL THEN
    TRUNCATE TABLE app.snapshot_cache;
  END IF;
END $$;
SQL
  then
    SNAPSHOT_CACHE_STATUS="$(mark_status CLEARED)"
  else
    SNAPSHOT_CACHE_STATUS="WARN"
    record_warning "Nao foi possivel limpar app.snapshot_cache; revise o log e limpe manualmente se necessario"
  fi
}

step_post_boot() {
  if [[ -f "$ROOT_DIR/deploy/scripts/prod-post-boot-check.sh" ]]; then
    run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" RUN_ETL=0 "$ROOT_DIR/deploy/scripts/prod-post-boot-check.sh"
  fi

  run docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api python - <<'PY'
from app import repos_analytics, schemas_bi
from app.config import settings
from app.db_clickhouse import query_scalar

required_responses = {
    "DashboardHomeResponse",
    "SalesOverviewResponse",
    "CashOverviewResponse",
    "FraudOverviewResponse",
    "FinanceOverviewResponse",
}
responses = {name for name in dir(schemas_bi) if name.endswith("Response")}
missing_responses = sorted(required_responses - responses)
if missing_responses:
    raise SystemExit(f"Missing response models: {missing_responses}")
if not settings.use_clickhouse:
    raise SystemExit("settings.use_clickhouse is false inside API container")
select_one = query_scalar("SELECT 1")
if select_one != 1:
    raise SystemExit(f"ClickHouse SELECT 1 returned {select_one!r}")
inventory = repos_analytics.analytics_backend_inventory()
required_functions = {
    "dashboard_home_bundle",
    "sales_overview_bundle",
    "cash_overview",
    "fraud_kpis",
    "finance_kpis",
}
missing_functions = sorted(required_functions - set(inventory["functions"]))
if missing_functions:
    raise SystemExit(f"Analytics inventory missing functions: {missing_functions}")
print("config ok")
print({
    "use_clickhouse": settings.use_clickhouse,
    "responses": sorted(responses),
    "clickhouse_select_1": select_one,
    "functions": len(inventory["functions"]),
})
PY

  run docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api python - <<'PY'
from app import schemas_bi
print([name for name in dir(schemas_bi) if name.endswith("Response")])
PY

  API_STATUS="$(mark_status HEALTHY)"
  WEB_STATUS="$(mark_status HEALTHY)"
  POST_BOOT_STATUS="$(mark_status OK)"
}

step_enable_cron() {
  if (( SKIP_CRON )); then
    CRON_STATUS="SKIPPED"
    return 0
  fi

  run touch "$PIPELINE_LOG"
  run env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$PROD_COMPOSE_FILE" OPERATIONAL_INTERVAL_MINUTES=2 RISK_INTERVAL_MINUTES=30 PIPELINE_TIMEOUT_SECONDS=180 PIPELINE_WARN_SECONDS=60 PIPELINE_LOG="$PIPELINE_LOG" "$ROOT_DIR/deploy/scripts/prod-install-cron.sh"
  run sudo systemctl enable --now cron
  run crontab -l

  if (( DRY_RUN )); then
    CRON_RESTARTED=1
    CRON_STATUS="DRY-RUN"
    return 0
  fi

  if ! sudo systemctl is-active --quiet cron; then
    log ERROR "cron nao ficou ativo apos a reinstalacao"
    return 1
  fi

  CRON_RESTARTED=1
  CRON_STATUS="ENABLED"
}

main() {
  parse_args "$@"
  init_logging

  log INFO "Starting TorqMind homologation apply orchestrator"
  log INFO "LOG_FILE=$LOG_FILE"

  run_step "0" "Preflight local" step_preflight_local
  run_step "1" "Checagem de seguranca do env" step_security_env
  run_step "2" "Parar cron" step_stop_cron
  run_step "3" "Validar compose" step_validate_compose
  confirm
  run_step "4" "Build e recreate de API/Web/Nginx" step_build_runtime
  run_step "5" "Migracoes PostgreSQL" step_migrate
  run_step "6" "Rebuild derivado desde STG" step_derived_rebuild
  run_step "7" "ClickHouse full ou incremental" step_clickhouse
  run_step "8" "Audits" step_audits
  run_step "9" "Streaming opcional" step_streaming
  run_step "10" "Limpar snapshot cache" step_snapshot_cache
  run_step "11" "Post boot check" step_post_boot
  run_step "12" "Instalar e religar cron" step_enable_cron

  CURRENT_STEP_ID="13"
  CURRENT_STEP_LABEL="Relatorio final"
}

main "$@"