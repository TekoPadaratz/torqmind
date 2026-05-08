#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YES=false
DRY_RUN=false
WITH_DDL=false
WITH_CRON=false
WITH_VALIDATE=false
SYNC_CODE=true
PROOF_OUTPUT="${PROOF_OUTPUT:-}"

# shellcheck source=deploy/scripts/lib/multivm.sh
source "$ROOT_DIR/deploy/scripts/lib/multivm.sh"

usage() {
  cat <<'EOF'
Usage:
  ENV_FILE=/etc/torqmind/prod.app.env \
  CLUSTER_ENV=/etc/torqmind/cluster.env \
  ./deploy/scripts/prod-multivm-bootstrap.sh --yes --with-ddl --with-cron --validate

Options:
  --yes              Non-interactive
  --dry-run          Print SSH/compose commands without executing
  --with-ddl         Apply ClickHouse streaming/current/mart_rt DDL
  --with-cron        Install App-server incremental ETL cron
  --validate         Run blocking validation before proof
  --skip-sync-code   Do not sync remote working copies
  --proof-output P   Write proof JSON to path P on the orchestrator
EOF
}

while [[ $# -gt 0 ]]; do
  if tm_mv_parse_common_flag "$1"; then
    shift
    continue
  fi
  case "$1" in
    --with-ddl)
      WITH_DDL=true
      shift
      ;;
    --with-cron)
      WITH_CRON=true
      shift
      ;;
    --validate)
      WITH_VALIDATE=true
      shift
      ;;
    --skip-sync-code)
      SYNC_CODE=false
      shift
      ;;
    --proof-output)
      [[ $# -ge 2 ]] || tm_mv_die "--proof-output requires a path"
      PROOF_OUTPUT="$2"
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
tm_mv_confirm "Bootstrap TorqMind production multi-VM cluster?"

PG_ENV="$(tm_mv_env_file_for_role pg)"
ANALYTICS_ENV="$(tm_mv_env_file_for_role analytics)"
APP_ENV="$(tm_mv_env_file_for_role app)"

step() {
  tm_mv_log "=== $* ==="
}

child_common_args=(--yes)
if [[ "$DRY_RUN" == "true" ]]; then
  child_common_args+=(--dry-run)
fi

step "Validate SSH"
tm_mv_validate_ssh_all

step "Validate Docker"
tm_mv_validate_docker_all

step "Validate env files"
tm_mv_validate_remote_env_files

if [[ "$SYNC_CODE" == "true" ]]; then
  step "Sync code"
  CLUSTER_ENV="$CLUSTER_ENV" "$ROOT_DIR/deploy/scripts/prod-multivm-sync-code.sh" "${child_common_args[@]}"
fi

step "Start PostgreSQL"
tm_mv_remote_compose pg "up -d --build"
tm_mv_remote_compose pg "ps"

step "Build API image for migrations"
tm_mv_ssh app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") build api
"

step "Run PostgreSQL migrations from App container"
tm_mv_ssh app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") run --rm --no-deps api python -m app.cli.migrate
"

step "Seed production auth/config"
tm_mv_ssh app "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  docker compose -f docker-compose.app.yml --env-file $(tm_mv_quote "$APP_ENV") run --rm --no-deps api env SEED_MODE=master-only python -m app.cli.seed
"

step "Prepare PostgreSQL logical replication/publication for Debezium"
tm_mv_ssh pg "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  ENV_FILE=$(tm_mv_quote "$PG_ENV") COMPOSE_FILE=docker-compose.pg.yml ./deploy/scripts/streaming-prepare-postgres.sh
"

step "Start Analytics/Streaming"
tm_mv_remote_compose analytics "up -d --build"
tm_mv_remote_compose analytics "ps"

if [[ "$WITH_DDL" == "true" ]]; then
  step "Apply ClickHouse streaming/current/ops DDL"
  tm_mv_ssh analytics "
    cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
    ENV_FILE=$(tm_mv_quote "$ANALYTICS_ENV") COMPOSE_FILE=docker-compose.analytics.yml ./deploy/scripts/streaming-init-clickhouse.sh
  "

  step "Apply ClickHouse mart_rt DDL"
  tm_mv_ssh analytics "
    cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
    ENV_FILE=$(tm_mv_quote "$ANALYTICS_ENV") COMPOSE_FILE=docker-compose.analytics.yml ./deploy/scripts/streaming-init-mart-rt.sh
  "
fi

step "Register Debezium connector"
tm_mv_ssh analytics "
  cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
  ENV_FILE=$(tm_mv_quote "$ANALYTICS_ENV") DEBEZIUM_URL=http://127.0.0.1:18083 ./deploy/scripts/streaming-register-debezium.sh
"

step "Start App/Web/Nginx"
tm_mv_remote_compose app "up -d --build"
tm_mv_remote_compose app "ps"

if [[ "$WITH_CRON" == "true" ]]; then
  step "Install incremental ETL cron"
  CLUSTER_ENV="$CLUSTER_ENV" "$ROOT_DIR/deploy/scripts/prod-multivm-install-cron.sh" "${child_common_args[@]}"
fi

if [[ "$WITH_VALIDATE" == "true" ]]; then
  step "Run blocking validation"
  CLUSTER_ENV="$CLUSTER_ENV" "$ROOT_DIR/deploy/scripts/prod-multivm-validate.sh" "${child_common_args[@]}"
fi

step "Generate proof"
proof_args=()
if [[ -n "$PROOF_OUTPUT" ]]; then
  proof_args+=(--output "$PROOF_OUTPUT")
fi
if [[ "$DRY_RUN" == "true" ]]; then
  proof_args+=(--dry-run)
fi
CLUSTER_ENV="$CLUSTER_ENV" "$ROOT_DIR/deploy/scripts/prod-multivm-proof.sh" "${proof_args[@]}"

tm_mv_log "bootstrap complete"
