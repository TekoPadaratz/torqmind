#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YES=false
DRY_RUN=false
SKIP_FIRST_RUN=false

# shellcheck source=deploy/scripts/lib/multivm.sh
source "$ROOT_DIR/deploy/scripts/lib/multivm.sh"

usage() {
  cat <<'EOF'
Usage:
  CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-install-cron.sh [--yes] [--dry-run] [--skip-first-run]

Installs the incremental STG->DW cron on the App server only. The cron uses
flock and writes to /home/deploy/logs/prod-etl-incremental-cron.log by default.
EOF
}

while [[ $# -gt 0 ]]; do
  if tm_mv_parse_common_flag "$1"; then
    shift
    continue
  fi
  case "$1" in
    --skip-first-run)
      SKIP_FIRST_RUN=true
      shift
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
tm_mv_confirm "Install TorqMind incremental ETL cron on the App server?"

APP_ENV_FILE="$(tm_mv_env_file_for_role app)"
CRON_INTERVAL_MINUTES="${CRON_INTERVAL_MINUTES:-2}"
CRON_TRACK="${CRON_TRACK:-operational}"
CRON_LOG="${CRON_LOG:-/home/${TORQMIND_SSH_USER}/logs/prod-etl-incremental-cron.log}"
CRON_LOCK="${CRON_LOCK:-/tmp/torqmind-prod-etl-incremental-cron.lock}"
FIRST_RUN_TIMEOUT_SECONDS="${FIRST_RUN_TIMEOUT_SECONDS:-180}"

tm_mv_log "installing cron on app"
tm_mv_ssh_raw app "$TORQMIND_REPO_DIR" "$APP_ENV_FILE" "$CRON_INTERVAL_MINUTES" "$CRON_TRACK" "$CRON_LOG" "$CRON_LOCK" "$FIRST_RUN_TIMEOUT_SECONDS" "$SKIP_FIRST_RUN" <<'REMOTE'
set -Eeuo pipefail

repo_dir="$1"
env_file="$2"
interval="$3"
track="$4"
log_file="$5"
lock_file="$6"
first_run_timeout="$7"
skip_first_run="$8"

if ! [[ "$interval" =~ ^[0-9]+$ ]] || [[ "$interval" -lt 1 || "$interval" -gt 59 ]]; then
  echo "CRON_INTERVAL_MINUTES must be between 1 and 59" >&2
  exit 2
fi

mkdir -p "$(dirname "$log_file")"
touch "$log_file"

job_cmd="cd $repo_dir && flock -n $lock_file env ENV_FILE=$env_file COMPOSE_FILE=docker-compose.app.yml TRACK=$track ./deploy/scripts/prod-etl-incremental.sh"
cron_line="*/$interval * * * * $job_cmd >> $log_file 2>&1"
marker="# TorqMind multi-VM incremental ETL"

existing_cron="$(crontab -l 2>/dev/null || true)"
filtered_cron="$(printf '%s\n' "$existing_cron" \
  | grep -v 'TorqMind multi-VM incremental ETL' \
  | grep -v 'prod-etl-incremental-cron.log' \
  | grep -v 'prod-etl-incremental.sh' || true)"

{
  printf '%s\n' "$filtered_cron"
  printf '%s\n' "$marker"
  printf '%s\n' "$cron_line"
} | awk 'NF || last {print} {last=NF}' | crontab -

echo "Cron installed: $cron_line"

if [[ "$skip_first_run" != "true" ]]; then
  echo "Validating first ETL execution with timeout ${first_run_timeout}s"
  timeout "$first_run_timeout" bash -lc "$job_cmd"
fi
REMOTE

tm_mv_log "cron installation complete"
