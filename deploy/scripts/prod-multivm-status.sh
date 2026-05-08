#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YES=true
DRY_RUN=false

# shellcheck source=deploy/scripts/lib/multivm.sh
source "$ROOT_DIR/deploy/scripts/lib/multivm.sh"

usage() {
  cat <<'EOF'
Usage:
  CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-status.sh [--dry-run]
EOF
}

while [[ $# -gt 0 ]]; do
  if tm_mv_parse_common_flag "$1"; then
    shift
    continue
  fi
  case "$1" in
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

status_role() {
  local role="$1"
  local compose_file env_file
  compose_file="$(tm_mv_compose_file_for_role "$role")"
  env_file="$(tm_mv_env_file_for_role "$role")"
  printf '\n== %s (%s) ==\n' "$role" "$(tm_mv_target_for_role "$role")"
  tm_mv_ssh "$role" "
    set -euo pipefail
    cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
    test -f $(tm_mv_quote "$env_file")
    git rev-parse --short HEAD 2>/dev/null || true
    docker compose -f $(tm_mv_quote "$compose_file") --env-file $(tm_mv_quote "$env_file") ps
  "
}

for role in $(tm_mv_for_each_role); do
  status_role "$role"
done
