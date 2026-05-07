#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YES=false
DRY_RUN=false

# shellcheck source=deploy/scripts/lib/multivm.sh
source "$ROOT_DIR/deploy/scripts/lib/multivm.sh"

usage() {
  cat <<'EOF'
Usage:
  CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-sync-code.sh [--yes] [--dry-run]

Clones or force-syncs TORQMIND_BRANCH on all three hosts. This only resets the
remote deployment working copies, never the local workspace.
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

if [[ -z "${TORQMIND_REPO_URL:-}" ]]; then
  TORQMIND_REPO_URL="$(git -C "$ROOT_DIR" config --get remote.origin.url || true)"
fi
[[ -n "$TORQMIND_REPO_URL" ]] || tm_mv_die "TORQMIND_REPO_URL is not set and local remote.origin.url is unavailable"

tm_mv_confirm "Sync branch '$TORQMIND_BRANCH' to all TorqMind hosts?"

sync_role() {
  local role="$1"
  local repo_parent
  repo_parent="$(dirname "$TORQMIND_REPO_DIR")"
  tm_mv_log "syncing code on $role"
  tm_mv_ssh "$role" "
    set -euo pipefail
    mkdir -p $(tm_mv_quote "$repo_parent")
    if [[ ! -d $(tm_mv_quote "$TORQMIND_REPO_DIR/.git") ]]; then
      git clone $(tm_mv_quote "$TORQMIND_REPO_URL") $(tm_mv_quote "$TORQMIND_REPO_DIR")
    fi
    cd $(tm_mv_quote "$TORQMIND_REPO_DIR")
    git remote set-url origin $(tm_mv_quote "$TORQMIND_REPO_URL")
    git fetch --prune origin
    git checkout $(tm_mv_quote "$TORQMIND_BRANCH")
    git reset --hard $(tm_mv_quote "origin/$TORQMIND_BRANCH")
    find deploy/scripts -type f -name '*.sh' -exec chmod +x {} +
    git rev-parse --short HEAD
  "
}

for role in $(tm_mv_for_each_role); do
  sync_role "$role"
done

tm_mv_log "code sync complete"
