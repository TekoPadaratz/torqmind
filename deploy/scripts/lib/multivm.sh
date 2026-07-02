#!/usr/bin/env bash

tm_mv_log() {
  printf '%s [multivm] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

tm_mv_die() {
  echo "ERROR: $*" >&2
  exit 1
}

tm_mv_repo_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd
}

tm_mv_quote() {
  printf '%q' "$1"
}

tm_mv_load_cluster_env() {
  CLUSTER_ENV="${CLUSTER_ENV:-/etc/torqmind/cluster.env}"
  if [[ ! -f "$CLUSTER_ENV" ]]; then
    tm_mv_die "cluster env not found: $CLUSTER_ENV"
  fi
  set -a
  # shellcheck disable=SC1090
  source "$CLUSTER_ENV"
  set +a

  local required=(
    TORQMIND_SSH_USER
    TORQMIND_PG_HOST
    TORQMIND_ANALYTICS_HOST
    TORQMIND_APP_HOST
    TORQMIND_REPO_DIR
    TORQMIND_BRANCH
    TORQMIND_ENV_DIR
  )
  local key
  for key in "${required[@]}"; do
    if [[ -z "${!key:-}" ]]; then
      tm_mv_die "required variable missing in $CLUSTER_ENV: $key"
    fi
  done
}

tm_mv_host_for_role() {
  case "$1" in
    pg) printf '%s' "$TORQMIND_PG_HOST" ;;
    analytics) printf '%s' "$TORQMIND_ANALYTICS_HOST" ;;
    app) printf '%s' "$TORQMIND_APP_HOST" ;;
    *) tm_mv_die "unknown role: $1" ;;
  esac
}

tm_mv_target_for_role() {
  local host
  host="$(tm_mv_host_for_role "$1")"
  printf '%s@%s' "$TORQMIND_SSH_USER" "$host"
}

tm_mv_env_file_for_role() {
  case "$1" in
    pg) printf '%s/prod.pg.env' "$TORQMIND_ENV_DIR" ;;
    analytics) printf '%s/prod.analytics.env' "$TORQMIND_ENV_DIR" ;;
    app) printf '%s/prod.app.env' "$TORQMIND_ENV_DIR" ;;
    *) tm_mv_die "unknown role: $1" ;;
  esac
}

tm_mv_compose_file_for_role() {
  case "$1" in
    pg) printf '%s' "docker-compose.pg.yml" ;;
    analytics) printf '%s' "docker-compose.analytics.yml" ;;
    app) printf '%s' "docker-compose.app.yml" ;;
    *) tm_mv_die "unknown role: $1" ;;
  esac
}

tm_mv_ssh() {
  local role="$1"
  shift
  local cmd="$*"
  local target
  target="$(tm_mv_target_for_role "$role")"

  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    printf '[dry-run] ssh %s bash -lc %s\n' "$target" "$(tm_mv_quote "$cmd")"
    return 0
  fi

  ssh \
    -o BatchMode=yes \
    -o ConnectTimeout="${SSH_CONNECT_TIMEOUT:-10}" \
    -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING:-accept-new}" \
    "$target" \
    "bash -lc $(tm_mv_quote "$cmd")"
}

tm_mv_ssh_raw() {
  local role="$1"
  shift
  local target
  target="$(tm_mv_target_for_role "$role")"

  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    printf '[dry-run] ssh %s bash -s\n' "$target"
    cat
    return 0
  fi

  ssh \
    -o BatchMode=yes \
    -o ConnectTimeout="${SSH_CONNECT_TIMEOUT:-10}" \
    -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING:-accept-new}" \
    "$target" \
    "bash -s" "$@"
}

tm_mv_for_each_role() {
  printf '%s\n' pg analytics app
}

tm_mv_validate_ssh_all() {
  local role
  for role in $(tm_mv_for_each_role); do
    tm_mv_log "checking SSH for $role ($(tm_mv_target_for_role "$role"))"
    tm_mv_ssh "$role" "printf 'ssh-ok\n' >/dev/null"
  done
}

tm_mv_validate_docker_all() {
  local role
  for role in $(tm_mv_for_each_role); do
    tm_mv_log "checking Docker/Compose for $role"
    tm_mv_ssh "$role" "docker version >/dev/null && docker compose version >/dev/null"
  done
}

tm_mv_validate_remote_env_files() {
  local role env_file
  for role in $(tm_mv_for_each_role); do
    env_file="$(tm_mv_env_file_for_role "$role")"
    tm_mv_log "checking env for $role: $env_file"
    tm_mv_ssh "$role" "test -f $(tm_mv_quote "$env_file") && test -s $(tm_mv_quote "$env_file")"
  done
}

tm_mv_remote_compose() {
  local role="$1"
  shift
  local compose_file env_file
  compose_file="$(tm_mv_compose_file_for_role "$role")"
  env_file="$(tm_mv_env_file_for_role "$role")"
  tm_mv_ssh "$role" "cd $(tm_mv_quote "$TORQMIND_REPO_DIR") && docker compose -f $(tm_mv_quote "$compose_file") --env-file $(tm_mv_quote "$env_file") $*"
}

tm_mv_confirm() {
  local prompt="${1:-Continue?}"
  if [[ "${YES:-false}" == "true" || "${DRY_RUN:-false}" == "true" ]]; then
    return 0
  fi
  read -r -p "$prompt [y/N] " answer
  case "${answer,,}" in
    y|yes|s|sim) return 0 ;;
    *) tm_mv_die "aborted" ;;
  esac
}

tm_mv_parse_common_flag() {
  case "$1" in
    --yes)
      YES=true
      return 0
      ;;
    --dry-run)
      DRY_RUN=true
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}
