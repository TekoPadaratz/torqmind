#!/usr/bin/env bash
set -Eeuo pipefail

ROLE=""
YES=false
DRY_RUN=false
WITH_UFW=false

usage() {
  cat <<'EOF'
Usage:
  sudo ./deploy/scripts/prod-multivm-prepare-host.sh --role pg|analytics|app --yes [--with-ufw] [--dry-run]

Environment:
  TORQMIND_SSH_USER=deploy
  TORQMIND_REPO_DIR=/home/deploy/apps/torqmind
  TORQMIND_ENV_DIR=/etc/torqmind
  TORQMIND_APP_PRIVATE_IP=<private app ip>
  TORQMIND_ANALYTICS_PRIVATE_IP=<private analytics ip>

This script is idempotent and prepares one Ubuntu host for its assigned role.
EOF
}

log() {
  printf '%s [prepare-host] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] %s\n' "$*"
    return 0
  fi
  "$@"
}

confirm() {
  [[ "$YES" == "true" || "$DRY_RUN" == "true" ]] && return 0
  read -r -p "$1 [y/N] " answer
  case "${answer,,}" in
    y|yes|s|sim) return 0 ;;
    *) die "aborted" ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --role)
      [[ $# -ge 2 ]] || die "--role requires pg, analytics, or app"
      ROLE="$2"
      shift 2
      ;;
    --yes)
      YES=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --with-ufw)
      WITH_UFW=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$ROLE" in
  pg|analytics|app) ;;
  *) usage >&2; die "--role must be pg, analytics, or app" ;;
esac

DEPLOY_USER="${TORQMIND_SSH_USER:-deploy}"
REPO_DIR="${TORQMIND_REPO_DIR:-/home/${DEPLOY_USER}/apps/torqmind}"
ENV_DIR="${TORQMIND_ENV_DIR:-/etc/torqmind}"
LOG_DIR="${TORQMIND_LOG_DIR:-/home/${DEPLOY_USER}/logs}"

if ! id "$DEPLOY_USER" >/dev/null 2>&1; then
  die "user '$DEPLOY_USER' does not exist; create it before running this script"
fi

if ! command -v sudo >/dev/null 2>&1; then
  die "sudo is required"
fi

confirm "Prepare this host as TorqMind role '$ROLE'?"

log "Installing base packages"
run sudo apt-get update
run sudo apt-get install -y ca-certificates curl gnupg git jq tmux ufw util-linux

if ! command -v docker >/dev/null 2>&1; then
  log "Installing Docker Engine from Docker apt repository"
  run sudo install -m 0755 -d /etc/apt/keyrings
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] curl Docker GPG key and install apt source"
  else
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    . /etc/os-release
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
      | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  fi
  run sudo apt-get update
  run sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
else
  log "Docker already installed"
fi

log "Ensuring Docker and cron services"
run sudo systemctl enable --now docker
run sudo systemctl enable --now cron
run sudo usermod -aG docker "$DEPLOY_USER"

log "Creating directories"
run sudo mkdir -p "$ENV_DIR" "$(dirname "$REPO_DIR")" "$LOG_DIR"
run sudo chown -R "$DEPLOY_USER:$DEPLOY_USER" "$(dirname "$REPO_DIR")" "$LOG_DIR"
run sudo chmod 750 "$ENV_DIR"

if [[ "$WITH_UFW" == "true" ]]; then
  log "Configuring UFW for role $ROLE"
  run sudo ufw default deny incoming
  run sudo ufw default allow outgoing
  run sudo ufw allow OpenSSH
  case "$ROLE" in
    pg)
      [[ -n "${TORQMIND_APP_PRIVATE_IP:-}" ]] || die "TORQMIND_APP_PRIVATE_IP is required for pg UFW"
      [[ -n "${TORQMIND_ANALYTICS_PRIVATE_IP:-}" ]] || die "TORQMIND_ANALYTICS_PRIVATE_IP is required for pg UFW"
      run sudo ufw allow from "$TORQMIND_APP_PRIVATE_IP" to any port 5432 proto tcp
      run sudo ufw allow from "$TORQMIND_ANALYTICS_PRIVATE_IP" to any port 5432 proto tcp
      ;;
    analytics)
      [[ -n "${TORQMIND_APP_PRIVATE_IP:-}" ]] || die "TORQMIND_APP_PRIVATE_IP is required for analytics UFW"
      run sudo ufw allow from "$TORQMIND_APP_PRIVATE_IP" to any port 8123 proto tcp
      run sudo ufw allow from "$TORQMIND_APP_PRIVATE_IP" to any port 9000 proto tcp
      ;;
    app)
      run sudo ufw allow 80/tcp
      run sudo ufw allow 443/tcp
      ;;
  esac
  run sudo ufw --force enable
fi

log "Host prepared (role=$ROLE). Re-login may be required for Docker group membership."
