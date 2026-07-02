#!/usr/bin/env bash
# =============================================================================
# TorqMind host hygiene — SAFE weekly cleanup (run as root via /etc/cron.d).
# -----------------------------------------------------------------------------
# What it DOES (all reversible / non-destructive to data):
#   1) Removes ONLY dangling (untagged, unreferenced) Docker images.
#   2) Prunes Docker BUILD CACHE beyond KEEP_CACHE (keeps recent for fast builds).
#   3) Caps journald to JOURNAL_KEEP_DAYS days / JOURNAL_MAX size.
#   4) Truncates RUNNING-container json logs bigger than LOG_MAX_MB (Docker keeps
#      writing after truncation; no restart, no data loss beyond old log lines).
#
# What it NEVER does (hard safety rules — do not add):
#   - NO `docker system prune`, NO `-a` (would delete in-use/base images).
#   - NO `docker volume prune` / NO touching /var/lib/docker/volumes (that is DATA).
#   - NO deleting config files, env files, dumps, or backups.
#   - NO restarting the Docker daemon or any container.
#
# Idempotent and safe to run any time. Logs to LOG_FILE (self-rotating).
# =============================================================================
set -Eeuo pipefail

KEEP_CACHE="${KEEP_CACHE:-10GB}"            # build cache to KEEP (fast rebuilds)
JOURNAL_KEEP_DAYS="${JOURNAL_KEEP_DAYS:-7}" # keep at most N days of journald
JOURNAL_MAX="${JOURNAL_MAX:-500M}"          # and at most this journald size
LOG_MAX_MB="${LOG_MAX_MB:-50}"              # truncate container logs bigger than this
LOG_FILE="${LOG_FILE:-/var/log/torqmind-hygiene.log}"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" >> "$LOG_FILE" 2>/dev/null || true; }

log "=== hygiene start on $(hostname) — before: $(df -h / | tail -1 | awk '{print $3" used, "$4" free ("$5")"}') ==="

# 1) Dangling images only (never -a).
if command -v docker >/dev/null 2>&1; then
  out=$(docker image prune -f 2>&1 || true)
  log "image prune (dangling only): $(echo "$out" | tail -1)"

  # 2) Build cache beyond the keep threshold.
  out=$(docker builder prune -f --keep-storage="$KEEP_CACHE" 2>&1 || true)
  log "builder prune (keep $KEEP_CACHE): $(echo "$out" | tail -1)"

  # 4) Truncate oversized RUNNING container json logs (safe; Docker keeps writing).
  for cid in $(docker ps -q 2>/dev/null || true); do
    lf=$(docker inspect --format '{{.LogPath}}' "$cid" 2>/dev/null || true)
    [ -n "$lf" ] && [ -f "$lf" ] || continue
    sz=$(stat -c%s "$lf" 2>/dev/null || echo 0)
    if [ "$sz" -gt $(( LOG_MAX_MB * 1048576 )) ]; then
      name=$(docker inspect --format '{{.Name}}' "$cid" 2>/dev/null | tr -d '/')
      : > "$lf" 2>/dev/null && log "truncated log of ${name:-$cid} (was $(( sz / 1048576 ))MB)"
    fi
  done
fi

# 3) journald cap.
if command -v journalctl >/dev/null 2>&1; then
  journalctl --vacuum-time="${JOURNAL_KEEP_DAYS}d" >/dev/null 2>&1 || true
  journalctl --vacuum-size="$JOURNAL_MAX"          >/dev/null 2>&1 || true
  log "journald vacuumed to <= ${JOURNAL_KEEP_DAYS}d / $JOURNAL_MAX"
fi

# 5) Self-rotate this log (keep last 300 lines).
if [ -f "$LOG_FILE" ]; then
  tail -n 300 "$LOG_FILE" > "${LOG_FILE}.tmp" 2>/dev/null && mv "${LOG_FILE}.tmp" "$LOG_FILE" 2>/dev/null || true
fi
log "=== hygiene done — after: $(df -h / | tail -1 | awk '{print $3" used, "$4" free ("$5")"}') ==="
