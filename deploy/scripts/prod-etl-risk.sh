#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

export TRACK="${TRACK:-risk}"
export SKIP_BUSY_TENANTS="${SKIP_BUSY_TENANTS:-true}"
export LOCK_FILE="${LOCK_FILE:-/tmp/torqmind-prod-etl-pipeline.lock}"

"$ROOT_DIR/deploy/scripts/prod-etl-incremental.sh"
