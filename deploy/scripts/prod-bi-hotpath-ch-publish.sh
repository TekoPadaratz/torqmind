#!/usr/bin/env bash
# Publica marts BI hot-path PG → ClickHouse (053).
# Uso: ENV_FILE=/etc/torqmind/prod.app.env ./deploy/scripts/prod-bi-hotpath-ch-publish.sh [id_empresa]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
ID_EMPRESA="${1:-1}"
# Prefer analytics env for CH password
if [[ -f /etc/torqmind/prod.analytics.env ]]; then
  set -a; source /etc/torqmind/prod.analytics.env; set +a
fi
set -a; source "$ENV_FILE"; set +a
echo "=== BI hot-path CH publish empresa=$ID_EMPRESA ==="
docker exec torqmind-api python -c "from app.publish_bi_ch import publish_all_hotpath; import json; print(json.dumps(publish_all_hotpath('platform_master', $ID_EMPRESA), default=str))"
