#!/usr/bin/env bash
# Publica marts de cliente preço fixo (PG homolog/prod → CH write).
# Homolog API é RO no CH — este script usa prod.analytics.env (user de escrita).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_APP="${ENV_FILE:-/etc/torqmind/homolog.app.env}"
ENV_CH="${CLUSTER_ENV:-/etc/torqmind/prod.analytics.env}"
DAYS="${DAYS:-120}"
EMPRESA="${ID_EMPRESA:-1}"

set -a
# shellcheck disable=SC1090
source "$ENV_APP"
# shellcheck disable=SC1090
source "$ENV_CH"
set +a

export PG_DATABASE="${PG_DATABASE:-${POSTGRES_DB:-torqmind_homolog}}"
export POSTGRES_DB="$PG_DATABASE"
export PYTHONPATH="$ROOT/apps/api${PYTHONPATH:+:$PYTHONPATH}"

cd "$ROOT"
.venv/bin/python - <<PY
from app.services.cliente_preco_fixo import publish_and_rebuild
import json
out = publish_and_rebuild("platform_master", int("${EMPRESA}"), days=int("${DAYS}"))
print(json.dumps(out, default=str))
PY
