#!/usr/bin/env bash
# deploy/scripts/prod-pg-profit-refresh.sh
#
# Refresh RAPIDO e confiavel (PG-only, sem ClickHouse) das marts que alimentam
# as telas de Liquidez/Solvencia (com estoque) e Gestao Orcamentaria.
#
# Estas funcoes NAO estao no orquestrador operacional (*/2), entao ficam frescas
# via este wrapper agendado. Idempotentes + statement_timeout para nunca travar.
#
# Uso: ENV_FILE=/etc/torqmind/prod.app.env ID_EMPRESA=1 ./deploy/scripts/prod-pg-profit-refresh.sh
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-180s}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ENV_FILE nao encontrado: $ENV_FILE" >&2
  exit 1
fi

envval() { grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2-; }

PG_H="$(envval PG_HOST)";        PG_H="${PG_H:-172.30.0.8}"
PG_P="$(envval PG_PORT)";        PG_P="${PG_P:-5432}"
PG_U="$(envval POSTGRES_USER)";  PG_U="${PG_U:-torqmind}"
PG_DB="$(envval POSTGRES_DB)";   PG_DB="${PG_DB:-torqmind}"
PGPASSWORD="$(envval POSTGRES_PASSWORD)"
export PGPASSWORD
if [[ -z "$PGPASSWORD" ]]; then
  echo "POSTGRES_PASSWORD ausente em $ENV_FILE" >&2
  exit 1
fi

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] PG profit/liquidez/orcamentaria refresh (empresa=${ID_EMPRESA})"

psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" -v ON_ERROR_STOP=1 --no-align -t <<SQL
SET statement_timeout='${STATEMENT_TIMEOUT}';
SELECT etl.refresh_liquidez_solvencia(${ID_EMPRESA});
SELECT etl.refresh_liquidez_estoque(${ID_EMPRESA});
SELECT etl.refresh_gestao_orcamentaria(${ID_EMPRESA});
SELECT etl.refresh_solvencia_itens(${ID_EMPRESA});
SQL

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] OK: liquidez_solvencia + gestao_orcamentaria atualizados."
