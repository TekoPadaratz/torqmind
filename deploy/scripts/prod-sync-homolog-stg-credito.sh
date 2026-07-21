#!/usr/bin/env bash
# Sincroniza STG críticos prod → homolog (funcionarios, entidades, …)
# e materializa mash antifraude crédito funcionário no PG homolog.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
SOURCE_DB="${SOURCE_DB:-torqmind}"
TARGET_DB="${TARGET_DB:-torqmind_homolog}"
ID_EMPRESA="${ID_EMPRESA:-1}"
TABLES="${TABLES:-funcionarios,entidades}"
ANO_MES="${ANO_MES:-}"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export PG_HOST="${POSTGRES_HOST:-172.30.0.8}"
export PG_PORT="${POSTGRES_PORT:-5432}"
export PG_USER="${POSTGRES_USER:?}"
export PG_PASSWORD="${POSTGRES_PASSWORD:?}"

cd "$ROOT_DIR"
PATH="$ROOT_DIR/.venv/bin:$PATH" python -u tools/sync_stg_pg_to_homolog.py \
  --source-db "$SOURCE_DB" \
  --target-db "$TARGET_DB" \
  --tables "$TABLES" \
  --id-empresa "$ID_EMPRESA"

# Mash antifraude no homolog (API homolog aponta para torqmind_homolog)
if [[ -z "$ANO_MES" ]]; then
  ANO_MES="$(date +%Y%m)"
fi

echo "refresh_fraud_credito_funcionario empresa=${ID_EMPRESA} mes=${ANO_MES} on ${TARGET_DB}"
PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$TARGET_DB" -v ON_ERROR_STOP=1 -c \
  "SELECT etl.refresh_fraud_credito_funcionario(${ID_EMPRESA}, ${ANO_MES});"

# Homolog costuma ter CONTASRECEBER parcial — espelha mash canônico do prod no PG homolog
# (a API realtime lê ClickHouse; este passo garante fallback PG e provas locais).
echo "mirror mart fraud credito funcionario prod → homolog"
PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$TARGET_DB" -v ON_ERROR_STOP=1 -c \
  "DELETE FROM mart.fraud_credito_funcionario_uso WHERE id_empresa=${ID_EMPRESA};
   DELETE FROM mart.fraud_credito_funcionario_resumo WHERE id_empresa=${ID_EMPRESA};"
pg_dump -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$SOURCE_DB" --data-only --no-owner \
  -t mart.fraud_credito_funcionario_resumo \
  -t mart.fraud_credito_funcionario_uso \
  | PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$TARGET_DB" -v ON_ERROR_STOP=1

# Publica no CH compartilhado via API prod (homolog CH user é RO)
if docker ps --format '{{.Names}}' | grep -qx torqmind-api; then
  echo "publish fraud credito funcionario → ClickHouse (via prod API)"
  docker exec torqmind-api python -c \
    "from app.repos_mart import publish_fraud_credito_funcionario_to_ch; print(publish_fraud_credito_funcionario_to_ch('platform_master', ${ID_EMPRESA}, ${ANO_MES}))"
fi

echo "DONE homolog sync+refresh"
