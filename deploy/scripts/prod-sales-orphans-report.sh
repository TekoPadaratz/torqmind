#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"
ID_EMPRESA="${ID_EMPRESA:?missing ID_EMPRESA}"
ID_FILIAL="${ID_FILIAL:-}"
LIMIT="${LIMIT:-50}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

if [[ "$ALLOW_INSECURE_ENV" != "1" ]]; then
  tm_require_prod_runtime_env "$ENV_FILE"
fi

cd "$ROOT_DIR"

if ! [[ "$ID_EMPRESA" =~ ^[0-9]+$ ]]; then
  echo "ID_EMPRESA must be numeric" >&2
  exit 2
fi
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" && ! "$ID_FILIAL" =~ ^[0-9]+$ ]]; then
  echo "ID_FILIAL must be numeric or -1" >&2
  exit 2
fi

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
}

pg_branch=""
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" ]]; then
  pg_branch=" AND i.id_filial = ${ID_FILIAL}"
fi

echo "== TorqMind sales orphan item report =="
echo "empresa=${ID_EMPRESA} filial=${ID_FILIAL:-todas}"
echo

orphan_count="$(pg "SELECT count(*)::bigint FROM dw.fact_venda_item i LEFT JOIN dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_comprovante = i.id_comprovante WHERE i.id_empresa = ${ID_EMPRESA}${pg_branch} AND v.id_comprovante IS NULL;")"
echo "orphan_items=${orphan_count}"
echo "severity=WARN"
echo "cause=dw.fact_venda_item has item rows whose parent comprovante is absent from dw.fact_venda for the same tenant/branch/db/comprovante key."
echo "recommendation=Audit upstream comprovantes/itenscomprovantes ingestion and DW sales ETL; do not delete automatically."
echo
echo "examples:"
pg "
SELECT
  i.id_filial || '|' ||
  COALESCE(i.data_key::text, '') || '|' ||
  COALESCE(i.id_comprovante::text, '') || '|' ||
  COALESCE(i.id_itemcomprovante::text, '') || '|' ||
  COALESCE(i.id_movprodutos::text, '') || '|' ||
  COALESCE(i.id_itensmovprodutos::text, '') || '|' ||
  COALESCE(i.id_db::text, '') || '|' ||
  COALESCE(i.updated_at::text, '')
FROM dw.fact_venda_item i
LEFT JOIN dw.fact_venda v
  ON v.id_empresa = i.id_empresa
 AND v.id_filial = i.id_filial
 AND v.id_db = i.id_db
 AND v.id_comprovante = i.id_comprovante
WHERE i.id_empresa = ${ID_EMPRESA}${pg_branch}
  AND v.id_comprovante IS NULL
ORDER BY i.data_key DESC NULLS LAST, i.id_filial, i.id_comprovante, i.id_itemcomprovante
LIMIT ${LIMIT};
"
