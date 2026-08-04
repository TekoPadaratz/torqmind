#!/usr/bin/env bash
# backfill-itens-slim-id-funcionario.sh
# Preenche id_funcionario na slim a partir da fat (ID_FUNCIONARIOS / shadow).
# Uso: ENV_FILE=/etc/torqmind/homolog.app.env ./deploy/scripts/backfill-itens-slim-id-funcionario.sh [YYYYMMDD_INI] [YYYYMMDD_FIM]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/homolog.app.env}"
CLUSTER_ENV="${CLUSTER_ENV:-/etc/torqmind/prod.analytics.env}"
# shellcheck disable=SC1090
[[ -f "$ENV_FILE" ]] && set -a && source "$ENV_FILE" && set +a
# shellcheck disable=SC1090
[[ -f "$CLUSTER_ENV" ]] && set -a && source "$CLUSTER_ENV" && set +a

CH_HOST="${CLICKHOUSE_HOST:-${CH_HOST:-127.0.0.1}}"
CH_PORT="${CLICKHOUSE_PORT:-${CLICKHOUSE_HTTP_PORT:-${CH_HTTP_PORT:-8123}}}"
CH_USER="${CLICKHOUSE_USER:-${CH_USER:-default}}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-${CH_PASSWORD:-}}"
CH_DB="${CLICKHOUSE_CURRENT_DB:-torqmind_current}"

INI="${1:-20250101}"
FIM="${2:-$(date +%Y%m%d)}"

auth=()
if [[ -n "$CH_PASSWORD" ]]; then
  auth=(-u "$CH_USER:$CH_PASSWORD")
fi

ch() {
  curl -fsS "${auth[@]}" "http://${CH_HOST}:${CH_PORT}/" \
    --data-binary @- \
    -H "X-ClickHouse-Database: ${CH_DB}"
}

echo "==> ADD COLUMN id_funcionario (se faltar)"
ch <<SQL
ALTER TABLE ${CH_DB}.stg_itenscomprovantes_slim
  ADD COLUMN IF NOT EXISTS id_funcionario Int32 DEFAULT 0 AFTER id_grupo_produto
SQL

echo "==> Backfill id_funcionario data_key ${INI}..${FIM} (só shadow — sem ler payload)"
ch <<SQL
INSERT INTO ${CH_DB}.stg_itenscomprovantes_slim (
  id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante,
  data_key, id_produto, id_grupo_produto, id_funcionario, cfop,
  qtd, total, desconto, custo_total, is_deleted, source_ts_ms
)
SELECT
  s.id_empresa, s.id_filial, s.id_db, s.id_comprovante, s.id_itemcomprovante,
  s.data_key, s.id_produto, s.id_grupo_produto,
  toInt32(ifNull(i.id_funcionario_shadow, 0)) AS id_funcionario,
  s.cfop, s.qtd, s.total, s.desconto, s.custo_total,
  s.is_deleted,
  s.source_ts_ms + 1
FROM ${CH_DB}.stg_itenscomprovantes_slim AS s FINAL
INNER JOIN ${CH_DB}.stg_itenscomprovantes AS i FINAL
  ON i.id_empresa = s.id_empresa
 AND i.id_filial = s.id_filial
 AND i.id_db = s.id_db
 AND i.id_comprovante = s.id_comprovante
 AND i.id_itemcomprovante = s.id_itemcomprovante
WHERE s.data_key BETWEEN ${INI} AND ${FIM}
SETTINGS max_execution_time = 600, max_memory_usage = 6000000000
SQL

echo "==> Amostra"
ch <<SQL
SELECT
  count() AS rows_slim,
  countIf(id_funcionario > 0) AS with_func,
  round(100.0 * countIf(id_funcionario > 0) / nullIf(count(), 0), 1) AS pct
FROM ${CH_DB}.stg_itenscomprovantes_slim FINAL
WHERE data_key BETWEEN ${INI} AND ${FIM}
SQL

echo "OK backfill id_funcionario"
