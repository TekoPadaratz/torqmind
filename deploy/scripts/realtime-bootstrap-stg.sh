#!/usr/bin/env bash
set -Eeuo pipefail

# Realtime Bootstrap STG
# Copies STG tables from PostgreSQL directly to ClickHouse current layer
# using ClickHouse's postgresql() table function.
#
# Use this when Debezium snapshot did not complete for large tables.
# After bootstrap, Debezium handles incremental CDC from the WAL.
#
# Usage:
#   COMPOSE_FILE=docker-compose.yml ENV_FILE=.env.e2e.local \
#     bash deploy/scripts/realtime-bootstrap-stg.sh --id-empresa 1 --from-date 2025-01-01
#
# Prerequisites:
#   - PostgreSQL and ClickHouse containers running on same network
#   - ClickHouse current tables already created (streaming-init-clickhouse.sh)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

PG_USER="${POSTGRES_USER:-${PG_USER:-postgres}}"
PG_PASS="${POSTGRES_PASSWORD:-${PG_PASSWORD:-postgres}}"
PG_DB="${POSTGRES_DB:-${PG_DATABASE:-TORQMIND}}"
PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
CH_USER="${CLICKHOUSE_USER:-torqmind}"
CH_PASS="${CLICKHOUSE_PASSWORD:-torqmind}"

ID_EMPRESA="${ID_EMPRESA:-1}"
FROM_DATE="${FROM_DATE:-2025-01-01}"

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --id-empresa) ID_EMPRESA="$2"; shift 2;;
    --from-date) FROM_DATE="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

log() {
  printf '%s [BOOTSTRAP] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

ch_exec() {
  compose exec -T clickhouse clickhouse-client \
    --user "$CH_USER" --password "$CH_PASS" \
    --format TabSeparated \
    "$@"
}

pg_func() {
  echo "postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', '\$1', '${PG_USER}', '${PG_PASS}', 'stg')"
}

bootstrap_comprovantes() {
  log "Bootstrapping stg.comprovantes → torqmind_current.stg_comprovantes ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'comprovantes', '${PG_USER}', '${PG_PASS}', 'stg')"

  local count
  count="$(ch_exec -q "SELECT count() FROM $src WHERE id_empresa = $ID_EMPRESA AND dt_evento >= '$FROM_DATE'" 2>/dev/null)"
  count="${count//[[:space:]]/}"
  log "  Source rows: $count"

  if (( count == 0 )); then
    log "  Skipping (no data)"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.stg_comprovantes (
        id_empresa, id_filial, id_db, id_comprovante,
        payload, dt_evento, id_db_shadow, id_chave_natural, received_at,
        referencia_shadow, id_usuario_shadow, id_turno_shadow,
        id_cliente_shadow, valor_total_shadow, cancelado_shadow, situacao_shadow,
        is_deleted, source_ts_ms
    )
    SELECT
        id_empresa, id_filial, id_db, id_comprovante,
        payload::text, dt_evento, id_db, id_chave_natural, received_at,
        referencia_shadow, id_usuario_shadow, id_turno_shadow,
        NULL, valor_total_shadow, cancelado_shadow::Int32, situacao_shadow,
        0, toInt64(toUnixTimestamp(coalesce(received_at, now())) * 1000)
    FROM $src
    WHERE id_empresa = $ID_EMPRESA AND dt_evento >= '$FROM_DATE'
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.stg_comprovantes FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

bootstrap_itenscomprovantes() {
  log "Bootstrapping stg.itenscomprovantes → torqmind_current.stg_itenscomprovantes ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'itenscomprovantes', '${PG_USER}', '${PG_PASS}', 'stg')"

  local count
  count="$(ch_exec -q "SELECT count() FROM $src WHERE id_empresa = $ID_EMPRESA" 2>/dev/null)"
  count="${count//[[:space:]]/}"
  log "  Source rows: $count"

  if (( count == 0 )); then
    log "  Skipping (no data)"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.stg_itenscomprovantes (
        id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante,
        payload, dt_evento, id_db_shadow, id_chave_natural, received_at,
        id_produto_shadow, id_grupo_produto_shadow, id_local_venda_shadow,
        id_funcionario_shadow, cfop_shadow, qtd_shadow, valor_unitario_shadow,
        total_shadow, desconto_shadow, custo_unitario_shadow,
        is_deleted, source_ts_ms
    )
    SELECT
        id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante,
        payload::text, dt_evento, id_db, id_chave_natural, received_at,
        id_produto_shadow, id_grupo_produto_shadow, id_local_venda_shadow,
        id_funcionario_shadow, cfop_shadow, qtd_shadow, valor_unitario_shadow,
        total_shadow, desconto_shadow, custo_unitario_shadow,
        0, toInt64(toUnixTimestamp(coalesce(received_at, now())) * 1000)
    FROM $src
    WHERE id_empresa = $ID_EMPRESA
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.stg_itenscomprovantes FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

bootstrap_formas_pgto() {
  log "Bootstrapping stg.formas_pgto_comprovantes → torqmind_current.stg_formas_pgto_comprovantes ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'formas_pgto_comprovantes', '${PG_USER}', '${PG_PASS}', 'stg')"

  local count
  count="$(ch_exec -q "SELECT count() FROM $src WHERE id_empresa = $ID_EMPRESA" 2>/dev/null)"
  count="${count//[[:space:]]/}"
  log "  Source rows: $count"

  if (( count == 0 )); then
    log "  Skipping (no data)"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.stg_formas_pgto_comprovantes (
        id_empresa, id_filial, id_referencia, tipo_forma,
        payload, dt_evento, id_db_shadow, id_chave_natural, received_at,
        valor_shadow, nsu_shadow, autorizacao_shadow, bandeira_shadow,
        rede_shadow, tef_shadow, is_deleted, source_ts_ms
    )
    SELECT
        id_empresa, id_filial, id_referencia, tipo_forma,
        payload::text, dt_evento, id_db_shadow, id_chave_natural, received_at,
        valor_shadow, nsu_shadow, autorizacao_shadow, bandeira_shadow,
        rede_shadow, tef_shadow,
        0, toInt64(toUnixTimestamp(coalesce(received_at, now())) * 1000)
    FROM $src
    WHERE id_empresa = $ID_EMPRESA
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.stg_formas_pgto_comprovantes FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

main() {
  log "=== Bootstrap STG: PostgreSQL → ClickHouse current ==="
  log "  id_empresa=$ID_EMPRESA from_date=$FROM_DATE"
  log "  Using ClickHouse postgresql() table function for direct transfer"

  bootstrap_comprovantes
  bootstrap_itenscomprovantes
  bootstrap_formas_pgto

  log "=== Bootstrap complete. Running mart backfill... ==="

  # Trigger mart backfill from current STG data
  compose exec -T cdc-consumer python -m torqmind_cdc_consumer.cli backfill-stg \
    --from-date "$FROM_DATE" \
    --id-empresa "$ID_EMPRESA"

  log "=== Done. Debezium will handle incremental CDC from here. ==="
}

main "$@"
