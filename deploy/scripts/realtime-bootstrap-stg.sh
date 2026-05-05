#!/usr/bin/env bash
set -Eeuo pipefail

# Realtime Bootstrap STG
# Copies ALL STG and dimension tables from PostgreSQL directly to ClickHouse current layer
# using ClickHouse's postgresql() table function.
#
# Use this when Debezium snapshot did not complete for large tables,
# or as the canonical way to seed historical data before cutover.
#
# Usage:
#   COMPOSE_FILE=docker-compose.yml ENV_FILE=.env.e2e.local \
#     bash deploy/scripts/realtime-bootstrap-stg.sh --id-empresa 1 --from-date 2025-01-01
#
# Prerequisites:
#   - PostgreSQL and ClickHouse containers running on same network
#   - ClickHouse current tables already created (streaming-init-clickhouse.sh)
#
# Security:
#   - Credentials are passed via ClickHouse postgresql() function arguments.
#   - ClickHouse query_log may contain the SQL. If deploying in production,
#     consider setting log_queries=0 for the bootstrap session or using named
#     collections if available (ClickHouse 23.8+).
#   - This script suppresses SQL output via 2>/dev/null on ch_exec calls.

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
SKIP_PARITY=0
SKIP_MART_BACKFILL=0

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --id-empresa) ID_EMPRESA="$2"; shift 2;;
    --from-date) FROM_DATE="$2"; shift 2;;
    --skip-parity) SKIP_PARITY=1; shift;;
    --skip-mart-backfill) SKIP_MART_BACKFILL=1; shift;;
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
    --send_logs_level=error \
    "$@"
}

pg_scalar() {
  compose exec -T postgres psql \
    -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null || echo "0"
}

# --- Core fact tables ---

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

# --- Dimension / lookup tables ---

bootstrap_simple_stg() {
  local pg_table="$1"
  local ch_table="$2"
  local pk_cols="$3"
  local required="$4"

  log "Bootstrapping stg.$pg_table → torqmind_current.$ch_table ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', '${pg_table}', '${PG_USER}', '${PG_PASS}', 'stg')"

  local pg_count
  pg_count="$(pg_scalar "SELECT count(*) FROM stg.${pg_table} WHERE id_empresa = ${ID_EMPRESA}")"
  pg_count="${pg_count//[[:space:]]/}"
  log "  PG rows: $pg_count"

  if (( pg_count == 0 )); then
    if [[ "$required" == "required" ]]; then
      log "  ERROR: Required table stg.$pg_table has 0 rows in PostgreSQL!"
      return 1
    fi
    log "  WARN: Optional table stg.$pg_table has 0 rows - skipping"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.${ch_table} (
        ${pk_cols}, payload, ingested_at, dt_evento,
        id_db_shadow, id_chave_natural, received_at, is_deleted, source_ts_ms
    )
    SELECT
        ${pk_cols}, payload::text, ingested_at, dt_evento,
        id_db_shadow, id_chave_natural, received_at,
        0, toInt64(toUnixTimestamp(coalesce(received_at, now())) * 1000)
    FROM ${src}
    WHERE id_empresa = $ID_EMPRESA
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.${ch_table} FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

bootstrap_contaspagar() {
  log "Bootstrapping stg.contaspagar → torqmind_current.stg_contaspagar ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'contaspagar', '${PG_USER}', '${PG_PASS}', 'stg')"

  local pg_count
  pg_count="$(pg_scalar "SELECT count(*) FROM stg.contaspagar WHERE id_empresa = ${ID_EMPRESA}")"
  pg_count="${pg_count//[[:space:]]/}"
  log "  PG rows: $pg_count"

  if (( pg_count == 0 )); then
    log "  WARN: stg.contaspagar has 0 rows - skipping"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.stg_contaspagar (
        id_empresa, id_filial, id_db, id_contaspagar,
        payload, ingested_at, dt_evento, id_db_shadow, id_chave_natural, received_at,
        is_deleted, source_ts_ms
    )
    SELECT
        id_empresa, id_filial, id_db, id_contaspagar,
        payload::text, ingested_at, dt_evento, id_db_shadow, id_chave_natural, received_at,
        0, toInt64(toUnixTimestamp(coalesce(received_at, now())) * 1000)
    FROM ${src}
    WHERE id_empresa = $ID_EMPRESA
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.stg_contaspagar FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

bootstrap_contasreceber() {
  log "Bootstrapping stg.contasreceber → torqmind_current.stg_contasreceber ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'contasreceber', '${PG_USER}', '${PG_PASS}', 'stg')"

  local pg_count
  pg_count="$(pg_scalar "SELECT count(*) FROM stg.contasreceber WHERE id_empresa = ${ID_EMPRESA}")"
  pg_count="${pg_count//[[:space:]]/}"
  log "  PG rows: $pg_count"

  if (( pg_count == 0 )); then
    log "  WARN: stg.contasreceber has 0 rows - skipping"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.stg_contasreceber (
        id_empresa, id_filial, id_db, id_contasreceber,
        payload, ingested_at, dt_evento, id_db_shadow, id_chave_natural, received_at,
        is_deleted, source_ts_ms
    )
    SELECT
        id_empresa, id_filial, id_db, id_contasreceber,
        payload::text, ingested_at, dt_evento, id_db_shadow, id_chave_natural, received_at,
        0, toInt64(toUnixTimestamp(coalesce(received_at, now())) * 1000)
    FROM ${src}
    WHERE id_empresa = $ID_EMPRESA
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.stg_contasreceber FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

bootstrap_payment_type_map() {
  log "Bootstrapping app.payment_type_map → torqmind_current.payment_type_map ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'payment_type_map', '${PG_USER}', '${PG_PASS}', 'app')"

  local pg_count
  pg_count="$(pg_scalar "SELECT count(*) FROM app.payment_type_map")"
  pg_count="${pg_count//[[:space:]]/}"
  log "  PG rows: $pg_count"

  if (( pg_count == 0 )); then
    log "  WARN: app.payment_type_map has 0 rows - skipping"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.payment_type_map (
        id, id_empresa, tipo_forma, label, category, severity_hint, active,
        is_deleted, source_ts_ms, ingested_at, updated_at
    )
    SELECT
        id, id_empresa, tipo_forma, label, category,
        coalesce(severity_hint, 'INFO'),
        active::UInt8,
        0,
        toInt64(toUnixTimestamp(coalesce(updated_at, now())) * 1000),
        now64(6),
        coalesce(updated_at, now64(6))
    FROM ${src}
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.payment_type_map FINAL WHERE is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

bootstrap_goals() {
  log "Bootstrapping app.goals → torqmind_current.goals ..."
  local src="postgresql('${PG_HOST}:${PG_PORT}', '${PG_DB}', 'goals', '${PG_USER}', '${PG_PASS}', 'app')"

  local pg_count
  pg_count="$(pg_scalar "SELECT count(*) FROM app.goals WHERE id_empresa = $ID_EMPRESA")"
  pg_count="${pg_count//[[:space:]]/}"
  log "  PG rows for tenant ${ID_EMPRESA}: $pg_count"

  if (( pg_count == 0 )); then
    log "  WARN: app.goals has 0 rows for tenant ${ID_EMPRESA} - skipping"
    return 0
  fi

  ch_exec -q "
    INSERT INTO torqmind_current.goals (
        id, id_empresa, id_filial, goal_date, goal_type, target_value, created_at,
        is_deleted, source_ts_ms, ingested_at
    )
    SELECT
        id,
        id_empresa,
        id_filial,
        goal_date,
        goal_type,
        target_value,
        created_at,
        0,
        toInt64(toUnixTimestamp(coalesce(created_at, now())) * 1000),
        now64(6)
    FROM ${src}
    WHERE id_empresa = ${ID_EMPRESA}
  " 2>/dev/null

  local ch_count
  ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.goals FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
  log "  ClickHouse total after: ${ch_count//[[:space:]]/}"
}

# --- Parity check ---

validate_parity() {
  log "=== Validating PG vs ClickHouse parity ==="
  local errors=0

  check_parity() {
    local pg_schema="$1"
    local pg_table="$2"
    local ch_table="$3"
    local required="$4"
    local filter="${5:-id_empresa = $ID_EMPRESA}"

    local pg_count ch_count
    pg_count="$(pg_scalar "SELECT count(*) FROM ${pg_schema}.${pg_table} WHERE ${filter}")"
    pg_count="${pg_count//[[:space:]]/}"
    ch_count="$(ch_exec -q "SELECT count() FROM torqmind_current.${ch_table} FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" 2>/dev/null)"
    ch_count="${ch_count//[[:space:]]/}"

    local ratio=0
    if (( pg_count > 0 )); then
      ratio="$(awk "BEGIN { printf \"%.1f\", ($ch_count / $pg_count) * 100 }")"
    fi

    if (( pg_count > 0 && ch_count == 0 )); then
      if [[ "$required" == "required" ]]; then
        log "  FAIL: $ch_table → PG=$pg_count CH=$ch_count (0%)"
        errors=$((errors + 1))
      else
        log "  WARN: $ch_table → PG=$pg_count CH=$ch_count (0%) [optional]"
      fi
    else
      log "  OK: $ch_table → PG=$pg_count CH=$ch_count ($ratio%)"
    fi
  }

  check_parity stg comprovantes stg_comprovantes required "id_empresa = $ID_EMPRESA AND dt_evento >= '$FROM_DATE'"
  check_parity stg itenscomprovantes stg_itenscomprovantes required "id_empresa = $ID_EMPRESA"
  check_parity stg formas_pgto_comprovantes stg_formas_pgto_comprovantes required "id_empresa = $ID_EMPRESA"
  check_parity stg produtos stg_produtos required "id_empresa = $ID_EMPRESA"
  check_parity stg grupoprodutos stg_grupoprodutos required "id_empresa = $ID_EMPRESA"
  check_parity stg usuarios stg_usuarios optional "id_empresa = $ID_EMPRESA"
  check_parity stg funcionarios stg_funcionarios optional "id_empresa = $ID_EMPRESA"
  check_parity stg turnos stg_turnos optional "id_empresa = $ID_EMPRESA"
  check_parity stg entidades stg_entidades optional "id_empresa = $ID_EMPRESA"
  check_parity stg localvendas stg_localvendas optional "id_empresa = $ID_EMPRESA"
  check_parity stg contaspagar stg_contaspagar optional "id_empresa = $ID_EMPRESA"
  check_parity stg contasreceber stg_contasreceber optional "id_empresa = $ID_EMPRESA"

  if (( errors > 0 )); then
    log "PARITY CHECK FAILED: $errors required table(s) have data in PG but zero in ClickHouse"
    return 1
  fi
  log "Parity check PASSED"
}

# --- Main ---

main() {
  log "=== Bootstrap STG: PostgreSQL → ClickHouse current ==="
  log "  id_empresa=$ID_EMPRESA from_date=$FROM_DATE"
  log "  Using ClickHouse postgresql() table function for direct transfer"
  log "  NOTE: Credentials passed to postgresql() function; SQL is not printed."

  # Core fact tables (required for sales/payments marts)
  bootstrap_comprovantes
  bootstrap_itenscomprovantes
  bootstrap_formas_pgto

  # Dimension/lookup tables
  bootstrap_simple_stg turnos stg_turnos "id_empresa, id_filial, id_turno" optional
  bootstrap_simple_stg usuarios stg_usuarios "id_empresa, id_filial, id_usuario" optional
  bootstrap_simple_stg produtos stg_produtos "id_empresa, id_filial, id_produto" required
  bootstrap_simple_stg grupoprodutos stg_grupoprodutos "id_empresa, id_filial, id_grupoprodutos" required
  bootstrap_simple_stg entidades stg_entidades "id_empresa, id_filial, id_entidade" optional
  bootstrap_simple_stg funcionarios stg_funcionarios "id_empresa, id_filial, id_funcionario" optional
  bootstrap_simple_stg localvendas stg_localvendas "id_empresa, id_filial, id_localvendas" optional

  # Financial tables (4-part key)
  bootstrap_contaspagar
  bootstrap_contasreceber

  # App-level lookup
  bootstrap_payment_type_map
  bootstrap_goals

  # Parity validation
  if (( ! SKIP_PARITY )); then
    validate_parity || exit 1
  else
    log "Parity check SKIPPED (--skip-parity)"
  fi

  log "=== Bootstrap complete ==="

  # Trigger mart backfill from current STG data
  if (( ! SKIP_MART_BACKFILL )); then
    log "Running mart backfill..."
    compose exec -T cdc-consumer python -m torqmind_cdc_consumer.cli backfill-stg \
      --from-date "$FROM_DATE" \
      --id-empresa "$ID_EMPRESA" || {
        log "WARN: Mart backfill via CLI failed; may need manual trigger"
      }
  else
    log "Mart backfill SKIPPED (--skip-mart-backfill)"
  fi

  log "=== Done. Debezium will handle incremental CDC from here. ==="
}

main "$@"
