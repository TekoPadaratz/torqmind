#!/usr/bin/env bash
set -Eeuo pipefail

# Realtime E2E Smoke Test
# Validates the STG-direct realtime pipeline from ingestion through API response.
#
# Prerequisites:
#   - Docker Compose stack running (prod + streaming)
#   - Debezium connector registered and RUNNING
#   - ClickHouse mart_rt tables initialized
#
# What it does:
#   1. Inserts a synthetic test sale into PostgreSQL STG canonical tables
#   2. Waits for the CDC event to appear in torqmind_raw
#   3. Confirms the event in torqmind_current
#   4. Triggers mart builder refresh
#   5. Confirms aggregation in mart_rt
#   6. Calls API endpoint with USE_REALTIME_MARTS=true + FALLBACK=false
#   7. Confirms the test data appears in the API response
#   8. Cleans up the test record

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROD_COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
STREAMING_COMPOSE_FILE="${STREAMING_COMPOSE_FILE:-docker-compose.streaming.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
PROOF_DIR="${PROOF_DIR:-${ROOT_DIR}/tmp}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

CH_USER="${CLICKHOUSE_USER:-torqmind}"
CH_PASS="${CLICKHOUSE_PASSWORD:-}"
PG_USER="${POSTGRES_USER:-torqmind}"
PG_PASS="${POSTGRES_PASSWORD:-torqmind}"
PG_DB="${POSTGRES_DB:-torqmind}"
TEST_ID_EMPRESA="${TEST_ID_EMPRESA:-1}"
TEST_ID_FILIAL="${TEST_ID_FILIAL:-1}"
TEST_RUN_SUFFIX="${TEST_RUN_SUFFIX:-$(date +%s)}"
TEST_DAY="$(printf '%02d' "$(( (TEST_RUN_SUFFIX % 28) + 1 ))")"
# Use an isolated future business date within ClickHouse Date range.
TEST_DATA_KEY="${TEST_DATA_KEY:-209912${TEST_DAY}}"
TEST_DATE_ISO="${TEST_DATE_ISO:-2099-12-${TEST_DAY}T10:30:00Z}"
TEST_TS_SQL="${TEST_TS_SQL:-2099-12-${TEST_DAY} 10:30:00+00}"
TEST_MARKER="${TEST_MARKER:-__E2E_SMOKE_TEST_${TEST_RUN_SUFFIX}__}"
TEST_ID_DB="${TEST_ID_DB:-$((900000000 + (TEST_RUN_SUFFIX % 10000000)))}"
TEST_ID_COMPROVANTE="${TEST_ID_COMPROVANTE:-$TEST_ID_DB}"
TEST_ID_ITEM="${TEST_ID_ITEM:-1}"
TEST_REFERENCIA="${TEST_REFERENCIA:-$((990000000 + (TEST_RUN_SUFFIX % 10000000)))}"
API_FAT_RESULT=0

log() {
  printf '%s [E2E] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

compose_prod() {
  docker compose -f "$PROD_COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

compose_streaming() {
  docker compose -f "$STREAMING_COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

pg_exec() {
  compose_prod exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"
}

ch_query() {
  compose_prod exec -T clickhouse clickhouse-client \
    --user "$CH_USER" --password "$CH_PASS" \
    --format=TabSeparated -q "$1" 2>/dev/null || echo "__ERROR__"
}

ch_count() {
  local result
  result="$(ch_query "$1")"
  result="${result//[[:space:]]/}"
  printf '%s' "$result"
}

ch_exec() {
  compose_prod exec -T clickhouse clickhouse-client \
    --user "$CH_USER" --password "$CH_PASS" \
    --multiquery -q "$1" >/dev/null
}

run_mart_builder_refresh() {
  compose_streaming exec -T cdc-consumer python -c "
from torqmind_cdc_consumer.mart_builder import MartBuilder
from torqmind_cdc_consumer.config import settings
builder = MartBuilder(
    clickhouse_host=settings.clickhouse_host,
    clickhouse_port=settings.clickhouse_port,
    clickhouse_user=settings.clickhouse_user,
    clickhouse_password=settings.clickhouse_password,
    source='stg',
)
builder.state.mark($TEST_ID_EMPRESA, $TEST_ID_FILIAL, $TEST_DATA_KEY, 'comprovantes')
builder.state.mark($TEST_ID_EMPRESA, $TEST_ID_FILIAL, $TEST_DATA_KEY, 'itenscomprovantes')
builder.state.mark($TEST_ID_EMPRESA, $TEST_ID_FILIAL, $TEST_DATA_KEY, 'formas_pgto_comprovantes')
results = builder.refresh_if_needed()
for r in results:
    print(f'  {r.mart_name}: rows={r.rows_written} ms={r.duration_ms} err={r.error}')
errors = [r for r in results if r.error]
assert not errors, f'Mart builder returned errors: {errors!r}'
print('MART_BUILD_OK')
"
}

wait_for_current_cleanup() {
  local max_wait=60
  local interval=3
  local elapsed=0
  local comp_count=""
  local item_count=""
  local pgto_count=""
  local comp_slim_count=""
  local item_slim_count=""
  local pgto_slim_count=""

  while (( elapsed < max_wait )); do
    comp_count="$(ch_count "SELECT count() FROM torqmind_current.stg_comprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
    item_count="$(ch_count "SELECT count() FROM torqmind_current.stg_itenscomprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
    pgto_count="$(ch_count "SELECT count() FROM torqmind_current.stg_formas_pgto_comprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_referencia=$TEST_REFERENCIA AND is_deleted=0")"
    comp_slim_count="$(ch_count "SELECT count() FROM torqmind_current.stg_comprovantes_slim WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
    item_slim_count="$(ch_count "SELECT count() FROM torqmind_current.stg_itenscomprovantes_slim WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND id_itemcomprovante=$TEST_ID_ITEM AND is_deleted=0")"
    pgto_slim_count="$(ch_count "SELECT count() FROM torqmind_current.stg_formas_pgto_slim WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_referencia=$TEST_REFERENCIA AND is_deleted=0")"

    if [[ "$comp_count" == "0" && "$item_count" == "0" && "$pgto_count" == "0" && "$comp_slim_count" == "0" && "$item_slim_count" == "0" && "$pgto_slim_count" == "0" ]]; then
      return 0
    fi

    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  log "WARN: current/slim cleanup did not converge after ${max_wait}s (comprovantes=$comp_count itens=$item_count pagamentos=$pgto_count comprovantes_slim=$comp_slim_count itens_slim=$item_slim_count formas_slim=$pgto_slim_count)"
  return 1
}

wait_for_mart_cleanup() {
  local max_wait=60
  local interval=3
  local elapsed=0
  local sales_daily_count=""
  local sales_hourly_count=""
  local sales_products_count=""
  local sales_groups_count=""
  local payments_count=""
  local dashboard_count=""
  local fraud_count=""
  local risk_count=""

  while (( elapsed < max_wait )); do
    sales_daily_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    sales_hourly_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    sales_products_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    sales_groups_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.sales_groups_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    payments_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    dashboard_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.dashboard_home_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    fraud_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
    risk_count="$(ch_count "SELECT count() FROM torqmind_mart_rt.risk_recent_events_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"

    if [[ "$sales_daily_count" == "0" && "$sales_hourly_count" == "0" && "$sales_products_count" == "0" && "$sales_groups_count" == "0" && "$payments_count" == "0" && "$dashboard_count" == "0" && "$fraud_count" == "0" && "$risk_count" == "0" ]]; then
      return 0
    fi

    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  log "WARN: mart cleanup did not converge after ${max_wait}s (sales_daily_rt=$sales_daily_count sales_hourly_rt=$sales_hourly_count sales_products_rt=$sales_products_count sales_groups_rt=$sales_groups_count payments_by_type_rt=$payments_count dashboard_home_rt=$dashboard_count fraud_daily_rt=$fraud_count risk_recent_events_rt=$risk_count)"
  return 1
}

force_clickhouse_cleanup() {
  ch_exec "
    ALTER TABLE torqmind_current.stg_comprovantes DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND id_db = $TEST_ID_DB AND id_comprovante = $TEST_ID_COMPROVANTE SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_current.stg_itenscomprovantes DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND id_db = $TEST_ID_DB AND id_comprovante = $TEST_ID_COMPROVANTE SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_current.stg_formas_pgto_comprovantes DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND id_referencia = $TEST_REFERENCIA SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_current.stg_comprovantes_slim DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND id_db = $TEST_ID_DB AND id_comprovante = $TEST_ID_COMPROVANTE SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_current.stg_itenscomprovantes_slim DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND id_db = $TEST_ID_DB AND id_comprovante = $TEST_ID_COMPROVANTE AND id_itemcomprovante = $TEST_ID_ITEM SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_current.stg_formas_pgto_slim DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND id_referencia = $TEST_REFERENCIA SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.sales_daily_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.sales_hourly_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.sales_products_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.sales_groups_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.payments_by_type_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.dashboard_home_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.fraud_daily_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
    ALTER TABLE torqmind_mart_rt.risk_recent_events_rt DELETE WHERE id_empresa = $TEST_ID_EMPRESA AND id_filial = $TEST_ID_FILIAL AND data_key = $TEST_DATA_KEY SETTINGS mutations_sync = 1;
  "
}

cleanup() {
  log "Cleaning up test data (data_key=$TEST_DATA_KEY)..."
  pg_exec "DELETE FROM stg.formas_pgto_comprovantes WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_referencia=$TEST_REFERENCIA;" 2>/dev/null || true
  pg_exec "DELETE FROM stg.itenscomprovantes WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE;" 2>/dev/null || true
  pg_exec "DELETE FROM stg.comprovantes WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE;" 2>/dev/null || true

  if ! wait_for_current_cleanup; then
    log "Applying direct ClickHouse cleanup fallback for synthetic key $TEST_DATA_KEY"
    force_clickhouse_cleanup
  fi

  run_mart_builder_refresh

  if ! wait_for_mart_cleanup; then
    log "Reapplying direct ClickHouse cleanup fallback for mart residue on $TEST_DATA_KEY"
    force_clickhouse_cleanup
    wait_for_mart_cleanup
  fi

  log "Cleanup done."
}

# Ensure cleanup runs on exit
trap cleanup EXIT

step_check_prerequisites() {
  log "=== Step 0: Prerequisites ==="

  # Check containers are running
  compose_prod ps --status=running | grep -q "api" || { log "ERROR: API container not running"; exit 1; }
  compose_prod ps --status=running | grep -q "clickhouse" || { log "ERROR: ClickHouse not running"; exit 1; }
  compose_prod ps --status=running | grep -q "postgres" || { log "ERROR: PostgreSQL not running"; exit 1; }
  compose_streaming ps --status=running | grep -q "cdc-consumer" || { log "ERROR: CDC Consumer not running"; exit 1; }

  # Check Debezium connector
  local dbz_status
  dbz_status="$(compose_streaming exec -T debezium-connect \
    curl -sf http://localhost:8083/connectors/torqmind-postgres-cdc/status 2>/dev/null \
    | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || echo "UNKNOWN")"
  if [[ "$dbz_status" != "RUNNING" ]]; then
    log "ERROR: Debezium connector not RUNNING (status=$dbz_status)"
    exit 1
  fi

  local table_count
  table_count="$(ch_query "SELECT count() FROM system.tables WHERE database='torqmind_mart_rt' AND name IN ('dashboard_home_rt','sales_daily_rt','sales_hourly_rt','sales_products_rt','sales_groups_rt','payments_by_type_rt','cash_overview_rt','fraud_daily_rt','risk_recent_events_rt','finance_overview_rt','source_freshness','mart_publication_log')")"
  table_count="${table_count//[[:space:]]/}"
  if (( table_count != 12 )); then
    log "ERROR: mart_rt tables not initialized (found $table_count mandatory tables, need 12)"
    exit 1
  fi

  log "Prerequisites OK (debezium=RUNNING, mandatory_mart_rt_tables=$table_count)"
}

step_insert_test_sale() {
  log "=== Step 1: Insert synthetic test sale ==="

  # Insert into canonical STG tables. No STG->DW ETL is called by this smoke.
  pg_exec "
    INSERT INTO stg.comprovantes (
      id_empresa, id_filial, id_db, id_comprovante, payload, dt_evento,
      referencia_shadow, id_usuario_shadow, id_turno_shadow, valor_total_shadow,
      cancelado_shadow, situacao_shadow, received_at
    )
    VALUES (
      $TEST_ID_EMPRESA, $TEST_ID_FILIAL, $TEST_ID_DB, $TEST_ID_COMPROVANTE,
      jsonb_build_object(
        'TORQMIND_MARKER', '$TEST_MARKER',
        'ID_FILIAL', $TEST_ID_FILIAL,
        'ID_DB', $TEST_ID_DB,
        'ID_COMPROVANTE', $TEST_ID_COMPROVANTE,
        'REFERENCIA', $TEST_REFERENCIA,
        'ID_USUARIOS', 1,
        'ID_TURNOS', 1,
        'VLRTOTAL', '42.50',
        'SITUACAO', 3,
        'CANCELADO', false,
        'DATA', '$TEST_DATE_ISO'
      ),
      TIMESTAMPTZ '$TEST_TS_SQL',
      $TEST_REFERENCIA, 1, 1, 42.50, false, 3, now()
    )
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
    DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento,
      referencia_shadow = EXCLUDED.referencia_shadow,
      valor_total_shadow = EXCLUDED.valor_total_shadow,
      cancelado_shadow = EXCLUDED.cancelado_shadow,
      situacao_shadow = EXCLUDED.situacao_shadow,
      received_at = now();
  "

  pg_exec "
    INSERT INTO stg.itenscomprovantes (
      id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante, payload,
      dt_evento, id_produto_shadow, id_grupo_produto_shadow, id_local_venda_shadow,
      id_funcionario_shadow, cfop_shadow, qtd_shadow, valor_unitario_shadow,
      total_shadow, desconto_shadow, custo_unitario_shadow, received_at
    )
    VALUES (
      $TEST_ID_EMPRESA, $TEST_ID_FILIAL, $TEST_ID_DB, $TEST_ID_COMPROVANTE, $TEST_ID_ITEM,
      jsonb_build_object(
        'TORQMIND_MARKER', '$TEST_MARKER',
        'ID_PRODUTOS', 1,
        'ID_GRUPOPRODUTOS', 1,
        'CFOP', 5102,
        'QTDE', '1',
        'VLRUNITARIO', '42.50',
        'TOTAL', '42.50',
        'VLRDESCONTO', '0'
      ),
      TIMESTAMPTZ '$TEST_TS_SQL',
      1, 1, 1, 1, 5102, 1.000, 42.500000, 42.50, 0.00, 20.000000, now()
    )
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
    DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento,
      cfop_shadow = EXCLUDED.cfop_shadow, qtd_shadow = EXCLUDED.qtd_shadow,
      total_shadow = EXCLUDED.total_shadow, received_at = now();
  "

  pg_exec "
    INSERT INTO stg.formas_pgto_comprovantes (
      id_empresa, id_filial, id_referencia, tipo_forma, id_db_shadow, dt_evento,
      valor_shadow, payload, received_at
    )
    VALUES (
      $TEST_ID_EMPRESA, $TEST_ID_FILIAL, $TEST_REFERENCIA, 0, $TEST_ID_DB,
      TIMESTAMPTZ '$TEST_TS_SQL',
      42.50,
      jsonb_build_object('TORQMIND_MARKER', '$TEST_MARKER', 'VALOR', '42.50', 'TIPO_FORMA', 0),
      now()
    )
    ON CONFLICT (id_empresa, id_filial, id_referencia, tipo_forma)
    DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento,
      valor_shadow = EXCLUDED.valor_shadow, received_at = now();
  "

  log "STG test sale inserted (data_key=$TEST_DATA_KEY, total=42.50)"
}

step_wait_raw_event() {
  log "=== Step 2: Wait for CDC event in torqmind_raw ==="

  local max_wait=60
  local elapsed=0
  local interval=3

  while (( elapsed < max_wait )); do
    local raw_count
    raw_count="$(ch_query "SELECT count() FROM torqmind_raw.cdc_events WHERE table_schema='stg' AND table_name='comprovantes' AND JSONExtractInt(key_json, 'id_db')=$TEST_ID_DB AND JSONExtractInt(key_json, 'id_comprovante')=$TEST_ID_COMPROVANTE AND (position(after_json, '$TEST_MARKER') > 0 OR position(before_json, '$TEST_MARKER') > 0)")"
    raw_count="${raw_count//[[:space:]]/}"
    if (( raw_count > 0 )); then
      log "Raw event found after ${elapsed}s"
      return 0
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  log "ERROR: No raw event found after ${max_wait}s"
  exit 1
}

step_wait_current() {
  log "=== Step 3: Confirm in torqmind_current ==="

  local max_wait=60
  local elapsed=0
  local interval=3

  # Wait for comprovantes
  while (( elapsed < max_wait )); do
    local current_count
    current_count="$(ch_query "SELECT count() FROM torqmind_current.stg_comprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
    current_count="${current_count//[[:space:]]/}"
    if (( current_count > 0 )); then
      log "Current stg_comprovantes confirmed (count=$current_count) after ${elapsed}s"
      break
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  if (( elapsed >= max_wait )); then
    log "ERROR: stg_comprovantes not found in torqmind_current after ${max_wait}s"
    exit 1
  fi

  # Also wait for itenscomprovantes (needed for INNER JOIN in mart builder)
  while (( elapsed < max_wait )); do
    local itens_count
    itens_count="$(ch_query "SELECT count() FROM torqmind_current.stg_itenscomprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
    itens_count="${itens_count//[[:space:]]/}"
    if (( itens_count > 0 )); then
      log "Current stg_itenscomprovantes confirmed (count=$itens_count) after ${elapsed}s"
      return 0
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  log "ERROR: stg_itenscomprovantes not found in torqmind_current after ${max_wait}s"
  exit 1
}

step_trigger_mart_builder() {
  log "=== Step 4: Trigger mart builder for test data_key ==="

  # Call the mart builder backfill for just our test key
  run_mart_builder_refresh

  log "Mart builder triggered"
}

step_verify_mart_rt() {
  log "=== Step 5: Verify mart_rt has test data ==="

  local count
  count="$(ch_query "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
  count="${count//[[:space:]]/}"
  if (( count == 0 )); then
    log "ERROR: No data in sales_daily_rt for test data_key"
    exit 1
  fi

  local fat
  fat="$(ch_query "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
  fat="${fat//[[:space:]]/}"
  log "mart_rt verified: sales_daily_rt rows=$count faturamento=$fat"

  # Verify it's approximately 42.50
  local expected="42.5"
  local match
  match="$(awk "BEGIN { diff = ($fat - $expected); print (diff < 0 ? -diff : diff) < 1 ? 1 : 0 }")"
  if [[ "$match" != "1" ]]; then
    log "ERROR: Expected faturamento ~42.50, got $fat"
    exit 1
  fi
}

step_verify_api() {
  log "=== Step 6: Verify API serves realtime data ==="

  local api_result
  api_result="$(compose_prod exec -T api python -c "
import os
os.environ['USE_REALTIME_MARTS'] = 'true'
os.environ['REALTIME_MARTS_SOURCE'] = 'stg'
os.environ['REALTIME_MARTS_FALLBACK'] = 'false'
from datetime import datetime
from app.config import settings
assert settings.use_realtime_marts is True
assert settings.realtime_marts_source == 'stg'
assert settings.realtime_marts_fallback is False
from app import repos_analytics
dt = datetime.strptime('$TEST_DATA_KEY', '%Y%m%d').date()
result = getattr(repos_analytics, 'dashboard_kpis')('admin', $TEST_ID_EMPRESA, None, dt.replace(day=1), dt)
fat = float(result.get('faturamento', 0))
print(f'faturamento={fat}')
if fat >= 42:
    print('API_OK')
else:
    print(f'API_FAIL: expected >=42, got {fat}')
" 2>&1 || echo "API_ERROR")"

  if [[ "$api_result" == *"API_OK"* ]]; then
    log "API smoke PASSED: $api_result"
    # Extract faturamento for proof JSON
    API_FAT_RESULT="$(echo "$api_result" | grep -oP 'faturamento=\K[0-9.]+' | head -1 || echo "0")"
  else
    log "ERROR: API smoke FAILED with fallback=false: $api_result"
    exit 1
  fi
}

step_report() {
  log "============================================"
  log "  E2E SMOKE TEST COMPLETE"
  log "============================================"
  log "Pipeline validated:"
  log "  PostgreSQL STG INSERT -> Debezium CDC -> Redpanda -> CDC Consumer -> ClickHouse current -> MartBuilder -> mart_rt -> API facade"
  log "  Current origin: STG direct. No STG->DW ETL was invoked by this smoke."
  log ""
  log "Test parameters:"
  log "  id_empresa=$TEST_ID_EMPRESA"
  log "  id_filial=$TEST_ID_FILIAL"
  log "  data_key=$TEST_DATA_KEY (synthetic future date)"
  log "  test_value=42.50"
  log "============================================"
}

step_generate_proof() {
  mkdir -p "$PROOF_DIR"
  local proof_file="${PROOF_DIR}/realtime-proof-$(date +%Y%m%d_%H%M%S).json"

  # Collect evidence
  local commit_hash
  commit_hash="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")"

  local raw_count
  raw_count="$(ch_query "SELECT count() FROM torqmind_raw.cdc_events WHERE table_schema='stg' AND table_name='comprovantes' AND JSONExtractInt(key_json, 'id_db')=$TEST_ID_DB AND JSONExtractInt(key_json, 'id_comprovante')=$TEST_ID_COMPROVANTE AND (position(after_json, '$TEST_MARKER') > 0 OR position(before_json, '$TEST_MARKER') > 0)")"
  raw_count="${raw_count//[[:space:]]/}"

  local current_comp_count
  current_comp_count="$(ch_query "SELECT count() FROM torqmind_current.stg_comprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
  current_comp_count="${current_comp_count//[[:space:]]/}"

  local current_itens_count
  current_itens_count="$(ch_query "SELECT count() FROM torqmind_current.stg_itenscomprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_db=$TEST_ID_DB AND id_comprovante=$TEST_ID_COMPROVANTE AND is_deleted=0")"
  current_itens_count="${current_itens_count//[[:space:]]/}"

  local current_pgto_count
  current_pgto_count="$(ch_query "SELECT count() FROM torqmind_current.stg_formas_pgto_comprovantes FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND id_filial=$TEST_ID_FILIAL AND id_referencia=$TEST_REFERENCIA AND is_deleted=0")"
  current_pgto_count="${current_pgto_count//[[:space:]]/}"

  local sales_daily_count
  sales_daily_count="$(ch_query "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
  sales_daily_count="${sales_daily_count//[[:space:]]/}"

  local sales_daily_fat
  sales_daily_fat="$(ch_query "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$TEST_ID_EMPRESA AND data_key=$TEST_DATA_KEY")"
  sales_daily_fat="${sales_daily_fat//[[:space:]]/}"

  local topics
  topics="torqmind.stg.comprovantes,torqmind.stg.itenscomprovantes,torqmind.stg.formas_pgto_comprovantes"

  cat > "$proof_file" <<JSON
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "commit": "$commit_hash",
  "source": "stg",
  "topics": "$topics",
  "synthetic_id": $TEST_ID_COMPROVANTE,
  "data_key": $TEST_DATA_KEY,
  "raw_count": ${raw_count:-0},
  "current_comprovantes_count": ${current_comp_count:-0},
  "current_itens_count": ${current_itens_count:-0},
  "current_pagamentos_count": ${current_pgto_count:-0},
  "sales_daily_rt_count": ${sales_daily_count:-0},
  "sales_daily_rt_faturamento": ${sales_daily_fat:-0},
  "api_response_faturamento": ${API_FAT_RESULT:-0},
  "fallback": false,
  "etl_invoked": false,
  "result": "PASS"
}
JSON

  log "Proof JSON written to: $proof_file"
  cat "$proof_file"
}

main() {
  step_check_prerequisites
  step_insert_test_sale
  step_wait_raw_event
  step_wait_current
  step_trigger_mart_builder
  step_verify_mart_rt
  step_verify_api
  step_report
  step_generate_proof
}

main "$@"
