#!/usr/bin/env bash
set -Eeuo pipefail

# Blocking realtime cutover validation.
# Fails on ClickHouse/API connection errors, missing mart_rt tables, empty realtime
# marts when source/legacy has data, or metric divergence above tolerance.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
ID_EMPRESA="${ID_EMPRESA:-1}"
DECIMAL_TOLERANCE="${DECIMAL_TOLERANCE:-${TOLERANCE:-0.001}}"
COUNT_TOLERANCE="${COUNT_TOLERANCE:-0.001}"  # 0.1% tolerance for integer counts (TZ boundary drift)
SOURCE="${SOURCE:-${REALTIME_MARTS_SOURCE:-stg}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      [[ $# -ge 2 ]] || { echo "ERROR: --source requires stg or dw" >&2; exit 2; }
      SOURCE="$2"; shift ;;
    --id-empresa)
      [[ $# -ge 2 ]] || { echo "ERROR: --id-empresa requires a value" >&2; exit 2; }
      ID_EMPRESA="$2"; shift ;;
    --help|-h)
      echo "Usage: ENV_FILE=/etc/torqmind/prod.env $0 [--source stg|dw] [--id-empresa 1]" >&2
      exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

SOURCE="$(printf '%s' "$SOURCE" | tr '[:upper:]' '[:lower:]')"
if [[ "$SOURCE" != "stg" && "$SOURCE" != "dw" ]]; then
  echo "ERROR: SOURCE must be stg or dw (got '$SOURCE')" >&2
  exit 2
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: ENV_FILE=$ENV_FILE not found" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${CLICKHOUSE_USER:=torqmind}"
: "${CLICKHOUSE_PASSWORD:=}"
: "${POSTGRES_USER:=${PG_USER:-postgres}}"
: "${POSTGRES_DB:=${PG_DATABASE:-torqmind}}"

FAILURES=0
CHECKS=0

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

compose_prod() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

ch_query() {
  local sql="$1"
  local out
  if ! out="$(compose_prod exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --format=TabSeparated -q "$sql" 2>&1)"; then
    printf '__ERROR__:%s' "$out"
    return 0
  fi
  printf '%s' "$out"
}

pg_query() {
  local sql="$1"
  local out
  if ! out="$(compose_prod exec -T postgres psql \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -tAc "$sql" 2>&1)"; then
    printf '__ERROR__:%s' "$out"
    return 0
  fi
  printf '%s' "$out"
}

normalize_number() {
  local value="$1"
  value="${value//[[:space:]]/}"
  case "$value" in
    ""|"\\N"|"NULL"|"nan"|"NaN") value="0" ;;
  esac
  printf '%s' "$value"
}

record_failure() {
  local label="$1"
  local detail="$2"
  printf '  %-48s %s\n' "$label" "$detail"
  FAILURES=$((FAILURES + 1))
}

require_clickhouse_connection() {
  CHECKS=$((CHECKS + 1))
  local result
  result="$(ch_query "SELECT 1")"
  if [[ "$(normalize_number "$result")" != "1" ]]; then
    record_failure "clickhouse.connection" "FAILED"
    return 1
  fi
  printf '  %-48s OK\n' "clickhouse.connection"
}

table_exists() {
  local full_table="$1"
  local db table result
  db="${full_table%%.*}"
  table="${full_table#*.}"
  result="$(ch_query "SELECT count() FROM system.tables WHERE database='$db' AND name='$table'")"
  [[ "$(normalize_number "$result")" == "1" ]]
}

validate_table_exists() {
  local rt_table="$1"
  CHECKS=$((CHECKS + 1))
  if ! table_exists "$rt_table"; then
    record_failure "$rt_table" "TABLE_MISSING"
    return 1
  fi
  printf '  %-48s OK\n' "$rt_table"
}

compare_metric() {
  local label="$1"
  local legacy_sql="$2"
  local rt_sql="$3"
  local mode="${4:-decimal}"

  CHECKS=$((CHECKS + 1))

  local legacy_val rt_val
  legacy_val="$(ch_query "$legacy_sql")"
  rt_val="$(ch_query "$rt_sql")"

  if [[ "$legacy_val" == __ERROR__* ]]; then
    record_failure "$label" "LEGACY_QUERY_FAILED"
    return
  fi
  if [[ "$rt_val" == __ERROR__* ]]; then
    record_failure "$label" "RT_QUERY_FAILED"
    return
  fi

  legacy_val="$(normalize_number "$legacy_val")"
  rt_val="$(normalize_number "$rt_val")"

  local status="OK"
  if [[ "$mode" == "count" ]]; then
    if [[ "$legacy_val" != "$rt_val" ]]; then
      status="DIVERGENT"
      if [[ "$legacy_val" != "0" && "$rt_val" == "0" ]]; then
        status="RT_EMPTY"
      fi
      FAILURES=$((FAILURES + 1))
    fi
  else
    local diff over_tolerance legacy_positive rt_zero
    diff="$(awk "BEGIN { l=$legacy_val+0; r=$rt_val+0; base=(l<0?-l:l); if(base<0.01) base=0.01; print ((l-r)<0 ? (r-l) : (l-r)) / base }")"
    over_tolerance="$(awk "BEGIN { print ($diff > $DECIMAL_TOLERANCE) ? 1 : 0 }")"
    legacy_positive="$(awk "BEGIN { print ($legacy_val+0 > 0) ? 1 : 0 }")"
    rt_zero="$(awk "BEGIN { print ($rt_val+0 == 0) ? 1 : 0 }")"
    if [[ "$legacy_positive" == "1" && "$rt_zero" == "1" ]]; then
      status="RT_EMPTY"
      FAILURES=$((FAILURES + 1))
    elif [[ "$over_tolerance" == "1" ]]; then
      status="DIVERGENT(delta=${diff})"
      FAILURES=$((FAILURES + 1))
    fi
  fi

  printf '  %-48s legacy=%-14s rt=%-14s [%s]\n' "$label" "$legacy_val" "$rt_val" "$status"
}

compare_pg_ch_metric() {
  local label="$1"
  local pg_sql="$2"
  local rt_sql="$3"
  local mode="${4:-decimal}"

  CHECKS=$((CHECKS + 1))

  local source_val rt_val
  source_val="$(pg_query "$pg_sql")"
  rt_val="$(ch_query "$rt_sql")"

  if [[ "$source_val" == __ERROR__* ]]; then
    record_failure "$label" "STG_QUERY_FAILED"
    return
  fi
  if [[ "$rt_val" == __ERROR__* ]]; then
    record_failure "$label" "RT_QUERY_FAILED"
    return
  fi

  source_val="$(normalize_number "$source_val")"
  rt_val="$(normalize_number "$rt_val")"

  local status="OK"
  if [[ "$mode" == "count" ]]; then
    if [[ "$source_val" != "$rt_val" ]]; then
      # Allow small tolerance for counts (timezone boundary drift)
      local count_diff_ok
      count_diff_ok="$(awk "BEGIN { s=$source_val+0; r=$rt_val+0; base=(s<1?1:s); diff=((s-r)<0?(r-s):(s-r)); print (diff/base <= $COUNT_TOLERANCE) ? 1 : 0 }")"
      if [[ "$source_val" != "0" && "$rt_val" == "0" ]]; then
        status="RT_EMPTY"
        FAILURES=$((FAILURES + 1))
      elif [[ "$count_diff_ok" == "0" ]]; then
        status="DIVERGENT"
        FAILURES=$((FAILURES + 1))
      else
        status="OK"  # within count tolerance
      fi
    fi
  else
    local diff over_tolerance source_positive rt_zero
    diff="$(awk "BEGIN { l=$source_val+0; r=$rt_val+0; base=(l<0?-l:l); if(base<0.01) base=0.01; print ((l-r)<0 ? (r-l) : (l-r)) / base }")"
    over_tolerance="$(awk "BEGIN { print ($diff > $DECIMAL_TOLERANCE) ? 1 : 0 }")"
    source_positive="$(awk "BEGIN { print ($source_val+0 > 0) ? 1 : 0 }")"
    rt_zero="$(awk "BEGIN { print ($rt_val+0 == 0) ? 1 : 0 }")"
    if [[ "$source_positive" == "1" && "$rt_zero" == "1" ]]; then
      status="RT_EMPTY"
      FAILURES=$((FAILURES + 1))
    elif [[ "$over_tolerance" == "1" ]]; then
      status="DIVERGENT(delta=${diff})"
      FAILURES=$((FAILURES + 1))
    fi
  fi

  printf '  %-48s stg=%-17s rt=%-14s [%s]\n' "$label" "$source_val" "$rt_val" "$status"
}

compare_grouped_sum() {
  local label="$1"
  local legacy_sql="$2"
  local rt_sql="$3"

  CHECKS=$((CHECKS + 1))

  local divergence_sql result
  divergence_sql="
    SELECT count()
    FROM (
      SELECT
        coalesce(l.k, r.k) AS k,
        toFloat64(coalesce(l.v, 0)) AS legacy_value,
        toFloat64(coalesce(r.v, 0)) AS rt_value
      FROM ($legacy_sql) AS l
      FULL OUTER JOIN ($rt_sql) AS r ON l.k = r.k
    )
    WHERE if(
      legacy_value = 0 AND rt_value = 0,
      0,
      abs(legacy_value - rt_value) / greatest(abs(legacy_value), 0.01)
    ) > $DECIMAL_TOLERANCE
  "
  result="$(ch_query "$divergence_sql")"
  if [[ "$result" == __ERROR__* ]]; then
    record_failure "$label" "GROUP_QUERY_FAILED"
    return
  fi
  result="$(normalize_number "$result")"
  if (( result > 0 )); then
    record_failure "$label" "DIVERGENT_GROUPS=$result"
    return
  fi
  printf '  %-48s OK\n' "$label"
}

stg_diagnostics() {
  # Diagnostic breakdown when STG divergences are detected
  log ""
  log "=== DIAGNOSTIC BREAKDOWN ==="
  log ""
  log "Slim dedup check:"
  ch_query "SELECT
    'comprovantes_slim' AS tbl,
    count() AS raw_rows,
    uniqExact(id_empresa, id_filial, id_db, id_comprovante) AS unique_keys,
    count() - uniqExact(id_empresa, id_filial, id_db, id_comprovante) AS duplicates
  FROM torqmind_current.stg_comprovantes_slim
  UNION ALL
  SELECT 'itens_slim', count(),
    uniqExact(id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante),
    count() - uniqExact(id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
  FROM torqmind_current.stg_itenscomprovantes_slim
  UNION ALL
  SELECT 'formas_slim', count(),
    uniqExact(id_empresa, id_filial, id_referencia, tipo_forma),
    count() - uniqExact(id_empresa, id_filial, id_referencia, tipo_forma)
  FROM torqmind_current.stg_formas_pgto_slim
  FORMAT PrettyCompact" || true
  log ""
  log "Top 20 data_key by faturamento divergence (STG slim vs RT + payments sanity):"
  ch_query "
  WITH stg AS (
    SELECT c.data_key,
      sum(i.total) AS stg_faturamento
    FROM torqmind_current.stg_comprovantes_slim AS c
    INNER JOIN torqmind_current.stg_itenscomprovantes_slim AS i
      ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante
    WHERE c.id_empresa=$ID_EMPRESA AND c.is_deleted=0 AND i.is_deleted=0 AND c.cancelado=0 AND i.cfop > 5000
    GROUP BY c.data_key
  ),
  rt AS (
    SELECT data_key, sum(faturamento) AS rt_faturamento
    FROM torqmind_mart_rt.sales_daily_rt FINAL
    WHERE id_empresa=$ID_EMPRESA
    GROUP BY data_key
  ),
  pay AS (
    SELECT data_key, sum(valor_total) AS payments_total
    FROM torqmind_mart_rt.payments_by_type_rt FINAL
    WHERE id_empresa=$ID_EMPRESA
    GROUP BY data_key
  )
  SELECT
    coalesce(stg.data_key, rt.data_key, pay.data_key) AS data_key,
    coalesce(stg.stg_faturamento, 0) AS stg_faturamento,
    coalesce(rt.rt_faturamento, 0) AS rt_faturamento,
    coalesce(pay.payments_total, 0) AS payments_total,
    coalesce(rt.rt_faturamento, 0) - coalesce(stg.stg_faturamento, 0) AS delta_rt_stg
  FROM stg
  FULL OUTER JOIN rt ON stg.data_key = rt.data_key
  FULL OUTER JOIN pay ON coalesce(stg.data_key, rt.data_key) = pay.data_key
  ORDER BY abs(toFloat64(delta_rt_stg)) DESC, data_key DESC
  LIMIT 20
  SETTINGS max_memory_usage=3000000000, max_threads=2, join_algorithm='partial_merge'
  FORMAT PrettyCompact" || true
  log ""
  log "Top 20 filiais by faturamento divergence (STG slim vs RT + payments sanity):"
  ch_query "
  WITH stg AS (
    SELECT c.id_filial,
      sum(i.total) AS stg_faturamento
    FROM torqmind_current.stg_comprovantes_slim AS c
    INNER JOIN torqmind_current.stg_itenscomprovantes_slim AS i
      ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante
    WHERE c.id_empresa=$ID_EMPRESA AND c.is_deleted=0 AND i.is_deleted=0 AND c.cancelado=0 AND i.cfop > 5000
    GROUP BY c.id_filial
  ),
  rt AS (
    SELECT id_filial, sum(faturamento) AS rt_faturamento
    FROM torqmind_mart_rt.sales_daily_rt FINAL
    WHERE id_empresa=$ID_EMPRESA
    GROUP BY id_filial
  ),
  pay AS (
    SELECT id_filial, sum(valor_total) AS payments_total
    FROM torqmind_mart_rt.payments_by_type_rt FINAL
    WHERE id_empresa=$ID_EMPRESA
    GROUP BY id_filial
  )
  SELECT
    coalesce(stg.id_filial, rt.id_filial, pay.id_filial) AS id_filial,
    coalesce(stg.stg_faturamento, 0) AS stg_faturamento,
    coalesce(rt.rt_faturamento, 0) AS rt_faturamento,
    coalesce(pay.payments_total, 0) AS payments_total,
    coalesce(rt.rt_faturamento, 0) - coalesce(stg.stg_faturamento, 0) AS delta_rt_stg
  FROM stg
  FULL OUTER JOIN rt ON stg.id_filial = rt.id_filial
  FULL OUTER JOIN pay ON coalesce(stg.id_filial, rt.id_filial) = pay.id_filial
  ORDER BY abs(toFloat64(delta_rt_stg)) DESC, id_filial
  LIMIT 20
  SETTINGS max_memory_usage=3000000000, max_threads=2, join_algorithm='partial_merge'
  FORMAT PrettyCompact" || true
  log ""
  log "Top 50 comprovantes by item/header/payment divergence:"
  ch_query "
  WITH item_docs AS (
    SELECT id_empresa, id_filial, id_db, id_comprovante,
      sumIf(total, is_deleted = 0 AND cfop > 5000) AS item_total,
      countIf(is_deleted = 0 AND cfop > 5000) AS item_rows
    FROM torqmind_current.stg_itenscomprovantes_slim
    WHERE id_empresa=$ID_EMPRESA
    GROUP BY id_empresa, id_filial, id_db, id_comprovante
  ),
  payment_docs AS (
    SELECT c.id_empresa, c.id_filial, c.id_db, c.id_comprovante,
      sum(p.valor) AS payment_total
    FROM torqmind_current.stg_comprovantes_slim AS c
    INNER JOIN torqmind_current.stg_formas_pgto_slim AS p
      ON p.id_empresa=c.id_empresa AND p.id_filial=c.id_filial AND p.id_referencia=c.referencia
    WHERE c.id_empresa=$ID_EMPRESA AND c.is_deleted=0 AND p.is_deleted=0
    GROUP BY c.id_empresa, c.id_filial, c.id_db, c.id_comprovante
  )
  SELECT
    c.data_key,
    c.id_filial,
    c.id_db,
    c.id_comprovante,
    c.valor_total AS header_total,
    coalesce(i.item_total, 0) AS item_total,
    coalesce(p.payment_total, 0) AS payment_total,
    coalesce(i.item_rows, 0) AS item_rows,
    coalesce(i.item_total, 0) - c.valor_total AS item_minus_header,
    coalesce(p.payment_total, 0) - c.valor_total AS payment_minus_header
  FROM torqmind_current.stg_comprovantes_slim AS c
  LEFT JOIN item_docs AS i
    ON i.id_empresa=c.id_empresa AND i.id_filial=c.id_filial AND i.id_db=c.id_db AND i.id_comprovante=c.id_comprovante
  LEFT JOIN payment_docs AS p
    ON p.id_empresa=c.id_empresa AND p.id_filial=c.id_filial AND p.id_db=c.id_db AND p.id_comprovante=c.id_comprovante
  WHERE c.id_empresa=$ID_EMPRESA AND c.is_deleted=0 AND c.cancelado=0
  ORDER BY greatest(abs(toFloat64(item_minus_header)), abs(toFloat64(payment_minus_header))) DESC
  LIMIT 50
  SETTINGS max_memory_usage=3000000000, max_threads=2, join_algorithm='partial_merge'
  FORMAT PrettyCompact" || true
  log ""
  log "Item payload value-field sample:"
  ch_query "
  SELECT
    id_filial, id_db, id_comprovante, id_itemcomprovante,
    JSONExtractRaw(payload, 'VLRTOTALITEM') AS VLRTOTALITEM,
    JSONExtractRaw(payload, 'TOTAL') AS TOTAL,
    JSONExtractRaw(payload, 'VLRTOTAL') AS VLRTOTAL,
    JSONExtractRaw(payload, 'VALOR_TOTAL') AS VALOR_TOTAL,
    JSONExtractRaw(payload, 'VLRLIQUIDO') AS VLRLIQUIDO,
    JSONExtractRaw(payload, 'SUBTOTAL') AS SUBTOTAL,
    JSONExtractRaw(payload, 'QTDE') AS QTDE,
    JSONExtractRaw(payload, 'VLRUNITARIO') AS VLRUNITARIO,
    total_shadow
  FROM torqmind_current.stg_itenscomprovantes FINAL
  WHERE id_empresa=$ID_EMPRESA
    AND (JSONHas(payload, 'VLRTOTALITEM') OR JSONHas(payload, 'TOTAL') OR JSONHas(payload, 'VLRTOTAL'))
  LIMIT 20
  SETTINGS max_memory_usage=1000000000, max_threads=2
  FORMAT PrettyCompact" || true
  log ""
  log "CFOP distribution and faturamento impact:"
  ch_query "
  SELECT
    i.cfop,
    count() AS item_rows,
    sum(i.qtd) AS qtd,
    sum(i.total) AS total_all,
    sumIf(i.total, c.cancelado=0) AS total_not_cancelled,
    sumIf(i.total, c.cancelado=1) AS total_cancelled
  FROM torqmind_current.stg_itenscomprovantes_slim AS i
  INNER JOIN torqmind_current.stg_comprovantes_slim AS c
    ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante
  WHERE i.id_empresa=$ID_EMPRESA AND i.is_deleted=0 AND c.is_deleted=0
  GROUP BY i.cfop
  ORDER BY total_not_cancelled DESC
  LIMIT 50
  SETTINGS max_memory_usage=3000000000, max_threads=2, join_algorithm='partial_merge'
  FORMAT PrettyCompact" || true
  log ""
  log "id_db and repeated comprovantes diagnostics:"
  ch_query "
  SELECT 'itens_id_db_zero' AS metric, toInt64(countIf(id_db=0)) AS value
  FROM torqmind_current.stg_itenscomprovantes FINAL
  WHERE id_empresa=$ID_EMPRESA AND is_deleted=0
  UNION ALL
  SELECT 'comprovantes_id_db_zero', toInt64(countIf(id_db=0))
  FROM torqmind_current.stg_comprovantes FINAL
  WHERE id_empresa=$ID_EMPRESA AND is_deleted=0
  UNION ALL
  SELECT 'comprovantes_repeated_without_id_db', toInt64(count())
  FROM (
    SELECT id_empresa, id_filial, id_comprovante, count() AS c
    FROM torqmind_current.stg_comprovantes FINAL
    WHERE id_empresa=$ID_EMPRESA AND is_deleted=0
    GROUP BY id_empresa, id_filial, id_comprovante
    HAVING c > 1
  )
  UNION ALL
  SELECT 'comprovantes_repeated_with_id_db', toInt64(count())
  FROM (
    SELECT id_empresa, id_filial, id_db, id_comprovante, count() AS c
    FROM torqmind_current.stg_comprovantes FINAL
    WHERE id_empresa=$ID_EMPRESA AND is_deleted=0
    GROUP BY id_empresa, id_filial, id_db, id_comprovante
    HAVING c > 1
  )
  UNION ALL
  SELECT 'itens_raw_minus_dedup_slim', toInt64(count()) - toInt64(uniqExact(id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante))
  FROM torqmind_current.stg_itenscomprovantes_slim
  WHERE id_empresa=$ID_EMPRESA
  FORMAT PrettyCompact" || true
  log ""
}

validate_api_realtime() {
  CHECKS=$((CHECKS + 1))
  local result
  if ! result="$(compose_prod exec -T api env \
    USE_REALTIME_MARTS=true \
    REALTIME_MARTS_SOURCE="$SOURCE" \
    REALTIME_MARTS_FALLBACK=false \
    USE_CLICKHOUSE=true \
    ID_EMPRESA="$ID_EMPRESA" \
    python - <<'PY' 2>&1
import os
from datetime import date, timedelta

from app.config import settings

assert settings.use_realtime_marts is True, "USE_REALTIME_MARTS is not effective"
assert settings.realtime_marts_fallback is False, "REALTIME_MARTS_FALLBACK must be false"
assert settings.realtime_marts_source == os.environ["REALTIME_MARTS_SOURCE"], "REALTIME_MARTS_SOURCE is not effective"

from app import repos_analytics

dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=30)
payload = getattr(repos_analytics, "dashboard_kpis")(
    "admin",
    int(os.environ["ID_EMPRESA"]),
    None,
    dt_ini,
    dt_fim,
)
assert isinstance(payload, dict), "dashboard_kpis did not return a dict"
print("API_REALTIME_OK")
PY
)"; then
    record_failure "api.realtime.facade" "FAILED"
    printf '%s\n' "$result" | sed 's/^/    /'
    return
  fi

  if [[ "$result" != *"API_REALTIME_OK"* ]]; then
    record_failure "api.realtime.facade" "FAILED"
    printf '%s\n' "$result" | sed 's/^/    /'
    return
  fi
  printf '  %-48s OK\n' "api.realtime.facade"
}

emit_proof_json() {
  local result="$1"
  printf '{"proof":"realtime-validate-cutover","source":"%s","id_empresa":%s,"result":"%s","checks":%s,"failures":%s}\n' \
    "$SOURCE" "$ID_EMPRESA" "$result" "$CHECKS" "$FAILURES"
}

main() {
  log "=== Realtime Cutover Validation (BLOCKING) ==="
  log "id_empresa=$ID_EMPRESA source=$SOURCE decimal_tolerance=$DECIMAL_TOLERANCE env_file=$ENV_FILE"
  log ""

  log "ClickHouse connectivity:"
  require_clickhouse_connection || true
  log ""

  log "Required mart_rt tables:"
  local required_tables=(
    torqmind_mart_rt.dashboard_home_rt
    torqmind_mart_rt.sales_daily_rt
    torqmind_mart_rt.sales_hourly_rt
    torqmind_mart_rt.sales_products_rt
    torqmind_mart_rt.sales_groups_rt
    torqmind_mart_rt.payments_by_type_rt
    torqmind_mart_rt.cash_overview_rt
    torqmind_mart_rt.fraud_daily_rt
    torqmind_mart_rt.risk_recent_events_rt
    torqmind_mart_rt.finance_overview_rt
    torqmind_mart_rt.source_freshness
    torqmind_mart_rt.mart_publication_log
  )
  local table
  for table in "${required_tables[@]}"; do
    validate_table_exists "$table" || true
  done
  log ""

  if [[ "$SOURCE" == "stg" ]]; then
  log "STG canonical source vs mart_rt:"
  local stg_sales_source="
    SELECT COALESCE(sum(etl.resolve_item_total(i.total_shadow, i.payload)::numeric(18,2)), 0)
    FROM stg.itenscomprovantes i
    JOIN stg.comprovantes c
      ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante
    WHERE i.id_empresa=$ID_EMPRESA
      AND NOT etl.comprovante_is_cancelled(
        COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false),
        COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO'))
      )
      AND COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) > 5000
  "
  local stg_sales_docs="
    SELECT COALESCE(count(DISTINCT (c.id_empresa, c.id_filial, c.id_db, c.id_comprovante)), 0)
    FROM stg.comprovantes c
    JOIN stg.itenscomprovantes i
      ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante
    WHERE c.id_empresa=$ID_EMPRESA
      AND NOT etl.comprovante_is_cancelled(
        COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false),
        COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO'))
      )
      AND COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) > 5000
  "
  compare_pg_ch_metric "stg.sales.faturamento" \
    "$stg_sales_source" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_pg_ch_metric "stg.sales.qtd_vendas" \
    "$stg_sales_docs" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_pg_ch_metric "stg.items.rows" \
    "SELECT count(DISTINCT (i.id_empresa, i.id_filial, i.id_db, i.id_comprovante, i.id_itemcomprovante)) FROM stg.itenscomprovantes i JOIN stg.comprovantes c ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante WHERE i.id_empresa=$ID_EMPRESA AND NOT etl.comprovante_is_cancelled(COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false), COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO'))) AND COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) > 5000" \
    "SELECT sum(qtd_itens) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "STG payments:"
  compare_pg_ch_metric "stg.payments.total" \
    "SELECT COALESCE(sum(COALESCE(valor_shadow, etl.safe_numeric(payload->>'VALOR')::numeric(18,2))), 0) FROM stg.formas_pgto_comprovantes WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_pg_ch_metric "stg.payments.types" \
    "SELECT count(DISTINCT tipo_forma) FROM stg.formas_pgto_comprovantes WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count(DISTINCT tipo_forma) FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "STG risk/fraud:"
  compare_pg_ch_metric "stg.cancelados.count" \
    "SELECT count(*) FROM stg.comprovantes c WHERE c.id_empresa=$ID_EMPRESA AND etl.comprovante_is_cancelled(COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false), COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')))" \
    "SELECT sum(qtd_eventos) FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_pg_ch_metric "stg.cancelados.valor" \
    "SELECT COALESCE(sum(COALESCE(valor_total_shadow, etl.safe_numeric(payload->>'VLRTOTAL')::numeric(18,2))), 0) FROM stg.comprovantes c WHERE c.id_empresa=$ID_EMPRESA AND etl.comprovante_is_cancelled(COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false), COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')))" \
    "SELECT sum(impacto_total) FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  log "STG finance:"
  compare_pg_ch_metric "stg.finance.count" \
    "SELECT (SELECT count(*) FROM stg.financeiro WHERE id_empresa=$ID_EMPRESA) + (SELECT count(*) FROM stg.contaspagar WHERE id_empresa=$ID_EMPRESA) + (SELECT count(*) FROM stg.contasreceber WHERE id_empresa=$ID_EMPRESA)" \
    "SELECT sum(qtd_titulos) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_pg_ch_metric "stg.finance.valor" \
    "SELECT COALESCE((SELECT sum(etl.safe_numeric(payload->>'VALOR')::numeric(18,2)) FROM stg.financeiro WHERE id_empresa=$ID_EMPRESA),0) + COALESCE((SELECT sum(etl.safe_numeric(payload->>'VALOR')::numeric(18,2)) FROM stg.contaspagar WHERE id_empresa=$ID_EMPRESA),0) + COALESCE((SELECT sum(etl.safe_numeric(payload->>'VALOR')::numeric(18,2)) FROM stg.contasreceber WHERE id_empresa=$ID_EMPRESA),0)" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  # If any STG divergence found, print diagnostic breakdown
  if (( FAILURES > 0 )); then
    stg_diagnostics
  fi

  log "Sales mart data_key coverage (slim→mart completeness):"
  # Check that every data_key with valid sales in slim appears in all 4 sales marts
  local coverage_result
  coverage_result="$(ch_query "
    WITH slim_keys AS (
      SELECT DISTINCT c.data_key
      FROM torqmind_current.stg_comprovantes_slim AS c
      INNER JOIN torqmind_current.stg_itenscomprovantes_slim AS i
        ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial
        AND c.id_db=i.id_db AND c.id_comprovante=i.id_comprovante
      WHERE c.id_empresa=$ID_EMPRESA AND c.data_key > 0
        AND c.cancelado=0 AND c.is_deleted=0 AND i.is_deleted=0 AND i.cfop > 5000
    ),
    daily_keys AS (SELECT DISTINCT data_key FROM torqmind_mart_rt.sales_daily_rt WHERE id_empresa=$ID_EMPRESA),
    hourly_keys AS (SELECT DISTINCT data_key FROM torqmind_mart_rt.sales_hourly_rt WHERE id_empresa=$ID_EMPRESA),
    products_keys AS (SELECT DISTINCT data_key FROM torqmind_mart_rt.sales_products_rt WHERE id_empresa=$ID_EMPRESA),
    groups_keys AS (SELECT DISTINCT data_key FROM torqmind_mart_rt.sales_groups_rt WHERE id_empresa=$ID_EMPRESA)
    SELECT
      countIf(sk.data_key NOT IN (SELECT data_key FROM daily_keys)) AS missing_daily,
      countIf(sk.data_key NOT IN (SELECT data_key FROM hourly_keys)) AS missing_hourly,
      countIf(sk.data_key NOT IN (SELECT data_key FROM products_keys)) AS missing_products,
      countIf(sk.data_key NOT IN (SELECT data_key FROM groups_keys)) AS missing_groups
    FROM slim_keys AS sk
    SETTINGS max_memory_usage=3000000000, max_threads=2
  ")"
  if [[ "$coverage_result" == __ERROR__* ]]; then
    record_failure "sales.data_key_coverage" "QUERY_FAILED"
  else
    local missing_daily missing_hourly missing_products missing_groups
    missing_daily="$(echo "$coverage_result" | cut -f1)"
    missing_hourly="$(echo "$coverage_result" | cut -f2)"
    missing_products="$(echo "$coverage_result" | cut -f3)"
    missing_groups="$(echo "$coverage_result" | cut -f4)"
    missing_daily="$(normalize_number "$missing_daily")"
    missing_hourly="$(normalize_number "$missing_hourly")"
    missing_products="$(normalize_number "$missing_products")"
    missing_groups="$(normalize_number "$missing_groups")"

    CHECKS=$((CHECKS + 1))
    if [[ "$missing_daily" != "0" ]]; then
      record_failure "sales_daily_rt.data_key_coverage" "MISSING_KEYS=$missing_daily"
    else
      printf '  %-48s OK\n' "sales_daily_rt.data_key_coverage"
    fi
    CHECKS=$((CHECKS + 1))
    if [[ "$missing_hourly" != "0" ]]; then
      record_failure "sales_hourly_rt.data_key_coverage" "MISSING_KEYS=$missing_hourly"
    else
      printf '  %-48s OK\n' "sales_hourly_rt.data_key_coverage"
    fi
    CHECKS=$((CHECKS + 1))
    if [[ "$missing_products" != "0" ]]; then
      record_failure "sales_products_rt.data_key_coverage" "MISSING_KEYS=$missing_products"
    else
      printf '  %-48s OK\n' "sales_products_rt.data_key_coverage"
    fi
    CHECKS=$((CHECKS + 1))
    if [[ "$missing_groups" != "0" ]]; then
      record_failure "sales_groups_rt.data_key_coverage" "MISSING_KEYS=$missing_groups"
    else
      printf '  %-48s OK\n' "sales_groups_rt.data_key_coverage"
    fi
  fi
  log ""

  log "Sales mart data_key=0 prohibition:"
  local zero_check
  for mart_table in sales_daily_rt sales_hourly_rt sales_products_rt sales_groups_rt; do
    CHECKS=$((CHECKS + 1))
    zero_check="$(ch_query "SELECT count() FROM torqmind_mart_rt.$mart_table WHERE id_empresa=$ID_EMPRESA AND data_key=0")"
    zero_check="$(normalize_number "$zero_check")"
    if [[ "$zero_check" != "0" ]]; then
      record_failure "${mart_table}.data_key_zero" "ROWS_WITH_ZERO=$zero_check"
    else
      printf '  %-48s OK\n' "${mart_table}.no_data_key_zero"
    fi
  done
  log ""

  else
  log "Sales daily:"
  compare_metric "sales_daily.rows" \
    "SELECT count() FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_daily.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_daily.qtd_vendas" \
    "SELECT sum(vendas) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart_rt.sales_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "Sales hourly:"
  compare_metric "sales_hourly.rows" \
    "SELECT count() FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_hourly.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_hourly.qtd_vendas" \
    "SELECT sum(vendas) FROM torqmind_mart.agg_vendas_hora WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_vendas) FROM torqmind_mart_rt.sales_hourly_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "Products and groups:"
  compare_metric "sales_products.rows" \
    "SELECT count() FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_products.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_products.qtd" \
    "SELECT sum(qtd) FROM torqmind_mart.agg_produtos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd) FROM torqmind_mart_rt.sales_products_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "sales_groups.rows" \
    "SELECT count() FROM torqmind_mart.agg_grupos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.sales_groups_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "sales_groups.faturamento" \
    "SELECT sum(faturamento) FROM torqmind_mart.agg_grupos_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(faturamento) FROM torqmind_mart_rt.sales_groups_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""

  log "Payments by type:"
  compare_metric "payments.total" \
    "SELECT sum(total_valor) FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_grouped_sum "payments.grouped_by_label" \
    "SELECT concat(category, '|', label) AS k, sum(total_valor) AS v FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa=$ID_EMPRESA GROUP BY k" \
    "SELECT concat(category, '|', label) AS k, sum(valor_total) AS v FROM torqmind_mart_rt.payments_by_type_rt FINAL WHERE id_empresa=$ID_EMPRESA GROUP BY k"
  log ""

  log "Risk/Fraud:"
  compare_metric "risk_daily.count" \
    "SELECT sum(eventos_risco_total) FROM torqmind_mart.agg_risco_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(qtd_eventos) FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "risk_daily.impacto" \
    "SELECT sum(impacto_estimado_total) FROM torqmind_mart.agg_risco_diaria WHERE id_empresa=$ID_EMPRESA" \
    "SELECT sum(impacto_total) FROM torqmind_mart_rt.fraud_daily_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "risk_recent_events.rows" \
    "SELECT count() FROM torqmind_mart.risco_eventos_recentes WHERE id_empresa=$ID_EMPRESA" \
    "SELECT count() FROM torqmind_mart_rt.risk_recent_events_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  log ""

  log "Finance:"
  compare_metric "finance.count" \
    "SELECT count() FROM torqmind_current.fact_financeiro FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" \
    "SELECT sum(qtd_titulos) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA" \
    "count"
  compare_metric "finance.valor_total" \
    "SELECT sum(coalesce(valor, 0)) FROM torqmind_current.fact_financeiro FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" \
    "SELECT sum(valor_total) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  compare_metric "finance.valor_pago" \
    "SELECT sum(coalesce(valor_pago, 0)) FROM torqmind_current.fact_financeiro FINAL WHERE id_empresa=$ID_EMPRESA AND is_deleted=0" \
    "SELECT sum(valor_pago_total) FROM torqmind_mart_rt.finance_overview_rt FINAL WHERE id_empresa=$ID_EMPRESA"
  log ""
  fi

  log "API realtime facade:"
  validate_api_realtime
  log ""

  log "Mart RT publications:"
  ch_query "SELECT mart_name, max(published_at) AS last, sum(rows_written) FROM torqmind_mart_rt.mart_publication_log GROUP BY mart_name ORDER BY last DESC" || true
  log ""

  log "CDC state:"
  ch_query "SELECT table_name, events_total, last_event_at FROM torqmind_ops.cdc_table_state FINAL ORDER BY table_name" || true
  log ""

  log "============================================"
  log "CHECKS=$CHECKS  FAILURES=$FAILURES"
  if (( FAILURES > 0 )); then
    log "RESULT: FAILED - cutover BLOCKED."
    emit_proof_json "FAIL"
    exit 1
  fi
  log "RESULT: PASSED - all checks within tolerance."
  emit_proof_json "PASS"
  log "============================================"
}

main "$@"
