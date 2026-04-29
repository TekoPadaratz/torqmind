#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"
MODE="${MODE:-incremental}"
DT_INI="${DT_INI:-}"
DT_FIM="${DT_FIM:-}"
ID_EMPRESA="${ID_EMPRESA:-}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

if [[ "$ALLOW_INSECURE_ENV" != "1" ]]; then
  tm_require_prod_runtime_env "$ENV_FILE"
fi

cd "$ROOT_DIR"

if [[ "$MODE" != "full" && "$MODE" != "incremental" ]]; then
  echo "MODE must be full or incremental" >&2
  exit 2
fi
if [[ -n "$ID_EMPRESA" && ! "$ID_EMPRESA" =~ ^[0-9]+$ ]]; then
  echo "ID_EMPRESA must be numeric when provided" >&2
  exit 2
fi

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

clickhouse_client_args=(clickhouse-client)
if [[ -n "${CLICKHOUSE_USER:-}" ]]; then
  clickhouse_client_args+=(--user "$CLICKHOUSE_USER")
fi
if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
  clickhouse_client_args+=(--password "$CLICKHOUSE_PASSWORD")
fi

ch() {
  compose exec -T clickhouse "${clickhouse_client_args[@]}" "$@"
}

ensure_ops_metadata() {
  ch --multiquery --query "
CREATE DATABASE IF NOT EXISTS torqmind_ops;
CREATE TABLE IF NOT EXISTS torqmind_ops.sync_state (
  name String,
  mode String,
  status String,
  last_success_at DateTime64(6),
  dt_ini_key Nullable(Int32),
  dt_fim_key Nullable(Int32),
  changed_rows UInt64,
  message String,
  updated_at DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY name;
"
}

date_to_key() {
  local value="$1"
  python3 - "$value" <<'PY'
from datetime import date
import sys
print(int(date.fromisoformat(sys.argv[1]).strftime("%Y%m%d")))
PY
}

key_to_date() {
  local value="$1"
  python3 - "$value" <<'PY'
from datetime import date
import sys
raw = str(sys.argv[1])
print(date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])).isoformat())
PY
}

resolve_window() {
  if [[ -n "$DT_INI" && -n "$DT_FIM" ]]; then
    printf "%s|%s" "$(date_to_key "$DT_INI")" "$(date_to_key "$DT_FIM")"
    return 0
  fi
  ch --query "SELECT concat(toString(coalesce(argMax(dt_ini_key, updated_at), 0)), '|', toString(coalesce(argMax(dt_fim_key, updated_at), 0))) FROM torqmind_ops.sync_state WHERE name = 'dw_incremental' AND status = 'ok' AND mode = 'incremental'"
}

run_full_refresh() {
  ensure_ops_metadata
  echo "== recreate torqmind_mart tables =="
  ch --query "DROP DATABASE IF EXISTS torqmind_mart SYNC"
  ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_design.sql"
  echo "== backfill torqmind_mart =="
  ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase3_native_backfill.sql"
  echo "== create streaming materialized views =="
  ch --multiquery < "$ROOT_DIR/sql/clickhouse/phase2_mvs_streaming_triggers.sql"
  ch --query "INSERT INTO torqmind_ops.sync_state SELECT 'mart_publication', 'full', 'ok', now64(6), null, toInt32(coalesce(max(data_key), 0)), toUInt64(count()), 'full_mart_refresh_completed', now64(6) FROM torqmind_mart.agg_vendas_diaria"
}

delete_data_key_window() {
  local table="$1"
  local dt_ini_key="$2"
  local dt_fim_key="$3"
  local tenant_clause=""
  if [[ -n "$ID_EMPRESA" ]]; then
    tenant_clause=" AND id_empresa = ${ID_EMPRESA}"
  fi
  ch --query "ALTER TABLE torqmind_mart.${table} DELETE WHERE data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_clause} SETTINGS mutations_sync = 1"
}

delete_tenant_table() {
  local table="$1"
  if [[ -n "$ID_EMPRESA" ]]; then
    ch --query "ALTER TABLE torqmind_mart.${table} DELETE WHERE id_empresa = ${ID_EMPRESA} SETTINGS mutations_sync = 1"
  else
    ch --query "TRUNCATE TABLE torqmind_mart.${table}"
  fi
}

run_incremental_refresh() {
  ensure_ops_metadata
  local window dt_ini_key dt_fim_key dt_ini_date dt_fim_date tenant_filter_v tenant_filter_i tenant_filter_p tenant_filter_c tenant_filter_t tenant_filter_f tenant_filter_r tenant_filter_output
  window="$(resolve_window)"
  IFS='|' read -r dt_ini_key dt_fim_key <<< "$window"
  if [[ "${dt_ini_key:-0}" -le 0 || "${dt_fim_key:-0}" -le 0 ]]; then
    echo "No ClickHouse mart window to refresh."
    return 0
  fi
  dt_ini_date="$(key_to_date "$dt_ini_key")"
  dt_fim_date="$(key_to_date "$dt_fim_key")"

  tenant_filter_v=""
  tenant_filter_i=""
  tenant_filter_p=""
  tenant_filter_c=""
  tenant_filter_t=""
  tenant_filter_f=""
  tenant_filter_r=""
  tenant_filter_output=""
  if [[ -n "$ID_EMPRESA" ]]; then
    tenant_filter_v=" AND v.id_empresa = ${ID_EMPRESA}"
    tenant_filter_i=" AND i.id_empresa = ${ID_EMPRESA}"
    tenant_filter_p=" AND p.id_empresa = ${ID_EMPRESA}"
    tenant_filter_c=" AND c.id_empresa = ${ID_EMPRESA}"
    tenant_filter_t=" AND t.id_empresa = ${ID_EMPRESA}"
    tenant_filter_f=" AND f.id_empresa = ${ID_EMPRESA}"
    tenant_filter_r=" AND r.id_empresa = ${ID_EMPRESA}"
    tenant_filter_output=" AND id_empresa = ${ID_EMPRESA}"
  fi

  echo "== refresh torqmind_mart incremental window ${dt_ini_key}-${dt_fim_key} =="

  for table in \
    agg_vendas_diaria agg_vendas_hora agg_produtos_diaria agg_grupos_diaria agg_funcionarios_diaria insights_base_diaria \
    fraude_cancelamentos_diaria fraude_cancelamentos_eventos agg_risco_diaria risco_top_funcionarios_diaria risco_turno_local_diaria \
    financeiro_vencimentos_diaria agg_pagamentos_diaria agg_pagamentos_turno pagamentos_anomalias_diaria anonymous_retention_daily; do
    if [[ "$table" == "anonymous_retention_daily" ]]; then
      ch --query "ALTER TABLE torqmind_mart.${table} DELETE WHERE dt_ref BETWEEN toDate('${dt_ini_date}') AND toDate('${dt_fim_date}')${tenant_filter_output} SETTINGS mutations_sync = 1"
    else
      delete_data_key_window "$table" "$dt_ini_key" "$dt_fim_key"
    fi
  done

  for table in agg_caixa_turno_aberto agg_caixa_forma_pagamento agg_caixa_cancelamentos alerta_caixa_aberto finance_aging_daily health_score_daily; do
    delete_tenant_table "$table"
  done

  ch --multiquery <<SQL
SET max_partitions_per_insert_block = 0;

INSERT INTO torqmind_mart.agg_vendas_diaria
SELECT v.id_empresa, v.id_filial, v.data_key, toDecimal128(sum(ifNull(i.total, 0)), 2), toInt32(count()), toDecimal128(sum(ifNull(i.margem, 0)), 2), toDecimal128(if(countDistinct(v.id_comprovante) = 0, 0, sum(ifNull(i.total, 0)) / countDistinct(v.id_comprovante)), 2), now()
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key;

INSERT INTO torqmind_mart.agg_vendas_hora
SELECT v.id_empresa, v.id_filial, v.data_key, toInt8(toHour(v.data)), toDecimal128(sum(ifNull(i.total, 0)), 2), toDecimal128(sum(ifNull(i.margem, 0)), 2), toInt32(countDistinct(v.id_comprovante)), now()
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND v.data IS NOT NULL AND ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt8(toHour(v.data));

INSERT INTO torqmind_mart.agg_produtos_diaria
SELECT v.id_empresa, v.id_filial, v.data_key, toInt32(i.id_produto), ifNull(p.nome, ''), toDecimal128(sum(ifNull(i.total, 0)), 2), toDecimal128(sum(ifNull(i.custo_total, 0)), 2), toDecimal128(sum(ifNull(i.margem, 0)), 2), toDecimal128(sum(ifNull(i.qtd, 0)), 3), now()
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_produto p ON p.id_empresa = i.id_empresa AND p.id_filial = i.id_filial AND p.id_produto = i.id_produto
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt32(i.id_produto), ifNull(p.nome, '');

INSERT INTO torqmind_mart.agg_grupos_diaria
SELECT v.id_empresa, v.id_filial, v.data_key, toInt32(ifNull(i.id_grupo_produto, -1)), ifNull(g.nome, '(Sem grupo)'), toDecimal128(sum(ifNull(i.total, 0)), 2), toDecimal128(sum(ifNull(i.margem, 0)), 2), now()
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_grupo_produto g ON g.id_empresa = i.id_empresa AND g.id_filial = i.id_filial AND g.id_grupo_produto = i.id_grupo_produto
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt32(ifNull(i.id_grupo_produto, -1)), ifNull(g.nome, '(Sem grupo)');

INSERT INTO torqmind_mart.agg_funcionarios_diaria
SELECT v.id_empresa, v.id_filial, v.data_key, toInt32(ifNull(i.id_funcionario, -1)), ifNull(f.nome, '(Sem funcionario)'), toDecimal128(sum(ifNull(i.total, 0)), 2), toDecimal128(sum(ifNull(i.margem, 0)), 2), toInt32(countDistinct(v.id_comprovante)), now()
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_funcionario f ON f.id_empresa = i.id_empresa AND f.id_filial = i.id_filial AND f.id_funcionario = i.id_funcionario
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt32(ifNull(i.id_funcionario, -1)), ifNull(f.nome, '(Sem funcionario)');

INSERT INTO torqmind_mart.insights_base_diaria
SELECT v.id_empresa, v.id_filial, v.data_key, toDecimal128(sum(ifNull(i.total, 0)), 2), toDecimal128(sum(ifNull(i.total, 0)), 2), toDecimal128(0, 2), CAST(NULL, 'Nullable(String)'), CAST(NULL, 'Nullable(Decimal(38,2))'), CAST(NULL, 'Nullable(Decimal(38,2))'), CAST(NULL, 'Nullable(Decimal(38,4))'), CAST(NULL, 'Nullable(String)'), CAST(NULL, 'Nullable(Decimal(38,4))'), CAST(NULL, 'Nullable(Decimal(38,2))'), now(), '{}'
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key;

INSERT INTO torqmind_mart.fraude_cancelamentos_diaria
SELECT c.id_empresa, c.id_filial, c.data_key, toInt32(count()), toDecimal128(sum(ifNull(c.valor_total, 0)), 2), now()
FROM torqmind_dw.fact_comprovante c
WHERE c.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_c} AND ifNull(c.cancelado, 0) = 1
GROUP BY c.id_empresa, c.id_filial, c.data_key;

INSERT INTO torqmind_mart.fraude_cancelamentos_eventos
SELECT c.id_empresa, c.id_filial, c.id_db, toString(c.id_comprovante), c.data, c.data_key, c.id_usuario, c.id_turno, toDecimal128(ifNull(c.valor_total, 0), 2), now()
FROM torqmind_dw.fact_comprovante c
WHERE c.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_c} AND ifNull(c.cancelado, 0) = 1;

INSERT INTO torqmind_mart.agg_risco_diaria
SELECT r.id_empresa, r.id_filial, r.data_key, toInt32(count()), toInt32(countIf(ifNull(r.score_risco, 0) >= 80)), toDecimal128(sum(ifNull(r.impacto_estimado, 0)), 2), toDecimal128(avg(ifNull(r.score_risco, 0)), 2), toDecimal128(quantileExact(0.95)(ifNull(r.score_risco, 0)), 2), now()
FROM torqmind_dw.fact_risco_evento r
WHERE r.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_r}
GROUP BY r.id_empresa, r.id_filial, r.data_key;

INSERT INTO torqmind_mart.risco_top_funcionarios_diaria
SELECT r.id_empresa, r.id_filial, r.data_key, toInt32(ifNull(r.id_funcionario, -1)), ifNull(df.nome, '(Sem funcionario)'), toInt32(count()), toInt32(countIf(ifNull(r.score_risco, 0) >= 80)), toDecimal128(sum(ifNull(r.impacto_estimado, 0)), 2), toDecimal128(avg(ifNull(r.score_risco, 0)), 2), now()
FROM torqmind_dw.fact_risco_evento r
LEFT JOIN torqmind_dw.dim_funcionario df ON df.id_empresa = r.id_empresa AND df.id_filial = r.id_filial AND df.id_funcionario = r.id_funcionario
WHERE r.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_r}
GROUP BY r.id_empresa, r.id_filial, r.data_key, toInt32(ifNull(r.id_funcionario, -1)), ifNull(df.nome, '(Sem funcionario)');

INSERT INTO torqmind_mart.risco_turno_local_diaria
SELECT r.id_empresa, r.id_filial, r.data_key, toInt32(ifNull(r.id_turno, -1)), toInt32(ifNull(i.id_local_venda, -1)), toInt32(count()), toInt32(countIf(ifNull(r.score_risco, 0) >= 80)), toDecimal128(sum(ifNull(r.impacto_estimado, 0)), 2), toDecimal128(avg(ifNull(r.score_risco, 0)), 2), now()
FROM torqmind_dw.fact_risco_evento r
LEFT JOIN torqmind_dw.fact_venda_item i ON i.id_empresa = r.id_empresa AND i.id_filial = r.id_filial AND i.id_db = r.id_db AND i.id_movprodutos = r.id_movprodutos
WHERE r.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_r}
GROUP BY r.id_empresa, r.id_filial, r.data_key, toInt32(ifNull(r.id_turno, -1)), toInt32(ifNull(i.id_local_venda, -1));

INSERT INTO torqmind_mart.financeiro_vencimentos_diaria
SELECT f.id_empresa, f.id_filial, f.data_key_venc, toInt8(f.tipo_titulo), toDecimal128(sum(ifNull(f.valor, 0)), 2), toDecimal128(sum(ifNull(f.valor_pago, 0)), 2), toDecimal128(sum(if(isNull(f.data_pagamento), greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), 0)), 2), now()
FROM torqmind_dw.fact_financeiro f
WHERE f.data_key_venc BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_f}
GROUP BY f.id_empresa, f.id_filial, f.data_key_venc, toInt8(f.tipo_titulo);

INSERT INTO torqmind_mart.agg_pagamentos_diaria
SELECT p.id_empresa, p.id_filial, p.data_key, multiIf(p.tipo_forma IN (3, 13, 23), 'PIX', p.tipo_forma IN (4, 5, 6), 'CARTAO', p.tipo_forma IN (1), 'DINHEIRO', 'NAO_IDENTIFICADO'), concat('FORMA_', toString(p.tipo_forma)), toDecimal128(sum(ifNull(p.valor, 0)), 2), toInt32(countDistinct(p.referencia)), toDecimal64(0, 2), now()
FROM torqmind_dw.fact_pagamento_comprovante p
WHERE p.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_p}
GROUP BY p.id_empresa, p.id_filial, p.data_key, multiIf(p.tipo_forma IN (3, 13, 23), 'PIX', p.tipo_forma IN (4, 5, 6), 'CARTAO', p.tipo_forma IN (1), 'DINHEIRO', 'NAO_IDENTIFICADO'), concat('FORMA_', toString(p.tipo_forma));

INSERT INTO torqmind_mart.agg_pagamentos_turno
SELECT p.id_empresa, p.id_filial, p.data_key, toInt32(ifNull(p.id_turno, -1)), multiIf(p.tipo_forma IN (3, 13, 23), 'PIX', p.tipo_forma IN (4, 5, 6), 'CARTAO', p.tipo_forma IN (1), 'DINHEIRO', 'NAO_IDENTIFICADO'), concat('FORMA_', toString(p.tipo_forma)), toDecimal128(sum(ifNull(p.valor, 0)), 2), toInt32(countDistinct(p.referencia)), now()
FROM torqmind_dw.fact_pagamento_comprovante p
WHERE p.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_p}
GROUP BY p.id_empresa, p.id_filial, p.data_key, toInt32(ifNull(p.id_turno, -1)), multiIf(p.tipo_forma IN (3, 13, 23), 'PIX', p.tipo_forma IN (4, 5, 6), 'CARTAO', p.tipo_forma IN (1), 'DINHEIRO', 'NAO_IDENTIFICADO'), concat('FORMA_', toString(p.tipo_forma));

INSERT INTO torqmind_mart.pagamentos_anomalias_diaria
SELECT p.id_empresa, p.id_filial, p.data_key, toInt32(ifNull(p.id_turno, -1)), 'PAYMENT_PATTERN', multiIf(sum(ifNull(p.valor, 0)) >= 100000, 'CRITICAL', sum(ifNull(p.valor, 0)) >= 30000, 'WARN', 'INFO'), toDecimal64(least(100, greatest(0, sum(ifNull(p.valor, 0)) / 1000)), 2), concat('PAY|', toString(p.id_empresa), '|', toString(p.id_filial), '|', toString(p.data_key), '|', toString(toInt32(ifNull(p.id_turno, -1)))), toInt32(if(countDistinct(p.tipo_forma) >= 3, countDistinct(p.referencia), 0)), toInt32(countDistinct(p.referencia)), toDecimal128(sum(ifNull(p.valor, 0)), 2), toDecimal64(avg(toFloat64(ifNull(p.tipo_forma, 0))), 2), now()
FROM torqmind_dw.fact_pagamento_comprovante p
WHERE p.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_p}
GROUP BY p.id_empresa, p.id_filial, p.data_key, toInt32(ifNull(p.id_turno, -1));

INSERT INTO torqmind_mart.anonymous_retention_daily
SELECT toDate(v.data), v.id_empresa, v.id_filial, toDecimal128(sumIf(ifNull(v.total_venda, 0), isNull(v.id_cliente) OR v.id_cliente = -1), 2), toDecimal128(0, 2), toDecimal64(0, 2), toDecimal64(if(sum(ifNull(v.total_venda, 0)) = 0, 0, (sumIf(ifNull(v.total_venda, 0), isNull(v.id_cliente) OR v.id_cliente = -1) / sum(ifNull(v.total_venda, 0))) * 100), 2), toDecimal64(0, 2), toDecimal128(0, 2), '{}', now()
FROM torqmind_dw.fact_venda v
WHERE v.data_key BETWEEN ${dt_ini_key} AND ${dt_fim_key}${tenant_filter_v} AND ifNull(v.cancelado, 0) = 0 AND v.data IS NOT NULL
GROUP BY toDate(v.data), v.id_empresa, v.id_filial;

INSERT INTO torqmind_mart.agg_caixa_turno_aberto
SELECT t.id_empresa, t.id_filial, ifNull(df.nome, ''), t.id_turno, toInt32(ifNull(t.id_usuario, -1)), ifNull(u.nome, concat('Usuario ', toString(ifNull(t.id_usuario, -1)))), t.abertura_ts, t.fechamento_ts, toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2), multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'CRITICAL', dateDiff('hour', t.abertura_ts, now()) >= 12, 'HIGH', dateDiff('hour', t.abertura_ts, now()) >= 6, 'WARN', 'OK'), multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'Critico', dateDiff('hour', t.abertura_ts, now()) >= 12, 'Atencao alta', dateDiff('hour', t.abertura_ts, now()) >= 6, 'Monitorar', 'Dentro da janela'), toDecimal128(sumIf(ifNull(c.valor_total, 0), ifNull(c.cancelado, 0) = 0), 2), toInt32(countIf(ifNull(c.cancelado, 0) = 0)), toDecimal128(sumIf(ifNull(c.valor_total, 0), ifNull(c.cancelado, 0) = 1), 2), toInt32(countIf(ifNull(c.cancelado, 0) = 1)), toDecimal128(sum(ifNull(p.valor, 0)), 2), now()
FROM torqmind_dw.fact_caixa_turno t
LEFT JOIN torqmind_dw.dim_filial df ON df.id_empresa = t.id_empresa AND df.id_filial = t.id_filial
LEFT JOIN torqmind_dw.dim_usuario_caixa u ON u.id_empresa = t.id_empresa AND u.id_filial = t.id_filial AND u.id_usuario = t.id_usuario
LEFT JOIN torqmind_dw.fact_comprovante c ON c.id_empresa = t.id_empresa AND c.id_filial = t.id_filial AND c.id_turno = t.id_turno
LEFT JOIN torqmind_dw.fact_pagamento_comprovante p ON p.id_empresa = t.id_empresa AND p.id_filial = t.id_filial AND p.id_turno = t.id_turno
WHERE ifNull(t.is_aberto, 0) = 1 AND t.abertura_ts IS NOT NULL${tenant_filter_t}
GROUP BY t.id_empresa, t.id_filial, ifNull(df.nome, ''), t.id_turno, toInt32(ifNull(t.id_usuario, -1)), ifNull(u.nome, concat('Usuario ', toString(ifNull(t.id_usuario, -1)))), t.abertura_ts, t.fechamento_ts, multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'CRITICAL', dateDiff('hour', t.abertura_ts, now()) >= 12, 'HIGH', dateDiff('hour', t.abertura_ts, now()) >= 6, 'WARN', 'OK'), multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'Critico', dateDiff('hour', t.abertura_ts, now()) >= 12, 'Atencao alta', dateDiff('hour', t.abertura_ts, now()) >= 6, 'Monitorar', 'Dentro da janela');

INSERT INTO torqmind_mart.agg_caixa_forma_pagamento
SELECT p.id_empresa, p.id_filial, toInt32(ifNull(p.id_turno, -1)), toInt32(p.tipo_forma), concat('FORMA_', toString(p.tipo_forma)), multiIf(p.tipo_forma IN (3, 13, 23), 'PIX', p.tipo_forma IN (4, 5, 6), 'CARTAO', p.tipo_forma IN (1), 'DINHEIRO', 'NAO_IDENTIFICADO'), toDecimal128(sum(ifNull(p.valor, 0)), 2), toInt32(countDistinct(p.referencia)), now()
FROM torqmind_dw.fact_pagamento_comprovante p
WHERE p.id_turno IS NOT NULL${tenant_filter_p}
GROUP BY p.id_empresa, p.id_filial, toInt32(ifNull(p.id_turno, -1)), toInt32(p.tipo_forma), concat('FORMA_', toString(p.tipo_forma)), multiIf(p.tipo_forma IN (3, 13, 23), 'PIX', p.tipo_forma IN (4, 5, 6), 'CARTAO', p.tipo_forma IN (1), 'DINHEIRO', 'NAO_IDENTIFICADO');

INSERT INTO torqmind_mart.agg_caixa_cancelamentos
SELECT c.id_empresa, c.id_filial, toInt32(ifNull(c.id_turno, -1)), ifNull(df.nome, ''), toDecimal128(sum(ifNull(c.valor_total, 0)), 2), toInt32(count()), now()
FROM torqmind_dw.fact_comprovante c
LEFT JOIN torqmind_dw.dim_filial df ON df.id_empresa = c.id_empresa AND df.id_filial = c.id_filial
WHERE ifNull(c.cancelado, 0) = 1 AND c.id_turno IS NOT NULL${tenant_filter_c}
GROUP BY c.id_empresa, c.id_filial, toInt32(ifNull(c.id_turno, -1)), ifNull(df.nome, '');

INSERT INTO torqmind_mart.alerta_caixa_aberto
SELECT t.id_empresa, t.id_filial, ifNull(df.nome, ''), t.id_turno, toInt32(ifNull(t.id_usuario, -1)), ifNull(u.nome, concat('Usuario ', toString(ifNull(t.id_usuario, -1)))), t.abertura_ts, toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2), 'CRITICAL', concat('Caixa ', toString(t.id_turno), ' aberto ha ', toString(toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2)), ' horas'), concat('O caixa ', toString(t.id_turno), ' da filial ', ifNull(df.nome, toString(t.id_filial)), ' esta aberto ha ', toString(toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2)), ' horas. Operador: ', ifNull(u.nome, 'nao identificado'), '.'), '/cash', now()
FROM torqmind_dw.fact_caixa_turno t
LEFT JOIN torqmind_dw.dim_filial df ON df.id_empresa = t.id_empresa AND df.id_filial = t.id_filial
LEFT JOIN torqmind_dw.dim_usuario_caixa u ON u.id_empresa = t.id_empresa AND u.id_filial = t.id_filial AND u.id_usuario = t.id_usuario
WHERE ifNull(t.is_aberto, 0) = 1 AND t.abertura_ts IS NOT NULL AND dateDiff('hour', t.abertura_ts, now()) >= 24${tenant_filter_t};

INSERT INTO torqmind_mart.finance_aging_daily
SELECT today(), f.id_empresa, f.id_filial, toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today()), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 0), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 0 AND f.vencimento < today()), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 0 AND 7), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 8 AND 15), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 16 AND 30), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 31 AND 60), 2), toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) > 60), 2), toDecimal64(0, 2), toUInt8(if(count() = 0, 1, 0)), now()
FROM torqmind_dw.fact_financeiro f
WHERE f.vencimento IS NOT NULL${tenant_filter_f}
GROUP BY f.id_empresa, f.id_filial;

INSERT INTO torqmind_mart.health_score_daily
SELECT today(), v.id_empresa, v.id_filial, toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30), 2), toDecimal128(sumIf(ifNull(i.margem, 0), toDate(v.data) >= today() - 30), 2), toDecimal128(if(countIf(toDate(v.data) >= today() - 30) = 0, 0, sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30) / countIf(toDate(v.data) >= today() - 30)), 2), toInt32(0), toInt32(0), toDecimal128(0, 2), toDecimal64(80, 2), toDecimal64(80, 2), toDecimal64(80, 2), toDecimal64(80, 2), now()
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v ON v.id_empresa = i.id_empresa AND v.id_filial = i.id_filial AND v.id_db = i.id_db AND v.id_movprodutos = i.id_movprodutos
WHERE ifNull(v.cancelado, 0) = 0 AND ifNull(i.cfop, 0) >= 5000 AND v.data IS NOT NULL${tenant_filter_v}
GROUP BY v.id_empresa, v.id_filial;
SQL

  ch --query "INSERT INTO torqmind_ops.sync_state SELECT 'mart_publication', 'incremental', 'ok', now64(6), toInt32(${dt_ini_key}), toInt32(${dt_fim_key}), toUInt64(0), 'mart_refresh_completed', now64(6)"
  echo "ClickHouse mart incremental refresh completed. dt_ini_key=${dt_ini_key} dt_fim_key=${dt_fim_key}"
}

compose exec -T clickhouse sh -lc 'wget -q -O - http://127.0.0.1:8123/ping | grep -q Ok'

if [[ "$MODE" == "full" ]]; then
  run_full_refresh
else
  run_incremental_refresh
fi
