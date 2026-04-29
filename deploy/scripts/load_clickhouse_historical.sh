#!/usr/bin/env bash
# ==============================================================================
# TorqMind — Carga Histórica Completa: PostgreSQL mart → ClickHouse torqmind_mart
# ==============================================================================
# Uso    : bash deploy/scripts/load_clickhouse_historical.sh
# Pré-req: - Container torqmind-api-1 em execução
#          - Variáveis de ambiente configuradas no container
#          - Tabelas ClickHouse criadas (sql/clickhouse/phase2_mvs_design.sql)
# Aviso  : Realiza carga TOTAL (sem filtro de data). Use apenas para backfill
#          inicial ou re-carga completa. Para carga incremental use os MVs CDC.
# ==============================================================================

set -euo pipefail

CONTAINER="${TORQMIND_CONTAINER:-torqmind-api-1}"

echo "================================================================"
echo "  TorqMind — Carga Histórica Completa: PostgreSQL → ClickHouse  "
echo "================================================================"
echo "Container : $CONTAINER"
echo "Início    : $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

docker exec -i "$CONTAINER" python - <<'PYTHON_EOF'
import sys
import os
import json
from datetime import datetime, date

sys.path.append("/app")

from app.db import get_conn
from app.db_clickhouse import insert_batch

_NOW = datetime.utcnow()


# ---------------------------------------------------------------------------
# Helpers de coerção de tipos nulos
# ---------------------------------------------------------------------------

def _i(v, d=0):
    """int; None → d"""
    return int(v) if v is not None else d


def _f(v, d=0.0):
    """float; None → d"""
    return float(v) if v is not None else d


def _s(v, d=""):
    """str; None → d"""
    return str(v) if v is not None else d


def _b(v):
    """bool; None → False"""
    return bool(v) if v is not None else False


def _dt(v):
    """datetime; None → utcnow()"""
    return v if v is not None else _NOW


def _j(v, d="{}"):
    """jsonb/dict/str → JSON string; None → d"""
    if v is None:
        return d
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


# ==============================================================================
# 1. agg_vendas_diaria
# ==============================================================================
def load_agg_vendas_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, faturamento, "
            "quantidade_itens, margem, ticket_medio, updated_at "
            "FROM mart.agg_vendas_diaria "
            "ORDER BY id_empresa, data_key, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "faturamento":      _f(r["faturamento"]),
            "quantidade_itens": _i(r["quantidade_itens"]),
            "margem":           _f(r["margem"]),
            "ticket_medio":     _f(r["ticket_medio"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_vendas_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial"],
    )
    print(f"✅ agg_vendas_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 2. agg_vendas_hora
# ==============================================================================
def load_agg_vendas_hora():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, hora, faturamento, "
            "margem, vendas, updated_at "
            "FROM mart.agg_vendas_hora "
            "ORDER BY id_empresa, data_key, id_filial, hora"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":  _i(r["id_empresa"]),
            "id_filial":   _i(r["id_filial"]),
            "data_key":    _i(r["data_key"]),
            "hora":        _i(r["hora"]),
            "faturamento": _f(r["faturamento"]),
            "margem":      _f(r["margem"]),
            "vendas":      _i(r["vendas"]),
            "updated_at":  _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_vendas_hora",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "hora"],
    )
    print(f"✅ agg_vendas_hora: {n:,} linhas migradas!")


# ==============================================================================
# 3. agg_produtos_diaria
# ==============================================================================
def load_agg_produtos_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_produto, produto_nome, "
            "faturamento, custo_total, margem, qtd, updated_at "
            "FROM mart.agg_produtos_diaria "
            "ORDER BY id_empresa, data_key, id_filial, id_produto"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":   _i(r["id_empresa"]),
            "id_filial":    _i(r["id_filial"]),
            "data_key":     _i(r["data_key"]),
            "id_produto":   _i(r["id_produto"]),
            "produto_nome": _s(r["produto_nome"]),
            "faturamento":  _f(r["faturamento"]),
            "custo_total":  _f(r["custo_total"]),
            "margem":       _f(r["margem"]),
            "qtd":          _f(r["qtd"]),
            "updated_at":   _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_produtos_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "id_produto"],
    )
    print(f"✅ agg_produtos_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 4. agg_grupos_diaria
# ==============================================================================
def load_agg_grupos_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_grupo_produto, grupo_nome, "
            "faturamento, margem, updated_at "
            "FROM mart.agg_grupos_diaria "
            "ORDER BY id_empresa, data_key, id_filial, id_grupo_produto"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "id_grupo_produto": _i(r["id_grupo_produto"]),
            "grupo_nome":       _s(r["grupo_nome"]),
            "faturamento":      _f(r["faturamento"]),
            "margem":           _f(r["margem"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_grupos_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "id_grupo_produto"],
    )
    print(f"✅ agg_grupos_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 5. agg_funcionarios_diaria
# ==============================================================================
def load_agg_funcionarios_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_funcionario, funcionario_nome, "
            "faturamento, margem, vendas, updated_at "
            "FROM mart.agg_funcionarios_diaria "
            "ORDER BY id_empresa, data_key, id_filial, id_funcionario"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "id_funcionario":   _i(r["id_funcionario"]),
            "funcionario_nome": _s(r["funcionario_nome"]),
            "faturamento":      _f(r["faturamento"]),
            "margem":           _f(r["margem"]),
            "vendas":           _i(r["vendas"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_funcionarios_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "id_funcionario"],
    )
    print(f"✅ agg_funcionarios_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 6. insights_base_diaria
#    Nullable columns (top_vendedor_key, top_vendedor_valor, inadimplencia_valor,
#    inadimplencia_pct, cliente_em_risco_key, margem_media_pct, giro_estoque)
#    são mantidas como None — ClickHouse aceita Python None para Nullable(T).
#    batch_info não existe no mart PG; ClickHouse usará DEFAULT '{}'.
# ==============================================================================
def load_insights_base_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, faturamento_dia, "
            "faturamento_mes_acum, comparativo_mes_anterior, "
            "top_vendedor_key, top_vendedor_valor, "
            "inadimplencia_valor, inadimplencia_pct, "
            "cliente_em_risco_key, margem_media_pct, giro_estoque, "
            "updated_at "
            "FROM mart.insights_base_diaria "
            "ORDER BY id_empresa, data_key, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":               _i(r["id_empresa"]),
            "id_filial":                _i(r["id_filial"]),
            "data_key":                 _i(r["data_key"]),
            "faturamento_dia":          _f(r["faturamento_dia"]),
            "faturamento_mes_acum":     _f(r["faturamento_mes_acum"]),
            "comparativo_mes_anterior": _f(r["comparativo_mes_anterior"]),
            "top_vendedor_key":         r["top_vendedor_key"],
            "top_vendedor_valor":       float(r["top_vendedor_valor"]) if r["top_vendedor_valor"] is not None else None,
            "inadimplencia_valor":      float(r["inadimplencia_valor"]) if r["inadimplencia_valor"] is not None else None,
            "inadimplencia_pct":        float(r["inadimplencia_pct"]) if r["inadimplencia_pct"] is not None else None,
            "cliente_em_risco_key":     r["cliente_em_risco_key"],
            "margem_media_pct":         float(r["margem_media_pct"]) if r["margem_media_pct"] is not None else None,
            "giro_estoque":             float(r["giro_estoque"]) if r["giro_estoque"] is not None else None,
            "updated_at":               _dt(r["updated_at"]),
            # batch_info omitido — ClickHouse usa DEFAULT '{}'
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.insights_base_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial"],
    )
    print(f"✅ insights_base_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 7. fraude_cancelamentos_diaria
# ==============================================================================
def load_fraude_cancelamentos_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, cancelamentos, "
            "valor_cancelado, updated_at "
            "FROM mart.fraude_cancelamentos_diaria "
            "ORDER BY id_empresa, data_key, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":      _i(r["id_empresa"]),
            "id_filial":       _i(r["id_filial"]),
            "data_key":        _i(r["data_key"]),
            "cancelamentos":   _i(r["cancelamentos"]),
            "valor_cancelado": _f(r["valor_cancelado"]),
            "updated_at":      _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.fraude_cancelamentos_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial"],
    )
    print(f"✅ fraude_cancelamentos_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 8. fraude_cancelamentos_eventos
#    id_comprovante é string no CH (pode ser numérico no PG — forçar str).
#    id_turno é Nullable(Int32) no CH — manter None quando ausente.
# ==============================================================================
def load_fraude_cancelamentos_eventos():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, id_db, id_comprovante, data, "
            "data_key, id_usuario, id_turno, valor_total, updated_at "
            "FROM mart.fraude_cancelamentos_eventos "
            "ORDER BY id_empresa, id_filial, data, id_db"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":     _i(r["id_empresa"]),
            "id_filial":      _i(r["id_filial"]),
            "id_db":          _i(r["id_db"]),
            "id_comprovante": _s(r["id_comprovante"]),
            "data":           _dt(r["data"]),
            "data_key":       _i(r["data_key"]),
            "id_usuario":     _i(r["id_usuario"]),
            "id_turno":       int(r["id_turno"]) if r["id_turno"] is not None else None,
            "valor_total":    _f(r["valor_total"]),
            "updated_at":     _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.fraude_cancelamentos_eventos",
        rows_ch,
        order_by=["id_empresa", "id_filial", "data", "id_db"],
    )
    print(f"✅ fraude_cancelamentos_eventos: {n:,} linhas migradas!")


# ==============================================================================
# 9. agg_risco_diaria
# ==============================================================================
def load_agg_risco_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, eventos_risco_total, "
            "eventos_alto_risco, impacto_estimado_total, score_medio, "
            "p95_score, updated_at "
            "FROM mart.agg_risco_diaria "
            "ORDER BY id_empresa, data_key, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":             _i(r["id_empresa"]),
            "id_filial":              _i(r["id_filial"]),
            "data_key":               _i(r["data_key"]),
            "eventos_risco_total":    _i(r["eventos_risco_total"]),
            "eventos_alto_risco":     _i(r["eventos_alto_risco"]),
            "impacto_estimado_total": _f(r["impacto_estimado_total"]),
            "score_medio":            _f(r["score_medio"]),
            "p95_score":              _f(r["p95_score"]),
            "updated_at":             _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_risco_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial"],
    )
    print(f"✅ agg_risco_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 10. risco_top_funcionarios_diaria
# ==============================================================================
def load_risco_top_funcionarios_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_funcionario, "
            "funcionario_nome, eventos, alto_risco, impacto_estimado, "
            "score_medio, updated_at "
            "FROM mart.risco_top_funcionarios_diaria "
            "ORDER BY id_empresa, data_key, id_filial, id_funcionario"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "id_funcionario":   _i(r["id_funcionario"]),
            "funcionario_nome": _s(r["funcionario_nome"]),
            "eventos":          _i(r["eventos"]),
            "alto_risco":       _i(r["alto_risco"]),
            "impacto_estimado": _f(r["impacto_estimado"]),
            "score_medio":      _f(r["score_medio"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.risco_top_funcionarios_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "id_funcionario"],
    )
    print(f"✅ risco_top_funcionarios_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 11. risco_turno_local_diaria
# ==============================================================================
def load_risco_turno_local_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_turno, id_local_venda, "
            "eventos, alto_risco, impacto_estimado, score_medio, updated_at "
            "FROM mart.risco_turno_local_diaria "
            "ORDER BY id_empresa, data_key, id_filial, id_turno, id_local_venda"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "id_turno":         _i(r["id_turno"]),
            "id_local_venda":   _i(r["id_local_venda"]),
            "eventos":          _i(r["eventos"]),
            "alto_risco":       _i(r["alto_risco"]),
            "impacto_estimado": _f(r["impacto_estimado"]),
            "score_medio":      _f(r["score_medio"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.risco_turno_local_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "id_turno", "id_local_venda"],
    )
    print(f"✅ risco_turno_local_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 12. clientes_churn_risco
#    last_purchase → Nullable(Date): manter None quando ausente.
#    reasons → jsonb no PG, String no CH.
# ==============================================================================
def load_clientes_churn_risco():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, id_cliente, cliente_nome, "
            "last_purchase, compras_30d, compras_60_30, faturamento_30d, "
            "faturamento_60_30, churn_score, reasons, updated_at "
            "FROM mart.clientes_churn_risco "
            "ORDER BY id_empresa, id_filial, id_cliente"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":        _i(r["id_empresa"]),
            "id_filial":         _i(r["id_filial"]),
            "id_cliente":        _i(r["id_cliente"]),
            "cliente_nome":      _s(r["cliente_nome"]),
            "last_purchase":     r["last_purchase"],
            "compras_30d":       _i(r["compras_30d"]),
            "compras_60_30":     _i(r["compras_60_30"]),
            "faturamento_30d":   _f(r["faturamento_30d"]),
            "faturamento_60_30": _f(r["faturamento_60_30"]),
            "churn_score":       _i(r["churn_score"]),
            "reasons":           _j(r["reasons"]),
            "updated_at":        _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.clientes_churn_risco",
        rows_ch,
        order_by=["id_empresa", "id_filial", "id_cliente"],
    )
    print(f"✅ clientes_churn_risco: {n:,} linhas migradas!")


# ==============================================================================
# 13. customer_rfm_daily
#    dt_ref → Date (psycopg retorna datetime.date — ClickHouse connect aceita).
#    last_purchase → Nullable(Date): manter None.
# ==============================================================================
def load_customer_rfm_daily():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT dt_ref, id_empresa, id_filial, id_cliente, cliente_nome, "
            "last_purchase, recency_days, frequency_30, frequency_90, "
            "monetary_30, monetary_90, ticket_30, expected_cycle_days, "
            "trend_frequency, trend_monetary, updated_at "
            "FROM mart.customer_rfm_daily "
            "ORDER BY dt_ref, id_empresa, id_filial, id_cliente"
        ).fetchall()
    rows_ch = [
        {
            "dt_ref":              r["dt_ref"],
            "id_empresa":          _i(r["id_empresa"]),
            "id_filial":           _i(r["id_filial"]),
            "id_cliente":          _i(r["id_cliente"]),
            "cliente_nome":        _s(r["cliente_nome"]),
            "last_purchase":       r["last_purchase"],
            "recency_days":        _i(r["recency_days"]),
            "frequency_30":        _i(r["frequency_30"]),
            "frequency_90":        _i(r["frequency_90"]),
            "monetary_30":         _f(r["monetary_30"]),
            "monetary_90":         _f(r["monetary_90"]),
            "ticket_30":           _f(r["ticket_30"]),
            "expected_cycle_days": _f(r["expected_cycle_days"]),
            "trend_frequency":     _i(r["trend_frequency"]),
            "trend_monetary":      _f(r["trend_monetary"]),
            "updated_at":          _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.customer_rfm_daily",
        rows_ch,
        order_by=["dt_ref", "id_empresa", "id_filial", "id_cliente"],
    )
    print(f"✅ customer_rfm_daily: {n:,} linhas migradas!")


# ==============================================================================
# 14. customer_churn_risk_daily
#    reasons → jsonb no PG, String no CH.
# ==============================================================================
def load_customer_churn_risk_daily():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT dt_ref, id_empresa, id_filial, id_cliente, cliente_nome, "
            "last_purchase, recency_days, frequency_30, frequency_90, "
            "monetary_30, monetary_90, ticket_30, expected_cycle_days, "
            "churn_score, revenue_at_risk_30d, recommendation, reasons, updated_at "
            "FROM mart.customer_churn_risk_daily "
            "ORDER BY dt_ref, id_empresa, id_filial, id_cliente"
        ).fetchall()
    rows_ch = [
        {
            "dt_ref":              r["dt_ref"],
            "id_empresa":          _i(r["id_empresa"]),
            "id_filial":           _i(r["id_filial"]),
            "id_cliente":          _i(r["id_cliente"]),
            "cliente_nome":        _s(r["cliente_nome"]),
            "last_purchase":       r["last_purchase"],
            "recency_days":        _i(r["recency_days"]),
            "frequency_30":        _i(r["frequency_30"]),
            "frequency_90":        _i(r["frequency_90"]),
            "monetary_30":         _f(r["monetary_30"]),
            "monetary_90":         _f(r["monetary_90"]),
            "ticket_30":           _f(r["ticket_30"]),
            "expected_cycle_days": _f(r["expected_cycle_days"]),
            "churn_score":         _i(r["churn_score"]),
            "revenue_at_risk_30d": _f(r["revenue_at_risk_30d"]),
            "recommendation":      _s(r["recommendation"]),
            "reasons":             _j(r["reasons"]),
            "updated_at":          _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.customer_churn_risk_daily",
        rows_ch,
        order_by=["dt_ref", "id_empresa", "id_filial", "id_cliente"],
    )
    print(f"✅ customer_churn_risk_daily: {n:,} linhas migradas!")


# ==============================================================================
# 15. financeiro_vencimentos_diaria
# ==============================================================================
def load_financeiro_vencimentos_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, tipo_titulo, "
            "valor_total, valor_pago, valor_aberto, updated_at "
            "FROM mart.financeiro_vencimentos_diaria "
            "ORDER BY id_empresa, data_key, id_filial, tipo_titulo"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":   _i(r["id_empresa"]),
            "id_filial":    _i(r["id_filial"]),
            "data_key":     _i(r["data_key"]),
            "tipo_titulo":  _i(r["tipo_titulo"]),
            "valor_total":  _f(r["valor_total"]),
            "valor_pago":   _f(r["valor_pago"]),
            "valor_aberto": _f(r["valor_aberto"]),
            "updated_at":   _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.financeiro_vencimentos_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "tipo_titulo"],
    )
    print(f"✅ financeiro_vencimentos_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 16. finance_aging_daily
#    data_gaps → Bool (psycopg retorna Python bool — ClickHouse connect aceita).
# ==============================================================================
def load_finance_aging_daily():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT dt_ref, id_empresa, id_filial, receber_total_aberto, "
            "receber_total_vencido, pagar_total_aberto, pagar_total_vencido, "
            "bucket_0_7, bucket_8_15, bucket_16_30, bucket_31_60, bucket_60_plus, "
            "top5_concentration_pct, data_gaps, updated_at "
            "FROM mart.finance_aging_daily "
            "ORDER BY dt_ref, id_empresa, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "dt_ref":                 r["dt_ref"],
            "id_empresa":             _i(r["id_empresa"]),
            "id_filial":              _i(r["id_filial"]),
            "receber_total_aberto":   _f(r["receber_total_aberto"]),
            "receber_total_vencido":  _f(r["receber_total_vencido"]),
            "pagar_total_aberto":     _f(r["pagar_total_aberto"]),
            "pagar_total_vencido":    _f(r["pagar_total_vencido"]),
            "bucket_0_7":             _f(r["bucket_0_7"]),
            "bucket_8_15":            _f(r["bucket_8_15"]),
            "bucket_16_30":           _f(r["bucket_16_30"]),
            "bucket_31_60":           _f(r["bucket_31_60"]),
            "bucket_60_plus":         _f(r["bucket_60_plus"]),
            "top5_concentration_pct": _f(r["top5_concentration_pct"]),
            "data_gaps":              _b(r["data_gaps"]),
            "updated_at":             _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.finance_aging_daily",
        rows_ch,
        order_by=["dt_ref", "id_empresa", "id_filial"],
    )
    print(f"✅ finance_aging_daily: {n:,} linhas migradas!")


# ==============================================================================
# 17. agg_pagamentos_diaria
# ==============================================================================
def load_agg_pagamentos_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, category, label, "
            "total_valor, qtd_comprovantes, share_percent, updated_at "
            "FROM mart.agg_pagamentos_diaria "
            "ORDER BY id_empresa, data_key, id_filial, category, label"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "category":         _s(r["category"]),
            "label":            _s(r["label"]),
            "total_valor":      _f(r["total_valor"]),
            "qtd_comprovantes": _i(r["qtd_comprovantes"]),
            "share_percent":    _f(r["share_percent"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_pagamentos_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "category", "label"],
    )
    print(f"✅ agg_pagamentos_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 18. agg_pagamentos_turno
# ==============================================================================
def load_agg_pagamentos_turno():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_turno, category, label, "
            "total_valor, qtd_comprovantes, updated_at "
            "FROM mart.agg_pagamentos_turno "
            "ORDER BY id_empresa, data_key, id_filial, id_turno, category, label"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "data_key":         _i(r["data_key"]),
            "id_turno":         _i(r["id_turno"]),
            "category":         _s(r["category"]),
            "label":            _s(r["label"]),
            "total_valor":      _f(r["total_valor"]),
            "qtd_comprovantes": _i(r["qtd_comprovantes"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_pagamentos_turno",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "id_turno", "category", "label"],
    )
    print(f"✅ agg_pagamentos_turno: {n:,} linhas migradas!")


# ==============================================================================
# 19. pagamentos_anomalias_diaria
#    Mapeamento PG → CH:
#      score (int/numeric)         → score (Decimal64)
#      impacto_estimado            → valor_total  (melhor correspondência semântica)
#      insight_id_hash (bigint)    → insight_id_hash (String) — forçar str()
#      reasons (jsonb)             → comprovantes_multiplos, comprovantes_total,
#                                    avg_formas  (extraídos do JSON quando disponível)
#      id_turno                    → Nullable(Int32) — manter None quando ausente
# ==============================================================================
def load_pagamentos_anomalias_diaria():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, data_key, id_turno, event_type, "
            "severity, score, impacto_estimado, reasons, "
            "insight_id_hash, updated_at "
            "FROM mart.pagamentos_anomalias_diaria "
            "ORDER BY id_empresa, data_key, id_filial, severity, score DESC"
        ).fetchall()
    rows_ch = []
    for r in rows_pg:
        reasons = r["reasons"] or {}
        if isinstance(reasons, str):
            try:
                reasons = json.loads(reasons)
            except Exception:
                reasons = {}
        rows_ch.append({
            "id_empresa":             _i(r["id_empresa"]),
            "id_filial":              _i(r["id_filial"]),
            "data_key":               _i(r["data_key"]),
            "id_turno":               int(r["id_turno"]) if r["id_turno"] is not None else None,
            "event_type":             _s(r["event_type"]),
            "severity":               _s(r["severity"]),
            "score":                  _f(r["score"]),
            "insight_id_hash":        str(r["insight_id_hash"]) if r["insight_id_hash"] is not None else "",
            "comprovantes_multiplos": int(reasons.get("comprovantes_multiplos") or 0),
            "comprovantes_total":     int(reasons.get("comprovantes_total") or 0),
            "valor_total":            _f(r["impacto_estimado"]),
            "avg_formas":             float(reasons.get("avg_formas_por_comprovante") or 0),
            "updated_at":             _dt(r["updated_at"]),
        })
    n = insert_batch(
        "torqmind_mart.pagamentos_anomalias_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial", "severity", "score"],
    )
    print(f"✅ pagamentos_anomalias_diaria: {n:,} linhas migradas!")


# ==============================================================================
# 20. agg_caixa_turno_aberto
#    fechamento_ts → Nullable(DateTime): manter None quando ausente.
# ==============================================================================
def load_agg_caixa_turno_aberto():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, filial_nome, id_turno, id_usuario, "
            "usuario_nome, abertura_ts, fechamento_ts, horas_aberto, severity, "
            "status_label, total_vendas, qtd_vendas, total_cancelamentos, "
            "qtd_cancelamentos, total_pagamentos, updated_at "
            "FROM mart.agg_caixa_turno_aberto "
            "ORDER BY id_empresa, id_filial, id_turno"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":          _i(r["id_empresa"]),
            "id_filial":           _i(r["id_filial"]),
            "filial_nome":         _s(r["filial_nome"]),
            "id_turno":            _i(r["id_turno"]),
            "id_usuario":          _i(r["id_usuario"]),
            "usuario_nome":        _s(r["usuario_nome"]),
            "abertura_ts":         _dt(r["abertura_ts"]),
            "fechamento_ts":       r["fechamento_ts"],
            "horas_aberto":        _f(r["horas_aberto"]),
            "severity":            _s(r["severity"]),
            "status_label":        _s(r["status_label"]),
            "total_vendas":        _f(r["total_vendas"]),
            "qtd_vendas":          _i(r["qtd_vendas"]),
            "total_cancelamentos": _f(r["total_cancelamentos"]),
            "qtd_cancelamentos":   _i(r["qtd_cancelamentos"]),
            "total_pagamentos":    _f(r["total_pagamentos"]),
            "updated_at":          _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_caixa_turno_aberto",
        rows_ch,
        order_by=["id_empresa", "id_filial", "id_turno"],
    )
    print(f"✅ agg_caixa_turno_aberto: {n:,} linhas migradas!")


# ==============================================================================
# 21. agg_caixa_forma_pagamento
#    forma_category não existe no mart PG; populado via CDC streaming do DW.
#    Backfill usa "" como placeholder — será sobrescrito pelo streaming CDC.
# ==============================================================================
def load_agg_caixa_forma_pagamento():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, id_turno, tipo_forma, forma_label, "
            "total_valor, qtd_comprovantes, updated_at "
            "FROM mart.agg_caixa_forma_pagamento "
            "ORDER BY id_empresa, id_filial, id_turno, tipo_forma"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":       _i(r["id_empresa"]),
            "id_filial":        _i(r["id_filial"]),
            "id_turno":         _i(r["id_turno"]),
            "tipo_forma":       _i(r["tipo_forma"]),
            "forma_label":      _s(r["forma_label"]),
            "forma_category":   "",
            "total_valor":      _f(r["total_valor"]),
            "qtd_comprovantes": _i(r["qtd_comprovantes"]),
            "updated_at":       _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_caixa_forma_pagamento",
        rows_ch,
        order_by=["id_empresa", "id_filial", "id_turno", "tipo_forma", "forma_category"],
    )
    print(f"✅ agg_caixa_forma_pagamento: {n:,} linhas migradas!")


# ==============================================================================
# 22. agg_caixa_cancelamentos
# ==============================================================================
def load_agg_caixa_cancelamentos():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, id_turno, filial_nome, "
            "total_cancelamentos, qtd_cancelamentos, updated_at "
            "FROM mart.agg_caixa_cancelamentos "
            "ORDER BY id_empresa, id_filial, id_turno"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":          _i(r["id_empresa"]),
            "id_filial":           _i(r["id_filial"]),
            "id_turno":            _i(r["id_turno"]),
            "filial_nome":         _s(r["filial_nome"]),
            "total_cancelamentos": _f(r["total_cancelamentos"]),
            "qtd_cancelamentos":   _i(r["qtd_cancelamentos"]),
            "updated_at":          _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.agg_caixa_cancelamentos",
        rows_ch,
        order_by=["id_empresa", "id_filial", "id_turno"],
    )
    print(f"✅ agg_caixa_cancelamentos: {n:,} linhas migradas!")


# ==============================================================================
# 23. alerta_caixa_aberto
#    PG usa coluna "url"; CH usa "action_url" — mapeado explicitamente.
#    insight_id_hash (bigint) não existe no CH — descartado.
# ==============================================================================
def load_alerta_caixa_aberto():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT id_empresa, id_filial, filial_nome, id_turno, id_usuario, "
            "usuario_nome, abertura_ts, horas_aberto, severity, title, body, "
            "url, updated_at "
            "FROM mart.alerta_caixa_aberto "
            "ORDER BY id_empresa, id_filial, severity, abertura_ts"
        ).fetchall()
    rows_ch = [
        {
            "id_empresa":   _i(r["id_empresa"]),
            "id_filial":    _i(r["id_filial"]),
            "filial_nome":  _s(r["filial_nome"]),
            "id_turno":     _i(r["id_turno"]),
            "id_usuario":   _i(r["id_usuario"]),
            "usuario_nome": _s(r["usuario_nome"]),
            "abertura_ts":  _dt(r["abertura_ts"]),
            "horas_aberto": _f(r["horas_aberto"]),
            "severity":     _s(r["severity"]),
            "title":        _s(r["title"]),
            "body":         _s(r["body"]),
            "action_url":   _s(r["url"]),
            "updated_at":   _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.alerta_caixa_aberto",
        rows_ch,
        order_by=["id_empresa", "id_filial", "severity", "abertura_ts"],
    )
    print(f"✅ alerta_caixa_aberto: {n:,} linhas migradas!")


# ==============================================================================
# 24. anonymous_retention_daily
#    details → jsonb no PG, String no CH.
# ==============================================================================
def load_anonymous_retention_daily():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT dt_ref, id_empresa, id_filial, anon_faturamento_7d, "
            "anon_faturamento_prev_28d, trend_pct, anon_share_pct_7d, "
            "repeat_proxy_idx, impact_estimated_7d, details, updated_at "
            "FROM mart.anonymous_retention_daily "
            "ORDER BY dt_ref, id_empresa, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "dt_ref":                    r["dt_ref"],
            "id_empresa":                _i(r["id_empresa"]),
            "id_filial":                 _i(r["id_filial"]),
            "anon_faturamento_7d":       _f(r["anon_faturamento_7d"]),
            "anon_faturamento_prev_28d": _f(r["anon_faturamento_prev_28d"]),
            "trend_pct":                 _f(r["trend_pct"]),
            "anon_share_pct_7d":         _f(r["anon_share_pct_7d"]),
            "repeat_proxy_idx":          _f(r["repeat_proxy_idx"]),
            "impact_estimated_7d":       _f(r["impact_estimated_7d"]),
            "details":                   _j(r["details"]),
            "updated_at":                _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.anonymous_retention_daily",
        rows_ch,
        order_by=["dt_ref", "id_empresa", "id_filial"],
    )
    print(f"✅ anonymous_retention_daily: {n:,} linhas migradas!")


# ==============================================================================
# 25. health_score_daily
# ==============================================================================
def load_health_score_daily():
    with get_conn() as conn:
        rows_pg = conn.execute(
            "SELECT dt_ref, id_empresa, id_filial, comp_margem, comp_fraude, "
            "comp_churn, comp_finance, comp_operacao, comp_dados, score_total, "
            "updated_at "
            "FROM mart.health_score_daily "
            "ORDER BY dt_ref, id_empresa, id_filial"
        ).fetchall()
    rows_ch = [
        {
            "dt_ref":            r["dt_ref"],
            "id_empresa":        _i(r["id_empresa"]),
            "id_filial":         _i(r["id_filial"]),
            "fat_30d":           0.0,
            "margem_30d":        0.0,
            "ticket_30d":        0.0,
            "high_risk_30d":     0,
            "total_risk_30d":    0,
            "impacto_risco_30d": 0.0,
            "health_pct":        _f(r["comp_margem"]),
            "customer_pct":      _f(r["comp_churn"]),
            "risk_pct":          _f(r["comp_fraude"]),
            "final_score":       _f(r["score_total"]),
            "updated_at":        _dt(r["updated_at"]),
        }
        for r in rows_pg
    ]
    n = insert_batch(
        "torqmind_mart.health_score_daily",
        rows_ch,
        order_by=["dt_ref", "id_empresa", "id_filial"],
    )
    print(f"✅ health_score_daily: {n:,} linhas migradas!")


# ==============================================================================
# ENTRY POINT — execução sequencial das 25 tabelas
# ==============================================================================
if __name__ == "__main__":
    print("")
    print(">>> Iniciando migração sequencial das 25 tabelas...")
    print("")

    load_agg_vendas_diaria()
    load_agg_vendas_hora()
    load_agg_produtos_diaria()
    load_agg_grupos_diaria()
    load_agg_funcionarios_diaria()
    load_insights_base_diaria()
    load_fraude_cancelamentos_diaria()
    load_fraude_cancelamentos_eventos()
    load_agg_risco_diaria()
    load_risco_top_funcionarios_diaria()
    load_risco_turno_local_diaria()
    load_clientes_churn_risco()
    load_customer_rfm_daily()
    load_customer_churn_risk_daily()
    load_financeiro_vencimentos_diaria()
    load_finance_aging_daily()
    load_agg_pagamentos_diaria()
    load_agg_pagamentos_turno()
    load_pagamentos_anomalias_diaria()
    load_agg_caixa_turno_aberto()
    load_agg_caixa_forma_pagamento()
    load_agg_caixa_cancelamentos()
    load_alerta_caixa_aberto()
    load_anonymous_retention_daily()
    load_health_score_daily()

    print("")
    print("================================================================")
    print("Todas as 25 tabelas migradas com sucesso!")
    print("================================================================")

PYTHON_EOF

EXIT_CODE=$?
echo ""
echo "================================================================"
echo "Fim: $(date '+%Y-%m-%d %H:%M:%S')"
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "Script finalizado com sucesso (exit 0)"
else
    echo "Script finalizado com ERRO (exit $EXIT_CODE)" >&2
    exit "$EXIT_CODE"
fi
echo "================================================================"
