from __future__ import annotations

"""Repositories (SQL access) for MART/DW.

PT-BR: Este módulo concentra queries de leitura para dashboards.
EN   : This module centralizes read queries for dashboards.

Design:
- Prefer reading from `mart.*` (materialized views) for performance.
- When something is not in MART yet, we read from `dw.*` facts/dims.
"""

from datetime import date, timedelta
from typing import Optional, List, Dict, Any

from app.db import get_conn


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def list_filiais(role: str, id_empresa: int) -> List[Dict[str, Any]]:
    sql = """
      SELECT id_filial, nome
      FROM auth.filiais
      WHERE id_empresa = %s AND is_active = true
      ORDER BY id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        return list(conn.execute(sql, (id_empresa,)).fetchall())


# ========================
# Dashboard (existing)
# ========================

def dashboard_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT
        COALESCE(SUM(faturamento),0) AS faturamento,
        COALESCE(SUM(margem),0) AS margem,
        COALESCE(AVG(ticket_medio),0) AS ticket_medio,
        COALESCE(SUM(quantidade_itens),0) AS itens
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"faturamento": 0, "margem": 0, "ticket_medio": 0, "itens": 0}


def dashboard_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT data_key, id_filial, faturamento, margem
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def insights_base(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT data_key, id_filial, faturamento_dia, faturamento_mes_acum, comparativo_mes_anterior
      FROM mart.insights_base_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Vendas & Stores
# ========================

def sales_by_hour(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT data_key, id_filial, hora, faturamento, margem, vendas
      FROM mart.agg_vendas_hora
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, hora
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def sales_top_products(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
    sql = f"""
      SELECT
        id_produto,
        MAX(produto_nome) AS produto_nome,
        SUM(faturamento) AS faturamento,
        SUM(margem) AS margem,
        SUM(qtd) AS qtd
      FROM mart.agg_produtos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY id_produto
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def sales_top_groups(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
    sql = f"""
      SELECT
        id_grupo_produto,
        MAX(grupo_nome) AS grupo_nome,
        SUM(faturamento) AS faturamento,
        SUM(margem) AS margem
      FROM mart.agg_grupos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY id_grupo_produto
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def sales_top_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
    sql = f"""
      SELECT
        id_funcionario,
        MAX(funcionario_nome) AS funcionario_nome,
        SUM(faturamento) AS faturamento,
        SUM(margem) AS margem,
        SUM(vendas)::int AS vendas
      FROM mart.agg_funcionarios_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY id_funcionario
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Anti-fraude
# ========================

def fraud_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT
        COALESCE(SUM(cancelamentos),0)::int AS cancelamentos,
        COALESCE(SUM(valor_cancelado),0)::numeric(18,2) AS valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"cancelamentos": 0, "valor_cancelado": 0}


def fraud_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT data_key, id_filial, cancelamentos, valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def fraud_last_events(role: str, id_empresa: int, id_filial: Optional[int], limit: int = 30) -> List[Dict[str, Any]]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT id_filial, id_db, id_comprovante, data, id_usuario, id_turno, valor_total
      FROM mart.fraude_cancelamentos_eventos
      WHERE id_empresa = %s
      {where_filial}
      ORDER BY data DESC NULLS LAST
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def fraud_top_users(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT
        id_usuario,
        COUNT(*)::int AS cancelamentos,
        COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_cancelado
      FROM dw.fact_comprovante
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        AND cancelado = true
        {where_filial}
      GROUP BY id_usuario
      ORDER BY cancelamentos DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Clientes
# ========================

def customers_top(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    """Top customers by revenue for the selected period."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)

    where_filial = "" if id_filial is None else "AND v.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT
        COALESCE(v.id_cliente, -1) AS id_cliente,
        COALESCE(dc.nome, '(Sem cliente)') AS cliente_nome,
        COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
        COALESCE(COUNT(DISTINCT v.id_comprovante),0)::int AS compras,
        MAX(v.data) AS ultima_compra,
        CASE WHEN COUNT(DISTINCT v.id_comprovante)=0 THEN 0
             ELSE (SUM(i.total)/COUNT(DISTINCT v.id_comprovante))::numeric(18,2)
        END AS ticket_medio
      FROM dw.fact_venda v
      JOIN dw.fact_venda_item i
        ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
      LEFT JOIN dw.dim_cliente dc
        ON dc.id_empresa=v.id_empresa AND dc.id_filial=v.id_filial AND dc.id_cliente=v.id_cliente
      WHERE v.id_empresa = %s
        AND v.data_key BETWEEN %s AND %s
        AND COALESCE(v.cancelado,false) = false
        AND COALESCE(i.cfop,0) >= 5000
        {where_filial}
      GROUP BY COALESCE(v.id_cliente,-1), COALESCE(dc.nome,'(Sem cliente)')
      ORDER BY faturamento DESC
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def customers_rfm_snapshot(role: str, id_empresa: int, id_filial: Optional[int], as_of: date) -> Dict[str, Any]:
    """Very lightweight RFM-like snapshot for *today* (rule-based, no ML yet)."""

    # Last 90 days window
    dt_ini = as_of - timedelta(days=90)
    ini = _date_key(dt_ini)
    fim = _date_key(as_of)

    where_filial = "" if id_filial is None else "AND v.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      WITH base AS (
        SELECT
          COALESCE(v.id_cliente, -1) AS id_cliente,
          MAX(v.data)::date AS last_purchase,
          COUNT(DISTINCT v.id_comprovante)::int AS freq,
          SUM(i.total)::numeric(18,2) AS monetary
        FROM dw.fact_venda v
        JOIN dw.fact_venda_item i
          ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
        WHERE v.id_empresa = %s
          AND v.data_key BETWEEN %s AND %s
          AND COALESCE(v.cancelado,false) = false
          AND COALESCE(i.cfop,0) >= 5000
          {where_filial}
        GROUP BY COALESCE(v.id_cliente, -1)
      )
      SELECT
        COUNT(*) FILTER (WHERE id_cliente <> -1)::int AS clientes_identificados,
        COUNT(*) FILTER (WHERE last_purchase >= (%s::date - interval '7 days'))::int AS ativos_7d,
        COUNT(*) FILTER (WHERE last_purchase < (%s::date - interval '30 days'))::int AS em_risco_30d,
        COALESCE(SUM(monetary),0)::numeric(18,2) AS faturamento_90d
      FROM base
    """

    params2 = params + [as_of, as_of]
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params2).fetchone()
        return row or {
            "clientes_identificados": 0,
            "ativos_7d": 0,
            "em_risco_30d": 0,
            "faturamento_90d": 0,
        }


# ========================
# Financeiro
# ========================

def finance_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    """Finance KPIs by due date (vencimento) within the selected range."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)

    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    # tipo_titulo: 0 pagar, 1 receber
    sql = f"""
      SELECT
        COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_total ELSE 0 END),0)::numeric(18,2) AS receber_total,
        COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_pago  ELSE 0 END),0)::numeric(18,2) AS receber_pago,
        COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS receber_aberto,

        COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_total ELSE 0 END),0)::numeric(18,2) AS pagar_total,
        COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_pago  ELSE 0 END),0)::numeric(18,2) AS pagar_pago,
        COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS pagar_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {
            "receber_total": 0,
            "receber_pago": 0,
            "receber_aberto": 0,
            "pagar_total": 0,
            "pagar_pago": 0,
            "pagar_aberto": 0,
        }


def finance_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)

    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT data_key, id_filial, tipo_titulo, valor_total, valor_pago, valor_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, tipo_titulo
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Metas & Equipe
# ========================

def goals_today(role: str, id_empresa: int, id_filial: int, goal_date: date) -> List[Dict[str, Any]]:
    """Goals configured for a given date (branch)."""

    sql = """
      SELECT goal_type, target_value
      FROM app.goals
      WHERE id_empresa = %s AND id_filial = %s AND goal_date = %s
      ORDER BY goal_type
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, (id_empresa, id_filial, goal_date)).fetchall())


def leaderboard_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    """Employee leaderboard for gamification."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT
        id_funcionario,
        MAX(funcionario_nome) AS funcionario_nome,
        SUM(faturamento)::numeric(18,2) AS faturamento,
        SUM(margem)::numeric(18,2) AS margem,
        SUM(vendas)::int AS vendas
      FROM mart.agg_funcionarios_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY id_funcionario
      ORDER BY faturamento DESC
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Jarvis (rule-based briefing)
# ========================

def jarvis_briefing(role: str, id_empresa: int, id_filial: Optional[int], dt_ref: date) -> Dict[str, Any]:
    """Return a short executive briefing.

    PT-BR: Ainda não é LLM/ML (isso vem depois). Aqui já entregamos inteligência operacional
    com regras simples e objetivas.
    """

    d0 = dt_ref
    d1 = dt_ref - timedelta(days=1)

    k0 = _date_key(d0)
    k1 = _date_key(d1)

    where_filial = "" if id_filial is None else "AND id_filial = %s"

    # Revenue day 0/1
    sql_rev = f"""
      SELECT data_key, COALESCE(SUM(faturamento),0)::numeric(18,2) AS faturamento,
             COALESCE(SUM(margem),0)::numeric(18,2) AS margem
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key IN (%s,%s)
      {where_filial}
      GROUP BY data_key
    """
    params_rev = [id_empresa, k0, k1] + ([] if id_filial is None else [id_filial])

    # Fraud cancellations day 0/1
    sql_can = f"""
      SELECT data_key, COALESCE(SUM(cancelamentos),0)::int AS cancelamentos,
             COALESCE(SUM(valor_cancelado),0)::numeric(18,2) AS valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key IN (%s,%s)
      {where_filial}
      GROUP BY data_key
    """
    params_can = [id_empresa, k0, k1] + ([] if id_filial is None else [id_filial])

    # Open receivables overdue (any day < dt_ref)
    sql_overdue = f"""
      SELECT COALESCE(SUM(valor_aberto),0)::numeric(18,2) AS receber_vencido_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s
        AND tipo_titulo = 1
        AND data_key < %s
        {where_filial}
    """
    params_overdue = [id_empresa, k0] + ([] if id_filial is None else [id_filial])

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rev_rows = conn.execute(sql_rev, params_rev).fetchall()
        can_rows = conn.execute(sql_can, params_can).fetchall()
        overdue = conn.execute(sql_overdue, params_overdue).fetchone() or {"receber_vencido_aberto": 0}

    rev_map = {r["data_key"]: r for r in rev_rows}
    can_map = {r["data_key"]: r for r in can_rows}

    f0 = float(rev_map.get(k0, {}).get("faturamento", 0) or 0)
    f1 = float(rev_map.get(k1, {}).get("faturamento", 0) or 0)
    m0 = float(rev_map.get(k0, {}).get("margem", 0) or 0)
    m1 = float(rev_map.get(k1, {}).get("margem", 0) or 0)

    c0 = int(can_map.get(k0, {}).get("cancelamentos", 0) or 0)
    c1 = int(can_map.get(k1, {}).get("cancelamentos", 0) or 0)
    cv0 = float(can_map.get(k0, {}).get("valor_cancelado", 0) or 0)
    cv1 = float(can_map.get(k1, {}).get("valor_cancelado", 0) or 0)

    receber_vencido_aberto = float(overdue.get("receber_vencido_aberto", 0) or 0)

    delta_f = f0 - f1
    delta_m = m0 - m1

    bullets: List[str] = []

    # Simple, high-impact heuristics
    if f1 > 0 and delta_f / f1 <= -0.08:
        bullets.append(f"📉 Faturamento caiu {abs(delta_f):,.2f} vs ontem. Ação: validar preço x concorrência e ruptura de bombas/loja.")
    elif delta_f > 0:
        bullets.append(f"📈 Faturamento subiu {delta_f:,.2f} vs ontem. Ação: replicar condições (preço/promo/escala) nas demais filiais.")

    if m0 < 0 and f0 > 0:
        bullets.append("⚠️ Margem negativa no dia. Ação: checar custo médio (cadastro de produtos) e descontos/erros de preço.")
    elif f0 > 0 and (m0 / f0) < 0.05:
        bullets.append("⚠️ Margem baixa (<5%). Ação: revisar mix (loja vs combustível), descontos e condições com fornecedores.")

    if c0 > max(3, int(c1 * 1.5)):
        bullets.append(f"🧨 Cancelamentos altos hoje ({c0}). Ação: auditar operador/turno e ativar alerta Telegram para o dono.")

    if receber_vencido_aberto > 0:
        bullets.append(f"💰 Recebíveis vencidos em aberto: {receber_vencido_aberto:,.2f}. Ação: cobrança ativa + renegociação (reduz churn e inadimplência).")

    if not bullets:
        bullets.append("✅ Operação dentro do esperado para o período selecionado. Ação: foque em aumentar ticket na loja e reduzir cancelamentos.")

    return {
        "data_ref": d0.isoformat(),
        "kpis": {
            "faturamento": f0,
            "margem": m0,
            "cancelamentos": c0,
            "valor_cancelado": cv0,
        },
        "comparativo": {
            "faturamento_vs_ontem": delta_f,
            "margem_vs_ontem": delta_m,
            "cancelamentos_vs_ontem": c0 - c1,
        },
        "bullets": bullets,
    }
