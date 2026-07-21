"""Profit Management (Gestão de Lucro) API routes.

Provides endpoints for the premium profit management module including:
- Overview/KPIs
- DRE Gerencial
- Expense breakdown
- Product margin grid with repricing simulation
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app import repos_mart
from app.business_time import resolve_business_date
from app.db import get_conn
from app.db_clickhouse import query_dict, query_scalar
from app.deps import get_current_claims
from app.permissions import require_screen, redact_sensitive, can_view_sensitive_financials
from app.scope import resolve_scope_filters

router = APIRouter(prefix="/bi/profit-management", tags=["profit-management"])
logger = logging.getLogger(__name__)


def _despesas_pg_por_regime(
    id_empresa: int,
    branch_ids: List[int],
    ano_mes: int,
    *,
    regime_caixa: bool,
) -> Dict[str, Any]:
    """Agrega dw.fact_despesa_operacional por competência (DTAVCTO) ou caixa (DTAPGTO).

    Retorna buckets alinhados ao DRE (pessoal/comercial/…) + breakdown por
    classificacao_gerencial para a aba Despesas. Fonte: Postgres (não CH).
    """
    if regime_caixa:
        mes_filter = """
          AND dt_pagamento IS NOT NULL
          AND (EXTRACT(YEAR FROM dt_pagamento)::int * 100
               + EXTRACT(MONTH FROM dt_pagamento)::int) = %s
        """
        valor_expr = "COALESCE(NULLIF(vlr_pago, 0), valor)"
    else:
        mes_filter = "AND ano_mes_competencia = %s"
        valor_expr = "valor"

    params: List[Any] = [id_empresa, int(ano_mes)]
    if len(branch_ids) == 1:
        branch_sql = "AND id_filial = %s"
        params.append(int(branch_ids[0]))
    else:
        branch_sql = "AND id_filial = ANY(%s)"
        params.append([int(b) for b in branch_ids])

    sql_buckets = f"""
        SELECT
          COALESCE(SUM(CASE WHEN classificacao_gerencial = 'pessoal'
                            THEN {valor_expr} ELSE 0 END), 0)::float AS desp_pessoal,
          COALESCE(SUM(CASE WHEN classificacao_gerencial = 'comercial'
                            THEN {valor_expr} ELSE 0 END), 0)::float AS desp_comercial,
          COALESCE(SUM(CASE WHEN classificacao_gerencial = 'administrativo'
                            THEN {valor_expr} ELSE 0 END), 0)::float AS desp_administrativa,
          COALESCE(SUM(CASE WHEN classificacao_gerencial = 'financeiro'
                            THEN {valor_expr} ELSE 0 END), 0)::float AS desp_financeira,
          COALESCE(SUM(CASE WHEN COALESCE(is_tributo_operacional, false)
                              OR classificacao_gerencial = 'tributos'
                            THEN {valor_expr} ELSE 0 END), 0)::float AS desp_tributaria_operacional,
          COALESCE(SUM(CASE WHEN COALESCE(is_excepcional, false)
                              OR classificacao_gerencial IN ('excepcional', 'perdas')
                            THEN {valor_expr} ELSE 0 END), 0)::float AS desp_excepcional,
          COALESCE(SUM({valor_expr}), 0)::float AS desp_operacional_total,
          COUNT(*)::int AS qtd_lancamentos
        FROM dw.fact_despesa_operacional
        WHERE id_empresa = %s
          AND COALESCE(entra_dre, true)
          {mes_filter}
          {branch_sql}
    """
    sql_cat = f"""
        SELECT
          COALESCE(NULLIF(classificacao_gerencial, ''), 'nao_classificado') AS classificacao_gerencial,
          SUM({valor_expr})::float AS valor_total,
          COUNT(*)::int AS qtd_lancamentos,
          SUM(CASE WHEN tipo_conta = 0 THEN {valor_expr} ELSE 0 END)::float AS valor_tipo_0,
          SUM(CASE WHEN tipo_conta = 1 THEN {valor_expr} ELSE 0 END)::float AS valor_tipo_1,
          SUM(CASE WHEN COALESCE(entra_rateio_produto, false) THEN {valor_expr} ELSE 0 END)::float AS valor_rateavel,
          SUM(CASE WHEN NOT COALESCE(entra_rateio_produto, false) THEN {valor_expr} ELSE 0 END)::float AS valor_nao_rateavel
        FROM dw.fact_despesa_operacional
        WHERE id_empresa = %s
          AND COALESCE(entra_dre, true)
          {mes_filter}
          {branch_sql}
        GROUP BY 1
        ORDER BY valor_total DESC
    """
    sql_top = f"""
        SELECT
          COALESCE(codigo_plano, '') AS codigo_plano,
          COALESCE(nome_plano, '') AS nome_plano,
          COALESCE(NULLIF(classificacao_gerencial, ''), 'nao_classificado') AS classificacao_gerencial,
          SUM({valor_expr})::float AS valor_total,
          COUNT(*)::int AS qtd_lancamentos
        FROM dw.fact_despesa_operacional
        WHERE id_empresa = %s
          AND COALESCE(entra_dre, true)
          {mes_filter}
          {branch_sql}
        GROUP BY 1, 2, 3
        ORDER BY valor_total DESC
        LIMIT 20
    """
    with get_conn(role="MASTER", tenant_id=id_empresa) as conn:
        buckets = dict(conn.execute(sql_buckets, params).fetchone() or {})
        categorias = [dict(r) for r in conn.execute(sql_cat, params).fetchall()]
        top = [dict(r) for r in conn.execute(sql_top, params).fetchall()]
    return {"buckets": buckets, "categorias": categorias, "top": top}

# Default desired margins by sector
DEFAULT_MARGINS = {
    "conveniencia": 0.30,
    "automotivo": 0.30,
    "cigarro": 0.12,
    "combustivel": 0.08,
    "servico": 0.20,
    "outros": 0.25,
}


def _extract_profit_scope(
    claims: dict,
    id_empresa_q: Optional[int] = None,
    id_filial_q: Optional[int] = None,
    id_filiais_q: Optional[List[int]] = None,
) -> tuple:
    """Resolve tenant and branch list using the standard scope system.

    Returns (tenant_id, branch_ids) where branch_ids is a list of ints.
    For multi-branch/all: aggregates across branches.
    For single branch: list with one element.
    """
    tenant_id, branch_scope, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa_q, id_filial_q=id_filial_q, id_filiais_q=id_filiais_q,
    )
    if isinstance(branch_scope, list):
        return tenant_id, [int(b) for b in branch_scope if b]
    if isinstance(branch_scope, int) and branch_scope > 0:
        return tenant_id, [branch_scope]
    return tenant_id, []


def _branch_filter_sql(branch_ids: List[int], params: dict) -> str:
    """Build a ClickHouse WHERE clause fragment for branch filtering."""
    if len(branch_ids) == 1:
        params["id_filial"] = branch_ids[0]
        return "AND id_filial = %(id_filial)s"
    params["branch_ids"] = branch_ids
    return "AND id_filial IN %(branch_ids)s"


def _resolve_reference_month(id_empresa: int, branch_ids: List[int]) -> Optional[int]:
    """Find the latest closed month with sufficient data across branches."""
    params: Dict[str, Any] = {"id_empresa": id_empresa}
    branch_clause = _branch_filter_sql(branch_ids, params)
    sql = f"""
        SELECT ano_mes
        FROM torqmind_mart_rt.profit_dre_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          {branch_clause}
          AND receita_bruta_total > 0
        ORDER BY ano_mes DESC
        LIMIT 1
    """
    result = query_scalar(sql, params)
    return int(result) if result else None


def _parse_ano_mes_query(ano_mes: Optional[int]) -> Optional[int]:
    if ano_mes is None:
        return None
    ym = int(ano_mes)
    mm = ym % 100
    if ym >= 190001 and 1 <= mm <= 12:
        return ym
    return None


def _resolve_product_month(
    id_empresa: int,
    branch_ids: List[int],
    ano_mes_q: Optional[int] = None,
) -> Optional[int]:
    """Mês para Produtos/Oportunidades: query explícita, senão último mês com linhas no mart de produto.

    Não usar só o mês do DRE — refresh de produto pode atrasar e a aba fica vazia
    enquanto DRE/despesas já avançaram.
    """
    explicit = _parse_ano_mes_query(ano_mes_q)
    if explicit is not None:
        return explicit

    params: Dict[str, Any] = {"id_empresa": id_empresa}
    branch_clause = _branch_filter_sql(branch_ids, params)
    sql = f"""
        SELECT ano_mes
        FROM torqmind_mart_rt.profit_produto_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          {branch_clause}
          AND receita > 0
        ORDER BY ano_mes DESC
        LIMIT 1
    """
    result = query_scalar(sql, params)
    if result:
        return int(result)
    return _resolve_reference_month(id_empresa, branch_ids)


def _previous_month(ano_mes: int) -> int:
    year = ano_mes // 100
    month = ano_mes % 100
    if month == 1:
        return (year - 1) * 100 + 12
    return year * 100 + (month - 1)


def _format_month_label(ano_mes: int) -> str:
    months = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
              "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    year = ano_mes // 100
    month = ano_mes % 100
    return f"{months[month]}/{year}" if 1 <= month <= 12 else str(ano_mes)


@router.get("/metadata")
def profit_metadata(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Return metadata about the profit module: reference month, freshness."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"available": False, "message": "Nenhuma filial selecionada."}

    ref_month = _resolve_reference_month(tenant_id, branch_ids)
    if not ref_month:
        return {
            "available": False,
            "message": "Ainda não há dados suficientes para calcular o Lucro Gerencial Estimado.",
        }

    is_multi = len(branch_ids) > 1
    return {
        "available": True,
        "id_empresa": tenant_id,
        "id_filial": branch_ids[0] if not is_multi else None,
        "filiais": branch_ids if is_multi else None,
        "consolidado": is_multi,
        "ano_mes_referencia": ref_month,
        "mes_referencia_label": _format_month_label(ref_month),
        "mes_anterior": _previous_month(ref_month),
    }


@router.get("/overview")
def profit_overview(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.overview")),
):
    """Executive overview with KPIs for the profit management module."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None, "message": "Nenhuma filial selecionada."}

    ref_month = _resolve_reference_month(tenant_id, branch_ids)
    if not ref_month:
        return {
            "data": None,
            "message": "Ainda não há dados suficientes para calcular o Lucro Gerencial Estimado.",
        }

    params: Dict[str, Any] = {"id_empresa": tenant_id, "ano_mes": ref_month}
    branch_clause = _branch_filter_sql(branch_ids, params)

    # DRE summary — aggregate across branches
    dre_sql = f"""
        SELECT
            SUM(receita_bruta_total) AS receita_bruta_total,
            SUM(impostos_sobre_vendas) AS impostos_sobre_vendas,
            SUM(receita_liquida_gerencial) AS receita_liquida_gerencial,
            SUM(cmv_total) AS cmv_total,
            SUM(margem_bruta) AS margem_bruta,
            SUM(desp_pessoal) AS desp_pessoal,
            SUM(desp_comercial) AS desp_comercial,
            SUM(desp_administrativa) AS desp_administrativa,
            SUM(desp_financeira) AS desp_financeira,
            SUM(desp_tributaria_operacional) AS desp_tributaria_operacional,
            SUM(desp_excepcional) AS desp_excepcional,
            SUM(desp_operacional_total) AS desp_operacional_total,
            SUM(lucro_gerencial_estimado) AS lucro_gerencial_estimado,
            SUM(receita_pista) AS receita_pista,
            SUM(receita_conveniencia) AS receita_conveniencia,
            SUM(cmv_pista) AS cmv_pista,
            SUM(cmv_conveniencia) AS cmv_conveniencia,
            MAX(updated_at) AS updated_at
        FROM torqmind_mart_rt.profit_dre_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          {branch_clause}
          AND ano_mes = %(ano_mes)s
    """
    dre_rows = query_dict(dre_sql, params)
    dre = dre_rows[0] if dre_rows else {}

    # Resumo — aggregate across branches
    resumo_sql = f"""
        SELECT
            SUM(impacto_positivo_60d) AS impacto_positivo_60d,
            SUM(produtos_com_reajuste) AS produtos_com_reajuste,
            SUM(produtos_abaixo_minimo) AS produtos_abaixo_minimo
        FROM torqmind_mart_rt.profit_resumo_filial FINAL
        WHERE id_empresa = %(id_empresa)s
          {branch_clause}
          AND ano_mes_referencia = %(ano_mes)s
    """
    resumo_rows = query_dict(resumo_sql, params)
    resumo = resumo_rows[0] if resumo_rows else {}

    receita = float(dre.get("receita_bruta_total", 0))
    lucro = float(dre.get("lucro_gerencial_estimado", 0))
    desp = float(dre.get("desp_operacional_total", 0))

    is_multi = len(branch_ids) > 1
    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes_referencia": ref_month,
        "id_filial": branch_ids[0] if not is_multi else None,
        "consolidado": is_multi,
        "filiais_count": len(branch_ids) if is_multi else None,
        "kpis": {
            "lucro_gerencial_estimado": lucro,
            "margem_gerencial_pct": lucro / receita if receita > 0 else 0,
            "receita_bruta": receita,
            "desp_operacional_total": desp,
            "desp_sobre_receita_pct": desp / receita if receita > 0 else 0,
            "impacto_positivo_60d": float(resumo.get("impacto_positivo_60d", 0)),
            "produtos_com_reajuste": int(resumo.get("produtos_com_reajuste", 0)),
            "produtos_abaixo_minimo": int(resumo.get("produtos_abaixo_minimo", 0)),
        },
        "freshness": str(dre.get("updated_at", "")),
        "disclaimer": "Estimativa gerencial baseada em vendas, custo no momento da venda, despesas por vencimento e rateio proporcional. Não representa lucro contábil/fiscal oficial.",
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/dre")
def profit_dre(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    ano_mes: Optional[int] = Query(
        None,
        description="Mês fechado YYYYMM. Default = mês corrente (America/Sao_Paulo).",
    ),
    regime_caixa: bool = Query(
        True,
        description="Quando true, prioriza despesas pelo pagamento (regime de caixa Xpert)",
    ),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.overview")),
):
    """DRE Gerencial Resumida."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    # Mês do seletor (não o "último mês com receita" do CH).
    ref_month = _solvencia_target_month(ano_mes, tenant_id)

    params: Dict[str, Any] = {"id_empresa": tenant_id, "ano_mes": ref_month}
    branch_clause = _branch_filter_sql(branch_ids, params)

    dre_sql = f"""
        SELECT
            SUM(receita_bruta_total) AS receita_bruta_total,
            SUM(impostos_sobre_vendas) AS impostos_sobre_vendas,
            SUM(receita_liquida_gerencial) AS receita_liquida_gerencial,
            SUM(cmv_total) AS cmv_total,
            SUM(margem_bruta) AS margem_bruta,
            SUM(desp_pessoal) AS desp_pessoal,
            SUM(desp_comercial) AS desp_comercial,
            SUM(desp_administrativa) AS desp_administrativa,
            SUM(desp_financeira) AS desp_financeira,
            SUM(desp_tributaria_operacional) AS desp_tributaria_operacional,
            SUM(desp_excepcional) AS desp_excepcional,
            SUM(desp_operacional_total) AS desp_operacional_total,
            SUM(lucro_gerencial_estimado) AS lucro_gerencial_estimado,
            SUM(receita_pista) AS receita_pista,
            SUM(receita_conveniencia) AS receita_conveniencia,
            SUM(receita_automotivo) AS receita_automotivo,
            SUM(receita_cigarro) AS receita_cigarro,
            SUM(receita_servico) AS receita_servico,
            SUM(cmv_pista) AS cmv_pista,
            SUM(cmv_conveniencia) AS cmv_conveniencia
        FROM torqmind_mart_rt.profit_dre_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          {branch_clause}
          AND ano_mes = %(ano_mes)s
    """
    rows = query_dict(dre_sql, params)
    dre = rows[0] if rows else {}

    def f(key):
        return float(dre.get(key, 0) or 0)

    # Despesas: CH-first (profit_despesas_mensal). PG só se CH vazio (legado/regime).
    desp = {
        "desp_pessoal": f("desp_pessoal"),
        "desp_comercial": f("desp_comercial"),
        "desp_administrativa": f("desp_administrativa"),
        "desp_financeira": f("desp_financeira"),
        "desp_tributaria_operacional": f("desp_tributaria_operacional"),
        "desp_excepcional": f("desp_excepcional"),
        "desp_operacional_total": f("desp_operacional_total"),
    }
    desp_source = "clickhouse"
    ch_desp_total = float(desp["desp_operacional_total"] or 0)
    if ch_desp_total <= 0:
        try:
            pg = _despesas_pg_por_regime(
                tenant_id, branch_ids, ref_month, regime_caixa=bool(regime_caixa),
            )
            b = pg["buckets"]
            if int(b.get("qtd_lancamentos") or 0) > 0 or float(b.get("desp_operacional_total") or 0) > 0:
                desp = {k: float(b.get(k) or 0) for k in desp}
                desp_source = "postgres_caixa" if regime_caixa else "postgres_competencia"
        except Exception as exc:
            logger.warning("profit_dre: falha despesas PG fallback (empresa=%s mes=%s): %s", tenant_id, ref_month, exc)

    receita = f("receita_bruta_total")
    margem = f("margem_bruta")
    lucro = round(margem - float(desp["desp_operacional_total"]), 2)

    if regime_caixa:
        disclaimer = (
            "Despesas do Demonstrativo Xpert: MOVLCTOS por DTACONTA (nível 3 do plano). "
            "Receita/CMV TorqMind. Não é lucro contábil/fiscal oficial."
        )
    else:
        disclaimer = (
            "Despesas do Demonstrativo Xpert: MOVLCTOS por DTACONTA (nível 3 do plano). "
            "Lucro gerencial estimado = margem bruta − despesas. Não é lucro contábil/fiscal oficial."
        )

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes": ref_month,
        "regime_caixa": bool(regime_caixa),
        "despesas_source": desp_source,
        "linhas": [
            {"label": "Receita Bruta", "valor": f("receita_bruta_total"), "tipo": "receita"},
            {"label": "(-) Impostos sobre Vendas", "valor": -f("impostos_sobre_vendas"), "tipo": "deducao"},
            {"label": "Receita Líquida Gerencial", "valor": f("receita_liquida_gerencial"), "tipo": "subtotal"},
            {"label": "(-) CMV (Custo da Mercadoria Vendida)", "valor": -f("cmv_total"), "tipo": "custo"},
            {"label": "Margem Bruta", "valor": margem, "tipo": "subtotal"},
            {"label": "(-) Despesas com Funcionários", "valor": -desp["desp_pessoal"], "tipo": "despesa"},
            {"label": "(-) Despesas Comerciais", "valor": -desp["desp_comercial"], "tipo": "despesa"},
            {"label": "(-) Despesas Administrativas", "valor": -desp["desp_administrativa"], "tipo": "despesa"},
            {"label": "(-) Tributos Operacionais", "valor": -desp["desp_tributaria_operacional"], "tipo": "despesa"},
            {
                "label": "(-) Despesas Financeiras/Excepcionais",
                "valor": -(desp["desp_financeira"] + desp["desp_excepcional"]),
                "tipo": "despesa",
            },
            {"label": "Lucro Gerencial Estimado", "valor": lucro, "tipo": "resultado"},
        ],
        "margem_gerencial_pct": lucro / receita if receita > 0 else 0,
        "setores": {
            "pista": {"receita": f("receita_pista"), "cmv": f("cmv_pista")},
            "conveniencia": {"receita": f("receita_conveniencia"), "cmv": f("cmv_conveniencia")},
            "automotivo": {"receita": f("receita_automotivo")},
            "cigarro": {"receita": f("receita_cigarro")},
            "servico": {"receita": f("receita_servico")},
        },
        "disclaimer": disclaimer,
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/expenses")
def profit_expenses(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    ano_mes: Optional[int] = Query(
        None,
        description="Mês fechado YYYYMM. Default = mês corrente (America/Sao_Paulo).",
    ),
    regime_caixa: bool = Query(
        True,
        description="Quando true, agrega despesas pelo pagamento (DTAPGTO); senão por vencimento",
    ),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.overview")),
):
    """Expense breakdown by classification."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _solvencia_target_month(ano_mes, tenant_id)

    categories: List[Dict[str, Any]] = []
    top_expenses: List[Dict[str, Any]] = []
    desp_source = "clickhouse"
    params: Dict[str, Any] = {"id_empresa": tenant_id, "ano_mes": ref_month}
    branch_clause = _branch_filter_sql(branch_ids, params)
    try:
        sql = f"""
            SELECT
                classificacao_gerencial,
                SUM(valor_total) AS valor_total,
                SUM(qtd_lancamentos) AS qtd_lancamentos,
                SUM(valor_tipo_0) AS valor_tipo_0,
                SUM(valor_tipo_1) AS valor_tipo_1,
                SUM(valor_rateavel) AS valor_rateavel,
                SUM(valor_nao_rateavel) AS valor_nao_rateavel
            FROM torqmind_mart_rt.profit_despesas_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              {branch_clause}
              AND ano_mes = %(ano_mes)s
            GROUP BY classificacao_gerencial
            ORDER BY valor_total DESC
        """
        categories = query_dict(sql, params)
        top_sql = f"""
            SELECT
                codigo_plano, nome_plano, classificacao_gerencial,
                SUM(valor_total) AS valor_total,
                SUM(qtd_lancamentos) AS qtd_lancamentos
            FROM torqmind_mart_rt.profit_despesas_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              {branch_clause}
              AND ano_mes = %(ano_mes)s
            GROUP BY codigo_plano, nome_plano, classificacao_gerencial
            ORDER BY valor_total DESC
            LIMIT 20
        """
        top_expenses = query_dict(top_sql, params)
    except Exception as exc:
        logger.warning("profit_expenses: falha CH (empresa=%s mes=%s): %s", tenant_id, ref_month, exc)

    if not categories:
        try:
            pg = _despesas_pg_por_regime(
                tenant_id, branch_ids, ref_month, regime_caixa=bool(regime_caixa),
            )
            if pg["categorias"] or float((pg["buckets"] or {}).get("desp_operacional_total") or 0) > 0:
                categories = pg["categorias"]
                top_expenses = pg["top"]
                desp_source = "postgres_caixa" if regime_caixa else "postgres_competencia"
        except Exception as exc:
            logger.warning("profit_expenses: falha PG fallback (empresa=%s mes=%s): %s", tenant_id, ref_month, exc)

    total = sum(float(c.get("valor_total", 0) or 0) for c in categories)

    if regime_caixa:
        disclaimer = "Regime de caixa: despesas pelo pagamento efetivo (DTAPGTO)."
    else:
        disclaimer = "Competência por vencimento (DTAVCTO / ano_mes_competencia)."

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes": ref_month,
        "regime_caixa": bool(regime_caixa),
        "despesas_source": desp_source,
        "total_despesas": total,
        "categorias": [
            {
                "classificacao": c.get("classificacao_gerencial", ""),
                "valor": float(c.get("valor_total", 0) or 0),
                "qtd_lancamentos": int(c.get("qtd_lancamentos", 0) or 0),
                "percentual": float(c.get("valor_total", 0) or 0) / total if total > 0 else 0,
                "valor_rateavel": float(c.get("valor_rateavel", 0) or 0),
                "valor_nao_rateavel": float(c.get("valor_nao_rateavel", 0) or 0),
                "tipo_0": float(c.get("valor_tipo_0", 0) or 0),
                "tipo_1": float(c.get("valor_tipo_1", 0) or 0),
            }
            for c in categories
        ],
        "top_despesas": [
            {
                "codigo": t.get("codigo_plano", ""),
                "nome": t.get("nome_plano", ""),
                "classificacao": t.get("classificacao_gerencial", ""),
                "valor": float(t.get("valor_total", 0) or 0),
                "qtd": int(t.get("qtd_lancamentos", 0) or 0),
            }
            for t in top_expenses
        ],
        "disclaimer": disclaimer,
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/products")
def profit_products(
    ano_mes: Optional[int] = Query(None, description="Mês YYYYMM; default = último mês com dados em profit_produto_mensal"),
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    setor: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("impacto_estimado_60d"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.products")),
):
    """Product margin grid with repricing simulation."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _resolve_product_month(tenant_id, branch_ids, ano_mes_q=ano_mes)
    if not ref_month:
        return {"data": None, "message": "Sem dados de produtos."}

    # Build filters
    filters = "AND 1=1"
    if setor:
        filters += f" AND setor_gerencial = %(setor)s"
    if status:
        filters += f" AND status_preco = %(status)s"

    # Validate sort column
    allowed_sorts = {
        "impacto_estimado_60d", "receita", "margem_bruta_pct", "margem_gerencial_pct",
        "reajuste_sugerido_pct", "qtd_vendida", "preco_medio", "nome_produto",
    }
    order_col = sort_by if sort_by in allowed_sorts else "impacto_estimado_60d"
    order_dir = "ASC" if order_col == "nome_produto" else "DESC"

    params: Dict[str, Any] = {
        "id_empresa": tenant_id, "ano_mes": ref_month,
        "setor": setor, "status": status, "limit": limit, "offset": offset,
    }
    branch_clause = _branch_filter_sql(branch_ids, params)

    # For multi-branch, aggregate products by id_produto
    is_multi = len(branch_ids) > 1
    if is_multi:
        # Two-level subquery to avoid ClickHouse 24.8 alias resolution conflicts
        # Level 1 (innermost): raw aggregates only with unique alias names
        # Level 2 (outer): derived metrics computed from the aggregated values
        sql = f"""
            SELECT
                id_produto, nome_produto, nome_grupo_produto,
                setor_gerencial, qtd_vendida_agg AS qtd_vendida,
                receita_agg AS receita,
                toFloat64(receita_agg) / nullIf(qtd_vendida_agg, 0) AS preco_medio,
                toFloat64(cmv_agg) / nullIf(qtd_vendida_agg, 0) AS custo_medio,
                cmv_agg AS cmv,
                1 - toFloat64(cmv_agg) / nullIf(receita_agg, 0) AS margem_bruta_pct,
                toFloat64(desp_op_total) / nullIf(qtd_vendida_agg, 0) AS desp_operacional_unitaria,
                toFloat64(receita_agg - cmv_agg - desp_op_total) / nullIf(receita_agg, 0) AS margem_gerencial_pct,
                toFloat64(receita_agg) / nullIf(cmv_agg, 0) - 1 AS markup_real,
                preco_minimo_saudavel, preco_ideal_sugerido,
                reajuste_sugerido_valor, reajuste_sugerido_pct,
                qtd_mes_anterior, impacto_estimado_60d,
                status_preco, recomendacao_curta
            FROM (
                SELECT
                    id_produto,
                    any(nome_produto) AS nome_produto,
                    any(nome_grupo_produto) AS nome_grupo_produto,
                    any(setor_gerencial) AS setor_gerencial,
                    SUM(qtd_vendida) AS qtd_vendida_agg,
                    SUM(receita) AS receita_agg,
                    SUM(cmv) AS cmv_agg,
                    SUM(desp_operacional_unitaria * qtd_vendida) AS desp_op_total,
                    AVG(preco_minimo_saudavel) AS preco_minimo_saudavel,
                    AVG(preco_ideal_sugerido) AS preco_ideal_sugerido,
                    AVG(reajuste_sugerido_valor) AS reajuste_sugerido_valor,
                    AVG(reajuste_sugerido_pct) AS reajuste_sugerido_pct,
                    SUM(qtd_mes_anterior) AS qtd_mes_anterior,
                    SUM(impacto_estimado_60d) AS impacto_estimado_60d,
                    any(status_preco) AS status_preco,
                    any(recomendacao_curta) AS recomendacao_curta
                FROM torqmind_mart_rt.profit_produto_mensal FINAL
                WHERE id_empresa = %(id_empresa)s
                  {branch_clause}
                  AND ano_mes = %(ano_mes)s
                GROUP BY id_produto
                HAVING SUM(qtd_vendida) > 0
            )
            WHERE 1=1 {filters}
            ORDER BY {order_col} {order_dir}
            LIMIT %(limit)s OFFSET %(offset)s
        """
    else:
        sql = f"""
            SELECT *
            FROM torqmind_mart_rt.profit_produto_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              {branch_clause}
              AND ano_mes = %(ano_mes)s
              AND qtd_vendida > 0
              {filters}
            ORDER BY {order_col} {order_dir}
            LIMIT %(limit)s OFFSET %(offset)s
        """
    rows = query_dict(sql, params)

    # Count total
    if is_multi:
        count_sql = f"""
            SELECT count() AS total FROM (
                SELECT id_produto
                FROM torqmind_mart_rt.profit_produto_mensal FINAL
                WHERE id_empresa = %(id_empresa)s
                  {branch_clause}
                  AND ano_mes = %(ano_mes)s
                  {filters}
                GROUP BY id_produto
                HAVING SUM(qtd_vendida) > 0
            )
        """
    else:
        count_sql = f"""
            SELECT count() AS total
            FROM torqmind_mart_rt.profit_produto_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              {branch_clause}
              AND ano_mes = %(ano_mes)s
              AND qtd_vendida > 0
              {filters}
        """
    total_count = query_scalar(count_sql, params) or 0

    products = []
    for r in rows:
        products.append({
            "id_produto": int(r.get("id_produto", 0)),
            "nome_produto": r.get("nome_produto", ""),
            "grupo": r.get("nome_grupo_produto", ""),
            "setor": r.get("setor_gerencial", ""),
            "qtd_vendida": float(r.get("qtd_vendida", 0)),
            "receita": float(r.get("receita", 0)),
            "preco_atual": float(r.get("preco_medio", 0)),
            "custo_unitario": float(r.get("custo_medio", 0)),
            "cmv": float(r.get("cmv", 0)),
            "margem_bruta_pct": float(r.get("margem_bruta_pct", 0)),
            "desp_unitaria": float(r.get("desp_operacional_unitaria", 0)),
            "margem_gerencial_pct": float(r.get("margem_gerencial_pct", 0)),
            "markup_real": float(r.get("markup_real", 0)),
            "preco_minimo": float(r.get("preco_minimo_saudavel", 0)),
            "preco_ideal": float(r.get("preco_ideal_sugerido", 0)),
            "reajuste_valor": float(r.get("reajuste_sugerido_valor", 0)),
            "reajuste_pct": float(r.get("reajuste_sugerido_pct", 0)),
            "qtd_mes_anterior": float(r.get("qtd_mes_anterior", 0)),
            "impacto_60d": float(r.get("impacto_estimado_60d", 0)),
            "status": r.get("status_preco", "sem_dados"),
            "recomendacao": r.get("recomendacao_curta", ""),
        })

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes": ref_month,
        "total": int(total_count),
        "produtos": products,
        "disclaimer": "Estimativa baseada no volume vendido no mês anterior. Assume manutenção do volume. Não considera elasticidade de preço.",
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/repricing")
def profit_repricing(
    ano_mes: Optional[int] = Query(None, description="Mês YYYYMM; default = último mês com dados em profit_produto_mensal"),
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.repricing")),
):
    """Top repricing opportunities sorted by positive impact."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _resolve_product_month(tenant_id, branch_ids, ano_mes_q=ano_mes)
    if not ref_month:
        return {"data": None, "message": "Sem dados para simulação."}

    params: Dict[str, Any] = {"id_empresa": tenant_id, "ano_mes": ref_month, "limit": limit}
    branch_clause = _branch_filter_sql(branch_ids, params)

    is_multi = len(branch_ids) > 1
    if is_multi:
        sql = f"""
            SELECT
                id_produto, nome_produto, nome_grupo_produto,
                setor_gerencial,
                toFloat64(receita_agg) / nullIf(qtd_vendida_agg, 0) AS preco_medio,
                preco_ideal_sugerido,
                reajuste_sugerido_valor, reajuste_sugerido_pct,
                qtd_mes_anterior_agg AS qtd_mes_anterior,
                impacto_60d_agg AS impacto_estimado_60d, status_preco,
                1 - toFloat64(cmv_agg) / nullIf(receita_agg, 0) AS margem_bruta_pct
            FROM (
                SELECT
                    id_produto,
                    any(nome_produto) AS nome_produto,
                    any(nome_grupo_produto) AS nome_grupo_produto,
                    any(setor_gerencial) AS setor_gerencial,
                    SUM(receita) AS receita_agg,
                    SUM(qtd_vendida) AS qtd_vendida_agg,
                    SUM(cmv) AS cmv_agg,
                    AVG(preco_ideal_sugerido) AS preco_ideal_sugerido,
                    AVG(reajuste_sugerido_valor) AS reajuste_sugerido_valor,
                    AVG(reajuste_sugerido_pct) AS reajuste_sugerido_pct,
                    SUM(qtd_mes_anterior) AS qtd_mes_anterior_agg,
                    SUM(impacto_estimado_60d) AS impacto_60d_agg,
                    any(status_preco) AS status_preco,
                    SUM(entra_simulador_reajuste) AS entra_simulador_reajuste_sum
                FROM torqmind_mart_rt.profit_produto_mensal FINAL
                WHERE id_empresa = %(id_empresa)s
                  {branch_clause}
                  AND ano_mes = %(ano_mes)s
                GROUP BY id_produto
                HAVING impacto_60d_agg > 0
            )
            WHERE status_preco IN ('abaixo_minimo', 'abaixo_ideal')
              AND entra_simulador_reajuste_sum > 0
            ORDER BY impacto_60d_agg DESC
            LIMIT %(limit)s
        """
    else:
        sql = f"""
            SELECT *
            FROM torqmind_mart_rt.profit_produto_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              {branch_clause}
              AND ano_mes = %(ano_mes)s
              AND impacto_estimado_60d > 0
              AND status_preco IN ('abaixo_minimo', 'abaixo_ideal')
              AND entra_simulador_reajuste = 1
            ORDER BY impacto_estimado_60d DESC
            LIMIT %(limit)s
        """
    rows = query_dict(sql, params)

    total_impact = sum(float(r.get("impacto_estimado_60d", 0)) for r in rows)

    opportunities = []
    for r in rows:
        opportunities.append({
            "id_produto": int(r.get("id_produto", 0)),
            "nome_produto": r.get("nome_produto", ""),
            "grupo": r.get("nome_grupo_produto", ""),
            "setor": r.get("setor_gerencial", ""),
            "preco_atual": float(r.get("preco_medio", 0)),
            "preco_ideal": float(r.get("preco_ideal_sugerido", 0)),
            "reajuste_valor": float(r.get("reajuste_sugerido_valor", 0)),
            "reajuste_pct": float(r.get("reajuste_sugerido_pct", 0)),
            "qtd_mes_anterior": float(r.get("qtd_mes_anterior", 0)),
            "impacto_60d": float(r.get("impacto_estimado_60d", 0)),
            "status": r.get("status_preco", ""),
            "margem_atual_pct": float(r.get("margem_bruta_pct", 0)),
        })

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes": ref_month,
        "total_oportunidades": len(opportunities),
        "impacto_total_60d": total_impact,
        "oportunidades": opportunities,
        "disclaimer": "Estimativa baseada no volume vendido no mês anterior. Assume manutenção do volume. Não considera elasticidade de preço.",
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/solvencia")
def profit_solvencia(
    ano_mes: Optional[int] = Query(None, description="Mês-alvo no formato YYYYMM; default = mês corrente"),
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.solvencia")),
):
    """Solvência / Capital de Giro: ativo circulante x contas a pagar do mês.

    Aba \"Solvência\" do DRE Gerencial, com filtro de mês. Responde \"meus ativos
    cobrem o passivo do mês?\" cruzando o disponível (caixa/banco), os recebíveis
    de curto prazo (cartões/cheques) e o estoque a custo com as contas a pagar
    que vencem no mês-alvo.
    """
    role = claims["role"]
    # Solvencia expoe estoque A CUSTO e a saude financeira (ativos x passivos):
    # dado gerencial sensivel. Alem de exigir a tela (require_screen), so quem pode
    # ver financeiros sensiveis (owner/master/admin) acessa; gerente/canal/vendedor
    # ficam de fora mesmo tendo a tela concedida.
    if not can_view_sensitive_financials(claims):
        return {
            "data": None,
            "message": "Sem permissão para ver a análise de solvência (dados financeiros gerenciais).",
        }

    tenant, filial, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais,
    )

    target: Optional[int] = None
    if ano_mes is not None:
        mm = int(ano_mes) % 100
        if int(ano_mes) >= 190001 and 1 <= mm <= 12:
            target = int(ano_mes)
    if target is None:
        today = resolve_business_date(None, tenant)
        target = today.year * 100 + today.month

    return redact_sensitive(
        {"data": repos_mart.solvencia_overview(role, tenant, filial, target)},
        claims,
    )


def _solvencia_target_month(ano_mes: Optional[int], tenant: int) -> int:
    if ano_mes is not None:
        mm = int(ano_mes) % 100
        if int(ano_mes) >= 190001 and 1 <= mm <= 12:
            return int(ano_mes)
    today = resolve_business_date(None, tenant)
    return today.year * 100 + today.month


@router.get("/solvencia/detalhada")
def profit_solvencia_detalhada(
    ano_mes: Optional[int] = Query(None, description="Mês-alvo YYYYMM; default = mês corrente"),
    ativos_do_mes: bool = Query(
        True,
        description="Quando true, prioriza recebíveis com vencimento no mês e ainda em aberto",
    ),
    considerar_nao_circulantes: bool = Query(
        False,
        description="Quando true, inclui Ativo Não Circulante nos totalizadores (ativo/capital/liquidez)",
    ),
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.solvencia")),
):
    """Solvência detalhada (Fechamento de Caixa Geral): grupos/seções/itens.

    Ativo Circulante (combustível por tipo, estoque, a prazo, cartões, cheques,
    dinheiro, bancos), Ativo Não Circulante (investimentos) e Passivo Circulante
    (boletos), com itens e totais. Bancos/investimentos são preenchidos
    manualmente por mês (seções editaveis=true).
    """
    role = claims["role"]
    if not can_view_sensitive_financials(claims):
        return {"data": None, "message": "Sem permissão para ver a análise de solvência."}
    tenant, filial, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais,
    )
    target = _solvencia_target_month(ano_mes, tenant)
    data = repos_mart.solvencia_detalhada(
        role,
        tenant,
        filial,
        target,
        ativos_do_mes=ativos_do_mes,
        considerar_nao_circulantes=considerar_nao_circulantes,
    )
    data["ativos_do_mes"] = bool(ativos_do_mes)
    data["considerar_nao_circulantes"] = bool(considerar_nao_circulantes)
    return redact_sensitive({"data": data}, claims)


class SolvenciaManualItem(BaseModel):
    descricao: str
    valor: float = 0.0


class SolvenciaManualUpsert(BaseModel):
    id_filial: int
    ano_mes: int
    id_tipo: int
    itens: List[SolvenciaManualItem] = []


@router.post("/solvencia/manual")
def profit_solvencia_manual_upsert(
    body: SolvenciaManualUpsert,
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.solvencia")),
):
    """Grava (replace) os itens manuais de um painel (bancos/investimentos) para
    a filial e o mês selecionados. Recebe a lista completa do painel."""
    if not can_view_sensitive_financials(claims):
        return {"ok": False, "message": "Sem permissão."}
    tenant, filial, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa, id_filial_q=body.id_filial, id_filiais_q=None,
    )
    fid = filial if filial is not None else body.id_filial
    try:
        result = repos_mart.solvencia_manual_upsert(
            claims["role"], tenant, fid, body.ano_mes, body.id_tipo,
            [it.model_dump() for it in body.itens],
        )
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}
    return result


# ---------------------------------------------------------------------------
# Compliance ANP / CDC
# ---------------------------------------------------------------------------

class AnpConfigUpsert(BaseModel):
    id_filial: int = 0  # 0 = default empresa
    limite_alerta_amarelo_perc: float = 50.0
    limite_abusivo_anp_perc: float = 70.0


@router.get("/anp-compliance")
def profit_anp_compliance(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    dt_ini: Optional[date] = Query(None),
    dt_fim: Optional[date] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.anp")),
):
    """Grid + KPIs de variação de margem (lastro ANP/CDC)."""
    if not can_view_sensitive_financials(claims):
        return {"ok": False, "message": "Sem permissão para Compliance ANP."}
    tenant_id, branch_ids = _extract_profit_scope(
        claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais,
    )
    if not branch_ids:
        return {"data": None, "message": "Selecione ao menos uma filial."}
    from app.services import anp_compliance as anp

    data = anp.overview_payload(
        tenant_id, branch_ids, dt_ini=dt_ini, dt_fim=dt_fim, prefer_mart=True,
    )
    return redact_sensitive({"data": data}, claims)


@router.get("/anp-compliance/config")
def profit_anp_config_get(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.anp")),
):
    if not can_view_sensitive_financials(claims):
        return {"ok": False, "message": "Sem permissão."}
    tenant_id, _, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=None,
    )
    from app.services import anp_compliance as anp

    fid = int(id_filial) if id_filial else 0
    return {"data": anp.load_config(tenant_id, fid)}


@router.put("/anp-compliance/config")
def profit_anp_config_put(
    body: AnpConfigUpsert,
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.anp")),
):
    if not can_view_sensitive_financials(claims):
        return {"ok": False, "message": "Sem permissão."}
    tenant_id, _, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa, id_filial_q=body.id_filial or None, id_filiais_q=None,
    )
    from app.services import anp_compliance as anp

    try:
        row = anp.upsert_config(
            tenant_id,
            body.id_filial,
            body.limite_alerta_amarelo_perc,
            body.limite_abusivo_anp_perc,
            updated_by=str(claims.get("sub") or claims.get("email") or claims.get("role") or ""),
        )
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}
    return {"ok": True, "data": row}


@router.get("/anp-compliance/export")
def profit_anp_export(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    dt_ini: Optional[date] = Query(None),
    dt_fim: Optional[date] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.anp")),
):
    if not can_view_sensitive_financials(claims):
        return {"ok": False, "message": "Sem permissão."}
    tenant_id, branch_ids = _extract_profit_scope(
        claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais,
    )
    from app.services import anp_compliance as anp
    from fastapi.responses import Response

    data = anp.overview_payload(
        tenant_id, branch_ids, dt_ini=dt_ini, dt_fim=dt_fim, prefer_mart=True,
    )
    csv_body = anp.events_to_csv(data.get("eventos") or [])
    return Response(
        content=csv_body,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="anp_compliance.csv"'},
    )


@router.post("/anp-compliance/refresh")
def profit_anp_refresh(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    dt_ini: Optional[date] = Query(None),
    dt_fim: Optional[date] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management.anp")),
):
    """Publica proxy em mart_anp_compliance (exige CH WRITE). Sempre recalcula live."""
    if not can_view_sensitive_financials(claims):
        return {"ok": False, "message": "Sem permissão."}
    role = str(claims.get("role") or "")
    if role not in ("platform_master", "owner"):
        return {"ok": False, "message": "Apenas owner/platform pode forçar refresh."}
    tenant_id, branch_ids = _extract_profit_scope(
        claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais,
    )
    from app.services import anp_compliance as anp

    live = anp.overview_payload(
        tenant_id, branch_ids, dt_ini=dt_ini, dt_fim=dt_fim, prefer_mart=True,
    )
    published = None
    try:
        published = anp.publish_proxy_to_mart(
            tenant_id, branch_ids, dt_ini=dt_ini, dt_fim=dt_fim,
        )
    except Exception as exc:
        logger.warning("ANP mart publish failed (CH RO?): %s", exc)
        published = {"inserted": 0, "error": str(exc)[:240]}
    return {"ok": True, "data": {"live_total": live["total_eventos"], "publish": published}}

