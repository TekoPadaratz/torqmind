"""Profit Management (Gestão de Lucro) API routes.

Provides endpoints for the premium profit management module including:
- Overview/KPIs
- DRE Gerencial
- Expense breakdown
- Product margin grid with repricing simulation
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from app.db_clickhouse import query_dict, query_scalar
from app.deps import get_current_claims
from app.permissions import require_screen, redact_sensitive
from app.scope import resolve_scope_filters

router = APIRouter(prefix="/bi/profit-management", tags=["profit-management"])
logger = logging.getLogger(__name__)

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
    _screen=Depends(require_screen("profit_management")),
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
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """DRE Gerencial Resumida."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch_ids)
    if not ref_month:
        return {"data": None, "message": "Sem dados para DRE."}

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
        return float(dre.get(key, 0))

    receita = f("receita_bruta_total")
    lucro = f("lucro_gerencial_estimado")

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes": ref_month,
        "linhas": [
            {"label": "Receita Bruta", "valor": f("receita_bruta_total"), "tipo": "receita"},
            {"label": "(-) Impostos sobre Vendas", "valor": -f("impostos_sobre_vendas"), "tipo": "deducao"},
            {"label": "Receita Líquida Gerencial", "valor": f("receita_liquida_gerencial"), "tipo": "subtotal"},
            {"label": "(-) CMV (Custo da Mercadoria Vendida)", "valor": -f("cmv_total"), "tipo": "custo"},
            {"label": "Margem Bruta", "valor": f("margem_bruta"), "tipo": "subtotal"},
            {"label": "(-) Despesas com Pessoal", "valor": -f("desp_pessoal"), "tipo": "despesa"},
            {"label": "(-) Despesas Comerciais", "valor": -f("desp_comercial"), "tipo": "despesa"},
            {"label": "(-) Despesas Administrativas", "valor": -f("desp_administrativa"), "tipo": "despesa"},
            {"label": "(-) Tributos Operacionais", "valor": -f("desp_tributaria_operacional"), "tipo": "despesa"},
            {"label": "(-) Despesas Financeiras/Excepcionais", "valor": -(f("desp_financeira") + f("desp_excepcional")), "tipo": "despesa"},
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
        "disclaimer": "Lucro gerencial estimado = receita líquida - CMV - despesas operacionais. Não é lucro contábil/fiscal oficial.",
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/expenses")
def profit_expenses(
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Expense breakdown by classification."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch_ids)
    if not ref_month:
        return {"data": None, "message": "Sem dados de despesas."}

    params: Dict[str, Any] = {"id_empresa": tenant_id, "ano_mes": ref_month}
    branch_clause = _branch_filter_sql(branch_ids, params)

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

    # Top expenses by account — aggregate across branches
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

    total = sum(float(c.get("valor_total", 0)) for c in categories)

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes": ref_month,
        "total_despesas": total,
        "categorias": [
            {
                "classificacao": c.get("classificacao_gerencial", ""),
                "valor": float(c.get("valor_total", 0)),
                "qtd_lancamentos": int(c.get("qtd_lancamentos", 0)),
                "percentual": float(c.get("valor_total", 0)) / total if total > 0 else 0,
                "valor_rateavel": float(c.get("valor_rateavel", 0)),
                "valor_nao_rateavel": float(c.get("valor_nao_rateavel", 0)),
                "tipo_0": float(c.get("valor_tipo_0", 0)),
                "tipo_1": float(c.get("valor_tipo_1", 0)),
            }
            for c in categories
        ],
        "top_despesas": [
            {
                "codigo": t.get("codigo_plano", ""),
                "nome": t.get("nome_plano", ""),
                "classificacao": t.get("classificacao_gerencial", ""),
                "valor": float(t.get("valor_total", 0)),
                "qtd": int(t.get("qtd_lancamentos", 0)),
            }
            for t in top_expenses
        ],
        "disclaimer": "Despesas usam vencimento como competência. Pagamento/baixa é informativo.",
    }

    return redact_sensitive({"data": payload}, claims)


@router.get("/products")
def profit_products(
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
    _screen=Depends(require_screen("profit_management")),
):
    """Product margin grid with repricing simulation."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch_ids)
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
    id_empresa: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    branch_scope: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Top repricing opportunities sorted by positive impact."""
    tenant_id, branch_ids = _extract_profit_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    if not branch_ids:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch_ids)
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
