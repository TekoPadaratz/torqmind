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
from app.scope import resolve_scope_filters, primary_branch_id

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


def _resolve_reference_month(id_empresa: int, id_filial: int) -> Optional[int]:
    """Find the latest closed month with sufficient data."""
    sql = """
        SELECT ano_mes
        FROM torqmind_mart_rt.profit_dre_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND receita_bruta_total > 0
        ORDER BY ano_mes DESC
        LIMIT 1
    """
    result = query_scalar(sql, {"id_empresa": id_empresa, "id_filial": id_filial})
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
    id_filial: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Return metadata about the profit module: reference month, freshness."""
    role = claims["role"]
    tenant_id = claims.get("tenant_id") or claims.get("id_empresa")
    branch = id_filial or primary_branch_id(claims)

    if not branch:
        return {"available": False, "message": "Nenhuma filial selecionada."}

    ref_month = _resolve_reference_month(tenant_id, branch)
    if not ref_month:
        return {
            "available": False,
            "message": "Ainda não há dados suficientes para calcular o Lucro Gerencial Estimado desta filial.",
        }

    return {
        "available": True,
        "id_empresa": tenant_id,
        "id_filial": branch,
        "ano_mes_referencia": ref_month,
        "mes_referencia_label": _format_month_label(ref_month),
        "mes_anterior": _previous_month(ref_month),
    }


@router.get("/overview")
def profit_overview(
    id_filial: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Executive overview with KPIs for the profit management module."""
    role = claims["role"]
    tenant_id = claims.get("tenant_id") or claims.get("id_empresa")
    branch = id_filial or primary_branch_id(claims)

    if not branch:
        return {"data": None, "message": "Nenhuma filial selecionada."}

    ref_month = _resolve_reference_month(tenant_id, branch)
    if not ref_month:
        return {
            "data": None,
            "message": "Ainda não há dados suficientes para calcular o Lucro Gerencial Estimado desta filial.",
        }

    # DRE summary
    dre_sql = """
        SELECT *
        FROM torqmind_mart_rt.profit_dre_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND ano_mes = %(ano_mes)s
    """
    dre_rows = query_dict(dre_sql, {"id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month})
    dre = dre_rows[0] if dre_rows else {}

    # Resumo filial
    resumo_sql = """
        SELECT *
        FROM torqmind_mart_rt.profit_resumo_filial FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND ano_mes_referencia = %(ano_mes)s
    """
    resumo_rows = query_dict(resumo_sql, {"id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month})
    resumo = resumo_rows[0] if resumo_rows else {}

    payload = {
        "periodo_base": _format_month_label(ref_month),
        "ano_mes_referencia": ref_month,
        "id_filial": branch,
        "kpis": {
            "lucro_gerencial_estimado": float(dre.get("lucro_gerencial_estimado", 0)),
            "margem_gerencial_pct": float(dre.get("lucro_gerencial_pct", 0)),
            "receita_bruta": float(dre.get("receita_bruta_total", 0)),
            "desp_operacional_total": float(dre.get("desp_operacional_total", 0)),
            "desp_sobre_receita_pct": (
                float(dre.get("desp_operacional_total", 0)) / float(dre.get("receita_bruta_total", 1))
                if float(dre.get("receita_bruta_total", 0)) > 0 else 0
            ),
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
    id_filial: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """DRE Gerencial Resumida."""
    role = claims["role"]
    tenant_id = claims.get("tenant_id") or claims.get("id_empresa")
    branch = id_filial or primary_branch_id(claims)

    if not branch:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch)
    if not ref_month:
        return {"data": None, "message": "Sem dados para DRE."}

    dre_sql = """
        SELECT *
        FROM torqmind_mart_rt.profit_dre_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND ano_mes = %(ano_mes)s
    """
    rows = query_dict(dre_sql, {"id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month})
    dre = rows[0] if rows else {}

    def f(key):
        return float(dre.get(key, 0))

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
            {"label": "Lucro Gerencial Estimado", "valor": f("lucro_gerencial_estimado"), "tipo": "resultado"},
        ],
        "margem_gerencial_pct": f("lucro_gerencial_pct"),
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
    id_filial: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Expense breakdown by classification."""
    role = claims["role"]
    tenant_id = claims.get("tenant_id") or claims.get("id_empresa")
    branch = id_filial or primary_branch_id(claims)

    if not branch:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch)
    if not ref_month:
        return {"data": None, "message": "Sem dados de despesas."}

    sql = """
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
          AND id_filial = %(id_filial)s
          AND ano_mes = %(ano_mes)s
        GROUP BY classificacao_gerencial
        ORDER BY valor_total DESC
    """
    categories = query_dict(sql, {"id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month})

    # Top expenses by account
    top_sql = """
        SELECT
            codigo_plano, nome_plano, classificacao_gerencial,
            valor_total, qtd_lancamentos
        FROM torqmind_mart_rt.profit_despesas_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND ano_mes = %(ano_mes)s
        ORDER BY valor_total DESC
        LIMIT 20
    """
    top_expenses = query_dict(top_sql, {"id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month})

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
    id_filial: Optional[int] = Query(None),
    setor: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("impacto_estimado_60d"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Product margin grid with repricing simulation."""
    role = claims["role"]
    tenant_id = claims.get("tenant_id") or claims.get("id_empresa")
    branch = id_filial or primary_branch_id(claims)

    if not branch:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch)
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

    sql = f"""
        SELECT *
        FROM torqmind_mart_rt.profit_produto_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND ano_mes = %(ano_mes)s
          AND qtd_vendida > 0
          {filters}
        ORDER BY {order_col} {order_dir}
        LIMIT %(limit)s OFFSET %(offset)s
    """
    params = {
        "id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month,
        "setor": setor, "status": status, "limit": limit, "offset": offset,
    }
    rows = query_dict(sql, params)

    # Count total
    count_sql = f"""
        SELECT count() AS total
        FROM torqmind_mart_rt.profit_produto_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
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
    id_filial: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("profit_management")),
):
    """Top repricing opportunities sorted by positive impact."""
    role = claims["role"]
    tenant_id = claims.get("tenant_id") or claims.get("id_empresa")
    branch = id_filial or primary_branch_id(claims)

    if not branch:
        return {"data": None}

    ref_month = _resolve_reference_month(tenant_id, branch)
    if not ref_month:
        return {"data": None, "message": "Sem dados para simulação."}

    sql = """
        SELECT *
        FROM torqmind_mart_rt.profit_produto_mensal FINAL
        WHERE id_empresa = %(id_empresa)s
          AND id_filial = %(id_filial)s
          AND ano_mes = %(ano_mes)s
          AND impacto_estimado_60d > 0
          AND status_preco IN ('abaixo_minimo', 'abaixo_ideal')
          AND entra_simulador_reajuste = 1
        ORDER BY impacto_estimado_60d DESC
        LIMIT %(limit)s
    """
    rows = query_dict(sql, {"id_empresa": tenant_id, "id_filial": branch, "ano_mes": ref_month, "limit": limit})

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
