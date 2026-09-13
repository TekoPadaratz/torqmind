"""Investigação determinística da carteira CAP/CAR (Phase 3 — 2º domínio).

Somente leitura da mart ClickHouse ``mart_finance_titles_rt``.
Snapshot operacional atual — não inventa série histórica inexistente.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.db_clickhouse import query_dict
from app.repos_mart_realtime import MART_RT_DB, _branch_clause

TOP_N = 8


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def investigate_finance_portfolio(
    role: str,
    id_empresa: int,
    id_filial: Any,
    *,
    tipo: Optional[int] = None,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Carteira aberta + concentração por filial/status (evidências rotuladas)."""
    del role
    branch_clause = _branch_clause("id_filial", id_filial)
    if branch_clause.strip() == "AND 0":
        return {
            "status": "forbidden_scope",
            "domain": "finance_portfolio",
            "message": "Nenhuma filial autorizada nas permissões atuais.",
        }

    tipo_filter = ""
    params: Dict[str, Any] = {"id_empresa": int(id_empresa)}
    if tipo in (0, 1):
        tipo_filter = " AND tipo_titulo = {tipo:Int8}"
        params["tipo"] = int(tipo)

    try:
        totals_rows = query_dict(
            f"""
            SELECT
              count() AS n_titulos,
              sum(valor_aberto) AS total_aberto,
              sumIf(valor_aberto, status = 'vencido') AS vencido,
              sumIf(valor_aberto, status = 'a_vencer') AS a_vencer,
              sumIf(valor_aberto, tipo_titulo = 1) AS receber_aberto,
              sumIf(valor_aberto, tipo_titulo = 0) AS pagar_aberto,
              countIf(valor_aberto > 0.01) AS n_abertos,
              max(published_at) AS last_updated
            FROM {MART_RT_DB}.mart_finance_titles_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND valor_aberto > 0.01
              {branch_clause}
              {tipo_filter}
            """,
            parameters=params,
        )
        by_filial = query_dict(
            f"""
            SELECT
              id_filial,
              sum(valor_aberto) AS total_aberto,
              count() AS n_titulos,
              sumIf(valor_aberto, status = 'vencido') AS vencido
            FROM {MART_RT_DB}.mart_finance_titles_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND valor_aberto > 0.01
              {branch_clause}
              {tipo_filter}
            GROUP BY id_filial
            ORDER BY total_aberto DESC
            LIMIT {{lim:Int32}}
            """,
            parameters={**params, "lim": TOP_N},
        )
        by_status = query_dict(
            f"""
            SELECT
              status,
              sum(valor_aberto) AS total_aberto,
              count() AS n_titulos
            FROM {MART_RT_DB}.mart_finance_titles_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND valor_aberto > 0.01
              {branch_clause}
              {tipo_filter}
            GROUP BY status
            ORDER BY total_aberto DESC
            """,
            parameters=params,
        )
        top_vencidos = query_dict(
            f"""
            SELECT
              id_filial, id_db, id_titulo, tipo_titulo, nro_documento,
              entidade_nome, status, valor_aberto, dt_vencimento, published_at
            FROM {MART_RT_DB}.mart_finance_titles_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND valor_aberto > 0.01
              AND status = 'vencido'
              {branch_clause}
              {tipo_filter}
            ORDER BY valor_aberto DESC
            LIMIT {{lim:Int32}}
            """,
            parameters={**params, "lim": TOP_N},
        )
    except Exception as exc:  # noqa: BLE001
        logger = __import__("logging").getLogger(__name__)
        logger.warning("finance_portfolio_investigate unavailable: %s", str(exc)[:160])
        return {
            "status": "unavailable",
            "domain": "finance_portfolio",
            "message": "Não foi possível consultar a carteira agora.",
            "totals": None,
            "factors": [],
        }

    tot = totals_rows[0] if totals_rows else {}
    n_abertos = int(tot.get("n_abertos") or 0)
    if n_abertos <= 0:
        return {
            "status": "no_data",
            "domain": "finance_portfolio",
            "message": (
                "Não há títulos em aberto para as filiais selecionadas. "
                "Isso não confirma saldo zero no sistema de origem."
            ),
            "totals": None,
            "factors": [],
            "freshness": {
                "mode": "realtime",
                "source": f"{MART_RT_DB}.mart_finance_titles_rt",
            },
        }

    aberto = _as_float(tot.get("total_aberto"))
    vencido = _as_float(tot.get("vencido"))
    receber = _as_float(tot.get("receber_aberto"))
    pagar = _as_float(tot.get("pagar_aberto"))

    factors: List[Dict[str, Any]] = []
    for row in by_filial:
        val = _as_float(row.get("total_aberto"))
        share = round((val / aberto) * 100.0, 1) if aberto >= 0.01 else None
        factors.append(
            {
                "kind": "contribution",
                "dimension": "filial",
                "label": f"Filial {row.get('id_filial')}",
                "delta": val,
                "share_of_total_delta_pct": share,
                "summary": (
                    f"Filial {row.get('id_filial')}: R$ {val:,.2f} em aberto "
                    f"({int(row.get('n_titulos') or 0)} títulos; "
                    f"vencido R$ {_as_float(row.get('vencido')):,.2f})"
                ),
                "evidence": {
                    "dimension": "filial",
                    "key": str(row.get("id_filial")),
                    "source": f"{MART_RT_DB}.mart_finance_titles_rt",
                },
                "causality": "not_proven",
            }
        )

    status_view = {
        "dimension": "status",
        "note": "Composição da carteira aberta por situação.",
        "items": [
            {
                "kind": "contribution",
                "dimension": "status",
                "label": str(r.get("status") or "—"),
                "delta": _as_float(r.get("total_aberto")),
                "summary": (
                    f"{r.get('status')}: R$ {_as_float(r.get('total_aberto')):,.2f} "
                    f"({int(r.get('n_titulos') or 0)} títulos)"
                ),
                "causality": "not_proven",
                "evidence": {
                    "dimension": "status",
                    "key": str(r.get("status")),
                    "source": f"{MART_RT_DB}.mart_finance_titles_rt",
                },
            }
            for r in by_status
        ],
        "truncated": False,
    }

    evidence_titles = [
        {
            "id_filial": int(r.get("id_filial") or 0),
            "id_db": int(r.get("id_db") or 0),
            "id_titulo": int(r.get("id_titulo") or 0),
            "tipo_titulo": int(r.get("tipo_titulo") or 0),
            "nro_documento": str(r.get("nro_documento") or "—"),
            "entidade_nome": str(r.get("entidade_nome") or ""),
            "valor_aberto": _as_float(r.get("valor_aberto")),
            "dt_vencimento": str(r.get("dt_vencimento") or ""),
            "kind": "evidence",
            "causality": "fact",
        }
        for r in top_vencidos
    ]

    from app.intelligence.playbooks.engine import run_playbook

    pb = run_playbook("delinquency_priority", context={}, can_view_profit=False) or {}
    hypotheses = [
        {
            "kind": "hypothesis",
            "label": h,
            "summary": h,
            "causality": "hypothesis",
            "evidence": {"source": "playbook:delinquency_v1"},
        }
        for h in (pb.get("hypotheses") or [])
    ]
    recommendations = [
        {
            "kind": "recommendation",
            "title": a.get("title"),
            "screen": a.get("screen") or "/finance",
            "causality": "recommendation",
            "summary": a.get("title"),
        }
        for a in (pb.get("actions") or [])[:5]
    ]

    last_updated = tot.get("last_updated")
    if isinstance(last_updated, datetime):
        freshness_ts = last_updated.astimezone(timezone.utc).isoformat()
    else:
        freshness_ts = str(last_updated) if last_updated else None

    headline = (
        f"Carteira aberta R$ {aberto:,.2f} "
        f"(receber R$ {receber:,.2f} / pagar R$ {pagar:,.2f}); "
        f"vencido R$ {vencido:,.2f} em {n_abertos} títulos."
    )

    return {
        "status": "ok",
        "domain": "finance_portfolio",
        "headline": headline,
        "comparison": {
            "basis": "current_mart_snapshot",
            "basis_label": (
                "Posição atual da carteira (não é comparação entre períodos)."
            ),
            "compatible": True,
        },
        "totals": {
            "valor_aberto": aberto,
            "vencido": vencido,
            "a_vencer": _as_float(tot.get("a_vencer")),
            "receber_aberto": receber,
            "pagar_aberto": pagar,
            "n_abertos": n_abertos,
            "current_has_data": True,
        },
        "dimension_views": {
            "filial": {
                "dimension": "filial",
                "note": "Concentração da carteira aberta por filial.",
                "items": [f for f in factors if f.get("dimension") == "filial"],
                "truncated": len(by_filial) >= TOP_N,
                "shown_count": len(by_filial),
            },
            "status": status_view,
        },
        "evidence_titles": evidence_titles,
        "factors": factors + hypotheses,
        "hypotheses": hypotheses,
        "recommendations": recommendations,
        "next_checks": [
            {"title": r.get("title"), "screen": r.get("screen")} for r in recommendations
        ],
        "follow_ups": [
            "Quais títulos vencidos concentram o risco?",
            "Investigar variação de vendas",
            "Detalhe por filial da carteira",
        ],
        "warnings": [
            "Ausência de títulos aqui não significa saldo zero no sistema de origem.",
            "Esta leitura é a posição atual — sem variação entre períodos.",
        ],
        "freshness": {
            "mode": "realtime",
            "source": f"{MART_RT_DB}.mart_finance_titles_rt",
            "last_updated": freshness_ts,
        },
        "legend": {
            "contribution": "Concentração observada na posição atual (não prova causa).",
            "hypothesis": "Hipótese operacional para verificação.",
            "recommendation": "Orientação — não executa cobrança nem altera títulos.",
            "fact": "Fato sustentado pelo título e pelo documento.",
        },
        "scope": {"id_empresa": int(id_empresa), "id_filial": id_filial, "tipo": tipo},
    }
