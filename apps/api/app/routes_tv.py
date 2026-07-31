"""Routes: TV / Kiosk-mode endpoints.

Dedicated lightweight endpoints for TV display screens.
These endpoints:
  - use the current date automatically;
  - scope to the user's branch from claims;
  - NEVER expose margin, profit, or cost fields;
  - require explicit TV screen permissions.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from app.business_time import business_today
from app.deps import get_current_claims
from app.permissions import require_screen

router = APIRouter(prefix="/bi/tv", tags=["tv"])
logger = logging.getLogger(__name__)

# Fields that are safe to expose on public TV displays
_RANKING_SAFE_FIELDS = {"id_vendedor", "vendedor", "nome", "total", "qtd_vendas", "ticket_medio"}


def _tv_scope(claims: dict[str, Any]) -> tuple[str, int, int]:
    """Extract role, tenant and branch from claims.  Branch is mandatory for TV."""
    role = claims.get("role") or claims.get("user_role") or ""
    tenant = claims.get("id_empresa")
    branch = claims.get("id_filial")
    if not tenant or not branch:
        raise HTTPException(
            status_code=400,
            detail="TV endpoints require a user with id_empresa and id_filial.",
        )
    return role, int(tenant), int(branch)


def _strip_fields(rows: list[dict[str, Any]], allowed: set[str]) -> list[dict[str, Any]]:
    """Return copies of *rows* keeping only keys in *allowed*."""
    return [{k: v for k, v in row.items() if k in allowed} for row in rows]


def _hour_label(hora: Any) -> str:
    try:
        h = int(hora)
    except (TypeError, ValueError):
        h = 0
    h = max(0, min(23, h))
    return f"{h:02d}:00"


def _build_hourly_points(commercial_by_hour: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize 0..23 hour series for the TV floor chart."""
    buckets = {h: 0.0 for h in range(24)}
    for row in commercial_by_hour or []:
        try:
            hora = int(row.get("hora") if row.get("hora") is not None else row.get("hour") or 0)
        except (TypeError, ValueError):
            continue
        if hora < 0 or hora > 23:
            continue
        valor = row.get("saidas")
        if valor is None:
            valor = row.get("faturamento")
        if valor is None:
            valor = row.get("total")
        try:
            buckets[hora] += float(valor or 0)
        except (TypeError, ValueError):
            continue
    return [
        {
            "hora": h,
            "hour": _hour_label(h),
            "label": _hour_label(h),
            "total": round(buckets[h], 2),
            "faturamento": round(buckets[h], 2),
            "saidas": round(buckets[h], 2),
        }
        for h in range(24)
    ]


# ---------------------------------------------------------------------------
# GET /bi/tv/sales-hourly
# ---------------------------------------------------------------------------
@router.get("/sales-hourly")
def tv_sales_hourly(
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("tv_sales_hourly")),
):
    """Hourly sales + floor totals for the current day — TV display safe.

    Returns:
      - totals: vendas / cancelamentos / devoluções (sem margem/custo)
      - points: série horária 0..23
    """
    role, tenant, branch = _tv_scope(claims)
    today = business_today(tenant)

    try:
        from app import repos_analytics as repos_mart

        bundle = repos_mart.sales_overview_bundle(
            role, tenant, branch, today, today, include_details=False,
        )
    except Exception:
        logger.exception("Error fetching TV hourly floor payload")
        raise HTTPException(status_code=500, detail="Erro ao buscar painel de vendas por hora.")

    commercial = bundle.get("commercial_kpis") or {}
    kpis = bundle.get("kpis") or {}
    devolucoes = float(kpis.get("devolucoes") or commercial.get("entradas") or 0)
    qtd_devolucoes = int(commercial.get("qtd_entradas") or 0)

    points = _build_hourly_points(bundle.get("commercial_by_hour") or [])
    return {
        "ok": True,
        "dt": today.isoformat(),
        "totals": {
            "vendas": round(float(commercial.get("saidas") or kpis.get("faturamento") or 0), 2),
            "qtd_vendas": int(commercial.get("qtd_saidas") or (bundle.get("stats") or {}).get("vendas") or 0),
            "cancelamentos": round(float(commercial.get("cancelamentos") or 0), 2),
            "qtd_cancelamentos": int(commercial.get("qtd_cancelamentos") or 0),
            "devolucoes": round(devolucoes, 2),
            "qtd_devolucoes": qtd_devolucoes,
        },
        "points": points,
    }


# ---------------------------------------------------------------------------
# GET /bi/tv/sales-ranking
# ---------------------------------------------------------------------------
@router.get("/sales-ranking")
def tv_sales_ranking(
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("tv_sales_ranking")),
):
    """Seller ranking for the current day — TV display safe."""
    role, tenant, branch = _tv_scope(claims)
    today = business_today(tenant)

    try:
        from app import repos_analytics as repos_mart

        bundle = repos_mart.sales_overview_bundle(
            role, tenant, branch, today, today, include_details=True,
        )
    except Exception:
        logger.exception("Error fetching TV sales ranking")
        raise HTTPException(status_code=500, detail="Erro ao buscar ranking de vendedores.")

    raw_sellers: list[dict[str, Any]] = bundle.get("sellers") or bundle.get("vendedores") or []
    safe_sellers = _strip_fields(raw_sellers, _RANKING_SAFE_FIELDS)
    return {"ok": True, "sellers": safe_sellers, "dt": today.isoformat()}
