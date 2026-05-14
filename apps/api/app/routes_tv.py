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
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from app.deps import get_current_claims
from app.permissions import require_screen

router = APIRouter(prefix="/bi/tv", tags=["tv"])
logger = logging.getLogger(__name__)

# Fields that are safe to expose on public TV displays
_HOURLY_SAFE_FIELDS = {"data_key", "id_filial", "faturamento", "hour", "label", "dt", "total"}
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


# ---------------------------------------------------------------------------
# GET /bi/tv/sales-hourly
# ---------------------------------------------------------------------------
@router.get("/sales-hourly")
def tv_sales_hourly(
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("tv_sales_hourly")),
):
    """Hourly sales series for the current day — TV display safe."""
    role, tenant, branch = _tv_scope(claims)
    today = date.today()

    try:
        from app import repos_analytics as repos_mart

        points = repos_mart.dashboard_series(role, tenant, branch, today, today)
    except Exception:
        logger.exception("Error fetching TV hourly series")
        raise HTTPException(status_code=500, detail="Erro ao buscar série horária.")

    safe_points = _strip_fields(points, _HOURLY_SAFE_FIELDS)
    return {"ok": True, "points": safe_points, "dt": today.isoformat()}


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
    today = date.today()

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
