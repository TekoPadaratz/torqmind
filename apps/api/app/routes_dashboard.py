from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps import get_current_claims
from app.permissions import require_screen, redact_sensitive
from app.scope import resolve_scope
from app.schemas import DashboardKpisResponse, DashboardSeriesResponse, InsightsResponse
from app import repos_analytics as repos_mart

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

MAX_SERIES_DAYS = 400  # ~13 months; prevents unbounded queries


def _clamp_date_range(dt_ini: date, dt_fim: date) -> tuple[date, date]:
    """Ensure date range does not exceed MAX_SERIES_DAYS."""
    if dt_fim < dt_ini:
        raise HTTPException(status_code=400, detail="dt_fim deve ser >= dt_ini")
    if (dt_fim - dt_ini).days > MAX_SERIES_DAYS:
        dt_ini = dt_fim - timedelta(days=MAX_SERIES_DAYS)
    return dt_ini, dt_fim


@router.get("/kpis", response_model=DashboardKpisResponse)
def get_kpis(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("dashboard_home")),
):
    """Main KPI tiles for the general dashboard."""

    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return redact_sensitive(
        repos_mart.dashboard_kpis(role, tenant, filial, dt_ini, dt_fim),
        claims,
    )


@router.get("/series", response_model=DashboardSeriesResponse)
def get_series(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("dashboard_home")),
):
    """Daily series for charts (faturamento + margem)."""

    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    dt_ini, dt_fim = _clamp_date_range(dt_ini, dt_fim)
    points = repos_mart.dashboard_series(role, tenant, filial, dt_ini, dt_fim)
    return redact_sensitive({"points": points}, claims)


@router.get("/insights", response_model=InsightsResponse)
def get_insights(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("dashboard_home")),
):
    """Jarvis base: daily and month-to-date comparatives."""

    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    dt_ini, dt_fim = _clamp_date_range(dt_ini, dt_fim)
    points = repos_mart.insights_base(role, tenant, filial, dt_ini, dt_fim)
    return redact_sensitive({"points": points}, claims)
