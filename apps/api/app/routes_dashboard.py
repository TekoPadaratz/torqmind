from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.deps import get_current_claims
from app.scope import resolve_scope
from app.schemas import DashboardKpisResponse, DashboardSeriesResponse, InsightsResponse
from app import repos_mart

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/kpis", response_model=DashboardKpisResponse)
def get_kpis(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    """Main KPI tiles for the general dashboard."""

    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return repos_mart.dashboard_kpis(role, tenant, filial, dt_ini, dt_fim)


@router.get("/series", response_model=DashboardSeriesResponse)
def get_series(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    """Daily series for charts (faturamento + margem)."""

    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    points = repos_mart.dashboard_series(role, tenant, filial, dt_ini, dt_fim)
    return {"points": points}


@router.get("/insights", response_model=InsightsResponse)
def get_insights(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    """Jarvis base: daily and month-to-date comparatives."""

    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    points = repos_mart.insights_base(role, tenant, filial, dt_ini, dt_fim)
    return {"points": points}
