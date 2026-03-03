from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.deps import get_current_claims
from app.scope import resolve_scope
from app import repos_mart

router = APIRouter(prefix="/bi", tags=["bi"])


@router.get("/filiais")
def get_filiais(
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, _ = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=None)
    return {"items": repos_mart.list_filiais(role, tenant)}


# ------------------------
# Dashboard Geral
# ------------------------

@router.get("/dashboard/overview")
def dashboard_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    return {
        "kpis": repos_mart.dashboard_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.dashboard_series(role, tenant, filial, dt_ini, dt_fim),
        "insights": repos_mart.insights_base(role, tenant, filial, dt_ini, dt_fim),
        "insights_generated": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=20),
        "risk": {
            "kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
            "by_day": repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim),
        },
        "operational_score": repos_mart.operational_score(role, tenant, filial, dt_ini, dt_fim),
        "jarvis": repos_mart.jarvis_briefing(role, tenant, filial, dt_ref=dt_fim),
    }


# ------------------------
# Vendas & Stores
# ------------------------

@router.get("/sales/overview")
def sales_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    return {
        "kpis": repos_mart.dashboard_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.dashboard_series(role, tenant, filial, dt_ini, dt_fim),
        "by_hour": repos_mart.sales_by_hour(role, tenant, filial, dt_ini, dt_fim),
        "top_products": repos_mart.sales_top_products(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "top_groups": repos_mart.sales_top_groups(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "top_employees": repos_mart.sales_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=10),
    }


# ------------------------
# Anti-fraude
# ------------------------

@router.get("/fraud/overview")
def fraud_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    return {
        "kpis": repos_mart.fraud_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.fraud_series(role, tenant, filial, dt_ini, dt_fim),
        "top_users": repos_mart.fraud_top_users(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "last_events": repos_mart.fraud_last_events(role, tenant, filial, limit=30),
        "risk_kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
        "risk_by_day": repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim),
        "risk_top_employees": repos_mart.risk_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "risk_by_turn_local": repos_mart.risk_by_turn_local(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "risk_last_events": repos_mart.risk_last_events(role, tenant, filial, limit=30),
        "insights": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=15),
    }


@router.get("/risk/overview")
def risk_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    status: Optional[str] = Query(None, description="NOVO/LIDO/RESOLVIDO"),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    return {
        "kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim),
        "top_employees": repos_mart.risk_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "by_turn_local": repos_mart.risk_by_turn_local(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "last_events": repos_mart.risk_last_events(role, tenant, filial, limit=30),
        "insights": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, status=status, limit=30),
        "operational_score": repos_mart.operational_score(role, tenant, filial, dt_ini, dt_fim),
    }


# ------------------------
# Clientes
# ------------------------

@router.get("/customers/overview")
def customers_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    return {
        "top_customers": repos_mart.customers_top(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "rfm": repos_mart.customers_rfm_snapshot(role, tenant, filial, as_of=dt_fim),
        "churn_top": repos_mart.customers_churn_risk(role, tenant, filial, min_score=60, limit=10),
    }


# ------------------------
# Financeiro
# ------------------------

@router.get("/finance/overview")
def finance_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    return {
        "kpis": repos_mart.finance_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.finance_series(role, tenant, filial, dt_ini, dt_fim),
    }


# ------------------------
# Metas & Equipe
# ------------------------

@router.get("/goals/overview")
def goals_overview(
    dt_ini: date,
    dt_fim: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)

    # For goals, it makes sense to require a branch for now.
    filial_for_goals = filial or 1

    return {
        "leaderboard": repos_mart.leaderboard_employees(role, tenant, filial, dt_ini, dt_fim, limit=20),
        "goals_today": repos_mart.goals_today(role, tenant, filial_for_goals, goal_date=dt_fim),
    }


# ------------------------
# Jarvis briefing
# ------------------------

@router.get("/jarvis/briefing")
def jarvis_briefing(
    dt_ref: date,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return repos_mart.jarvis_briefing(role, tenant, filial, dt_ref=dt_ref)
