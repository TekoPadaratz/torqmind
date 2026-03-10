from __future__ import annotations

from datetime import date
from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from fastapi import HTTPException
from pydantic import BaseModel, Field

from app.deps import get_current_claims
from app.scope import resolve_scope
from app import repos_mart
from app.services.jarvis_ai import ai_usage_summary, generate_jarvis_ai_plans
from app.services.telegram import send_telegram_alert

router = APIRouter(prefix="/bi", tags=["bi"])


class CompetitorPriceItem(BaseModel):
    id_produto: int
    competitor_price: float = Field(..., gt=0)


class CompetitorPriceUpsertRequest(BaseModel):
    items: List[CompetitorPriceItem] = Field(default_factory=list)


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
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    as_of = dt_ref or dt_fim

    return {
        "kpis": repos_mart.dashboard_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.dashboard_series(role, tenant, filial, dt_ini, dt_fim),
        "insights": repos_mart.insights_base(role, tenant, filial, dt_ini, dt_fim),
        "insights_generated": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=20),
        "payments": repos_mart.payments_overview(role, tenant, filial, dt_ini, dt_fim, anomaly_limit=8),
        "open_cash": repos_mart.open_cash_monitor(role, tenant, filial),
        "risk": {
            "kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
            "by_day": repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim),
            "window": repos_mart.risk_data_window(role, tenant, filial),
        },
        "operational_score": repos_mart.operational_score(role, tenant, filial, dt_ini, dt_fim),
        "health_score": repos_mart.health_score_latest(role, tenant, filial, as_of=as_of),
        "jarvis": repos_mart.jarvis_briefing(role, tenant, filial, dt_ref=as_of),
        "notifications_unread": repos_mart.notifications_unread_count(role, tenant, filial),
    }


# ------------------------
# Vendas & Stores
# ------------------------

@router.get("/sales/overview")
def sales_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
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
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
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
        "risk_window": repos_mart.risk_data_window(role, tenant, filial),
        "risk_top_employees": repos_mart.risk_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "risk_by_turn_local": repos_mart.risk_by_turn_local(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "risk_last_events": repos_mart.risk_last_events(role, tenant, filial, limit=30),
        "insights": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "payments_risk": repos_mart.payments_anomalies(role, tenant, filial, dt_ini, dt_fim, limit=20),
        "open_cash": repos_mart.open_cash_monitor(role, tenant, filial),
    }


@router.get("/risk/overview")
def risk_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
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
        "window": repos_mart.risk_data_window(role, tenant, filial),
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
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    as_of = dt_ref or dt_fim
    churn_diamond = repos_mart.customers_churn_diamond(role, tenant, filial, as_of=as_of, min_score=40, limit=10)
    churn_top = []
    for c in churn_diamond:
        freq_30 = int(c.get("frequency_30") or 0)
        freq_90 = int(c.get("frequency_90") or 0)
        mon_30 = float(c.get("monetary_30") or 0)
        mon_90 = float(c.get("monetary_90") or 0)
        churn_top.append(
            {
                "id_cliente": c.get("id_cliente"),
                "cliente_nome": c.get("cliente_nome"),
                "churn_score": c.get("churn_score"),
                "last_purchase": c.get("last_purchase"),
                "compras_30d": freq_30,
                "compras_60_30": max(0, freq_90 - freq_30),
                "faturamento_30d": mon_30,
                "faturamento_60_30": max(0.0, mon_90 - mon_30),
                "revenue_at_risk_30d": c.get("revenue_at_risk_30d"),
                "reasons": c.get("reasons"),
                "recommendation": c.get("recommendation"),
            }
        )

    return {
        "top_customers": repos_mart.customers_top(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "rfm": repos_mart.customers_rfm_snapshot(role, tenant, filial, as_of=as_of),
        "churn_top": churn_top,
        "anonymous_retention": repos_mart.anonymous_retention_overview(role, tenant, filial, dt_ini, dt_fim),
    }


@router.get("/clients/churn")
def clients_churn(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_cliente: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    min_score: int = Query(60, ge=0, le=100),
    limit: int = Query(20, ge=1, le=100),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    as_of = dt_ref or dt_fim
    top = repos_mart.customers_churn_diamond(role, tenant, filial, as_of=as_of, min_score=min_score, limit=limit)
    total_revenue_at_risk = float(sum(float(row.get("revenue_at_risk_30d") or 0) for row in top))
    avg_churn_score = (
        round(sum(float(row.get("churn_score") or 0) for row in top) / len(top), 2) if top else 0.0
    )

    drilldown = {"snapshot": {}, "series": []}
    if id_cliente is not None:
        drilldown = repos_mart.customer_churn_drilldown(
            role,
            tenant,
            filial,
            id_cliente=id_cliente,
            dt_ini=dt_ini,
            dt_fim=dt_fim,
            as_of=as_of,
        )

    return {
        "top_risk": top,
        "summary": {
            "total_top_risk": len(top),
            "avg_churn_score": avg_churn_score,
            "revenue_at_risk_30d": round(total_revenue_at_risk, 2),
        },
        "drilldown": drilldown,
    }


@router.get("/clients/retention-anonymous")
def clients_retention_anonymous(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return repos_mart.anonymous_retention_overview(role, tenant, filial, dt_ini, dt_fim)


# ------------------------
# Financeiro
# ------------------------

@router.get("/finance/overview")
def finance_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    as_of = dt_ref or dt_fim

    return {
        "kpis": repos_mart.finance_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.finance_series(role, tenant, filial, dt_ini, dt_fim),
        "aging": repos_mart.finance_aging_overview(role, tenant, filial, as_of=as_of),
        "payments": repos_mart.payments_overview(role, tenant, filial, dt_ini, dt_fim, anomaly_limit=10),
        "open_cash": repos_mart.open_cash_monitor(role, tenant, filial),
    }


@router.get("/payments/overview")
def payments_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return repos_mart.payments_overview(role, tenant, filial, dt_ini, dt_fim, anomaly_limit=30)


# ------------------------
# Precificacao Concorrencia
# ------------------------

@router.get("/pricing/competitor/overview")
def pricing_competitor_overview(
    dt_ini: date,
    dt_fim: date,
    days_simulation: int = Query(10, ge=1, le=60),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    if filial is None:
        raise HTTPException(status_code=400, detail="id_filial is required for competitor pricing simulation")

    return repos_mart.competitor_pricing_overview(
        role,
        tenant,
        filial,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        days_simulation=days_simulation,
    )


@router.post("/pricing/competitor/prices")
def pricing_competitor_prices_upsert(
    payload: CompetitorPriceUpsertRequest,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER", "MANAGER"}:
        raise HTTPException(status_code=403, detail="forbidden")

    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    if filial is None:
        raise HTTPException(status_code=400, detail="id_filial is required to save competitor prices")

    if not payload.items:
        return {"ok": True, "saved": 0}

    items = [{"id_produto": it.id_produto, "competitor_price": it.competitor_price} for it in payload.items]
    result = repos_mart.competitor_pricing_upsert(
        role,
        tenant,
        filial,
        items=items,
        updated_by=str(claims.get("sub") or ""),
    )
    return {"ok": True, **result}


# ------------------------
# Metas & Equipe
# ------------------------

@router.get("/goals/overview")
def goals_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    as_of = dt_ref or dt_fim

    # For goals, it makes sense to require a branch for now.
    filial_for_goals = filial or 1

    return {
        "leaderboard": repos_mart.leaderboard_employees(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "goals_today": repos_mart.goals_today(role, tenant, filial_for_goals, goal_date=as_of),
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


@router.post("/jarvis/generate")
def jarvis_generate(
    dt_ref: date,
    id_filial: Optional[int] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    force: bool = Query(False),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    stats = generate_jarvis_ai_plans(role, tenant, filial, dt_ref=dt_ref, limit=limit, force=force)
    return {
        "ok": True,
        "id_empresa": tenant,
        "id_filial": filial,
        "dt_ref": dt_ref.isoformat(),
        "stats": stats,
    }


@router.get("/admin/ai-usage")
def admin_ai_usage(
    days: int = Query(30, ge=1, le=365),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER"}:
        raise HTTPException(status_code=403, detail="forbidden")

    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return ai_usage_summary(role, tenant, filial, days=days)


@router.post("/admin/telegram/test")
def admin_telegram_test(
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER"}:
        raise HTTPException(status_code=403, detail="forbidden")

    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    payload = {
        "severity": "CRITICAL",
        "id_filial": filial,
        "event_type": "TELEGRAM_TEST",
        "event_time": (dt_ref or date.today()).isoformat(),
        "impacto_estimado": 0,
        "title": "Teste de alerta Telegram",
        "body": "Mensagem de teste enviada pelo endpoint /bi/admin/telegram/test",
        "url": "/dashboard",
    }
    result = send_telegram_alert(id_empresa=tenant, payload=payload, force=True)
    return {"ok": True, "id_empresa": tenant, "id_filial": filial, "result": result}


@router.get("/notifications")
def notifications_list(
    id_filial: Optional[int] = Query(None),
    unread_only: bool = Query(False),
    limit: int = Query(30, ge=1, le=200),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return {
        "items": repos_mart.notifications_list(role, tenant, filial, limit=limit, unread_only=unread_only),
        "unread": repos_mart.notifications_unread_count(role, tenant, filial),
    }


@router.post("/notifications/{notification_id}/read")
def notifications_mark_read(
    notification_id: int,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    row = repos_mart.notification_mark_read(role, tenant, filial, notification_id)
    return {"ok": True, "item": row}


@router.get("/notifications/unread-count")
def notifications_unread_count(
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    return {"unread": repos_mart.notifications_unread_count(role, tenant, filial)}
