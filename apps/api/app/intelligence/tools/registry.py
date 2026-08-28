"""Allowlist de tools do Assistente — somente leitura, sem SQL do usuário."""

from __future__ import annotations

from typing import Any, Callable


WRITE_BLOCKLIST: frozenset[str] = frozenset(
    {
        "competitor_pricing_upsert",
        "upsert_goal",
        "budget_config_upsert",
        "solvencia_manual_upsert",
        "team_employee_cost_upsert",
        "notification_create",
        "notifications_mark_read",
        "notifications_mark_all_read",
        "generate_jarvis_ai_plans",
        "ai_usage_summary",
        "insert_insight",
        "save_insight",
        "upsert_commission",
        "manager_commission_upsert",
        "cliente_preco_fixo_upsert",
        "alert_preco_fixo_upsert",
    }
)


def _spec(
    *,
    version: str = "1",
    analytics_fn: str | None = None,
    handler: str | None = None,
    screens: list[str] | None = None,
    requires_sensitive: bool = False,
    max_rows: int = 50,
    timeout_s: int = 12,
) -> dict[str, Any]:
    return {
        "version": version,
        "analytics_fn": analytics_fn,
        "handler": handler,
        "screens": screens or [],
        "requires_sensitive": requires_sensitive,
        "max_rows": max_rows,
        "timeout_s": timeout_s,
        "write": False,
    }


TOOLS: dict[str, dict[str, Any]] = {
    "sales.overview": _spec(analytics_fn="sales_overview_bundle", screens=["sales", "sales.overview"]),
    "sales.hourly": _spec(analytics_fn="sales_by_hour", screens=["sales", "sales.hourly"]),
    "sales.products": _spec(analytics_fn="sales_top_products", screens=["sales", "sales.top"]),
    "sales.groups": _spec(analytics_fn="sales_top_groups", screens=["sales", "sales.top"]),
    "sales.payments": _spec(analytics_fn="payments_overview", screens=["sales", "cash"]),
    "sales.abc": _spec(analytics_fn="sales_abc_curve", screens=["sales", "sales.abc"]),
    "customer.search": _spec(
        analytics_fn="customers_summary_paginated",
        handler="customer_search",
        screens=["customers"],
        max_rows=5,
    ),
    "customer.overview": _spec(analytics_fn="customers_summary_paginated", screens=["customers"]),
    "customer.churn": _spec(analytics_fn="customers_churn_bundle", screens=["customers"]),
    "customer.delinquency": _spec(analytics_fn="customers_delinquency_overview", screens=["customers"]),
    "customer.open_titles": _spec(
        analytics_fn="finance_titles_overview",
        handler="open_titles",
        screens=["finance.receivable", "customers", "finance"],
    ),
    "finance.overview": _spec(analytics_fn="finance_kpis", screens=["finance", "finance.overview"]),
    "finance.titles": _spec(analytics_fn="finance_titles_overview", handler="finance_titles", screens=["finance"]),
    "finance.expenses": _spec(analytics_fn="finance_despesas_overview", screens=["finance.despesas", "finance"]),
    "finance.payments": _spec(analytics_fn="payments_overview", screens=["finance", "sales"]),
    "finance.cheques": _spec(
        analytics_fn="cheques_pendentes_overview",
        handler="cheques_or_unsupported",
        screens=["finance.cheques", "finance"],
    ),
    "cash.overview": _spec(analytics_fn="cash_overview", screens=["cash"]),
    "risk.events": _spec(
        analytics_fn="risk_last_events",
        handler="risk_events",
        screens=["fraud", "fraud.core", "fraud.risco_financeiro"],
    ),
    "risk.cancellations": _spec(analytics_fn="fraud_kpis", screens=["fraud", "fraud.core"]),
    "risk.credit_sales": _spec(
        analytics_fn="fraud_credito_funcionario",
        handler="credit_sales",
        screens=["fraud.credito_funcionario", "fraud"],
    ),
    "inventory.fuel": _spec(analytics_fn="inventory_fuel_overview", screens=["inventory"]),
    "inventory.loss": _spec(analytics_fn="inventory_fuel_loss_overview", screens=["fuel_loss", "inventory"]),
    "inventory.measurements": _spec(
        analytics_fn="inventory_fuel_afericoes_overview",
        screens=["inventory"],
    ),
    "inventory.products": _spec(handler="unsupported", screens=["inventory"]),
    "pricing.competitors": _spec(
        analytics_fn="competitor_pricing_overview",
        screens=["competitor_pricing"],
    ),
    "goals.overview": _spec(
        analytics_fn="goals_today",
        handler="goals_overview",
        screens=["goals_team", "goals_team.metas"],
    ),
    "goals.pace": _spec(analytics_fn="monthly_goal_projection", screens=["goals_team", "goals_team.metas"]),
    "team.overview": _spec(
        analytics_fn="sales_top_employees",
        handler="team_overview",
        screens=["team", "goals_team"],
    ),
    "team.commissions_readonly": _spec(
        handler="commissions_readonly",
        screens=["goals_team.comissoes", "goals_team"],
    ),
    "profit.overview": _spec(
        handler="profit_overview",
        screens=["profit_management", "profit_management.overview"],
        requires_sensitive=True,
    ),
    "profit.dre": _spec(
        handler="profit_overview",
        screens=["profit_management", "profit_management.overview"],
        requires_sensitive=True,
    ),
    "profit.products": _spec(
        handler="profit_overview",
        screens=["profit_management", "profit_management.products"],
        requires_sensitive=True,
    ),
    "profit.repricing": _spec(
        handler="profit_overview",
        screens=["profit_management", "profit_management.repricing"],
        requires_sensitive=True,
    ),
    "profit.solvency": _spec(
        handler="profit_overview",
        screens=["profit_management", "profit_management.solvencia"],
        requires_sensitive=True,
    ),
    "anp.reference": _spec(
        handler="anp_reference",
        screens=["profit_management.anp", "profit_management"],
        requires_sensitive=True,
    ),
    "data.freshness": _spec(analytics_fn="streaming_health", handler="data_freshness", screens=["sales"]),
    "navigation.resolve": _spec(handler="navigation_resolve", screens=["assistant"]),
    "assistant.capabilities": _spec(handler="assistant_capabilities", screens=["assistant"]),
    "action.plan_revenue_drop": _spec(handler="playbook_revenue_drop", screens=["sales"]),
    "action.plan_delinquency": _spec(handler="playbook_delinquency", screens=["customers", "finance"]),
    "action.plan_mix": _spec(handler="playbook_mix", screens=["sales"]),
    "action.plan_goals": _spec(handler="playbook_goals", screens=["goals_team", "goals_team.metas"]),
    "action.plan_idle_hours": _spec(handler="playbook_idle_hours", screens=["sales", "team"]),
    "meta.what_can_i_ask": _spec(handler="assistant_capabilities", screens=["assistant"]),
    "meta.explain_metric": _spec(handler="navigation_resolve", screens=["assistant"]),
    "meta.mutation_denied": _spec(handler="unsupported", screens=["assistant"]),
    "meta.unsupported": _spec(handler="unsupported", screens=["assistant"]),
}


def get_tool(tool_name: str) -> dict[str, Any] | None:
    spec = TOOLS.get(tool_name)
    if not spec:
        return None
    out = dict(spec)
    out["name"] = tool_name
    out["tool_name"] = tool_name
    return out


def assert_no_write(fn_name: str) -> None:
    if fn_name in WRITE_BLOCKLIST:
        raise RuntimeError(f"write function blocked: {fn_name}")
