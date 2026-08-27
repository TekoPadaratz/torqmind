"""Deep links de produto para intents do assistente."""

from __future__ import annotations

from typing import Any


_FALLBACK = {
    "sales.overview": "/sales",
    "sales.hourly": "/sales",
    "sales.products": "/sales",
    "sales.groups": "/sales",
    "sales.payments": "/sales",
    "sales.abc": "/sales/abc",
    "customer.search": "/customers",
    "customer.overview": "/customers",
    "customer.churn": "/customers",
    "customer.delinquency": "/customers",
    "customer.open_titles": "/finance?view=receivable",
    "finance.overview": "/finance",
    "finance.titles": "/finance",
    "finance.expenses": "/finance?view=despesas",
    "finance.payments": "/finance",
    "finance.cheques": "/finance?view=cheques",
    "cash.overview": "/cash",
    "risk.events": "/fraud",
    "risk.cancellations": "/fraud",
    "risk.credit_sales": "/fraud",
    "inventory.fuel": "/inventory",
    "inventory.loss": "/fuel-loss",
    "inventory.measurements": "/inventory",
    "pricing.competitors": "/pricing",
    "goals.overview": "/goals",
    "goals.pace": "/goals",
    "team.overview": "/team",
    "team.commissions_readonly": "/goals?tab=comissoes",
    "profit.overview": "/profit-management",
    "anp.reference": "/profit-management",
    "assistant.capabilities": "/sales",
    "navigation.resolve": "/sales",
    "data.freshness": "/sales",
}


def deep_link_for_intent(intent_id: str | None, intent_meta: dict[str, Any] | None = None) -> str | None:
    meta = intent_meta or {}
    if meta.get("deep_link_key"):
        return str(meta["deep_link_key"])
    if intent_id and intent_id in _FALLBACK:
        return _FALLBACK[intent_id]
    return None
