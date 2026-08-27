"""Contexto opaco de conversa (disambiguação / slots / período)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.intelligence.authz import permission_hash


OPAQUE_KEYS = (
    "pending_disambiguation",
    "last_intent",
    "last_slots",
    "last_period",
    "last_entities",
)


def empty_context() -> dict[str, Any]:
    return {
        "pending_disambiguation": None,
        "last_intent": None,
        "last_slots": {},
        "last_period": None,
        "last_entities": [],
        "permission_hash": "",
        "branch_scope": [],
    }


def normalize_context(raw: dict[str, Any] | None) -> dict[str, Any]:
    base = empty_context()
    if not isinstance(raw, dict):
        return base
    for key in OPAQUE_KEYS:
        if key in raw:
            base[key] = deepcopy(raw.get(key))
    if "permission_hash" in raw:
        base["permission_hash"] = str(raw.get("permission_hash") or "")
    if "branch_scope" in raw:
        base["branch_scope"] = list(raw.get("branch_scope") or [])
    return base


def invalidate_if_scope_changed(
    context: dict[str, Any],
    claims: dict[str, Any],
    branch_scope: list[Any] | None,
) -> dict[str, Any]:
    ctx = normalize_context(context)
    ph = permission_hash(claims)
    branches = list(branch_scope or [])
    if ctx.get("permission_hash") and ctx["permission_hash"] != ph:
        return {**empty_context(), "permission_hash": ph, "branch_scope": branches}
    if ctx.get("branch_scope") and list(ctx.get("branch_scope") or []) != branches:
        return {**empty_context(), "permission_hash": ph, "branch_scope": branches}
    ctx["permission_hash"] = ph
    ctx["branch_scope"] = branches
    return ctx


def update_after_turn(
    context: dict[str, Any],
    *,
    intent_id: str | None,
    slots: dict[str, Any] | None,
    period: dict[str, Any] | None,
    entities: list[Any] | None,
    pending: dict[str, Any] | None,
) -> dict[str, Any]:
    ctx = normalize_context(context)
    ctx["last_intent"] = intent_id
    ctx["last_slots"] = dict(slots or {})
    ctx["last_period"] = period
    ctx["last_entities"] = list(entities or [])
    ctx["pending_disambiguation"] = pending
    return ctx
