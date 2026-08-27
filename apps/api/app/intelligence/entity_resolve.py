"""Wrappers de resolução de entidades (cliente) — nunca só por id_entidade."""

from __future__ import annotations

from typing import Any

from app.intelligence.tools.executor import execute_tool


def search_customers(
    claims: dict[str, Any],
    scope: dict[str, Any],
    name: str,
    *,
    evidence=None,
) -> dict[str, Any]:
    """Busca 3–5 candidatos com documento mascarado via tool allowlisted."""
    return execute_tool(
        "customer.search",
        {"customer_name": name, "search": name},
        claims,
        scope,
        evidence=evidence,
    )
