"""Capability map loader — single source of truth accessors."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional

_CATALOG_PATH = Path(__file__).resolve().parent / "catalog_v1.json"

# Screens that expose sensitive financials (margin/profit/cost).
_SENSITIVE_SCREEN_PREFIXES = (
    "profit_management",
    "goals_team.gerente",
)


@lru_cache(maxsize=1)
def load_catalog() -> dict[str, Any]:
    """Load and cache catalog_v1.json (UTF-8)."""
    raw = _CATALOG_PATH.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict) or "intents" not in data:
        raise ValueError("catalog_v1.json inválido: falta 'intents'")
    return data


def list_intents() -> list[dict[str, Any]]:
    """Return all intent records from the catalog."""
    return list(load_catalog().get("intents") or [])


def get_intent(intent_id: str) -> Optional[dict[str, Any]]:
    """Return one intent by id, or None."""
    if not intent_id:
        return None
    for item in list_intents():
        if item.get("intent_id") == intent_id:
            return item
    return None


def intents_for_screens(screens: Iterable[str]) -> list[dict[str, Any]]:
    """Intents whose screen_key is in the allowed screen set (or parent match)."""
    allowed = {str(s) for s in screens if s}
    if not allowed:
        return []
    out: list[dict[str, Any]] = []
    for intent in list_intents():
        sk = intent.get("screen_key")
        if not sk:
            # Meta / navigation without a product screen — include for capability help
            if intent.get("intent_id", "").startswith(("meta.", "assistant.", "navigation.", "data.")):
                out.append(intent)
            continue
        if sk in allowed:
            out.append(intent)
            continue
        # parent screen grants child panels (e.g. sales → sales.abc)
        parent = sk.split(".", 1)[0] if "." in sk else None
        if parent and parent in allowed:
            out.append(intent)
    return out


def suggestions_for_claims(
    claims_screens: Iterable[str],
    can_sensitive: bool,
    *,
    limit: int = 12,
) -> list[dict[str, str]]:
    """Return short suggestion chips for the chat UI based on ACL.

    Each item: {intent_id, text, deep_link_key?} from synonyms[0] / follow-ups.
    Sensitive intents are omitted unless can_sensitive is True.
    Kiosk-hidden intents are omitted when 'tenant_kiosk' screens-only TV set.
    """
    screens = {str(s) for s in claims_screens if s}
    suggestions: list[dict[str, str]] = []
    for intent in intents_for_screens(screens):
        if intent.get("unsupported"):
            continue
        if intent.get("requires_sensitive_role") and not can_sensitive:
            continue
        if intent.get("hidden_from_kiosk") and _looks_kiosk_only(screens):
            continue
        sk = intent.get("screen_key")
        if sk and any(sk.startswith(p) for p in _SENSITIVE_SCREEN_PREFIXES) and not can_sensitive:
            continue
        synonyms = intent.get("synonyms") or []
        label = synonyms[0] if synonyms else intent.get("intent_id", "")
        text = f"Sobre {label}?" if label else intent.get("intent_id", "")
        item = {
            "intent_id": str(intent.get("intent_id") or ""),
            "text": text,
        }
        if intent.get("deep_link_key"):
            item["deep_link_key"] = str(intent["deep_link_key"])
        suggestions.append(item)
        if len(suggestions) >= limit:
            break
    return suggestions


def _looks_kiosk_only(screens: set[str]) -> bool:
    if not screens:
        return False
    return all(s.startswith("tv_") or s == "tv" for s in screens)


def clear_catalog_cache() -> None:
    """Test helper: drop cached catalog."""
    load_catalog.cache_clear()
