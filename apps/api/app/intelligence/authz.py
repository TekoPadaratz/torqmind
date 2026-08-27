"""Autorização revalidada por tool (nunca confiar só no parse)."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from app.permissions import (
    can_access_screen,
    can_view_sensitive_financials,
    get_allowed_screens,
    is_kiosk_user,
)


def permission_hash(claims: dict[str, Any]) -> str:
    payload = {
        "role": claims.get("user_role") or claims.get("role") or "",
        "screens": sorted(get_allowed_screens(claims)),
        "sensitive": bool(can_view_sensitive_financials(claims)),
        "empresa": claims.get("id_empresa"),
        "filial": claims.get("id_filial"),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


def reauthorize(
    claims: dict[str, Any],
    tool_spec: dict[str, Any],
    screen_keys: list[str] | None = None,
) -> tuple[bool, str | None]:
    """Retorna (ok, reason_code)."""
    if is_kiosk_user(claims):
        return False, "kiosk_blocked"

    keys = list(screen_keys or tool_spec.get("screens") or [])
    if keys and not any(can_access_screen(claims, k) for k in keys):
        return False, "screen_denied"

    if tool_spec.get("requires_sensitive") and not can_view_sensitive_financials(claims):
        return False, "sensitive_denied"

    # managers nunca executam tools de lucro/custo
    name = str(tool_spec.get("name") or tool_spec.get("tool_name") or "")
    if name.startswith("profit.") and not can_view_sensitive_financials(claims):
        return False, "sensitive_denied"

    return True, None
