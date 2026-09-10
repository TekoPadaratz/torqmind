"""Privileged MFA enforcement policy (prepared, default OFF).

Does not mutate production user rows. When ``mfa_enforce_privileged`` is false
(the default), login behaviour is unchanged for existing admins. Enabling the
flag later forces enrollment via the existing ``totp_required`` setup path
without a bulk UPDATE of cadastros.
"""
from __future__ import annotations

from typing import Any, Optional

from app.config import settings

# Roles that should eventually require MFA when enforcement is turned on.
PRIVILEGED_MFA_ROLES: frozenset[str] = frozenset(
    {
        "platform_master",
        "platform_admin",
        "product_global",
        "tenant_admin",
    }
)


def is_privileged_mfa_role(user_role: Optional[str]) -> bool:
    return (user_role or "").strip() in PRIVILEGED_MFA_ROLES


def enforcement_enabled() -> bool:
    return bool(getattr(settings, "mfa_enforce_privileged", False))


def effective_totp_required(
    mfa_state: Optional[dict[str, Any]],
    *,
    user_role: Optional[str] = None,
) -> bool:
    """Whether login must complete MFA setup before issuing an access token.

    Column ``totp_required`` always wins. Enforcement flag only adds privileged
    roles when enabled — never auto-writes the column.
    """
    state = mfa_state or {}
    if bool(state.get("totp_required")):
        return True
    if enforcement_enabled() and is_privileged_mfa_role(user_role):
        return True
    return False


def policy_snapshot(user_role: Optional[str], mfa_state: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Informational fields for status/UI — never blocks by itself."""
    state = mfa_state or {}
    privileged = is_privileged_mfa_role(user_role)
    return {
        "privileged_role": privileged,
        "enforcement_enabled": enforcement_enabled(),
        "policy_would_require_mfa": bool(
            privileged and not state.get("totp_enabled") and not state.get("totp_required")
        ),
    }
