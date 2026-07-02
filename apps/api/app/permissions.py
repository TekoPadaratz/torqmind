"""Central screen-level authorization for TorqMind BI.

Every BI route maps to a ``screen_key``.  Access is granted based on:

1. **Role defaults** — platform_master / tenant_admin get all screens.
2. **Explicit permissions** — tenant_manager / tenant_viewer / tenant_kiosk
   require rows in ``auth.user_screen_permissions``.
3. **Sensitive-field redaction** — margin/profit/cost fields are stripped
   from API responses for roles without financial visibility.

Usage in routes::

    from app.permissions import require_screen, redact_sensitive

    @router.get("/sales/overview")
    def sales_overview(
        ...,
        claims=Depends(get_current_claims),
        _screen=Depends(require_screen("sales")),
    ):
        data = build_payload(...)
        return redact_sensitive(data, claims)
"""
from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List, Optional, Set

from fastapi import Depends, HTTPException

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Screen registry  (add new screens here — no migration needed)
# ──────────────────────────────────────────────────────────────────────
SCREEN_REGISTRY: Dict[str, Dict[str, Any]] = {
    "dashboard_home": {
        "label": "Dashboard",
        "category": "BI",
        "has_sensitive": True,
    },
    "sales": {
        "label": "Vendas",
        "category": "BI",
        "has_sensitive": True,
    },
    "cash": {
        "label": "Caixa",
        "category": "Operação",
        "has_sensitive": True,
    },
    "fraud": {
        "label": "Antifraude",
        "category": "Operação",
        "has_sensitive": False,
    },
    "finance": {
        "label": "Financeiro",
        "category": "Financeiro",
        "has_sensitive": True,
    },
    "customers": {
        "label": "Clientes",
        "category": "Comercial",
        "has_sensitive": False,
    },
    "competitor_pricing": {
        "label": "Preço Concorrente",
        "category": "Comercial",
        "has_sensitive": False,
    },
    "goals_team": {
        "label": "Metas & Equipe",
        "category": "Equipe",
        "has_sensitive": True,
    },
    "profit_management": {
        "label": "Gestão de Lucro",
        "category": "Financeiro",
        "has_sensitive": True,
    },
    "platform": {
        "label": "Plataforma",
        "category": "Administração Global",
        "platform_only": True,
    },
    "user_management": {
        "label": "Usuários",
        "category": "Administração",
        "platform_only": True,
    },
    "tv_sales_hourly": {
        "label": "TV – Vendas por Hora",
        "category": "TV",
        "has_sensitive": False,
        "kiosk_only": True,
    },
    "tv_sales_ranking": {
        "label": "TV – Ranking Vendedores",
        "category": "TV",
        "has_sensitive": False,
        "kiosk_only": True,
    },
}

# Keys available to each role *by default* (without explicit DB rows).
# tenant_manager / tenant_viewer / tenant_kiosk must have explicit rows.
_ALL_PRODUCT_SCREENS = {
    k for k, v in SCREEN_REGISTRY.items()
    if not v.get("platform_only") and not v.get("kiosk_only")
}

_ALL_SCREENS = set(SCREEN_REGISTRY.keys())

_TV_SCREENS = {
    k for k, v in SCREEN_REGISTRY.items()
    if v.get("kiosk_only")
}

ROLE_DEFAULT_SCREENS: Dict[str, Set[str]] = {
    "platform_master": _ALL_SCREENS,
    "platform_admin": _ALL_SCREENS - {"platform"},  # no finance but has ops
    "product_global": _ALL_PRODUCT_SCREENS,
    "channel_admin": _ALL_PRODUCT_SCREENS | {"user_management"},
    "tenant_admin": _ALL_PRODUCT_SCREENS,
}
# tenant_manager, tenant_viewer, tenant_kiosk → from DB only

# ──────────────────────────────────────────────────────────────────────
# Sensitive field names (lowercase) to redact for non-financial roles
# ──────────────────────────────────────────────────────────────────────
SENSITIVE_FIELD_NAMES: Set[str] = {
    "margem",
    "margem_total",
    "s_margem",
    "margin",
    "lucro",
    "profit",
    "cmv",
    "custo",
    "custo_medio",
    "custo_total",
    "custo_unitario",
    "cost",
    "markup",
    "rentabilidade",
    "rentab",
    "margem_acumulada",
    "margem_percentual",
    "margin_10d",
    "margem_score",
    "profit_margin",
    "gross_margin",
}

# Stems for substring-based redaction — any key whose lowercased name
# contains one of these stems is treated as sensitive.
_SENSITIVE_STEMS: tuple[str, ...] = (
    "margem", "margin", "lucro", "profit", "cmv",
    "custo", "cost", "markup", "rentab",
)

# Roles that can see sensitive financial data
_FINANCIAL_ROLES: Set[str] = {
    "platform_master",
    "platform_admin",
    "product_global",
    "tenant_admin",
}


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────

def get_allowed_screens(claims: dict[str, Any]) -> Set[str]:
    """Return the set of screen_keys this user can access.

    For platform/admin/owner roles the default set is returned.
    For tenant_manager/viewer/kiosk the set is read from the JWT claims
    which were populated during session building.
    """
    cached = claims.get("allowed_screens")
    if cached is not None:
        return set(cached)

    user_role: str = claims.get("user_role") or ""
    defaults = ROLE_DEFAULT_SCREENS.get(user_role)
    if defaults is not None:
        return defaults

    # Fallback: should not happen if session builder ran correctly
    return set()


def can_access_screen(claims: dict[str, Any], screen_key: str) -> bool:
    return screen_key in get_allowed_screens(claims)


def can_view_sensitive_financials(claims: dict[str, Any]) -> bool:
    """True if the user may see margin / profit / cost fields."""
    cached = claims.get("can_view_sensitive_financials")
    if cached is not None:
        return bool(cached)
    return (claims.get("user_role") or "") in _FINANCIAL_ROLES


def is_kiosk_user(claims: dict[str, Any]) -> bool:
    return (claims.get("user_role") or "") == "tenant_kiosk"


def resolve_default_route(claims: dict[str, Any]) -> str:
    """Determine the home page path for this user."""
    user_role = claims.get("user_role") or ""

    if user_role == "tenant_kiosk":
        screens = get_allowed_screens(claims)
        if "tv_sales_hourly" in screens:
            return "/tv/sales-hourly"
        if "tv_sales_ranking" in screens:
            return "/tv/sales-ranking"
        return "/tv"

    if user_role in _FINANCIAL_ROLES or user_role in {"tenant_manager", "tenant_viewer"}:
        screens = get_allowed_screens(claims)
        # First product screen in product order
        ordered = [
            "dashboard_home", "sales", "cash", "fraud",
            "customers", "finance", "competitor_pricing", "goals_team",
        ]
        for key in ordered:
            if key in screens:
                route_map = {
                    "dashboard_home": "/dashboard",
                    "sales": "/sales",
                    "cash": "/cash",
                    "fraud": "/fraud",
                    "customers": "/customers",
                    "finance": "/finance",
                    "competitor_pricing": "/pricing",
                    "goals_team": "/goals",
                }
                return route_map[key]

    return "/dashboard"


# ──────────────────────────────────────────────────────────────────────
# Sensitive field redaction
# ──────────────────────────────────────────────────────────────────────

def redact_sensitive(data: Any, claims: dict[str, Any]) -> Any:
    """Recursively remove / zero-out sensitive financial fields.

    Only applied when ``can_view_sensitive_financials(claims)`` is False.
    Returns the (possibly modified) data — mutates in place for dicts/lists.
    """
    if can_view_sensitive_financials(claims):
        return data
    return _redact(data)


def _is_sensitive_key(key: str) -> bool:
    """Return True if *key* is a known sensitive financial field."""
    lower = key.lower()
    if lower in SENSITIVE_FIELD_NAMES:
        return True
    return any(stem in lower for stem in _SENSITIVE_STEMS)


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            if _is_sensitive_key(key):
                obj[key] = None
            else:
                _redact(obj[key])
    elif isinstance(obj, list):
        for item in obj:
            _redact(item)
    return obj


# ──────────────────────────────────────────────────────────────────────
# FastAPI dependency: require_screen("screen_key")
# ──────────────────────────────────────────────────────────────────────

def require_screen(screen_key: str):
    """Return a FastAPI Depends-compatible callable that raises 403
    if the current user cannot access *screen_key*."""

    def _check(claims: dict[str, Any] = Depends(_get_claims_ref())):
        if not can_access_screen(claims, screen_key):
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "screen_access_denied",
                    "message": f"Acesso negado à tela '{screen_key}'.",
                    "screen_key": screen_key,
                },
            )

    return _check


def require_not_kiosk():
    """Return a FastAPI Depends-compatible callable that raises 403
    if the current user is a kiosk (tenant_kiosk) session."""

    def _check(claims: dict[str, Any] = Depends(_get_claims_ref())):
        if (claims.get("user_role") or "") == "tenant_kiosk":
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "kiosk_not_allowed",
                    "message": "Endpoint não disponível para modo TV.",
                },
            )

    return _check


def _get_claims_ref():
    """Lazy import to avoid circular dependency with deps.py."""
    from app.deps import get_current_claims
    return get_current_claims


# ──────────────────────────────────────────────────────────────────────
# Allowed screen_keys per role group (for validation on save)
# ──────────────────────────────────────────────────────────────────────

_ALLOWED_SCREENS_BY_ROLE: Dict[str, Set[str]] = {
    "tenant_kiosk": _TV_SCREENS,
    "tenant_manager": _ALL_PRODUCT_SCREENS,
    "tenant_viewer": _ALL_PRODUCT_SCREENS,
}


def validate_screen_permissions_for_role(role: str, screen_keys: List[str]) -> List[str]:
    """Validate that screen_keys are allowed for the given role.

    Raises HTTPException(422) if any disallowed key is found.
    Returns the validated list.
    """
    allowed = _ALLOWED_SCREENS_BY_ROLE.get(role)
    if allowed is None:
        return screen_keys  # admin/owner roles — no restriction

    disallowed = [k for k in screen_keys if k not in allowed]
    if disallowed:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "screen_permission_invalid",
                "message": f"Telas não permitidas para role '{role}': {', '.join(sorted(disallowed))}.",
                "disallowed": sorted(disallowed),
            },
        )
    return screen_keys


# ──────────────────────────────────────────────────────────────────────
# DB helpers  (used by session builder in repos_auth.py)
# ──────────────────────────────────────────────────────────────────────

def load_user_screen_permissions(conn, user_id: str) -> Set[str]:
    """Read explicit screen permissions from auth.user_screen_permissions."""
    cur = conn.execute(
        "SELECT screen_key FROM auth.user_screen_permissions WHERE user_id = %s::uuid",
        (user_id,),
    )
    return {row["screen_key"] for row in cur.fetchall()}


def save_user_screen_permissions(conn, user_id: str, screen_keys: List[str]) -> None:
    """Replace all screen permissions for a user (within current transaction)."""
    conn.execute(
        "DELETE FROM auth.user_screen_permissions WHERE user_id = %s::uuid",
        (user_id,),
    )
    for key in screen_keys:
        if key in SCREEN_REGISTRY:
            conn.execute(
                """INSERT INTO auth.user_screen_permissions (user_id, screen_key)
                   VALUES (%s::uuid, %s)
                   ON CONFLICT (user_id, screen_key) DO NOTHING""",
                (user_id, key),
            )
