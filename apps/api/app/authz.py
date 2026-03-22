from __future__ import annotations

from datetime import date
from typing import Any

CANONICAL_ROLES = {
    "platform_master",
    "platform_admin",
    "product_global",
    "channel_admin",
    "tenant_admin",
    "tenant_manager",
    "tenant_viewer",
}

LEGACY_ROLE_MAP = {
    "MASTER": "platform_master",
    "OWNER": "tenant_admin",
    "MANAGER": "tenant_manager",
}

ROLE_PRIORITY = {
    "platform_master": 0,
    "platform_admin": 1,
    "product_global": 2,
    "channel_admin": 3,
    "tenant_admin": 4,
    "tenant_manager": 5,
    "tenant_viewer": 6,
}

TENANT_LOGIN_ALLOWED_STATUSES = {
    "active",
    "trial",
    "overdue",
    "grace",
    "suspended_readonly",
}


def normalize_role(role: str | None) -> str:
    value = str(role or "").strip()
    if not value:
        return ""
    return LEGACY_ROLE_MAP.get(value, value.lower())


def role_priority(role: str | None) -> int:
    return ROLE_PRIORITY.get(normalize_role(role), 99)


def analytics_role_for_user_role(role: str | None) -> str | None:
    normalized = normalize_role(role)
    if normalized in {"platform_master", "platform_admin", "product_global"}:
        return "MASTER"
    if normalized == "tenant_admin":
        return "OWNER"
    if normalized in {"tenant_manager", "tenant_viewer"}:
        return "MANAGER"
    return None


def can_access_platform(role: str | None) -> bool:
    return normalize_role(role) in {"platform_master", "platform_admin", "channel_admin"}


def can_manage_platform_operations(role: str | None) -> bool:
    return normalize_role(role) in {"platform_master", "platform_admin"}


def can_manage_platform_finance(role: str | None) -> bool:
    return normalize_role(role) == "platform_master"


def can_access_product(role: str | None) -> bool:
    return normalize_role(role) in {
        "platform_master",
        "platform_admin",
        "product_global",
        "tenant_admin",
        "tenant_manager",
        "tenant_viewer",
    }


def is_product_readonly_role(role: str | None) -> bool:
    return normalize_role(role) == "tenant_viewer"


def role_label(role: str | None) -> str:
    normalized = normalize_role(role)
    return {
        "platform_master": "Platform Master",
        "platform_admin": "Platform Admin",
        "product_global": "Product Global User",
        "channel_admin": "Channel Admin",
        "tenant_admin": "Tenant Admin",
        "tenant_manager": "Tenant Manager",
        "tenant_viewer": "Tenant Viewer",
    }.get(normalized, normalized or "Unknown")


def is_date_in_window(target: date, valid_from: date | None, valid_until: date | None) -> bool:
    if valid_from and target < valid_from:
        return False
    if valid_until and target > valid_until:
        return False
    return True


def tenant_status_allows_login(status: str | None) -> bool:
    value = str(status or "active")
    return value in TENANT_LOGIN_ALLOWED_STATUSES


def tenant_status_is_warning(status: str | None) -> bool:
    return str(status or "") in {"overdue", "grace", "suspended_readonly"}


def tenant_status_warning_message(status: str | None) -> str | None:
    value = str(status or "")
    if value == "overdue":
        return "Empresa com cobrança em atraso."
    if value == "grace":
        return "Empresa em período de carência comercial."
    if value == "suspended_readonly":
        return "Empresa em modo leitura por bloqueio comercial."
    return None


def claims_access_flag(claims: dict[str, Any], key: str) -> bool:
    access = claims.get("access") or {}
    return bool(access.get(key))
