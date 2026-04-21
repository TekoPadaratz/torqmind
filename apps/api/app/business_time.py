from __future__ import annotations

import json
from functools import lru_cache
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from app.config import settings


@lru_cache(maxsize=1)
def _tenant_timezone_map() -> dict[str, str]:
    raw = str(settings.business_tenant_timezones or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, str] = {}
    for tenant_id, timezone_name in payload.items():
        key = str(tenant_id).strip()
        value = str(timezone_name or "").strip()
        if key and value:
            normalized[key] = value
    return normalized


@lru_cache(maxsize=64)
def _zoneinfo(timezone_name: str) -> ZoneInfo:
    return ZoneInfo(timezone_name)


def business_timezone_name(tenant_id: int | None = None) -> str:
    tenant_key = None if tenant_id is None else str(int(tenant_id))
    tenant_map = _tenant_timezone_map()
    if tenant_key and tenant_key in tenant_map:
        return tenant_map[tenant_key]
    configured = str(settings.business_timezone or "").strip()
    return configured or "America/Sao_Paulo"


def business_timezone(tenant_id: int | None = None) -> ZoneInfo:
    return _zoneinfo(business_timezone_name(tenant_id))


def coerce_operational_datetime(value: datetime | None, tenant_id: int | None = None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=business_timezone(tenant_id))
    return value


def business_now(tenant_id: int | None = None) -> datetime:
    return datetime.now(timezone.utc).astimezone(business_timezone(tenant_id))


def business_today(tenant_id: int | None = None) -> date:
    return business_now(tenant_id).date()


def business_date_for_datetime(value: datetime | None, tenant_id: int | None = None) -> date:
    if value is None:
        return business_today(tenant_id)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(business_timezone(tenant_id)).date()


def resolve_business_date(dt_ref: date | None, tenant_id: int | None = None) -> date:
    return dt_ref or business_today(tenant_id)


def business_clock_payload(tenant_id: int | None = None) -> dict[str, str]:
    now_local = business_now(tenant_id)
    return {
        "timezone": business_timezone_name(tenant_id),
        "business_now": now_local.isoformat(),
        "business_date": now_local.date().isoformat(),
    }
