from __future__ import annotations

from datetime import datetime, timedelta, timezone


BUSINESS_TIMEZONE_NAME = "America/Sao_Paulo"
BUSINESS_TIMEZONE = timezone(timedelta(hours=-3), name=BUSINESS_TIMEZONE_NAME)


def ensure_business_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        # SQL Server datetimes arrive as naive local wall time. Use a stable
        # business offset instead of historical IANA offsets such as -03:06:28
        # for legacy sentinel values like 1900-01-01.
        return value.replace(tzinfo=BUSINESS_TIMEZONE)
    return value.astimezone(BUSINESS_TIMEZONE)


def business_datetime_iso(value: datetime, *, timespec: str = "microseconds") -> str:
    return ensure_business_datetime(value).isoformat(timespec=timespec)


def sqlserver_datetime_param(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    normalized = ensure_business_datetime(value)
    return normalized.replace(tzinfo=None)
