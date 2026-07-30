"""WatermarkGuard — no future poison, no sentinel epochs, safe cursor commits."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from agent.state.watermark import WatermarkStore
from agent.utils.timezone import business_datetime_iso

# Reject absurd / placeholder timestamps often seen in dirty ERP rows.
_SENTINEL_YEARS = {1970, 1900, 1899, 1}

# Allow small clock skew between agent host and SQL Server.
DEFAULT_FUTURE_SLACK = timedelta(hours=24)


def parse_watermark_dt(value: Optional[str]) -> Optional[datetime]:
    return WatermarkStore.parse_watermark_dt(value)


def is_sentinel_datetime(dt: datetime) -> bool:
    return int(dt.year) in _SENTINEL_YEARS


def sanitize_temporal_watermark(
    watermark: Optional[str],
    *,
    dataset: str = "dataset",
    future_slack: timedelta = DEFAULT_FUTURE_SLACK,
    logger=None,
) -> Tuple[Optional[str], bool]:
    """Return (safe_watermark, was_clamped).

    - Future dates beyond now+slack → clamp to cap.
    - Sentinel years (1970/1900/…) → return None (caller must not advance cursor).
    """
    if not watermark:
        return watermark, False
    watermark_dt = parse_watermark_dt(watermark)
    if watermark_dt is None:
        return watermark, False
    if is_sentinel_datetime(watermark_dt):
        if logger is not None:
            logger.warning(
                "dataset=%s phase=watermark_rejected raw=%s reason=sentinel_epoch",
                dataset,
                watermark,
            )
        return None, True

    if watermark_dt.tzinfo is None:
        cap = datetime.now() + future_slack
        too_future = watermark_dt > cap
        clamped_dt = cap
    else:
        now = datetime.now(timezone.utc)
        cap = now + future_slack
        too_future = watermark_dt.astimezone(timezone.utc) > cap
        clamped_dt = cap.astimezone(watermark_dt.tzinfo)

    if not too_future:
        return watermark, False

    clamped = business_datetime_iso(clamped_dt, timespec="microseconds")
    if logger is not None:
        logger.warning(
            "dataset=%s phase=watermark_clamped raw=%s clamped=%s reason=future_poison",
            dataset,
            watermark,
            clamped,
        )
    return clamped, True


def watermark_age_seconds(watermark: Optional[str]) -> Optional[float]:
    dt = parse_watermark_dt(watermark)
    if dt is None:
        return None
    if dt.tzinfo is None:
        now = datetime.now()
        return max(0.0, (now - dt).total_seconds())
    now = datetime.now(timezone.utc)
    return max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds())
