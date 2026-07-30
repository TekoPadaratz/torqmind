"""SQL query budgets — keep Xpert server healthy under continuous extract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class QueryBudget:
    batch_size: int = 2000
    fetch_size: int = 1000
    batch_delay_seconds: float = 0.75
    revisit_max_rows: int = 5000
    query_timeout_seconds: int = 600


def budget_from_runtime(runtime: Any, ds_cfg: Optional[Dict[str, Any]] = None) -> QueryBudget:
    """Merge global runtime defaults with optional per-dataset overrides."""
    ds = ds_cfg or {}

    def _int(key: str, default: int) -> int:
        raw = ds.get(key, None)
        if raw is None:
            raw = getattr(runtime, key, default)
        try:
            return max(1, int(raw))
        except (TypeError, ValueError):
            return default

    def _float(key: str, default: float) -> float:
        raw = ds.get(key, None)
        if raw is None:
            raw = getattr(runtime, key, default)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return default

    return QueryBudget(
        batch_size=_int("batch_size", 2000),
        fetch_size=_int("fetch_size", 1000),
        batch_delay_seconds=_float("batch_delay_seconds", 0.75),
        revisit_max_rows=_int("revisit_max_rows", 5000),
        query_timeout_seconds=_int("query_timeout_seconds", 600),
    )
