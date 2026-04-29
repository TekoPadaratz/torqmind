from __future__ import annotations

"""ClickHouse-first facade for BI analytics repositories.

This module is intentionally thin: route handlers keep calling ``repos_mart`` by
their existing function names, while this facade chooses the concrete backend.
PostgreSQL remains the legacy fallback when ``USE_CLICKHOUSE=false``.
"""

from functools import wraps
import inspect
import logging
from typing import Any, Callable

from app import repos_mart as _postgres
from app import repos_mart_clickhouse as _clickhouse
from app.config import settings
from app.db_clickhouse import get_dual_read_validator

logger = logging.getLogger(__name__)


_PUBLIC_POSTGRES_FUNCTIONS = {
    name
    for name, value in inspect.getmembers(_postgres, inspect.isfunction)
    if not name.startswith("_") and value.__module__ == _postgres.__name__
}

_CLICKHOUSE_FUNCTIONS = {
    name
    for name, value in inspect.getmembers(_clickhouse, inspect.isfunction)
    if not name.startswith("_") and value.__module__ == _clickhouse.__name__
}

_DISPATCH_CACHE: dict[str, Callable[..., Any]] = {}

# These are still PostgreSQL by design. They are either OLTP/configuration
# operations or app-owned serving tables, not ClickHouse analytical marts.
_POSTGRES_OWNED_FUNCTIONS = {
    "list_filiais",
    "competitor_pricing_upsert",
    "competitor_fuel_product_ids",
    "goals_today",
    "upsert_goal",
    "risk_insights",
    "notifications_list",
    "notifications_unread_count",
    "notification_mark_read",
}

# Analytical functions that remain on the legacy path until a mart exists with
# the same grain/contract. The warning is emitted only when USE_CLICKHOUSE=true.
_CLICKHOUSE_DEBT_FUNCTIONS = {
    "stock_position_summary": "estoque mart is not present in sql/clickhouse yet",
    "customers_delinquency_overview": "finance delinquency drilldown needs a customer-level mart",
    "cash_dre_summary": "DRE still depends on transactional finance facts",
    "competitor_pricing_overview": "pricing simulation needs fuel product dimension and competitor app table",
    "monthly_goal_projection": "goal projection mixes app.goals with analytical sales series",
}


def _legacy_function(name: str) -> Callable[..., Any]:
    value = getattr(_postgres, name)
    if not callable(value):
        raise AttributeError(name)
    return value


def _clickhouse_function(name: str) -> Callable[..., Any] | None:
    value = getattr(_clickhouse, name, None)
    return value if callable(value) else None


def _compare_dual_read(name: str, pg_result: Any, ch_result: Any) -> None:
    try:
        get_dual_read_validator().compare(name, pg_result, ch_result)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Dual-read comparison failed for %s: %s", name, exc.__class__.__name__, exc_info=exc)


def _dispatch(name: str) -> Callable[..., Any]:
    cached = _DISPATCH_CACHE.get(name)
    if cached is not None:
        return cached

    legacy = _legacy_function(name)
    clickhouse = _clickhouse_function(name)

    @wraps(legacy)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        use_clickhouse = bool(settings.use_clickhouse)
        dual_read = bool(settings.dual_read_mode)

        if name in _POSTGRES_OWNED_FUNCTIONS:
            return legacy(*args, **kwargs)

        if clickhouse is None:
            if use_clickhouse and name in _CLICKHOUSE_DEBT_FUNCTIONS:
                logger.warning(
                    "Analytics function %s is using PostgreSQL legacy path with USE_CLICKHOUSE=true: %s",
                    name,
                    _CLICKHOUSE_DEBT_FUNCTIONS[name],
                )
            return legacy(*args, **kwargs)

        if dual_read:
            pg_result: Any = None
            pg_error: Exception | None = None
            ch_result: Any = None
            ch_error: Exception | None = None

            try:
                pg_result = legacy(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                pg_error = exc

            try:
                ch_result = clickhouse(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                ch_error = exc

            if pg_error is None and ch_error is None:
                _compare_dual_read(name, pg_result, ch_result)
                return ch_result if use_clickhouse else pg_result

            if use_clickhouse:
                if ch_error is not None:
                    logger.error("ClickHouse analytics read failed for %s", name, exc_info=ch_error)
                    raise ch_error
                logger.warning("PostgreSQL dual-read side failed for %s while ClickHouse result was returned", name, exc_info=pg_error)
                return ch_result

            if pg_error is not None:
                logger.error("PostgreSQL analytics fallback failed for %s", name, exc_info=pg_error)
                raise pg_error
            logger.warning("ClickHouse dual-read side failed for %s while PostgreSQL fallback was returned", name, exc_info=ch_error)
            return pg_result

        if use_clickhouse:
            return clickhouse(*args, **kwargs)

        return legacy(*args, **kwargs)

    _DISPATCH_CACHE[name] = wrapper
    return wrapper


def __getattr__(name: str) -> Any:
    if name in _PUBLIC_POSTGRES_FUNCTIONS:
        return _dispatch(name)
    return getattr(_postgres, name)


def analytics_backend_inventory() -> dict[str, Any]:
    """Return the active facade map for tests, docs and release checks."""
    functions = []
    for name in sorted(_PUBLIC_POSTGRES_FUNCTIONS):
        if name in _POSTGRES_OWNED_FUNCTIONS:
            source = "postgres_app"
        elif name in _CLICKHOUSE_FUNCTIONS:
            source = "clickhouse"
        elif name in _CLICKHOUSE_DEBT_FUNCTIONS:
            source = "postgres_debt"
        else:
            source = "postgres_legacy"
        functions.append(
            {
                "function": name,
                "source": source,
                "clickhouse_implemented": name in _CLICKHOUSE_FUNCTIONS,
                "debt": _CLICKHOUSE_DEBT_FUNCTIONS.get(name),
            }
        )
    return {
        "use_clickhouse": bool(settings.use_clickhouse),
        "dual_read_mode": bool(settings.dual_read_mode),
        "functions": functions,
    }
