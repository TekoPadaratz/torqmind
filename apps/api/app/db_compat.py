from __future__ import annotations

from typing import Type

import psycopg

try:  # pragma: no cover - import shape varies by installed psycopg build
    from psycopg import errors as psycopg_errors
except Exception:  # noqa: BLE001
    psycopg_errors = None


def _resolve_error(name: str, fallback: Type[BaseException]) -> Type[BaseException]:
    if psycopg_errors is None:
        return fallback
    return getattr(psycopg_errors, name, fallback)


def _unique_error_types(*items: Type[BaseException]) -> tuple[Type[BaseException], ...]:
    ordered: list[Type[BaseException]] = []
    for item in items:
        if item not in ordered:
            ordered.append(item)
    return tuple(ordered)


QueryCanceledError = _resolve_error("QueryCanceled", psycopg.OperationalError)
LockNotAvailableError = _resolve_error("LockNotAvailable", psycopg.OperationalError)
DeadlockDetectedError = _resolve_error("DeadlockDetected", psycopg.OperationalError)
UndefinedTableError = _resolve_error("UndefinedTable", psycopg.ProgrammingError)
UndefinedColumnError = _resolve_error("UndefinedColumn", psycopg.ProgrammingError)
InvalidSchemaNameError = _resolve_error("InvalidSchemaName", psycopg.ProgrammingError)
InvalidCatalogNameError = _resolve_error("InvalidCatalogName", psycopg.ProgrammingError)
OperationalDbError = psycopg.OperationalError
ProgrammingDbError = psycopg.ProgrammingError
GenericDbError = psycopg.Error

SNAPSHOT_FALLBACK_ERRORS = _unique_error_types(
    QueryCanceledError,
    LockNotAvailableError,
    DeadlockDetectedError,
    UndefinedTableError,
    UndefinedColumnError,
    InvalidSchemaNameError,
    InvalidCatalogNameError,
    OperationalDbError,
    ProgrammingDbError,
)

DEPLOY_SAFE_IMPORT_ERRORS = _unique_error_types(
    QueryCanceledError,
    LockNotAvailableError,
    DeadlockDetectedError,
    UndefinedTableError,
    UndefinedColumnError,
    InvalidSchemaNameError,
    InvalidCatalogNameError,
    GenericDbError,
)
