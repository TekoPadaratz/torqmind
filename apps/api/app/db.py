"""PostgreSQL connection helpers with purpose-separated pools and TLS preservation.

Prompt 6: DATABASE_URL query TLS options must not be discarded. Credentials are
passed through ``psycopg.conninfo.make_conninfo`` (no unsafe concatenation).
TLS enforcement stays OFF by default until infrastructure is ready.
"""
from __future__ import annotations

import contextlib
import contextvars
import re
import threading
from typing import Any, Iterator, Optional
from urllib.parse import parse_qs, unquote, urlparse

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - local env may not be updated yet
    ConnectionPool = None  # type: ignore[assignment]

from app.config import settings

# Keys accepted from DATABASE_URL query string / discrete TLS settings.
_PG_CONNINFO_SAFE_KEYS = frozenset(
    {
        "sslmode",
        "sslrootcert",
        "sslcert",
        "sslkey",
        "sslcrl",
        "sslpassword",
        "channel_binding",
        "gssencmode",
        "target_session_attrs",
        "connect_timeout",
        "options",
        "application_name",
        "keepalives",
        "keepalives_idle",
        "keepalives_interval",
        "keepalives_count",
        "tcp_user_timeout",
    }
)

# Logical connection purposes. Distinct env DSNs are optional; without them all
# purposes share credentials but keep separate pools + application_name so a
# single POSTGRES_USER change does NOT equal privilege separation.
DB_PURPOSES = ("api", "ingest", "etl", "migrate", "auth")

_default_purpose: contextvars.ContextVar[str] = contextvars.ContextVar("torqmind_db_purpose", default="api")

_pools: dict[str, ConnectionPool] = {}
_pool_lock = threading.Lock()


@contextlib.contextmanager
def db_purpose(purpose: str) -> Iterator[str]:
    """Bind default ``get_conn`` purpose for the current context (ETL jobs, etc.)."""
    normalized = (purpose or "api").strip().lower() or "api"
    if normalized not in DB_PURPOSES:
        normalized = "api"
    token = _default_purpose.set(normalized)
    try:
        yield normalized
    finally:
        _default_purpose.reset(token)


def _first_query_value(qs: dict[str, list[str]], key: str) -> Optional[str]:
    values = qs.get(key) or qs.get(key.lower()) or qs.get(key.upper())
    if not values:
        return None
    return values[0]


def _tls_kwargs_from_settings() -> dict[str, str]:
    """Discrete PG_* TLS settings (do not enable TLS unless explicitly set)."""
    out: dict[str, str] = {}
    sslmode = (getattr(settings, "pg_sslmode", None) or "").strip()
    if sslmode:
        out["sslmode"] = sslmode
    for attr, key in (
        ("pg_sslrootcert", "sslrootcert"),
        ("pg_sslcert", "sslcert"),
        ("pg_sslkey", "sslkey"),
        ("pg_sslcrl", "sslcrl"),
    ):
        value = (getattr(settings, attr, None) or "").strip()
        if value:
            out[key] = value
    return out


def _database_url_for_purpose(purpose: str) -> Optional[str]:
    purpose = (purpose or "api").strip().lower() or "api"
    # Prefer purpose-specific URL when present (gradual separation).
    attr = f"database_url_{purpose}"
    specific = getattr(settings, attr, None)
    if specific and str(specific).strip():
        return str(specific).strip()
    if purpose == "api" and settings.database_url:
        return str(settings.database_url).strip()
    # Fallback chain: purpose → api → generic database_url
    if settings.database_url:
        return str(settings.database_url).strip()
    return None


def _discrete_user_for_purpose(purpose: str) -> tuple[str, str]:
    """Return (user, password) for purpose-specific discrete env, else defaults."""
    purpose = (purpose or "api").strip().lower() or "api"
    user_attr = f"pg_user_{purpose}"
    pass_attr = f"pg_password_{purpose}"
    user = (getattr(settings, user_attr, None) or "").strip() or settings.pg_user
    password = getattr(settings, pass_attr, None)
    if password is None or str(password) == "":
        password = settings.pg_password
    return str(user), str(password)


def build_conninfo(
    *,
    purpose: str = "api",
    application_name: Optional[str] = None,
    database_url: Optional[str] = None,
    overrides: Optional[dict[str, Any]] = None,
) -> str:
    """Build a libpq conninfo string preserving TLS options and escaping secrets.

    ``overrides`` is for tests (e.g. force sslmode / sslrootcert).
    """
    purpose = (purpose or "api").strip().lower() or "api"
    if purpose not in DB_PURPOSES:
        purpose = "api"

    kwargs: dict[str, Any] = {}
    url = database_url if database_url is not None else _database_url_for_purpose(purpose)

    if url:
        parsed = urlparse(url)
        if not parsed.scheme.startswith("postgresql"):
            raise ValueError(f"Unsupported database URL scheme: {parsed.scheme!r}")
        user_default, password_default = _discrete_user_for_purpose(purpose)
        kwargs["host"] = parsed.hostname or settings.pg_host
        kwargs["port"] = parsed.port or settings.pg_port
        kwargs["dbname"] = (parsed.path or "").lstrip("/") or settings.pg_database
        kwargs["user"] = unquote(parsed.username) if parsed.username else user_default
        kwargs["password"] = unquote(parsed.password) if parsed.password is not None else password_default
        qs = parse_qs(parsed.query, keep_blank_values=True)
        for key in _PG_CONNINFO_SAFE_KEYS:
            value = _first_query_value(qs, key)
            if value is not None and value != "":
                kwargs[key] = value
    else:
        user, password = _discrete_user_for_purpose(purpose)
        kwargs["host"] = settings.pg_host
        kwargs["port"] = settings.pg_port
        kwargs["dbname"] = settings.pg_database
        kwargs["user"] = user
        kwargs["password"] = password

    # Discrete TLS settings fill gaps but do not override explicit URL params.
    for key, value in _tls_kwargs_from_settings().items():
        kwargs.setdefault(key, value)

    app_name = application_name or f"torqmind-{purpose}"
    kwargs.setdefault("application_name", app_name)

    if overrides:
        for key, value in overrides.items():
            if value is None:
                continue
            if key in _PG_CONNINFO_SAFE_KEYS or key in {
                "host",
                "port",
                "dbname",
                "user",
                "password",
            }:
                kwargs[key] = value

    return make_conninfo(**kwargs)


def _conn_str(purpose: str = "api") -> str:
    return build_conninfo(purpose=purpose)


_CONNINFO_SECRET_KEYS = frozenset({"password", "passwd", "pwd", "sslpassword"})


def redact_conninfo_text(value: str) -> str:
    """Strip secret key=value pairs from a libpq conninfo / DSN-like string.

    Safe for logs and AssertionError messages. Never round-trip into ``connect``.
    """
    text = value or ""
    # URL form: scheme://user:password@host
    text = re.sub(r"(://[^:/?#\s]+):([^@/\s]+)@", r"\1:***@", text)
    # libpq key=value (handles simple and single-quoted values)
    for key in _CONNINFO_SECRET_KEYS:
        text = re.sub(
            rf"\b{key}=(?:'(?:\\'|[^'])*'|[^\s]+)",
            f"{key}=***",
            text,
            flags=re.IGNORECASE,
        )
    return text


def redacted_conn_summary(purpose: str = "api", *, conninfo: Optional[str] = None) -> dict[str, Any]:
    """Identify the effective connection target without exposing secrets."""
    info = conninfo or build_conninfo(purpose=purpose)
    # Parse back via make_conninfo reverse is awkward; use libpq parse.
    try:
        from psycopg.conninfo import conninfo_to_dict

        raw = conninfo_to_dict(info)
    except Exception:
        return {"purpose": purpose, "error": "unparseable_conninfo"}
    return {
        "purpose": purpose,
        "host": raw.get("host"),
        "port": raw.get("port"),
        "dbname": raw.get("dbname"),
        "user": raw.get("user"),
        "sslmode": raw.get("sslmode") or "",
        "sslrootcert_set": bool(raw.get("sslrootcert")),
        "application_name": raw.get("application_name"),
        "password_set": bool(raw.get("password")),
    }


def assert_db_target(
    *,
    purpose: str = "api",
    expected_dbname: str,
    expected_host: Optional[str] = None,
    conninfo: Optional[str] = None,
) -> dict[str, Any]:
    """Assert migrate/API target using a redacted summary only (never raise secrets)."""
    summary = redacted_conn_summary(purpose=purpose, conninfo=conninfo)
    dbname = summary.get("dbname")
    host = summary.get("host")
    if dbname != expected_dbname:
        raise AssertionError(
            f"unexpected dbname purpose={purpose} got={dbname!r} expected={expected_dbname!r} "
            f"host={host!r} user={summary.get('user')!r}"
        )
    if expected_host is not None and host != expected_host:
        raise AssertionError(
            f"unexpected host purpose={purpose} got={host!r} expected={expected_host!r} "
            f"dbname={dbname!r} user={summary.get('user')!r}"
        )
    return summary


def _sql_quote(value: str) -> str:
    """Escape single quotes for SET literals (role names / session GUCs)."""
    return value.replace("'", "''")


def _get_pool(purpose: str = "api") -> ConnectionPool | None:
    if ConnectionPool is None:
        return None

    purpose = (purpose or "api").strip().lower() or "api"
    if purpose not in DB_PURPOSES:
        purpose = "api"

    existing = _pools.get(purpose)
    if existing is not None:
        return existing

    with _pool_lock:
        existing = _pools.get(purpose)
        if existing is not None:
            return existing
        pool = ConnectionPool(
            conninfo=_conn_str(purpose),
            min_size=max(1, int(settings.db_pool_min_size)),
            max_size=max(1, int(settings.db_pool_max_size)),
            timeout=max(1, int(settings.db_pool_timeout_seconds)),
            max_idle=max(30, int(settings.db_pool_max_idle_seconds)),
            kwargs={"row_factory": dict_row},
            open=True,
        )
        _pools[purpose] = pool
        return pool


def reset_pools_for_tests() -> None:
    """Close purpose pools (unit tests only)."""
    with _pool_lock:
        for purpose, pool in list(_pools.items()):
            try:
                pool.close()
            except Exception:
                pass
            _pools.pop(purpose, None)


def get_conn(
    role: Optional[str] = None,
    tenant_id: Optional[int] = None,
    branch_id: Optional[int] = None,
    *,
    purpose: Optional[str] = None,
) -> Iterator[psycopg.Connection]:
    """Open a Postgres connection and set session variables used for scope.

    ``purpose`` selects which pool/DSN to use (api|ingest|etl|migrate|auth).
    When omitted, uses :func:`db_purpose` context or default ``api``.
    Without purpose-specific env, all purposes share the same credentials —
    changing POSTGRES_USER alone does not separate privileges.
    """

    resolved = (purpose or _default_purpose.get() or "api").strip().lower() or "api"
    if resolved not in DB_PURPOSES:
        resolved = "api"
    pool = _get_pool(resolved)

    def _set_scope(conn: psycopg.Connection) -> None:
        conn.execute("RESET app.role; RESET app.tenant_id; RESET app.branch_id")
        conn.execute("SET lock_timeout = '10s'")
        conn.execute("SET statement_timeout = '55s'")
        if role is not None:
            conn.execute(f"SET app.role = '{_sql_quote(role)}'")
        if tenant_id is not None:
            conn.execute(f"SET app.tenant_id = {int(tenant_id)}")
        if branch_id is not None:
            conn.execute(f"SET app.branch_id = {int(branch_id)}")

    def _reset_scope(conn: psycopg.Connection) -> None:
        try:
            conn.execute("RESET app.role")
        except Exception:
            pass
        try:
            conn.execute("RESET app.tenant_id")
        except Exception:
            pass
        try:
            conn.execute("RESET app.branch_id")
        except Exception:
            pass

    if pool is None:
        conn = psycopg.connect(_conn_str(resolved), row_factory=dict_row)
        try:
            _set_scope(conn)
            yield conn
        finally:
            _reset_scope(conn)
            conn.close()
        return

    with pool.connection() as conn:
        try:
            _set_scope(conn)
            yield conn
        finally:
            _reset_scope(conn)


# Re-bind as contextmanager after defining the generator body above.
get_conn = contextlib.contextmanager(get_conn)  # type: ignore[misc]
