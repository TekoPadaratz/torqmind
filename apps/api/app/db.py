from __future__ import annotations

import contextlib
from typing import Iterator, Optional
from urllib.parse import urlparse, unquote

import psycopg
from psycopg.rows import dict_row

from app.config import settings


def _conn_str() -> str:
    """Build psycopg connection string.

    PT-BR:
    - Em Docker, preferimos DATABASE_URL para garantir que API/CLI usem o mesmo destino.
    - Se não existir, usamos PG_*.

    EN:
    - In Docker, we prefer DATABASE_URL so API/CLI point to the same database.
    - Fallback to PG_* when DATABASE_URL is not set.
    """

    if settings.database_url:
        parsed = urlparse(settings.database_url)
        if parsed.scheme.startswith("postgresql"):
            user = unquote(parsed.username or settings.pg_user)
            password = unquote(parsed.password or settings.pg_password)
            host = parsed.hostname or settings.pg_host
            port = parsed.port or settings.pg_port
            dbname = (parsed.path or "").lstrip("/") or settings.pg_database
            return f"host={host} port={port} dbname={dbname} user={user} password={password}"

    return (
        f"host={settings.pg_host} port={settings.pg_port} dbname={settings.pg_database} "
        f"user={settings.pg_user} password={settings.pg_password}"
    )


def _sql_quote(value: str) -> str:
    """Very small helper to safely quote a string literal for SQL.

    PT-BR: Escapa aspas simples para evitar quebrar o SQL.
    EN: Escapes single quotes so the SQL string literal stays valid.
    """

    return value.replace("'", "''")


@contextlib.contextmanager
def get_conn(
    role: Optional[str] = None,
    tenant_id: Optional[int] = None,
    branch_id: Optional[int] = None,
) -> Iterator[psycopg.Connection]:
    """Open a Postgres connection and set session variables used for scope.

    PT-BR:
    - Usamos SET (escopo de sessão), e NÃO SET LOCAL.
    - SET LOCAL zera após COMMIT e quebra fluxos CLI/ETL que fazem commit no meio.
    - No finally fazemos RESET explícito para evitar vazamento de contexto no futuro
      caso o projeto migre para pool de conexões.

    EN:
    - We use session-level SET, NOT SET LOCAL.
    - SET LOCAL is cleared after COMMIT and breaks CLI/ETL flows that commit mid-run.
    - We explicitly RESET in finally to avoid context leakage if pooling is introduced.
    """

    conn = psycopg.connect(_conn_str(), row_factory=dict_row)
    try:
        if role is not None:
            conn.execute(f"SET app.role = '{_sql_quote(role)}'")
        else:
            conn.execute("RESET app.role")

        if tenant_id is not None:
            conn.execute(f"SET app.tenant_id = {int(tenant_id)}")
        else:
            conn.execute("RESET app.tenant_id")

        if branch_id is not None:
            conn.execute(f"SET app.branch_id = {int(branch_id)}")
        else:
            conn.execute("RESET app.branch_id")

        yield conn
    finally:
        with contextlib.suppress(Exception):
            conn.execute("RESET app.role")
            conn.execute("RESET app.tenant_id")
            conn.execute("RESET app.branch_id")
        conn.close()
