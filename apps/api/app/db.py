from __future__ import annotations

import contextlib
from typing import Iterator, Optional

import psycopg
from psycopg.rows import dict_row

from app.config import settings


def _conn_str() -> str:
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
    """Open a Postgres connection and set session variables used for RLS.

    IMPORTANT:
    - We use `SET` (session-scoped), NOT `SET LOCAL` (transaction-scoped).
      `SET LOCAL` is cleared on COMMIT, which breaks CLIs and any code that
      commits mid-flow.
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
        conn.close()
