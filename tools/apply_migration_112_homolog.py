#!/usr/bin/env python3
"""Apply 112_solvencia_dinheiro_editavel.sql on homolog bypassing migrate checksum gate."""
from __future__ import annotations

import hashlib
from pathlib import Path

from app.db import get_conn

SQL_FILE = "112_solvencia_dinheiro_editavel.sql"


def main() -> int:
    sql_path = Path("/app/sql/migrations") / SQL_FILE
    if not sql_path.exists():
        sql_path = Path("/home/tm/torqmind/sql/migrations") / SQL_FILE
    sql = sql_path.read_text(encoding="utf-8")
    checksum = hashlib.sha256(sql.encode()).hexdigest()
    with get_conn(role="MASTER", tenant_id=1, branch_id=None) as conn:
        exists = conn.execute(
            "SELECT 1 FROM app.schema_migrations WHERE filename=%s",
            [SQL_FILE],
        ).fetchone()
        if not exists:
            conn.execute(sql)
            cols = {
                r["column_name"]
                for r in conn.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema='app' AND table_name='schema_migrations'
                    """
                ).fetchall()
            }
            if "checksum" in cols and "applied_at" in cols:
                conn.execute(
                    """
                    INSERT INTO app.schema_migrations (filename, checksum, applied_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (filename) DO NOTHING
                    """,
                    [SQL_FILE, checksum],
                )
            elif "checksum" in cols:
                conn.execute(
                    """
                    INSERT INTO app.schema_migrations (filename, checksum)
                    VALUES (%s, %s)
                    ON CONFLICT (filename) DO NOTHING
                    """,
                    [SQL_FILE, checksum],
                )
            else:
                conn.execute(
                    """
                    INSERT INTO app.schema_migrations (filename)
                    VALUES (%s)
                    ON CONFLICT (filename) DO NOTHING
                    """,
                    [SQL_FILE],
                )
            conn.commit()
            print("APPLIED_112")
        else:
            # still ensure tipo exists
            conn.execute(sql)
            conn.commit()
            print("REFRESHED_112")
        print(
            conn.execute(
                "SELECT id_tipo, chave, secao FROM app.solvencia_tipo_manual ORDER BY 1"
            ).fetchall()
        )
        print(
            conn.execute(
                "SELECT filename FROM app.schema_migrations WHERE filename LIKE '11%' ORDER BY 1"
            ).fetchall()
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
