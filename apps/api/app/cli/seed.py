"""Seed minimal data (tenant + users) for local/dev.

Usage (inside docker):
  docker compose exec api python -m app.cli.seed

PT-BR:
- Cria Empresa 1 (tenant) se não existir
- Cria usuários MASTER/OWNER/MANAGER
- Cria escopos (auth.user_tenants)

EN:
- Creates Tenant 1 if missing
- Creates MASTER/OWNER/MANAGER users
- Creates scopes (auth.user_tenants)

NOTE:
- psycopg3 starts a transaction by default (autocommit=False).
  If we don't commit, closing the connection will rollback.
"""

from __future__ import annotations

import os
from typing import Optional

from app.db import get_conn
from app.security import hash_password

DEFAULT_PASSWORD = os.getenv("SEED_PASSWORD", "TorqMind@123")

# Use valid-looking domains to avoid any external email validations
MASTER_EMAIL = "master@torqmind.com"
OWNER_EMAIL = "owner@empresa1.com"
MANAGER_EMAIL = "manager@empresa1.com"


def _upsert_user(email: str, password: str) -> str:
    """Return user_id."""

    pwd_hash = hash_password(password)
    sql = """
      INSERT INTO auth.users (email, password_hash)
      VALUES (%s, %s)
      ON CONFLICT (email)
      DO UPDATE SET password_hash = EXCLUDED.password_hash
      RETURNING id
    """

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (email, pwd_hash)).fetchone()
        conn.commit()  # <-- critical for FK usage in other connections
        return str(row["id"])


def _ensure_scope(user_id: str, role: str, id_empresa: Optional[int], id_filial: Optional[int]) -> None:
    sql = """
      INSERT INTO auth.user_tenants (user_id, role, id_empresa, id_filial)
      VALUES (%s, %s, %s, %s)
      ON CONFLICT DO NOTHING
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (user_id, role, id_empresa, id_filial))
        conn.commit()


def _ensure_tenant(id_empresa: int, nome: str) -> str:
    sql = """
      INSERT INTO app.tenants (id_empresa, nome)
      VALUES (%s, %s)
      ON CONFLICT (id_empresa) DO UPDATE SET nome = EXCLUDED.nome
      RETURNING ingest_key
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (id_empresa, nome)).fetchone()
        conn.commit()  # <-- important for subsequent reads
        return str(row["ingest_key"])


def _ensure_filial_placeholder(id_empresa: int, id_filial: int, nome: str) -> None:
    sql = """
      INSERT INTO auth.filiais (id_empresa, id_filial, nome)
      VALUES (%s, %s, %s)
      ON CONFLICT (id_empresa, id_filial)
      DO UPDATE SET nome = EXCLUDED.nome, is_active = true
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (id_empresa, id_filial, nome))
        conn.commit()


def _ensure_notification_settings(user_id: str) -> None:
    sql = """
      INSERT INTO app.user_notification_settings (user_id)
      VALUES (%s)
      ON CONFLICT (user_id) DO NOTHING
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (user_id,))
        conn.commit()


def main() -> None:
    ingest_key = _ensure_tenant(1, "Empresa 1 (dev)")
    _ensure_filial_placeholder(1, 1, "Filial 1")

    master_id = _upsert_user(MASTER_EMAIL, DEFAULT_PASSWORD)
    owner_id = _upsert_user(OWNER_EMAIL, DEFAULT_PASSWORD)
    manager_id = _upsert_user(MANAGER_EMAIL, DEFAULT_PASSWORD)

    _ensure_scope(master_id, "MASTER", None, None)
    _ensure_scope(owner_id, "OWNER", 1, None)
    _ensure_scope(manager_id, "MANAGER", 1, 1)

    _ensure_notification_settings(owner_id)
    _ensure_notification_settings(master_id)

    print("\n=== TorqMind seed concluído ===")
    print(f"Tenant (id_empresa=1) ingest_key: {ingest_key}")
    print("\nUsuários criados/atualizados (senha padrão):")
    print(f"  MASTER  -> {MASTER_EMAIL} / {DEFAULT_PASSWORD}")
    print(f"  OWNER   -> {OWNER_EMAIL} / {DEFAULT_PASSWORD}")
    print(f"  MANAGER -> {MANAGER_EMAIL} / {DEFAULT_PASSWORD}")
    print("\nDica: para alertas Telegram, atualize app.user_notification_settings.telegram_chat_id e telegram_enabled=true.")


if __name__ == "__main__":
    main()
