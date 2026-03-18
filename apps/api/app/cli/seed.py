"""Seed minimal data (tenant + users) for local/dev."""

from __future__ import annotations

import os
from typing import Optional

from app.db import get_conn
from app.security import hash_password

DEFAULT_PASSWORD = os.getenv("SEED_PASSWORD", "TorqMind@123")

MASTER_EMAIL = "master@torqmind.com"
OWNER_EMAIL = "owner@empresa1.com"
MANAGER_EMAIL = "manager@empresa1.com"


def _upsert_user(email: str, password: str, nome: str, role: str) -> str:
    pwd_hash = hash_password(password)
    sql = """
      INSERT INTO auth.users (email, password_hash, nome, role, valid_from, is_active)
      VALUES (%s, %s, %s, %s, CURRENT_DATE, true)
      ON CONFLICT (email)
      DO UPDATE SET
        password_hash = EXCLUDED.password_hash,
        nome = EXCLUDED.nome,
        role = EXCLUDED.role,
        is_active = true,
        valid_from = COALESCE(auth.users.valid_from, CURRENT_DATE)
      RETURNING id
    """

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (email.lower(), pwd_hash, nome, role)).fetchone()
        conn.commit()
        return str(row["id"])


def _ensure_scope(
    user_id: str,
    role: str,
    id_empresa: Optional[int],
    id_filial: Optional[int],
    channel_id: Optional[int] = None,
) -> None:
    sql = """
      INSERT INTO auth.user_tenants (
        user_id,
        role,
        channel_id,
        id_empresa,
        id_filial,
        is_enabled,
        valid_from
      )
      VALUES (%s, %s, %s, %s, %s, true, CURRENT_DATE)
      ON CONFLICT DO NOTHING
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (user_id, role, channel_id, id_empresa, id_filial))
        conn.commit()


def _ensure_tenant(id_empresa: int, nome: str) -> str:
    sql = """
      INSERT INTO app.tenants (
        id_empresa,
        nome,
        status,
        billing_status,
        valid_from,
        is_active
      )
      VALUES (%s, %s, 'active', 'current', CURRENT_DATE, true)
      ON CONFLICT (id_empresa)
      DO UPDATE SET
        nome = EXCLUDED.nome,
        status = COALESCE(app.tenants.status, 'active'),
        billing_status = COALESCE(app.tenants.billing_status, 'current'),
        valid_from = COALESCE(app.tenants.valid_from, CURRENT_DATE),
        is_active = true
      RETURNING ingest_key
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (id_empresa, nome)).fetchone()
        conn.commit()
        return str(row["ingest_key"])


def _ensure_filial_placeholder(id_empresa: int, id_filial: int, nome: str) -> None:
    sql = """
      INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
      VALUES (%s, %s, %s, true, CURRENT_DATE)
      ON CONFLICT (id_empresa, id_filial)
      DO UPDATE SET nome = EXCLUDED.nome, is_active = true, valid_from = COALESCE(auth.filiais.valid_from, CURRENT_DATE)
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (id_empresa, id_filial, nome))
        conn.commit()


def _ensure_notification_settings(user_id: str, email: str) -> None:
    sql = """
      INSERT INTO app.user_notification_settings (user_id, email)
      VALUES (%s, %s)
      ON CONFLICT (user_id)
      DO UPDATE SET email = EXCLUDED.email
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (user_id, email.lower()))
        conn.commit()


def main() -> None:
    ingest_key = _ensure_tenant(1, "Empresa 1 (dev)")
    _ensure_filial_placeholder(1, 1, "Filial 1")

    master_id = _upsert_user(MASTER_EMAIL, DEFAULT_PASSWORD, "Platform Master", "platform_master")
    owner_id = _upsert_user(OWNER_EMAIL, DEFAULT_PASSWORD, "Tenant Admin Empresa 1", "tenant_admin")
    manager_id = _upsert_user(MANAGER_EMAIL, DEFAULT_PASSWORD, "Tenant Manager Filial 1", "tenant_manager")

    _ensure_scope(master_id, "platform_master", None, None)
    _ensure_scope(owner_id, "tenant_admin", 1, None)
    _ensure_scope(manager_id, "tenant_manager", 1, 1)

    _ensure_notification_settings(master_id, MASTER_EMAIL)
    _ensure_notification_settings(owner_id, OWNER_EMAIL)
    _ensure_notification_settings(manager_id, MANAGER_EMAIL)

    print("\n=== TorqMind seed concluído ===")
    print(f"Tenant (id_empresa=1) ingest_key: {ingest_key}")
    print("\nUsuários criados/atualizados (senha padrão):")
    print(f"  PLATFORM MASTER -> {MASTER_EMAIL} / {DEFAULT_PASSWORD}")
    print(f"  TENANT ADMIN    -> {OWNER_EMAIL} / {DEFAULT_PASSWORD}")
    print(f"  TENANT MANAGER  -> {MANAGER_EMAIL} / {DEFAULT_PASSWORD}")


if __name__ == "__main__":
    main()
