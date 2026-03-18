"""Seed users for local/dev and the canonical production bootstrap."""

from __future__ import annotations

import os
from typing import Optional

from app.db import get_conn
from app.security import hash_password

DEFAULT_PASSWORD = os.getenv("SEED_PASSWORD", "TorqMind@123")
SEED_MODE = (os.getenv("SEED_MODE") or "demo").strip().lower()

PLATFORM_MASTER_EMAIL = (os.getenv("PLATFORM_MASTER_EMAIL") or "teko94@gmail.com").strip().lower()
PLATFORM_MASTER_PASSWORD = os.getenv("PLATFORM_MASTER_PASSWORD") or "@Crmjr105"
PLATFORM_MASTER_NAME = os.getenv("PLATFORM_MASTER_NAME") or "TorqMind Platform Master"

CHANNEL_BOOTSTRAP_EMAIL = (os.getenv("CHANNEL_BOOTSTRAP_EMAIL") or "master@torqmind.com").strip().lower()
CHANNEL_BOOTSTRAP_PASSWORD = os.getenv("CHANNEL_BOOTSTRAP_PASSWORD") or DEFAULT_PASSWORD
CHANNEL_BOOTSTRAP_USER_NAME = os.getenv("CHANNEL_BOOTSTRAP_USER_NAME") or "TorqMind Channel Admin"
CHANNEL_BOOTSTRAP_NAME = os.getenv("CHANNEL_BOOTSTRAP_NAME") or "Canal TorqMind"

OWNER_EMAIL = "owner@empresa1.com"
MANAGER_EMAIL = "manager@empresa1.com"


def _sync_tenant_identity(conn) -> None:
    conn.execute(
        """
        SELECT setval(
          pg_get_serial_sequence('app.tenants', 'id_empresa'),
          GREATEST(COALESCE((SELECT MAX(id_empresa) FROM app.tenants), 0), 1),
          true
        )
        """
    )


def _upsert_user(email: str, password: str, nome: str, role: str, *, must_change_password: bool = False) -> str:
    pwd_hash = hash_password(password)
    sql = """
      INSERT INTO auth.users (email, password_hash, nome, role, valid_from, is_active, must_change_password)
      VALUES (%s, %s, %s, %s, CURRENT_DATE, true, %s)
      ON CONFLICT (email)
      DO UPDATE SET
        password_hash = EXCLUDED.password_hash,
        nome = EXCLUDED.nome,
        role = EXCLUDED.role,
        is_active = true,
        must_change_password = EXCLUDED.must_change_password,
        valid_from = COALESCE(auth.users.valid_from, CURRENT_DATE)
      RETURNING id
    """

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (email.lower(), pwd_hash, nome, role, must_change_password)).fetchone()
        conn.commit()
        return str(row["id"])


def _replace_scopes(user_id: str, accesses: list[dict[str, object]]) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute("DELETE FROM auth.user_tenants WHERE user_id = %s::uuid", (user_id,))
        for access in accesses:
            conn.execute(
                """
                INSERT INTO auth.user_tenants (
                  user_id,
                  role,
                  channel_id,
                  id_empresa,
                  id_filial,
                  is_enabled,
                  valid_from,
                  valid_until
                )
                VALUES (
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  COALESCE(%s, true),
                  COALESCE(%s, CURRENT_DATE),
                  %s
                )
                """,
                (
                    user_id,
                    access["role"],
                    access.get("channel_id"),
                    access.get("id_empresa"),
                    access.get("id_filial"),
                    access.get("is_enabled"),
                    access.get("valid_from"),
                    access.get("valid_until"),
                ),
            )
        conn.commit()


def _ensure_channel(name: str, *, email: str | None = None, contact_name: str | None = None) -> int:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        if email:
            existing = conn.execute(
                """
                SELECT id
                FROM app.channels
                WHERE lower(COALESCE(email, '')) = lower(%s)
                   OR lower(name) = lower(%s)
                ORDER BY
                  CASE WHEN lower(COALESCE(email, '')) = lower(%s) THEN 0 ELSE 1 END,
                  id
                LIMIT 1
                """,
                (email, name, email),
            ).fetchone()
        else:
            existing = conn.execute(
                """
                SELECT id
                FROM app.channels
                WHERE lower(name) = lower(%s)
                ORDER BY id
                LIMIT 1
                """,
                (name,),
            ).fetchone()
        if existing:
            row = conn.execute(
                """
                UPDATE app.channels
                SET
                  name = %s,
                  email = COALESCE(%s, email),
                  contact_name = COALESCE(%s, contact_name),
                  is_enabled = true,
                  notes = COALESCE(notes, 'Bootstrap channel managed by seed'),
                  updated_at = now()
                WHERE id = %s
                RETURNING id
                """,
                (name, email, contact_name, existing["id"]),
            ).fetchone()
        else:
            row = conn.execute(
                """
                INSERT INTO app.channels (name, email, contact_name, is_enabled, notes)
                VALUES (%s, %s, %s, true, 'Bootstrap channel managed by seed')
                RETURNING id
                """,
                (name, email, contact_name),
            ).fetchone()
        conn.commit()
        return int(row["id"])


def _ensure_tenant(id_empresa: int, nome: str, channel_id: Optional[int] = None) -> str:
    sql = """
      INSERT INTO app.tenants (
        id_empresa,
        nome,
        channel_id,
        status,
        billing_status,
        valid_from,
        is_active
      )
      VALUES (%s, %s, %s, 'active', 'current', CURRENT_DATE, true)
      ON CONFLICT (id_empresa)
      DO UPDATE SET
        nome = EXCLUDED.nome,
        channel_id = COALESCE(EXCLUDED.channel_id, app.tenants.channel_id),
        status = COALESCE(app.tenants.status, 'active'),
        billing_status = COALESCE(app.tenants.billing_status, 'current'),
        valid_from = COALESCE(app.tenants.valid_from, CURRENT_DATE),
        is_active = true
      RETURNING ingest_key
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (id_empresa, nome, channel_id)).fetchone()
        _sync_tenant_identity(conn)
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


def _bootstrap_internal_users() -> tuple[str, str, int]:
    platform_master_id = _upsert_user(
        PLATFORM_MASTER_EMAIL,
        PLATFORM_MASTER_PASSWORD,
        PLATFORM_MASTER_NAME,
        "platform_master",
        must_change_password=False,
    )
    _replace_scopes(
        platform_master_id,
        [
            {
                "role": "platform_master",
                "channel_id": None,
                "id_empresa": None,
                "id_filial": None,
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
            }
        ],
    )
    _ensure_notification_settings(platform_master_id, PLATFORM_MASTER_EMAIL)

    bootstrap_channel_id = _ensure_channel(
        CHANNEL_BOOTSTRAP_NAME,
        email=CHANNEL_BOOTSTRAP_EMAIL,
        contact_name=CHANNEL_BOOTSTRAP_USER_NAME,
    )
    channel_admin_id = _upsert_user(
        CHANNEL_BOOTSTRAP_EMAIL,
        CHANNEL_BOOTSTRAP_PASSWORD,
        CHANNEL_BOOTSTRAP_USER_NAME,
        "channel_admin",
        must_change_password=False,
    )
    _replace_scopes(
        channel_admin_id,
        [
            {
                "role": "channel_admin",
                "channel_id": bootstrap_channel_id,
                "id_empresa": None,
                "id_filial": None,
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
            }
        ],
    )
    _ensure_notification_settings(channel_admin_id, CHANNEL_BOOTSTRAP_EMAIL)
    return platform_master_id, channel_admin_id, bootstrap_channel_id


def main() -> None:
    _, _, bootstrap_channel_id = _bootstrap_internal_users()

    print("\n=== TorqMind seed concluído ===")
    print(f"Modo: {SEED_MODE}")
    print("\nUsuários criados/atualizados (senha bootstrap):")
    print(f"  PLATFORM MASTER -> {PLATFORM_MASTER_EMAIL} / {PLATFORM_MASTER_PASSWORD}")
    print(f"  CHANNEL ADMIN   -> {CHANNEL_BOOTSTRAP_EMAIL} / {CHANNEL_BOOTSTRAP_PASSWORD}")
    print(f"  CANAL BOOTSTRAP -> {CHANNEL_BOOTSTRAP_NAME}")

    if SEED_MODE != "master-only":
        ingest_key = _ensure_tenant(1, "Empresa 1 (dev)", channel_id=bootstrap_channel_id)
        _ensure_filial_placeholder(1, 1, "Filial 1")

        owner_id = _upsert_user(OWNER_EMAIL, DEFAULT_PASSWORD, "Tenant Admin Empresa 1", "tenant_admin", must_change_password=False)
        manager_id = _upsert_user(MANAGER_EMAIL, DEFAULT_PASSWORD, "Tenant Manager Filial 1", "tenant_manager", must_change_password=False)

        _replace_scopes(
            owner_id,
            [
                {
                    "role": "tenant_admin",
                    "channel_id": None,
                    "id_empresa": 1,
                    "id_filial": None,
                    "is_enabled": True,
                    "valid_from": None,
                    "valid_until": None,
                }
            ],
        )
        _replace_scopes(
            manager_id,
            [
                {
                    "role": "tenant_manager",
                    "channel_id": None,
                    "id_empresa": 1,
                    "id_filial": 1,
                    "is_enabled": True,
                    "valid_from": None,
                    "valid_until": None,
                }
            ],
        )

        _ensure_notification_settings(owner_id, OWNER_EMAIL)
        _ensure_notification_settings(manager_id, MANAGER_EMAIL)

        print(f"  TENANT ADMIN    -> {OWNER_EMAIL} / {DEFAULT_PASSWORD}")
        print(f"  TENANT MANAGER  -> {MANAGER_EMAIL} / {DEFAULT_PASSWORD}")
        print(f"\nTenant demo (id_empresa=1) ingest_key: {ingest_key}")
    else:
        print("\nNenhum tenant/filial demo foi criado nesse seed. Apenas os usuários internos e o canal bootstrap foram sincronizados.")


if __name__ == "__main__":
    main()
