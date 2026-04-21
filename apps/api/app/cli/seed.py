"""Seed users for local/dev and the canonical production bootstrap."""

from __future__ import annotations

import os
from typing import Optional

from app.db import get_conn
from app.security import hash_password
from app.usernames import username_from_email_candidate, validate_username

DEV_LIKE_ENVS = {"dev", "local", "test"}
APP_ENV = (os.getenv("APP_ENV") or "dev").strip().lower()
SEED_MODE = (os.getenv("SEED_MODE") or "demo").strip().lower()

DEV_DEFAULT_PASSWORD = "TorqMind@123"
DEV_DEFAULT_PLATFORM_MASTER_EMAIL = "teko94@gmail.com"
DEV_DEFAULT_PLATFORM_MASTER_PASSWORD = "@Crmjr105"
DEV_DEFAULT_CHANNEL_BOOTSTRAP_EMAIL = "master@torqmind.com"

INSECURE_SECRET_VALUES = frozenset(
    {
        "",
        "1234",
        "TorqMind@123",
        "@Crmjr105",
        "CHANGE_ME",
        "CHANGE_ME_API_JWT_SECRET",
        "CHANGE_ME_POSTGRES_PASSWORD",
        "CHANGE_ME_PLATFORM_MASTER_PASSWORD",
        "CHANGE_ME_SEED_PASSWORD",
        "CHANGE_ME_CHANNEL_BOOTSTRAP_PASSWORD",
    }
)


def _env_value(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    normalized = raw.strip()
    return normalized or None


def _is_prod_like() -> bool:
    return APP_ENV not in DEV_LIKE_ENVS


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip().upper()
    if not normalized:
        return True
    return normalized.startswith("CHANGE_ME") or normalized in {"<CHANGE_ME>", "YOUR_VALUE_HERE", "REPLACE_ME"}


def _is_insecure_secret(value: str) -> bool:
    normalized = value.strip()
    return normalized in INSECURE_SECRET_VALUES or _looks_like_placeholder(normalized)


def _secret_origin(env_name: str, *, fallback_label: str) -> str:
    return f"env:{env_name}" if _env_value(env_name) is not None else fallback_label


def _masked_secret_label(env_name: str, *, fallback_label: str) -> str:
    return f"senha bootstrap mascarada ({_secret_origin(env_name, fallback_label=fallback_label)})"


DEFAULT_PASSWORD = _env_value("SEED_PASSWORD") or DEV_DEFAULT_PASSWORD

PLATFORM_MASTER_EMAIL = (_env_value("PLATFORM_MASTER_EMAIL") or DEV_DEFAULT_PLATFORM_MASTER_EMAIL).lower()
PLATFORM_MASTER_PASSWORD = _env_value("PLATFORM_MASTER_PASSWORD") or DEV_DEFAULT_PLATFORM_MASTER_PASSWORD
PLATFORM_MASTER_NAME = _env_value("PLATFORM_MASTER_NAME") or "TorqMind Platform Master"

CHANNEL_BOOTSTRAP_EMAIL = (_env_value("CHANNEL_BOOTSTRAP_EMAIL") or DEV_DEFAULT_CHANNEL_BOOTSTRAP_EMAIL).lower()
CHANNEL_BOOTSTRAP_PASSWORD = _env_value("CHANNEL_BOOTSTRAP_PASSWORD") or DEFAULT_PASSWORD
CHANNEL_BOOTSTRAP_USER_NAME = os.getenv("CHANNEL_BOOTSTRAP_USER_NAME") or "TorqMind Channel Admin"
CHANNEL_BOOTSTRAP_NAME = os.getenv("CHANNEL_BOOTSTRAP_NAME") or "Canal TorqMind"

OWNER_EMAIL = "owner@empresa1.com"
MANAGER_EMAIL = "manager@empresa1.com"


def _assert_safe_bootstrap_runtime() -> None:
    if not _is_prod_like():
        return

    problems: list[str] = []
    if SEED_MODE != "master-only":
        problems.append("SEED_MODE precisa ser master-only fora de dev/local/test.")
    if _env_value("PLATFORM_MASTER_EMAIL") is None or PLATFORM_MASTER_EMAIL == DEV_DEFAULT_PLATFORM_MASTER_EMAIL:
        problems.append("PLATFORM_MASTER_EMAIL precisa ser explicito e nao pode usar o default do repositorio.")
    if _env_value("CHANNEL_BOOTSTRAP_EMAIL") is not None and _looks_like_placeholder(CHANNEL_BOOTSTRAP_EMAIL):
        problems.append("CHANNEL_BOOTSTRAP_EMAIL usa placeholder e precisa ser explicitado.")
    if _is_insecure_secret(PLATFORM_MASTER_PASSWORD):
        problems.append("PLATFORM_MASTER_PASSWORD esta ausente ou usa valor inseguro/placeholder.")
    if _is_insecure_secret(DEFAULT_PASSWORD):
        problems.append("SEED_PASSWORD esta ausente ou usa valor inseguro/placeholder.")
    if _env_value("CHANNEL_BOOTSTRAP_PASSWORD") is not None and _is_insecure_secret(CHANNEL_BOOTSTRAP_PASSWORD):
        problems.append("CHANNEL_BOOTSTRAP_PASSWORD usa valor inseguro/placeholder.")

    if problems:
        raise RuntimeError("Unsafe production seed bootstrap: " + " ".join(problems))


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


def _upsert_user(
    email: str,
    password: str,
    nome: str,
    role: str,
    *,
    username: str | None = None,
    must_change_password: bool = False,
) -> str:
    normalized_email = email.strip().lower()
    normalized_username = validate_username(username or username_from_email_candidate(normalized_email))
    pwd_hash = hash_password(password)
    sql = """
      INSERT INTO auth.users (email, username, password_hash, nome, role, valid_from, is_active, must_change_password)
      VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, true, %s)
      ON CONFLICT (email)
      DO UPDATE SET
        username = EXCLUDED.username,
        password_hash = EXCLUDED.password_hash,
        nome = EXCLUDED.nome,
        role = EXCLUDED.role,
        is_active = true,
        must_change_password = EXCLUDED.must_change_password,
        valid_from = COALESCE(auth.users.valid_from, CURRENT_DATE)
      RETURNING id
    """

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            sql,
            (normalized_email, normalized_username, pwd_hash, nome, role, must_change_password),
        ).fetchone()
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
    _assert_safe_bootstrap_runtime()
    _, _, bootstrap_channel_id = _bootstrap_internal_users()

    print("\n=== TorqMind seed concluído ===")
    print(f"Modo: {SEED_MODE}")
    print("\nUsuários criados/atualizados:")
    print(
        "  PLATFORM MASTER -> "
        f"{PLATFORM_MASTER_EMAIL} / {_masked_secret_label('PLATFORM_MASTER_PASSWORD', fallback_label='padrao local/dev')}"
    )
    print(
        "  CHANNEL ADMIN   -> "
        f"{CHANNEL_BOOTSTRAP_EMAIL} / "
        f"{_masked_secret_label('CHANNEL_BOOTSTRAP_PASSWORD', fallback_label=_secret_origin('SEED_PASSWORD', fallback_label='padrao local/dev'))}"
    )
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

        print(
            "  TENANT ADMIN    -> "
            f"{OWNER_EMAIL} / {_masked_secret_label('SEED_PASSWORD', fallback_label='padrao local/dev')}"
        )
        print(
            "  TENANT MANAGER  -> "
            f"{MANAGER_EMAIL} / {_masked_secret_label('SEED_PASSWORD', fallback_label='padrao local/dev')}"
        )
        print(f"\nTenant demo (id_empresa=1) ingest_key: {ingest_key}")
    else:
        print("\nNenhum tenant/filial demo foi criado nesse seed. Apenas os usuários internos e o canal bootstrap foram sincronizados.")


if __name__ == "__main__":
    main()
