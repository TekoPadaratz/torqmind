"""Repository for TOTP/2FA state on auth.users.

Kept separate from repos_auth so the existing user SELECTs stay untouched; this
module reads only the MFA-relevant columns and never returns the encrypted
secret to callers other than the verification path.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.db import get_conn

logger = logging.getLogger("torqmind.mfa")


def get_mfa_state(user_id: str) -> Optional[Dict[str, Any]]:
    """Return MFA flags for a user (no decrypted secret)."""
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            SELECT id, email, username, nome, role, is_active,
                   totp_enabled, totp_confirmed_at, totp_required,
                   totp_last_used_at, mfa_reset_required,
                   (totp_secret_encrypted IS NOT NULL) AS has_secret
            FROM auth.users
            WHERE id = %s::uuid
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def _get_secret_row(user_id: str) -> Optional[Dict[str, Any]]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        return conn.execute(
            """
            SELECT totp_enabled, totp_secret_encrypted
            FROM auth.users
            WHERE id = %s::uuid
            """,
            (user_id,),
        ).fetchone()


def get_encrypted_secret(user_id: str, *, require_enabled: bool) -> Optional[str]:
    """Return the encrypted secret, optionally requiring totp_enabled."""
    row = _get_secret_row(user_id)
    if not row or not row.get("totp_secret_encrypted"):
        return None
    if require_enabled and not row.get("totp_enabled"):
        return None
    return str(row["totp_secret_encrypted"])


def stage_secret(user_id: str, encrypted_secret: str) -> None:
    """Store a freshly-generated (not yet confirmed) secret. Keeps 2FA disabled."""
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            """
            UPDATE auth.users
            SET totp_secret_encrypted = %s,
                totp_enabled = false,
                totp_confirmed_at = NULL,
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (encrypted_secret, user_id),
        )
        conn.commit()


def enable_after_confirm(user_id: str) -> None:
    """Mark 2FA enabled after the first valid code confirms the secret."""
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            """
            UPDATE auth.users
            SET totp_enabled = true,
                totp_confirmed_at = NOW(),
                totp_last_used_at = NOW(),
                mfa_reset_required = false,
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (user_id,),
        )
        conn.commit()


def mark_used(user_id: str) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            "UPDATE auth.users SET totp_last_used_at = NOW() WHERE id = %s::uuid",
            (user_id,),
        )
        conn.commit()


def disable(user_id: str, *, clear_secret: bool = True) -> None:
    """Disable 2FA for a user (used by self-disable and admin reset)."""
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        if clear_secret:
            conn.execute(
                """
                UPDATE auth.users
                SET totp_enabled = false,
                    totp_secret_encrypted = NULL,
                    totp_confirmed_at = NULL,
                    mfa_reset_required = false,
                    updated_at = NOW()
                WHERE id = %s::uuid
                """,
                (user_id,),
            )
            conn.execute(
                "DELETE FROM auth.user_recovery_codes WHERE user_id = %s::uuid",
                (user_id,),
            )
        else:
            conn.execute(
                "UPDATE auth.users SET totp_enabled = false, updated_at = NOW() WHERE id = %s::uuid",
                (user_id,),
            )
        conn.commit()


def set_required(user_id: str, required: bool) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            "UPDATE auth.users SET totp_required = %s, updated_at = NOW() WHERE id = %s::uuid",
            (required, user_id),
        )
        conn.commit()


def admin_reset(user_id: str) -> None:
    """Admin reset: wipe 2FA so the user must reconfigure from scratch."""
    disable(user_id, clear_secret=True)
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            "UPDATE auth.users SET mfa_reset_required = true, updated_at = NOW() WHERE id = %s::uuid",
            (user_id,),
        )
        conn.commit()


# ── Recovery codes ───────────────────────────────────────────

def replace_recovery_codes(user_id: str, code_hashes: list[str]) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            "DELETE FROM auth.user_recovery_codes WHERE user_id = %s::uuid",
            (user_id,),
        )
        for h in code_hashes:
            conn.execute(
                "INSERT INTO auth.user_recovery_codes (user_id, code_hash) VALUES (%s::uuid, %s)",
                (user_id, h),
            )
        conn.commit()


def consume_recovery_code(user_id: str, code_hash: str) -> bool:
    """Atomically consume an unused recovery code. Returns True if consumed."""
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            UPDATE auth.user_recovery_codes
            SET used_at = NOW()
            WHERE id = (
                SELECT id FROM auth.user_recovery_codes
                WHERE user_id = %s::uuid AND code_hash = %s AND used_at IS NULL
                LIMIT 1
            )
            RETURNING id
            """,
            (user_id, code_hash),
        ).fetchone()
        conn.commit()
    return bool(row)
