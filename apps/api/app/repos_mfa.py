"""Repository for TOTP/2FA state on auth.users.

Kept separate from repos_auth so the existing user SELECTs stay untouched; this
module reads only the MFA-relevant columns and never returns the encrypted
secret to callers other than the verification path.

Attempt throttling uses the shared ``auth.security_attempt_buckets`` helper
(``app.security_attempts``) — same table as Prompt 4 / migration 155.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.config import settings
from app.db import get_conn
from app import security_attempts
from app.totp import decrypt_secret, hash_recovery_code, match_code

logger = logging.getLogger("torqmind.mfa")

# In-process TOTP counter claim only when APP_ENV=test (no Postgres).
_test_counters: dict[str, int] = {}


def _use_memory_store() -> bool:
    return str(getattr(settings, "app_env", "") or "").strip().lower() == "test"


@dataclass(frozen=True)
class MfaVerifyOutcome:
    ok: bool
    error: Optional[str] = None  # too_many_attempts | invalid_code | mfa_not_enabled | replay
    used_recovery: bool = False
    totp_counter: Optional[int] = None


def attempt_bucket_key(purpose: str, user_id: str) -> str:
    return security_attempts.bucket_key("mfa", purpose, user_id)


def is_rate_limited(bucket_key: str, *, max_attempts: int | None = None, window_seconds: int | None = None) -> bool:
    limit = int(max_attempts if max_attempts is not None else settings.mfa_max_attempts)
    window = int(window_seconds if window_seconds is not None else settings.mfa_challenge_ttl_minutes * 60)
    return security_attempts.is_rate_limited(bucket_key, max_attempts=limit, window_seconds=window)


def record_failed_attempt(bucket_key: str, *, window_seconds: int | None = None) -> int:
    window = int(window_seconds if window_seconds is not None else settings.mfa_challenge_ttl_minutes * 60)
    return security_attempts.record_attempt(bucket_key, window_seconds=window)


def clear_attempts(bucket_key: str) -> None:
    security_attempts.clear_attempts(bucket_key)


def claim_totp_counter(user_id: str, counter: int) -> bool:
    """Atomically accept a TOTP counter once (replay → False)."""
    if _use_memory_store():
        last = _test_counters.get(user_id)
        if last is not None and last >= counter:
            return False
        _test_counters[user_id] = counter
        return True

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            UPDATE auth.users
            SET totp_last_counter = %s,
                totp_last_used_at = NOW(),
                updated_at = NOW()
            WHERE id = %s::uuid
              AND totp_enabled = true
              AND (totp_last_counter IS NULL OR totp_last_counter < %s)
            RETURNING id
            """,
            (counter, user_id, counter),
        ).fetchone()
        conn.commit()
    return bool(row)


def verify_totp_or_recovery(
    user_id: str,
    code: str,
    *,
    purpose: str,
    require_enabled: bool = True,
    allow_recovery: bool = True,
) -> MfaVerifyOutcome:
    """Shared MFA gate: PG rate-limit + TOTP counter claim or one-time recovery."""
    bucket = attempt_bucket_key(purpose, user_id)
    if is_rate_limited(bucket):
        return MfaVerifyOutcome(ok=False, error="too_many_attempts")

    enc = get_encrypted_secret(user_id, require_enabled=require_enabled)
    if not enc:
        return MfaVerifyOutcome(ok=False, error="mfa_not_enabled")

    raw = (code or "").strip()
    if not raw:
        record_failed_attempt(bucket)
        return MfaVerifyOutcome(ok=False, error="invalid_code")

    try:
        secret = decrypt_secret(enc)
        matched = match_code(secret, raw)
    except Exception:  # noqa: BLE001 — never leak crypto errors
        logger.warning("TOTP verify failed to decrypt/evaluate for user purpose=%s", purpose)
        matched = None

    if matched is not None:
        if require_enabled:
            if not claim_totp_counter(user_id, matched):
                record_failed_attempt(bucket)
                return MfaVerifyOutcome(ok=False, error="replay")
        clear_attempts(bucket)
        return MfaVerifyOutcome(ok=True, totp_counter=matched)

    if allow_recovery and consume_recovery_code(user_id, hash_recovery_code(raw)):
        clear_attempts(bucket)
        mark_used(user_id)
        return MfaVerifyOutcome(ok=True, used_recovery=True)

    record_failed_attempt(bucket)
    return MfaVerifyOutcome(ok=False, error="invalid_code")


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


def enable_after_confirm(user_id: str, *, totp_counter: int | None = None) -> bool:
    """Mark 2FA enabled after the first valid code confirms the secret.

    Concurrency-safe: only one winner when ``totp_enabled`` flips false→true.
    Returns False if another request already enabled MFA.
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            UPDATE auth.users
            SET totp_enabled = true,
                totp_confirmed_at = NOW(),
                totp_last_used_at = NOW(),
                totp_last_counter = COALESCE(%s, totp_last_counter),
                mfa_reset_required = false,
                updated_at = NOW()
            WHERE id = %s::uuid
              AND totp_enabled = false
              AND totp_secret_encrypted IS NOT NULL
            RETURNING id
            """,
            (totp_counter, user_id),
        ).fetchone()
        conn.commit()
    return bool(row)


def mark_used(user_id: str) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            "UPDATE auth.users SET totp_last_used_at = NOW() WHERE id = %s::uuid",
            (user_id,),
        )
        conn.commit()


def disable(user_id: str, *, clear_secret: bool = True, force: bool = False) -> bool:
    """Disable 2FA for a user (used by self-disable and admin reset).

    Returns False when MFA was already disabled (lost race), unless ``force``.
    """
    enabled_gate = "" if force else "AND totp_enabled = true"
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        if clear_secret:
            row = conn.execute(
                f"""
                UPDATE auth.users
                SET totp_enabled = false,
                    totp_secret_encrypted = NULL,
                    totp_confirmed_at = NULL,
                    totp_last_counter = NULL,
                    mfa_reset_required = false,
                    updated_at = NOW()
                WHERE id = %s::uuid
                  {enabled_gate}
                RETURNING id
                """,
                (user_id,),
            ).fetchone()
            if row or force:
                conn.execute(
                    "DELETE FROM auth.user_recovery_codes WHERE user_id = %s::uuid",
                    (user_id,),
                )
        else:
            row = conn.execute(
                f"""
                UPDATE auth.users
                SET totp_enabled = false, updated_at = NOW()
                WHERE id = %s::uuid
                  {enabled_gate}
                RETURNING id
                """,
                (user_id,),
            ).fetchone()
        conn.commit()
    return bool(row)


def set_required(user_id: str, required: bool) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            "UPDATE auth.users SET totp_required = %s, updated_at = NOW() WHERE id = %s::uuid",
            (required, user_id),
        )
        conn.commit()


def admin_reset(user_id: str) -> None:
    """Admin reset: wipe 2FA so the user must reconfigure from scratch."""
    disable(user_id, clear_secret=True, force=True)
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
