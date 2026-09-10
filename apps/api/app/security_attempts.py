"""Shared abuse-attempt buckets backed by ``auth.security_attempt_buckets``.

Prompt 4 introduced this table (migration 155). Prompt 5+ MUST reuse it —
do not create a second limiter table or a competing policy.

Production: Postgres, shared across API workers.
APP_ENV=test: in-process map (no PG required for unit tests).
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Optional

from app.config import settings
from app.db import get_conn

logger = logging.getLogger("torqmind.security_attempts")

_test_attempts: dict[str, tuple[int, float]] = {}


def _use_memory_store() -> bool:
    return str(getattr(settings, "app_env", "") or "").strip().lower() == "test"


def bucket_key(*parts: object) -> str:
    """Stable key; joins non-empty parts with ':'."""
    cleaned = [str(p).strip() for p in parts if p is not None and str(p).strip()]
    return ":".join(cleaned)


def identity_hash(value: str) -> str:
    """Hash an identifier/email for bucket keys (no plaintext in PK)."""
    normalized = (value or "").strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


def is_rate_limited(
    key: str,
    *,
    max_attempts: int,
    window_seconds: int,
) -> bool:
    """True when the shared bucket is exhausted."""
    limit = max(1, int(max_attempts))
    window = max(1, int(window_seconds))
    if _use_memory_store():
        count, started = _test_attempts.get(key, (0, time.time()))
        if time.time() - started > window:
            return False
        return count >= limit

    with get_conn(role="MASTER", tenant_id=None, branch_id=None, purpose="auth") as conn:
        row = conn.execute(
            """
            SELECT attempt_count, window_started_at
            FROM auth.security_attempt_buckets
            WHERE bucket_key = %s
            """,
            (key,),
        ).fetchone()
        if not row:
            return False
        age = conn.execute(
            "SELECT EXTRACT(EPOCH FROM (NOW() - %s::timestamptz)) AS age",
            (row["window_started_at"],),
        ).fetchone()
        age_s = float(age["age"] if age else 0)
        if age_s > window:
            return False
        return int(row["attempt_count"] or 0) >= limit


def record_attempt(
    key: str,
    *,
    window_seconds: int,
    prune_after_seconds: int | None = None,
) -> int:
    """Increment the shared counter; returns the new count in the active window."""
    window = max(1, int(window_seconds))
    if _use_memory_store():
        count, started = _test_attempts.get(key, (0, time.time()))
        if time.time() - started > window:
            count, started = 0, time.time()
        count += 1
        _test_attempts[key] = (count, started)
        return count

    with get_conn(role="MASTER", tenant_id=None, branch_id=None, purpose="auth") as conn:
        row = conn.execute(
            """
            INSERT INTO auth.security_attempt_buckets (bucket_key, attempt_count, window_started_at, updated_at)
            VALUES (%s, 1, NOW(), NOW())
            ON CONFLICT (bucket_key) DO UPDATE
            SET
              attempt_count = CASE
                WHEN EXTRACT(EPOCH FROM (NOW() - auth.security_attempt_buckets.window_started_at)) > %s
                  THEN 1
                ELSE auth.security_attempt_buckets.attempt_count + 1
              END,
              window_started_at = CASE
                WHEN EXTRACT(EPOCH FROM (NOW() - auth.security_attempt_buckets.window_started_at)) > %s
                  THEN NOW()
                ELSE auth.security_attempt_buckets.window_started_at
              END,
              updated_at = NOW()
            RETURNING attempt_count
            """,
            (key, window, window),
        ).fetchone()
        # Opportunistic cardinality bound: drop stale buckets (best-effort).
        ttl = int(prune_after_seconds if prune_after_seconds is not None else 86400)
        try:
            conn.execute(
                """
                DELETE FROM auth.security_attempt_buckets
                WHERE updated_at < NOW() - make_interval(secs => %s)
                  AND bucket_key <> %s
                """,
                (ttl, key),
            )
        except Exception:  # noqa: BLE001 — prune must never fail the request
            logger.debug("security_attempt_buckets prune skipped", exc_info=True)
        conn.commit()
    return int(row["attempt_count"] if row else 1)


def clear_attempts(key: str) -> None:
    if _use_memory_store():
        _test_attempts.pop(key, None)
        return
    with get_conn(role="MASTER", tenant_id=None, branch_id=None, purpose="auth") as conn:
        conn.execute("DELETE FROM auth.security_attempt_buckets WHERE bucket_key = %s", (key,))
        conn.commit()


def reset_memory_store_for_tests() -> None:
    _test_attempts.clear()
