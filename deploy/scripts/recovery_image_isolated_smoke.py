#!/usr/bin/env python3
"""Isolated recovery-image smoke (no network, synthetic secrets only).

Copied into a ``docker run --rm --network none`` of a rollback-dual-verify image.
Does not attach Hom/Prod databases or put the image in service.
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    os.environ.setdefault("APP_ENV", "test")
    os.environ.setdefault("API_JWT_SECRET", "recovery-smoke-secret-32chars-min!!")
    os.environ.setdefault("API_JWT_ISSUER", "torqmind-api")
    os.environ.setdefault("INGEST_REQUIRE_KEY", "false")

    from passlib.context import CryptContext

    from app.security import (
        TOKEN_USE_ACCESS,
        TOKEN_USE_MFA_CHALLENGE,
        _argon2_available,
        create_access_token,
        decode_token,
        hash_password,
        identify_password_scheme,
        verify_password,
    )

    assert _argon2_available(), "argon2 missing in recovery image"
    pw = "RecoverySmoke!9f3a"
    bcrypt_hash = CryptContext(schemes=["bcrypt"], deprecated="auto").hash(pw)
    assert identify_password_scheme(bcrypt_hash) == "bcrypt"
    assert verify_password(pw, bcrypt_hash), "bcrypt verify failed"

    argon_hash = hash_password(pw)
    assert identify_password_scheme(argon_hash) == "argon2"
    assert verify_password(pw, argon_hash), "argon2 verify failed"
    assert not verify_password("wrong", argon_hash)

    access = decode_token(
        create_access_token({"sub": "smoke", "user_role": "tenant_admin"}, token_use=TOKEN_USE_ACCESS)
    )
    assert access.get("token_use") == TOKEN_USE_ACCESS
    assert "session_exp" in access

    challenge = decode_token(
        create_access_token({"sub": "smoke"}, minutes=5, token_use=TOKEN_USE_MFA_CHALLENGE)
    )
    assert challenge.get("token_use") == TOKEN_USE_MFA_CHALLENGE
    assert challenge.get("mfa_pending") is True
    assert "session_exp" not in challenge

    import app.security  # noqa: F401
    from app import config as _config  # noqa: F401

    print("RECOVERY_ISOLATED_SMOKE_PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print("RECOVERY_ISOLATED_SMOKE_FAIL", type(exc).__name__, str(exc)[:200], file=sys.stderr)
        raise
