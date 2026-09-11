#!/usr/bin/env python3
"""Isolated recovery-image route smoke (mocked DB/services, no network).

Run inside ``docker run --rm --network none -v ...:/smoke.py IMAGE python /smoke.py``.
Does not attach Hom/Prod databases or put the image in service.

Covers:
- create_access_token old contract (keeps mfa_pending/scope)
- login MFA → only intermediate token; /auth/me + protected deps reject it
- login sem MFA → access_token
- senha incorreta → 401
- bcrypt + Argon2 verify
"""
from __future__ import annotations

import inspect
import os
import sys
from typing import Any
from unittest.mock import patch


def main() -> int:
    os.environ.setdefault("APP_ENV", "test")
    os.environ.setdefault("API_JWT_SECRET", "recovery-smoke-secret-32chars-min!!")
    os.environ.setdefault("API_JWT_ISSUER", "torqmind-api")
    os.environ.setdefault("INGEST_REQUIRE_KEY", "false")

    from passlib.context import CryptContext
    from fastapi.testclient import TestClient

    from app.security import create_access_token, decode_token, hash_password, identify_password_scheme, verify_password
    from app import repos_auth
    from app.deps import get_current_claims

    # --- password dual verify ---
    pw = "RecoverySmoke!9f3a"
    bcrypt_hash = CryptContext(schemes=["bcrypt"], deprecated="auto").hash(pw)
    assert identify_password_scheme(bcrypt_hash) == "bcrypt"
    assert verify_password(pw, bcrypt_hash)
    argon_hash = hash_password(pw)
    assert identify_password_scheme(argon_hash) == "argon2"
    assert verify_password(pw, argon_hash)
    assert not verify_password("wrong-password", argon_hash)

    # --- old create_access_token contract ---
    sig = inspect.signature(create_access_token)
    assert list(sig.parameters) == ["payload", "minutes"], sig
    challenge = decode_token(
        create_access_token(
            {"sub": "u1", "scope": "mfa_challenge", "mfa_pending": True, "id_empresa": 1, "id_filial": 2},
            minutes=5,
        )
    )
    assert challenge.get("mfa_pending") is True
    assert challenge.get("scope") == "mfa_challenge"
    assert "token_use" not in challenge
    access = decode_token(create_access_token({"sub": "u1", "role": "MANAGER"}, minutes=30))
    assert access.get("mfa_pending") is None
    assert "token_use" not in access

    uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    session_ok: dict[str, Any] = {
        "sub": uid,
        "email": "recovery.smoke@example.com",
        "user_role": "tenant_manager",
        "role": "MANAGER",
        "analytics_role": "MANAGER",
        "id_empresa": 1,
        "id_filial": 14458,
        "channel_id": None,
        "must_change_password": False,
        "home_path": "/sales",
        "allowed_screens": ["dashboard_home"],
    }

    from app.main import app

    client = TestClient(app)

    def _login(identifier: str, password: str):
        return client.post("/auth/login", json={"identifier": identifier, "password": password})

    # wrong password
    with patch.object(
        repos_auth,
        "verify_login",
        side_effect=repos_auth.AuthError(401, "invalid_credentials", "Credenciais inválidas."),
    ):
        bad = _login("recovery.smoke@example.com", "wrong")
        assert bad.status_code == 401, bad.text
        assert (bad.json().get("error") or bad.json().get("detail", {}).get("error")) == "invalid_credentials"

    # login without MFA → access_token only
    with patch.object(repos_auth, "verify_login", return_value=session_ok), patch(
        "app.repos_mfa.get_mfa_state", return_value={"totp_enabled": False, "totp_required": False}
    ), patch.object(repos_auth, "get_session_context", return_value=session_ok):
        ok = _login("recovery.smoke@example.com", pw)
        assert ok.status_code == 200, ok.text
        body = ok.json()
        assert body.get("access_token")
        assert not body.get("mfa_required")
        assert not body.get("mfa_challenge_token")
        me = client.get("/auth/me", headers={"Authorization": f"Bearer {body['access_token']}"})
        assert me.status_code == 200, me.text

    # login with MFA → intermediate only
    with patch.object(repos_auth, "verify_login", return_value=session_ok), patch(
        "app.repos_mfa.get_mfa_state", return_value={"totp_enabled": True, "totp_required": False}
    ):
        mfa = _login("recovery.smoke@example.com", pw)
        assert mfa.status_code == 200, mfa.text
        body = mfa.json()
        assert body.get("mfa_required") is True
        challenge_tok = body.get("mfa_challenge_token")
        assert challenge_tok
        assert not body.get("access_token")
        payload = decode_token(challenge_tok)
        assert payload.get("mfa_pending") is True
        assert payload.get("scope") == "mfa_challenge"

        me = client.get("/auth/me", headers={"Authorization": f"Bearer {challenge_tok}"})
        assert me.status_code == 401, me.text
        detail = me.json()
        err = detail.get("error") or (detail.get("detail") or {}).get("error")
        assert err == "mfa_required", detail

        # Protected dependency path (BI-style)
        try:
            get_current_claims(authorization=f"Bearer {challenge_tok}")
            raise AssertionError("get_current_claims accepted MFA challenge token")
        except Exception as exc:  # HTTPException
            status = getattr(exc, "status_code", None)
            detail = getattr(exc, "detail", {})
            assert status == 401, exc
            if isinstance(detail, dict):
                assert detail.get("error") == "mfa_required", detail

    print("RECOVERY_ROUTE_SMOKE_PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print("RECOVERY_ROUTE_SMOKE_FAIL", type(exc).__name__, str(exc)[:300], file=sys.stderr)
        raise
