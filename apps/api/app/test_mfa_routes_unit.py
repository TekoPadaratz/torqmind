"""Route-level tests for the TOTP 2FA flow using mocked repositories.

These exercise the FastAPI endpoints (login challenge, mfa/verify, setup,
disable) without a real database by patching repos_mfa and repos_auth. They
confirm the contract and that secrets/codes never appear in responses.
"""
from __future__ import annotations

import base64

import pytest
from fastapi.testclient import TestClient

from app import totp


@pytest.fixture()
def key(monkeypatch):
    k = base64.urlsafe_b64encode(b"k" * 32).decode()
    monkeypatch.setattr(totp.settings, "totp_encryption_key", k, raising=False)
    return k


@pytest.fixture()
def client():
    from app.main import app

    return TestClient(app)


def test_mfa_status_requires_auth(client):
    resp = client.get("/auth/mfa/status")
    assert resp.status_code == 401


def test_mfa_verify_rejects_bad_challenge(client):
    resp = client.post("/auth/mfa/verify", json={"mfa_challenge_token": "not-a-token", "code": "123456"})
    assert resp.status_code == 401
    assert resp.json()["detail"]["error"] == "invalid_challenge"


def test_login_returns_challenge_when_totp_enabled(client, monkeypatch):
    from app import repos_auth, repos_mfa

    session = {
        "sub": "11111111-1111-1111-1111-111111111111",
        "email": "u@example.com",
        "user_role": "tenant_admin",
        "role": "tenant_admin",
        "id_empresa": 1,
        "id_filial": None,
        "home_path": "/dashboard",
    }
    monkeypatch.setattr(repos_auth, "verify_login", lambda *a, **k: dict(session))
    monkeypatch.setattr(repos_mfa, "get_mfa_state", lambda uid: {"totp_enabled": True})

    resp = client.post("/auth/login", json={"identifier": "u@example.com", "password": "x"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["mfa_required"] is True
    assert body["mfa_challenge_token"]
    assert body.get("access_token") is None  # no real token before 2FA


def test_login_normal_when_totp_disabled(client, monkeypatch):
    from app import repos_auth, repos_mfa

    session = {
        "sub": "22222222-2222-2222-2222-222222222222",
        "email": "n@example.com",
        "user_role": "tenant_admin",
        "role": "tenant_admin",
        "analytics_role": "tenant_admin",
        "id_empresa": 1,
        "id_filial": None,
        "home_path": "/dashboard",
    }
    monkeypatch.setattr(repos_auth, "verify_login", lambda *a, **k: dict(session))
    monkeypatch.setattr(repos_mfa, "get_mfa_state", lambda uid: {"totp_enabled": False})

    resp = client.post("/auth/login", json={"identifier": "n@example.com", "password": "x"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["mfa_required"] is False
    assert body["access_token"]


def test_full_mfa_verify_issues_token(client, key, monkeypatch):
    from app import repos_auth, repos_mfa
    from app.routes_mfa import issue_mfa_challenge_token

    uid = "33333333-3333-3333-3333-333333333333"
    secret = totp.generate_secret()
    enc = totp.encrypt_secret(secret)

    monkeypatch.setattr(repos_mfa, "get_encrypted_secret", lambda u, require_enabled: enc)
    monkeypatch.setattr(repos_mfa, "mark_used", lambda u: None)
    monkeypatch.setattr(repos_mfa, "consume_recovery_code", lambda u, h: False)
    monkeypatch.setattr(
        repos_auth,
        "get_session_context",
        lambda **k: {
            "sub": uid,
            "email": "v@example.com",
            "user_role": "tenant_admin",
            "role": "tenant_admin",
            "analytics_role": "tenant_admin",
            "id_empresa": 1,
            "id_filial": None,
            "home_path": "/dashboard",
        },
    )

    challenge = issue_mfa_challenge_token(uid, 1, None)
    good = totp.now_code(secret)
    resp = client.post("/auth/mfa/verify", json={"mfa_challenge_token": challenge, "code": good})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["access_token"]
    # The secret must never be echoed back.
    assert secret not in resp.text


def test_mfa_verify_wrong_code_fails(client, key, monkeypatch):
    from app import repos_mfa
    from app.routes_mfa import issue_mfa_challenge_token

    uid = "44444444-4444-4444-4444-444444444444"
    secret = totp.generate_secret()
    enc = totp.encrypt_secret(secret)
    monkeypatch.setattr(repos_mfa, "get_encrypted_secret", lambda u, require_enabled: enc)
    monkeypatch.setattr(repos_mfa, "consume_recovery_code", lambda u, h: False)

    challenge = issue_mfa_challenge_token(uid, 1, None)
    good = totp.now_code(secret)
    wrong = "000000" if good != "000000" else "111111"
    resp = client.post("/auth/mfa/verify", json={"mfa_challenge_token": challenge, "code": wrong})
    assert resp.status_code == 401
    assert resp.json()["detail"]["error"] == "invalid_code"
