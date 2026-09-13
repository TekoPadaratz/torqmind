"""Unit tests — Prompt 7 cookie session extract / browser vs bearer / CSRF."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.config import settings
from app import session_cookies as sc


def _request(
    *,
    cookies: dict | None = None,
    headers: dict | None = None,
    method: str = "GET",
) -> Request:
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": "/auth/me",
        "raw_path": b"/auth/me",
        "query_string": b"",
        "headers": hdrs,
        "client": ("127.0.0.1", 123),
        "server": ("test", 80),
    }
    req = Request(scope)
    if cookies:
        # Starlette reads cookies from header
        cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())
        scope["headers"] = list(scope["headers"]) + [(b"cookie", cookie_header.encode())]
        req = Request(scope)
    return req


@pytest.fixture(autouse=True)
def _cookie_env(monkeypatch):
    monkeypatch.setattr(settings, "app_env", "test")
    monkeypatch.setattr(settings, "auth_cookie_suffix", "_test")
    monkeypatch.setattr(settings, "auth_allow_bearer_fallback", True)
    monkeypatch.setattr(settings, "auth_csrf_enforce", True)
    monkeypatch.setattr(settings, "app_cors_origins", "http://localhost:3000")
    yield


def test_browser_requires_cookie_ignores_bearer():
    req = _request(
        headers={"sec-fetch-site": "same-origin", "authorization": "Bearer leaked"},
    )
    with pytest.raises(HTTPException) as exc:
        sc.extract_access_token(req, "Bearer leaked")
    assert exc.value.status_code == 401


def test_browser_uses_access_cookie():
    name = sc.access_cookie_name()
    req = _request(
        cookies={name: "cookie-jwt"},
        headers={"sec-fetch-site": "same-origin"},
    )
    assert sc.extract_access_token(req, "Bearer ignored") == "cookie-jwt"


def test_non_browser_bearer_fallback():
    req = _request(headers={})
    assert sc.extract_access_token(req, "Bearer service-jwt") == "service-jwt"


def test_csrf_skipped_without_session_cookies():
    req = _request(method="POST", headers={"sec-fetch-site": "same-origin"})
    sc.validate_csrf(req)  # must not raise


def test_csrf_requires_header_match():
    name = sc.access_cookie_name()
    csrf_name = sc.csrf_cookie_name()
    req = _request(
        method="POST",
        cookies={name: "jwt", csrf_name: "abc"},
        headers={"sec-fetch-site": "same-origin", "x-csrf-token": "abc"},
    )
    sc.validate_csrf(req)

    bad = _request(
        method="POST",
        cookies={name: "jwt", csrf_name: "abc"},
        headers={"sec-fetch-site": "same-origin", "x-csrf-token": "zzz"},
    )
    with pytest.raises(HTTPException) as exc:
        sc.validate_csrf(bad)
    assert exc.value.status_code == 403


def test_mfa_challenge_browser_cookie_only():
    ch = sc.mfa_challenge_cookie_name()
    req = _request(
        cookies={ch: "challenge-jwt"},
        headers={"sec-fetch-site": "same-origin"},
    )
    assert sc.extract_mfa_challenge_token(req, "body-ignored") == "challenge-jwt"


def test_cookie_names_suffix():
    assert sc.access_cookie_name().endswith("_test")
    assert sc.csrf_cookie_name().endswith("_test")


def test_attach_login_sets_csrf(monkeypatch):
    response = MagicMock()
    token = sc.attach_login_cookies(response, access_token="access-jwt")
    assert token
    assert response.set_cookie.called


def test_allowed_origins_includes_cors_and_web_public_url(monkeypatch):
    monkeypatch.setattr(
        settings,
        "app_cors_origins",
        "http://redevr.ddns.me:14023,http://172.30.0.10,https://www.torqmind.com.br,https://torqmind.com.br",
    )
    monkeypatch.setattr(settings, "web_public_url", "https://www.torqmind.com.br")
    origins = sc.allowed_origins()
    assert "https://www.torqmind.com.br" in origins
    assert "https://torqmind.com.br" in origins
    assert "http://redevr.ddns.me:14023" in origins
    assert "http://172.30.0.10" in origins
