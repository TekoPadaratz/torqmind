"""Browser session cookies (Prompt 7) — HttpOnly access + CSRF companion.

Same-origin browser traffic uses Next rewrite ``/api/*`` → API. Cookies use
``Path=/`` so the non-HttpOnly CSRF cookie is readable on app pages and credentials
are sent with ``/api`` calls. Homolog vs Prod isolation: distinct cookie name
suffix per ``app_env``.

Browser requests (Sec-Fetch-Site / Origin) MUST authenticate via cookie — bearer
from localStorage is ignored to avoid an indefinite dual-accept window.
Non-browser clients (TestClient, ETL scripts) may use Authorization when
``auth_allow_bearer_fallback`` is true and no browser signals are present.
"""
from __future__ import annotations

import hmac
import secrets
from typing import Any, Optional

from fastapi import HTTPException, Request, Response

from app.config import settings

COOKIE_PATH = "/"
CSRF_HEADER = "x-csrf-token"


def _env_suffix() -> str:
    env = str(getattr(settings, "app_env", "") or "").strip().lower()
    explicit = (getattr(settings, "auth_cookie_suffix", None) or "").strip()
    if explicit:
        return explicit
    if env in {"homolog", "homologacao", "homologation", "hom"}:
        return "_hom"
    if env in {"prod", "production"}:
        return ""
    if env == "test":
        return "_test"
    return f"_{env}" if env else "_dev"


def access_cookie_name() -> str:
    return f"tm_at{_env_suffix()}"


def mfa_challenge_cookie_name() -> str:
    return f"tm_mfa_ch{_env_suffix()}"


def mfa_setup_cookie_name() -> str:
    return f"tm_mfa_setup{_env_suffix()}"


def csrf_cookie_name() -> str:
    return f"tm_csrf{_env_suffix()}"


def cookie_secure() -> bool:
    """Secure flag: on for prod/homolog HTTPS; off for local http TestClient."""
    configured = getattr(settings, "auth_cookie_secure", None)
    if configured is not None and str(configured).strip() != "":
        return bool(configured)
    env = str(getattr(settings, "app_env", "") or "").strip().lower()
    return env in {"prod", "production", "homolog", "homologacao", "homologation", "hom"}


def cookie_samesite() -> str:
    value = (getattr(settings, "auth_cookie_samesite", None) or "lax").strip().lower()
    return value if value in {"lax", "strict", "none"} else "lax"


def is_browser_request(request: Request) -> bool:
    if request.headers.get("sec-fetch-site"):
        return True
    origin = (request.headers.get("origin") or "").strip()
    if origin:
        return True
    # Next rewrite same-origin often omits Origin on GET; Sec-Fetch-* still set.
    return False


def _max_age_seconds(minutes: Optional[int] = None) -> int:
    mins = int(minutes if minutes is not None else getattr(settings, "api_access_token_minutes", 480) or 480)
    return max(60, mins * 60)


def set_access_cookie(response: Response, token: str, *, minutes: Optional[int] = None) -> None:
    response.set_cookie(
        key=access_cookie_name(),
        value=token,
        max_age=_max_age_seconds(minutes),
        httponly=True,
        secure=cookie_secure(),
        samesite=cookie_samesite(),
        path=COOKIE_PATH,
    )


def set_mfa_challenge_cookie(response: Response, token: str) -> None:
    ttl = int(getattr(settings, "mfa_challenge_ttl_minutes", 5) or 5)
    response.set_cookie(
        key=mfa_challenge_cookie_name(),
        value=token,
        max_age=max(60, ttl * 60),
        httponly=True,
        secure=cookie_secure(),
        samesite=cookie_samesite(),
        path=COOKIE_PATH,
    )


def set_mfa_setup_cookie(response: Response, token: str) -> None:
    ttl = int(getattr(settings, "mfa_challenge_ttl_minutes", 5) or 5)
    response.set_cookie(
        key=mfa_setup_cookie_name(),
        value=token,
        max_age=max(60, ttl * 60),
        httponly=True,
        secure=cookie_secure(),
        samesite=cookie_samesite(),
        path=COOKIE_PATH,
    )


def issue_csrf_cookie(response: Response) -> str:
    token = secrets.token_urlsafe(32)
    response.set_cookie(
        key=csrf_cookie_name(),
        value=token,
        max_age=_max_age_seconds(),
        httponly=False,
        secure=cookie_secure(),
        samesite=cookie_samesite(),
        path=COOKIE_PATH,
    )
    return token


def clear_session_cookies(response: Response) -> None:
    for name in (
        access_cookie_name(),
        mfa_challenge_cookie_name(),
        mfa_setup_cookie_name(),
        csrf_cookie_name(),
    ):
        response.delete_cookie(key=name, path=COOKIE_PATH)


def clear_mfa_challenge_cookie(response: Response) -> None:
    response.delete_cookie(key=mfa_challenge_cookie_name(), path=COOKIE_PATH)


def clear_mfa_setup_cookie(response: Response) -> None:
    response.delete_cookie(key=mfa_setup_cookie_name(), path=COOKIE_PATH)


def _parse_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", 1)[1].strip()
    return token or None


def extract_access_token(request: Request, authorization: Optional[str] = None) -> str:
    """Resolve the access JWT for this request (cookie-first for browsers)."""
    cookie_token = (request.cookies.get(access_cookie_name()) or "").strip()
    bearer = _parse_bearer(authorization)
    browser = is_browser_request(request)

    if browser:
        if cookie_token:
            return cookie_token
        raise HTTPException(
            status_code=401,
            detail={"error": "missing_session", "message": "Sessão ausente. Faça login novamente."},
        )

    if cookie_token:
        return cookie_token
    allow_bearer = bool(getattr(settings, "auth_allow_bearer_fallback", True))
    if allow_bearer and bearer:
        return bearer
    raise HTTPException(
        status_code=401,
        detail={"error": "missing_bearer", "message": "Missing bearer token"},
    )


def extract_mfa_challenge_token(request: Request, body_token: Optional[str] = None) -> str:
    cookie_token = (request.cookies.get(mfa_challenge_cookie_name()) or "").strip()
    body = (body_token or "").strip()
    if is_browser_request(request):
        if cookie_token:
            return cookie_token
        raise HTTPException(
            status_code=401,
            detail={"error": "invalid_challenge", "message": "Desafio inválido ou expirado."},
        )
    token = cookie_token or body
    if not token:
        raise HTTPException(
            status_code=401,
            detail={"error": "invalid_challenge", "message": "Desafio inválido ou expirado."},
        )
    return token


def extract_mfa_setup_token(request: Request, authorization: Optional[str] = None) -> Optional[str]:
    cookie_token = (request.cookies.get(mfa_setup_cookie_name()) or "").strip()
    if cookie_token:
        return cookie_token
    if is_browser_request(request):
        return None
    return _parse_bearer(authorization)


def allowed_origins() -> set[str]:
    origins = {item.strip() for item in str(settings.app_cors_origins or "").split(",") if item.strip()}
    web = (getattr(settings, "web_public_url", None) or "").rstrip("/")
    if web:
        origins.add(web)
    return origins


def validate_browser_mutation_origin(request: Request) -> None:
    """Reject cross-site mutating requests when Origin/Referer is present and foreign."""
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    if not is_browser_request(request) and not request.cookies.get(access_cookie_name()):
        return
    origin = (request.headers.get("origin") or "").strip()
    referer = (request.headers.get("referer") or "").strip()
    allowed = allowed_origins()
    if not allowed:
        return
    if origin:
        if origin.rstrip("/") not in {o.rstrip("/") for o in allowed}:
            raise HTTPException(
                status_code=403,
                detail={"error": "origin_forbidden", "message": "Origin não permitida."},
            )
        return
    if referer:
        if not any(referer.startswith(o.rstrip("/") + "/") or referer.rstrip("/") == o.rstrip("/") for o in allowed):
            raise HTTPException(
                status_code=403,
                detail={"error": "origin_forbidden", "message": "Referer não permitido."},
            )


def validate_csrf(request: Request) -> None:
    """Double-submit CSRF for cookie-authenticated mutating browser calls."""
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    has_cookie_auth = bool(
        request.cookies.get(access_cookie_name())
        or request.cookies.get(mfa_challenge_cookie_name())
        or request.cookies.get(mfa_setup_cookie_name())
    )
    if not has_cookie_auth:
        # Login/forgot and bearer-only service clients skip CSRF.
        return
    cookie_csrf = (request.cookies.get(csrf_cookie_name()) or "").strip()
    header_csrf = (request.headers.get(CSRF_HEADER) or "").strip()
    if not cookie_csrf or not header_csrf or not hmac.compare_digest(cookie_csrf, header_csrf):
        raise HTTPException(
            status_code=403,
            detail={"error": "csrf_failed", "message": "Falha de verificação CSRF."},
        )


def attach_login_cookies(
    response: Response,
    *,
    access_token: Optional[str] = None,
    mfa_challenge_token: Optional[str] = None,
    mfa_setup_token: Optional[str] = None,
    access_minutes: Optional[int] = None,
) -> str:
    """Set session cookies and return the CSRF token (also in a readable cookie)."""
    if access_token:
        set_access_cookie(response, access_token, minutes=access_minutes)
        clear_mfa_challenge_cookie(response)
        clear_mfa_setup_cookie(response)
    if mfa_challenge_token:
        set_mfa_challenge_cookie(response, mfa_challenge_token)
        # Intermediate step — drop any prior access session.
        response.delete_cookie(key=access_cookie_name(), path=COOKIE_PATH)
        clear_mfa_setup_cookie(response)
    if mfa_setup_token:
        set_mfa_setup_cookie(response, mfa_setup_token)
        response.delete_cookie(key=access_cookie_name(), path=COOKIE_PATH)
        clear_mfa_challenge_cookie(response)
    return issue_csrf_cookie(response)
