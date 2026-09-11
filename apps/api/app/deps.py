from __future__ import annotations

from typing import Any, Optional

from fastapi import Header, HTTPException, Request

from app import repos_auth
from app.security import (
    TOKEN_USE_ACCESS,
    TOKEN_USE_MFA_CHALLENGE,
    TOKEN_USE_MFA_SETUP,
    assert_not_revoked_by_password_change,
    assert_session_not_absolutely_expired,
    classify_token_use,
    decode_token,
    is_intermediate_mfa_token,
)
from app.session_cookies import extract_access_token


def _extract_bearer(authorization: Optional[str]) -> str:
    """Legacy helper — prefer extract_access_token(request, authorization)."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"error": "missing_bearer", "message": "Missing bearer token"})
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail={"error": "missing_bearer", "message": "Missing bearer token"})
    return token


def decode_bearer_payload(authorization: Optional[str]) -> dict[str, Any]:
    token = _extract_bearer(authorization)
    try:
        return decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})


def decode_request_payload(
    request: Request,
    authorization: Optional[str] = None,
) -> dict[str, Any]:
    token = extract_access_token(request, authorization)
    try:
        return decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})


def _reject_wrong_token_use(payload: dict[str, Any], *, allowed: set[str]) -> None:
    token_use = classify_token_use(payload)
    if token_use in allowed:
        return
    if is_intermediate_mfa_token(payload):
        raise HTTPException(
            status_code=401,
            detail={"error": "mfa_required", "message": "Two-factor authentication required."},
        )
    raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})


def resolve_access_session(
    authorization: Optional[str] = None,
    *,
    request: Optional[Request] = None,
    include_default_scope: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Decode an access token, enforce revocation/absolute expiry, reload live session.

    ``include_default_scope`` defaults False so BI/deps hot paths skip mart/PG
    product-scope bootstrap. Login uses ``verify_login(..., True)``; ``/auth/me``,
    ``/auth/refresh`` and MFA completion pass True explicitly when the FE needs
    ``default_scope``.
    """
    if request is not None:
        payload = decode_request_payload(request, authorization)
    else:
        # Unit tests / callers without Request — bearer only.
        payload = decode_bearer_payload(authorization)
    _reject_wrong_token_use(payload, allowed={TOKEN_USE_ACCESS})

    try:
        assert_session_not_absolutely_expired(payload)
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail={"error": "session_expired", "message": "Sessão expirada. Faça login novamente."},
        )

    user_id = str(payload.get("sub") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})

    try:
        session = repos_auth.get_session_context(
            user_id=user_id,
            id_empresa=payload.get("id_empresa"),
            id_filial=payload.get("id_filial"),
            channel_id=payload.get("channel_id"),
            include_default_scope=include_default_scope,
        )
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())

    try:
        assert_not_revoked_by_password_change(payload, session.get("password_changed_at"))
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail={"error": "token_revoked", "message": "Sessão invalidada. Faça login novamente."},
        )

    return session, payload


def _resolve_session(
    authorization: Optional[str] = None,
    request: Optional[Request] = None,
    *,
    include_default_scope: bool = False,
) -> dict[str, Any]:
    # Hot-path BI deps skip product-scope defaults: that path used to MAX-scan
    # dw.fact_* on every authenticated request (~seconds under ETL IO).
    # Login /auth/me still request include_default_scope=True explicitly.
    session, _payload = resolve_access_session(
        authorization,
        request=request,
        include_default_scope=include_default_scope,
    )
    return session


def get_current_claims(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Standard dependency — blocks if user must change password."""
    session = _resolve_session(authorization, request=request, include_default_scope=False)
    if session.get("must_change_password"):
        raise HTTPException(
            status_code=403,
            detail={
                "error": "password_change_required",
                "message": "You must change your password before accessing this resource.",
            },
        )
    return session


def get_current_claims_allow_password_change(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Variant used by /auth/change-password — does NOT block on must_change_password."""
    return _resolve_session(authorization, request=request, include_default_scope=False)


# Re-export token constants for callers that need MFA enrollment checks.
__all__ = [
    "TOKEN_USE_ACCESS",
    "TOKEN_USE_MFA_CHALLENGE",
    "TOKEN_USE_MFA_SETUP",
    "resolve_access_session",
    "get_current_claims",
    "get_current_claims_allow_password_change",
    "decode_request_payload",
]
