"""Two-factor authentication (TOTP) endpoints.

Login flow:
  1. POST /auth/login with valid password and a 2FA-enabled user returns
     ``mfa_required=true`` + a short-lived ``mfa_challenge_token`` (no access token).
  2. POST /auth/mfa/verify exchanges the challenge + 6-digit code for a real
     access token.

Setup flow (authenticated user):
  - POST /auth/mfa/setup/start  -> returns otpauth URI + secret to scan a QR
  - POST /auth/mfa/setup/confirm -> validates first code, enables 2FA, returns
    one-time recovery codes
  - POST /auth/mfa/disable -> requires a valid code; turns 2FA off

Secrets are never returned after setup confirmation and never logged.

Attempt throttling is Postgres-backed (``auth.security_attempt_buckets``) so it
is shared across API workers. Successful TOTP codes claim ``totp_last_counter``
to block replay inside the validity window.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app import repos_auth, repos_mfa
from app.config import settings
from app.deps import _resolve_session, get_current_claims, get_current_claims_allow_password_change
from app.mfa_policy import policy_snapshot
from app.security import (
    TOKEN_USE_ACCESS,
    TOKEN_USE_MFA_CHALLENGE,
    TOKEN_USE_MFA_SETUP,
    classify_token_use,
    create_access_token,
    decode_token,
)
from app.session_cookies import (
    attach_login_cookies,
    clear_mfa_challenge_cookie,
    clear_mfa_setup_cookie,
    extract_mfa_challenge_token,
    extract_mfa_setup_token,
    is_browser_request,
)
from app.totp import (
    encrypt_secret,
    generate_recovery_codes,
    generate_secret,
    hash_recovery_code,
    is_totp_configured,
    provisioning_uri,
    qr_svg_data_uri,
)

logger = logging.getLogger("torqmind.mfa")

router = APIRouter(prefix="/auth/mfa", tags=["auth-mfa"])


def _raise_mfa_outcome(outcome: repos_mfa.MfaVerifyOutcome) -> None:
    if outcome.ok:
        return
    if outcome.error == "too_many_attempts":
        raise HTTPException(
            status_code=429,
            detail={"error": "too_many_attempts", "message": "Muitas tentativas. Faça login novamente."},
        )
    if outcome.error == "mfa_not_enabled":
        raise HTTPException(status_code=400, detail={"error": "mfa_not_enabled", "message": "2FA não está ativo."})
    if outcome.error == "replay":
        raise HTTPException(status_code=401, detail={"error": "invalid_code", "message": "Código já utilizado. Aguarde o próximo."})
    raise HTTPException(status_code=401, detail={"error": "invalid_code", "message": "Código inválido."})


def issue_mfa_challenge_token(user_id: str, id_empresa: Optional[int], id_filial: Optional[int]) -> str:
    """Short-lived token proving the password step passed; 2FA still pending."""
    payload = {
        "sub": user_id,
        "id_empresa": id_empresa,
        "id_filial": id_filial,
    }
    return create_access_token(
        payload,
        minutes=settings.mfa_challenge_ttl_minutes,
        token_use=TOKEN_USE_MFA_CHALLENGE,
    )


def issue_mfa_setup_token(user_id: str, id_empresa: Optional[int], id_filial: Optional[int]) -> str:
    """Short-lived token for FORCED 2FA setup (totp_required, not yet enabled).

    Authorizes only the setup endpoints (start/confirm); rejected as a normal
    bearer everywhere else.
    """
    payload = {
        "sub": user_id,
        "id_empresa": id_empresa,
        "id_filial": id_filial,
    }
    return create_access_token(
        payload,
        minutes=settings.mfa_challenge_ttl_minutes,
        token_use=TOKEN_USE_MFA_SETUP,
    )


def setup_claims(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Authorize 2FA setup via full session OR mfa_setup cookie/token."""
    setup_token = extract_mfa_setup_token(request, authorization)
    token = setup_token
    if not token and authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    if not token:
        # Fall through to access session (cookie/bearer).
        session = _resolve_session(authorization, request=request)
        session["mode"] = "session"
        return session
    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})
    if classify_token_use(payload) == TOKEN_USE_MFA_SETUP:
        uid = str(payload.get("sub") or "").strip()
        if not uid:
            raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})
        return {
            "sub": uid,
            "email": payload.get("email"),
            "id_empresa": payload.get("id_empresa"),
            "id_filial": payload.get("id_filial"),
            "mode": "setup",
        }
    if classify_token_use(payload) == TOKEN_USE_MFA_CHALLENGE:
        raise HTTPException(
            status_code=401,
            detail={"error": "mfa_required", "message": "Two-factor authentication required."},
        )
    session = _resolve_session(authorization, request=request)
    session["mode"] = "session"
    return session


def _issue_session_token(
    user_id: str,
    id_empresa: Optional[int],
    id_filial: Optional[int],
    *,
    response: Optional[Response] = None,
    request: Optional[Request] = None,
) -> dict[str, Any]:
    """Build the full session + final access token after a passed 2FA check."""
    session = repos_auth.get_session_context(
        user_id=user_id,
        id_empresa=id_empresa,
        id_filial=id_filial,
        include_default_scope=True,
    )
    payload = {
        "sub": session["sub"],
        "email": session.get("email"),
        "user_role": session.get("user_role"),
        "role": session.get("role"),
        "id_empresa": session.get("id_empresa"),
        "id_filial": session.get("id_filial"),
        "channel_id": session.get("channel_id"),
        "must_change_password": session.get("must_change_password", False),
    }
    token_minutes = 1440 if session.get("user_role") == "tenant_kiosk" else None
    token = create_access_token(payload, minutes=token_minutes, token_use=TOKEN_USE_ACCESS)
    result = {
        "access_token": token,
        "role": session.get("role"),
        "user_role": session.get("user_role"),
        "analytics_role": session.get("analytics_role"),
        "id_empresa": session.get("id_empresa"),
        "id_filial": session.get("id_filial"),
        "home_path": session.get("home_path"),
        "session": session,
    }
    if response is not None:
        csrf = attach_login_cookies(response, access_token=token, access_minutes=token_minutes)
        result["csrf_token"] = csrf
        if request is not None and is_browser_request(request) and bool(
            getattr(settings, "auth_omit_tokens_in_json_for_browser", True)
        ):
            result["access_token"] = None
    return result


# ── Verify (login step 2) ────────────────────────────────────

class MfaVerifyRequest(BaseModel):
    mfa_challenge_token: Optional[str] = Field(default=None, min_length=1, max_length=4096)
    code: str = Field(..., min_length=1, max_length=16)


@router.post("/verify")
def mfa_verify(body: MfaVerifyRequest, request: Request, response: Response):
    """Exchange a challenge token + TOTP code for a real access token."""
    challenge = extract_mfa_challenge_token(request, body.mfa_challenge_token)
    try:
        payload = decode_token(challenge)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_challenge", "message": "Desafio inválido ou expirado."})
    if classify_token_use(payload) != TOKEN_USE_MFA_CHALLENGE:
        raise HTTPException(status_code=401, detail={"error": "invalid_challenge", "message": "Desafio inválido."})

    user_id = str(payload.get("sub") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail={"error": "invalid_challenge", "message": "Desafio inválido."})

    outcome = repos_mfa.verify_totp_or_recovery(
        user_id,
        body.code,
        purpose="verify",
        require_enabled=True,
        allow_recovery=True,
    )
    _raise_mfa_outcome(outcome)

    try:
        result = _issue_session_token(
            user_id,
            payload.get("id_empresa"),
            payload.get("id_filial"),
            response=response,
            request=request,
        )
        clear_mfa_challenge_cookie(response)
        return result
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


# ── Setup (authenticated) ────────────────────────────────────

@router.post("/setup/start")
def mfa_setup_start(claims=Depends(setup_claims)):
    """Generate a new secret and return otpauth URI for QR provisioning."""
    if not is_totp_configured():
        raise HTTPException(status_code=503, detail={"error": "mfa_unavailable", "message": "2FA indisponível: chave de criptografia não configurada."})
    user_id = claims["sub"]
    account = claims.get("email") or claims.get("user_role") or user_id
    secret = generate_secret()
    repos_mfa.stage_secret(user_id, encrypt_secret(secret))
    otpauth = provisioning_uri(secret, str(account))
    return {
        "secret": secret,
        "otpauth_uri": otpauth,
        "qr_svg": qr_svg_data_uri(otpauth),
        "issuer": settings.totp_issuer,
        "account": account,
    }


class MfaConfirmRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=16)


@router.post("/setup/confirm")
def mfa_setup_confirm(
    body: MfaConfirmRequest,
    request: Request,
    response: Response,
    claims=Depends(setup_claims),
):
    """Validate the first code, enable 2FA, and return one-time recovery codes.

    When invoked through a forced-setup token (``totp_required``), also returns a
    full access token so the user lands logged in right after enrollment.
    """
    user_id = claims["sub"]
    outcome = repos_mfa.verify_totp_or_recovery(
        user_id,
        body.code,
        purpose="setup_confirm",
        require_enabled=False,
        allow_recovery=False,
    )
    if outcome.error == "mfa_not_enabled":
        raise HTTPException(status_code=400, detail={"error": "no_pending_secret", "message": "Inicie a configuração do 2FA primeiro."})
    _raise_mfa_outcome(outcome)

    if not repos_mfa.enable_after_confirm(user_id, totp_counter=outcome.totp_counter):
        raise HTTPException(
            status_code=409,
            detail={"error": "mfa_already_enabled", "message": "2FA já foi confirmado. Faça login novamente."},
        )

    codes = generate_recovery_codes()
    repos_mfa.replace_recovery_codes(user_id, [hash_recovery_code(c) for c in codes])
    result: dict[str, Any] = {"ok": True, "totp_enabled": True, "recovery_codes": codes}
    if claims.get("mode") == "setup":
        # Forced enrollment just completed → issue the final session token.
        try:
            result["login"] = _issue_session_token(
                user_id,
                claims.get("id_empresa"),
                claims.get("id_filial"),
                response=response,
                request=request,
            )
            clear_mfa_setup_cookie(response)
        except repos_auth.AuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    return result


class MfaDisableRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=16)


@router.post("/disable")
def mfa_disable(body: MfaDisableRequest, claims=Depends(get_current_claims)):
    """Disable the caller's own 2FA after validating a current code."""
    user_id = claims["sub"]
    outcome = repos_mfa.verify_totp_or_recovery(
        user_id,
        body.code,
        purpose="disable",
        require_enabled=True,
        allow_recovery=True,
    )
    _raise_mfa_outcome(outcome)
    if not repos_mfa.disable(user_id, clear_secret=True):
        raise HTTPException(status_code=400, detail={"error": "mfa_not_enabled", "message": "2FA não está ativo."})
    return {"ok": True, "totp_enabled": False}


@router.get("/status")
def mfa_status(claims=Depends(get_current_claims_allow_password_change)):
    """Return the caller's 2FA status (no secret).

    Usa allow_password_change: a tela de troca obrigatória de senha precisa
    saber se MFA é exigido antes de liberar o usuário.
    """
    state = repos_mfa.get_mfa_state(claims["sub"]) or {}
    snap = policy_snapshot(claims.get("user_role") or claims.get("role"), state)
    return {
        "totp_enabled": bool(state.get("totp_enabled")),
        "totp_required": bool(state.get("totp_required")),
        "mfa_reset_required": bool(state.get("mfa_reset_required")),
        "configured": is_totp_configured(),
        **snap,
    }
