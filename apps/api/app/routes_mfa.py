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
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from app import repos_auth, repos_mfa
from app.config import settings
from app.deps import _resolve_session, get_current_claims
from app.security import create_access_token, decode_token
from app.totp import (
    decrypt_secret,
    encrypt_secret,
    generate_recovery_codes,
    generate_secret,
    hash_recovery_code,
    is_totp_configured,
    provisioning_uri,
    qr_svg_data_uri,
    verify_code,
)

logger = logging.getLogger("torqmind.mfa")

router = APIRouter(prefix="/auth/mfa", tags=["auth-mfa"])

# Best-effort in-process attempt limiter for the 6-digit challenge. Combined with
# the 5-minute challenge TTL and the 1e6 code space this throttles brute force.
_attempts: dict[str, tuple[int, float]] = {}


def _too_many_attempts(user_id: str) -> bool:
    count, started = _attempts.get(user_id, (0, time.time()))
    window = settings.mfa_challenge_ttl_minutes * 60
    if time.time() - started > window:
        _attempts[user_id] = (0, time.time())
        return False
    return count >= settings.mfa_max_attempts


def _record_attempt(user_id: str) -> None:
    count, started = _attempts.get(user_id, (0, time.time()))
    if time.time() - started > settings.mfa_challenge_ttl_minutes * 60:
        count, started = 0, time.time()
    _attempts[user_id] = (count + 1, started)


def _clear_attempts(user_id: str) -> None:
    _attempts.pop(user_id, None)


def issue_mfa_challenge_token(user_id: str, id_empresa: Optional[int], id_filial: Optional[int]) -> str:
    """Short-lived token proving the password step passed; 2FA still pending."""
    payload = {
        "sub": user_id,
        "scope": "mfa_challenge",
        "mfa_pending": True,
        "id_empresa": id_empresa,
        "id_filial": id_filial,
    }
    return create_access_token(payload, minutes=settings.mfa_challenge_ttl_minutes)


def issue_mfa_setup_token(user_id: str, id_empresa: Optional[int], id_filial: Optional[int]) -> str:
    """Short-lived token for FORCED 2FA setup (totp_required, not yet enabled).

    Authorizes only the setup endpoints (start/confirm); ``mfa_pending=True`` so
    it is rejected as a normal bearer everywhere else.
    """
    payload = {
        "sub": user_id,
        "scope": "mfa_setup",
        "mfa_pending": True,
        "id_empresa": id_empresa,
        "id_filial": id_filial,
    }
    return create_access_token(payload, minutes=settings.mfa_challenge_ttl_minutes)


def setup_claims(authorization: Optional[str] = Header(default=None)) -> dict[str, Any]:
    """Authorize 2FA setup via either a full session OR an mfa_setup token.

    The mfa_setup path lets a ``totp_required`` user complete the mandatory
    enrollment right after the password step, without a full session.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"error": "missing_bearer", "message": "Missing bearer token"})
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})
    if payload.get("scope") == "mfa_setup":
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
    session = _resolve_session(authorization)
    session["mode"] = "session"
    return session


def _issue_session_token(user_id: str, id_empresa: Optional[int], id_filial: Optional[int]) -> dict[str, Any]:
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
    token = create_access_token(payload, minutes=token_minutes)
    return {
        "access_token": token,
        "role": session.get("role"),
        "user_role": session.get("user_role"),
        "analytics_role": session.get("analytics_role"),
        "id_empresa": session.get("id_empresa"),
        "id_filial": session.get("id_filial"),
        "home_path": session.get("home_path"),
        "session": session,
    }


# ── Verify (login step 2) ────────────────────────────────────

class MfaVerifyRequest(BaseModel):
    mfa_challenge_token: str = Field(..., min_length=1)
    code: str = Field(..., min_length=1, max_length=16)


@router.post("/verify")
def mfa_verify(body: MfaVerifyRequest):
    """Exchange a challenge token + TOTP code for a real access token."""
    try:
        payload = decode_token(body.mfa_challenge_token)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_challenge", "message": "Desafio inválido ou expirado."})
    if payload.get("scope") != "mfa_challenge" or not payload.get("mfa_pending"):
        raise HTTPException(status_code=401, detail={"error": "invalid_challenge", "message": "Desafio inválido."})

    user_id = str(payload.get("sub") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail={"error": "invalid_challenge", "message": "Desafio inválido."})

    if _too_many_attempts(user_id):
        raise HTTPException(status_code=429, detail={"error": "too_many_attempts", "message": "Muitas tentativas. Faça login novamente."})

    enc = repos_mfa.get_encrypted_secret(user_id, require_enabled=True)
    if not enc:
        raise HTTPException(status_code=400, detail={"error": "mfa_not_enabled", "message": "2FA não está ativo."})

    ok = False
    try:
        ok = verify_code(decrypt_secret(enc), body.code)
    except Exception:  # noqa: BLE001 — never leak crypto errors
        logger.warning("TOTP verify failed to decrypt/evaluate for user")
    if not ok:
        # Recovery code fallback (one-time).
        if repos_mfa.consume_recovery_code(user_id, hash_recovery_code(body.code)):
            ok = True
    if not ok:
        _record_attempt(user_id)
        raise HTTPException(status_code=401, detail={"error": "invalid_code", "message": "Código inválido."})

    _clear_attempts(user_id)
    repos_mfa.mark_used(user_id)
    try:
        return _issue_session_token(user_id, payload.get("id_empresa"), payload.get("id_filial"))
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
def mfa_setup_confirm(body: MfaConfirmRequest, claims=Depends(setup_claims)):
    """Validate the first code, enable 2FA, and return one-time recovery codes.

    When invoked through a forced-setup token (``totp_required``), also returns a
    full access token so the user lands logged in right after enrollment.
    """
    user_id = claims["sub"]
    enc = repos_mfa.get_encrypted_secret(user_id, require_enabled=False)
    if not enc:
        raise HTTPException(status_code=400, detail={"error": "no_pending_secret", "message": "Inicie a configuração do 2FA primeiro."})
    try:
        ok = verify_code(decrypt_secret(enc), body.code)
    except Exception:  # noqa: BLE001
        ok = False
    if not ok:
        raise HTTPException(status_code=401, detail={"error": "invalid_code", "message": "Código inválido. Tente novamente."})

    repos_mfa.enable_after_confirm(user_id)
    codes = generate_recovery_codes()
    repos_mfa.replace_recovery_codes(user_id, [hash_recovery_code(c) for c in codes])
    result: dict[str, Any] = {"ok": True, "totp_enabled": True, "recovery_codes": codes}
    if claims.get("mode") == "setup":
        # Forced enrollment just completed → issue the final session token.
        try:
            result["login"] = _issue_session_token(user_id, claims.get("id_empresa"), claims.get("id_filial"))
        except repos_auth.AuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    return result


class MfaDisableRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=16)


@router.post("/disable")
def mfa_disable(body: MfaDisableRequest, claims=Depends(get_current_claims)):
    """Disable the caller's own 2FA after validating a current code."""
    user_id = claims["sub"]
    enc = repos_mfa.get_encrypted_secret(user_id, require_enabled=True)
    if not enc:
        raise HTTPException(status_code=400, detail={"error": "mfa_not_enabled", "message": "2FA não está ativo."})
    ok = False
    try:
        ok = verify_code(decrypt_secret(enc), body.code)
    except Exception:  # noqa: BLE001
        ok = False
    if not ok and repos_mfa.consume_recovery_code(user_id, hash_recovery_code(body.code)):
        ok = True
    if not ok:
        raise HTTPException(status_code=401, detail={"error": "invalid_code", "message": "Código inválido."})
    repos_mfa.disable(user_id, clear_secret=True)
    return {"ok": True, "totp_enabled": False}


@router.get("/status")
def mfa_status(claims=Depends(get_current_claims)):
    """Return the caller's 2FA status (no secret)."""
    state = repos_mfa.get_mfa_state(claims["sub"]) or {}
    return {
        "totp_enabled": bool(state.get("totp_enabled")),
        "totp_required": bool(state.get("totp_required")),
        "mfa_reset_required": bool(state.get("mfa_reset_required")),
        "configured": is_totp_configured(),
    }
