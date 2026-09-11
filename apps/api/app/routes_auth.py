from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app import repos_auth
from app.config import settings
from app.deps import get_current_claims, get_current_claims_allow_password_change, resolve_access_session
from app.email_service import send_password_reset_email
from app.password_policy import policy_message, validate_password
from app.schemas_auth import LoginRequest, LoginResponse
from app.security import (
    TOKEN_USE_ACCESS,
    create_access_token,
    hash_password,
    resolve_session_exp,
    verify_password,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def _maybe_omit_tokens(request: Request, payload: dict) -> dict:
    from app.session_cookies import is_browser_request

    out = dict(payload)
    if is_browser_request(request) and bool(getattr(settings, "auth_omit_tokens_in_json_for_browser", True)):
        out["access_token"] = None
        out["mfa_challenge_token"] = None
        out["mfa_setup_token"] = None
    return out


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest, request: Request, response: Response):
    from app import security_attempts
    from app.session_cookies import attach_login_cookies

    # Identity-scoped throttle (shared across workers via security_attempt_buckets).
    # Does not depend on X-Forwarded-For.
    ident_key = security_attempts.bucket_key(
        "auth", "login", "id", security_attempts.identity_hash(body.identifier)
    )
    id_max = int(settings.auth_login_identity_max_per_window)
    id_window = int(settings.auth_login_identity_window_seconds)
    if str(settings.app_env or "").strip().lower() != "test":
        if security_attempts.is_rate_limited(ident_key, max_attempts=id_max, window_seconds=id_window):
            raise HTTPException(
                status_code=429,
                detail={"error": "rate_limited", "message": "Muitas tentativas de login. Aguarde 1 minuto."},
            )
        security_attempts.record_attempt(ident_key, window_seconds=id_window)

    try:
        session = repos_auth.verify_login(
            body.identifier,
            body.password,
            id_empresa=body.id_empresa,
            id_filial=body.id_filial,
            include_default_scope=True,
        )
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())

    if str(settings.app_env or "").strip().lower() != "test":
        security_attempts.clear_attempts(ident_key)

    # Two-factor: if the user has TOTP enabled, do NOT issue a final token yet.
    # Return a short-lived challenge; the client completes via /auth/mfa/verify.
    from app import repos_mfa
    from app.mfa_policy import effective_totp_required
    from app.routes_mfa import issue_mfa_challenge_token, issue_mfa_setup_token

    mfa_state = repos_mfa.get_mfa_state(session["sub"]) or {}
    if mfa_state.get("totp_enabled"):
        challenge = issue_mfa_challenge_token(
            session["sub"], session.get("id_empresa"), session.get("id_filial")
        )
        csrf = attach_login_cookies(response, mfa_challenge_token=challenge)
        return _maybe_omit_tokens(
            request,
            LoginResponse(
                mfa_required=True,
                mfa_challenge_token=challenge,
                csrf_token=csrf,
            ).model_dump(),
        )

    # Enforced enrollment: column totp_required and/or privileged policy flag
    # (default OFF — does not block existing admins until explicitly enabled).
    if effective_totp_required(mfa_state, user_role=session.get("user_role")):
        setup_token = issue_mfa_setup_token(
            session["sub"], session.get("id_empresa"), session.get("id_filial")
        )
        csrf = attach_login_cookies(response, mfa_setup_token=setup_token)
        return _maybe_omit_tokens(
            request,
            LoginResponse(
                mfa_setup_required=True,
                mfa_setup_token=setup_token,
                csrf_token=csrf,
            ).model_dump(),
        )

    payload = {
        "sub": session["sub"],
        "email": session["email"],
        "user_role": session["user_role"],
        "role": session["role"],
        "id_empresa": session.get("id_empresa"),
        "id_filial": session.get("id_filial"),
        "channel_id": session.get("channel_id"),
        "must_change_password": session.get("must_change_password", False),
    }
    # Kiosk sessions last 24h
    token_minutes = 1440 if session.get("user_role") == "tenant_kiosk" else None
    token = create_access_token(payload, minutes=token_minutes, token_use=TOKEN_USE_ACCESS)
    csrf = attach_login_cookies(response, access_token=token, access_minutes=token_minutes)
    return _maybe_omit_tokens(
        request,
        LoginResponse(
            access_token=token,
            role=session["role"],
            user_role=session["user_role"],
            analytics_role=session.get("analytics_role"),
            id_empresa=session.get("id_empresa"),
            id_filial=session.get("id_filial"),
            home_path=session["home_path"],
            session=session,
            csrf_token=csrf,
        ).model_dump(),
    )


@router.get("/me")
def me(request: Request, authorization: str | None = Header(default=None)):
    # Same access-token gates as BI dependencies (rejects MFA intermediate tokens,
    # absolute expiry and password-change revocation). Cookie-first for browsers.
    # default_scope is required here for FE home/scope bootstrap — not on every BI GET.
    session, _payload = resolve_access_session(
        authorization,
        request=request,
        include_default_scope=True,
    )
    return session


@router.post("/logout")
def logout(response: Response):
    """Clear HttpOnly session cookies (revokes browser session material)."""
    from app.session_cookies import clear_session_cookies

    clear_session_cookies(response)
    return {"ok": True}


# ── Change password ──────────────────────────────────────────

class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)
    totp_code: str | None = Field(default=None, max_length=16)


@router.post("/change-password")
def change_password(
    body: ChangePasswordRequest,
    request: Request,
    response: Response,
    claims=Depends(get_current_claims_allow_password_change),
):
    """
    Change user password. Validates current password, updates hash,
    clears must_change_password flag, sets password_changed_at.
    Returns a fresh access token with must_change_password=False.
    """
    user_id = claims["sub"]

    # If the user has 2FA enabled, require a valid TOTP (or recovery) code.
    from app import repos_mfa
    from app.routes_mfa import _raise_mfa_outcome

    if repos_mfa.get_encrypted_secret(user_id, require_enabled=True):
        outcome = repos_mfa.verify_totp_or_recovery(
            user_id,
            body.totp_code or "",
            purpose="change_password",
            require_enabled=True,
            allow_recovery=True,
        )
        if not outcome.ok and outcome.error == "mfa_not_enabled":
            raise HTTPException(status_code=401, detail={"error": "mfa_required", "message": "Código do autenticador é obrigatório."})
        if not outcome.ok and outcome.error == "invalid_code" and not (body.totp_code or "").strip():
            raise HTTPException(status_code=401, detail={"error": "mfa_required", "message": "Código do autenticador é obrigatório."})
        _raise_mfa_outcome(outcome)

    from app.db import get_conn

    with get_conn() as conn:
        row = conn.execute(
            "SELECT password_hash FROM auth.users WHERE id = %s::uuid",
            (user_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail={"error": "user_not_found", "message": "User not found"})

        if not verify_password(body.current_password, row["password_hash"]):
            raise HTTPException(status_code=400, detail={"error": "wrong_password", "message": "Current password is incorrect"})

        if body.current_password == body.new_password:
            raise HTTPException(status_code=400, detail={"error": "same_password", "message": "New password must differ from current"})

        policy_errors = validate_password(body.new_password)
        if policy_errors:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "weak_password",
                    "message": policy_message(),
                    "unmet_rules": policy_errors,
                },
            )

        new_hash = hash_password(body.new_password)
        conn.execute(
            """
            UPDATE auth.users
            SET password_hash = %s,
                must_change_password = FALSE,
                password_changed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (new_hash, user_id),
        )
        conn.commit()

    # Issue fresh token — minimal payload (session context is not JWT-safe).
    # Absolute session_exp starts anew after an authenticated password change.
    new_payload = {
        "sub": claims["sub"],
        "email": claims.get("email"),
        "user_role": claims.get("user_role"),
        "role": claims.get("role"),
        "id_empresa": claims.get("id_empresa"),
        "id_filial": claims.get("id_filial"),
        "channel_id": claims.get("channel_id"),
        "must_change_password": False,
    }
    token = create_access_token(new_payload, token_use=TOKEN_USE_ACCESS)
    from app.session_cookies import attach_login_cookies

    csrf = attach_login_cookies(response, access_token=token)
    body_out = {"ok": True, "access_token": token, "csrf_token": csrf}
    return _maybe_omit_tokens(request, body_out)


@router.post("/refresh")
def refresh_token(
    request: Request,
    response: Response,
    authorization: str | None = Header(default=None),
):
    """Reissue a fresh access token based on current valid session.

    Preserves absolute ``session_exp`` so refresh cannot extend the wall-clock
    session indefinitely. Kiosk sessions keep the 24h relative TTL while still
    capped by absolute expiry. Reloads permissions/state from the database.
    """
    # Refresh re-bootstraps default_scope for the FE (same as /auth/me).
    session, payload = resolve_access_session(
        authorization,
        request=request,
        include_default_scope=True,
    )
    if session.get("must_change_password"):
        raise HTTPException(
            status_code=403,
            detail={
                "error": "password_change_required",
                "message": "You must change your password before accessing this resource.",
            },
        )

    user_role = session.get("user_role") or ""
    new_payload = {
        "sub": session["sub"],
        "email": session.get("email"),
        "user_role": user_role,
        "role": session.get("role"),
        "id_empresa": session.get("id_empresa"),
        "id_filial": session.get("id_filial"),
        "channel_id": session.get("channel_id"),
        "must_change_password": bool(session.get("must_change_password")),
    }
    token_minutes = 1440 if user_role == "tenant_kiosk" else None
    try:
        token = create_access_token(
            new_payload,
            minutes=token_minutes,
            token_use=TOKEN_USE_ACCESS,
            session_exp=resolve_session_exp(payload, user_role=user_role, relative_minutes=token_minutes),
        )
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail={"error": "session_expired", "message": "Sessão expirada. Faça login novamente."},
        )
    from app.session_cookies import attach_login_cookies

    csrf = attach_login_cookies(response, access_token=token, access_minutes=token_minutes)
    return _maybe_omit_tokens(request, {"ok": True, "access_token": token, "csrf_token": csrf})


# ── Password reset ("esqueci minha senha") ──────────────────
# Fluxo seguro: o usuário informa um identificador (e-mail ou username); se ele
# existir e estiver ativo, geramos um token aleatório de alta entropia, guardamos
# apenas o hash, e enviamos por e-mail um link que carrega SOMENTE o token (sem
# e-mail embutido). A resposta é sempre genérica para evitar enumeração de contas.

_GENERIC_FORGOT_MESSAGE = (
    "Se houver uma conta para este e-mail, enviaremos um link de recuperação em instantes."
)


def _build_reset_url(raw_token: str) -> str:
    base = (settings.web_public_url or "").rstrip("/")
    path = settings.password_reset_link_path or "/reset-password"
    if not path.startswith("/"):
        path = "/" + path
    from urllib.parse import quote

    return f"{base}{path}?token={quote(raw_token, safe='')}"


def _client_ip(request: Request) -> str | None:
    """Best-effort peer address for reset audit — see app.client_ip trust model."""
    from app.client_ip import client_ip_for_rate_limit

    return client_ip_for_rate_limit(request)


class ForgotPasswordRequest(BaseModel):
    identifier: str = Field(..., min_length=1, max_length=320)


@router.post("/forgot-password")
def forgot_password(body: ForgotPasswordRequest, request: Request):
    """Start a password reset. Always returns a generic success (anti-enumeration)."""
    from app import security_attempts

    generic = {"ok": True, "message": _GENERIC_FORGOT_MESSAGE}

    if str(settings.app_env or "").strip().lower() != "test":
        ident_key = security_attempts.bucket_key(
            "auth", "mail", "id", security_attempts.identity_hash(body.identifier)
        )
        max_n = int(settings.auth_mail_identity_max_per_window)
        window = int(settings.auth_mail_identity_window_seconds)
        if security_attempts.is_rate_limited(ident_key, max_attempts=max_n, window_seconds=window):
            # Same generic body to avoid account enumeration via 429 timing alone is hard;
            # still return 429 so operators see abuse without revealing existence.
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "rate_limited",
                    "message": "Muitas tentativas de recuperação de senha. Aguarde 1 minuto.",
                },
            )
        security_attempts.record_attempt(ident_key, window_seconds=window)

    user = repos_auth.get_user_by_identifier(body.identifier)
    if not user or not user.get("is_active") or not user.get("email"):
        return generic

    raw_token = repos_auth.create_password_reset_token(
        user_id=str(user["id"]),
        ttl_minutes=settings.password_reset_token_ttl_minutes,
        requested_ip=_client_ip(request),
        requested_user_agent=(request.headers.get("user-agent") or "")[:512] or None,
    )
    reset_url = _build_reset_url(raw_token)
    send_password_reset_email(
        to_email=user["email"],
        reset_url=reset_url,
        nome=user.get("nome"),
        ttl_minutes=settings.password_reset_token_ttl_minutes,
    )
    return generic


@router.get("/reset-password/validate")
def validate_reset_token(token: str):
    """Validate a reset token and return the associated email for display.

    The token alone authorizes this lookup (it proves inbox possession), so the
    email is never passed in the URL by the client.
    """
    user = repos_auth.get_reset_token_user(token)
    if not user or not user.get("is_active"):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_token", "message": "Link inválido ou expirado. Solicite um novo."},
        )
    from app import repos_mfa

    mfa_state = repos_mfa.get_mfa_state(str(user["id"])) or {}
    return {
        "valid": True,
        "email": user["email"],
        "rules_message": policy_message(),
        "mfa_required": bool(mfa_state.get("totp_enabled")),
    }


class ResetPasswordRequest(BaseModel):
    token: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=1, max_length=128)
    totp_code: str | None = Field(default=None, max_length=16)


@router.post("/reset-password")
def reset_password(body: ResetPasswordRequest):
    """Complete a password reset using a valid token."""
    user = repos_auth.get_reset_token_user(body.token)
    if not user or not user.get("is_active"):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_token", "message": "Link inválido ou expirado. Solicite um novo."},
        )

    policy_errors = validate_password(body.new_password)
    if policy_errors:
        raise HTTPException(
            status_code=422,
            detail={"error": "weak_password", "message": policy_message(), "unmet_rules": policy_errors},
        )

    # If the user has 2FA enabled, a valid TOTP (or recovery) code is required to
    # complete the reset — a leaked reset link alone must not bypass 2FA.
    from app import repos_mfa
    from app.routes_mfa import _raise_mfa_outcome

    uid = str(user["id"])
    if repos_mfa.get_encrypted_secret(uid, require_enabled=True):
        outcome = repos_mfa.verify_totp_or_recovery(
            uid,
            body.totp_code or "",
            purpose="reset_password",
            require_enabled=True,
            allow_recovery=True,
        )
        if not outcome.ok and (
            outcome.error in {"mfa_not_enabled", "invalid_code"} and not (body.totp_code or "").strip()
        ):
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "mfa_required",
                    "message": "Código do autenticador é obrigatório para concluir a redefinição.",
                },
            )
        _raise_mfa_outcome(outcome)

    new_hash = hash_password(body.new_password)
    user_id = repos_auth.reset_password_with_token(body.token, new_hash)
    if not user_id:
        # Token consumido/expirado entre a validação e o commit.
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_token", "message": "Link inválido ou expirado. Solicite um novo."},
        )

    return {"ok": True, "message": "Senha redefinida com sucesso. Você já pode entrar com a nova senha."}
