from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field

from app import repos_auth
from app.config import settings
from app.deps import get_current_claims, get_current_claims_allow_password_change
from app.email_service import send_password_reset_email
from app.password_policy import policy_message, validate_password
from app.schemas_auth import LoginRequest, LoginResponse
from app.security import create_access_token, decode_token, hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest):
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

    # Two-factor: if the user has TOTP enabled, do NOT issue a final token yet.
    # Return a short-lived challenge; the client completes via /auth/mfa/verify.
    from app import repos_mfa
    from app.routes_mfa import issue_mfa_challenge_token

    mfa_state = repos_mfa.get_mfa_state(session["sub"]) or {}
    if mfa_state.get("totp_enabled"):
        challenge = issue_mfa_challenge_token(
            session["sub"], session.get("id_empresa"), session.get("id_filial")
        )
        return LoginResponse(mfa_required=True, mfa_challenge_token=challenge)

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
    token = create_access_token(payload, minutes=token_minutes)
    return LoginResponse(
        access_token=token,
        role=session["role"],
        user_role=session["user_role"],
        analytics_role=session.get("analytics_role"),
        id_empresa=session.get("id_empresa"),
        id_filial=session.get("id_filial"),
        home_path=session["home_path"],
        session=session,
    )


@router.get("/me")
def me(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"error": "missing_bearer", "message": "Missing bearer token"})

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})

    user_id = str(payload.get("sub") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail={"error": "invalid_token", "message": "Invalid token"})

    try:
        return repos_auth.get_session_context(
            user_id=user_id,
            id_empresa=payload.get("id_empresa"),
            id_filial=payload.get("id_filial"),
            channel_id=payload.get("channel_id"),
            include_default_scope=True,
        )
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


# ── Change password ──────────────────────────────────────────

class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)
    totp_code: str | None = Field(default=None, max_length=16)


@router.post("/change-password")
def change_password(body: ChangePasswordRequest, claims=Depends(get_current_claims_allow_password_change)):
    """
    Change user password. Validates current password, updates hash,
    clears must_change_password flag, sets password_changed_at.
    Returns a fresh access token with must_change_password=False.
    """
    user_id = claims["sub"]

    # If the user has 2FA enabled, require a valid TOTP (or recovery) code.
    from app import repos_mfa
    from app.totp import decrypt_secret, hash_recovery_code, verify_code

    enc = repos_mfa.get_encrypted_secret(user_id, require_enabled=True)
    if enc:
        code = (body.totp_code or "").strip()
        ok = False
        if code:
            try:
                ok = verify_code(decrypt_secret(enc), code)
            except Exception:  # noqa: BLE001
                ok = False
            if not ok and repos_mfa.consume_recovery_code(user_id, hash_recovery_code(code)):
                ok = True
        if not ok:
            raise HTTPException(status_code=401, detail={"error": "mfa_required", "message": "Código do autenticador é obrigatório."})

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

    # Issue fresh token — minimal payload (session context is not JWT-safe)
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
    token = create_access_token(new_payload)

    return {"ok": True, "access_token": token}


@router.post("/refresh")
def refresh_token(claims=Depends(get_current_claims)):
    """Reissue a fresh access token based on current valid session.

    Kiosk sessions get 24h tokens; other roles get the default TTL.
    Called periodically by the frontend to keep sessions alive.
    """
    user_role = claims.get("user_role") or ""
    new_payload = {
        "sub": claims["sub"],
        "email": claims.get("email"),
        "user_role": user_role,
        "role": claims.get("role"),
        "id_empresa": claims.get("id_empresa"),
        "id_filial": claims.get("id_filial"),
        "channel_id": claims.get("channel_id"),
        "must_change_password": False,
    }
    token_minutes = 1440 if user_role == "tenant_kiosk" else None
    token = create_access_token(new_payload, minutes=token_minutes)
    return {"ok": True, "access_token": token}


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
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip() or None
    return request.client.host if request.client else None


class ForgotPasswordRequest(BaseModel):
    identifier: str = Field(..., min_length=1, max_length=320)


@router.post("/forgot-password")
def forgot_password(body: ForgotPasswordRequest, request: Request):
    """Start a password reset. Always returns a generic success (anti-enumeration)."""
    generic = {"ok": True, "message": _GENERIC_FORGOT_MESSAGE}

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
    from app.totp import decrypt_secret, hash_recovery_code, verify_code

    enc = repos_mfa.get_encrypted_secret(str(user["id"]), require_enabled=True)
    if enc:
        code = (body.totp_code or "").strip()
        ok = False
        if code:
            try:
                ok = verify_code(decrypt_secret(enc), code)
            except Exception:  # noqa: BLE001
                ok = False
            if not ok and repos_mfa.consume_recovery_code(str(user["id"]), hash_recovery_code(code)):
                ok = True
        if not ok:
            raise HTTPException(status_code=401, detail={"error": "mfa_required", "message": "Código do autenticador é obrigatório para concluir a redefinição."})

    new_hash = hash_password(body.new_password)
    user_id = repos_auth.reset_password_with_token(body.token, new_hash)
    if not user_id:
        # Token consumido/expirado entre a validação e o commit.
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_token", "message": "Link inválido ou expirado. Solicite um novo."},
        )

    return {"ok": True, "message": "Senha redefinida com sucesso. Você já pode entrar com a nova senha."}
