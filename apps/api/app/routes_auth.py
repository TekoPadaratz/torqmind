from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from app import repos_auth
from app.deps import get_current_claims, get_current_claims_allow_password_change
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
    token = create_access_token(payload)
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


@router.post("/change-password")
def change_password(body: ChangePasswordRequest, claims=Depends(get_current_claims_allow_password_change)):
    """
    Change user password. Validates current password, updates hash,
    clears must_change_password flag, sets password_changed_at.
    Returns a fresh access token with must_change_password=False.
    """
    user_id = claims["sub"]

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

    # Issue fresh token with must_change_password=False
    new_payload = {**claims, "must_change_password": False}
    for k in ("exp", "iat", "nbf"):
        new_payload.pop(k, None)
    token = create_access_token(new_payload)

    return {"ok": True, "access_token": token}
