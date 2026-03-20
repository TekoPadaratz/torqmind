from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException

from app import repos_auth
from app.schemas_auth import LoginRequest, LoginResponse
from app.security import create_access_token, decode_token

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest):
    try:
        session = repos_auth.verify_login(
            body.email,
            body.password,
            id_empresa=body.id_empresa,
            id_filial=body.id_filial,
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
