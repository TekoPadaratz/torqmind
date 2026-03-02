from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends

from app.deps import get_current_claims
from app.schemas import LoginRequest, LoginResponse
from app.security import create_access_token
from app import repos_auth

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest):
    try:
        user, scope = repos_auth.verify_login(body.email, body.password)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    payload = {
        "sub": str(user["id"]),
        "email": user["email"],
        "role": scope["role"],
        "id_empresa": scope["id_empresa"],
        "id_filial": scope["id_filial"],
    }
    token = create_access_token(payload)
    return LoginResponse(
        access_token=token,
        role=scope["role"],
        id_empresa=scope["id_empresa"],
        id_filial=scope["id_filial"],
    )


@router.get("/me")
def me(claims=Depends(get_current_claims)):
    """Return current JWT claims (for UI)."""

    return claims
