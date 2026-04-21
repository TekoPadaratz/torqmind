from __future__ import annotations

from typing import Any, Optional

from fastapi import Header, HTTPException

from app import repos_auth
from app.security import decode_token


def get_current_claims(authorization: Optional[str] = Header(default=None)) -> dict[str, Any]:
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
        )
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
