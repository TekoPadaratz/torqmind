from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from app.usernames import normalize_login_identifier


class LoginRequest(BaseModel):
    identifier: str = Field(min_length=1)
    password: str
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def accept_legacy_email_field(cls, data):
        if isinstance(data, dict) and data.get("identifier") is None and data.get("email") is not None:
            payload = dict(data)
            payload["identifier"] = payload.get("email")
            return payload
        return data

    @field_validator("identifier")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        normalized = normalize_login_identifier(value)
        if not normalized:
            raise ValueError("Identificador de login é obrigatório.")
        return normalized


class LoginResponse(BaseModel):
    access_token: Optional[str] = None
    token_type: str = "bearer"
    role: Optional[str] = None
    user_role: Optional[str] = None
    analytics_role: Optional[str] = None
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None
    home_path: Optional[str] = None
    session: Optional[Dict[str, Any]] = None
    # Two-factor (TOTP): when the user has 2FA enabled, the password step returns
    # a short-lived challenge instead of a final access token.
    mfa_required: bool = False
    mfa_challenge_token: Optional[str] = None
