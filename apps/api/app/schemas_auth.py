from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class LoginRequest(BaseModel):
    email: str
    password: str
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    user_role: str
    analytics_role: Optional[str] = None
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None
    home_path: str
