"""Recovery overlay: dual password verify (bcrypt + Argon2id) with OLD token contract.

Used only by ``Dockerfile.api-rollback-dual-verify``.

Preserves the pre-hardening ``create_access_token(payload, minutes=None)`` signature
and JWT shape (``mfa_pending`` / ``scope`` markers must survive). Do NOT replace this
with the post-hardening ``security.py`` that defaults ``token_use=access`` and strips
intermediate markers — that turns MFA challenge tokens into full access tokens.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from jose import jwt
from passlib.context import CryptContext

from app.config import settings


def _argon2_available() -> bool:
    try:
        import argon2  # noqa: F401

        return True
    except ImportError:
        return False


def _build_pwd_context() -> CryptContext:
    if _argon2_available():
        return CryptContext(
            schemes=["argon2", "bcrypt"],
            default="argon2",
            deprecated="auto",
            argon2__type="ID",
            argon2__time_cost=2,
            argon2__memory_cost=65536,
            argon2__parallelism=2,
        )
    return CryptContext(schemes=["bcrypt"], deprecated="auto")


pwd_context = _build_pwd_context()


def identify_password_scheme(password_hash: str) -> str:
    h = password_hash or ""
    if h.startswith("$argon2"):
        return "argon2"
    if h.startswith(("$2a$", "$2b$", "$2y$")):
        return "bcrypt"
    return "unknown"


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bool(pwd_context.verify(password, password_hash))
    except Exception:
        return False


def create_access_token(payload: Dict[str, Any], minutes: Optional[int] = None) -> str:
    """Pre-hardening contract: forward payload markers (mfa_pending/scope) unchanged."""
    issued_at = datetime.now(timezone.utc)
    exp = issued_at + timedelta(minutes=minutes or settings.api_access_token_minutes)
    to_encode = {**payload, "iss": settings.api_jwt_issuer, "iat": issued_at, "exp": exp}
    return jwt.encode(to_encode, settings.api_jwt_secret, algorithm="HS256")


def decode_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, settings.api_jwt_secret, algorithms=["HS256"], issuer=settings.api_jwt_issuer)
