from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from jose import jwt
from passlib.context import CryptContext
from app.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)

def create_access_token(payload: Dict[str, Any], minutes: Optional[int]=None) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=minutes or settings.api_access_token_minutes)
    to_encode = {**payload, "iss": settings.api_jwt_issuer, "exp": exp}
    return jwt.encode(to_encode, settings.api_jwt_secret, algorithm="HS256")

def decode_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, settings.api_jwt_secret, algorithms=["HS256"], issuer=settings.api_jwt_issuer)
