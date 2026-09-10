from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from jose import jwt
from passlib.context import CryptContext

from app.config import settings

BCRYPT_MAX_PASSWORD_BYTES = 72


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
            argon2__time_cost=int(getattr(settings, "argon2_time_cost", 2) or 2),
            argon2__memory_cost=int(getattr(settings, "argon2_memory_kib", 65536) or 65536),
            argon2__parallelism=int(getattr(settings, "argon2_parallelism", 2) or 2),
        )
    return CryptContext(schemes=["bcrypt"], deprecated="auto")


pwd_context = _build_pwd_context()

# Explicit token classes. Intermediate MFA tokens must never authorize /me or BI.
TOKEN_USE_ACCESS = "access"
TOKEN_USE_MFA_CHALLENGE = "mfa_challenge"
TOKEN_USE_MFA_SETUP = "mfa_setup"

_SCOPE_TO_TOKEN_USE = {
    "mfa_challenge": TOKEN_USE_MFA_CHALLENGE,
    "mfa_setup": TOKEN_USE_MFA_SETUP,
}


@dataclass(frozen=True)
class PasswordCheck:
    ok: bool
    scheme: str = ""
    ambiguous_bcrypt_long: bool = False
    upgradeable_to_argon2: bool = False


def password_byte_length(password: str) -> int:
    return len((password or "").encode("utf-8"))


def identify_password_scheme(password_hash: str) -> str:
    h = password_hash or ""
    if h.startswith("$argon2"):
        return "argon2"
    if h.startswith(("$2a$", "$2b$", "$2y$")):
        return "bcrypt"
    return "unknown"


def hash_password(password: str) -> str:
    """Hash a new password with Argon2id when available; never truncate bcrypt silently."""
    raw_len = password_byte_length(password)
    if not _argon2_available() and raw_len > BCRYPT_MAX_PASSWORD_BYTES:
        raise RuntimeError(
            "argon2_cffi is required to hash passwords longer than 72 UTF-8 bytes"
        )
    return pwd_context.hash(password)


def verify_password_detailed(password: str, password_hash: str) -> PasswordCheck:
    """Verify legacy bcrypt and Argon2id hashes with bcrypt long-password awareness.

    bcrypt only uses the first 72 UTF-8 bytes. When a longer password verifies,
    the result is cryptographically ambiguous (distinct suffixes collide). Callers
    must NOT silently rehash those; force an authenticated password change instead.
    """
    scheme = identify_password_scheme(password_hash)
    try:
        ok = bool(pwd_context.verify(password, password_hash))
    except Exception:
        return PasswordCheck(ok=False, scheme=scheme)
    if not ok:
        return PasswordCheck(ok=False, scheme=scheme)

    raw_len = password_byte_length(password)
    ambiguous = scheme == "bcrypt" and raw_len > BCRYPT_MAX_PASSWORD_BYTES
    upgradeable = (
        scheme == "bcrypt"
        and not ambiguous
        and _argon2_available()
    )
    return PasswordCheck(
        ok=True,
        scheme=scheme,
        ambiguous_bcrypt_long=ambiguous,
        upgradeable_to_argon2=upgradeable,
    )


def verify_password(password: str, password_hash: str) -> bool:
    return verify_password_detailed(password, password_hash).ok


def _as_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    return None


def classify_token_use(payload: Dict[str, Any]) -> str:
    """Return the token class for a decoded JWT payload."""
    explicit = str(payload.get("token_use") or "").strip()
    if explicit in {TOKEN_USE_ACCESS, TOKEN_USE_MFA_CHALLENGE, TOKEN_USE_MFA_SETUP}:
        return explicit
    scope = str(payload.get("scope") or "").strip()
    if scope in _SCOPE_TO_TOKEN_USE:
        return _SCOPE_TO_TOKEN_USE[scope]
    if payload.get("mfa_pending"):
        # Legacy intermediate tokens without scope.
        return TOKEN_USE_MFA_CHALLENGE
    return TOKEN_USE_ACCESS


def is_intermediate_mfa_token(payload: Dict[str, Any]) -> bool:
    return classify_token_use(payload) in {TOKEN_USE_MFA_CHALLENGE, TOKEN_USE_MFA_SETUP}


def absolute_session_minutes(*, user_role: Optional[str] = None, relative_minutes: Optional[int] = None) -> int:
    """Absolute session lifetime preserved across refresh.

    Kiosk relative TTL is 24h — absolute must not be shorter.
    """
    configured = max(1, int(getattr(settings, "api_session_absolute_minutes", 1440) or 1440))
    relative = int(relative_minutes or settings.api_access_token_minutes or 480)
    if (user_role or "") == "tenant_kiosk":
        relative = max(relative, 1440)
    return max(configured, relative)


def resolve_session_exp(
    payload: Dict[str, Any],
    *,
    issued_at: Optional[datetime] = None,
    user_role: Optional[str] = None,
    relative_minutes: Optional[int] = None,
) -> datetime:
    """Absolute expiry: prefer claim; else derive from iat (legacy tokens)."""
    existing = _as_utc(payload.get("session_exp"))
    if existing is not None:
        return existing
    base = issued_at or _as_utc(payload.get("iat")) or datetime.now(timezone.utc)
    minutes = absolute_session_minutes(user_role=user_role or payload.get("user_role"), relative_minutes=relative_minutes)
    return base + timedelta(minutes=minutes)


def create_access_token(
    payload: Dict[str, Any],
    minutes: Optional[int] = None,
    *,
    token_use: str = TOKEN_USE_ACCESS,
    session_exp: Optional[datetime] = None,
) -> str:
    issued_at = datetime.now(timezone.utc)
    relative = minutes if minutes is not None else settings.api_access_token_minutes
    exp = issued_at + timedelta(minutes=relative)
    to_encode = {
        **payload,
        "iss": settings.api_jwt_issuer,
        "iat": issued_at,
        "exp": exp,
        "token_use": token_use,
    }
    if token_use == TOKEN_USE_ACCESS:
        abs_exp = _as_utc(session_exp) or resolve_session_exp(
            payload,
            issued_at=issued_at,
            user_role=payload.get("user_role"),
            relative_minutes=relative,
        )
        # Never issue an access token that already outlives the absolute cap.
        if abs_exp <= issued_at:
            raise ValueError("session_expired")
        to_encode["session_exp"] = int(abs_exp.timestamp())
        if exp > abs_exp:
            to_encode["exp"] = abs_exp
        # Clear intermediate markers if callers accidentally forward them.
        to_encode.pop("mfa_pending", None)
        if to_encode.get("scope") in _SCOPE_TO_TOKEN_USE:
            to_encode.pop("scope", None)
    elif token_use == TOKEN_USE_MFA_CHALLENGE:
        to_encode["scope"] = "mfa_challenge"
        to_encode["mfa_pending"] = True
        to_encode.pop("session_exp", None)
    elif token_use == TOKEN_USE_MFA_SETUP:
        to_encode["scope"] = "mfa_setup"
        to_encode["mfa_pending"] = True
        to_encode.pop("session_exp", None)
    else:
        raise ValueError(f"unsupported token_use: {token_use}")

    return jwt.encode(to_encode, settings.api_jwt_secret, algorithm="HS256")


def decode_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, settings.api_jwt_secret, algorithms=["HS256"], issuer=settings.api_jwt_issuer)


def assert_not_revoked_by_password_change(
    payload: Dict[str, Any],
    password_changed_at: Any,
    *,
    skew_seconds: float = 1.0,
) -> None:
    """Invalidate tokens issued at/before the last password change/reset."""
    changed = _as_utc(password_changed_at)
    if changed is None:
        return
    issued = _as_utc(payload.get("iat"))
    if issued is None:
        raise ValueError("missing_iat")
    # Allow tiny clock skew so the token minted in the same request as the UPDATE survives.
    if issued.timestamp() + skew_seconds < changed.timestamp():
        raise ValueError("token_revoked")


def assert_session_not_absolutely_expired(payload: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
    current = now or datetime.now(timezone.utc)
    session_exp = resolve_session_exp(payload)
    if current >= session_exp:
        raise ValueError("session_expired")
