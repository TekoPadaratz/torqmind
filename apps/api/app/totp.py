"""TOTP (RFC 6238) two-factor authentication helpers.

Self-contained: the TOTP algorithm uses only the standard library (``hmac`` /
``hashlib`` / ``struct`` / ``base64``), so there is no dependency on a paid or
external TOTP service. Secrets are encrypted at rest with Fernet
(``cryptography``) using ``settings.totp_encryption_key``.

Compatible with Google Authenticator, Microsoft Authenticator, Authy,
Bitwarden, 1Password, Proton Authenticator and any standard TOTP app.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
import struct
import time
from urllib.parse import quote

from app.config import settings

logger = logging.getLogger("torqmind.totp")

_DIGITS = 6
_PERIOD = 30
_B32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"


class TOTPNotConfigured(RuntimeError):
    """Raised when 2FA is requested but TOTP_ENCRYPTION_KEY is not set."""


def is_totp_configured() -> bool:
    """True when an encryption key is configured so 2FA can be used safely."""
    return bool((settings.totp_encryption_key or "").strip())


def _fernet():
    key = (settings.totp_encryption_key or "").strip()
    if not key:
        raise TOTPNotConfigured(
            "TOTP_ENCRYPTION_KEY não configurada — 2FA indisponível."
        )
    from cryptography.fernet import Fernet

    return Fernet(key.encode("utf-8"))


def generate_secret(length: int = 20) -> str:
    """Generate a new random base32 TOTP secret (default 160-bit)."""
    raw = secrets.token_bytes(length)
    return base64.b32encode(raw).decode("utf-8").rstrip("=")


def encrypt_secret(secret_b32: str) -> str:
    """Encrypt a base32 secret for storage at rest."""
    return _fernet().encrypt(secret_b32.encode("utf-8")).decode("utf-8")


def decrypt_secret(token: str) -> str:
    """Decrypt a stored secret back to its base32 form."""
    return _fernet().decrypt(token.encode("utf-8")).decode("utf-8")


def _b32_decode(secret_b32: str) -> bytes:
    padding = "=" * ((8 - len(secret_b32) % 8) % 8)
    return base64.b32decode(secret_b32.upper() + padding, casefold=True)


def _hotp(secret_b32: str, counter: int) -> str:
    key = _b32_decode(secret_b32)
    msg = struct.pack(">Q", counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    binary = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(binary % (10 ** _DIGITS)).zfill(_DIGITS)


def now_code(secret_b32: str, at: float | None = None) -> str:
    """Current TOTP code (used by tests; never logged in production)."""
    counter = int((at if at is not None else time.time()) // _PERIOD)
    return _hotp(secret_b32, counter)


def verify_code(secret_b32: str, code: str, *, valid_window: int | None = None, at: float | None = None) -> bool:
    """Verify a TOTP code with a small clock-tolerance window.

    Uses constant-time comparison and never logs the code or the secret.
    """
    if not code:
        return False
    code = code.strip().replace(" ", "")
    if not code.isdigit() or len(code) != _DIGITS:
        return False
    window = settings.totp_valid_window if valid_window is None else valid_window
    base = int((at if at is not None else time.time()) // _PERIOD)
    for drift in range(-window, window + 1):
        candidate = _hotp(secret_b32, base + drift)
        if hmac.compare_digest(candidate, code):
            return True
    return False


def provisioning_uri(secret_b32: str, account_label: str, issuer: str | None = None) -> str:
    """Build an otpauth:// URI for QR-code provisioning in authenticator apps."""
    issuer_name = issuer or settings.totp_issuer or "TorqMind"
    label = quote(f"{issuer_name}:{account_label}", safe="")
    params = (
        f"secret={secret_b32}"
        f"&issuer={quote(issuer_name, safe='')}"
        f"&algorithm=SHA1&digits={_DIGITS}&period={_PERIOD}"
    )
    return f"otpauth://totp/{label}?{params}"


# ── One-time recovery codes ──────────────────────────────────

def generate_recovery_codes(count: int = 8) -> list[str]:
    """Generate human-friendly one-time recovery codes (plaintext, shown once)."""
    codes: list[str] = []
    for _ in range(count):
        raw = "".join(secrets.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(10))
        codes.append(f"{raw[:5]}-{raw[5:]}")
    return codes


def hash_recovery_code(code: str) -> str:
    """Hash a recovery code for storage (normalized, sha256)."""
    normalized = code.strip().upper().replace("-", "").replace(" ", "")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
