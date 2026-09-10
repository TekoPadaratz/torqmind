"""Ed25519 manifest authenticity for Agent updates (cryptography).

Implemented control — not documentation:
- Canonical signed bytes bind product, channel, key_id, version, sha256, size,
  url, and validity window.
- Verification uses a local trust store of public keys (offline install).
- Secure mode rejects missing / invalid / expired / unknown-key signatures
  with **no** silent fallback to HMAC or unsigned manifests.

Not implemented here:
- Distribution of production private keys.
- Fetching the first trust root over unauthenticated HTTP.
- Treating deletion of ``floor_version.json`` as authorized recovery.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

PRODUCT_ID = "torqmind-agent"
CANONICAL_VERSION = "v1"


class SigningError(ValueError):
    pass


@dataclass(frozen=True)
class TrustedPublicKey:
    key_id: str
    public_key_b64: str
    status: str = "active"  # active | retired
    not_after: Optional[str] = None

    def is_usable(self, *, now: Optional[datetime] = None) -> bool:
        if str(self.status or "").lower() != "active":
            return False
        if not self.not_after:
            return True
        current = now or datetime.now(timezone.utc)
        try:
            limit = _parse_iso_z(self.not_after)
        except SigningError:
            return False
        return current <= limit


def _parse_iso_z(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise SigningError("empty timestamp")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SigningError(f"invalid timestamp: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def public_key_b64_from_private(private_key: Ed25519PrivateKey) -> str:
    raw = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return base64.b64encode(raw).decode("ascii")


def load_private_key_b64(private_key_b64: str) -> Ed25519PrivateKey:
    try:
        raw = base64.b64decode(str(private_key_b64).strip(), validate=True)
        return Ed25519PrivateKey.from_private_bytes(raw)
    except Exception as exc:  # noqa: BLE001
        raise SigningError("invalid Ed25519 private key material") from exc


def load_public_key_b64(public_key_b64: str) -> Ed25519PublicKey:
    try:
        raw = base64.b64decode(str(public_key_b64).strip(), validate=True)
        if len(raw) != 32:
            raise SigningError("Ed25519 public key must be 32 raw bytes")
        return Ed25519PublicKey.from_public_bytes(raw)
    except SigningError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SigningError("invalid Ed25519 public key material") from exc


def canonical_signed_bytes(
    *,
    product: str,
    channel: str,
    key_id: str,
    version: str,
    sha256: str,
    size: int,
    url: str,
    valid_not_before: str,
    valid_not_after: str,
) -> bytes:
    """Canonical representation for Ed25519 (ordered, LF-terminated fields)."""
    lines = [
        f"torqmind-agent-update:{CANONICAL_VERSION}",
        f"product={str(product).strip()}",
        f"channel={str(channel).strip()}",
        f"key_id={str(key_id).strip()}",
        f"version={str(version).strip()}",
        f"sha256={str(sha256).strip().lower()}",
        f"size={int(size)}",
        f"url={str(url).strip()}",
        f"valid_not_before={str(valid_not_before).strip()}",
        f"valid_not_after={str(valid_not_after).strip()}",
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def sign_canonical(private_key: Ed25519PrivateKey, payload: bytes) -> str:
    sig = private_key.sign(payload)
    return base64.b64encode(sig).decode("ascii")


def verify_canonical(
    public_key: Ed25519PublicKey,
    payload: bytes,
    signature_b64: str,
) -> bool:
    try:
        sig = base64.b64decode(str(signature_b64).strip(), validate=True)
        public_key.verify(sig, payload)
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False


def load_trust_store(path: Path | str) -> List[TrustedPublicKey]:
    """Load public keys from an offline-installed JSON trust store."""
    target = Path(path)
    if not target.is_file():
        raise SigningError(f"trust store not found: {target}")
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SigningError(f"trust store unreadable: {target}") from exc
    keys_raw = data.get("keys") if isinstance(data, dict) else None
    if not isinstance(keys_raw, list):
        raise SigningError("trust store must contain a keys array")
    out: List[TrustedPublicKey] = []
    for item in keys_raw:
        if not isinstance(item, dict):
            continue
        kid = str(item.get("key_id") or "").strip()
        pub = str(item.get("public_key_b64") or "").strip()
        if not kid or not pub:
            continue
        out.append(
            TrustedPublicKey(
                key_id=kid,
                public_key_b64=pub,
                status=str(item.get("status") or "active").strip().lower(),
                not_after=(str(item["not_after"]).strip() if item.get("not_after") else None),
            )
        )
    return out


def trust_keys_from_config(entries: Iterable[Mapping[str, Any]]) -> List[TrustedPublicKey]:
    out: List[TrustedPublicKey] = []
    for item in entries or []:
        if not isinstance(item, Mapping):
            continue
        kid = str(item.get("key_id") or "").strip()
        pub = str(item.get("public_key_b64") or "").strip()
        if not kid or not pub:
            continue
        out.append(
            TrustedPublicKey(
                key_id=kid,
                public_key_b64=pub,
                status=str(item.get("status") or "active").strip().lower(),
                not_after=(str(item["not_after"]).strip() if item.get("not_after") else None),
            )
        )
    return out


def resolve_trusted_key(
    keys: Iterable[TrustedPublicKey],
    key_id: str,
    *,
    now: Optional[datetime] = None,
) -> TrustedPublicKey:
    kid = str(key_id or "").strip()
    if not kid:
        raise SigningError("manifest missing key_id")
    for key in keys:
        if key.key_id == kid:
            if not key.is_usable(now=now):
                raise SigningError(f"trusted key is not usable: {kid}")
            return key
    raise SigningError(f"unknown update signing key_id: {kid}")


def generate_ephemeral_keypair_for_tests() -> tuple[str, str, Ed25519PrivateKey]:
    """Test-only key material — never use as production keys."""
    private = Ed25519PrivateKey.generate()
    priv_b64 = base64.b64encode(
        private.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    ).decode("ascii")
    pub_b64 = public_key_b64_from_private(private)
    return priv_b64, pub_b64, private
