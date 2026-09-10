"""Update channel security — Ed25519 authenticity, origin, caps, anti-rollback.

**Implemented**
- Ed25519 verify via ``cryptography`` over canonical product/channel/key/version/
  hash/size/url/validity bytes (see ``signing.py``).
- Secure mode: reject missing/invalid/expired/unknown-key signatures; no silent
  fallback to HMAC or unsigned manifests.
- Origin checks on scheme+hostname+port; TLS verify mandatory in secure mode.
- Floor/last_applied assist anti-rollback; deleting ``floor_version.json`` alone
  is **not** treated as authorized recovery (floor cannot fall below local or
  last_applied).

**Legacy (explicit, incomplete protection)**
- ``update_secure_mode=false`` (default): unsigned manifests still accepted so
  existing LAN HTTP agents keep updating until IT installs a trust store and
  enables secure mode. HMAC fields may be checked if a key is configured, but
  HMAC is **not** a substitute for Ed25519 in secure mode and must not be
  distributed as a global posto secret.

**Not implemented / external**
- Production key generation/distribution; first trust root over HTTP;
  Windows exe proof; publishing releases.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from agent.update.manifest import ReleaseManifest, is_newer_version, parse_semver, write_json
from agent.update.signing import (
    PRODUCT_ID,
    SigningError,
    TrustedPublicKey,
    canonical_signed_bytes,
    load_public_key_b64,
    load_trust_store,
    resolve_trusted_key,
    trust_keys_from_config,
    verify_canonical,
    _parse_iso_z,
)


class UpdatePolicyError(ValueError):
    pass


DEFAULT_MAX_DOWNLOAD_BYTES = 80 * 1024 * 1024
DEFAULT_CHANNEL = "stable"


def canonical_manifest_bytes_legacy_hmac(manifest: ReleaseManifest) -> bytes:
    """Legacy HMAC payload (version/sha256/size/url only) — not used in secure mode."""
    lines = [
        str(manifest.version).strip(),
        str(manifest.sha256).strip().lower(),
        str(int(manifest.size)),
        str(manifest.url).strip(),
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def compute_manifest_hmac(manifest: ReleaseManifest, key: str | bytes) -> str:
    import base64

    raw_key = key.encode("utf-8") if isinstance(key, str) else bytes(key)
    digest = hmac.new(
        raw_key, canonical_manifest_bytes_legacy_hmac(manifest), hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def verify_manifest_hmac(
    manifest: ReleaseManifest,
    *,
    key: str | bytes,
    signature_b64: str,
) -> bool:
    expected = compute_manifest_hmac(manifest, key)
    got = str(signature_b64 or "").strip()
    if not got:
        return False
    return hmac.compare_digest(expected, got)


def load_trusted_keys(
    *,
    trust_store_path: Optional[str],
    config_keys: Optional[List[Mapping[str, Any]]] = None,
) -> List[TrustedPublicKey]:
    keys: List[TrustedPublicKey] = []
    if config_keys:
        keys.extend(trust_keys_from_config(config_keys))
    if trust_store_path:
        keys.extend(load_trust_store(trust_store_path))
    # De-duplicate by key_id (last wins).
    by_id: Dict[str, TrustedPublicKey] = {k.key_id: k for k in keys}
    return list(by_id.values())


def verify_ed25519_manifest(
    manifest: ReleaseManifest,
    *,
    trusted_keys: List[TrustedPublicKey],
    now: Optional[datetime] = None,
) -> None:
    """Raise UpdatePolicyError unless Ed25519 signature verifies under trust store."""
    current = now or datetime.now(timezone.utc)
    sig = (manifest.signature_ed25519 or "").strip()
    if not sig:
        raise UpdatePolicyError("manifest missing signature_ed25519")
    if not (manifest.key_id or "").strip():
        raise UpdatePolicyError("manifest missing key_id")
    if not (manifest.valid_not_before or "").strip() or not (manifest.valid_not_after or "").strip():
        raise UpdatePolicyError("manifest missing validity window")
    try:
        nbf = _parse_iso_z(manifest.valid_not_before or "")
        exp = _parse_iso_z(manifest.valid_not_after or "")
    except SigningError as exc:
        raise UpdatePolicyError(str(exc)) from exc
    if current < nbf:
        raise UpdatePolicyError("manifest signature not yet valid")
    if current > exp:
        raise UpdatePolicyError("manifest signature expired")
    product = (manifest.product or PRODUCT_ID).strip() or PRODUCT_ID
    channel = (manifest.channel or DEFAULT_CHANNEL).strip() or DEFAULT_CHANNEL
    if product != PRODUCT_ID:
        raise UpdatePolicyError(f"unexpected product={product!r}")
    try:
        trusted = resolve_trusted_key(trusted_keys, manifest.key_id or "", now=current)
        pub = load_public_key_b64(trusted.public_key_b64)
    except SigningError as exc:
        raise UpdatePolicyError(str(exc)) from exc
    payload = canonical_signed_bytes(
        product=product,
        channel=channel,
        key_id=manifest.key_id or "",
        version=manifest.version,
        sha256=manifest.sha256,
        size=manifest.size,
        url=manifest.url,
        valid_not_before=manifest.valid_not_before or "",
        valid_not_after=manifest.valid_not_after or "",
    )
    if not verify_canonical(pub, payload, sig):
        raise UpdatePolicyError("manifest Ed25519 signature mismatch")


def enforce_signature_policy(
    manifest: ReleaseManifest,
    *,
    secure_mode: bool,
    trusted_keys: Optional[List[TrustedPublicKey]] = None,
    hmac_key: Optional[str] = None,
    now: Optional[datetime] = None,
) -> None:
    """Secure mode = Ed25519 only. Legacy may optionally check HMAC if configured."""
    if secure_mode:
        if not trusted_keys:
            raise UpdatePolicyError(
                "update_secure_mode=true requires an offline trust store / embedded public keys"
            )
        verify_ed25519_manifest(manifest, trusted_keys=trusted_keys, now=now)
        return

    # Legacy path: incomplete protection — unsigned OK; HMAC only if both sides set.
    sig_ed = (manifest.signature_ed25519 or "").strip()
    if sig_ed and trusted_keys:
        # If a signature is present and we have keys, verify (fail closed on bad sig).
        verify_ed25519_manifest(manifest, trusted_keys=trusted_keys, now=now)
        return
    key = (hmac_key or "").strip()
    sig_hmac = (manifest.signature_hmac_sha256 or "").strip()
    if key and sig_hmac:
        if not verify_manifest_hmac(manifest, key=key, signature_b64=sig_hmac):
            raise UpdatePolicyError("manifest HMAC signature mismatch")


def max_allowed_bytes(manifest: ReleaseManifest, configured_max: int) -> int:
    hard = int(configured_max) if configured_max > 0 else DEFAULT_MAX_DOWNLOAD_BYTES
    if manifest.size > 0:
        if manifest.size > hard:
            raise UpdatePolicyError(
                f"manifest size {manifest.size} exceeds max_download_bytes {hard}"
            )
        return min(hard, int(manifest.size) + 1024)
    return hard


def floor_version_path(updates_dir: Path) -> Path:
    return updates_dir / "floor_version.json"


def last_applied_path(updates_dir: Path) -> Path:
    return updates_dir / "last_applied.json"


def read_floor_version(updates_dir: Path) -> Optional[str]:
    path = floor_version_path(updates_dir)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    ver = str(data.get("version") or "").strip()
    return ver or None


def read_last_applied_version(updates_dir: Path) -> Optional[str]:
    path = last_applied_path(updates_dir)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    ver = str(data.get("version") or "").strip()
    return ver or None


def write_floor_version(updates_dir: Path, version: str) -> None:
    write_json(floor_version_path(updates_dir), {"version": str(version).strip()})


def write_last_applied(updates_dir: Path, *, version: str, key_id: Optional[str]) -> None:
    write_json(
        last_applied_path(updates_dir),
        {
            "version": str(version).strip(),
            "key_id": (str(key_id).strip() if key_id else None),
            "note": "Deleting floor_version.json is not authorized recovery",
        },
    )


def effective_floor_version(updates_dir: Path, local_version: str) -> str:
    """Highest known floor. Missing floor file must not lower protection below local/last_applied."""
    best = str(local_version).strip()
    for cand in (read_floor_version(updates_dir), read_last_applied_version(updates_dir)):
        if cand and is_newer_version(cand, best):
            best = cand
    return best


def assert_not_unauthorized_rollback(
    remote_version: str,
    *,
    local_version: str,
    floor_version: Optional[str],
) -> None:
    if not is_newer_version(remote_version, local_version):
        raise UpdatePolicyError(
            f"refusing non-upgrade remote={remote_version} local={local_version}"
        )
    if floor_version and parse_semver(remote_version)[:3] < parse_semver(floor_version)[:3]:
        raise UpdatePolicyError(
            f"unauthorized rollback blocked: remote={remote_version} floor={floor_version}"
        )


def record_successful_stage(
    updates_dir: Path,
    version: str,
    *,
    key_id: Optional[str] = None,
) -> None:
    current = read_floor_version(updates_dir)
    if current is None or is_newer_version(version, current):
        write_floor_version(updates_dir, version)
    write_last_applied(updates_dir, version=version, key_id=key_id)


def transition_notes() -> Dict[str, Any]:
    return {
        "legacy_default": "update_secure_mode=false — unsigned updates still accepted (incomplete protection)",
        "secure_mode": "install trust store offline, then update_secure_mode=true (Ed25519 required, TLS+HTTPS)",
        "trust_root": "first public keys via IT/USB/config.enc — never bootstrap trust over unauthenticated HTTP",
        "key_rotation": "add new key_id as active; retire old key_id; republish with new key_id",
        "hmac_legacy": "optional only outside secure mode; do not distribute a global HMAC to postos",
        "sha256": "sha256 remains mandatory integrity; Ed25519 provides authenticity",
        "floor_file": "deleting floor_version.json is NOT authorized recovery",
        "windows_proof": "exe/service validation pending until authorized Windows host",
    }
