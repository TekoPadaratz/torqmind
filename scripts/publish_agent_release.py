#!/usr/bin/env python3
"""Publish a TorqMind Agent release into AGENT_RELEASE_DIR (manifest + exe).

Signing (optional, publisher workstation only):
  export AGENT_UPDATE_ED25519_PRIVATE_KEY_FILE=/secure/path/key.raw.b64
  # file contains base64(raw 32-byte Ed25519 seed) — never pass via CLI argv / logs.

Usage
-----
    python scripts/publish_agent_release.py \\
        --exe /path/to/torqmind-agent.exe \\
        --version 2.0.13 \\
        --release-dir /var/torqmind/agent-releases \\
        --public-base-url https://www.torqmind.com.br/api \\
        --key-id agent-update-2026-a \\
        --valid-days 30

Does **not** generate or distribute production keys. Does **not** accept private
key material on the command line.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _load_private_key_b64_from_env() -> str | None:
    path = (os.environ.get("AGENT_UPDATE_ED25519_PRIVATE_KEY_FILE") or "").strip()
    if path:
        return Path(path).read_text(encoding="utf-8").strip()
    # Inline env accepted for CI publishers with secret stores — still not CLI argv.
    inline = (os.environ.get("AGENT_UPDATE_ED25519_PRIVATE_KEY_B64") or "").strip()
    return inline or None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--exe", required=True, help="Path to built torqmind-agent.exe")
    ap.add_argument("--version", required=True)
    ap.add_argument("--release-dir", default="/var/torqmind/agent-releases")
    ap.add_argument(
        "--public-base-url",
        default="",
        help="API public base including /api if needed (builds download URL)",
    )
    ap.add_argument("--mandatory", action="store_true")
    ap.add_argument("--min-version", default="2.0.0")
    ap.add_argument("--channel", default="stable")
    ap.add_argument("--key-id", default="", help="Signing key id (required when signing)")
    ap.add_argument("--valid-days", type=int, default=30)
    args = ap.parse_args()

    src = Path(args.exe).resolve()
    if not src.is_file():
        raise SystemExit(f"exe not found: {src}")

    release_dir = Path(args.release_dir)
    release_dir.mkdir(parents=True, exist_ok=True)
    versioned = release_dir / f"torqmind-agent-{args.version}.exe"
    latest = release_dir / "torqmind-agent.exe"
    shutil.copy2(src, versioned)
    shutil.copy2(src, latest)
    digest = sha256_file(versioned)
    size = versioned.stat().st_size

    base = str(args.public_base_url or "").rstrip("/")
    url = f"{base}/agent/update/download/{args.version}" if base else ""

    now = datetime.now(timezone.utc)
    nbf = now.isoformat().replace("+00:00", "Z")
    exp = (now + timedelta(days=max(1, int(args.valid_days)))).isoformat().replace("+00:00", "Z")

    manifest = {
        "product": "torqmind-agent",
        "channel": str(args.channel).strip() or "stable",
        "version": args.version,
        "min_version": args.min_version,
        "sha256": digest,
        "size": size,
        "url": url,
        "released_at": nbf,
        "valid_not_before": nbf,
        "valid_not_after": exp,
        "mandatory": bool(args.mandatory),
    }

    priv_b64 = _load_private_key_b64_from_env()
    if priv_b64:
        key_id = str(args.key_id or "").strip()
        if not key_id:
            raise SystemExit("--key-id is required when signing with Ed25519")
        # Import signing helpers without adding agent to PYTHONPATH permanently.
        import sys

        repo = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo / "apps" / "agent"))
        from agent.update.signing import (  # noqa: WPS433
            canonical_signed_bytes,
            load_private_key_b64,
            sign_canonical,
        )

        private = load_private_key_b64(priv_b64)
        payload = canonical_signed_bytes(
            product=manifest["product"],
            channel=manifest["channel"],
            key_id=key_id,
            version=manifest["version"],
            sha256=manifest["sha256"],
            size=manifest["size"],
            url=manifest["url"],
            valid_not_before=manifest["valid_not_before"],
            valid_not_after=manifest["valid_not_after"],
        )
        manifest["key_id"] = key_id
        manifest["signature_ed25519"] = sign_canonical(private, payload)

    # Never echo private key material. Manifest JSON is public.
    (release_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
