#!/usr/bin/env python3
"""Publish a TorqMind Agent release into AGENT_RELEASE_DIR (manifest + exe).

Usage
-----
    python scripts/publish_agent_release.py \\
        --exe /path/to/torqmind-agent.exe \\
        --version 2.0.0 \\
        --release-dir /var/torqmind/agent-releases \\
        --public-base-url http://redevr.ddns.me/api

Writes:
  manifest.json
  torqmind-agent-{version}.exe
  torqmind-agent.exe  (latest copy)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--exe", required=True, help="Path to built torqmind-agent.exe")
    ap.add_argument("--version", required=True)
    ap.add_argument(
        "--release-dir",
        default="/var/torqmind/agent-releases",
    )
    ap.add_argument(
        "--public-base-url",
        default="",
        help="API public base including /api if needed (builds download URL)",
    )
    ap.add_argument("--mandatory", action="store_true")
    ap.add_argument("--min-version", default="2.0.0")
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

    manifest = {
        "version": args.version,
        "min_version": args.min_version,
        "sha256": digest,
        "size": size,
        "url": url,
        "released_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "mandatory": bool(args.mandatory),
    }
    (release_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
