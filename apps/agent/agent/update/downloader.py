"""Download agent release to staging path and verify SHA256."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

from agent.update.manifest import ReleaseManifest, verify_sha256, write_json


class UpdateDownloadError(RuntimeError):
    pass


def download_release(
    manifest: ReleaseManifest,
    *,
    dest_dir: Path,
    headers: Optional[dict] = None,
    timeout: tuple[int, int] = (15, 300),
    session: Optional[requests.Session] = None,
) -> Path:
    """Download to ``torqmind-agent.exe.part`` then rename to ``.new`` after hash OK."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    part = dest_dir / "torqmind-agent.exe.part"
    final = dest_dir / "torqmind-agent.exe.new"
    if part.exists():
        part.unlink()
    if final.exists():
        final.unlink()

    http = session or requests.Session()
    try:
        with http.get(
            manifest.url,
            headers=headers or {},
            timeout=timeout,
            stream=True,
        ) as resp:
            if resp.status_code >= 400:
                raise UpdateDownloadError(
                    f"download failed status={resp.status_code} url={manifest.url}"
                )
            written = 0
            with part.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    written += len(chunk)
            if manifest.size > 0 and written != manifest.size:
                part.unlink(missing_ok=True)
                raise UpdateDownloadError(
                    f"size mismatch expected={manifest.size} got={written}"
                )
    except requests.RequestException as exc:
        part.unlink(missing_ok=True)
        raise UpdateDownloadError(str(exc)) from exc

    if not verify_sha256(part, manifest.sha256):
        part.unlink(missing_ok=True)
        raise UpdateDownloadError("sha256 mismatch — refusing to stage broken binary")

    part.replace(final)
    write_json(
        dest_dir / "pending.json",
        {
            "version": manifest.version,
            "sha256": manifest.sha256,
            "size": manifest.size,
            "staged": str(final),
            "mandatory": manifest.mandatory,
        },
    )
    return final


def resolve_manifest_url(api_base: str, path: str = "/agent/update/manifest") -> str:
    base = str(api_base or "").rstrip("/") + "/"
    return urljoin(base, path.lstrip("/"))
