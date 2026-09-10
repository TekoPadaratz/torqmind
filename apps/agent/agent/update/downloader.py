"""Download agent release to staging path and verify SHA256 (+ size/origin caps)."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

from agent.transport import (
    TransportPolicyError,
    assert_download_origin_allowed,
    assert_secure_transport,
    assert_transport_allowed,
    session_request_kwargs,
    strip_auth_if_foreign_host,
)
from agent.update.manifest import ReleaseManifest, verify_sha256, write_json
from agent.update.policy import (
    DEFAULT_MAX_DOWNLOAD_BYTES,
    UpdatePolicyError,
    max_allowed_bytes,
    record_successful_stage,
)


class UpdateDownloadError(RuntimeError):
    pass


def download_release(
    manifest: ReleaseManifest,
    *,
    dest_dir: Path,
    api_base_url: str,
    headers: Optional[dict] = None,
    timeout: tuple[int, int] = (15, 300),
    session: Optional[requests.Session] = None,
    allow_insecure_http: bool = True,
    require_https: bool = False,
    allow_foreign_download: bool = False,
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    secure_mode: bool = False,
    tls_verify: bool = True,
) -> Path:
    """Download to ``torqmind-agent.exe.part`` then rename to ``.new`` after hash OK."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    part = dest_dir / "torqmind-agent.exe.part"
    final = dest_dir / "torqmind-agent.exe.new"
    if part.exists():
        part.unlink()
    if final.exists():
        final.unlink()

    try:
        if secure_mode:
            assert_secure_transport(manifest.url, tls_verify=tls_verify)
            assert_download_origin_allowed(
                manifest.url,
                api_base_url=api_base_url,
                allow_foreign_download=False,
            )
        else:
            assert_transport_allowed(
                manifest.url,
                allow_insecure_http=allow_insecure_http,
                require_https=require_https,
            )
            assert_download_origin_allowed(
                manifest.url,
                api_base_url=api_base_url,
                allow_foreign_download=allow_foreign_download,
            )
        byte_cap = max_allowed_bytes(manifest, max_download_bytes)
    except (TransportPolicyError, UpdatePolicyError) as exc:
        raise UpdateDownloadError(str(exc)) from exc

    safe_headers = strip_auth_if_foreign_host(
        headers or {},
        request_url=manifest.url,
        api_base_url=api_base_url,
    )

    http = session or requests.Session()
    try:
        with http.get(
            manifest.url,
            headers=safe_headers,
            timeout=timeout,
            stream=True,
            **session_request_kwargs(),
        ) as resp:
            if resp.is_redirect or resp.status_code in {301, 302, 303, 307, 308}:
                location = resp.headers.get("Location") or ""
                raise UpdateDownloadError(
                    f"refusing redirect status={resp.status_code} location={location!r}"
                )
            if resp.status_code >= 400:
                raise UpdateDownloadError(
                    f"download failed status={resp.status_code} url={manifest.url}"
                )
            written = 0
            with part.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    written += len(chunk)
                    if written > byte_cap:
                        part.unlink(missing_ok=True)
                        raise UpdateDownloadError(
                            f"download exceeded byte cap={byte_cap} written={written}"
                        )
                    handle.write(chunk)
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
            "key_id": manifest.key_id,
        },
    )
    record_successful_stage(dest_dir, manifest.version, key_id=manifest.key_id)
    return final


def resolve_manifest_url(api_base: str, path: str = "/agent/update/manifest") -> str:
    base = str(api_base or "").rstrip("/") + "/"
    return urljoin(base, path.lstrip("/"))
