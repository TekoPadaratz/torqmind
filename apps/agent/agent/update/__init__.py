"""Self-update orchestration for Agent 2.0."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import requests

from agent import __version__
from agent.update.apply import agent_base_dir, prepare_and_schedule_update
from agent.update.downloader import UpdateDownloadError, download_release, resolve_manifest_url
from agent.update.manifest import ReleaseManifest, is_newer_version, write_json


def check_and_apply_update(
    *,
    api_base_url: str,
    ingest_key: Optional[str],
    logger,
    base_dir: Optional[Path] = None,
    local_version: Optional[str] = None,
    auto_update: bool = True,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Fetch manifest; if newer and auto_update, stage binary and schedule swap."""
    result: Dict[str, Any] = {
        "checked": True,
        "local_version": local_version or __version__,
        "update_available": False,
        "applied_scheduled": False,
    }
    if not auto_update:
        result["skipped"] = "auto_update_disabled"
        return result

    headers = {}
    if ingest_key:
        headers["X-Ingest-Key"] = str(ingest_key)
    headers["X-Agent-Version"] = str(result["local_version"])

    url = resolve_manifest_url(api_base_url)
    http = session or requests.Session()
    try:
        resp = http.get(url, headers=headers, timeout=(10, 30))
    except requests.RequestException as exc:
        logger.warning("phase=update_check_failed reason=%s", str(exc)[:200])
        result["error"] = str(exc)
        return result

    if resp.status_code == 404:
        result["skipped"] = "no_manifest"
        return result
    if resp.status_code >= 400:
        logger.warning("phase=update_check_failed status=%s", resp.status_code)
        result["error"] = f"status={resp.status_code}"
        return result

    try:
        manifest = ReleaseManifest.from_dict(resp.json())
    except (ValueError, TypeError) as exc:
        logger.warning("phase=update_manifest_invalid reason=%s", exc)
        result["error"] = str(exc)
        return result

    result["remote_version"] = manifest.version
    if not is_newer_version(manifest.version, result["local_version"]):
        result["skipped"] = "up_to_date"
        return result

    result["update_available"] = True
    root = agent_base_dir(base_dir)
    updates = root / "updates"
    try:
        staged = download_release(
            manifest,
            dest_dir=updates,
            headers=headers,
            session=http,
        )
    except UpdateDownloadError as exc:
        logger.warning("phase=update_download_failed reason=%s", str(exc)[:300])
        result["error"] = str(exc)
        return result

    prepare_and_schedule_update(root, staged_new=staged)
    logger.info(
        "phase=update_scheduled local=%s remote=%s staged=%s",
        result["local_version"],
        manifest.version,
        staged,
    )
    result["applied_scheduled"] = True
    write_json(
        updates / "last_check.json",
        {k: v for k, v in result.items() if k != "error"},
    )
    return result
