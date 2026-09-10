"""Self-update orchestration for Agent 2.0."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import requests

from agent import __version__
from agent.transport import (
    TransportPolicyError,
    assert_secure_transport,
    assert_transport_allowed,
    build_http_session,
    session_request_kwargs,
    strip_auth_if_foreign_host,
)
from agent.update.apply import agent_base_dir, prepare_and_schedule_update
from agent.update.downloader import UpdateDownloadError, download_release, resolve_manifest_url
from agent.update.manifest import ReleaseManifest, is_newer_version, write_json
from agent.update.policy import (
    DEFAULT_MAX_DOWNLOAD_BYTES,
    UpdatePolicyError,
    assert_not_unauthorized_rollback,
    effective_floor_version,
    enforce_signature_policy,
    load_trusted_keys,
)


def check_and_apply_update(
    *,
    api_base_url: str,
    ingest_key: Optional[str],
    logger,
    base_dir: Optional[Path] = None,
    local_version: Optional[str] = None,
    auto_update: bool = True,
    session: Optional[requests.Session] = None,
    tls_verify: bool = True,
    allow_insecure_http: bool = True,
    require_https: bool = False,
    allow_foreign_download: bool = False,
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    update_hmac_key: Optional[str] = None,
    update_secure_mode: bool = False,
    update_trust_store_path: Optional[str] = None,
    update_trusted_public_keys: Optional[List[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """Fetch manifest; if newer and auto_update, stage binary and schedule swap."""
    result: Dict[str, Any] = {
        "checked": True,
        "local_version": local_version or __version__,
        "update_available": False,
        "applied_scheduled": False,
        "secure_mode": bool(update_secure_mode),
        "legacy_incomplete_protection": not bool(update_secure_mode),
    }
    if not auto_update:
        result["skipped"] = "auto_update_disabled"
        return result

    secure = bool(update_secure_mode)
    try:
        if secure:
            assert_secure_transport(api_base_url, tls_verify=tls_verify)
            allow_insecure_http = False
            require_https = True
            if not tls_verify:
                raise TransportPolicyError("update_secure_mode requires tls_verify=true")
        else:
            assert_transport_allowed(
                api_base_url,
                allow_insecure_http=allow_insecure_http,
                require_https=require_https,
            )
    except TransportPolicyError as exc:
        logger.warning("phase=update_check_failed reason=%s", str(exc)[:200])
        result["error"] = str(exc)
        return result

    trusted = []
    try:
        trusted = load_trusted_keys(
            trust_store_path=update_trust_store_path,
            config_keys=update_trusted_public_keys,
        )
    except Exception as exc:  # noqa: BLE001 — SigningError/UpdatePolicyError
        if secure:
            logger.warning("phase=update_trust_store_failed reason=%s", str(exc)[:200])
            result["error"] = str(exc)
            return result

    headers = {}
    if ingest_key:
        headers["X-Ingest-Key"] = str(ingest_key)
    headers["X-Agent-Version"] = str(result["local_version"])

    url = resolve_manifest_url(api_base_url)
    safe_headers = strip_auth_if_foreign_host(
        headers, request_url=url, api_base_url=api_base_url
    )
    http = session or build_http_session(tls_verify=True if secure else tls_verify)
    try:
        resp = http.get(url, headers=safe_headers, timeout=(10, 30), **session_request_kwargs())
    except requests.RequestException as exc:
        logger.warning("phase=update_check_failed reason=%s", str(exc)[:200])
        result["error"] = str(exc)
        return result

    if resp.is_redirect or resp.status_code in {301, 302, 303, 307, 308}:
        logger.warning(
            "phase=update_check_failed reason=redirect_refused status=%s",
            resp.status_code,
        )
        result["error"] = f"redirect_refused status={resp.status_code}"
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
    root = agent_base_dir(base_dir)
    updates = root / "updates"
    floor = effective_floor_version(updates, result["local_version"])

    try:
        enforce_signature_policy(
            manifest,
            secure_mode=secure,
            trusted_keys=trusted,
            hmac_key=update_hmac_key,
        )
        if not is_newer_version(manifest.version, result["local_version"]):
            result["skipped"] = "up_to_date"
            return result
        assert_not_unauthorized_rollback(
            manifest.version,
            local_version=result["local_version"],
            floor_version=floor,
        )
    except UpdatePolicyError as exc:
        logger.warning("phase=update_policy_blocked reason=%s", str(exc)[:300])
        result["error"] = str(exc)
        return result

    result["update_available"] = True
    try:
        staged = download_release(
            manifest,
            dest_dir=updates,
            api_base_url=api_base_url,
            headers=headers,
            session=http,
            allow_insecure_http=allow_insecure_http,
            require_https=require_https or secure,
            allow_foreign_download=False if secure else allow_foreign_download,
            max_download_bytes=max_download_bytes,
            secure_mode=secure,
            tls_verify=True if secure else tls_verify,
        )
    except UpdateDownloadError as exc:
        logger.warning("phase=update_download_failed reason=%s", str(exc)[:300])
        result["error"] = str(exc)
        return result

    prepare_and_schedule_update(root, staged_new=staged)
    logger.info(
        "phase=update_scheduled local=%s remote=%s staged=%s secure=%s",
        result["local_version"],
        manifest.version,
        staged,
        secure,
    )
    result["applied_scheduled"] = True
    write_json(
        updates / "last_check.json",
        {k: v for k, v in result.items() if k != "error"},
    )
    return result
