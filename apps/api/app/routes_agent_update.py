"""Agent 2.0 update channel — manifest + binary download (X-Ingest-Key)."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from app.config import settings
from app.db import get_conn

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["agent"])

_VERSION_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+-]{0,63}$")


def _release_dir() -> Path:
    raw = getattr(settings, "agent_release_dir", None) or "/var/torqmind/agent-releases"
    return Path(str(raw))


def _resolve_tenant_from_ingest(x_ingest_key: Optional[str]) -> int:
    if not x_ingest_key:
        raise HTTPException(status_code=401, detail="X-Ingest-Key required")
    key = str(x_ingest_key).strip()
    try:
        UUID(key)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
    with get_conn() as conn:
        try:
            row = conn.execute(
                "SELECT id_empresa FROM app.tenants WHERE ingest_key = %s AND is_active = true",
                (key,),
            ).fetchone()
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
    if not row:
        raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
    return int(row["id_empresa"] if isinstance(row, dict) else row[0])


def _load_manifest() -> Dict[str, Any]:
    path = _release_dir() / "manifest.json"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="No agent release published")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("agent_manifest_read_failed path=%s err=%s", path, exc)
        raise HTTPException(status_code=500, detail="Agent manifest unreadable") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Invalid agent manifest")
    return data


@router.get("/hello")
def agent_hello(
    request: Request,
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_agent_version: Optional[str] = Header(None, alias="X-Agent-Version"),
):
    tenant_id = _resolve_tenant_from_ingest(x_ingest_key)
    min_version = str(getattr(settings, "agent_min_version", None) or "2.0.0")
    logger.info(
        "agent_hello tenant=%s agent_version=%s client=%s",
        tenant_id,
        x_agent_version,
        request.client.host if request.client else None,
    )
    return {
        "ok": True,
        "server_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "tenant_id": tenant_id,
        "min_agent_version": min_version,
        "channel": "agent-v2",
    }


@router.get("/update/manifest")
def agent_update_manifest(
    request: Request,
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_agent_version: Optional[str] = Header(None, alias="X-Agent-Version"),
):
    tenant_id = _resolve_tenant_from_ingest(x_ingest_key)
    data = _load_manifest()
    version = str(data.get("version") or "").strip()
    sha256 = str(data.get("sha256") or "").strip().lower()
    size = int(data.get("size") or 0)
    if not version or not sha256:
        raise HTTPException(status_code=500, detail="Incomplete agent manifest")

    # Prefer absolute URL from manifest; otherwise build download URL from request.
    url = str(data.get("url") or "").strip()
    if not url:
        base = str(request.base_url).rstrip("/")
        url = f"{base}/agent/update/download/{version}"

    logger.info(
        "agent_manifest tenant=%s local_agent=%s remote=%s",
        tenant_id,
        x_agent_version,
        version,
    )
    return {
        "version": version,
        "min_version": data.get("min_version") or getattr(settings, "agent_min_version", "2.0.0"),
        "sha256": sha256,
        "size": size,
        "url": url,
        "released_at": data.get("released_at"),
        "mandatory": bool(data.get("mandatory", False)),
    }


@router.get("/update/download/{version}")
def agent_update_download(
    version: str,
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_agent_version: Optional[str] = Header(None, alias="X-Agent-Version"),
):
    _resolve_tenant_from_ingest(x_ingest_key)
    if not _VERSION_RE.match(version):
        raise HTTPException(status_code=400, detail="Invalid version")
    release_dir = _release_dir()
    # Canonical filenames published by scripts/publish_agent_release.py
    candidates = [
        release_dir / f"torqmind-agent-{version}.exe",
        release_dir / "torqmind-agent.exe",
    ]
    path = next((p for p in candidates if p.is_file()), None)
    if path is None:
        raise HTTPException(status_code=404, detail="Release binary not found")
    logger.info(
        "agent_download version=%s agent_version=%s file=%s",
        version,
        x_agent_version,
        path.name,
    )
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename="torqmind-agent.exe",
    )
