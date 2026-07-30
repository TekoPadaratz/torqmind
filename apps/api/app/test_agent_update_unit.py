"""API tests for Agent 2.0 update manifest channel."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routes_agent_update import router


def _app(release_dir: Path) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_manifest_requires_ingest_key(tmp_path: Path):
    client = _app(tmp_path)
    with patch("app.routes_agent_update._release_dir", return_value=tmp_path):
        resp = client.get("/agent/update/manifest")
    assert resp.status_code == 401


def test_manifest_ok(tmp_path: Path):
    exe = tmp_path / "torqmind-agent-2.0.0.exe"
    exe.write_bytes(b"fake-exe")
    manifest = {
        "version": "2.0.0",
        "sha256": "abc",
        "size": exe.stat().st_size,
        "url": "",
        "min_version": "2.0.0",
        "mandatory": False,
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    client = _app(tmp_path)
    with patch("app.routes_agent_update._release_dir", return_value=tmp_path), patch(
        "app.routes_agent_update._resolve_tenant_from_ingest", return_value=1
    ):
        resp = client.get(
            "/agent/update/manifest",
            headers={"X-Ingest-Key": "00000000-0000-0000-0000-000000000001"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == "2.0.0"
    assert "download/2.0.0" in body["url"]


def test_download_ok(tmp_path: Path):
    exe = tmp_path / "torqmind-agent-2.0.0.exe"
    exe.write_bytes(b"fake-exe-bytes")
    client = _app(tmp_path)
    with patch("app.routes_agent_update._release_dir", return_value=tmp_path), patch(
        "app.routes_agent_update._resolve_tenant_from_ingest", return_value=1
    ):
        resp = client.get(
            "/agent/update/download/2.0.0",
            headers={"X-Ingest-Key": "00000000-0000-0000-0000-000000000001"},
        )
    assert resp.status_code == 200
    assert resp.content == b"fake-exe-bytes"


def test_hello_ok():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    with patch("app.routes_agent_update._resolve_tenant_from_ingest", return_value=7):
        resp = client.get(
            "/agent/hello",
            headers={
                "X-Ingest-Key": "00000000-0000-0000-0000-000000000001",
                "X-Agent-Version": "2.0.0",
            },
        )
    assert resp.status_code == 200
    assert resp.json()["tenant_id"] == 7
    assert resp.json()["ok"] is True
