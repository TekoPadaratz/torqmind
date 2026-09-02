"""GET /ingest/health must stay fast (auth-only by default)."""

from __future__ import annotations

from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routes_ingest import router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_ingest_health_auth_only_no_stg_scan():
    client = _client()
    with patch("app.routes_ingest._resolve_id_empresa", return_value=1) as resolve, patch(
        "app.routes_ingest.get_conn"
    ) as get_conn:
        resp = client.get(
            "/ingest/health",
            headers={"X-Ingest-Key": "00000000-0000-0000-0000-000000000001"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["id_empresa"] == 1
    assert body["mode"] == "auth"
    assert body["datasets"] == []
    resolve.assert_called_once()
    get_conn.assert_not_called()


def test_ingest_health_requires_key_when_configured():
    client = _client()
    with patch("app.routes_ingest.settings.ingest_require_key", True):
        resp = client.get("/ingest/health")
    assert resp.status_code in {401, 400}


def test_ingest_health_invalid_uuid_returns_401_not_500():
    client = _client()
    duplicated = "505aee7c-3651-4a1f-877f-ae96772caac5505aee7c-3651-4a1f-877f-ae96772caac5"
    with patch("app.routes_ingest.get_conn") as get_conn:
        resp = client.get("/ingest/health", headers={"X-Ingest-Key": duplicated})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid X-Ingest-Key"
    get_conn.assert_not_called()
