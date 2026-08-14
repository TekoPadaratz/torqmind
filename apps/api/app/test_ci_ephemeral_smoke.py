"""Live smoke against the disposable CI Postgres. Never point at prod/homolog."""

from __future__ import annotations

import os
import unittest

from fastapi.testclient import TestClient

from app.cli import seed as seed_cli
from app.main import app


def _ephemeral_or_skip() -> None:
    if (os.environ.get("APP_ENV") or "").strip().lower() != "test":
        raise unittest.SkipTest("CI ephemeral smoke requires APP_ENV=test")
    if (os.environ.get("TM_EPHEMERAL_LOCAL") or "").strip() != "1":
        raise unittest.SkipTest("CI ephemeral smoke requires TM_EPHEMERAL_LOCAL=1")
    host = (os.environ.get("PG_HOST") or "").strip().lower()
    if host.startswith("172.30.0."):
        raise unittest.SkipTest("refusing production/homolog PG_HOST")
    if host not in {"postgres", "localhost", "127.0.0.1"}:
        raise unittest.SkipTest(f"unexpected PG_HOST={host!r}")
    db = (os.environ.get("PG_DATABASE") or os.environ.get("POSTGRES_DB") or "").strip()
    if db != "torqmind_ci":
        raise unittest.SkipTest(f"PG_DATABASE must be torqmind_ci, got {db!r}")


class CiEphemeralSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _ephemeral_or_skip()
        seed_cli.main()
        cls.client = TestClient(app)

    def test_health_is_up(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertTrue(body.get("ok"), body)
        self.assertEqual(body.get("status"), "up")

    def test_seeded_master_can_login(self) -> None:
        response = self.client.post(
            "/auth/login",
            json={
                "email": seed_cli.PLATFORM_MASTER_EMAIL,
                "password": seed_cli.PLATFORM_MASTER_PASSWORD,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertIn("access_token", body)
        self.assertTrue(str(body.get("home_path") or "").startswith("/sales?"), body.get("home_path"))
