from __future__ import annotations

import json
import time
import unittest
import urllib.request
from datetime import datetime, timezone

from app.db import get_conn

API_BASE = "http://localhost:8000"


class SmokeApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.token = cls._login_token("owner@empresa1.com", "TorqMind@123")
        with get_conn(role="MASTER", tenant_id=1, branch_id=None) as conn:
            row = conn.execute(
                "SELECT ingest_key FROM app.tenants WHERE id_empresa = %s",
                (1,),
            ).fetchone()
            if not row or not row["ingest_key"]:
                raise AssertionError("Missing ingest_key for tenant 1")
            cls.ingest_key = str(row["ingest_key"])

    @staticmethod
    def _request(
        path: str,
        method: str = "GET",
        data: dict | None = None,
        headers: dict | None = None,
        timeout: int = 180,
    ):
        req_headers = headers.copy() if headers else {}
        payload = None
        if data is not None:
            payload = json.dumps(data).encode("utf-8")
            req_headers["Content-Type"] = "application/json"

        req = urllib.request.Request(API_BASE + path, method=method, headers=req_headers, data=payload)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body)

    @classmethod
    def _login_token(cls, email: str, password: str) -> str:
        status, body = cls._request(
            "/auth/login",
            method="POST",
            data={"email": email, "password": password},
        )
        if status != 200:
            raise AssertionError(f"Login failed with status {status}")
        token = body.get("access_token")
        if not token:
            raise AssertionError("Missing access_token")
        return token

    def test_ingest_ndjson_to_stg(self) -> None:
        unique_id = int(datetime.now(tz=timezone.utc).timestamp()) % 2000000000
        ndjson_line = {
            "ID_FILIAL": 1,
            "ID_PRODUTO": unique_id,
            "NOME": f"SMOKE PRODUTO {unique_id}",
        }

        body = json.dumps(ndjson_line, ensure_ascii=False) + "\n"
        req = urllib.request.Request(
            API_BASE + "/ingest/produtos",
            method="POST",
            headers={
                "X-Ingest-Key": self.ingest_key,
                "Content-Type": "application/x-ndjson",
            },
            data=body.encode("utf-8"),
        )
        with urllib.request.urlopen(req, timeout=180) as http_resp:
            ingest_resp = json.loads(http_resp.read().decode("utf-8"))

        self.assertTrue(ingest_resp.get("ok"))
        self.assertGreaterEqual(int(ingest_resp.get("inserted_or_updated", 0)), 1)

        with get_conn(role="MASTER", tenant_id=1, branch_id=None) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM stg.produtos
                WHERE id_empresa = %s AND id_filial = %s AND id_produto = %s
                """,
                (1, 1, unique_id),
            ).fetchone()
        self.assertEqual(int(row["total"]), 1)

    def test_etl_second_run_is_not_slower(self) -> None:
        t1 = time.perf_counter()
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row1 = conn.execute(
                "SELECT etl.run_all(%s, %s, %s) AS result",
                (999, False, False),
            ).fetchone()
            conn.commit()
        elapsed1 = time.perf_counter() - t1

        t2 = time.perf_counter()
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row2 = conn.execute(
                "SELECT etl.run_all(%s, %s, %s) AS result",
                (999, False, False),
            ).fetchone()
            conn.commit()
        elapsed2 = time.perf_counter() - t2

        self.assertIsNotNone(row1)
        self.assertIsNotNone(row2)
        self.assertIsInstance(row1["result"], dict)
        self.assertIsInstance(row2["result"], dict)
        # Incremental expectation: second run should be similar or faster.
        self.assertLessEqual(elapsed2, elapsed1 * 1.5)

    def test_bi_dashboard_overview_returns_data(self) -> None:
        status, body = self._request(
            "/bi/dashboard/overview?dt_ini=2025-08-01&dt_fim=2025-08-31&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("kpis", body)
        self.assertIn("by_day", body)
        self.assertIn("risk", body)

    def test_jarvis_ai_generate_and_usage(self) -> None:
        status_gen, body_gen = self._request(
            "/bi/jarvis/generate?dt_ref=2025-08-31&id_empresa=1&limit=3&force=true",
            method="POST",
            data={},
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status_gen, 200)
        self.assertTrue(body_gen.get("ok"))
        self.assertIn("stats", body_gen)

        status_usage, body_usage = self._request(
            "/bi/admin/ai-usage?days=90&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status_usage, 200)
        self.assertIn("totals", body_usage)
        self.assertIn("daily", body_usage)

    def test_notifications_endpoints(self) -> None:
        status_list, body_list = self._request(
            "/bi/notifications?id_empresa=1&limit=10",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status_list, 200)
        self.assertIn("items", body_list)
        self.assertIn("unread", body_list)

        status_unread, body_unread = self._request(
            "/bi/notifications/unread-count?id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status_unread, 200)
        self.assertIn("unread", body_unread)

        items = body_list.get("items") or []
        if items:
            first_id = int(items[0]["id"])
            status_read, body_read = self._request(
                f"/bi/notifications/{first_id}/read?id_empresa=1",
                method="POST",
                data={},
                headers={"Authorization": f"Bearer {self.token}"},
            )
            self.assertEqual(status_read, 200)
            self.assertTrue(body_read.get("ok"))


if __name__ == "__main__":
    unittest.main()
