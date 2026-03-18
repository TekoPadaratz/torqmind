from __future__ import annotations

import json
import time
import unittest
import urllib.request
from datetime import date, datetime, timezone

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
            filial_row = conn.execute(
                "SELECT id_filial FROM auth.filiais WHERE id_empresa = %s ORDER BY id_filial LIMIT 1",
                (1,),
            ).fetchone()
            cls.filial_id = int(filial_row["id_filial"]) if filial_row and filial_row["id_filial"] is not None else 1
            customer_branch_row = conn.execute(
                """
                SELECT v.id_filial
                FROM dw.fact_venda v
                JOIN dw.dim_cliente dc
                  ON dc.id_empresa = v.id_empresa
                 AND dc.id_filial = v.id_filial
                 AND dc.id_cliente = v.id_cliente
                WHERE v.id_empresa = %s
                  AND COALESCE(v.cancelado, false) = false
                  AND NULLIF(btrim(dc.nome), '') IS NOT NULL
                GROUP BY v.id_filial
                ORDER BY COUNT(*) DESC, v.id_filial
                LIMIT 1
                """,
                (1,),
            ).fetchone()
            cls.customer_branch_id = (
                int(customer_branch_row["id_filial"])
                if customer_branch_row and customer_branch_row["id_filial"] is not None
                else None
            )

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

    def test_incremental_run_all_refreshes_marts_and_keeps_payment_notifications_idempotent(self) -> None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            before_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM app.notifications
                WHERE id_empresa = %s
                  AND title LIKE %s
                """,
                (1, "Anomalia de pagamento (%)"),
            ).fetchone()

            first = conn.execute(
                "SELECT etl.run_all(%s, %s, %s, %s) AS result",
                (1, False, True, date.today()),
            ).fetchone()["result"]

            middle_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM app.notifications
                WHERE id_empresa = %s
                  AND title LIKE %s
                """,
                (1, "Anomalia de pagamento (%)"),
            ).fetchone()

            second = conn.execute(
                "SELECT etl.run_all(%s, %s, %s, %s) AS result",
                (1, False, True, date.today()),
            ).fetchone()["result"]

            after_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM app.notifications
                WHERE id_empresa = %s
                  AND title LIKE %s
                """,
                (1, "Anomalia de pagamento (%)"),
            ).fetchone()
            conn.commit()

        before_total = int(before_row["total"] or 0)
        middle_total = int(middle_row["total"] or 0)
        after_total = int(after_row["total"] or 0)

        self.assertTrue(first.get("ok"), first)
        self.assertTrue(second.get("ok"), second)
        self.assertTrue((first.get("meta") or {}).get("mart_refreshed"))
        self.assertIn("payment_notifications", first.get("meta") or {})
        self.assertIn("payment_notifications", second.get("meta") or {})
        self.assertGreaterEqual(middle_total, before_total)
        self.assertEqual(after_total, middle_total)

    def test_bi_dashboard_overview_returns_data(self) -> None:
        status, body = self._request(
            "/bi/dashboard/overview?dt_ini=2025-08-01&dt_fim=2025-08-31&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("kpis", body)
        self.assertIn("by_day", body)
        self.assertIn("risk", body)

        status_compact, compact = self._request(
            "/bi/dashboard/overview?dt_ini=2025-09-01&dt_fim=2025-09-18&dt_ref=2025-09-18&id_empresa=1&compact=true",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status_compact, 200)
        self.assertIn("risk", compact)
        self.assertIn("insights_generated", compact)
        self.assertIn("jarvis", compact)
        self.assertNotIn("payments", compact)

    def test_anonymous_retention_endpoint_returns_payload(self) -> None:
        status, body = self._request(
            "/bi/clients/retention-anonymous?dt_ini=2025-08-01&dt_fim=2025-08-31&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("kpis", body)
        self.assertIn("series", body)
        self.assertIn("breakdown_dow", body)

    def test_churn_endpoint_returns_contract(self) -> None:
        status, body = self._request(
            "/bi/clients/churn?dt_ini=2025-08-01&dt_fim=2025-08-31&id_empresa=1&min_score=40&limit=5",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("top_risk", body)
        self.assertIn("summary", body)
        self.assertIn("drilldown", body)

        summary = body.get("summary") or {}
        self.assertIn("total_top_risk", summary)
        self.assertIn("avg_churn_score", summary)
        self.assertIn("revenue_at_risk_30d", summary)

        drilldown = body.get("drilldown") or {}
        self.assertIn("snapshot", drilldown)
        self.assertIn("series", drilldown)

    def test_customers_overview_returns_customer_names_for_real_branch(self) -> None:
        if self.customer_branch_id is None:
            self.skipTest("No branch with named customers is available in the current demo dataset")

        status, body = self._request(
            (
                "/bi/customers/overview?dt_ini=2025-09-01&dt_fim=2025-09-18"
                f"&dt_ref=2025-09-18&id_empresa=1&id_filial={self.customer_branch_id}"
            ),
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        top_customers = body.get("top_customers") or []
        self.assertTrue(top_customers)
        self.assertTrue(str(top_customers[0].get("cliente_nome") or "").strip())
        self.assertFalse(str(top_customers[0].get("cliente_nome") or "").startswith("#ID "))

    def test_competitor_pricing_overview_and_save(self) -> None:
        status_overview, body_overview = self._request(
            "/bi/pricing/competitor/overview?dt_ini=2025-08-01&dt_fim=2025-08-31&id_empresa=1&id_filial=1&days_simulation=10",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status_overview, 200)
        self.assertIn("meta", body_overview)
        self.assertIn("summary", body_overview)
        self.assertIn("items", body_overview)

        items = body_overview.get("items") or []
        if items:
            first = items[0]
            self.assertIn("familia_combustivel", first)
            status_save, body_save = self._request(
                "/bi/pricing/competitor/prices?id_empresa=1&id_filial=1",
                method="POST",
                data={
                    "items": [
                        {
                            "id_produto": int(first["id_produto"]),
                            "competitor_price": float(first.get("avg_price_current") or 1.0),
                        }
                    ]
                },
                headers={"Authorization": f"Bearer {self.token}"},
            )
            self.assertEqual(status_save, 200)
            self.assertTrue(body_save.get("ok"))
            self.assertGreaterEqual(int(body_save.get("saved") or 0), 1)

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

    def test_payments_overview_endpoint_returns_contract(self) -> None:
        status, body = self._request(
            "/bi/payments/overview?dt_ini=2025-09-01&dt_fim=2025-09-18&dt_ref=2025-09-18&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("kpis", body)
        self.assertIn("by_day", body)
        self.assertIn("by_turno", body)
        self.assertIn("anomalies", body)
        self.assertIn("source_status", body["kpis"])
        self.assertIn("summary", body["kpis"])
        self.assertIn(body["kpis"].get("source_status"), {"ok", "partial", "value_gap", "unavailable"})
        for row in body.get("by_day") or []:
            self.assertIn("category_label", row)

    def test_cash_overview_endpoint_returns_contract(self) -> None:
        status, body = self._request(
            "/bi/cash/overview?dt_ini=2025-09-01&dt_fim=2025-09-18&dt_ref=2025-09-18&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("source_status", body)
        self.assertIn("summary", body)
        self.assertIn("kpis", body)
        self.assertIn("open_boxes", body)
        self.assertIn("payment_mix", body)
        self.assertIn("cancelamentos", body)
        self.assertIn("alerts", body)
        self.assertIn(body.get("source_status"), {"ok", "unavailable"})

    def test_finance_overview_exposes_snapshot_status(self) -> None:
        status, body = self._request(
            "/bi/finance/overview?dt_ini=2025-09-01&dt_fim=2025-09-18&dt_ref=2025-09-18&id_empresa=1",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        self.assertEqual(status, 200)
        self.assertIn("aging", body)
        self.assertIn("snapshot_status", body["aging"])

    def test_micro_risk_endpoint(self) -> None:
        # Keep telegram disabled by default in smoke; endpoint must still succeed.
        with get_conn(role="MASTER", tenant_id=1, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO app.telegram_settings (id_empresa, chat_id, is_enabled)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_empresa)
                DO UPDATE SET chat_id = EXCLUDED.chat_id, is_enabled = EXCLUDED.is_enabled
                """,
                (1, "123456789", False),
            )
            conn.commit()

        status, body = self._request(
            f"/etl/micro_risk?minutes=5&id_empresa=1&id_filial={self.filial_id}",
            method="POST",
            data={},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=900,
        )
        self.assertEqual(status, 200)
        self.assertTrue(body.get("ok"))
        self.assertIn("risk_events_computed", body)
        self.assertIn("notifications_upserted", body)
        self.assertIn("telegram_sent", body)


if __name__ == "__main__":
    unittest.main()
