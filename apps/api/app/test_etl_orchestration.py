from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from app.cli import etl_incremental as cli_etl_incremental
from app import routes_etl
from app import routes_ingest
from app.services import etl_orchestrator


class _DummyConn:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


@contextmanager
def _dummy_conn_ctx():
    yield _DummyConn()


class EtlOrchestrationTest(unittest.TestCase):
    def test_cli_and_endpoint_share_same_incremental_cycle_function(self) -> None:
        self.assertIs(cli_etl_incremental.run_incremental_cycle, etl_orchestrator.run_incremental_cycle)
        self.assertIs(routes_etl.run_incremental_cycle, etl_orchestrator.run_incremental_cycle)
        self.assertIs(routes_ingest.run_incremental_cycle, etl_orchestrator.run_incremental_cycle)

    @patch("app.services.etl_orchestrator._dispatch_cash_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._dispatch_payment_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._run_tenant_post_refresh_sql", return_value=etl_orchestrator._empty_post_meta())
    @patch("app.services.etl_orchestrator._run_global_refresh_sql", return_value={"ref_date": "2026-03-19", "refreshed_any": True, "sales_marts_refreshed": True})
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch("app.services.etl_orchestrator._run_tenant_phase_sql")
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_refreshes_global_marts_only_once(
        self,
        _mock_get_conn,
        mock_phase,
        _mock_clock_meta,
        mock_refresh,
        mock_post_refresh,
        _mock_payment_telegram,
        _mock_cash_telegram,
    ) -> None:
        mock_phase.side_effect = [
            {
                "ok": True,
                "id_empresa": 1,
                "ref_date": date(2026, 3, 19),
                "hot_window_days": 3,
                "meta": {"fact_venda": 12, "risk_events": 2},
            },
            {
                "ok": True,
                "id_empresa": 2,
                "ref_date": date(2026, 3, 19),
                "hot_window_days": 3,
                "meta": {"fact_financeiro": 4},
            },
        ]

        summary = etl_orchestrator.run_incremental_cycle(
            [1, 2],
            ref_date=date(2026, 3, 19),
            refresh_mart=True,
            force_full=False,
            fail_fast=False,
            tenant_rows=[
                {"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True},
                {"id_empresa": 2, "nome": "Tenant 2", "status": "active", "is_active": True},
            ],
            acquire_lock=False,
        )

        self.assertTrue(summary["ok"], summary)
        self.assertEqual(mock_refresh.call_count, 1)
        self.assertEqual(mock_post_refresh.call_count, 2)
        self.assertTrue(summary["global_refresh"]["refreshed_any"])
        self.assertTrue(all(bool(item["result"]["meta"]["mart_refreshed"]) for item in summary["items"]))

    @patch("app.services.etl_orchestrator._dispatch_cash_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._dispatch_payment_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch(
        "app.services.etl_orchestrator._run_tenant_post_refresh_sql",
        return_value={
            **etl_orchestrator._empty_post_meta(),
            "customer_rfm_refreshed": True,
            "customer_churn_risk_refreshed": True,
            "finance_aging_refreshed": True,
            "health_score_refreshed": True,
        },
    )
    @patch(
        "app.services.etl_orchestrator._run_global_refresh_sql",
        return_value={
            "ref_date": "2026-03-20",
            "refreshed_any": True,
            "sales_marts_refreshed": False,
            "churn_clock_mart_refreshed": True,
        },
    )
    @patch(
        "app.services.etl_orchestrator._run_tenant_clock_meta_sql",
        return_value={
            "clock_daily_rollover": True,
            "clock_churn_mart_refresh": True,
            "clock_customer_rfm_start_dt_ref": "2026-03-20",
            "clock_customer_rfm_end_dt_ref": "2026-03-20",
            "clock_customer_churn_risk_start_dt_ref": "2026-03-20",
            "clock_customer_churn_risk_end_dt_ref": "2026-03-20",
            "clock_finance_aging_start_dt_ref": "2026-03-20",
            "clock_finance_aging_end_dt_ref": "2026-03-20",
            "clock_health_score_start_dt_ref": "2026-03-20",
            "clock_health_score_end_dt_ref": "2026-03-20",
        },
    )
    @patch("app.services.etl_orchestrator._run_tenant_phase_sql")
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_runs_clock_driven_refresh_without_data_changes(
        self,
        _mock_get_conn,
        mock_phase,
        _mock_clock_meta,
        mock_refresh,
        mock_post_refresh,
        _mock_payment_telegram,
        _mock_cash_telegram,
    ) -> None:
        mock_phase.return_value = {
            "ok": True,
            "id_empresa": 1,
            "ref_date": date(2026, 3, 20),
            "hot_window_days": 3,
            "meta": {},
        }

        summary = etl_orchestrator.run_incremental_cycle(
            [1],
            ref_date=date(2026, 3, 20),
            refresh_mart=True,
            force_full=False,
            fail_fast=False,
            tenant_rows=[
                {"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True},
            ],
            acquire_lock=False,
        )

        self.assertTrue(summary["ok"], summary)
        self.assertEqual(mock_refresh.call_count, 1)
        self.assertEqual(mock_post_refresh.call_count, 1)
        refresh_input = mock_refresh.call_args.args[1]
        self.assertTrue(refresh_input["clock_churn_mart_refresh"])
        self.assertFalse(refresh_input.get("fact_venda"))
        item_meta = summary["items"][0]["result"]["meta"]
        self.assertTrue(item_meta["mart_refreshed"])
        self.assertTrue(item_meta["customer_rfm_refreshed"])
        self.assertTrue(item_meta["customer_churn_risk_refreshed"])
        self.assertTrue(item_meta["finance_aging_refreshed"])
        self.assertTrue(item_meta["health_score_refreshed"])

    @patch("app.services.etl_orchestrator._dispatch_cash_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._dispatch_payment_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._run_global_refresh_sql", return_value={"ref_date": "2026-03-20", "refreshed_any": True, "sales_marts_refreshed": True})
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch("app.services.etl_orchestrator._run_tenant_phase_sql")
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_fail_fast_marks_unprocessed_post_refresh_items_as_failed(
        self,
        _mock_get_conn,
        mock_phase,
        _mock_clock_meta,
        _mock_refresh,
        _mock_payment_telegram,
        _mock_cash_telegram,
    ) -> None:
        mock_phase.side_effect = [
            {
                "ok": True,
                "id_empresa": 1,
                "ref_date": date(2026, 3, 20),
                "hot_window_days": 3,
                "meta": {"fact_venda": 1},
            },
            {
                "ok": True,
                "id_empresa": 2,
                "ref_date": date(2026, 3, 20),
                "hot_window_days": 3,
                "meta": {"fact_venda": 1},
            },
        ]

        with patch(
            "app.services.etl_orchestrator._run_tenant_post_refresh_sql",
            side_effect=RuntimeError("post refresh exploded"),
        ) as mock_post_refresh:
            summary = etl_orchestrator.run_incremental_cycle(
                [1, 2],
                ref_date=date(2026, 3, 20),
                refresh_mart=True,
                force_full=False,
                fail_fast=True,
                tenant_rows=[
                    {"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True},
                    {"id_empresa": 2, "nome": "Tenant 2", "status": "active", "is_active": True},
                ],
                acquire_lock=False,
            )

        self.assertFalse(summary["ok"], summary)
        self.assertEqual(summary["failed"], 2)
        self.assertEqual(mock_post_refresh.call_count, 1)
        self.assertEqual(summary["items"][0]["tenant_id"], 1)
        self.assertFalse(summary["items"][0]["ok"])
        self.assertIn("post refresh exploded", summary["items"][0]["error"])
        self.assertEqual(summary["items"][1]["tenant_id"], 2)
        self.assertFalse(summary["items"][1]["ok"])
        self.assertIn("fail_fast", summary["items"][1]["error"])
        self.assertNotIn("result", summary["items"][1])


if __name__ == "__main__":
    unittest.main()
