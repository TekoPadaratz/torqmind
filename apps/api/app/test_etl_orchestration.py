from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from app.cli import etl_incremental as cli_etl_incremental
from app import routes_etl
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

    @patch("app.services.etl_orchestrator._dispatch_cash_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._dispatch_payment_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._run_tenant_post_refresh_sql", return_value=etl_orchestrator._empty_post_meta())
    @patch("app.services.etl_orchestrator._run_global_refresh_sql", return_value={"ref_date": "2026-03-19", "refreshed_any": True, "sales_marts_refreshed": True})
    @patch("app.services.etl_orchestrator._run_tenant_phase_sql")
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_refreshes_global_marts_only_once(
        self,
        _mock_get_conn,
        mock_phase,
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


if __name__ == "__main__":
    unittest.main()
