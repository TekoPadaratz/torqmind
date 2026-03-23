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
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def execute(self, *_args, **_kwargs):
        raise AssertionError("Unexpected SQL execution in this test")


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
    @patch("app.services.etl_orchestrator._run_tenant_post_refresh", return_value=etl_orchestrator._empty_post_meta())
    @patch("app.services.etl_orchestrator._run_global_refresh", return_value={"ref_date": "2026-03-19", "refreshed_any": True, "sales_marts_refreshed": True})
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch("app.services.etl_orchestrator._run_tenant_phase")
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
        "app.services.etl_orchestrator._run_tenant_post_refresh",
        return_value={
            **etl_orchestrator._empty_post_meta(),
            "customer_rfm_refreshed": True,
            "customer_churn_risk_refreshed": True,
            "finance_aging_refreshed": True,
            "health_score_refreshed": True,
        },
    )
    @patch(
        "app.services.etl_orchestrator._run_global_refresh",
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
    @patch("app.services.etl_orchestrator._run_tenant_phase")
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
    @patch("app.services.etl_orchestrator._run_global_refresh", return_value={"ref_date": "2026-03-20", "refreshed_any": True, "sales_marts_refreshed": True})
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch("app.services.etl_orchestrator._run_tenant_phase")
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
            "app.services.etl_orchestrator._run_tenant_post_refresh",
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

    def test_logged_count_step_persists_running_and_finished_transitions(self) -> None:
        conn = _DummyConn()
        progress_events: list[dict[str, object]] = []

        with patch("app.services.etl_orchestrator._start_step_log", return_value=101) as mock_start, patch(
            "app.services.etl_orchestrator._finish_step_log"
        ) as mock_finish:
            rows, duration_ms = etl_orchestrator._run_logged_count_step(
                conn,
                1,
                "fact_venda_item",
                stage="phase",
                ref_date=date(2026, 3, 23),
                operation=lambda: 7,
                meta={"step_index": 12, "step_count": 14},
                progress_callback=progress_events.append,
            )

        self.assertEqual(rows, 7)
        self.assertGreaterEqual(duration_ms, 0)
        self.assertEqual(conn.commit_calls, 2)
        self.assertEqual(mock_start.call_count, 1)
        self.assertEqual(mock_finish.call_count, 1)
        self.assertEqual(mock_finish.call_args.kwargs["status"], "ok")
        self.assertEqual(mock_finish.call_args.kwargs["rows_processed"], 7)
        self.assertEqual([event["event"] for event in progress_events], ["step_started", "step_finished"])

    @patch("app.services.etl_orchestrator._hot_window_days", return_value=3)
    @patch("app.services.etl_orchestrator._log_stage_summary")
    @patch("app.services.etl_orchestrator._log_instant_step")
    @patch("app.services.etl_orchestrator._run_logged_count_step")
    def test_tenant_phase_runs_explicit_steps_in_loader_order(
        self,
        mock_logged_step,
        _mock_log_instant,
        mock_stage_summary,
        _mock_hot_window_days,
    ) -> None:
        manual_counts = {
            "dim_filial": 1,
            "dim_grupos": 2,
            "dim_localvendas": 3,
            "dim_produtos": 4,
            "dim_funcionarios": 5,
            "dim_usuario_caixa": 6,
            "dim_clientes": 7,
            "fact_comprovante": 8,
            "fact_caixa_turno": 9,
            "fact_pagamento_comprovante": 10,
            "fact_venda": 11,
            "fact_venda_item": 12,
            "fact_financeiro": 13,
            "risk_events": 14,
        }
        step_order: list[str] = []

        def _logged_step_side_effect(_conn, _tenant_id, step_name, **_kwargs):
            step_order.append(step_name)
            return manual_counts[step_name], manual_counts[step_name] * 10

        mock_logged_step.side_effect = _logged_step_side_effect

        result = etl_orchestrator._run_tenant_phase(
            _DummyConn(),
            1,
            False,
            date(2026, 3, 23),
        )

        self.assertEqual(step_order, [name for name, _query in etl_orchestrator.PHASE_SQL_STEPS] + ["risk_events"])
        for step_name, expected_rows in manual_counts.items():
            self.assertEqual(result["meta"][step_name], expected_rows)
        self.assertTrue(result["meta"]["refresh_domains"]["sales"])
        self.assertTrue(result["meta"]["refresh_domains"]["finance"])
        self.assertTrue(result["meta"]["refresh_domains"]["risk"])
        self.assertTrue(result["meta"]["refresh_domains"]["payments"])
        self.assertTrue(result["meta"]["refresh_domains"]["cash"])
        self.assertEqual(mock_stage_summary.call_count, 1)
        self.assertEqual(mock_stage_summary.call_args.args[2], "run_tenant_phase")

    @patch("app.services.etl_orchestrator._run_tenant_post_refresh", return_value=etl_orchestrator._empty_post_meta())
    @patch("app.services.etl_orchestrator._run_global_refresh")
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch(
        "app.services.etl_orchestrator._run_tenant_phase",
        return_value={
            "ok": True,
            "id_empresa": 1,
            "ref_date": date(2026, 3, 23),
            "hot_window_days": 3,
            "meta": {"fact_venda": 1},
        },
    )
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_does_not_run_global_refresh_when_refresh_disabled(
        self,
        _mock_get_conn,
        _mock_phase,
        _mock_clock_meta,
        mock_refresh,
        mock_post_refresh,
    ) -> None:
        summary = etl_orchestrator.run_incremental_cycle(
            [1],
            ref_date=date(2026, 3, 23),
            refresh_mart=False,
            force_full=False,
            fail_fast=True,
            tenant_rows=[{"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True}],
            acquire_lock=False,
        )

        self.assertTrue(summary["ok"], summary)
        mock_refresh.assert_not_called()
        mock_post_refresh.assert_not_called()
        self.assertFalse(summary["global_refresh"]["refreshed_any"])
        self.assertFalse(summary["items"][0]["result"]["meta"]["mart_refreshed"])


if __name__ == "__main__":
    unittest.main()
