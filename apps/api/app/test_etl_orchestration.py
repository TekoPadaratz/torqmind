from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

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


class _LoaderCursor:
    def __init__(self, row: dict[str, object] | None = None) -> None:
        self._row = row or {}

    def fetchone(self):
        return self._row


class _ChunkLoaderConn:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0
        self.calls: list[tuple[str, object]] = []

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def execute(self, query, params=None):
        sql = str(query)
        self.calls.append((sql, params))
        if "fact_pagamento_comprovante_pending_bounds" in sql:
            return _LoaderCursor(
                {
                    "result": {
                        "candidate_refs": 300000,
                        "min_referencia": 10,
                        "max_referencia": 210,
                        "watermark_before": datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc),
                        "bridge_watermark_before": datetime(2026, 4, 1, 11, 0, tzinfo=timezone.utc),
                    }
                }
            )
        if "load_fact_pagamento_comprovante_range_detail" in sql:
            start_ref = int(params[1])
            rows = 7 if start_ref == 10 else 5
            return _LoaderCursor(
                {
                    "result": {
                        "rows": rows,
                        "candidate_count": rows * 2,
                        "bridge_rows": 2,
                        "bridge_miss_count": 1,
                        "upsert_inserts": rows,
                        "upsert_updates": 0,
                        "conflict_count": 0,
                    }
                }
            )
        if "fact_venda_item_pending_bounds" in sql:
            return _LoaderCursor(
                {
                    "result": {
                        "candidate_rows": 260000,
                        "min_id_comprovante": 100,
                        "max_id_comprovante": 320,
                        "watermark_before": datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                    }
                }
            )
        if "load_fact_venda_item_range_detail" in sql:
            start_doc = int(params[1])
            rows = 11 if start_doc == 100 else 13
            return _LoaderCursor(
                {
                    "result": {
                        "rows": rows,
                        "candidate_count": rows * 3,
                        "upsert_inserts": rows,
                        "upsert_updates": 0,
                        "conflict_count": 0,
                    }
                }
            )
        if "FROM etl.run_log" in sql and "status = 'running'" in sql:
            return _LoaderCursor({"id": 999})
        if "MAX(received_at) AS max_ts" in sql:
            return _LoaderCursor({"max_ts": datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)})
        if "MAX(updated_at) AS max_ts" in sql:
            return _LoaderCursor({"max_ts": datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc)})
        return _LoaderCursor({})


class EtlOrchestrationTest(unittest.TestCase):
    def test_cli_and_endpoint_share_same_incremental_cycle_function(self) -> None:
        self.assertIs(cli_etl_incremental.run_incremental_cycle, etl_orchestrator.run_incremental_cycle)
        self.assertIs(routes_etl.run_incremental_cycle, etl_orchestrator.run_incremental_cycle)
        self.assertIs(routes_ingest.run_incremental_cycle, etl_orchestrator.run_incremental_cycle)

    def test_risk_source_watermarks_follow_canonical_ingest_datasets(self) -> None:
        self.assertEqual(
            etl_orchestrator.RISK_SOURCE_WATERMARK_DATASETS,
            (
                "comprovantes",
                "itenscomprovantes",
                "formas_pgto_comprovantes",
                "turnos",
            ),
        )

    @patch("app.services.etl_orchestrator._dispatch_cash_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._dispatch_payment_telegram_alerts", return_value=etl_orchestrator._empty_notification_details())
    @patch("app.services.etl_orchestrator._run_tenant_post_refresh", return_value=etl_orchestrator._empty_post_meta())
    @patch("app.services.etl_orchestrator._run_global_refresh", return_value={"ref_date": "2026-03-19", "refreshed_any": True, "sales_marts_refreshed": True})
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch("app.services.etl_orchestrator._run_tenant_phase")
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_refreshes_global_marts_only_once(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
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
    @patch("app.services.etl_orchestrator._run_global_refresh")
    @patch("app.services.etl_orchestrator._run_tenant_clock_meta_sql", return_value={})
    @patch(
        "app.services.etl_orchestrator._run_tenant_phase",
        return_value={
            "ok": True,
            "id_empresa": 1,
            "ref_date": date(2026, 4, 7),
            "hot_window_days": 3,
            "meta": {
                "risk_events": 0,
                "risk_events_skipped": True,
                "risk_events_skip_reason": "source_watermarks_not_ahead_of_risk",
            },
        },
    )
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_skips_global_refresh_when_track_has_no_requested_work(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
        _mock_phase,
        _mock_clock_meta,
        mock_refresh,
        _mock_payment_telegram,
        _mock_cash_telegram,
    ) -> None:
        summary = etl_orchestrator.run_incremental_cycle(
            [1],
            ref_date=date(2026, 4, 7),
            refresh_mart=True,
            force_full=False,
            fail_fast=False,
            track=etl_orchestrator.TRACK_RISK,
            tenant_rows=[
                {"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True},
            ],
            acquire_lock=False,
        )

        self.assertTrue(summary["ok"], summary)
        self.assertEqual(mock_refresh.call_count, 0)
        self.assertFalse(summary["global_refresh"]["requested"])
        self.assertFalse(summary["global_refresh"]["refreshed_any"])

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
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_runs_clock_driven_refresh_without_data_changes(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
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
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_fail_fast_marks_unprocessed_post_refresh_items_as_failed(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
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

    def test_phase_domains_ignore_catalog_only_sales_changes_for_operational_snapshots(self) -> None:
        only_catalog = etl_orchestrator._phase_domains(
            {
                "dim_grupos": 3,
                "dim_produtos": 12,
                "dim_funcionarios": 4,
            },
            force_full=False,
            track=etl_orchestrator.TRACK_OPERATIONAL,
        )
        self.assertFalse(only_catalog["sales"])

        customer_dim = etl_orchestrator._phase_domains(
            {"dim_clientes": 2},
            force_full=False,
            track=etl_orchestrator.TRACK_OPERATIONAL,
        )
        self.assertTrue(customer_dim["sales"])

    def test_refresh_meta_treats_risk_mutations_without_upsert_rows_as_requested_work(self) -> None:
        meta = {
            "risk_events": 0,
            "risk_events_has_changes": True,
            "risk_events_total_mutations": 2,
        }

        self.assertTrue(etl_orchestrator._refresh_meta_has_requested_work(meta))
        domains = etl_orchestrator._phase_domains(
            meta,
            force_full=False,
            track=etl_orchestrator.TRACK_RISK,
        )
        self.assertTrue(domains["risk"])

    def test_run_tenant_phase_skips_risk_track_when_source_watermarks_did_not_advance(self) -> None:
        conn = _DummyConn()

        with patch("app.services.etl_orchestrator._hot_window_days", return_value=3), patch(
            "app.services.etl_orchestrator._risk_source_watermarks_ahead",
            return_value=False,
        ), patch("app.services.etl_orchestrator._run_logged_count_step") as mock_step, patch(
            "app.services.etl_orchestrator._log_instant_step"
        ) as mock_log_instant, patch("app.services.etl_orchestrator._log_stage_summary"):
            result = etl_orchestrator._run_tenant_phase(
                conn,
                1,
                False,
                date(2026, 4, 7),
                track=etl_orchestrator.TRACK_RISK,
            )

        mock_step.assert_not_called()
        self.assertEqual(result["meta"]["risk_events"], 0)
        self.assertTrue(result["meta"]["risk_events_skipped"])
        self.assertEqual(result["meta"]["risk_events_skip_reason"], "source_watermarks_not_ahead_of_risk")
        self.assertEqual(mock_log_instant.call_args.kwargs["meta"]["reason"], "source_watermarks_not_ahead_of_risk")

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

    def test_logged_count_step_merges_extra_meta_from_operation_result(self) -> None:
        conn = _DummyConn()

        with patch("app.services.etl_orchestrator._start_step_log", return_value=102), patch(
            "app.services.etl_orchestrator._finish_step_log"
        ) as mock_finish:
            rows, _duration_ms = etl_orchestrator._run_logged_count_step(
                conn,
                1,
                "fact_pagamento_comprovante",
                stage="phase",
                ref_date=date(2026, 4, 8),
                operation=lambda: (
                    3,
                    {
                        "candidate_count": 7,
                        "bridge_resolve_ms": 12,
                        "upsert_inserts": 2,
                        "upsert_updates": 1,
                        "conflict_count": 1,
                    },
                ),
            )

        self.assertEqual(rows, 3)
        self.assertEqual(mock_finish.call_count, 1)
        finish_meta = mock_finish.call_args.kwargs["meta"]
        self.assertEqual(int(finish_meta["candidate_count"]), 7)
        self.assertEqual(int(finish_meta["bridge_resolve_ms"]), 12)
        self.assertEqual(int(finish_meta["upsert_inserts"]), 2)
        self.assertEqual(int(finish_meta["upsert_updates"]), 1)
        self.assertEqual(int(finish_meta["conflict_count"]), 1)
        self.assertIn("ms", finish_meta)

    @patch("app.services.etl_orchestrator._update_running_step_log")
    def test_payment_loader_chunks_large_backfill_and_commits_each_chunk(self, _mock_update_step_log) -> None:
        conn = _ChunkLoaderConn()
        progress_events: list[dict[str, object]] = []

        with patch.object(etl_orchestrator, "SALES_BULK_CHUNK_THRESHOLD_ROWS", 100000), patch.object(
            etl_orchestrator,
            "PAYMENT_REFERENCE_CHUNK_ROWS",
            150000,
        ):
            rows, meta = etl_orchestrator._run_payment_loader(
                conn,
                1,
                force_full=False,
                progress_callback=progress_events.append,
            )

        self.assertEqual(rows, 12)
        self.assertTrue(meta["chunked"])
        self.assertEqual(int(meta["chunk_count"]), 2)
        self.assertEqual(int(meta["upsert_inserts"]), 12)
        self.assertEqual(int(meta["bridge_rows"]), 4)
        self.assertEqual(conn.commit_calls, 3)
        self.assertEqual([event["event"] for event in progress_events], ["step_chunk", "step_chunk"])

    @patch("app.services.etl_orchestrator._update_running_step_log")
    def test_venda_item_loader_chunks_large_backfill_and_commits_each_chunk(self, _mock_update_step_log) -> None:
        conn = _ChunkLoaderConn()
        progress_events: list[dict[str, object]] = []

        with patch.object(etl_orchestrator, "SALES_BULK_CHUNK_THRESHOLD_ROWS", 100000), patch.object(
            etl_orchestrator,
            "VENDA_ITEM_COMPROVANTE_CHUNK_ROWS",
            130000,
        ):
            rows, meta = etl_orchestrator._run_venda_item_loader(
                conn,
                1,
                force_full=False,
                progress_callback=progress_events.append,
            )

        self.assertEqual(rows, 24)
        self.assertTrue(meta["chunked"])
        self.assertEqual(int(meta["chunk_count"]), 2)
        self.assertEqual(int(meta["upsert_inserts"]), 24)
        self.assertEqual(conn.commit_calls, 3)
        self.assertEqual([event["event"] for event in progress_events], ["step_chunk", "step_chunk"])

    def test_inspect_running_etl_state_marks_rows_without_matching_locks_as_stale(self) -> None:
        select_cursor = unittest.mock.MagicMock()
        select_cursor.fetchall.return_value = [
            {
                "id": 101,
                "id_empresa": 1,
                "started_at": datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc),
                "step_name": "refresh_marts",
                "meta": {"track": "operational"},
            }
        ]
        update_cursor = unittest.mock.MagicMock()
        conn = unittest.mock.MagicMock()
        conn.execute.side_effect = [select_cursor, update_cursor]

        with patch("app.services.etl_orchestrator.advisory_lock_is_available", return_value=True):
            state = etl_orchestrator.inspect_running_etl_state(conn, tenant_id=1)

        self.assertEqual(state["live_rows"], [])
        self.assertEqual(len(state["stale_rows"]), 1)
        self.assertEqual(int(state["stale_rows"][0]["id"]), 101)
        self.assertEqual(conn.execute.call_count, 2)
        self.assertEqual(conn.execute.call_args_list[1].args[1][-1], 101)
        conn.commit.assert_called_once()

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

    @patch("app.services.etl_orchestrator._hot_window_days", return_value=3)
    @patch("app.services.etl_orchestrator._log_stage_summary")
    @patch("app.services.etl_orchestrator._log_instant_step")
    @patch("app.services.etl_orchestrator._run_logged_count_step")
    def test_operational_phase_skips_risk_events_and_marks_track_in_meta(
        self,
        mock_logged_step,
        mock_log_instant,
        _mock_stage_summary,
        _mock_hot_window_days,
    ) -> None:
        step_order: list[str] = []

        def _logged_step_side_effect(_conn, _tenant_id, step_name, **_kwargs):
            step_order.append(step_name)
            return 1, 10

        mock_logged_step.side_effect = _logged_step_side_effect

        result = etl_orchestrator._run_tenant_phase(
            _DummyConn(),
            1,
            False,
            date(2026, 3, 23),
            track=etl_orchestrator.TRACK_OPERATIONAL,
        )

        self.assertEqual(step_order, [name for name, _query in etl_orchestrator.PHASE_SQL_STEPS])
        self.assertEqual(result["track"], etl_orchestrator.TRACK_OPERATIONAL)
        self.assertEqual(result["meta"]["track"], etl_orchestrator.TRACK_OPERATIONAL)
        self.assertTrue(result["meta"]["risk_events_skipped"])
        self.assertEqual(result["meta"]["risk_events_skip_reason"], "track_excludes_risk")
        self.assertFalse(result["meta"]["refresh_domains"]["risk"])
        mock_log_instant.assert_not_called()

    @patch("app.services.etl_orchestrator._hot_window_days", return_value=3)
    @patch("app.services.etl_orchestrator._log_stage_summary")
    @patch("app.services.etl_orchestrator._log_instant_step")
    @patch("app.services.etl_orchestrator._run_logged_count_step")
    @patch("app.services.etl_orchestrator._risk_source_watermarks_ahead", return_value=True)
    def test_risk_phase_runs_only_risk_step(
        self,
        _mock_risk_source_watermarks_ahead,
        mock_logged_step,
        _mock_log_instant,
        _mock_stage_summary,
        _mock_hot_window_days,
    ) -> None:
        step_order: list[str] = []

        def _logged_step_side_effect(_conn, _tenant_id, step_name, **_kwargs):
            step_order.append(step_name)
            return 9, 90

        mock_logged_step.side_effect = _logged_step_side_effect

        result = etl_orchestrator._run_tenant_phase(
            _DummyConn(),
            1,
            False,
            date(2026, 3, 23),
            track=etl_orchestrator.TRACK_RISK,
        )

        self.assertEqual(step_order, ["risk_events"])
        self.assertEqual(result["track"], etl_orchestrator.TRACK_RISK)
        self.assertEqual(result["meta"]["risk_events"], 9)
        self.assertTrue(result["meta"]["refresh_domains"]["risk"])
        self.assertFalse(result["meta"]["refresh_domains"]["sales"])

    @patch("app.services.etl_orchestrator._log_stage_summary")
    @patch("app.services.etl_orchestrator._log_instant_step")
    @patch("app.services.etl_orchestrator._run_logged_count_step")
    def test_operational_post_refresh_skips_risk_dependent_steps(
        self,
        mock_logged_step,
        mock_log_instant,
        _mock_stage_summary,
    ) -> None:
        step_order: list[str] = []

        def _logged_step_side_effect(_conn, _tenant_id, step_name, **_kwargs):
            step_order.append(step_name)
            return 5, 50

        mock_logged_step.side_effect = _logged_step_side_effect

        meta = {
            "fact_venda": 1,
            "fact_pagamento_comprovante": 1,
            "fact_caixa_turno": 1,
        }
        result = etl_orchestrator._run_tenant_post_refresh(
            _DummyConn(),
            1,
            meta,
            date(2026, 3, 23),
            False,
            3,
            track=etl_orchestrator.TRACK_OPERATIONAL,
        )

        self.assertEqual(
            step_order,
            [
                "customer_sales_daily_snapshot",
                "customer_rfm_snapshot",
                "customer_churn_risk_snapshot",
                "payment_notifications",
                "cash_notifications",
            ],
        )
        self.assertFalse(result["health_score_refreshed"])
        self.assertFalse(result["insights_generated"])
        skipped_reasons = [call.kwargs["meta"]["reason"] for call in mock_log_instant.call_args_list]
        self.assertIn("track_excludes_step", skipped_reasons)

    @patch("app.services.etl_orchestrator._log_stage_summary")
    @patch("app.services.etl_orchestrator._log_instant_step")
    @patch("app.services.etl_orchestrator._run_logged_count_step")
    def test_risk_post_refresh_uses_exact_risk_delta_window_for_health_score(
        self,
        mock_logged_step,
        _mock_log_instant,
        _mock_stage_summary,
    ) -> None:
        step_order: list[str] = []
        step_meta: dict[str, dict[str, object]] = {}

        def _logged_step_side_effect(_conn, _tenant_id, step_name, **kwargs):
            step_order.append(step_name)
            step_meta[step_name] = dict(kwargs.get("meta") or {})
            return 4, 40

        mock_logged_step.side_effect = _logged_step_side_effect

        result = etl_orchestrator._run_tenant_post_refresh(
            _DummyConn(),
            1,
            {
                "risk_events": 0,
                "risk_events_has_changes": True,
                "risk_events_total_mutations": 3,
                "risk_events_window_start_dt_ref": "2026-03-21",
                "risk_events_window_end_dt_ref": "2026-03-22",
            },
            date(2026, 3, 23),
            False,
            3,
            track=etl_orchestrator.TRACK_RISK,
        )

        self.assertEqual(step_order, ["health_score_snapshot", "insights_generated"])
        self.assertEqual(step_meta["health_score_snapshot"]["start_dt_ref"], "2026-03-21")
        self.assertEqual(step_meta["health_score_snapshot"]["end_dt_ref"], "2026-03-22")
        self.assertEqual(result["health_score_window_source"], "risk_delta")
        self.assertTrue(result["health_score_refreshed"])
        self.assertEqual(result["risk_events_window_start_dt_ref"], date(2026, 3, 21))
        self.assertEqual(result["risk_events_window_end_dt_ref"], date(2026, 3, 22))

    @patch("app.services.etl_orchestrator._log_stage_summary")
    @patch("app.services.etl_orchestrator._log_instant_step")
    @patch("app.services.etl_orchestrator._run_logged_count_step")
    def test_risk_post_refresh_skips_insights_when_delta_is_outside_recent_window(
        self,
        mock_logged_step,
        mock_log_instant,
        _mock_stage_summary,
    ) -> None:
        step_order: list[str] = []

        def _logged_step_side_effect(_conn, _tenant_id, step_name, **_kwargs):
            step_order.append(step_name)
            return 2, 20

        mock_logged_step.side_effect = _logged_step_side_effect

        result = etl_orchestrator._run_tenant_post_refresh(
            _DummyConn(),
            1,
            {
                "risk_events": 0,
                "risk_events_has_changes": True,
                "risk_events_total_mutations": 1,
                "risk_events_window_start_dt_ref": "2026-03-01",
                "risk_events_window_end_dt_ref": "2026-03-02",
            },
            date(2026, 3, 23),
            False,
            3,
            track=etl_orchestrator.TRACK_RISK,
        )

        self.assertEqual(step_order, ["health_score_snapshot"])
        self.assertFalse(result["insights_generated"])
        skipped_reasons = [call.kwargs["meta"]["reason"] for call in mock_log_instant.call_args_list]
        self.assertIn("no_domain_changes", skipped_reasons)

    def test_risk_window_detail_expands_changed_range_for_forward_propagation(self) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "min_source_ts": datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            "max_source_ts": datetime(2026, 4, 20, 18, 0, tzinfo=timezone.utc),
            "min_changed_ts": datetime(2026, 4, 8, 9, 0, tzinfo=timezone.utc),
            "max_changed_ts": datetime(2026, 4, 9, 11, 0, tzinfo=timezone.utc),
            "risk_watermark": datetime(2026, 4, 9, 8, 0, tzinfo=timezone.utc),
        }
        conn = MagicMock()
        conn.execute.return_value = cursor

        detail = etl_orchestrator._risk_window_detail(
            conn,
            1,
            force_full=False,
            lookback_days=14,
        )

        self.assertTrue(detail["has_source_data"])
        self.assertEqual(detail["window_start_dt_ref"], date(2026, 4, 7))
        self.assertEqual(detail["window_end_dt_ref"], date(2026, 4, 20))
        self.assertEqual(detail["window_days"], 14)

    @patch("app.services.etl_orchestrator._unlock_cycle_locks")
    @patch("app.services.etl_orchestrator._try_cycle_locks", return_value=[(62041, 230319)])
    @patch("app.services.etl_orchestrator._try_tenant_track_lock", return_value=False)
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_can_skip_busy_tenants_without_failing(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
        _mock_try_tenant_lock,
        _mock_try_cycle_locks,
        _mock_unlock_cycle_locks,
    ) -> None:
        summary = etl_orchestrator.run_incremental_cycle(
            [1],
            ref_date=date(2026, 3, 23),
            refresh_mart=True,
            force_full=False,
            fail_fast=True,
            track=etl_orchestrator.TRACK_OPERATIONAL,
            skip_busy_tenants=True,
            tenant_rows=[{"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True}],
            acquire_lock=True,
        )

        self.assertTrue(summary["ok"], summary)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(summary["skipped"], 1)
        self.assertTrue(summary["items"][0]["skipped"])
        self.assertEqual(summary["items"][0]["reason"], "tenant_busy")

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
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_incremental_cycle_does_not_run_global_refresh_when_refresh_disabled(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
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

    @patch("app.services.etl_orchestrator._run_tenant_post_refresh")
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
    @patch("app.services.etl_orchestrator.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []})
    @patch("app.services.etl_orchestrator.get_conn", side_effect=lambda **_: _dummy_conn_ctx())
    def test_operational_cycle_defers_heavy_publication_even_when_refresh_requested(
        self,
        _mock_get_conn,
        _mock_inspect_running_state,
        _mock_phase,
        _mock_clock_meta,
        mock_refresh,
        mock_post_refresh,
    ) -> None:
        mock_post_refresh.return_value = etl_orchestrator._empty_post_meta()
        summary = etl_orchestrator.run_incremental_cycle(
            [1],
            ref_date=date(2026, 3, 23),
            refresh_mart=True,
            force_full=False,
            fail_fast=True,
            track=etl_orchestrator.TRACK_OPERATIONAL,
            tenant_rows=[{"id_empresa": 1, "nome": "Tenant 1", "status": "active", "is_active": True}],
            acquire_lock=False,
        )

        self.assertTrue(summary["ok"], summary)
        mock_refresh.assert_not_called()
        self.assertEqual(mock_post_refresh.call_count, 1)
        self.assertEqual(mock_post_refresh.call_args.kwargs["publication_mode"], etl_orchestrator.PUBLICATION_MODE_FAST_PATH)
        self.assertTrue(summary["global_refresh"]["requested"])
        self.assertTrue(summary["global_refresh"]["deferred"])
        self.assertEqual(summary["global_refresh"]["recommended_track"], etl_orchestrator.TRACK_RISK)
        self.assertTrue(summary["global_refresh"]["fast_path_available"])
        self.assertTrue(summary["global_refresh"]["fast_path_executed"])
        self.assertEqual(summary["global_refresh"]["fast_path_items"], 1)
        self.assertFalse(summary["items"][0]["result"]["meta"]["mart_refreshed"])
        self.assertTrue(summary["items"][0]["result"]["meta"]["publication_deferred"])
        self.assertEqual(
            summary["items"][0]["result"]["meta"]["publication_mode"],
            etl_orchestrator.PUBLICATION_MODE_FAST_PATH,
        )
        self.assertTrue(summary["items"][0]["result"]["meta"]["publication_executed"])


if __name__ == "__main__":
    unittest.main()
