import json
import unittest
from decimal import Decimal
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from app import routes_bi
from app.services import snapshot_cache


class SnapshotCacheTests(unittest.TestCase):
    def test_build_scope_signature_is_deterministic(self):
        context = {"dt_ini": "2026-03-01", "branch_ids": [10, 20]}
        sig1 = snapshot_cache.build_scope_signature(context)
        sig2 = snapshot_cache.build_scope_signature({"branch_ids": [10, 20], "dt_ini": "2026-03-01"})
        self.assertEqual(sig1, sig2)
        self.assertEqual(json.loads(sig1), json.loads(sig2))

    def test_read_snapshot_queries_db(self):
        mock_execute = MagicMock()
        mock_execute.fetchone.return_value = {"snapshot_data": {"ok": True}}
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_execute
        mock_conn.__enter__.return_value = mock_conn
        with patch("app.services.snapshot_cache.get_conn", return_value=mock_conn):
            result = snapshot_cache.read_snapshot("MASTER", 1, None, "dashboard_home", "sig")
        mock_conn.execute.assert_called_once()
        self.assertEqual(result, {"ok": True})

    def test_write_snapshot_inserts_cache(self):
        updated_at = datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"updated_at": updated_at}
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        with patch("app.services.snapshot_cache.get_conn", return_value=mock_conn):
            snapshot_cache.write_snapshot(
                role="MASTER",
                tenant_id=1,
                branch_id=None,
                snapshot_key="dashboard_home",
                scope_signature="sig",
                context={"dt_ini": date(2026, 3, 1)},
                payload={
                    "dt_ref": date(2026, 3, 10),
                    "updated_at": updated_at,
                    "kpis": {"total": Decimal("100.50")},
                },
            )
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        params = mock_conn.execute.call_args[0][1]
        self.assertEqual(json.loads(params[4]), {"dt_ini": "2026-03-01"})
        self.assertEqual(
            json.loads(params[5]),
            {
                "dt_ref": "2026-03-10",
                "updated_at": "2026-03-26T12:00:00+00:00",
                "kpis": {"total": 100.5},
            },
        )

    def test_write_snapshot_skips_placeholder_payloads(self):
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn

        with patch("app.services.snapshot_cache.get_conn", return_value=mock_conn):
            updated_at = snapshot_cache.write_snapshot(
                role="MASTER",
                tenant_id=1,
                branch_id=None,
                snapshot_key="dashboard_home",
                scope_signature="sig",
                context={"dt_ini": date(2026, 3, 1)},
                payload={
                    "data_state": "transient_unavailable",
                    "_fallback_meta": {"fallback_state": "preparing"},
                },
            )

        self.assertIsNone(updated_at)
        mock_conn.execute.assert_not_called()
        mock_conn.commit.assert_not_called()

    def test_hot_bi_routes_use_snapshot_cache(self):
        self.assertFalse(snapshot_cache.route_snapshot_is_bypassed("sales_overview"))
        self.assertFalse(snapshot_cache.route_snapshot_is_bypassed("dashboard_home"))
        self.assertFalse(snapshot_cache.route_snapshot_is_bypassed("noncritical_probe"))

    def test_with_cached_response_prefers_snapshot_during_etl(self):
        cached_record = {
            "snapshot_data": {"kpis": {"cancelamentos": 3}},
            "scope_signature": "exact-sig",
            "updated_at": datetime.now(timezone.utc),
        }
        compatible_record = {
            "snapshot_data": {"kpis": {"cancelamentos": 99}},
            "scope_signature": "stale-sig",
            "updated_at": datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        }
        compute = MagicMock(side_effect=AssertionError("live compute should not run while ETL is active"))

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=cached_record),
            patch.object(routes_bi.snapshot_cache, "read_latest_compatible_snapshot_record", return_value=compatible_record),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "refresh_snapshot_async") as refresh_snapshot_async,
        ):
            payload = routes_bi._with_cached_response(
                scope_key="fraud_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=compute,
            )

        self.assertEqual(payload["kpis"]["cancelamentos"], 3)
        self.assertEqual(payload["_snapshot_cache"]["source"], "snapshot")
        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_snapshot")
        self.assertTrue(payload["_snapshot_cache"]["exact_scope_match"])
        self.assertEqual(payload["_snapshot_cache"]["matched_signature"], "exact-sig")
        compute.assert_not_called()
        refresh_snapshot_async.assert_not_called()

    def test_with_cached_response_attaches_exact_scope_metadata(self):
        cached_record = {
            "snapshot_data": {"summary": {"fuel_types": 2}},
            "scope_signature": "exact-sig",
            "updated_at": datetime.now(timezone.utc),
        }

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=cached_record),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": False, "reasons": [], "etl_running": False},
            ),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="pricing_probe",
                role="MASTER",
                tenant_id=7,
                branch_scope=[14458, 14459],
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=MagicMock(),
                extra_context={"days_simulation": 10},
            )

        self.assertEqual(payload["_scope"]["route_key"], "pricing_probe")
        self.assertEqual(payload["_scope"]["tenant_id"], 7)
        self.assertEqual(payload["_scope"]["signature"], routes_bi.snapshot_cache.build_scope_signature(payload["_scope"]["context"]))
        self.assertEqual(payload["_scope"]["matched_signature"], "exact-sig")
        self.assertTrue(payload["_scope"]["exact_scope_match"])
        self.assertEqual(payload["_scope"]["branch_scope"], [14458, 14459])
        self.assertEqual(payload["_scope"]["context"]["days_simulation"], 10)

    def test_with_cached_response_returns_fresh_snapshot_without_background_refresh(self):
        cached_record = {
            "snapshot_data": {"summary": {"fuel_types": 2}},
            "updated_at": datetime.now(timezone.utc),
        }
        compute = MagicMock()

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=cached_record),
            patch.object(routes_bi.snapshot_cache, "is_tenant_etl_running", return_value=False),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": False, "reasons": [], "etl_running": False},
            ),
            patch.object(routes_bi.snapshot_cache, "refresh_snapshot_async", return_value=True) as refresh_snapshot_async,
        ):
            payload = routes_bi._with_cached_response(
                scope_key="pricing_overview",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=compute,
            )

        self.assertEqual(payload["summary"]["fuel_types"], 2)
        self.assertEqual(payload["_snapshot_cache"]["mode"], "fresh_snapshot")
        self.assertIsNone(payload["_snapshot_cache"]["message"])
        compute.assert_not_called()
        refresh_snapshot_async.assert_not_called()

    def test_with_cached_response_starts_background_refresh_only_for_stale_snapshot(self):
        cached_record = {
            "snapshot_data": {"summary": {"fuel_types": 2}},
            "updated_at": datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
        }
        compute = MagicMock()

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=cached_record),
            patch.object(routes_bi.snapshot_cache, "is_tenant_etl_running", return_value=False),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": False, "reasons": [], "etl_running": False},
            ),
            patch.object(routes_bi.snapshot_cache, "refresh_snapshot_async", return_value=True) as refresh_snapshot_async,
        ):
            payload = routes_bi._with_cached_response(
                scope_key="pricing_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=compute,
            )

        self.assertEqual(payload["_snapshot_cache"]["mode"], "refreshing")
        refresh_snapshot_async.assert_called_once()

    def test_with_cached_response_uses_safe_fallback_when_etl_has_no_snapshot_yet(self):
        compute = MagicMock(side_effect=AssertionError("live compute should not run without snapshot during ETL"))

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(routes_bi.snapshot_cache, "read_latest_compatible_snapshot_record", return_value=None),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="cash_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=compute,
                safe_fallback=lambda: {"summary": "Contrato estável"},
            )

        self.assertEqual(payload["summary"], "Contrato estável")
        self.assertEqual(payload["_snapshot_cache"]["source"], "fallback")
        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_unavailable")
        self.assertEqual(payload["_snapshot_cache"]["reason"], "transient_snapshot_unavailable")
        self.assertFalse(payload["_snapshot_cache"]["exact_scope_match"])
        compute.assert_not_called()

    def test_with_cached_response_merges_fallback_metadata_overrides(self):
        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(routes_bi.snapshot_cache, "read_latest_compatible_snapshot_record", return_value=None),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="sales_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 27),
                dt_fim=date(2026, 3, 27),
                dt_ref=date(2026, 3, 27),
                compute=MagicMock(),
                safe_fallback=lambda: {
                    "reading_status": "operational_current",
                    "_fallback_meta": {
                        "fallback_state": "operational_current",
                        "message": "Mostrando a leitura operacional válida de hoje.",
                    },
                },
            )

        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_unavailable")
        self.assertEqual(payload["_snapshot_cache"]["fallback_state"], "operational_current")
        self.assertEqual(payload["_snapshot_cache"]["message"], "Mostrando a leitura operacional válida de hoje.")

    def test_with_cached_response_uses_stale_snapshot_when_exact_scope_is_missing_and_reads_are_protected(self):
        compatible_record = {
            "snapshot_data": {"kpis": {"cancelamentos": 7}},
            "scope_signature": "compatible-scope",
            "updated_at": datetime(2026, 3, 27, 8, 0, tzinfo=timezone.utc),
        }
        compute = MagicMock(side_effect=AssertionError("live compute should stay blocked during protected reads"))

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(routes_bi.snapshot_cache, "read_latest_compatible_snapshot_record", return_value=compatible_record),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "refresh_snapshot_async") as refresh_snapshot_async,
        ):
            payload = routes_bi._with_cached_response(
                scope_key="fraud_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=10,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=compute,
                safe_fallback=lambda: {"kpis": {"cancelamentos": None}},
            )

        self.assertEqual(payload["kpis"]["cancelamentos"], 7)
        self.assertEqual(payload["_snapshot_cache"]["source"], "snapshot")
        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_stale_snapshot")
        self.assertEqual(payload["_snapshot_cache"]["reason"], "stale_snapshot_fallback")
        self.assertFalse(payload["_snapshot_cache"]["exact_scope_match"])
        self.assertEqual(payload["_snapshot_cache"]["matched_signature"], "compatible-scope")
        compute.assert_not_called()
        refresh_snapshot_async.assert_not_called()

    def test_with_cached_response_computes_synchronously_when_snapshot_is_missing_and_db_is_free(self):
        updated_at = datetime(2026, 3, 27, 9, 30, tzinfo=timezone.utc)
        compute = MagicMock(return_value={"kpis": {"faturamento": 512.4}})

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(routes_bi.snapshot_cache, "is_tenant_etl_running", return_value=False),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": False, "reasons": [], "etl_running": False},
            ),
            patch.object(routes_bi.snapshot_cache, "refresh_snapshot_async", return_value=True) as refresh_snapshot_async,
            patch.object(routes_bi.snapshot_cache, "write_snapshot", return_value=updated_at) as write_snapshot,
        ):
            payload = routes_bi._with_cached_response(
                scope_key="sales_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                compute=compute,
                safe_fallback=lambda: {"kpis": {"faturamento": 0}},
            )

        self.assertEqual(payload["kpis"]["faturamento"], 512.4)
        self.assertEqual(payload["_snapshot_cache"]["source"], "live")
        self.assertEqual(payload["_snapshot_cache"]["mode"], "cold_miss_sync")
        self.assertEqual(payload["_snapshot_cache"]["reason"], "snapshot_cold_miss")
        self.assertEqual(payload["_snapshot_cache"]["updated_at"], updated_at.isoformat())
        compute.assert_called_once()
        write_snapshot.assert_called_once()
        refresh_snapshot_async.assert_not_called()

    def test_safe_sales_overview_payload_uses_operational_current_for_same_day(self):
        operational_payload = {
            "kpis": {"faturamento": 312.5, "margem": 44.2, "ticket_medio": 156.25, "devolucoes": 5},
            "by_day": [{"data_key": 20260327, "faturamento": 312.5, "margem": 44.2}],
            "by_hour": [{"hora": 8, "faturamento": 312.5, "margem": 44.2, "vendas": 2}],
            "top_products": [],
            "top_groups": [],
            "top_employees": [],
            "reading_status": "operational_current",
        }
        with patch.object(routes_bi.repos_mart, "sales_operational_current", return_value=operational_payload):
            payload = routes_bi._safe_sales_overview_payload(
                role="MASTER",
                tenant_id=1,
                filial=10,
                dt_ini=date(2026, 3, 27),
                dt_fim=date(2026, 3, 27),
                as_of=date(2026, 3, 27),
            )

        self.assertEqual(payload["reading_status"], "operational_current")
        self.assertEqual(payload["kpis"]["faturamento"], 312.5)
        self.assertEqual(payload["_fallback_meta"]["fallback_state"], "operational_current")

    def test_safe_sales_overview_payload_marks_preparing_when_operational_read_is_not_available(self):
        with patch.object(routes_bi.repos_mart, "sales_operational_current", return_value=None):
            payload = routes_bi._safe_sales_overview_payload(
                role="MASTER",
                tenant_id=1,
                filial=10,
                dt_ini=date(2026, 3, 27),
                dt_fim=date(2026, 3, 27),
                as_of=date(2026, 3, 27),
            )

        self.assertEqual(payload["reading_status"], "preparing")
        self.assertIsNone(payload["kpis"]["faturamento"])
        self.assertEqual(payload["_fallback_meta"]["fallback_state"], "preparing")
        self.assertEqual(payload["data_state"], "transient_unavailable")

    def test_with_cached_response_ignores_snapshot_write_errors_and_returns_live_payload(self):
        payload_date = date(2026, 3, 10)

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(routes_bi.snapshot_cache, "is_tenant_etl_running", return_value=False),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": False, "reasons": [], "etl_running": False},
            ),
            patch.object(routes_bi.snapshot_cache, "write_snapshot", side_effect=RuntimeError("cache down")),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="dashboard_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=None,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=payload_date,
                compute=lambda: {"dt_ref": payload_date, "kpis": {"total": 3}},
            )

        self.assertEqual(payload["dt_ref"], payload_date)
        self.assertEqual(payload["kpis"]["total"], 3)
        self.assertEqual(payload["_snapshot_cache"]["source"], "live")
        self.assertIsNone(payload["_snapshot_cache"]["updated_at"])

    def test_with_cached_response_uses_fresh_snapshot_for_hot_routes_even_when_etl_guard_is_busy(self):
        cached_record = {
            "snapshot_data": {"kpis": {"faturamento": 42}},
            "scope_signature": "exact-sig",
            "updated_at": datetime.now(timezone.utc),
        }
        compute = MagicMock(side_effect=AssertionError("live compute should not run when a fresh snapshot exists"))

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=cached_record),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "refresh_snapshot_async") as refresh_snapshot_async,
        ):
            payload = routes_bi._with_cached_response(
                scope_key="sales_overview",
                role="MASTER",
                tenant_id=1,
                branch_scope=14458,
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 28),
                dt_ref=date(2026, 3, 28),
                compute=compute,
                safe_fallback=lambda: {"kpis": {"faturamento": None}},
            )

        self.assertEqual(payload["kpis"]["faturamento"], 42)
        self.assertEqual(payload["_snapshot_cache"]["source"], "snapshot")
        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_snapshot")
        self.assertEqual(payload["_snapshot_cache"]["reason"], "etl_running")
        self.assertEqual(payload["_snapshot_cache"]["busy_reasons"], ["etl_running"])
        compute.assert_not_called()
        refresh_snapshot_async.assert_not_called()

    def test_snapshot_is_fresh_uses_route_refresh_window(self):
        fresh = datetime.now(timezone.utc)
        stale = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        self.assertTrue(snapshot_cache.snapshot_is_fresh(fresh, "dashboard_home"))
        self.assertFalse(snapshot_cache.snapshot_is_fresh(stale, "dashboard_home"))

    def test_hot_route_guard_marks_tenant_lock_as_busy(self):
        db_cursor = MagicMock()
        db_cursor.fetchone.return_value = {"lock_waiters": 0, "long_running_queries": 0}
        mock_conn = MagicMock()
        mock_conn.execute.return_value = db_cursor
        mock_conn.__enter__.return_value = mock_conn

        with (
            patch("app.services.snapshot_cache.get_conn", return_value=mock_conn),
            patch("app.services.snapshot_cache.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []}),
            patch("app.services.snapshot_cache.advisory_lock_is_available", return_value=False),
        ):
            guard = snapshot_cache.get_hot_route_guard(tenant_id=1)

        self.assertTrue(guard["protect_reads"])
        self.assertIn("tenant_lock_busy", guard["reasons"])

    def test_hot_route_guard_does_not_protect_reads_for_a_single_long_query_without_other_pressure(self):
        db_cursor = MagicMock()
        db_cursor.fetchone.return_value = {"lock_waiters": 0, "long_running_queries": 1}
        mock_conn = MagicMock()
        mock_conn.execute.return_value = db_cursor
        mock_conn.__enter__.return_value = mock_conn

        with (
            patch("app.services.snapshot_cache.get_conn", return_value=mock_conn),
            patch("app.services.snapshot_cache.inspect_running_etl_state", return_value={"live_rows": [], "stale_rows": []}),
            patch("app.services.snapshot_cache.advisory_lock_is_available", return_value=True),
        ):
            guard = snapshot_cache.get_hot_route_guard(tenant_id=1)

        self.assertFalse(guard["protect_reads"])
        self.assertEqual(guard["reasons"], [])

    def test_hot_route_guard_reconciles_stale_running_rows_without_blocking_reads(self):
        db_cursor = MagicMock()
        db_cursor.fetchone.return_value = {"lock_waiters": 0, "long_running_queries": 0}
        mock_conn = MagicMock()
        mock_conn.execute.return_value = db_cursor
        mock_conn.__enter__.return_value = mock_conn

        with (
            patch("app.services.snapshot_cache.get_conn", return_value=mock_conn),
            patch(
                "app.services.snapshot_cache.inspect_running_etl_state",
                return_value={"live_rows": [], "stale_rows": [{"id": 101, "step_name": "refresh_marts"}]},
            ),
            patch("app.services.snapshot_cache.advisory_lock_is_available", return_value=True),
        ):
            guard = snapshot_cache.get_hot_route_guard(tenant_id=1)

        self.assertFalse(guard["protect_reads"])
        self.assertFalse(guard["etl_running"])
        self.assertEqual(guard["stale_rows_reconciled"], 1)
        self.assertIn("stale_run_log_reconciled", guard["reasons"])

    def test_is_tenant_etl_running_ignores_stale_rows_without_live_locks(self):
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn

        with (
            patch("app.services.snapshot_cache.get_conn", return_value=mock_conn),
            patch(
                "app.services.snapshot_cache.inspect_running_etl_state",
                return_value={"live_rows": [], "stale_rows": [{"id": 77}]},
            ),
        ):
            result = snapshot_cache.is_tenant_etl_running(tenant_id=1)

        self.assertFalse(result)

    def test_last_consolidated_sync_falls_back_to_analytics_status_when_operational_phase_is_missing(self):
        snapshot_cursor = MagicMock()
        snapshot_cursor.fetchone.return_value = {}
        phase_cursor = MagicMock()
        phase_cursor.fetchone.return_value = {}
        analytics_cursor = MagicMock()
        analytics_cursor.fetchone.return_value = {
            "finished_at": datetime(2026, 3, 27, 8, 20, tzinfo=timezone.utc),
            "step_name": "run_tenant_post_refresh",
            "publication_mode": "global_refresh",
        }
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [snapshot_cursor, phase_cursor, analytics_cursor]
        mock_conn.__enter__.return_value = mock_conn

        with patch("app.services.snapshot_cache.get_conn", return_value=mock_conn):
            result = snapshot_cache.last_consolidated_sync(tenant_id=1, branch_id=None)

        self.assertTrue(result["available"])
        self.assertEqual(result["source"], "etl_post_refresh")
        self.assertEqual(result["last_sync_at"], "2026-03-27T08:20:00+00:00")
        self.assertFalse(result["operational"]["available"])
        self.assertTrue(result["analytics"]["available"])

    def test_last_consolidated_sync_prefers_operational_phase_when_available(self):
        snapshot_cursor = MagicMock()
        snapshot_cursor.fetchone.return_value = {}
        phase_cursor = MagicMock()
        phase_cursor.fetchone.return_value = {
            "finished_at": datetime(2026, 3, 27, 8, 35, tzinfo=timezone.utc),
            "step_name": "run_tenant_phase",
        }
        analytics_cursor = MagicMock()
        analytics_cursor.fetchone.return_value = {
            "finished_at": datetime(2026, 3, 27, 8, 20, tzinfo=timezone.utc),
            "step_name": "run_tenant_post_refresh",
            "publication_mode": "fast_path",
        }
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [snapshot_cursor, phase_cursor, analytics_cursor]
        mock_conn.__enter__.return_value = mock_conn

        with patch("app.services.snapshot_cache.get_conn", return_value=mock_conn):
            result = snapshot_cache.last_consolidated_sync(tenant_id=1, branch_id=None)

        self.assertTrue(result["available"])
        self.assertEqual(result["source"], "operational_phase")
        self.assertEqual(result["last_sync_at"], "2026-03-27T08:35:00+00:00")
        self.assertEqual(result["operational"]["last_sync_at"], "2026-03-27T08:35:00+00:00")
        self.assertEqual(result["analytics"]["last_sync_at"], "2026-03-27T08:20:00+00:00")
        self.assertEqual(result["analytics"]["source"], "etl_publication_fast_path")
        self.assertEqual(result["analytics"]["mode"], "fast_path")

    def test_last_consolidated_sync_reports_fast_path_when_only_tenant_publication_exists(self):
        snapshot_cursor = MagicMock()
        snapshot_cursor.fetchone.return_value = {}
        phase_cursor = MagicMock()
        phase_cursor.fetchone.return_value = {}
        analytics_cursor = MagicMock()
        analytics_cursor.fetchone.return_value = {
            "finished_at": datetime(2026, 3, 27, 8, 20, tzinfo=timezone.utc),
            "step_name": "run_tenant_post_refresh",
            "publication_mode": "fast_path",
        }
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [snapshot_cursor, phase_cursor, analytics_cursor]
        mock_conn.__enter__.return_value = mock_conn

        with patch("app.services.snapshot_cache.get_conn", return_value=mock_conn):
            result = snapshot_cache.last_consolidated_sync(tenant_id=1, branch_id=None)

        self.assertTrue(result["available"])
        self.assertEqual(result["source"], "etl_publication_fast_path")
        self.assertEqual(result["analytics"]["source"], "etl_publication_fast_path")
        self.assertEqual(result["analytics"]["mode"], "fast_path")
        self.assertIn("Publicação rápida por tenant", result["message"])

    def test_sales_overview_uses_same_snapshot_policy_as_other_hot_routes(self):
        claims = {"role": "MASTER"}
        expected = {"ok": True}

        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(1, None, None)),
            patch.object(routes_bi, "_with_cached_response", return_value=expected) as with_cached_response,
        ):
            result = routes_bi.sales_overview(
                dt_ini=date(2026, 3, 1),
                dt_fim=date(2026, 3, 10),
                dt_ref=date(2026, 3, 10),
                id_filial=None,
                id_filiais=None,
                id_empresa=1,
                claims=claims,
            )

        self.assertEqual(result, expected)
        self.assertEqual(with_cached_response.call_args.kwargs["scope_key"], "sales_overview")
        safe_fallback = with_cached_response.call_args.kwargs["safe_fallback"]
        self.assertTrue(callable(safe_fallback))

        with patch.object(routes_bi, "_safe_sales_overview_payload", return_value={"reading_status": "preparing"}) as fallback_builder:
            self.assertEqual(safe_fallback(), {"reading_status": "preparing"})

        fallback_builder.assert_called_once_with(
            "MASTER",
            1,
            None,
            date(2026, 3, 1),
            date(2026, 3, 10),
            date(2026, 3, 10),
        )
