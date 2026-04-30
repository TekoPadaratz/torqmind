from __future__ import annotations

import sys
import types
import unittest
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch


try:
    import clickhouse_connect  # noqa: F401
except ModuleNotFoundError:
    fake_clickhouse = types.ModuleType("clickhouse_connect")
    fake_client_module = types.SimpleNamespace(Client=object)
    fake_clickhouse.driver = types.SimpleNamespace(client=fake_client_module)
    fake_clickhouse.get_client = lambda **_kwargs: None
    sys.modules["clickhouse_connect"] = fake_clickhouse

from app import repos_analytics
from app import repos_mart_clickhouse


class AnalyticsFacadeUnitTest(unittest.TestCase):
    def setUp(self) -> None:
        repos_analytics._DISPATCH_CACHE.clear()
        self._use_clickhouse = repos_analytics.settings.use_clickhouse
        self._dual_read = repos_analytics.settings.dual_read_mode

    def tearDown(self) -> None:
        repos_analytics.settings.use_clickhouse = self._use_clickhouse
        repos_analytics.settings.dual_read_mode = self._dual_read
        repos_analytics._DISPATCH_CACHE.clear()

    def test_clickhouse_enabled_dispatches_to_clickhouse_function(self) -> None:
        repos_analytics.settings.use_clickhouse = True
        repos_analytics.settings.dual_read_mode = False

        with patch.object(repos_analytics._clickhouse, "dashboard_kpis", return_value={"source": "ch"}) as ch_call, patch.object(
            repos_analytics._postgres,
            "dashboard_kpis",
            return_value={"source": "pg"},
        ) as pg_call:
            result = repos_analytics.dashboard_kpis("MASTER", 7, None, date(2026, 4, 1), date(2026, 4, 2))

        self.assertEqual(result, {"source": "ch"})
        ch_call.assert_called_once()
        pg_call.assert_not_called()

    def test_clickhouse_disabled_dispatches_to_postgres_fallback(self) -> None:
        repos_analytics.settings.use_clickhouse = False
        repos_analytics.settings.dual_read_mode = False

        with patch.object(repos_analytics._clickhouse, "dashboard_kpis", return_value={"source": "ch"}) as ch_call, patch.object(
            repos_analytics._postgres,
            "dashboard_kpis",
            return_value={"source": "pg"},
        ) as pg_call:
            result = repos_analytics.dashboard_kpis("MASTER", 7, None, date(2026, 4, 1), date(2026, 4, 2))

        self.assertEqual(result, {"source": "pg"})
        ch_call.assert_not_called()
        pg_call.assert_called_once()

    def test_clickhouse_error_is_visible_when_clickhouse_is_enabled(self) -> None:
        repos_analytics.settings.use_clickhouse = True
        repos_analytics.settings.dual_read_mode = False

        with patch.object(repos_analytics._clickhouse, "dashboard_kpis", side_effect=RuntimeError("ch down")), patch.object(
            repos_analytics._postgres,
            "dashboard_kpis",
            return_value={"source": "pg"},
        ) as pg_call:
            with self.assertRaises(RuntimeError):
                repos_analytics.dashboard_kpis("MASTER", 7, None, date(2026, 4, 1), date(2026, 4, 2))

        pg_call.assert_not_called()

    def test_dual_read_calls_both_sources_and_returns_clickhouse_when_enabled(self) -> None:
        repos_analytics.settings.use_clickhouse = True
        repos_analytics.settings.dual_read_mode = True
        validator = Mock()

        with patch.object(repos_analytics._clickhouse, "dashboard_kpis", return_value={"source": "ch"}) as ch_call, patch.object(
            repos_analytics._postgres,
            "dashboard_kpis",
            return_value={"source": "pg"},
        ) as pg_call, patch.object(repos_analytics, "get_dual_read_validator", return_value=validator):
            result = repos_analytics.dashboard_kpis("MASTER", 7, None, date(2026, 4, 1), date(2026, 4, 2))

        self.assertEqual(result, {"source": "ch"})
        ch_call.assert_called_once()
        pg_call.assert_called_once()
        validator.compare.assert_called_once_with("dashboard_kpis", {"source": "pg"}, {"source": "ch"})

    def test_documented_debt_falls_back_to_postgres(self) -> None:
        repos_analytics.settings.use_clickhouse = True
        repos_analytics.settings.dual_read_mode = False

        with patch.object(repos_analytics._postgres, "stock_position_summary", return_value={"source": "pg"}) as pg_call:
            result = repos_analytics.stock_position_summary("MASTER", 7, None)

        self.assertEqual(result, {"source": "pg"})
        pg_call.assert_called_once()

    def test_competitor_pricing_overview_is_postgres_owned_app_flow(self) -> None:
        repos_analytics.settings.use_clickhouse = True
        repos_analytics.settings.dual_read_mode = False

        with patch.object(repos_analytics._postgres, "competitor_pricing_overview", return_value={"source": "pg"}) as pg_call:
            result = repos_analytics.competitor_pricing_overview("MASTER", 7, 14458, date(2026, 4, 1), date(2026, 4, 2))

        self.assertEqual(result, {"source": "pg"})
        pg_call.assert_called_once()

    def test_inventory_counts_only_repository_functions(self) -> None:
        inventory = repos_analytics.analytics_backend_inventory()
        names = {row["function"] for row in inventory["functions"]}

        self.assertEqual(len(names), 68)
        self.assertIn("dashboard_kpis", names)
        self.assertIn("cash_dre_summary", names)
        self.assertNotIn("business_today", names)


class ClickHouseQueryScopeUnitTest(unittest.TestCase):
    def test_clickhouse_epoch_iso_uses_business_timezone_and_ignores_zero(self) -> None:
        epoch = datetime(2026, 4, 29, 12, 24, 57, tzinfo=timezone.utc).timestamp()

        self.assertIsNone(repos_mart_clickhouse._iso_from_clickhouse_epoch(None, 7))
        self.assertIsNone(repos_mart_clickhouse._iso_from_clickhouse_epoch(0, 7))
        self.assertEqual(repos_mart_clickhouse._iso_from_clickhouse_epoch(epoch, 7), "2026-04-29T09:24:57-03:00")

    def test_sales_sync_meta_reads_updated_at_epoch_from_sales_mart(self) -> None:
        epoch = datetime(2026, 4, 29, 12, 24, 57, tzinfo=timezone.utc).timestamp()
        captured = {}

        def fake_query(query, parameters=None, tenant_id=None):
            captured["query"] = query
            captured["parameters"] = parameters
            captured["tenant_id"] = tenant_id
            return [{"row_count": 3, "max_data_key": 20260429, "latest_updated_at_epoch": epoch}]

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            payload = repos_mart_clickhouse._sales_sync_meta("MASTER", 7, 14458, date(2026, 4, 28), date(2026, 4, 29))

        self.assertEqual(payload["last_sync_at"], "2026-04-29T09:24:57-03:00")
        self.assertEqual(payload["snapshot_generated_at"], "2026-04-29T09:24:57-03:00")
        self.assertEqual(payload["dt_ref"], "2026-04-29")
        self.assertEqual(payload["row_count"], 3)
        self.assertIn("toUnixTimestamp(max(updated_at)) AS latest_updated_at_epoch", captured["query"])
        self.assertIn("id_empresa = {id_empresa:Int32}", captured["query"])
        self.assertIn("id_filial = 14458", captured["query"])
        self.assertEqual(captured["parameters"]["id_empresa"], 7)
        self.assertEqual(captured["tenant_id"], 7)

    def test_sales_overview_bundle_keeps_requested_window_when_requested_date_is_outside_coverage(self) -> None:
        expected = {
            "kpis": {"faturamento": 0.0, "margem": 0.0, "ticket_medio": 0.0, "devolucoes": 0.0},
            "commercial_kpis": {"saidas": 0.0, "qtd_saidas": 0, "entradas": 0.0, "qtd_entradas": 0, "cancelamentos": 0.0, "qtd_cancelamentos": 0},
            "by_day": [],
            "by_hour": [],
            "commercial_by_hour": [],
            "cfop_breakdown": [],
            "monthly_evolution": [],
            "annual_comparison": {},
            "top_products": [],
            "top_groups": [],
            "top_employees": [],
            "stats": {"vendas": 0},
            "operational_sync": {"last_sync_at": None, "snapshot_generated_at": None},
            "freshness": {"mode": "historical_snapshot", "source": "torqmind_mart.agg_vendas_diaria"},
        }

        with patch.object(
            repos_mart_clickhouse,
            "commercial_window_coverage",
            return_value={
                "mode": "requested_outside_coverage",
                "effective_dt_ini": date(2026, 4, 30),
                "effective_dt_fim": date(2026, 4, 30),
            },
        ), patch.object(
            repos_mart_clickhouse,
            "_sales_historical_bundle_from_marts",
            return_value=expected,
        ) as historical_bundle, patch.object(
            repos_mart_clickhouse,
            "sales_commercial_overview",
            return_value={
                "kpis": expected["commercial_kpis"],
                "cfop_breakdown": expected["cfop_breakdown"],
                "by_hour": expected["commercial_by_hour"],
                "monthly_evolution": expected["monthly_evolution"],
                "annual_comparison": expected["annual_comparison"],
            },
        ):
            payload = repos_mart_clickhouse.sales_overview_bundle(
                "MASTER",
                1,
                14458,
                date(2026, 4, 30),
                date(2026, 4, 30),
                as_of=date(2026, 4, 30),
            )

        historical_bundle.assert_called_once_with(
            "MASTER",
            1,
            14458,
            date(2026, 4, 30),
            date(2026, 4, 30),
            include_details=True,
        )
        self.assertEqual(payload["reading_status"], "unavailable_for_requested_window")
        self.assertEqual(payload["freshness"]["historical_through_dt"], "2026-04-30")

    def test_cash_live_now_does_not_return_epoch_zero_as_1970(self) -> None:
        responses = [
            [{"caixas_abertos_fonte": 0, "caixas_abertos": 0, "caixas_stale": 0, "snapshot_epoch": 0, "latest_activity_epoch": 0}],
            [],
            [],
            [],
        ]

        def fake_query(query, parameters=None, tenant_id=None):
            return responses.pop(0)

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            payload = repos_mart_clickhouse._cash_live_now("MASTER", 7, None)

        self.assertIsNone(payload["operational_sync"]["last_sync_at"])
        self.assertIsNone(payload["operational_sync"]["snapshot_generated_at"])
        self.assertIsNone(payload["freshness"]["live_through_at"])
        self.assertIsNone(payload["freshness"]["snapshot_generated_at"])
        self.assertNotIn("1970-01-01T00:00:00", str(payload))

    def test_dashboard_home_bundle_uses_sales_sync_when_sales_mart_is_published(self) -> None:
        sales_sync = {
            "last_sync_at": "2026-04-29T09:24:57-03:00",
            "snapshot_generated_at": "2026-04-29T09:24:57-03:00",
            "source": "torqmind_mart.agg_vendas_diaria",
            "dt_ref": "2026-04-29",
        }
        sales_payload = {
            "operational_sync": sales_sync,
            "freshness": {"live_through_at": sales_sync["last_sync_at"], "snapshot_generated_at": sales_sync["snapshot_generated_at"]},
            "commercial_coverage": {"mode": "exact", "latest_available_dt": date(2026, 4, 29)},
        }
        cash_payload = {
            "operational_sync": {"last_sync_at": None, "snapshot_generated_at": None, "source": "torqmind_mart.agg_caixa_turno_aberto"},
            "freshness": {"live_through_at": None, "snapshot_generated_at": None},
        }
        coverage = {
            "mode": "exact",
            "latest_available_dt": date(2026, 4, 29),
            "effective_dt_ini": date(2026, 4, 28),
            "effective_dt_fim": date(2026, 4, 29),
            "requested_has_coverage": True,
        }

        with (
            patch.object(repos_mart_clickhouse, "commercial_window_coverage", return_value=coverage),
            patch.object(repos_mart_clickhouse, "sales_overview_bundle", return_value=sales_payload),
            patch.object(repos_mart_clickhouse, "sales_peak_hours_signal", return_value={}),
            patch.object(repos_mart_clickhouse, "sales_declining_products_signal", return_value={}),
            patch.object(repos_mart_clickhouse, "fraud_kpis", return_value={}),
            patch.object(repos_mart_clickhouse, "fraud_data_window", return_value={}),
            patch.object(repos_mart_clickhouse, "risk_kpis", return_value={}),
            patch.object(repos_mart_clickhouse, "risk_data_window", return_value={}),
            patch.object(repos_mart_clickhouse, "customers_churn_bundle", return_value={}),
            patch.object(repos_mart_clickhouse, "finance_aging_overview", return_value={}),
            patch.object(repos_mart_clickhouse, "_cash_live_now", return_value=cash_payload),
            patch.object(repos_mart_clickhouse, "payments_overview", return_value={}),
            patch.object(repos_mart_clickhouse, "jarvis_briefing", return_value={}),
            patch.object(repos_mart_clickhouse, "health_score_latest", return_value={}),
            patch("app.repos_mart.notifications_unread_count", return_value=0),
        ):
            payload = repos_mart_clickhouse.dashboard_home_bundle(
                "MASTER",
                7,
                14458,
                date(2026, 4, 28),
                date(2026, 4, 29),
                date(2026, 4, 29),
            )

        self.assertEqual(payload["operational_sync"]["last_sync_at"], "2026-04-29T09:24:57-03:00")
        self.assertEqual(payload["freshness"]["live_through_at"], "2026-04-29T09:24:57-03:00")
        self.assertEqual(payload["commercial_coverage"]["mode"], "exact")

    def test_health_score_latest_query_does_not_aggregate_dt_ref_alias(self) -> None:
        captured = {}

        def fake_query(query, parameters=None, tenant_id=None):
            captured.setdefault("queries", []).append(query)
            if "min(dt_ref) AS coverage_start_dt_ref" in query:
                return [{"effective_dt_ref": date(2026, 4, 29), "has_exact": True, "row_count": 1}]
            return [{"dt_ref": date(2026, 4, 29), "score_total": 80}]

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            payload = repos_mart_clickhouse.health_score_latest("MASTER", 7, None, as_of=date(2026, 4, 29))

        score_query = captured["queries"][-1]
        self.assertNotIn("max(dt_ref) AS dt_ref", score_query)
        self.assertIn("dt_ref = {dt_ref:Date}", score_query)
        self.assertEqual(payload["dt_ref"], date(2026, 4, 29))

    def test_dashboard_kpis_query_filters_tenant_and_branch_list(self) -> None:
        captured = {}

        def fake_query(query, parameters=None, tenant_id=None):
            captured["query"] = query
            captured["parameters"] = parameters
            captured["tenant_id"] = tenant_id
            return [{"faturamento": 100, "margem": 20, "itens": 4, "ticket_medio": 25}]

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            payload = repos_mart_clickhouse.dashboard_kpis("MASTER", 7, [11, 12], date(2026, 4, 1), date(2026, 4, 2))

        self.assertEqual(payload["faturamento"], 100.0)
        self.assertIn("id_empresa = {id_empresa:Int32}", captured["query"])
        self.assertIn("id_filial IN (11, 12)", captured["query"])
        self.assertEqual(captured["parameters"]["id_empresa"], 7)
        self.assertEqual(captured["tenant_id"], 7)

    def test_minus_one_branch_scope_does_not_add_filial_filter(self) -> None:
        captured = {}

        def fake_query(query, parameters=None, tenant_id=None):
            captured["query"] = query
            return [{"faturamento": 0, "margem": 0, "itens": 0, "ticket_medio": 0}]

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            repos_mart_clickhouse.dashboard_kpis("MASTER", 7, -1, date(2026, 4, 1), date(2026, 4, 2))

        self.assertIn("id_empresa = {id_empresa:Int32}", captured["query"])
        self.assertNotIn("id_filial =", captured["query"])
        self.assertNotIn("id_filial IN", captured["query"])

    def test_risk_last_events_reads_event_type_from_recent_events_view(self) -> None:
        captured = {}

        def fake_query(query, parameters=None, tenant_id=None):
            captured["query"] = query
            captured["parameters"] = parameters
            captured["tenant_id"] = tenant_id
            return []

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            rows = repos_mart_clickhouse.risk_last_events("MASTER", 7, None, date(2026, 4, 1), date(2026, 4, 2), limit=5)

        self.assertEqual(rows, [])
        self.assertIn("FROM torqmind_mart.risco_eventos_recentes", captured["query"])
        self.assertIn("event_type", captured["query"])
        self.assertIn("filial_nome", captured["query"])
        self.assertIn("operador_caixa_nome", captured["query"])
        self.assertIn("LIMIT {limit:UInt32}", captured["query"])
        self.assertEqual(captured["parameters"]["id_empresa"], 7)
        self.assertEqual(captured["tenant_id"], 7)

    def test_risk_last_events_applies_branch_filter_against_recent_events_view(self) -> None:
        captured = {}

        def fake_query(query, parameters=None, tenant_id=None):
            captured["query"] = query
            captured["parameters"] = parameters
            captured["tenant_id"] = tenant_id
            return []

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            rows = repos_mart_clickhouse.risk_last_events("MASTER", 7, [11, 12], date(2026, 4, 1), date(2026, 4, 2), limit=5)

        self.assertEqual(rows, [])
        self.assertIn("id_filial IN (11, 12)", captured["query"])
        self.assertIn("FROM torqmind_mart.risco_eventos_recentes", captured["query"])
        self.assertEqual(captured["tenant_id"], 7)

    def test_risk_last_events_preserves_human_operator_and_branch_labels(self) -> None:
        def fake_query(query, parameters=None, tenant_id=None):
            return [
                {
                    "id": 1,
                    "id_filial": 14458,
                    "filial_nome": "Posto Central",
                    "data_key": 20260429,
                    "data": datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
                    "event_type": "CANCELAMENTO_CAIXA",
                    "id_db": 1,
                    "id_comprovante": "100",
                    "id_movprodutos": 100,
                    "id_usuario": 9,
                    "id_funcionario": None,
                    "funcionario_nome": "",
                    "id_turno": 2,
                    "turno_value": "2",
                    "operador_caixa_id": 9,
                    "operador_caixa_nome": "Maria Caixa",
                    "operador_caixa_source": "turno",
                    "id_cliente": None,
                    "valor_total": 50,
                    "impacto_estimado": 50,
                    "score_risco": 85,
                    "score_level": "HIGH",
                    "reasons": "{}",
                }
            ]

        with patch.object(repos_mart_clickhouse, "query_dict", side_effect=fake_query):
            rows = repos_mart_clickhouse.risk_last_events("MASTER", 7, None, date(2026, 4, 29), date(2026, 4, 29), limit=5)

        self.assertEqual(rows[0]["filial_label"], "Posto Central")
        self.assertEqual(rows[0]["operador_caixa_label"], "Maria Caixa")
        self.assertEqual(rows[0]["responsavel_label"], "Maria Caixa")
        self.assertEqual(rows[0]["responsavel_kind"], "operador_caixa")

    def test_cash_dre_summary_uses_finance_mart_and_never_epoch_zero(self) -> None:
        with patch.object(
            repos_mart_clickhouse,
            "finance_aging_overview",
            return_value={
                "dt_ref": date(2026, 4, 29),
                "snapshot_rows": 1,
                "pagar_total_aberto": 1000,
                "pagar_total_vencido": 250,
                "receber_total_aberto": 2200,
            },
        ):
            payload = repos_mart_clickhouse.cash_dre_summary("MASTER", 7, None, date(2026, 4, 29))

        self.assertEqual(payload["source_status"], "ok")
        self.assertEqual(payload["dt_ref"], "2026-04-29")
        self.assertEqual(payload["cards"][0]["amount"], 750.0)
        self.assertEqual(payload["cards"][2]["amount"], 1450.0)
        self.assertNotIn("1970-01-01", str(payload))

        with patch.object(repos_mart_clickhouse, "finance_aging_overview", return_value={"snapshot_rows": 0, "dt_ref": date(1970, 1, 1)}):
            empty_payload = repos_mart_clickhouse.cash_dre_summary("MASTER", 7, None, date(2026, 4, 29))

        self.assertIsNone(empty_payload["dt_ref"])
        self.assertNotIn("1970-01-01", str(empty_payload))


if __name__ == "__main__":
    unittest.main()
