from __future__ import annotations

import sys
import types
import unittest
from datetime import date
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

    def test_inventory_counts_only_repository_functions(self) -> None:
        inventory = repos_analytics.analytics_backend_inventory()
        names = {row["function"] for row in inventory["functions"]}

        self.assertEqual(len(names), 68)
        self.assertIn("dashboard_kpis", names)
        self.assertIn("cash_dre_summary", names)
        self.assertNotIn("business_today", names)


class ClickHouseQueryScopeUnitTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
