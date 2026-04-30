from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_claims
from app.main import app
from app import routes_bi


class BranchScopeRouteUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def setUp(self) -> None:
        app.dependency_overrides[get_current_claims] = lambda: {"role": "OWNER"}

    def tearDown(self) -> None:
        app.dependency_overrides.pop(get_current_claims, None)

    def _run_cached_route(self, path: str) -> dict:
        with patch.object(routes_bi, "_with_cached_response", side_effect=lambda *args, **kwargs: kwargs["compute"]()):
            response = self.client.get(path)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_dashboard_home_passes_multi_branch_scope_into_bundle(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(7, [11, 13], [11, 13])),
            patch.object(routes_bi.repos_mart, "dashboard_home_bundle", return_value={"ok": True}) as dashboard_home_bundle,
        ):
            body = self._run_cached_route("/bi/dashboard/home?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa=7&id_filiais=11&id_filiais=13")

        self.assertTrue(body["ok"])
        self.assertEqual(dashboard_home_bundle.call_args.args[2], [11, 13])

    def test_sales_overview_passes_multi_branch_scope_into_bundle(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(7, [11, 13], [11, 13])),
            patch.object(routes_bi.repos_mart, "sales_overview_bundle", return_value={"ok": True}) as sales_overview_bundle,
        ):
            body = self._run_cached_route("/bi/sales/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa=7&id_filiais=11&id_filiais=13")

        self.assertTrue(body["ok"])
        self.assertEqual(sales_overview_bundle.call_args.args[2], [11, 13])

    def test_cash_overview_passes_multi_branch_scope_into_repo(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(7, [11, 13], [11, 13])),
            patch.object(routes_bi.repos_mart, "cash_overview", return_value={"ok": True}) as cash_overview,
        ):
            body = self._run_cached_route("/bi/cash/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa=7&id_filiais=11&id_filiais=13")

        self.assertTrue(body["ok"])
        self.assertEqual(cash_overview.call_args.args[2], [11, 13])

    def test_fraud_overview_passes_multi_branch_scope_into_operational_and_risk_repos(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(7, [11, 13], [11, 13])),
            patch.object(routes_bi.repos_mart, "fraud_kpis", return_value={"cancelamentos": 0, "valor_cancelado": 0}) as fraud_kpis,
            patch.object(routes_bi.repos_mart, "fraud_series", return_value=[]),
            patch.object(routes_bi.repos_mart, "fraud_top_users", return_value=[]),
            patch.object(routes_bi.repos_mart, "fraud_last_events", return_value=[]),
            patch.object(routes_bi.repos_mart, "risk_kpis", return_value={"total_eventos": 0}),
            patch.object(routes_bi.repos_mart, "risk_series", return_value=[]),
            patch.object(routes_bi.repos_mart, "risk_data_window", return_value={"rows": 0}),
            patch.object(routes_bi.repos_mart, "risk_model_coverage", return_value={"coverage": "ok"}),
            patch.object(routes_bi.repos_mart, "risk_top_employees", return_value=[]),
            patch.object(routes_bi.repos_mart, "risk_by_turn_local", return_value=[]),
            patch.object(routes_bi.repos_mart, "risk_last_events", return_value=[]),
            patch.object(routes_bi.repos_mart, "payments_anomalies", return_value=[]),
            patch.object(routes_bi.repos_mart, "open_cash_monitor", return_value={"source_status": "ok"}) as open_cash_monitor,
            patch.object(routes_bi.repos_mart, "risk_insights", return_value=[]),
        ):
            body = self._run_cached_route("/bi/fraud/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa=7&id_filiais=11&id_filiais=13")

        self.assertEqual(body["kpis"]["cancelamentos"], 0)
        self.assertEqual(fraud_kpis.call_args.args[2], [11, 13])
        self.assertEqual(open_cash_monitor.call_args.args[2], [11, 13])

    def test_cash_overview_all_branches_uses_active_scope_returned_by_resolver(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(7, [11, 13], None)),
            patch.object(routes_bi.repos_mart, "cash_overview", return_value={"ok": True}) as cash_overview,
        ):
            body = self._run_cached_route("/bi/cash/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa=7")

        self.assertTrue(body["ok"])
        self.assertEqual(cash_overview.call_args.args[2], [11, 13])


if __name__ == "__main__":
    unittest.main()
