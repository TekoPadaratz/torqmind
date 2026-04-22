from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from app import routes_bi


def _direct_cached_response(*_args, **kwargs):
    return kwargs["compute"]()


class RoutesBiCommercialCoverageUnitTest(unittest.TestCase):
    @patch("app.routes_bi._with_cached_response", side_effect=_direct_cached_response)
    @patch("app.routes_bi.resolve_business_date", return_value=date(2026, 4, 22))
    @patch("app.routes_bi.resolve_scope_filters", return_value=(1, None, None))
    @patch("app.routes_bi.repos_mart.anonymous_retention_overview", return_value={"kpis": {}})
    @patch("app.routes_bi.repos_mart.customers_churn_snapshot_meta", return_value={"snapshot_status": "exact"})
    @patch("app.routes_bi.repos_mart.customers_churn_bundle", return_value={"top_risk": [], "snapshot_meta": {"snapshot_status": "exact"}})
    @patch("app.routes_bi.repos_mart.customers_delinquency_overview", return_value={"summary": {}, "buckets": [], "customers": []})
    @patch("app.routes_bi.repos_mart.customers_rfm_snapshot", return_value={"clientes_identificados": 10})
    @patch("app.routes_bi.repos_mart.customers_top", return_value=[{"id_cliente": 1}])
    @patch(
        "app.routes_bi.repos_mart.commercial_window_coverage",
        return_value={
            "mode": "shifted_latest",
            "effective_dt_ini": date(2026, 3, 1),
            "effective_dt_fim": date(2026, 3, 31),
            "latest_available_dt": date(2026, 3, 31),
        },
    )
    def test_customers_overview_uses_effective_commercial_window(
        self,
        mock_coverage,
        mock_top_customers,
        _mock_rfm,
        _mock_delinquency,
        _mock_churn_bundle,
        _mock_churn_snapshot,
        mock_anonymous_retention,
        _mock_scope,
        _mock_business_date,
        _mock_cache,
    ) -> None:
        payload = routes_bi.customers_overview(
            dt_ini=date(2026, 4, 1),
            dt_fim=date(2026, 4, 22),
            dt_ref=date(2026, 4, 22),
            claims={"role": "MASTER"},
        )

        mock_coverage.assert_called_once_with("MASTER", 1, None, date(2026, 4, 1), date(2026, 4, 22))
        mock_top_customers.assert_called_once_with("MASTER", 1, None, date(2026, 3, 1), date(2026, 3, 31), limit=15)
        mock_anonymous_retention.assert_called_once_with("MASTER", 1, None, date(2026, 3, 1), date(2026, 3, 31))
        self.assertEqual(payload["commercial_coverage"]["mode"], "shifted_latest")

    @patch("app.routes_bi._with_cached_response", side_effect=_direct_cached_response)
    @patch("app.routes_bi.resolve_business_date", return_value=date(2026, 4, 22))
    @patch("app.routes_bi.resolve_scope_filters", return_value=(1, 7, 7))
    @patch("app.routes_bi.repos_mart.monthly_goal_projection", return_value={"status": "latest_compatible"})
    @patch("app.routes_bi.repos_mart.risk_top_employees", return_value=[])
    @patch("app.routes_bi.repos_mart.goals_today", return_value=[])
    @patch("app.routes_bi.repos_mart.leaderboard_employees", return_value=[{"id_funcionario": 10}])
    @patch("app.routes_bi.business_clock_payload", return_value={"business_date": "2026-04-22"})
    @patch(
        "app.routes_bi.repos_mart.commercial_window_coverage",
        return_value={
            "mode": "shifted_latest",
            "effective_dt_ini": date(2026, 3, 16),
            "effective_dt_fim": date(2026, 3, 31),
            "latest_available_dt": date(2026, 3, 31),
        },
    )
    def test_goals_overview_uses_effective_commercial_window_for_leaderboard(
        self,
        mock_coverage,
        _mock_business_clock,
        mock_leaderboard,
        _mock_goals_today,
        _mock_risk_top,
        _mock_projection,
        _mock_scope,
        _mock_business_date,
        _mock_cache,
    ) -> None:
        payload = routes_bi.goals_overview(
            dt_ini=date(2026, 4, 16),
            dt_fim=date(2026, 4, 22),
            dt_ref=date(2026, 4, 22),
            claims={"role": "MASTER"},
        )

        mock_coverage.assert_called_once_with("MASTER", 1, 7, date(2026, 4, 16), date(2026, 4, 22))
        mock_leaderboard.assert_called_once_with("MASTER", 1, 7, date(2026, 3, 16), date(2026, 3, 31), limit=15)
        self.assertEqual(payload["commercial_coverage"]["effective_dt_fim"], date(2026, 3, 31))


if __name__ == "__main__":
    unittest.main()
