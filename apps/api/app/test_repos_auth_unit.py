import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app import repos_auth
from app.repos_auth import _build_dashboard_home_path


class _FakeQuery:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row=None):
        self.row = row
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return _FakeQuery(self.row)


class ReposAuthUnitTests(unittest.TestCase):
    def test_build_dashboard_home_path_includes_dt_ref_for_single_branch(self):
        path = _build_dashboard_home_path(
            {
                "dt_ini": "2026-03-01",
                "dt_fim": "2026-03-28",
                "dt_ref": "2026-03-28",
                "id_empresa": 1,
                "id_filial": 14458,
                "id_filiais": [14458],
            },
            include_dt_ref=True,
        )

        self.assertEqual(
            path,
            "/sales?dt_ini=2026-03-01&dt_fim=2026-03-28&id_empresa=1&dt_ref=2026-03-28&id_filial=14458",
        )

    def test_build_dashboard_home_path_keeps_multi_branch_query_string_stable(self):
        path = _build_dashboard_home_path(
            {
                "dt_ini": "2026-03-01",
                "dt_fim": "2026-03-28",
                "dt_ref": "2026-03-28",
                "id_empresa": 1,
                "id_filial": None,
                "id_filiais": [14458, 17337],
            },
            include_dt_ref=True,
        )

        self.assertEqual(
            path,
            "/sales?dt_ini=2026-03-01&dt_fim=2026-03-28&id_empresa=1&dt_ref=2026-03-28&id_filiais=14458&id_filiais=17337",
        )

    def test_build_dashboard_home_path_can_omit_server_side_date_defaults(self):
        path = _build_dashboard_home_path(
            {
                "dt_ini": "2026-03-01",
                "dt_fim": "2026-03-28",
                "dt_ref": "2026-03-28",
                "id_empresa": 1,
                "id_filial": 14458,
                "id_filiais": [14458],
            },
            include_dates=False,
        )

        self.assertEqual(path, "/sales?id_empresa=1&id_filial=14458")

    def test_default_product_scope_anchors_on_business_today_while_preserving_latest_operational_date(self):
        with patch(
            "app.repos_auth._load_product_scope_defaults",
            return_value={
                "default_product_scope_days": 1,
                "latest_dt_ref": repos_auth.date(2026, 3, 20),
                "current_date": repos_auth.date(2026, 4, 8),
                "has_operational_data": True,
                "latest_source": "sales_daily_rt",
            },
        ), patch("app.repos_auth.business_timezone_name", return_value="America/Sao_Paulo"):
            scope = repos_auth._build_default_product_scope(1, 14458)

        self.assertEqual(scope["dt_ini"], "2026-04-08")
        self.assertEqual(scope["dt_fim"], "2026-04-08")
        self.assertEqual(scope["dt_ref"], "2026-04-08")
        self.assertEqual(scope["source"], "business_today_default")
        self.assertEqual(scope["latest_operational_dt"], "2026-03-20")
        self.assertEqual(scope["latest_source"], "sales_daily_rt")

    def test_default_product_scope_includes_all_active_branches_for_company_level_scope(self):
        with patch(
            "app.repos_auth._load_product_scope_defaults",
            return_value={
                "default_product_scope_days": 7,
                "latest_dt_ref": repos_auth.date(2026, 5, 11),
                "current_date": repos_auth.date(2026, 5, 11),
                "has_operational_data": True,
                "latest_source": "sales_daily_rt",
            },
        ), patch("app.repos_auth._list_active_branch_ids", return_value=[14458, 17337]), patch(
            "app.repos_auth.business_timezone_name",
            return_value="America/Sao_Paulo",
        ):
            scope = repos_auth._build_default_product_scope(1, None)

        self.assertEqual(scope["id_empresa"], 1)
        self.assertEqual(scope["id_filial"], None)
        self.assertEqual(scope["id_filiais"], [14458, 17337])
        self.assertEqual(scope["branch_scope"], "all")
        self.assertEqual(
            _build_dashboard_home_path(scope, include_dates=False),
            "/sales?id_empresa=1&id_filiais=14458&id_filiais=17337",
        )

    def test_latest_operational_data_key_from_mart_filters_tenant_and_branch(self):
        captured: dict = {}

        def fake_query_dict(sql, parameters=None, tenant_id=None):
            captured["sql"] = sql
            captured["parameters"] = parameters
            captured["tenant_id"] = tenant_id
            return [{"latest_data_key": 20260320}]

        with patch("app.db_clickhouse.query_dict", fake_query_dict):
            key, source = repos_auth._latest_operational_data_key_from_mart(1, 14458)

        self.assertEqual(key, 20260320)
        self.assertEqual(source, "sales_daily_rt")
        self.assertIn("torqmind_mart_rt.sales_daily_rt", captured["sql"])
        self.assertIn("id_filial", captured["sql"])
        self.assertNotIn("dw.fact_", captured["sql"])
        self.assertEqual(captured["parameters"]["id_empresa"], 1)
        self.assertEqual(captured["parameters"]["id_filial"], 14458)
        self.assertEqual(captured["tenant_id"], 1)

    def test_latest_operational_data_key_from_mart_falls_back_on_ch_failure(self):
        with patch("app.db_clickhouse.query_dict", side_effect=RuntimeError("ch down")):
            key, source = repos_auth._latest_operational_data_key_from_mart(1, None)
        self.assertIsNone(key)
        self.assertIsNone(source)

    def test_load_product_scope_defaults_uses_mart_not_dw_facts(self):
        fake_conn = _FakeConn(row={"default_product_scope_days": 3})
        repos_auth._product_scope_cache.clear()

        @contextmanager
        def fake_get_conn(*args, **kwargs):
            yield fake_conn

        with patch("app.repos_auth.get_conn", fake_get_conn), patch(
            "app.repos_auth.business_today",
            return_value=repos_auth.date(2026, 4, 8),
        ), patch(
            "app.repos_auth._latest_operational_data_key_from_mart",
            return_value=(20260320, "sales_daily_rt"),
        ) as mart_mock:
            out = repos_auth._load_product_scope_defaults(1, None)

        self.assertEqual(out["default_product_scope_days"], 3)
        self.assertEqual(out["latest_dt_ref"], repos_auth.date(2026, 3, 20))
        self.assertEqual(out["current_date"], repos_auth.date(2026, 4, 8))
        self.assertTrue(out["has_operational_data"])
        self.assertEqual(out["latest_source"], "sales_daily_rt")
        mart_mock.assert_called_once_with(1, None)
        self.assertEqual(repos_auth.PRODUCT_SCOPE_CACHE_TTL_SECONDS, 300.0)
        for sql, _params in fake_conn.executed:
            self.assertNotIn("dw.fact_", sql)
            self.assertIn("default_product_scope_days", sql)

    def test_load_product_scope_defaults_falls_back_to_business_today_when_mart_empty(self):
        fake_conn = _FakeConn(row={"default_product_scope_days": 1})
        repos_auth._product_scope_cache.clear()

        @contextmanager
        def fake_get_conn(*args, **kwargs):
            yield fake_conn

        with patch("app.repos_auth.get_conn", fake_get_conn), patch(
            "app.repos_auth.business_today",
            return_value=repos_auth.date(2026, 4, 8),
        ), patch(
            "app.repos_auth._latest_operational_data_key_from_mart",
            return_value=(None, None),
        ):
            out = repos_auth._load_product_scope_defaults(9, None)

        self.assertEqual(out["latest_dt_ref"], repos_auth.date(2026, 4, 8))
        self.assertFalse(out["has_operational_data"])
        self.assertIsNone(out["latest_source"])

    def test_email_lookup_uses_parameterized_query(self):
        fake_conn = _FakeConn()

        @contextmanager
        def fake_get_conn(*args, **kwargs):
            yield fake_conn

        injected_identifier = "User@example.com' OR '1'='1"
        with patch("app.repos_auth.get_conn", fake_get_conn):
            repos_auth.get_user_by_identifier(injected_identifier)

        sql, params = fake_conn.executed[0]
        self.assertIn("WHERE lower(email) = %s", sql)
        self.assertNotIn(injected_identifier, sql)
        self.assertEqual(params, (injected_identifier.strip().lower(),))

    def test_username_lookup_uses_parameterized_query_and_lowercase_normalization(self):
        fake_conn = _FakeConn()

        @contextmanager
        def fake_get_conn(*args, **kwargs):
            yield fake_conn

        with patch("app.repos_auth.get_conn", fake_get_conn):
            repos_auth.get_user_by_identifier("Ops.Manager")

        sql, params = fake_conn.executed[0]
        self.assertIn("WHERE username = %s", sql)
        self.assertEqual(params, ("ops.manager",))
