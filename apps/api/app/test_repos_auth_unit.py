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
            "/dashboard?dt_ini=2026-03-01&dt_fim=2026-03-28&id_empresa=1&dt_ref=2026-03-28&id_filial=14458",
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
            "/dashboard?dt_ini=2026-03-01&dt_fim=2026-03-28&id_empresa=1&dt_ref=2026-03-28&id_filiais=14458&id_filiais=17337",
        )

    def test_default_product_scope_anchors_on_business_today_while_preserving_latest_operational_date(self):
        with patch(
            "app.repos_auth._load_product_scope_defaults",
            return_value={
                "default_product_scope_days": 1,
                "latest_dt_ref": repos_auth.date(2026, 3, 20),
                "current_date": repos_auth.date(2026, 4, 8),
                "has_operational_data": True,
                "latest_source": "fact_comprovante",
            },
        ), patch("app.repos_auth.business_timezone_name", return_value="America/Sao_Paulo"):
            scope = repos_auth._build_default_product_scope(1, 14458)

        self.assertEqual(scope["dt_ini"], "2026-04-08")
        self.assertEqual(scope["dt_fim"], "2026-04-08")
        self.assertEqual(scope["dt_ref"], "2026-04-08")
        self.assertEqual(scope["source"], "business_today_default")
        self.assertEqual(scope["latest_operational_dt"], "2026-03-20")
        self.assertEqual(scope["latest_source"], "fact_comprovante")

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
