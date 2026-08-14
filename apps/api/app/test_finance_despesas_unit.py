"""Unit tests for finance despesas ACL + search aggregation SQL."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_claims
from app.main import app
from app.permissions import can_access_screen
from app import repos_mart_realtime as rt


def _claims(screens):
    return {
        "sub": "00000000-0000-0000-0000-000000000001",
        "email": "manager@test.com",
        "user_role": "tenant_manager",
        "role": "tenant_manager",
        "id_empresa": 1,
        "id_filial": 14458,
        "allowed_screens": screens,
        "can_view_sensitive_financials": False,
        "access": {"product": True, "platform": False},
    }


class TestFinanceDespesasAcl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_panel_acl_grants_despesas(self):
        self.assertTrue(can_access_screen(_claims(["finance.despesas"]), "finance.despesas"))
        self.assertFalse(can_access_screen(_claims(["finance.payable"]), "finance.despesas"))

    def test_despesas_requires_screen(self):
        app.dependency_overrides[get_current_claims] = lambda: _claims(["finance.payable"])
        resp = self.client.get("/bi/finance/despesas?ano=2026&mes=7")
        self.assertEqual(resp.status_code, 403)


class TestFinanceDespesasSearchSql(unittest.TestCase):
    def test_summary_search_uses_subquery_not_alias_collision(self):
        """Busca no summary não pode filtrar pelo alias any(nome_plano) (CH 184)."""
        captured: list[str] = []

        def fake_query_dict(sql, parameters=None):
            captured.append(sql)
            return []

        with patch.object(rt, "query_dict", side_effect=fake_query_dict):
            out = rt.finance_despesas_overview(
                role="platform_master",
                id_empresa=1,
                id_filial=None,
                ano=2026,
                mes=7,
                q="energia",
            )
        self.assertEqual(out["mode"], "summary")
        self.assertEqual(out["ano_mes"], 202607)
        self.assertEqual(len(captured), 1)
        sql = captured[0]
        self.assertIn("FROM (", sql)
        self.assertIn("positionCaseInsensitiveUTF8", sql)
        self.assertIn("concat(", sql)
        # WHERE da busca fica na subquery; outer só agrega.
        where_idx = sql.find("WHERE")
        group_idx = sql.find("GROUP BY")
        self.assertTrue(0 <= where_idx < group_idx)
        self.assertIn("status IN ('entrada', 'saida')", sql)


if __name__ == "__main__":
    unittest.main()
