"""Unit tests for finance despesas ACL."""
from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from app.deps import get_current_claims
from app.main import app
from app.permissions import can_access_screen


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


if __name__ == "__main__":
    unittest.main()
