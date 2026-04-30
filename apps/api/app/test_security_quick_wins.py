"""Tests for security fail-fast config and product_global scope fix."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app.config import _validate_production_settings, Settings
from app import scope


class TestConfigFailFast(unittest.TestCase):
    """Validate that production rejects insecure defaults."""

    def test_dev_allows_insecure_defaults(self):
        s = Settings(app_env="dev")
        # Should NOT raise
        _validate_production_settings(s)

    def test_production_rejects_default_jwt_secret(self):
        s = Settings(app_env="production", pg_password="real", clickhouse_password="real", ingest_require_key=True)
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("api_jwt_secret", str(ctx.exception))

    def test_production_rejects_default_pg_password(self):
        s = Settings(app_env="production", api_jwt_secret="real", clickhouse_password="real", ingest_require_key=True)
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("pg_password", str(ctx.exception))

    def test_production_rejects_ingest_key_off(self):
        s = Settings(app_env="production", api_jwt_secret="real", pg_password="real", clickhouse_password="real", ingest_require_key=False)
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("ingest_require_key", str(ctx.exception))

    def test_production_passes_with_all_secure(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="a-proper-secret-32chars-long!!",
            pg_password="s3cur3p@ss",
            clickhouse_password="ch_pass",
            ingest_require_key=True,
        )
        # Should NOT raise
        _validate_production_settings(s)


class TestProductGlobalScope(unittest.TestCase):
    """product_global must not access arbitrary tenants."""

    def _claims(self, tenant_ids=None, id_empresa=None):
        return {
            "user_role": "product_global",
            "accesses": [{"id_empresa": 1, "id_filial": None, "product": True}],
            "id_empresa": id_empresa,
            "id_filial": None,
            "tenant_ids": tenant_ids or [],
            "product": True,
        }

    @patch("app.scope.claims_access_flag", return_value=True)
    @patch("app.scope.normalize_role", return_value="product_global")
    def test_rejects_empty_tenant_ids(self, _nr, _caf):
        claims = self._claims(tenant_ids=[])
        with self.assertRaises(HTTPException) as ctx:
            scope.resolve_scope(claims)
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("tenant_access_missing", str(ctx.exception.detail))

    @patch("app.scope.claims_access_flag", return_value=True)
    @patch("app.scope.normalize_role", return_value="product_global")
    def test_rejects_unauthorized_tenant(self, _nr, _caf):
        claims = self._claims(tenant_ids=[10, 20])
        with self.assertRaises(HTTPException) as ctx:
            scope.resolve_scope(claims, id_empresa_q=99)
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("tenant_access_denied", str(ctx.exception.detail))

    @patch("app.scope.claims_access_flag", return_value=True)
    @patch("app.scope.normalize_role", return_value="product_global")
    def test_allows_authorized_tenant(self, _nr, _caf):
        claims = self._claims(tenant_ids=[10, 20])
        tenant, filial = scope.resolve_scope(claims, id_empresa_q=20)
        self.assertEqual(tenant, 20)
        self.assertIsNone(filial)

    @patch("app.scope.claims_access_flag", return_value=True)
    @patch("app.scope.normalize_role", return_value="product_global")
    def test_fallback_to_first_preferred(self, _nr, _caf):
        claims = self._claims(tenant_ids=[5, 7])
        tenant, filial = scope.resolve_scope(claims)
        self.assertEqual(tenant, 5)


if __name__ == "__main__":
    unittest.main()
