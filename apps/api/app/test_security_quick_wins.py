"""Tests for security fail-fast config and product_global scope fix."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app.config import _is_production_like_env, _is_weak_secret, _validate_production_settings, Settings
from app import scope


class TestWeakSecretDetection(unittest.TestCase):
    """Verify _is_weak_secret catches all placeholder patterns."""

    def test_empty_is_weak(self):
        self.assertTrue(_is_weak_secret(""))
        self.assertTrue(_is_weak_secret("   "))

    def test_change_me_variants(self):
        self.assertTrue(_is_weak_secret("CHANGE_ME_SUPER_SECRET"))
        self.assertTrue(_is_weak_secret("CHANGE_ME_API_JWT_SECRET"))
        self.assertTrue(_is_weak_secret("CHANGE_ME_POSTGRES_PASSWORD"))
        self.assertTrue(_is_weak_secret("CHANGE_ME_CLICKHOUSE_PASSWORD"))
        self.assertTrue(_is_weak_secret("changeme123"))

    def test_common_weak_values(self):
        self.assertTrue(_is_weak_secret("1234"))
        self.assertTrue(_is_weak_secret("password"))
        self.assertTrue(_is_weak_secret("admin"))
        self.assertTrue(_is_weak_secret("postgres"))
        self.assertTrue(_is_weak_secret("default"))

    def test_short_secret_fails_when_min_length_is_required(self):
        self.assertTrue(_is_weak_secret("x" * 31, min_length=32))
        self.assertFalse(_is_weak_secret("x" * 32, min_length=32))

    def test_strong_values(self):
        self.assertFalse(_is_weak_secret("a-proper-secret-32chars-long!!"))
        self.assertFalse(_is_weak_secret("xK9#mP2$vL5nQ8rT"))
        self.assertFalse(_is_weak_secret("torqmind_prod_2026"))


class TestProductionLikeEnv(unittest.TestCase):
    def test_detects_production_like_envs(self):
        for env in ("prod", "production", "homolog", "homologation", "staging"):
            with self.subTest(env=env):
                self.assertTrue(_is_production_like_env(env))

    def test_allows_non_production_envs(self):
        for env in ("dev", "local", "test", None, ""):
            with self.subTest(env=env):
                self.assertFalse(_is_production_like_env(env))


class TestConfigFailFast(unittest.TestCase):
    """Validate that production rejects insecure defaults."""

    def test_dev_allows_insecure_defaults(self):
        s = Settings(app_env="dev")
        _validate_production_settings(s)

    def test_local_allows_insecure_defaults(self):
        s = Settings(app_env="local")
        _validate_production_settings(s)

    def test_test_env_allows_insecure_defaults(self):
        s = Settings(app_env="test")
        _validate_production_settings(s)

    def test_production_rejects_default_jwt_secret(self):
        s = Settings(
            app_env="production",
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("API_JWT_SECRET", str(ctx.exception))

    def test_prod_short_jwt_fails(self):
        s = Settings(
            app_env="prod",
            api_jwt_secret="short-secret",
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("API_JWT_SECRET", str(ctx.exception))

    def test_production_rejects_jwt_of_31_chars(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="x" * 31,
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("API_JWT_SECRET", str(ctx.exception))

    def test_production_rejects_change_me_pg_password(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="A" * 40,
            pg_password="CHANGE_ME_POSTGRES_PASSWORD",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("POSTGRES_PASSWORD", str(ctx.exception))

    def test_production_rejects_postgres_literal_password(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="A" * 40,
            pg_password="postgres",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("POSTGRES_PASSWORD", str(ctx.exception))

    def test_production_rejects_ingest_key_off(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="A" * 40,
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=False,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("INGEST_REQUIRE_KEY", str(ctx.exception))

    def test_homolog_blocks_ingest_require_key_false(self):
        s = Settings(
            app_env="homolog",
            api_jwt_secret="A" * 40,
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_s3cur3-pass!",
            ingest_require_key=False,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("INGEST_REQUIRE_KEY", str(ctx.exception))

    def test_staging_blocks_change_me_clickhouse_password(self):
        s = Settings(
            app_env="staging",
            api_jwt_secret="A" * 40,
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="CHANGE_ME_CLICKHOUSE_PASSWORD",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("CLICKHOUSE_PASSWORD", str(ctx.exception))

    def test_prod_default_ch_user_strong_pass_fails(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="A" * 40,
            pg_password="s3cur3-db-pass!",
            clickhouse_user="default",
            clickhouse_password="str0ng_ch_p@ss!",
            ingest_require_key=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("CLICKHOUSE_USER", str(ctx.exception))

    def test_production_with_32_char_jwt_passes(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="A" * 32,
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_pass_strong!",
            ingest_require_key=True,
        )
        _validate_production_settings(s)

    def test_prod_valid_config_passes(self):
        s = Settings(
            app_env="production",
            api_jwt_secret="prod-jwt-secret-with-32-chars!!OK",
            pg_password="s3cur3-db-pass!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_pass_strong!2026",
            ingest_require_key=True,
        )
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


class TestIngestKeyPolicy(unittest.TestCase):
    """Ingest key uses X-Ingest-Key header exclusively.

    Decision: Option A — single canonical header X-Ingest-Key.
    No aliases (X-TorqMind-Ingest-Key) to avoid confusion.
    Production enforces INGEST_REQUIRE_KEY=true via fail-fast.
    """

    def test_ingest_route_declares_x_ingest_key_header(self):
        """Verify the ingest endpoint declares X-Ingest-Key as the auth header."""
        import inspect
        from app import routes_ingest
        # Find the ingest function signature to confirm header name
        source = inspect.getsource(routes_ingest)
        self.assertIn('alias="X-Ingest-Key"', source)
        # Ensure no alternative alias is declared
        self.assertNotIn('X-TorqMind-Ingest-Key', source)

    def test_fail_fast_enforces_ingest_key_in_production(self):
        """Production config must have ingest_require_key=True."""
        s = Settings(
            app_env="production",
            api_jwt_secret="real-secret-long-enough!",
            pg_password="s3cur3!",
            clickhouse_user="torqmind",
            clickhouse_password="ch_str0ng!",
            ingest_require_key=False,
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_production_settings(s)
        self.assertIn("INGEST_REQUIRE_KEY", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
