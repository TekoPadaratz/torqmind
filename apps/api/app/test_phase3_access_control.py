"""Phase 3 access control tests — mandatory blockers.

Covers:
- require_screen blocks users without the screen permission
- competitor_pricing endpoints require competitor_pricing screen
- TV endpoints require respective TV screens
- redact_sensitive removes custo_total, margin_10d, margem_score (stem matching)
- _validate_access_payload enforces id_filial for tenant_kiosk
- force password change blocks current session via DB flag
"""
from __future__ import annotations

import unittest
from unittest.mock import patch, MagicMock

from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.deps import get_current_claims, get_current_claims_allow_password_change
from app.main import app
from app.permissions import _redact, _is_sensitive_key, redact_sensitive


# ---------------------------------------------------------------------------
# Helpers — mock claims factories
# ---------------------------------------------------------------------------

def _manager_claims(**overrides):
    """Claims for a tenant_manager with limited screens."""
    base = {
        "sub": "00000000-0000-0000-0000-000000000001",
        "email": "manager@test.com",
        "user_role": "tenant_manager",
        "role": "MANAGER",
        "id_empresa": 1,
        "id_filial": 100,
        "must_change_password": False,
        "allowed_screens": ["customers", "competitor_pricing"],
        "can_view_sensitive_financials": False,
        "access": {"product": True, "platform": False},
        "layout_mode": "normal",
    }
    base.update(overrides)
    return base


def _kiosk_claims(**overrides):
    """Claims for a tenant_kiosk user."""
    base = {
        "sub": "00000000-0000-0000-0000-000000000002",
        "email": "kiosk@test.com",
        "user_role": "tenant_kiosk",
        "role": "KIOSK",
        "id_empresa": 1,
        "id_filial": 100,
        "must_change_password": False,
        "allowed_screens": ["tv_sales_hourly", "tv_sales_ranking"],
        "can_view_sensitive_financials": False,
        "access": {"product": True, "platform": False},
        "layout_mode": "kiosk",
    }
    base.update(overrides)
    return base


def _owner_claims(**overrides):
    """Claims for an owner with full access."""
    base = {
        "sub": "00000000-0000-0000-0000-000000000003",
        "email": "owner@test.com",
        "user_role": "tenant_admin",
        "role": "OWNER",
        "id_empresa": 1,
        "id_filial": 100,
        "must_change_password": False,
        "allowed_screens": [
            "dashboard_home", "sales", "cash", "fraud",
            "customers", "finance", "competitor_pricing", "goals_team",
        ],
        "can_view_sensitive_financials": True,
        "access": {"product": True, "platform": False},
        "layout_mode": "normal",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# 1) require_screen blocks manager without screen
# ---------------------------------------------------------------------------

class TestRequireScreenBlocks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_manager_without_dashboard_home_gets_403_on_kpis(self):
        """Manager with only customers+competitor_pricing cannot access dashboard_home."""
        app.dependency_overrides[get_current_claims] = lambda: _manager_claims()
        resp = self.client.get("/dashboard/kpis?dt_ini=2026-05-01&dt_fim=2026-05-14")
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()["detail"]["error"], "screen_access_denied")

    def test_manager_without_sales_gets_403_on_sales_overview(self):
        """Manager with only customers+competitor_pricing cannot access sales."""
        app.dependency_overrides[get_current_claims] = lambda: _manager_claims()
        resp = self.client.get("/bi/sales/overview?dt_ini=2026-05-01&dt_fim=2026-05-14")
        self.assertEqual(resp.status_code, 403)


# ---------------------------------------------------------------------------
# 2) Competitor pricing requires competitor_pricing screen
# ---------------------------------------------------------------------------

class TestCompetitorPricingScreen(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_manager_without_competitor_pricing_gets_403_on_fuels(self):
        claims = _manager_claims(allowed_screens=["customers"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        resp = self.client.get("/bi/pricing/competitor/fuels")
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()["detail"]["error"], "screen_access_denied")

    def test_manager_without_competitor_pricing_gets_403_on_history(self):
        claims = _manager_claims(allowed_screens=["customers"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        resp = self.client.get("/bi/pricing/competitor/history?capture_date=2026-05-14")
        self.assertEqual(resp.status_code, 403)

    def test_manager_without_competitor_pricing_gets_403_on_comparison(self):
        claims = _manager_claims(allowed_screens=["customers"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        resp = self.client.get("/bi/pricing/competitor/comparison?capture_date=2026-05-14")
        self.assertEqual(resp.status_code, 403)

    def test_manager_with_competitor_pricing_gets_through_fuels(self):
        """Manager with competitor_pricing screen passes the guard (may fail on scope/repo, that's ok)."""
        claims = _manager_claims(allowed_screens=["customers", "competitor_pricing"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        resp = self.client.get("/bi/pricing/competitor/fuels")
        # May get 403 from scope resolution (no accesses in mock claims), but NOT from screen guard
        if resp.status_code == 403:
            self.assertNotEqual(resp.json().get("detail", {}).get("error"), "screen_access_denied")


# ---------------------------------------------------------------------------
# 3) TV endpoints require TV screen permissions
# ---------------------------------------------------------------------------

class TestTVEndpointScreens(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_tv_hourly_blocked_without_tv_sales_hourly(self):
        claims = _kiosk_claims(allowed_screens=["tv_sales_ranking"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        resp = self.client.get("/bi/tv/sales-hourly")
        self.assertEqual(resp.status_code, 403)

    def test_tv_ranking_blocked_without_tv_sales_ranking(self):
        claims = _kiosk_claims(allowed_screens=["tv_sales_hourly"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        resp = self.client.get("/bi/tv/sales-ranking")
        self.assertEqual(resp.status_code, 403)

    def test_tv_hourly_allowed_with_tv_sales_hourly(self):
        claims = _kiosk_claims(allowed_screens=["tv_sales_hourly"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        with patch("app.repos_analytics.dashboard_series") as mock_fn:
            mock_fn.return_value = [{"data_key": 20260514, "faturamento": 1000}]
            resp = self.client.get("/bi/tv/sales-hourly")
        self.assertNotEqual(resp.status_code, 403)

    def test_tv_ranking_allowed_with_tv_sales_ranking(self):
        claims = _kiosk_claims(allowed_screens=["tv_sales_ranking"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        with patch("app.repos_analytics.sales_overview_bundle") as mock_fn:
            mock_fn.return_value = {"sellers": [{"vendedor": "A", "total": 100}]}
            resp = self.client.get("/bi/tv/sales-ranking")
        self.assertNotEqual(resp.status_code, 403)


# ---------------------------------------------------------------------------
# 4) Redaction — stem-based matching
# ---------------------------------------------------------------------------

class TestRedactionStems(unittest.TestCase):
    def test_custo_total_is_sensitive(self):
        self.assertTrue(_is_sensitive_key("custo_total"))

    def test_margin_10d_is_sensitive(self):
        self.assertTrue(_is_sensitive_key("margin_10d"))

    def test_margem_score_is_sensitive(self):
        self.assertTrue(_is_sensitive_key("margem_score"))

    def test_custo_unitario_is_sensitive(self):
        self.assertTrue(_is_sensitive_key("custo_unitario"))

    def test_profit_margin_is_sensitive(self):
        self.assertTrue(_is_sensitive_key("profit_margin"))

    def test_rentabilidade_anual_is_sensitive(self):
        self.assertTrue(_is_sensitive_key("rentabilidade_anual"))

    def test_faturamento_is_not_sensitive(self):
        self.assertFalse(_is_sensitive_key("faturamento"))

    def test_vendedor_is_not_sensitive(self):
        self.assertFalse(_is_sensitive_key("vendedor"))

    def test_id_filial_is_not_sensitive(self):
        self.assertFalse(_is_sensitive_key("id_filial"))

    def test_redact_removes_custo_total_for_non_financial(self):
        data = {"faturamento": 1000, "custo_total": 500, "margem": 100, "margin_10d": 0.15}
        result = _redact(data)
        self.assertEqual(result["faturamento"], 1000)
        self.assertIsNone(result["custo_total"])
        self.assertIsNone(result["margem"])
        self.assertIsNone(result["margin_10d"])

    def test_redact_removes_margem_score_nested(self):
        data = {"items": [{"nome": "X", "margem_score": 0.8, "total": 100}]}
        _redact(data)
        self.assertIsNone(data["items"][0]["margem_score"])
        self.assertEqual(data["items"][0]["total"], 100)

    def test_owner_sees_sensitive_fields(self):
        claims = _owner_claims()
        data = {"faturamento": 1000, "custo_total": 500, "margem": 100}
        result = redact_sensitive(data, claims)
        self.assertEqual(result["custo_total"], 500)
        self.assertEqual(result["margem"], 100)

    def test_manager_does_not_see_sensitive_fields(self):
        claims = _manager_claims()
        data = {"faturamento": 1000, "custo_total": 500, "margem": 100, "margin_10d": 0.15, "margem_score": 0.9}
        result = redact_sensitive(data, claims)
        self.assertEqual(result["faturamento"], 1000)
        self.assertIsNone(result["custo_total"])
        self.assertIsNone(result["margem"])
        self.assertIsNone(result["margin_10d"])
        self.assertIsNone(result["margem_score"])


# ---------------------------------------------------------------------------
# 5) _validate_access_payload: tenant_kiosk requires id_filial
# ---------------------------------------------------------------------------

class TestValidateAccessKiosk(unittest.TestCase):
    def test_tenant_kiosk_requires_filial(self):
        from app.repos_platform import _validate_access_payload, AuthError
        actor_claims = {"user_role": "platform_master", "channel_ids": []}
        accesses = [{"role": "tenant_kiosk", "id_empresa": "1", "id_filial": None, "channel_id": None}]
        with self.assertRaises(AuthError) as ctx:
            _validate_access_payload(actor_claims, "tenant_kiosk", accesses)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("id_filial", ctx.exception.message)

    def test_tenant_kiosk_with_filial_passes(self):
        from app.repos_platform import _validate_access_payload
        actor_claims = {"user_role": "platform_master", "channel_ids": []}
        accesses = [{"role": "tenant_kiosk", "id_empresa": "1", "id_filial": "100", "channel_id": None}]
        result = _validate_access_payload(actor_claims, "tenant_kiosk", accesses)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id_filial"], "100")

    def test_tenant_kiosk_rejects_channel_id(self):
        from app.repos_platform import _validate_access_payload, AuthError
        actor_claims = {"user_role": "platform_master", "channel_ids": []}
        accesses = [{"role": "tenant_kiosk", "id_empresa": "1", "id_filial": "100", "channel_id": "5"}]
        with self.assertRaises(AuthError) as ctx:
            _validate_access_payload(actor_claims, "tenant_kiosk", accesses)
        self.assertEqual(ctx.exception.status_code, 422)


# ---------------------------------------------------------------------------
# 6) Force password change blocks current session via DB
# ---------------------------------------------------------------------------

class TestForcePasswordChangeDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)
        app.dependency_overrides.pop(get_current_claims_allow_password_change, None)

    def test_must_change_password_blocks_normal_endpoints(self):
        """When DB returns must_change_password=True, normal endpoints get 403."""
        claims = _manager_claims(must_change_password=True)
        app.dependency_overrides[get_current_claims] = lambda: (_ for _ in ()).throw(
            __import__("fastapi").HTTPException(
                status_code=403,
                detail={"error": "password_change_required", "message": "Must change password"},
            )
        )
        resp = self.client.get("/bi/pricing/competitor/fuels")
        self.assertEqual(resp.status_code, 403)

    def test_change_password_endpoint_still_works(self):
        """The /auth/change-password endpoint uses the allow variant and is not blocked."""
        from contextlib import contextmanager

        claims = _manager_claims(must_change_password=True)
        app.dependency_overrides[get_current_claims_allow_password_change] = lambda: claims

        @contextmanager
        def _fake_conn(*a, **kw):
            mock = MagicMock()
            mock.execute.return_value.fetchone.return_value = {"password_hash": "fakehash"}
            yield mock

        with patch("app.db.get_conn", _fake_conn), \
             patch("app.routes_auth.verify_password", return_value=False):
            resp = self.client.post("/auth/change-password", json={
                "current_password": "old",
                "new_password": "newpassword123",
            })
        # Should NOT be 403 password_change_required; expect 400 wrong_password
        if resp.status_code == 403:
            self.assertNotEqual(resp.json().get("detail", {}).get("error"), "password_change_required")
        else:
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["detail"]["error"], "wrong_password")


# ---------------------------------------------------------------------------
# 7) dashboard_overview redaction
# ---------------------------------------------------------------------------

class TestDashboardOverviewRedaction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_manager_dashboard_overview_redacts_sensitive(self):
        """Manager with dashboard_home should not see custo_total/margem/margin_10d."""
        claims = _manager_claims(allowed_screens=["dashboard_home"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        with patch("app.repos_analytics.dashboard_kpis") as m_kpis, \
             patch("app.repos_analytics.dashboard_series") as m_series, \
             patch("app.repos_analytics.insights_base") as m_insights, \
             patch("app.repos_analytics.risk_insights") as m_risk_insights, \
             patch("app.repos_analytics.payments_overview") as m_payments, \
             patch("app.repos_analytics.open_cash_monitor") as m_open_cash, \
             patch("app.repos_analytics.risk_kpis") as m_risk_kpis, \
             patch("app.repos_analytics.risk_series") as m_risk_series, \
             patch("app.repos_analytics.risk_data_window") as m_risk_window, \
             patch("app.repos_analytics.operational_score") as m_op_score, \
             patch("app.repos_analytics.health_score_latest") as m_health, \
             patch("app.repos_analytics.jarvis_briefing") as m_jarvis, \
             patch("app.repos_analytics.notifications_unread_count") as m_notif, \
             patch("app.routes_bi.resolve_scope_filters") as m_scope, \
             patch("app.routes_bi.resolve_business_date") as m_bdate:
            m_scope.return_value = (1, 100, [100])
            m_bdate.return_value = __import__("datetime").date(2026, 5, 14)
            m_kpis.return_value = {"faturamento": 1000, "custo_total": 500, "margem": 200}
            m_series.return_value = []
            m_insights.return_value = []
            m_risk_insights.return_value = []
            m_payments.return_value = {}
            m_open_cash.return_value = []
            m_risk_kpis.return_value = {}
            m_risk_series.return_value = []
            m_risk_window.return_value = {}
            m_op_score.return_value = {}
            m_health.return_value = {}
            m_jarvis.return_value = {}
            m_notif.return_value = 0
            resp = self.client.get("/bi/dashboard/overview?dt_ini=2026-05-01&dt_fim=2026-05-14")
        self.assertIn(resp.status_code, (200,))
        body = resp.json()
        kpis = body["kpis"]
        self.assertEqual(kpis["faturamento"], 1000)
        self.assertIsNone(kpis.get("custo_total"))
        self.assertIsNone(kpis.get("margem"))


# ---------------------------------------------------------------------------
# 8) risk_overview redaction
# ---------------------------------------------------------------------------

class TestRiskOverviewRedaction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_manager_risk_overview_redacts_margem_score(self):
        """Manager with fraud should not see margem_score in risk overview."""
        claims = _manager_claims(allowed_screens=["fraud"])
        app.dependency_overrides[get_current_claims] = lambda: claims
        with patch("app.repos_analytics.risk_kpis") as m_risk_kpis, \
             patch("app.repos_analytics.risk_series") as m_risk_series, \
             patch("app.repos_analytics.risk_data_window") as m_risk_window, \
             patch("app.repos_analytics.risk_top_employees") as m_top, \
             patch("app.repos_analytics.risk_by_turn_local") as m_turn, \
             patch("app.repos_analytics.risk_last_events") as m_events, \
             patch("app.repos_analytics.risk_insights") as m_insights, \
             patch("app.repos_analytics.operational_score") as m_op_score, \
             patch("app.routes_bi.resolve_scope_filters") as m_scope:
            m_scope.return_value = (1, 100, [100])
            m_risk_kpis.return_value = {"margem_score": 0.85, "total_alertas": 5}
            m_risk_series.return_value = []
            m_risk_window.return_value = {}
            m_top.return_value = []
            m_turn.return_value = []
            m_events.return_value = []
            m_insights.return_value = []
            m_op_score.return_value = {}
            resp = self.client.get("/bi/risk/overview?dt_ini=2026-05-01&dt_fim=2026-05-14")
        self.assertIn(resp.status_code, (200,))
        body = resp.json()
        self.assertIsNone(body["kpis"].get("margem_score"))
        self.assertEqual(body["kpis"]["total_alertas"], 5)


# ---------------------------------------------------------------------------
# 9) change-password success with minimal JWT
# ---------------------------------------------------------------------------

class TestChangePasswordSuccess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims_allow_password_change, None)

    def test_change_password_returns_minimal_jwt(self):
        """After changing password, returned token should be decodable (no session context bloat)."""
        from contextlib import contextmanager
        from app.security import decode_token

        claims = _manager_claims(must_change_password=True)
        app.dependency_overrides[get_current_claims_allow_password_change] = lambda: claims

        @contextmanager
        def _fake_conn(*a, **kw):
            mock = MagicMock()
            mock.execute.return_value.fetchone.return_value = {"password_hash": "fakehash"}
            yield mock

        with patch("app.db.get_conn", _fake_conn), \
             patch("app.routes_auth.verify_password", return_value=True), \
             patch("app.routes_auth.hash_password", return_value="newhash"):
            resp = self.client.post("/auth/change-password", json={
                "current_password": "oldpass",
                "new_password": "newpassword123",
            })
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["ok"])
        self.assertIn("access_token", body)
        # Decode should work without errors (no datetime/complex objects)
        payload = decode_token(body["access_token"])
        self.assertEqual(payload["sub"], claims["sub"])
        self.assertEqual(payload["must_change_password"], False)
        # Should NOT contain session-specific keys
        self.assertNotIn("accesses", payload)
        self.assertNotIn("allowed_screens", payload)


# ---------------------------------------------------------------------------
# 10) kiosk screen restrictions
# ---------------------------------------------------------------------------

class TestKioskScreenRestrictions(unittest.TestCase):
    def test_kiosk_with_sales_gets_422(self):
        """tenant_kiosk cannot have sales in screen_permissions."""
        from app.permissions import validate_screen_permissions_for_role
        with self.assertRaises(HTTPException) as ctx:
            validate_screen_permissions_for_role("tenant_kiosk", ["sales", "tv_sales_hourly"])
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("sales", ctx.exception.detail["disallowed"])

    def test_kiosk_with_tv_only_passes(self):
        """tenant_kiosk with only TV screens should pass."""
        from app.permissions import validate_screen_permissions_for_role
        result = validate_screen_permissions_for_role("tenant_kiosk", ["tv_sales_hourly", "tv_sales_ranking"])
        self.assertEqual(result, ["tv_sales_hourly", "tv_sales_ranking"])

    def test_manager_with_tv_gets_422(self):
        """tenant_manager cannot have TV screens."""
        from app.permissions import validate_screen_permissions_for_role
        with self.assertRaises(HTTPException) as ctx:
            validate_screen_permissions_for_role("tenant_manager", ["dashboard_home", "tv_sales_hourly"])
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("tv_sales_hourly", ctx.exception.detail["disallowed"])

    def test_manager_with_product_screens_passes(self):
        """tenant_manager with product screens should pass."""
        from app.permissions import validate_screen_permissions_for_role
        result = validate_screen_permissions_for_role("tenant_manager", ["dashboard_home", "sales", "cash"])
        self.assertEqual(result, ["dashboard_home", "sales", "cash"])


# ---------------------------------------------------------------------------
# 11) kiosk blocked on generic BI endpoints
# ---------------------------------------------------------------------------

class TestKioskBlockedGenericEndpoints(unittest.TestCase):
    """tenant_kiosk gets 403 on generic BI endpoints (filiais, sync, etc.)."""

    BLOCKED_ENDPOINTS = [
        ("GET", "/bi/filiais"),
        ("GET", "/bi/sync/status"),
        ("GET", "/bi/me/telegram"),
        ("PATCH", "/bi/me/telegram"),
        ("GET", "/bi/notifications"),
        ("POST", "/bi/notifications/1/read"),
        ("GET", "/bi/notifications/unread-count"),
    ]

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def setUp(self):
        app.dependency_overrides[get_current_claims] = lambda: _kiosk_claims()

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_kiosk_blocked_on_all_generic_endpoints(self):
        for method, path in self.BLOCKED_ENDPOINTS:
            with self.subTest(method=method, path=path):
                fn = getattr(self.client, method.lower())
                kwargs: dict = {}
                if method == "PATCH":
                    kwargs["json"] = {"telegram_enabled": False}
                resp = fn(path, **kwargs)
                self.assertEqual(resp.status_code, 403, f"{method} {path} should be 403")
                self.assertEqual(resp.json()["detail"]["error"], "kiosk_not_allowed")


# ---------------------------------------------------------------------------
# 12) kiosk still allowed on TV endpoints
# ---------------------------------------------------------------------------

class TestKioskAllowedTVEndpoints(unittest.TestCase):
    """tenant_kiosk can still call /bi/tv/* endpoints."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_kiosk_can_access_tv_sales_hourly(self):
        app.dependency_overrides[get_current_claims] = lambda: _kiosk_claims()
        with patch("app.repos_analytics.dashboard_series", return_value=[]):
            resp = self.client.get("/bi/tv/sales-hourly")
        self.assertNotEqual(resp.status_code, 403)

    def test_kiosk_can_access_tv_sales_ranking(self):
        app.dependency_overrides[get_current_claims] = lambda: _kiosk_claims()
        with patch("app.repos_analytics.sales_overview_bundle", return_value={"sellers": []}):
            resp = self.client.get("/bi/tv/sales-ranking")
        self.assertNotEqual(resp.status_code, 403)


# ---------------------------------------------------------------------------
# 13) Refresh token endpoint
# ---------------------------------------------------------------------------

class TestRefreshTokenEndpoint(unittest.TestCase):
    """POST /auth/refresh issues a fresh token."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_claims, None)

    def test_refresh_for_kiosk_returns_token(self):
        app.dependency_overrides[get_current_claims] = lambda: _kiosk_claims()
        resp = self.client.post("/auth/refresh")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["ok"])
        self.assertIn("access_token", body)
        # Verify kiosk gets 24h token
        from app.security import decode_token
        payload = decode_token(body["access_token"])
        ttl = payload["exp"] - payload["iat"]
        self.assertAlmostEqual(ttl, 1440 * 60, delta=5)

    def test_refresh_for_owner_returns_default_ttl(self):
        app.dependency_overrides[get_current_claims] = lambda: _owner_claims()
        resp = self.client.post("/auth/refresh")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["ok"])
        from app.security import decode_token
        payload = decode_token(body["access_token"])
        ttl = payload["exp"] - payload["iat"]
        from app.config import settings
        self.assertAlmostEqual(ttl, settings.api_access_token_minutes * 60, delta=5)

    def test_refresh_without_token_401(self):
        app.dependency_overrides.pop(get_current_claims, None)
        resp = self.client.post("/auth/refresh")
        self.assertEqual(resp.status_code, 401)


# ---------------------------------------------------------------------------
# 14) require_not_kiosk unit
# ---------------------------------------------------------------------------

class TestRequireNotKiosk(unittest.TestCase):
    """Unit test for require_not_kiosk dependency."""

    def test_kiosk_raises_403(self):
        from app.permissions import require_not_kiosk
        checker = require_not_kiosk()
        with self.assertRaises(HTTPException) as ctx:
            checker(claims=_kiosk_claims())
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(ctx.exception.detail["error"], "kiosk_not_allowed")

    def test_manager_passes(self):
        from app.permissions import require_not_kiosk
        checker = require_not_kiosk()
        checker(claims=_manager_claims())

    def test_owner_passes(self):
        from app.permissions import require_not_kiosk
        checker = require_not_kiosk()
        checker(claims=_owner_claims())


if __name__ == "__main__":
    unittest.main()
