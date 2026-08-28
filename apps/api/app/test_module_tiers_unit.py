"""Module tier presets and sensitive financial flag."""
from app.permissions import (
    module_tier_preset_screens,
    normalize_module_tier,
    user_can_view_sensitive_financials,
    can_view_sensitive_financials,
)


def test_module_tier_presets_expand():
    essencial = set(module_tier_preset_screens("essencial"))
    gestao = set(module_tier_preset_screens("gestao"))
    assert "sales" in essencial
    assert "finance" not in essencial
    assert "profit_management" in gestao
    assert essencial < gestao


def test_normalize_module_tier_fallback():
    assert normalize_module_tier("invalid") == "essencial"
    assert normalize_module_tier("gestao") == "gestao"


def test_user_sensitive_flag_for_manager():
    assert user_can_view_sensitive_financials("tenant_admin", False) is True
    assert user_can_view_sensitive_financials("tenant_manager", False) is False
    assert user_can_view_sensitive_financials("tenant_manager", True) is True


def test_claims_sensitive_from_user_flag():
    claims = {"user_role": "tenant_manager", "can_view_sensitive_financials": True}
    assert can_view_sensitive_financials(claims) is True
    claims_off = {"user_role": "tenant_manager", "can_view_sensitive_financials": False}
    assert can_view_sensitive_financials(claims_off) is False
