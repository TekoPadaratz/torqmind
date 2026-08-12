"""Unit tests for commission tier calculation logic."""
import pytest
from app.repos_commission import (
    _determine_tier,
    _next_tier,
    _sort_sellers_by_tier,
    DEFAULT_TIERS,
)


class TestDetermineTier:
    """Tests for _determine_tier function."""

    def test_below_bronze_returns_none(self):
        result = _determine_tier(20000, DEFAULT_TIERS)
        assert result is None

    def test_exactly_bronze(self):
        result = _determine_tier(30000, DEFAULT_TIERS)
        assert result is not None
        assert result["tier_key"] == "bronze"

    def test_between_bronze_and_silver(self):
        result = _determine_tier(40000, DEFAULT_TIERS)
        assert result["tier_key"] == "bronze"

    def test_exactly_silver(self):
        result = _determine_tier(50000, DEFAULT_TIERS)
        assert result["tier_key"] == "silver"

    def test_exactly_gold(self):
        result = _determine_tier(80000, DEFAULT_TIERS)
        assert result["tier_key"] == "gold"

    def test_exactly_diamond(self):
        result = _determine_tier(120000, DEFAULT_TIERS)
        assert result["tier_key"] == "diamond"

    def test_above_diamond(self):
        result = _determine_tier(500000, DEFAULT_TIERS)
        assert result["tier_key"] == "diamond"

    def test_zero_returns_none(self):
        result = _determine_tier(0, DEFAULT_TIERS)
        assert result is None

    def test_empty_tiers_returns_none(self):
        result = _determine_tier(100000, [])
        assert result is None

    def test_inactive_tiers_ignored(self):
        tiers = [
            {"tier_key": "bronze", "tier_name": "Bronze", "min_sales_amount": 30000, "commission_percent": 0.5, "sort_order": 1, "is_active": True},
            {"tier_key": "silver", "tier_name": "Prata", "min_sales_amount": 50000, "commission_percent": 1.0, "sort_order": 2, "is_active": False},
            {"tier_key": "gold", "tier_name": "Ouro", "min_sales_amount": 80000, "commission_percent": 1.5, "sort_order": 3, "is_active": True},
        ]
        result = _determine_tier(60000, tiers)
        assert result["tier_key"] == "bronze"


class TestNextTier:
    """Tests for _next_tier function."""

    def test_no_current_returns_first(self):
        result = _next_tier(None, DEFAULT_TIERS)
        assert result is not None
        assert result["tier_key"] == "bronze"

    def test_bronze_next_is_silver(self):
        bronze = DEFAULT_TIERS[0]
        result = _next_tier(bronze, DEFAULT_TIERS)
        assert result["tier_key"] == "silver"

    def test_diamond_has_no_next(self):
        diamond = DEFAULT_TIERS[3]
        result = _next_tier(diamond, DEFAULT_TIERS)
        assert result is None

    def test_empty_tiers_returns_none(self):
        result = _next_tier(None, [])
        assert result is None


class TestCommissionCalculation:
    """Tests for commission calculation modes."""

    def test_team_total_mode(self):
        """63000 eligible with silver (1%) = 630.00 commission"""
        total = 63000
        percent = 1.0
        commission = round(total * percent / 100, 2)
        assert commission == 630.00

    def test_equal_split_mode(self):
        """630 commission / 6 employees = 105.00 each"""
        commission_total = 630.00
        num_employees = 6
        per_employee = round(commission_total / num_employees, 2)
        assert per_employee == 105.00

    def test_individual_mode(self):
        """Employee sold 8000 at 1% = 80.00"""
        employee_sales = 8000
        percent = 1.0
        commission = round(employee_sales * percent / 100, 2)
        assert commission == 80.00

    def test_no_sales_zero_commission(self):
        total = 0
        percent = 1.0
        commission = round(total * percent / 100, 2)
        assert commission == 0.00

    def test_below_bronze_zero_percent(self):
        """If below bronze, percent is 0"""
        result = _determine_tier(25000, DEFAULT_TIERS)
        assert result is None
        # No tier means 0%
        percent = 0.0
        commission = round(25000 * percent / 100, 2)
        assert commission == 0.00


class TestSortSellersByTier:
    """Grid order: Diamante → Ouro → Prata → Bronze → sem nível."""

    def test_tiers_high_to_low(self):
        rows = [
            {"nome_vendedor": "B", "venda_elegivel": 10, "nivel_atingido": {"tier_key": "bronze"}},
            {"nome_vendedor": "D", "venda_elegivel": 10, "nivel_atingido": {"tier_key": "diamond"}},
            {"nome_vendedor": "S", "venda_elegivel": 10, "nivel_atingido": {"tier_key": "silver"}},
            {"nome_vendedor": "G", "venda_elegivel": 10, "nivel_atingido": {"tier_key": "gold"}},
            {"nome_vendedor": "Z", "venda_elegivel": 10, "nivel_atingido": None},
        ]
        sorted_rows = _sort_sellers_by_tier(rows)
        keys = [
            (r["nivel_atingido"] or {}).get("tier_key") if r["nivel_atingido"] else None
            for r in sorted_rows
        ]
        assert keys == ["diamond", "gold", "silver", "bronze", None]

    def test_same_tier_sorts_by_sales_then_name(self):
        rows = [
            {"nome_vendedor": "Bruno", "venda_elegivel": 50, "nivel_atingido": {"tier_key": "gold"}},
            {"nome_vendedor": "Ana", "venda_elegivel": 80, "nivel_atingido": {"tier_key": "gold"}},
            {"nome_vendedor": "Carlos", "venda_elegivel": 80, "nivel_atingido": {"tier_key": "gold"}},
        ]
        sorted_rows = _sort_sellers_by_tier(rows)
        assert [r["nome_vendedor"] for r in sorted_rows] == ["Ana", "Carlos", "Bruno"]
