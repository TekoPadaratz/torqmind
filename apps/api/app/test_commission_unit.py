"""Unit tests for commission tier calculation logic (quantity-based)."""
from app.repos_commission import (
    COMMISSION_EXCLUDED_CFOPS,
    _cfop_sales_predicate_sql,
    _determine_tier,
    _next_tier,
    _sort_sellers_by_tier,
    DEFAULT_TIERS,
)


class TestDetermineTier:
    """Tests for _determine_tier function (quantity, not R$)."""

    def test_below_bronze_returns_none(self):
        result = _determine_tier(20, DEFAULT_TIERS)
        assert result is None

    def test_exactly_bronze(self):
        result = _determine_tier(50, DEFAULT_TIERS)
        assert result is not None
        assert result["tier_key"] == "bronze"

    def test_between_bronze_and_silver(self):
        result = _determine_tier(80, DEFAULT_TIERS)
        assert result["tier_key"] == "bronze"

    def test_exactly_silver(self):
        result = _determine_tier(110, DEFAULT_TIERS)
        assert result["tier_key"] == "silver"

    def test_exactly_gold(self):
        result = _determine_tier(160, DEFAULT_TIERS)
        assert result["tier_key"] == "gold"

    def test_exactly_diamond(self):
        result = _determine_tier(300, DEFAULT_TIERS)
        assert result["tier_key"] == "diamond"

    def test_above_diamond(self):
        result = _determine_tier(500, DEFAULT_TIERS)
        assert result["tier_key"] == "diamond"

    def test_zero_returns_none(self):
        result = _determine_tier(0, DEFAULT_TIERS)
        assert result is None

    def test_empty_tiers_returns_none(self):
        result = _determine_tier(100, [])
        assert result is None

    def test_inactive_tiers_ignored(self):
        tiers = [
            {"tier_key": "bronze", "tier_name": "Bronze", "min_sales_amount": 50, "commission_percent": 2.0, "sort_order": 1, "is_active": True},
            {"tier_key": "silver", "tier_name": "Prata", "min_sales_amount": 110, "commission_percent": 3.0, "sort_order": 2, "is_active": False},
            {"tier_key": "gold", "tier_name": "Ouro", "min_sales_amount": 160, "commission_percent": 5.0, "sort_order": 3, "is_active": True},
        ]
        result = _determine_tier(120, tiers)
        assert result["tier_key"] == "bronze"

    def test_min_qty_alias(self):
        tiers = [
            {"tier_key": "bronze", "tier_name": "Bronze", "min_qty": 40, "commission_percent": 2.0, "is_active": True},
        ]
        assert _determine_tier(40, tiers)["tier_key"] == "bronze"
        assert _determine_tier(39, tiers) is None


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
        total = 63000
        percent = 3.0
        commission = round(total * percent / 100, 2)
        assert commission == 1890.00

    def test_equal_split_mode(self):
        commission_total = 630.00
        num_employees = 6
        per_employee = round(commission_total / num_employees, 2)
        assert per_employee == 105.00

    def test_individual_mode(self):
        employee_sales = 8000
        percent = 2.0
        commission = round(employee_sales * percent / 100, 2)
        assert commission == 160.00

    def test_no_sales_zero_commission(self):
        total = 0
        percent = 2.0
        commission = round(total * percent / 100, 2)
        assert commission == 0.00

    def test_below_bronze_zero_percent(self):
        result = _determine_tier(25, DEFAULT_TIERS)
        assert result is None
        percent = 0.0
        commission = round(25000 * percent / 100, 2)
        assert commission == 0.00


class TestSortSellersByTier:
    """Grid: Filial ASC → comissão DESC → qtd DESC → venda DESC → nome ASC."""

    def test_filial_then_commission_desc(self):
        rows = [
            {
                "nome_vendedor": "B",
                "comissao_estimada": 50,
                "quantidade_vendas": 1,
                "venda_elegivel": 10,
                "filial_label": "B",
                "id_filial": 2,
                "id_funcionario": 1,
            },
            {
                "nome_vendedor": "A2",
                "comissao_estimada": 80,
                "quantidade_vendas": 1,
                "venda_elegivel": 10,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 2,
            },
            {
                "nome_vendedor": "A1",
                "comissao_estimada": 120,
                "quantidade_vendas": 1,
                "venda_elegivel": 10,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 3,
            },
            {
                "nome_vendedor": "A0",
                "comissao_estimada": 80,
                "quantidade_vendas": 1,
                "venda_elegivel": 10,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 4,
            },
        ]
        sorted_rows = _sort_sellers_by_tier(rows)
        assert [r["nome_vendedor"] for r in sorted_rows] == ["A1", "A0", "A2", "B"]

    def test_zero_commission_falls_back_to_qty_then_sales_then_name(self):
        rows = [
            {
                "nome_vendedor": "Bruno",
                "comissao_estimada": 0,
                "quantidade_vendas": 40,
                "venda_elegivel": 900,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 1,
            },
            {
                "nome_vendedor": "Ana",
                "comissao_estimada": 0,
                "quantidade_vendas": 80,
                "venda_elegivel": 500,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 2,
            },
            {
                "nome_vendedor": "Carlos",
                "comissao_estimada": 0,
                "quantidade_vendas": 80,
                "venda_elegivel": 700,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 3,
            },
            {
                "nome_vendedor": "Diana",
                "comissao_estimada": 0,
                "quantidade_vendas": 80,
                "venda_elegivel": 700,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 4,
            },
        ]
        sorted_rows = _sort_sellers_by_tier(rows)
        assert [r["nome_vendedor"] for r in sorted_rows] == ["Carlos", "Diana", "Ana", "Bruno"]

    def test_same_commission_name_asc_when_qty_and_sales_tie(self):
        rows = [
            {
                "nome_vendedor": "Bruno",
                "comissao_estimada": 100,
                "quantidade_vendas": 10,
                "venda_elegivel": 1000,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 1,
            },
            {
                "nome_vendedor": "Ana",
                "comissao_estimada": 100,
                "quantidade_vendas": 10,
                "venda_elegivel": 1000,
                "filial_label": "A",
                "id_filial": 1,
                "id_funcionario": 2,
            },
        ]
        sorted_rows = _sort_sellers_by_tier(rows)
        assert [r["nome_vendedor"] for r in sorted_rows] == ["Ana", "Bruno"]


class TestCommissionCfopExclusion:
    def test_excludes_transfer_and_requires_sales_cfop_gate(self):
        assert COMMISSION_EXCLUDED_CFOPS == (5929, 6929)
        pred = _cfop_sales_predicate_sql("i").replace(" ", "").lower()
        assert "cfop,0)>5000" in pred
        assert "notin(5929,6929)" in pred
