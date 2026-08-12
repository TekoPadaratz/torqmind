"""Unit tests for the FASE 4 commission fixes.

Covers:
  * ensure_default_config creates a default config when none exists
    (regression for the indentation bug that made creation unreachable and
    raised UnboundLocalError for branches without a config).
  * calculate_commission_results honours the three payment modes
    (team_total, equal_split, individual_sales) instead of always computing
    individual commission.
  * payment_mode falls back to the configured default when omitted/invalid.
"""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

from app import repos_commission


class _FakeResult:
    def __init__(self, rows, one):
        self._rows = rows
        self._one = one

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._one


class _FakeConn:
    def __init__(self, rows=None, one=None):
        self._rows = rows or []
        self._one = one

    def execute(self, sql, params=None):
        return _FakeResult(self._rows, self._one)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _config(default_mode: str = "individual_sales"):
    return {
        "id": 1,
        "name": "Comissao",
        "is_active": True,
        "default_payment_mode": default_mode,
        "manager_commission_mode": "use_tiers",
        "manager_commission_percent": 0,
    }


def _sales_rows():
    # qtd equipe = 120 → prata (3%); valor 70.000 — grão produto (CH)
    return [
        {
            "id_filial": 14122,
            "id_funcionario": 1,
            "nome_vendedor": "ANA",
            "id_grupo_produto": 10,
            "nome_grupo_produto": "LOJA",
            "id_produto": 100,
            "venda_total": 40000,
            "quantidade_vendas": 70,
        },
        {
            "id_filial": 14122,
            "id_funcionario": 2,
            "nome_vendedor": "BRUNO",
            "id_grupo_produto": 10,
            "nome_grupo_produto": "LOJA",
            "id_produto": 101,
            "venda_total": 30000,
            "quantidade_vendas": 50,
        },
    ]


@contextmanager
def _patched_calc(default_mode: str = "team_total"):
    """Patch the DB-backed helpers used by calculate_commission_results."""
    with patch.object(repos_commission, "get_config", return_value=_config(default_mode)), \
         patch.object(repos_commission, "get_config_groups", return_value=[{"id_grupo_produto": 10}]), \
         patch.object(repos_commission, "get_config_tiers", return_value=list(repos_commission.DEFAULT_TIERS)), \
         patch.object(repos_commission, "get_config_product_excludes", return_value=[]), \
         patch.object(repos_commission, "_filial_labels", return_value={14122: "VR 05"}), \
         patch.object(repos_commission, "_query_eligible_sales_ch", return_value=_sales_rows()):
        yield


def test_individual_sales_mode_uses_per_employee_tier():
    with _patched_calc():
        res = repos_commission.calculate_commission_results(1, 14122, 6, 2026, "individual_sales")
    assert res["payment_mode"] == "individual_sales"
    # Cada um ≥50 e <110 → bronze 2%: 40000*2% + 30000*2% = 800 + 600
    assert res["comissao_total"] == 1400.00
    by_id = {e["id_funcionario"]: e for e in res["vendedores"]}
    assert by_id[1]["comissao_estimada"] == 800.00
    assert by_id[2]["comissao_estimada"] == 600.00
    # Team-level tier fields are not used in individual mode
    assert res["nivel_atingido"] is None
    assert res["percentual_aplicado"] is None


def test_team_total_mode_uses_team_tier_and_proportional_split():
    with _patched_calc():
        res = repos_commission.calculate_commission_results(1, 14122, 6, 2026, "team_total")
    assert res["payment_mode"] == "team_total"
    # qtd 120 → prata 3% sobre R$ 70.000 = 2.100
    assert res["comissao_total"] == 2100.00
    assert res["percentual_aplicado"] == 3.0
    assert res["nivel_atingido"]["tier_key"] == "silver"
    by_id = {e["id_funcionario"]: e for e in res["vendedores"]}
    # proporcional: 2100 * 40000/70000 = 1200 ; 2100 * 30000/70000 = 900
    assert by_id[1]["comissao_estimada"] == 1200.00
    assert by_id[2]["comissao_estimada"] == 900.00


def test_equal_split_mode_divides_team_commission_equally():
    with _patched_calc():
        res = repos_commission.calculate_commission_results(1, 14122, 6, 2026, "equal_split")
    assert res["payment_mode"] == "equal_split"
    assert res["comissao_total"] == 2100.00
    by_id = {e["id_funcionario"]: e for e in res["vendedores"]}
    # 2100 / 2 = 1050 cada
    assert by_id[1]["comissao_estimada"] == 1050.00
    assert by_id[2]["comissao_estimada"] == 1050.00


def test_invalid_payment_mode_falls_back_to_config_default():
    with _patched_calc(default_mode="equal_split"):
        res = repos_commission.calculate_commission_results(1, 14122, 6, 2026, "bogus_mode")
    assert res["payment_mode"] == "equal_split"


def test_ensure_default_config_creates_when_missing():
    """Regression: creation block must run when no config exists (Bug 1)."""
    created = {
        "id": 99,
        "id_empresa": 1,
        "id_filial": 14122,
        "name": "Comissao padrao",
        "is_active": True,
        "default_payment_mode": "individual_sales",
        "manager_commission_mode": "use_tiers",
        "manager_commission_percent": 0,
        "created_at": None,
        "updated_at": None,
    }

    def _get_conn(*args, **kwargs):
        return _FakeConn(one=created)

    with patch.object(repos_commission, "get_config", return_value=None), \
         patch.object(repos_commission, "get_conn", side_effect=_get_conn):
        res = repos_commission.ensure_default_config(1, 14122)
    assert res["id"] == 99
    assert res["default_payment_mode"] == "individual_sales"


def test_ensure_default_config_returns_existing():
    with patch.object(repos_commission, "get_config", return_value=_config()):
        res = repos_commission.ensure_default_config(1, 14122)
    assert res["id"] == 1
