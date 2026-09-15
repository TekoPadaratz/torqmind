"""Unit tests — órfãos header-sem-itens na comissão de vendedores."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app import repos_commission


def test_orphan_aviso_present_when_headers_without_items():
    """Totais não podem parecer completos se há cupons sem itens no período."""
    with patch.object(
        repos_commission,
        "_count_orphan_comprovantes_ch",
        return_value={
            "orphan_headers": 2,
            "orphan_valor_total": 150.5,
            "orphan_ids_sample": [3704983, 3703208],
            "aviso": "Há 2 venda(s) sem itens sincronizados no período.",
        },
    ), patch.object(
        repos_commission,
        "get_config",
        return_value={
            "id": 4,
            "name": "cfg",
            "default_payment_mode": "individual_sales",
            "manager_commission_mode": "use_tiers",
            "manager_commission_percent": 0,
            "include_central_mirror": True,
        },
    ), patch.object(
        repos_commission,
        "get_config_groups",
        return_value=[{"id_grupo_produto": 2, "nome_grupo_produto_snapshot": "LUBRIFICANTES"}],
    ), patch.object(
        repos_commission,
        "get_config_tiers",
        return_value=[
            {
                "tier_key": "gold",
                "tier_name": "Ouro",
                "min_sales_amount": 80,
                "commission_percent": 5,
                "sort_order": 1,
                "is_active": True,
            }
        ],
    ), patch.object(
        repos_commission, "get_config_product_excludes", return_value=[]
    ), patch.object(
        repos_commission, "get_excluded_funcionario_ids", return_value=None
    ), patch.object(
        repos_commission, "list_branch_employees_ch", return_value=[]
    ), patch.object(repos_commission, "_filial_labels", return_value={11621: "VR 06"}):
        out = repos_commission.calculate_commission_results(
            1,
            11621,
            date(2026, 8, 20),
            date(2026, 9, 13),
            sales_rows=[
                {
                    "id_filial": 11621,
                    "id_funcionario": 1038,
                    "nome_vendedor": "DANIEL",
                    "id_grupo_produto": 2,
                    "nome_grupo_produto": "LUBRIFICANTES",
                    "id_produto": 1,
                    "venda_total": 100.0,
                    "quantidade_vendas": 10.0,
                    "qtd_itens": 10,
                }
            ],
        )
    assert out["integridade_dados"]["orphan_headers"] == 2
    assert out["aviso_integridade"]
    assert "sem itens" in out["aviso_integridade"].lower()


def test_orphan_check_absent_means_no_aviso():
    with patch.object(
        repos_commission,
        "_count_orphan_comprovantes_ch",
        return_value={
            "orphan_headers": 0,
            "orphan_valor_total": 0.0,
            "orphan_ids_sample": [],
        },
    ), patch.object(
        repos_commission,
        "get_config",
        return_value={
            "id": 4,
            "name": "cfg",
            "default_payment_mode": "individual_sales",
            "manager_commission_mode": "use_tiers",
            "manager_commission_percent": 0,
            "include_central_mirror": True,
        },
    ), patch.object(
        repos_commission,
        "get_config_groups",
        return_value=[{"id_grupo_produto": 2}],
    ), patch.object(
        repos_commission,
        "get_config_tiers",
        return_value=[
            {
                "tier_key": "bronze",
                "tier_name": "Bronze",
                "min_sales_amount": 1,
                "commission_percent": 2,
                "sort_order": 1,
                "is_active": True,
            }
        ],
    ), patch.object(
        repos_commission, "get_config_product_excludes", return_value=[]
    ), patch.object(
        repos_commission, "get_excluded_funcionario_ids", return_value=None
    ), patch.object(
        repos_commission, "list_branch_employees_ch", return_value=[]
    ), patch.object(repos_commission, "_filial_labels", return_value={11621: "VR 06"}):
        out = repos_commission.calculate_commission_results(
            1,
            11621,
            date(2026, 8, 20),
            date(2026, 9, 13),
            sales_rows=[
                {
                    "id_filial": 11621,
                    "id_funcionario": 7,
                    "nome_vendedor": "OLIMPIO",
                    "id_grupo_produto": 2,
                    "nome_grupo_produto": "LUBRIFICANTES",
                    "id_produto": 1,
                    "venda_total": 50.0,
                    "quantidade_vendas": 5.0,
                    "qtd_itens": 5,
                }
            ],
        )
    assert out["integridade_dados"]["orphan_headers"] == 0
    assert not out.get("aviso_integridade")


def test_period_bounds_inclusive_edges_for_orphan_query_helper():
    """20/08 e 13/09 inclusivos; data_key half-open < 20260914."""
    from app.commission_period import data_key_bounds_half_open

    a, b = data_key_bounds_half_open(date(2026, 8, 20), date(2026, 9, 13))
    assert a == 20260820
    assert b == 20260914
    a2, b2 = data_key_bounds_half_open(date(2026, 8, 19), date(2026, 9, 14))
    assert a2 == 20260819
    assert b2 == 20260915
