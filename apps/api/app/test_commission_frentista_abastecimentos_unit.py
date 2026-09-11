"""Unit tests — Qtd abastecimentos / frentista média/highlight."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app import repos_commission
from app.repos_commission import (
    _abastecimentos_by_funcionario,
    annotate_frentista_abastecimentos,
    is_combustivel_grupo_nome,
    is_frentista_funcao,
)


def test_is_combustivel_grupo_nome():
    assert is_combustivel_grupo_nome("COMBUSTIVEIS") is True
    assert is_combustivel_grupo_nome("Gasolina") is True
    assert is_combustivel_grupo_nome("OLEO COMBUSTIVEL") is False
    assert is_combustivel_grupo_nome("LOJA") is False


def test_is_frentista_funcao():
    assert is_frentista_funcao("FRENTISTA") is True
    assert is_frentista_funcao("Frentista Pista") is True
    assert is_frentista_funcao("GERENTE") is False


def test_annotate_frentista_abastecimentos_mean_and_highlight():
    rows = [
        {"id_funcionario": 1, "qtd_abastecimentos": 100, "nome_vendedor": "A"},
        {"id_funcionario": 2, "qtd_abastecimentos": 40, "nome_vendedor": "B"},
        {"id_funcionario": 3, "qtd_abastecimentos": 0, "nome_vendedor": "Loja"},
    ]
    media = annotate_frentista_abastecimentos(rows)
    # Frentistas = quem tem abastecimento (1 e 2); média (100+40)/2 = 70
    assert media == 70.0
    assert rows[0]["is_frentista"] is True
    assert rows[1]["is_frentista"] is True
    assert rows[2]["is_frentista"] is False
    assert rows[0]["abaixo_media_abastecimentos"] is False
    assert rows[1]["abaixo_media_abastecimentos"] is True
    assert rows[2]["abaixo_media_abastecimentos"] is False


def test_annotate_frentista_by_cargo_even_without_fuel():
    rows = [
        {"id_funcionario": 9, "qtd_abastecimentos": 0, "nome_vendedor": "C"},
        {"id_funcionario": 8, "qtd_abastecimentos": 50, "nome_vendedor": "D"},
    ]
    media = annotate_frentista_abastecimentos(
        rows, funcao_by_id={9: "FRENTISTA", 8: "OPERADOR"}
    )
    # Ambos frentistas (cargo + combustível); média (0+50)/2 = 25
    assert media == 25.0
    assert rows[0]["is_frentista"] is True
    assert rows[0]["abaixo_media_abastecimentos"] is True
    assert rows[1]["abaixo_media_abastecimentos"] is False


def test_abastecimentos_from_raw_even_when_fuel_not_in_config_groups():
    """COMBUSTÍVEIS fora da config de comissão ainda conta qtd_abastecimentos."""
    raw = [
        {
            "id_funcionario": 1,
            "nome_vendedor": "ANA",
            "id_grupo_produto": 10,
            "nome_grupo_produto": "LOJA",
            "id_produto": 100,
            "venda_total": 500.0,
            "quantidade_vendas": 10,
            "qtd_itens": 10,
        },
        {
            "id_funcionario": 1,
            "nome_vendedor": "ANA",
            "id_grupo_produto": 1,
            "nome_grupo_produto": "COMBUSTIVEIS",
            "id_produto": 200,
            "venda_total": 9000.0,
            "quantidade_vendas": 100,
            "qtd_itens": 42,
        },
        {
            "id_funcionario": 2,
            "nome_vendedor": "BRUNO",
            "id_grupo_produto": 10,
            "nome_grupo_produto": "LOJA",
            "id_produto": 101,
            "venda_total": 300.0,
            "quantidade_vendas": 5,
            "qtd_itens": 5,
        },
    ]
    # Filtro de elegibilidade só LOJA → combustível some da base de comissão
    filtered = repos_commission._filter_sales_for_config(raw, [10], [], None)
    assert all(int(r.get("id_grupo_produto") or 0) == 10 for r in filtered)
    assert sum(int(r.get("qtd_abastecimentos") or 0) for r in filtered) == 0

    abast = _abastecimentos_by_funcionario(raw)
    assert abast == {1: 42}

    with patch.object(repos_commission, "get_config", return_value={
        "id": 1,
        "name": "C",
        "is_active": True,
        "default_payment_mode": "individual_sales",
        "manager_commission_mode": "use_tiers",
        "manager_commission_percent": 0,
    }), patch.object(
        repos_commission, "get_config_groups", return_value=[{"id_grupo_produto": 10}]
    ), patch.object(
        repos_commission, "get_config_tiers", return_value=list(repos_commission.DEFAULT_TIERS)
    ), patch.object(
        repos_commission, "get_config_product_excludes", return_value=[]
    ), patch.object(
        repos_commission, "get_excluded_funcionario_ids", return_value=None
    ), patch.object(
        repos_commission, "list_branch_employees_ch", return_value=[]
    ):
        out = repos_commission.calculate_commission_results(
            1,
            14122,
            date(2026, 9, 1),
            date(2026, 9, 11),
            "individual_sales",
            sales_rows=raw,
        )
    by_id = {int(e["id_funcionario"]): e for e in out["vendedores"]}
    assert by_id[1]["qtd_abastecimentos"] == 42
    assert by_id[1]["is_frentista"] is True
    assert by_id[2]["qtd_abastecimentos"] == 0
    assert by_id[2]["is_frentista"] is False
