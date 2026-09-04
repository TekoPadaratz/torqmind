"""Unit tests — Qtd abastecimentos / frentista média/highlight."""
from __future__ import annotations

from app.repos_commission import (
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
