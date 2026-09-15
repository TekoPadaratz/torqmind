"""Itens sem ID_FUNCIONARIOS herdam vendedor de outro item do mesmo cupom."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app import repos_commission


def test_query_eligible_sales_sql_coalesces_sibling_funcionario():
    """Cupom misto (combustível com vendedor + loja sem) conta loja no frentista."""
    captured = {}

    def _fake_query(sql, parameters=None):
        captured["sql"] = sql
        captured["parameters"] = parameters
        return [
            {
                "id_filial": 11621,
                "id_funcionario": 1038,
                "nome_vendedor": "DANIEL",
                "id_grupo_produto": 9,
                "nome_grupo_produto": "ADITIVOS",
                "id_produto": 3978,
                "venda_total": 35.0,
                "quantidade_vendas": 1.0,
                "qtd_itens": 1,
            }
        ]

    with patch.object(repos_commission, "query_dict", side_effect=_fake_query):
        rows = repos_commission._query_eligible_sales_ch(
            1, [11621], date(2026, 8, 20), date(2026, 9, 13)
        )

    sql = captured["sql"]
    assert "sib.id_funcionario_any" in sql
    assert "max(id_funcionario) AS id_funcionario_any" in sql
    assert "coalesce(sib.id_funcionario_any" in sql
    assert rows[0]["id_funcionario"] == 1038
    assert rows[0]["venda_total"] == 35.0
