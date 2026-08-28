"""Regras de suspeita crédito funcionário — saldo aberto × limite."""

from __future__ import annotations

from unittest.mock import patch

from app.repos_mart import mash_fraud_credito_funcionario_ch


def test_mash_credito_funcionario_suspeito_saldo_aberto():
    base = [{
        "id_entidade": 99,
        "nome_funcionario": "Teste",
        "cpf": "",
        "ativo": 1,
        "limite_prazo": 400.0,
        "limite_vale": 0.0,
    }]
    cr_open = [{
        "id_filial": 1,
        "id_contasreceber": 1,
        "id_entidade": 99,
        "valor": 500.0,
        "vlr_pago": 0.0,
        "dtapgto_raw": "",
        "historico": "venda a prazo",
        "observacao": "",
        "dt_evento": "2026-08-15T10:00:00",
        "dt_vencimento": "2026-08-20T10:00:00",
        "dt_pagamento": None,
    }]
    with patch("app.db_clickhouse.query_dict", side_effect=[base, cr_open]), patch(
        "app.db_clickhouse.execute_command"
    ), patch("app.db_clickhouse.insert_batch", side_effect=lambda _t, rows, **_k: len(rows)):
        out = mash_fraud_credito_funcionario_ch(1, 202608)
    assert out["resumo"] == 1

    captured: list = []

    def _capture(table, rows, **kw):
        captured.extend(rows)
        return len(rows)

    with patch("app.db_clickhouse.query_dict", side_effect=[base, cr_open]), patch(
        "app.db_clickhouse.execute_command"
    ), patch("app.db_clickhouse.insert_batch", side_effect=_capture):
        mash_fraud_credito_funcionario_ch(1, 202608)
    resumo = [r for r in captured if r.get("nome_funcionario") == "Teste"][0]
    assert resumo["status"] == "Suspeito"
    assert resumo["saldo_aberto_geral"] == 500.0
    assert "extrapola" in resumo["motivos"]
