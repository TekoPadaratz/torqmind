from datetime import date

from app import repos_mart_realtime


def test_finance_titles_realtime_contract_and_kpis(monkeypatch):
    assert "finance_titles_overview" in repos_mart_realtime.REALTIME_FUNCTIONS

    monkeypatch.setattr(
        repos_mart_realtime,
        "query_dict",
        lambda *args, **kwargs: [
            {"tipo_titulo": 1, "faixa": "vencido", "valor_em_aberto": 10},
            {"tipo_titulo": 1, "faixa": "vence_7d", "valor_em_aberto": 20},
            {"tipo_titulo": 0, "faixa": "vencido", "valor_em_aberto": 30},
            {"tipo_titulo": 0, "faixa": "futuro", "valor_em_aberto": 40},
            {"tipo_titulo": 1, "faixa": "pago", "valor_em_aberto": 999},
        ],
    )

    kpis = repos_mart_realtime.finance_kpis(
        "platform_master", 1, None, date(2026, 1, 1), date(2026, 1, 31)
    )

    assert kpis["receber_aberto"] == 30
    assert kpis["pagar_aberto"] == 70
    assert kpis["receber_vencido"] == 10
    assert kpis["pagar_vencido"] == 30
