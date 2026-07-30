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


def test_finance_titles_search_variants_br_value_and_date():
    value_variants = repos_mart_realtime._finance_titles_search_variants("1.234,56")
    assert "1.234,56" in value_variants
    assert "1234.56" in value_variants
    assert "1234,56" in value_variants

    comma_only = repos_mart_realtime._finance_titles_search_variants("99,90")
    assert "99,90" in comma_only
    assert "99.90" in comma_only

    date_variants = repos_mart_realtime._finance_titles_search_variants("29/07/2026")
    assert "29/07/2026" in date_variants
    assert "2026-07-29" in date_variants
    assert "29/07" in date_variants

    sql = repos_mart_realtime._finance_titles_search_sql("1.234,56", {})
    assert "positionCaseInsensitiveUTF8" in sql
    assert "%d/%m/%Y" in sql
    assert "replaceAll(toString(valor), '.', ',')" in sql
