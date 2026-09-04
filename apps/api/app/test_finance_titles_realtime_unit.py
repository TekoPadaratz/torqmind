from datetime import date

import pytest

from app import repos_mart_realtime


def test_finance_titles_preset_sql_carteira_aberta_vs_periodo():
    """Presets = carteira aberta (today); sem preset = janela dt_ini/dt_fim."""
    default = repos_mart_realtime._finance_titles_period_or_preset_sql(None)
    assert "dt_vencimento BETWEEN {dt_ini:Date} AND {dt_fim:Date}" in default
    assert "status = 'vencido'" in default

    vencidos = repos_mart_realtime._finance_titles_period_or_preset_sql("vencidos")
    assert "status = 'vencido'" in vencidos
    # Não clipar vencidos em dt_fim (tela muda com mês passado).
    assert "dt_fim" not in vencidos
    assert "today()" not in vencidos

    d7 = repos_mart_realtime._finance_titles_period_or_preset_sql("a_vencer_7d")
    assert "today()" in d7
    assert "today() + 7" in d7
    assert "dt_ini" not in d7

    mes = repos_mart_realtime._finance_titles_period_or_preset_sql("a_vencer_mes")
    assert "toLastDayOfMonth(today())" in mes
    assert "dt_ini" not in mes

    av = repos_mart_realtime._finance_titles_period_or_preset_sql("a_vencer")
    assert "dt_vencimento >= today()" in av
    assert "dt_ini" not in av

    with pytest.raises(ValueError, match="preset inválido"):
        repos_mart_realtime._finance_titles_period_or_preset_sql("pago")


def test_finance_titles_search_variants_br_date_and_money():
    variants = repos_mart_realtime._finance_titles_search_variants("1.234,56")
    assert "1234.56" in variants
    iso = repos_mart_realtime._finance_titles_search_variants("2026-08-15")
    assert "15/08/2026" in iso


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


def test_finance_titles_publish_deletes_before_insert(monkeypatch):
    from app.services import finance_titles as ft

    calls = []

    monkeypatch.setattr(
        ft,
        "fetch_finance_titles",
        lambda *_a, **_k: [
            {
                "id_empresa": 1,
                "id_filial": 10,
                "tipo_titulo": 0,
                "id_titulo": 99,
                "id_db": 1,
                "id_entidade": 1,
                "entidade_nome": "FORN",
                "nro_documento": "NF1",
                "dt_lancamento": date(2026, 8, 1),
                "dt_vencimento": date(2026, 8, 10),
                "valor": 100,
                "valor_pago": 0,
                "valor_aberto": 100,
                "status": "a_vencer",
            }
        ],
    )

    def _exec(sql, params=None):
        calls.append(("exec", sql, params))

    def _ins(table, payload, order_by=None):
        calls.append(("ins", table, len(payload)))
        return len(payload)

    monkeypatch.setattr(ft, "execute_command", _exec)
    monkeypatch.setattr(ft, "insert_batch", _ins)

    n = ft.publish_finance_titles("platform_master", 1, days=30)
    assert n == 1
    assert any("DELETE WHERE id_empresa" in c[1] for c in calls if c[0] == "exec")
    assert any(c[0] == "ins" for c in calls)


def test_finance_titles_fetch_excludes_deletar():
    from app.services import finance_titles as ft
    import inspect

    assert "DELETAR" in ft._NOT_DELETED
    src = inspect.getsource(ft.fetch_finance_titles)
    assert "_NOT_DELETED" in src
    assert "baixa_pagar" in src
    assert "VALORBAIXA" in src
    assert "contaspagarbaixa" in src
    assert "contasreceberbaixa" in src
