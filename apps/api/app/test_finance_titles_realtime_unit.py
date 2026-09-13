from datetime import date

import pytest

from app import repos_mart_realtime


def test_finance_titles_sort_allowlist_ignores_raw_column():
    from app.repos_mart_realtime import _finance_titles_order_sql

    default = _finance_titles_order_sql(None, None, "toString(id_filial)")
    assert "dt_vencimento ASC" in default
    assert "id_titulo ASC" in default
    injected = _finance_titles_order_sql("valor; DROP TABLE x", "desc", "toString(id_filial)")
    assert injected == default
    allowed = _finance_titles_order_sql("valor_aberto", "desc", "toString(id_filial)")
    assert allowed.startswith("valor_aberto DESC")
    assert "id_titulo ASC" in allowed


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
    from contextlib import contextmanager
    from datetime import datetime, timezone

    from app.services import finance_titles as ft

    calls = []
    covered = datetime(2026, 9, 11, 17, 0, tzinfo=timezone.utc)

    class _Rows:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

        def fetchall(self):
            return []

    class _Conn:
        def execute(self, sql, params=None):
            sql_l = str(sql).lower()
            calls.append(("sql", sql_l[:80]))
            if "clock_timestamp" in sql_l or "stg_max" in sql_l:
                return _Rows({"stg_max": covered, "read_started_at": covered})
            if "set_watermark" in sql_l:
                calls.append(("confirm", params))
                return _Rows(None)
            return _Rows(None)

        def commit(self):
            calls.append(("commit",))

    @contextmanager
    def _fake_conn(**_kwargs):
        yield _Conn()

    monkeypatch.setattr(ft, "get_conn", _fake_conn)
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

    result = ft.publish_finance_titles("platform_master", 1, days=30)
    assert result.confirmed is True
    assert result.inserted == 1
    assert result.covered_through == covered
    assert any("DELETE WHERE id_empresa" in c[1] for c in calls if c[0] == "exec")
    assert any(c[0] == "ins" for c in calls)
    assert any(c[0] == "confirm" for c in calls)


def test_finance_titles_baixa_during_publish_stays_pending(monkeypatch):
    """Baixa com received_at > covered_through após confirm → próximo ciclo pendente."""
    from datetime import datetime, timezone

    from app.services import finance_titles as ft
    from app.services import etl_orchestrator as orch

    w0 = datetime(2026, 9, 11, 17, 0, 0, tzinfo=timezone.utc)
    w1 = datetime(2026, 9, 11, 17, 0, 5, tzinfo=timezone.utc)  # chegou durante CH write

    # Após publish confirmado em W0, STG já tem W1.
    monkeypatch.setattr(
        ft,
        "probe_finance_stg_coverage",
        lambda _conn, _emp: ft.FinanceStgCoverage(
            ok=True, empty=False, max_received_at=w1, covered_through=w1
        ),
    )
    monkeypatch.setattr(
        ft,
        "read_finance_titles_cover_watermark",
        lambda _conn, _emp: (True, w0),
    )
    assert orch._finance_titles_publish_pending(object(), 1, finance_changed=False) is True


def test_finance_titles_publish_interrupted_does_not_confirm(monkeypatch):
    from contextlib import contextmanager
    from datetime import datetime, timezone

    from app.services import finance_titles as ft

    covered = datetime(2026, 9, 11, 17, 0, tzinfo=timezone.utc)
    confirmed = {"ok": False}

    class _Rows:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _Conn:
        def execute(self, sql, params=None):
            sql_l = str(sql).lower()
            if "clock_timestamp" in sql_l or "stg_max" in sql_l:
                return _Rows({"stg_max": covered, "read_started_at": covered})
            if "set_watermark" in sql_l:
                confirmed["ok"] = True
            return _Rows(None)

        def commit(self):
            pass

    @contextmanager
    def _fake_conn(**_kwargs):
        yield _Conn()

    monkeypatch.setattr(ft, "get_conn", _fake_conn)
    monkeypatch.setattr(ft, "fetch_finance_titles", lambda *_a, **_k: [])
    monkeypatch.setattr(
        ft,
        "execute_command",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("CH mutation aborted")),
    )

    result = ft.publish_finance_titles("platform_master", 1, days=30)
    assert result.confirmed is False
    assert "aborted" in (result.error or "")
    assert confirmed["ok"] is False


def test_finance_titles_probe_query_failure_is_not_empty():
    from app.services.finance_titles import FinanceStgCoverage, probe_finance_stg_coverage

    class _Boom:
        def execute(self, *_a, **_k):
            raise RuntimeError("statement timeout")

    cov = probe_finance_stg_coverage(_Boom(), 1)
    assert cov.ok is False
    assert cov.empty is False
    assert cov.covered_through is None
    assert "timeout" in cov.error


def test_finance_titles_publish_pending_false_on_probe_failure_is_forbidden():
    """Erro de consulta STG não pode virar 'sem pendência'."""
    from app.services import etl_orchestrator as orch

    class _Boom:
        def execute(self, *_a, **_k):
            raise RuntimeError("connection reset")

    # probe real falha → pending True (mesmo sem finance_changed).
    assert (
        orch._finance_titles_publish_pending(_Boom(), 1, finance_changed=False) is True
    )


def test_finance_titles_empty_publish_confirms_clock_cover(monkeypatch):
    """STG vazio legítimo: confirma covered_through=clock e inserted=0."""
    from contextlib import contextmanager
    from datetime import datetime, timezone

    from app.services import finance_titles as ft

    clock = datetime(2026, 9, 11, 18, 30, tzinfo=timezone.utc)
    confirms = []

    class _Rows:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _Conn:
        def execute(self, sql, params=None):
            sql_l = str(sql).lower()
            if "clock_timestamp" in sql_l or "stg_max" in sql_l:
                return _Rows({"stg_max": None, "read_started_at": clock})
            if "set_watermark" in sql_l:
                confirms.append(params)
                return _Rows(None)
            return _Rows(None)

        def commit(self):
            pass

    @contextmanager
    def _fake_conn(**_kwargs):
        yield _Conn()

    monkeypatch.setattr(ft, "get_conn", _fake_conn)
    monkeypatch.setattr(ft, "fetch_finance_titles", lambda *_a, **_k: [])
    monkeypatch.setattr(ft, "execute_command", lambda *_a, **_k: None)
    monkeypatch.setattr(ft, "insert_batch", lambda *_a, **_k: 0)

    result = ft.publish_finance_titles("platform_master", 1, days=30)
    assert result.confirmed is True
    assert result.empty is True
    assert result.inserted == 0
    assert result.covered_through == clock
    assert confirms and confirms[0][2] == clock


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
