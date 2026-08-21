"""Unit tests for inventory fuel overview helpers / ACL registration."""
from __future__ import annotations

import inspect
from datetime import date

from app.permissions import SCREEN_REGISTRY, SENSITIVE_FIELD_NAMES
from app.repos_mart_realtime import (
    REALTIME_FUNCTIONS,
    _inventory_period_clamp,
    estimate_tank_estoque_l,
)
from app.sales_semantics import SALE_STATUS
from app.services import inventory_fuel as inv


def test_inventory_screen_registered():
    assert "inventory" in SCREEN_REGISTRY
    assert SCREEN_REGISTRY["inventory"]["category"] == "Comercial"
    assert SCREEN_REGISTRY["inventory"]["has_sensitive"] is True
    assert SCREEN_REGISTRY["inventory"]["label"] == "Estoque de combustível"


def test_fuel_loss_screen_registered():
    assert "fuel_loss" in SCREEN_REGISTRY
    assert SCREEN_REGISTRY["fuel_loss"]["category"] == "Operação"
    assert SCREEN_REGISTRY["fuel_loss"]["label"] == "Movimentações de Combustível"


def test_inventory_fuel_diferenca_formula():
    """Dif Leitura = D − D−1; Movimentação = entradas − saídas; Diferença = Dif − Mov."""
    leit_ant, leit_atu = 10000.0, 8500.0
    saidas, entradas = 2000.0, 500.0
    dif_leitura = leit_atu - leit_ant  # -1500
    movimentacao = entradas - saidas  # -1500
    diferenca = dif_leitura - movimentacao  # 0
    assert dif_leitura == -1500.0
    assert movimentacao == -1500.0
    assert diferenca == 0.0
    # Queda sem movimento documentado → diferença negativa (vermelho)
    assert (leit_atu - leit_ant) - (0 - 100) == -1400.0
    # Subida por entrada → diferença ~0 se bate
    assert (12000.0 - 10000.0) - (2000.0 - 0.0) == 0.0


def test_estimate_tank_estoque_applies_movements_like_afericao():
    # LEITURA 10k, share 100%, saiu 1.5k no dia, entrou 0 → 8.5k
    assert estimate_tank_estoque_l(10000.0, 20000.0, 1.0, 0.0, 1500.0) == 8500.0
    # Entrada 2k no dia → sobe
    assert estimate_tank_estoque_l(10000.0, 20000.0, 1.0, 2000.0, 0.0) == 12000.0
    # Rateio 50% do produto entre 2 tanques
    assert estimate_tank_estoque_l(10000.0, 15000.0, 0.5, 0.0, 1000.0) == 9500.0
    # Não passa da capacidade nem fica negativo
    assert estimate_tank_estoque_l(100.0, 500.0, 1.0, 0.0, 9999.0) == 0.0
    assert estimate_tank_estoque_l(400.0, 500.0, 1.0, 9999.0, 0.0) == 500.0
    # Caso VR01 gasolina: não zerar com saída do dia < leitura
    assert estimate_tank_estoque_l(15631.0, 30210.0, 1.0, 0.0, 10932.731) == 4698.269


def test_inventory_period_clamp_defaults_7_days_and_caps_today():
    today = date(2026, 8, 6)
    ini, fim = _inventory_period_clamp(None, None, today)
    assert ini == date(2026, 7, 31)
    assert fim == today
    ini2, fim2 = _inventory_period_clamp(date(2026, 8, 1), date(2026, 8, 20), today)
    assert fim2 == today
    assert ini2 == date(2026, 8, 1)


def test_comprovante_ativo_sql_requires_sale_status():
    sql = inv._comprovante_ativo_sql("c")
    assert "cancelado_shadow" in sql
    assert f"= {SALE_STATUS}" in sql


def test_nfe_venda_ok_excludes_4_and_5():
    sql = inv._nfe_venda_ok_sql("nfe")
    compact = sql.replace(" ", "")
    assert "NOTIN(4,5)" in compact


def test_item_cfop_falls_back_to_payload():
    sql = inv._item_cfop_sql("i")
    assert "cfop_shadow" in sql
    assert "CFOP" in sql


def test_fetch_fuel_sales_sql_filters_active_and_nfe():
    src = inspect.getsource(inv.fetch_fuel_sales_daily)
    assert "_comprovante_ativo_sql" in src
    assert "_item_cfop_sql" in src
    assert "stg.comprovantes" in src
    assert "DTACONTA" in src
    assert "5202" in src  # devolução saída excluída


def test_fetch_fuel_entries_sql_requires_active_comprovante():
    src = inspect.getsource(inv.fetch_fuel_entries_daily)
    assert "stg.comprovantes" in src
    assert "_comprovante_ativo_sql" in src
    assert "DTACONTA" in src  # dia de negócio alinhado à leitura/saída


def test_inventory_fuel_in_realtime_functions():
    assert "inventory_fuel_overview" in REALTIME_FUNCTIONS
    assert "inventory_fuel_loss_overview" in REALTIME_FUNCTIONS
    assert "inventory_fuel_afericoes_overview" in REALTIME_FUNCTIONS


def test_afericoes_facade_exposes_realtime_only(monkeypatch):
    from app import repos_analytics
    from app import repos_mart_realtime as rt

    monkeypatch.setattr(repos_analytics.settings, "use_realtime_marts", True)

    def _fake(*_a, **_k):
        return {"source": "clickhouse", "itens": []}

    monkeypatch.setattr(rt, "inventory_fuel_afericoes_overview", _fake)
    fn = repos_analytics.inventory_fuel_afericoes_overview
    assert callable(fn)
    assert fn("platform_master", 1)["source"] == "clickhouse"


def test_custo_estoque_is_sensitive():
    assert "custo_estoque" in SENSITIVE_FIELD_NAMES
