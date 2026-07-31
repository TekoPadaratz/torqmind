"""Unit tests for inventory fuel overview helpers / ACL registration."""
from __future__ import annotations

from app.permissions import SCREEN_REGISTRY, SENSITIVE_FIELD_NAMES
from app.repos_mart_realtime import REALTIME_FUNCTIONS


def test_inventory_screen_registered():
    assert "inventory" in SCREEN_REGISTRY
    assert SCREEN_REGISTRY["inventory"]["category"] == "Comercial"
    assert SCREEN_REGISTRY["inventory"]["has_sensitive"] is True


def test_fuel_loss_screen_registered():
    assert "fuel_loss" in SCREEN_REGISTRY
    assert SCREEN_REGISTRY["fuel_loss"]["category"] == "Operação"
    assert SCREEN_REGISTRY["fuel_loss"]["label"] == "Aferição de Combustível"


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
