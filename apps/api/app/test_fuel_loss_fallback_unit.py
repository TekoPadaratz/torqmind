"""Fallback oculto: filtro = hoje sem leitura do sensor → última leitura."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app import repos_mart_realtime as rt


def test_fuel_loss_fallback_shifts_single_day_today(monkeypatch):
    today = date(2026, 8, 21)
    last = date(2026, 8, 20)

    def _qd(sql, params=None):
        s = str(sql)
        if "max(dia)" in s.replace(" ", "").lower() or "max(dia)" in s:
            return [{"max_dia": last}]
        if "mart_inventory_tank_readings_rt" in s:
            return []
        return []

    monkeypatch.setattr(rt, "business_today", lambda _e: today)
    monkeypatch.setattr(rt, "query_dict", _qd)
    monkeypatch.setattr(rt, "_branch_clause", lambda *_a, **_k: "")

    out = rt.inventory_fuel_loss_overview(
        "platform_master",
        1,
        None,
        today,
        today,
        refresh=False,
    )
    assert out["leitura_fallback_hoje"] is True
    assert out["ultima_leitura_disponivel"] == last.isoformat()
    assert out["dt_ini"] == last.isoformat()
    assert out["dt_fim"] == last.isoformat()


def test_fuel_loss_respects_historical_date(monkeypatch):
    today = date(2026, 8, 21)
    target = date(2026, 8, 10)

    def _qd(sql, params=None):
        return []

    monkeypatch.setattr(rt, "business_today", lambda _e: today)
    monkeypatch.setattr(rt, "query_dict", _qd)
    monkeypatch.setattr(rt, "_branch_clause", lambda *_a, **_k: "")

    out = rt.inventory_fuel_loss_overview(
        "platform_master",
        1,
        None,
        target,
        target,
        refresh=False,
    )
    assert out["leitura_fallback_hoje"] is False
    assert out["dt_ini"] == target.isoformat()
    assert out["dt_fim"] == target.isoformat()
