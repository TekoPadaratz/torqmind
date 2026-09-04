"""Unit tests for commission discounts overview (no CH)."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app.services.commission_discounts import commission_discounts_overview


def test_commission_discounts_overview_returns_period_without_year_nameerror():
    """Regressão: retorno usava year/month indefinidos → 500 no card de descontos."""
    venda = [
        {
            "id_filial": 14458,
            "dt_venda": date(2026, 8, 20),
            "documento": "2",
            "cliente": "A",
            "vendedor": "V",
            "produto": "P",
            "preco_aplicado": 10.0,
            "desconto_rs": 1.0,
            "desconto_pct": 10.0,
        },
        {
            "id_filial": 14458,
            "dt_venda": date(2026, 8, 21),
            "documento": "1",
            "cliente": "B",
            "vendedor": "V",
            "produto": "P",
            "preco_aplicado": 10.0,
            "desconto_rs": 2.0,
            "desconto_pct": 20.0,
        },
    ]

    with patch("app.services.commission_discounts.query_dict", side_effect=[venda, []]):
        with patch("app.services.commission_discounts.apelido_for", return_value="VR 01"):
            out = commission_discounts_overview(
                1,
                date(2026, 8, 21),
                date(2026, 9, 20),
                id_filiais=[14458],
                limit=50,
            )

    assert "ano" not in out or out.get("dt_ini")
    assert out["dt_ini"] == "2026-08-21"
    assert out["dt_fim"] == "2026-09-20"
    assert out["total"] == 2
    # Data DESC dentro da filial
    assert [r["documento"] for r in out["items"]] == ["1", "2"]
