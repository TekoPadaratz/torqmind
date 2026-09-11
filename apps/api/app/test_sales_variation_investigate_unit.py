"""Unit tests — investigação de variação de vendas (Phase 3 jornada 1)."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app.services import sales_variation_investigate as svi


def test_prior_equal_period_same_length():
    ini, fim = date(2026, 9, 5), date(2026, 9, 11)
    p_ini, p_fim = svi.prior_equal_period(ini, fim)
    assert (fim - ini).days == (p_fim - p_ini).days
    assert p_fim == date(2026, 9, 4)
    assert p_ini == date(2026, 8, 29)


def test_empty_branch_scope_does_not_widen_access():
    # id_filial=[] → _branch_clause AND 0
    out = svi.investigate_sales_variation(
        "tenant_manager", 1, [], date(2026, 9, 1), date(2026, 9, 7), as_of=date(2026, 9, 11)
    )
    assert out["status"] == "forbidden_scope"


def test_period_too_long():
    out = svi.investigate_sales_variation(
        "owner", 1, None, date(2026, 1, 1), date(2026, 6, 1), as_of=date(2026, 9, 11)
    )
    assert out["status"] == "period_too_long"


def test_unavailable_is_not_zero_totals():
    with patch.object(svi, "_totals", side_effect=RuntimeError("ch down")):
        out = svi.investigate_sales_variation(
            "owner", 1, None, date(2026, 9, 1), date(2026, 9, 7), as_of=date(2026, 9, 11)
        )
    assert out["status"] == "unavailable"
    assert out["totals"] is None


def test_no_data_is_not_zero_variation():
    empty = {
        "faturamento": 0.0,
        "qtd_vendas": 0,
        "valor_cancelado": 0.0,
        "has_data": False,
        "n_rows": 0,
        "last_updated": None,
    }
    with (
        patch.object(svi, "_totals", return_value=empty),
        patch.object(svi, "_by_filial", return_value=[]),
        patch.object(svi, "_by_group", return_value=[]),
        patch.object(svi, "_by_hour", return_value=[]),
    ):
        out = svi.investigate_sales_variation(
            "owner", 1, None, date(2026, 9, 1), date(2026, 9, 7), as_of=date(2026, 9, 11)
        )
    assert out["status"] == "no_data"
    assert out["totals"] is None


def test_decomposition_labels_contribution_not_cause():
    def _totals(_e, _f, dt_ini, dt_fim):
        # current week higher
        if dt_ini >= date(2026, 9, 5):
            return {
                "faturamento": 1000.0,
                "qtd_vendas": 10,
                "valor_cancelado": 0.0,
                "has_data": True,
                "n_rows": 7,
                "last_updated": None,
            }
        return {
            "faturamento": 800.0,
            "qtd_vendas": 8,
            "valor_cancelado": 0.0,
            "has_data": True,
            "n_rows": 7,
            "last_updated": None,
        }

    with (
        patch.object(svi, "_totals", side_effect=_totals),
        patch.object(
            svi,
            "_by_filial",
            side_effect=lambda *_a, **_k: (
                [{"id_filial": 10, "faturamento": 700, "qtd_vendas": 7}]
                if _a[2] >= date(2026, 9, 5)
                else [{"id_filial": 10, "faturamento": 400, "qtd_vendas": 4}]
            ),
        ),
        patch.object(
            svi,
            "_by_group",
            side_effect=lambda *_a, **_k: (
                [{"id_grupo_produto": 1, "grupo_nome": "COMBUSTIVEIS", "faturamento": 600}]
                if _a[2] >= date(2026, 9, 5)
                else [{"id_grupo_produto": 1, "grupo_nome": "COMBUSTIVEIS", "faturamento": 500}]
            ),
        ),
        patch.object(
            svi,
            "_by_hour",
            side_effect=lambda *_a, **_k: (
                [{"hora": 18, "faturamento": 200}]
                if _a[2] >= date(2026, 9, 5)
                else [{"hora": 18, "faturamento": 100}]
            ),
        ),
    ):
        out = svi.investigate_sales_variation(
            "owner", 1, None, date(2026, 9, 5), date(2026, 9, 11), as_of=date(2026, 9, 11)
        )

    assert out["status"] == "ok"
    assert out["totals"]["delta"] == 200.0
    assert out["comparison"]["basis"] == "prior_equal_length"
    assert out["comparison"]["period_incomplete"] is True
    contrib = [f for f in out["factors"] if f["kind"] == "contribution"]
    hypo = [f for f in out["factors"] if f["kind"] == "hypothesis"]
    assert contrib
    assert all(f.get("causality") == "not_proven" for f in contrib)
    assert hypo
    assert all(f.get("causality") == "hypothesis" for f in hypo)
    assert "proven_cause" in out["legend"]


def test_incomplete_period_warning_present():
    filled = {
        "faturamento": 100.0,
        "qtd_vendas": 1,
        "valor_cancelado": 0.0,
        "has_data": True,
        "n_rows": 1,
        "last_updated": None,
    }
    with (
        patch.object(svi, "_totals", return_value=filled),
        patch.object(svi, "_by_filial", return_value=[]),
        patch.object(svi, "_by_group", return_value=[]),
        patch.object(svi, "_by_hour", return_value=[]),
    ):
        out = svi.investigate_sales_variation(
            "owner", 1, 10, date(2026, 9, 11), date(2026, 9, 11), as_of=date(2026, 9, 11)
        )
    assert out["comparison"]["period_incomplete"] is True
    assert out["warnings"]
