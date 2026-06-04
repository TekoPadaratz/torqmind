"""Unit tests for payment-mix reconciliation against commercial sales.

The cash/payments overviews must guarantee that the sum of the payment mix
matches commercial sales for the same scope. Any residual gap (unrecorded
fiado/prazo, leftover troca-de-forma ghosts, rounding) is surfaced as an
explicit, non-hidden "Não conciliado (operacional)" line instead of being
silently dropped or bucketed into a real payment form.
"""
from __future__ import annotations

from app.repos_mart_realtime import _reconcile_payment_mix


def _forms(*pairs):
    return [
        {"label": label, "category": "cat", "total_valor": valor, "qtd_comprovantes": 1}
        for label, valor in pairs
    ]


def test_simple_single_form_matches_sales():
    mix, total, dif = _reconcile_payment_mix(_forms(("DINHEIRO", 100.0)), 100.0)
    assert total == 100.0
    assert dif == 0.0
    assert all(not p.get("is_reconciliation") for p in mix)
    assert round(sum(p["total_valor"] for p in mix), 2) == 100.0


def test_legitimate_split_is_preserved_and_sums_to_sales():
    mix, total, dif = _reconcile_payment_mix(_forms(("DINHEIRO", 50.0), ("CARTÃO", 50.0)), 100.0)
    assert total == 100.0
    assert dif == 0.0
    # Both forms preserved, no reconciliation line.
    assert len([p for p in mix if not p.get("is_reconciliation")]) == 2
    assert not any(p.get("is_reconciliation") for p in mix)


def test_payments_below_sales_creates_positive_reconciliation_line():
    # e.g. fiado/prazo not recorded as a payment form
    mix, total, dif = _reconcile_payment_mix(_forms(("DINHEIRO", 80.0)), 100.0)
    assert total == 80.0
    assert dif == 20.0
    recon = [p for p in mix if p.get("is_reconciliation")]
    assert len(recon) == 1
    assert recon[0]["total_valor"] == 20.0
    assert recon[0]["category"] == "reconciliation"
    # Mix now sums to sales.
    assert round(sum(p["total_valor"] for p in mix), 2) == 100.0


def test_payments_above_sales_creates_negative_reconciliation_line():
    # e.g. residual troca-de-forma ghost still inflating payments
    mix, total, dif = _reconcile_payment_mix(_forms(("DINHEIRO", 120.0)), 100.0)
    assert total == 120.0
    assert dif == -20.0
    recon = [p for p in mix if p.get("is_reconciliation")]
    assert len(recon) == 1
    assert recon[0]["total_valor"] == -20.0
    # Mix still reconciles to sales.
    assert round(sum(p["total_valor"] for p in mix), 2) == 100.0


def test_no_reconciliation_line_within_one_cent():
    mix, total, dif = _reconcile_payment_mix(_forms(("PIX", 100.004)), 100.0)
    assert abs(dif) <= 0.01
    assert not any(p.get("is_reconciliation") for p in mix)


def test_zero_sales_no_reconciliation_when_no_payments():
    mix, total, dif = _reconcile_payment_mix([], 0.0)
    assert total == 0.0
    assert dif == 0.0
    assert mix == []


def test_reconciliation_line_does_not_pollute_real_forms():
    mix, _total, _dif = _reconcile_payment_mix(_forms(("DINHEIRO", 80.0)), 100.0)
    real_forms = [p for p in mix if not p.get("is_reconciliation")]
    assert len(real_forms) == 1
    assert real_forms[0]["label"] == "DINHEIRO"
    assert real_forms[0]["total_valor"] == 80.0
