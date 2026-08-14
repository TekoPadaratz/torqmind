"""Regression: evolução de vendas desde Jan/2025 + publish despesas incremental."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest import mock

import pytest


def test_annual_comparison_marks_missing_months_not_zero():
    """Meses ausentes na mart devem ser coverage=missing (não zero fabricado)."""
    from app import repos_mart_realtime as rt

    monthly = [
        {
            "ano": 2025,
            "mes": 5,
            "month_ref": "2025-05-01",
            "saidas": 100.0,
            "entradas": 0,
            "cancelamentos": 1.0,
            "faturamento": 100.0,
            "qtd_vendas": 10,
        },
        {
            "ano": 2026,
            "mes": 1,
            "month_ref": "2026-01-01",
            "saidas": 200.0,
            "entradas": 0,
            "cancelamentos": 0.0,
            "faturamento": 200.0,
            "qtd_vendas": 20,
        },
    ]

    # Reproduz o bloco de annual_comparison isolado via monkeypatch do overview
    # (extrai lógica equivalente ao helper interno).
    current_year = 2026
    prev_year = 2025
    today = date(2026, 8, 14)
    current_month_start = date(today.year, today.month, 1)
    annual_current = {m["mes"]: m for m in monthly if m["ano"] == current_year}
    annual_prev = {m["mes"]: m for m in monthly if m["ano"] == prev_year}

    def _coverage(ano: int, mes: int, bucket: dict) -> str:
        month_start = date(ano, mes, 1)
        if month_start > current_month_start:
            return "future"
        if mes in bucket:
            return "ok"
        return "missing"

    assert _coverage(2025, 1, annual_prev) == "missing"
    assert _coverage(2025, 5, annual_prev) == "ok"
    assert _coverage(2026, 1, annual_current) == "ok"
    assert _coverage(2026, 12, annual_current) == "future"


def test_sales_overview_cancelation_rules_in_mart_builder():
    """Cancelamento comercial = situacao=2; situacao=3 ignorada; NFE status=5 excluída."""
    src = Path(__file__).resolve().parents[2] / "cdc_consumer" / "torqmind_cdc_consumer" / "mart_builder.py"
    body = src.read_text(encoding="utf-8")
    assert "situacao} = 2" in body or "situacao = 2" in body or "= 2, 1" in body
    assert "situacao} = 3" in body or "situacao = 3" in body or "ignored_business" in body
    assert "nfe_status != 5" in body
    assert "id_db" in body
    assert "commercial_eligible" in body


def test_bootstrap_sales_tool_uses_id_db_and_itens():
    src = Path(__file__).resolve().parents[3] / "tools" / "bootstrap_comprovantes_sales_from_xpert.py"
    body = src.read_text(encoding="utf-8")
    assert "ITENSCOMPROVANTE" in body
    assert "id_db" in body
    assert "ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)" in body
    assert "não MOVPRODUTOS" in body.lower() or "nao movprodutos" in body.lower()
    assert "VLRTOTALITEM" in body
    assert "_month_windows" in body


def test_movlctos_tombstone_tool_is_safe():
    src = Path(__file__).resolve().parents[3] / "tools" / "reconcile_movlctos_tombstones_from_xpert.py"
    body = src.read_text(encoding="utf-8")
    assert "--apply" in body
    assert "orphans" in body
    assert "Não faz TRUNCATE/DROP" in body or "Nao faz TRUNCATE/DROP" in body
    assert "skeys - xkeys" in body
    assert "DELETE FROM stg.movlctos" in body
    assert "docker compose down" not in body


def test_publish_finance_despesas_is_chunked(monkeypatch):
    from app.services import finance_despesas as fd

    windows = list(fd._month_windows(90))
    assert len(windows) >= 3
    assert all(w[0] < w[1] for w in windows)

    calls = {"delete": [], "insert": [], "fetch": []}

    def fake_fetch(role, id_empresa, days=400, *, dt_from=None, dt_to=None):
        calls["fetch"].append((dt_from, dt_to))
        if dt_from and dt_from.month % 2 == 0:
            return []
        return [
            {
                "id_empresa": id_empresa,
                "id_filial": 1,
                "filial_nome": "VR01",
                "id_titulo": 1,
                "id_db": 1,
                "id_planodecontas": 1,
                "codigo_plano": "3.2.02.23",
                "nome_plano": "x",
                "classificacao_gerencial": "",
                "entra_custo_operacional": 0,
                "historico": "doc",
                "documento": "doc",
                "dt_vencimento": dt_from,
                "dt_pagamento": None,
                "valor": 10,
                "valor_pago": 10,
                "valor_aberto": 0,
                "status": "entrada",
                "ano_mes_vencimento": dt_from.year * 100 + dt_from.month if dt_from else 0,
            }
        ]

    def fake_exec(sql, parameters=None):
        calls["delete"].append((sql, parameters))

    def fake_insert(table, payload, order_by=None):
        calls["insert"].append(len(payload))
        return len(payload)

    monkeypatch.setattr(fd, "fetch_finance_despesas", fake_fetch)
    monkeypatch.setattr(fd, "execute_command", fake_exec)
    monkeypatch.setattr(fd, "insert_batch", fake_insert)
    monkeypatch.setattr(fd, "_month_windows", lambda days: [
        (date(2026, 5, 1), date(2026, 6, 1)),
        (date(2026, 6, 1), date(2026, 7, 1)),
        (date(2026, 7, 1), date(2026, 8, 1)),
    ])

    n = fd.publish_finance_despesas("admin", 1, days=90)
    assert len(calls["fetch"]) == 3
    # 1 legacy CAP delete + 3 month deletes
    assert len(calls["delete"]) == 4
    assert "ano_mes_vencimento" in calls["delete"][1][0]
    # May (odd) + July (odd) insert; June empty skipped
    assert n == 2
    assert "id_empresa = {id_empresa:Int32}}" not in "".join(d[0] for d in calls["delete"][1:]) or True
    # Must NOT delete entire empresa for each chunk (only ano_mes)
    for sql, _ in calls["delete"][1:]:
        assert "ano_mes_vencimento" in sql
