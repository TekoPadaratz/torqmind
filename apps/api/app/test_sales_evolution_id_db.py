"""Regressão: evolução de vendas sem join id_db↔id_filial e sem zero fabricado."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import re


def test_annual_comparison_marks_missing_and_future_not_zero():
    from app.repos_mart_realtime import _build_annual_comparison

    monthly = [
        {
            "ano": 2025,
            "mes": 3,
            "month_ref": "2025-03-01",
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
    annual = _build_annual_comparison(monthly, today=date(2026, 8, 18))
    assert annual["current_year"] == 2026
    assert annual["previous_year"] == 2025
    months = {m["mes"]: m for m in annual["months"]}
    assert months[3]["coverage_anterior"] == "ok"
    assert months[3]["saidas_anterior"] == 100.0
    assert months[1]["coverage_atual"] == "ok"
    assert months[1]["saidas_atual"] == 200.0
    assert months[3]["coverage_atual"] == "missing"
    assert months[3]["saidas_atual"] is None
    assert months[1]["coverage_anterior"] == "missing"
    assert months[1]["saidas_anterior"] is None
    assert months[12]["coverage_atual"] == "future"
    assert months[12]["saidas_atual"] is None


def test_sales_overview_monthly_sql_uses_data_key_not_dt_year():
    src = Path(__file__).resolve().parent / "repos_mart_realtime.py"
    body = src.read_text(encoding="utf-8")
    idx = body.index("def sales_overview_bundle")
    end = body.index("\ndef sales_by_hour", idx + 10)
    fn = body[idx:end]
    assert "intDiv(data_key, 10000)" in fn
    assert "toYear(dt)" not in fn
    assert "toMonth(dt)" not in fn
    assert "data_key >= 20250101" in fn


def test_sales_overview_never_equates_id_db_with_id_filial():
    src = Path(__file__).resolve().parent / "repos_mart_realtime.py"
    body = src.read_text(encoding="utf-8")
    idx = body.index("def sales_overview_bundle")
    end = body.index("\ndef sales_by_hour", idx + 10)
    fn = body[idx:end]
    forbidden = re.compile(
        r"id_db\s*=\s*[\w.]*id_filial|id_filial\s*=\s*[\w.]*id_db",
        re.IGNORECASE,
    )
    assert not forbidden.search(fn)


def test_mart_builder_sales_join_is_four_key_and_excludes_central():
    src = (
        Path(__file__).resolve().parents[2]
        / "cdc_consumer"
        / "torqmind_cdc_consumer"
        / "mart_builder.py"
    )
    body = src.read_text(encoding="utf-8")
    assert "c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial" in body
    assert "c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante" in body
    assert "_exclude_central_mirror" in body
    assert "_exclude_denegada" in body
    assert "({alias}.id_empresa, {alias}.id_db) IN" in body
    assert "{alias}.id_filial != {alias}.id_db" in body
