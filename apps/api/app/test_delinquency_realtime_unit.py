"""Unit tests for the FASE 2 delinquency fix.

The realtime ClickHouse implementation used to multiply each customer by the
branch grain of ``mart_clientes_resumo`` (one row per filial) and ignored
partial baixas. It now delegates to the reconciled PostgreSQL mart
``mart.customer_delinquency_summary`` (one row per empresa/filial/cliente),
which deduplicates server-side and reconciles to the cent with the Xpert
source of truth. These tests lock in that behaviour.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app import repos_mart_realtime


def _pg_payload():
    return {
        "summary": {
            "clientes_em_aberto": 2,
            "titulos_em_aberto": 5,
            "valor_total": 1000.0,
            "titulos_ate_30d": 2,
            "valor_ate_30d": 400.0,
            "titulos_acima_30d": 3,
            "valor_acima_30d": 600.0,
            "titulos_a_vencer": 1,
            "valor_a_vencer": 100.0,
            "clientes_a_vencer": 1,
            "max_dias_atraso": 90,
        },
        "buckets": [
            {"bucket": "1_30", "label": "Até 30 dias", "valor": 400.0, "titulos": 2},
            {"bucket": "31_plus", "label": "30+ dias", "valor": 600.0, "titulos": 3},
        ],
        "customers": [
            {"id_cliente": 7383, "cliente_nome": "TRANSPORTES E.A.E.", "valor_total_vencido": 600.0,
             "valor_total_aberto": 700.0, "titulos_ate_30d": 1, "titulos_acima_30d": 2,
             "titulos_a_vencer": 1, "valor_a_vencer": 100.0, "max_dias_atraso": 90, "compras_30d": 5},
            {"id_cliente": 14049, "cliente_nome": "TRANSPORTADORA MARSARO", "valor_total_vencido": 400.0,
             "valor_total_aberto": 400.0, "titulos_ate_30d": 1, "titulos_acima_30d": 1,
             "titulos_a_vencer": 0, "valor_a_vencer": 0.0, "max_dias_atraso": 45, "compras_30d": 0},
        ],
        "sort_by": "gravity",
        "dt_ref": "2026-06-04",
    }


def test_realtime_delegates_to_pg_mart():
    as_of = date(2026, 6, 4)
    with patch.object(
        repos_mart_realtime, "_pg_customers_delinquency_overview", return_value=_pg_payload()
    ) as mocked:
        result = repos_mart_realtime.customers_delinquency_overview(
            "platform_master", 1, 14122, as_of, limit=10, sort_by="valor"
        )
    mocked.assert_called_once_with("platform_master", 1, 14122, as_of, limit=10, sort_by="valor")
    assert result == _pg_payload()


def test_realtime_payload_has_no_duplicate_clients():
    with patch.object(
        repos_mart_realtime, "_pg_customers_delinquency_overview", return_value=_pg_payload()
    ):
        result = repos_mart_realtime.customers_delinquency_overview(
            "platform_master", 1, 14122, date(2026, 6, 4)
        )
    ids = [c["id_cliente"] for c in result["customers"]]
    assert len(ids) == len(set(ids)), "delinquency payload must not duplicate id_cliente"


def test_realtime_does_not_query_clickhouse_for_delinquency():
    """Regression: the function must not hit ClickHouse (no more heavy dup query)."""
    with patch.object(
        repos_mart_realtime, "_pg_customers_delinquency_overview", return_value=_pg_payload()
    ), patch.object(repos_mart_realtime, "query_dict") as ch_query:
        repos_mart_realtime.customers_delinquency_overview(
            "platform_master", 1, 14122, date(2026, 6, 4)
        )
    ch_query.assert_not_called()
