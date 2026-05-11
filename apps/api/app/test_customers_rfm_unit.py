from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from app import repos_mart


class _FetchOneCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _CustomersRfmConnStub:
    def __init__(self, *, latest_snapshot_row, mart_snapshot_row=None, dw_snapshot_row=None):
        self.latest_snapshot_row = latest_snapshot_row
        self.mart_snapshot_row = mart_snapshot_row
        self.dw_snapshot_row = dw_snapshot_row
        self.calls: list[tuple[str, list | tuple]] = []

    def execute(self, sql, params):
        params_list = list(params) if isinstance(params, (list, tuple)) else [params]
        self.calls.append((sql, params_list))

        if "SELECT MAX(dt_ref)::date AS dt_ref" in sql and "FROM mart.customer_rfm_daily" in sql:
            return _FetchOneCursor(self.latest_snapshot_row)
        if "COALESCE(SUM(monetary_90),0)::numeric(18,2) AS faturamento_90d" in sql:
            return _FetchOneCursor(self.mart_snapshot_row)
        if "FROM dw.fact_venda v" in sql:
            return _FetchOneCursor(self.dw_snapshot_row)
        raise AssertionError(f"Unexpected SQL in test stub: {sql[:220]}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ARG002
        return False


class CustomersRfmSnapshotUnitTest(unittest.TestCase):
    def test_customers_rfm_prefers_materialized_snapshot(self) -> None:
        conn = _CustomersRfmConnStub(
            latest_snapshot_row={"dt_ref": date(2026, 5, 11)},
            mart_snapshot_row={
                "clientes_identificados": 42,
                "ativos_7d": 18,
                "em_risco_30d": 7,
                "faturamento_90d": Decimal("12345.67"),
            },
        )

        with patch("app.repos_mart.get_conn", return_value=conn):
            result = repos_mart.customers_rfm_snapshot("ADMIN", 1, None, date(2026, 5, 11))

        self.assertEqual(result["clientes_identificados"], 42)
        self.assertEqual(result["ativos_7d"], 18)
        self.assertEqual(result["em_risco_30d"], 7)
        self.assertEqual(result["faturamento_90d"], Decimal("12345.67"))
        self.assertTrue(any("FROM mart.customer_rfm_daily" in sql for sql, _ in conn.calls))
        self.assertFalse(any("FROM dw.fact_venda v" in sql for sql, _ in conn.calls))

    def test_customers_rfm_falls_back_to_dw_when_snapshot_is_missing(self) -> None:
        conn = _CustomersRfmConnStub(
            latest_snapshot_row={"dt_ref": None},
            dw_snapshot_row={
                "clientes_identificados": 10,
                "ativos_7d": 4,
                "em_risco_30d": 3,
                "faturamento_90d": Decimal("980.00"),
            },
        )

        with patch("app.repos_mart.get_conn", return_value=conn):
            result = repos_mart.customers_rfm_snapshot("ADMIN", 1, None, date(2026, 5, 11))

        self.assertEqual(result["clientes_identificados"], 10)
        self.assertEqual(result["ativos_7d"], 4)
        self.assertEqual(result["em_risco_30d"], 3)
        self.assertEqual(result["faturamento_90d"], Decimal("980.00"))
        self.assertTrue(any("FROM dw.fact_venda v" in sql for sql, _ in conn.calls))


if __name__ == "__main__":
    unittest.main()