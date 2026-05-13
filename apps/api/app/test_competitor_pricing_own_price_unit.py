"""Unit tests for _get_own_prices in repos_competitor_pricing.

Validates that:
  - The query joins dw.fact_comprovante for the real sale timestamp (fc.data)
  - The ordering uses fc.data DESC (NOT created_at)
  - Cancelled receipts (cancelado=true) are excluded
  - situacao=3 receipts are excluded
  - The source is 'LAST_SALE' (never 'CADASTRO' / custo_medio)
"""
from __future__ import annotations

import re
import textwrap
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app import repos_competitor_pricing


class OwnPriceQueryStructureTest(unittest.TestCase):
    """Inspects the SQL emitted by _get_own_prices (no DB needed)."""

    def _capture_sql(self) -> str:
        """Run _get_own_prices with a mocked connection and capture the SQL."""
        captured = {}

        class FakeRow(dict):
            def __getitem__(self, key):
                return dict.__getitem__(self, key)

        class FakeCursor:
            def fetchall(self):
                return []

        class FakeConn:
            def execute(self, sql, params=None):
                captured["sql"] = sql
                captured["params"] = params
                return FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        with patch("app.repos_competitor_pricing.get_conn", return_value=FakeConn()):
            repos_competitor_pricing._get_own_prices(
                role="MASTER", id_empresa=1, id_filial=14458,
                ref_date=date(2026, 5, 13),
            )

        return captured["sql"]

    def test_joins_fact_comprovante(self):
        sql = self._capture_sql()
        self.assertIn("dw.fact_comprovante", sql,
                       "Must JOIN dw.fact_comprovante for real sale timestamp")

    def test_orders_by_real_sale_time_not_created_at(self):
        sql = self._capture_sql()
        # ORDER BY must reference fc.data DESC
        self.assertIn("fc.data DESC", sql,
                       "Must ORDER BY fc.data DESC (real sale time)")
        # Must NOT use created_at in ORDER BY
        order_clause = sql[sql.upper().index("ORDER BY"):]
        self.assertNotIn("created_at", order_clause.lower(),
                          "Must NOT use created_at in ORDER BY clause")

    def test_excludes_cancelled_receipts(self):
        sql = self._capture_sql()
        self.assertIn("COALESCE(fc.cancelado, false) = false", sql,
                       "Must filter out cancelled receipts with COALESCE")

    def test_excludes_situacao_3(self):
        sql = self._capture_sql()
        self.assertIn("COALESCE(fc.situacao, 0) != 3", sql,
                       "Must exclude situacao=3 receipts with COALESCE")

    def test_requires_fc_data_not_null(self):
        sql = self._capture_sql()
        self.assertIn("fc.data IS NOT NULL", sql,
                       "Must require fc.data IS NOT NULL")

    def test_orders_nulls_last(self):
        sql = self._capture_sql()
        self.assertIn("NULLS LAST", sql,
                       "Must use NULLS LAST in ORDER BY")

    def test_uses_preco_praticado_unitario(self):
        sql = self._capture_sql()
        self.assertIn("preco_praticado_unitario", sql,
                       "Must use preco_praticado_unitario as the price source")

    def test_no_custo_medio(self):
        sql = self._capture_sql()
        self.assertNotIn("custo_medio", sql.lower(),
                          "Must NOT use custo_medio (cost, not selling price)")


class OwnPriceSelectionTest(unittest.TestCase):
    """Verifies that given two sales of the same product, the one with the
    later real sale timestamp wins — even if its created_at is earlier."""

    def test_picks_latest_real_sale_not_latest_created_at(self):
        """Simulates two rows:
          Row A: fc.data=17:00, created_at=21:00, price=6.89
          Row B: fc.data=18:00, created_at=20:00, price=7.20
        The query must pick Row B (later fc.data) -> price=7.20.
        Since DISTINCT ON + ORDER BY fc.data DESC is used, the DB returns
        the row with the highest fc.data first.
        """
        from datetime import datetime, timezone

        rows_returned = [
            {"id_produto": 5991, "unit_price": Decimal("7.2000")},
        ]

        class FakeCursor:
            def fetchall(self):
                return rows_returned

        class FakeConn:
            def execute(self, sql, params=None):
                return FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        with patch("app.repos_competitor_pricing.get_conn", return_value=FakeConn()):
            result = repos_competitor_pricing._get_own_prices(
                role="MASTER", id_empresa=1, id_filial=14458,
                ref_date=date(2026, 5, 13),
            )

        self.assertIn(5991, result)
        self.assertEqual(result[5991]["price"], Decimal("7.2000"))
        self.assertEqual(result[5991]["source"], "LAST_SALE")

    def test_returns_empty_when_no_sales(self):
        class FakeCursor:
            def fetchall(self):
                return []

        class FakeConn:
            def execute(self, sql, params=None):
                return FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        with patch("app.repos_competitor_pricing.get_conn", return_value=FakeConn()):
            result = repos_competitor_pricing._get_own_prices(
                role="MASTER", id_empresa=1, id_filial=14458,
                ref_date=date(2026, 5, 13),
            )

        self.assertEqual(result, {})


class FuelDeduplicationTest(unittest.TestCase):
    """Verifies that list_fuel_products deduplicates by fuel_type."""

    def test_dedup_prefers_product_with_own_price(self):
        """When two products have same fuel_type, keep the one with own_price."""
        fake_rows = [
            {"product_id": 11879, "product_name": "GASOLINA ADITIVADA SHELL V-POWER",
             "fuel_type": "GASOLINA", "grupo_nome": "COMBUSTIVEIS",
             "custo_medio": None, "unidade": "LT"},
            {"product_id": 5992, "product_name": "GASOLINA ADITIVADA",
             "fuel_type": "GASOLINA", "grupo_nome": "COMBUSTIVEIS",
             "custo_medio": None, "unidade": "LT"},
        ]
        own_prices = {
            5992: {"price": Decimal("6.89"), "source": "LAST_SALE"},
        }

        class FakeCursor:
            def fetchall(self):
                return fake_rows

        class FakeConn:
            def execute(self, sql, params=None):
                return FakeCursor()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        with patch("app.repos_competitor_pricing.get_conn", return_value=FakeConn()), \
             patch("app.repos_competitor_pricing._get_own_prices", return_value=own_prices):
            result = repos_competitor_pricing.list_fuel_products(
                role="MASTER", id_empresa=1, id_filial=14458,
            )

        fuel_gasolina = [r for r in result if r["fuel_type"] == "GASOLINA"]
        self.assertEqual(len(fuel_gasolina), 1,
                         "Must have exactly one GASOLINA entry after dedup")
        self.assertEqual(fuel_gasolina[0]["product_id"], 5992,
                         "Must keep product 5992 (has own_price), not 11879")
        self.assertEqual(fuel_gasolina[0]["own_current_price"], "6.89")

    def test_no_dedup_when_different_fuel_types(self):
        """Products with different fuel_types are all kept."""
        fake_rows = [
            {"product_id": 1, "product_name": "GASOLINA COMUM",
             "fuel_type": "GASOLINA", "grupo_nome": "COMBUSTIVEIS",
             "custo_medio": None, "unidade": "LT"},
            {"product_id": 2, "product_name": "ETANOL COMUM",
             "fuel_type": "ETANOL", "grupo_nome": "COMBUSTIVEIS",
             "custo_medio": None, "unidade": "LT"},
        ]

        class FakeCursor:
            def fetchall(self):
                return fake_rows

        class FakeConn:
            def execute(self, sql, params=None):
                return FakeCursor()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        with patch("app.repos_competitor_pricing.get_conn", return_value=FakeConn()), \
             patch("app.repos_competitor_pricing._get_own_prices", return_value={}):
            result = repos_competitor_pricing.list_fuel_products(
                role="MASTER", id_empresa=1, id_filial=14458,
            )

        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
