"""Ticket combustível deve espelhar Top grupos (sales_groups_rt), não console."""
from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from app import repos_mart_realtime as rt


class SalesTicketCombustivelUnitTest(unittest.TestCase):
    def test_ticket_uses_sales_groups_not_console_mart(self) -> None:
        calls: list[str] = []

        def fake_query(sql, parameters=None):
            calls.append(sql)
            if "sales_groups_rt" in sql:
                return [{"valor_sum": Decimal("154461.13"), "qtd_sum": 1207}]
            if "sales_products_rt" in sql:
                return [{"litros_sum": Decimal("24223.617")}]
            self.fail(f"unexpected query: {sql[:120]}")

        with patch.object(rt, "query_dict", side_effect=fake_query):
            out = rt.sales_ticket_combustivel(
                "platform_master", 1, 14458, date(2026, 8, 5), date(2026, 8, 5)
            )

        self.assertEqual(out["valor_total"], 154461.13)
        self.assertEqual(out["qtd_abastecimentos"], 1207)
        self.assertEqual(out["ticket_medio"], 127.97)
        self.assertEqual(out["source"], "sales_groups_rt")
        self.assertTrue(any("sales_groups_rt" in q for q in calls))
        self.assertFalse(any("mart_ticket_combustivel_diaria" in q for q in calls))
        self.assertTrue(any("COMBUST" in q for q in calls))


if __name__ == "__main__":
    unittest.main()
