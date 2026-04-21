from __future__ import annotations

import unittest
from unittest.mock import patch

from app.cash_operational_truth import cash_open_source_sql


class CashOperationalTruthUnitTest(unittest.TestCase):
    def test_cash_open_source_sql_supports_multi_branch_scope(self) -> None:
        with patch("app.cash_operational_truth.cash_open_schema_mode", return_value="rich"):
            sql, mode = cash_open_source_sql(object(), id_empresa=7, id_filial=[11, 13], alias="live_turns")

        self.assertEqual(mode, "rich")
        self.assertIn("t.id_empresa = 7", sql)
        self.assertIn("t.id_filial = ANY(ARRAY[11,13]::int[])", sql)

    def test_cash_open_source_sql_blocks_empty_effective_scope(self) -> None:
        with patch("app.cash_operational_truth.cash_open_schema_mode", return_value="legacy"):
            sql, mode = cash_open_source_sql(object(), id_empresa=7, id_filial=[], alias="live_turns")

        self.assertEqual(mode, "legacy")
        self.assertIn("AND 1 = 0", sql)

    def test_cash_open_source_sql_uses_exact_sales_status_and_return_split(self) -> None:
        with patch("app.cash_operational_truth.cash_open_schema_mode", return_value="rich"):
            sql, _mode = cash_open_source_sql(object(), id_empresa=7, id_filial=11, alias="live_turns")

        self.assertIn("dw.fact_comprovante c", sql)
        self.assertIn("etl.cfop_direction(etl.cfop_numeric_from_payload(c.payload)) IN ('saida', 'entrada')", sql)
        self.assertIn("docs.cancelado = false AND docs.cfop_direction = 'saida'", sql)
        self.assertIn("docs.cancelado = true AND docs.cfop_direction IN ('saida', 'entrada')", sql)
        self.assertIn("docs.cancelado = false AND docs.cfop_class IN ('devolucao_saida', 'devolucao_entrada')", sql)
        self.assertIn("total_devolucoes", sql)
        self.assertIn("qtd_devolucoes", sql)
        self.assertNotIn("dw.fact_venda v", sql)
        self.assertNotIn("dw.fact_venda_item i", sql)


if __name__ == "__main__":
    unittest.main()
