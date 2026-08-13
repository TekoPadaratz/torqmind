"""Unit tests for manager commission net formula and defaults."""
from __future__ import annotations

import unittest

from app.repos_manager_commission import (
    SALES_BASE_EXCLUDED_GROUP_IDS,
    SALES_EXCLUDED_CFOPS,
    _cfop_sales_predicate_sql,
    _nfe_documento,
    _date_key_iso,
    net_commission,
)


class ManagerCommissionFormulaTests(unittest.TestCase):
    def test_net_commission_formula(self):
        # bruta 1000 - perdas 100 + sobras_est 50 - furos 200 + sobras_cx 30 = 780
        self.assertEqual(
            net_commission(
                comissao_bruta=1000,
                perdas_estoque=100,
                sobras_estoque=50,
                furos_caixa=200,
                sobras_caixa=30,
            ),
            780.0,
        )

    def test_net_can_go_negative(self):
        self.assertEqual(
            net_commission(
                comissao_bruta=100,
                perdas_estoque=500,
                sobras_estoque=0,
                furos_caixa=0,
                sobras_caixa=0,
            ),
            -400.0,
        )

    def test_sales_excluded_defaults(self):
        self.assertIn(1, SALES_BASE_EXCLUDED_GROUP_IDS)
        self.assertIn(40, SALES_BASE_EXCLUDED_GROUP_IDS)
        self.assertNotIn(5, SALES_BASE_EXCLUDED_GROUP_IDS)

    def test_calc_branch_row_defaults_publish_false(self):
        import inspect
        from app.repos_manager_commission import calc_branch_row

        params = inspect.signature(calc_branch_row).parameters
        self.assertEqual(params["publish"].default, False)

    def test_sales_excluded_cfops_include_transfer_intra_and_interstate(self):
        self.assertEqual(SALES_EXCLUDED_CFOPS, (5929, 6929))
        pred = _cfop_sales_predicate_sql("i").replace(" ", "").lower()
        self.assertIn("cfop,0)>5000", pred)
        self.assertIn("notin(5929,6929)", pred)


    def test_nfe_documento_empty_is_honest_dash(self):
        self.assertEqual(_nfe_documento(""), "—")
        self.assertEqual(_nfe_documento("0"), "—")
        self.assertEqual(_nfe_documento(None), "—")
        self.assertEqual(_nfe_documento("114657"), "114657")

    def test_date_key_iso(self):
        self.assertEqual(_date_key_iso(20260715), "2026-07-15")
        self.assertEqual(_date_key_iso(0), "")

    def test_drilldown_function_exists(self):
        import inspect
        from app.repos_manager_commission import calc_branch_drilldown

        params = inspect.signature(calc_branch_drilldown).parameters
        self.assertIn("id_empresa", params)
        self.assertIn("id_filial", params)
        self.assertIn("year", params)
        self.assertIn("month", params)


if __name__ == "__main__":
    unittest.main()
