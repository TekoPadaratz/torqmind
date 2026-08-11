"""Unit tests for manager commission net formula and defaults."""
from __future__ import annotations

import unittest

from app.repos_manager_commission import (
    SALES_BASE_EXCLUDED_GROUP_IDS,
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


if __name__ == "__main__":
    unittest.main()
