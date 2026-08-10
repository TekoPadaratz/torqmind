"""Unit tests for employee_cost_manual helpers (sem DB)."""
from __future__ import annotations

import unittest

from app.services.employee_cost_manual import _normalize_money, _validate_ano_mes


class TestEmployeeCostManualHelpers(unittest.TestCase):
    def test_normalize_money_ok(self):
        self.assertEqual(_normalize_money("500,00".replace(",", ".")), 500.0)
        self.assertEqual(_normalize_money(800), 800.0)
        self.assertEqual(_normalize_money(0), 0.0)

    def test_normalize_money_rejects_negative(self):
        with self.assertRaises(ValueError):
            _normalize_money(-1)

    def test_ano_mes_ok(self):
        self.assertEqual(_validate_ano_mes(202608), 202608)

    def test_ano_mes_rejects_bad_month(self):
        with self.assertRaises(ValueError):
            _validate_ano_mes(202613)


if __name__ == "__main__":
    unittest.main()
