"""Tests for commission period helpers."""
from __future__ import annotations

import unittest
from datetime import date

from app.commission_period import (
    default_last_closed_commission_period,
    resolve_commission_period_from_query,
    validate_commission_period,
)


class TestCommissionPeriod(unittest.TestCase):
    def test_default_15_aug(self) -> None:
        start, end = default_last_closed_commission_period(date(2025, 8, 15))
        self.assertEqual(start, date(2025, 6, 21))
        self.assertEqual(end, date(2025, 7, 20))

    def test_default_28_aug(self) -> None:
        start, end = default_last_closed_commission_period(date(2025, 8, 28))
        self.assertEqual(start, date(2025, 7, 21))
        self.assertEqual(end, date(2025, 8, 20))

    def test_validate_rejects_inverted(self) -> None:
        with self.assertRaises(ValueError):
            validate_commission_period(date(2025, 8, 20), date(2025, 8, 1))

    def test_resolve_explicit(self) -> None:
        start, end = resolve_commission_period_from_query("2025-01-01", "2025-01-10")
        self.assertEqual(start, date(2025, 1, 1))
        self.assertEqual(end, date(2025, 1, 10))


if __name__ == "__main__":
    unittest.main()
