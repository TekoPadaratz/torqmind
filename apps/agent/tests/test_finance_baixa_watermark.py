"""Agent finance watermark: baixas CAP usam DATABAIXA (não só DATAREPL)."""
from __future__ import annotations

import unittest


class TestContasPagarBaixaWatermark(unittest.TestCase):
    def test_contaspagarbaixa_watermark_includes_databaixa(self):
        from agent.config import DEFAULT_DATASETS

        ds = DEFAULT_DATASETS["contaspagarbaixa"]
        q = ds["query"]
        self.assertIn("DATABAIXA", q)
        self.assertIn("TORQMIND_WATERMARK", q)
        self.assertIn("MAX(v.dt)", q.replace(" ", ""))
        # Não pode ser só DATAREPL (atrasa parcial).
        self.assertNotEqual(
            q.count("DATAREPL"),
            q.count("DATABAIXA") and 0,
        )

    def test_agent_version_bumped_for_finance_fix(self):
        from agent import __version__

        parts = [int(x) for x in __version__.split(".")]
        self.assertGreaterEqual(tuple(parts), (2, 0, 6))


if __name__ == "__main__":
    unittest.main()
