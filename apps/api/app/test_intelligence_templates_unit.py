"""Templates conversacionais do Assistente."""

from __future__ import annotations

import unittest

from app.intelligence.templates.responses import build_answer, uncertain_answer


class IntelligenceTemplatesTests(unittest.TestCase):
    def test_sales_overview_hoje_natural(self):
        text = build_answer(
            status="ok",
            intent_id="sales.overview",
            period_label="hoje",
            tool_result={
                "kpis": {"faturamento": 1000.0},
                "commercial_kpis": {"saidas": 1000.0, "qtd_saidas": 12},
            },
        )
        self.assertIn("Hoje o faturamento", text)
        self.assertIn("R$ 1.000,00", text)
        self.assertNotIn("Evidências", text)

    def test_uncertain_friendly(self):
        text = uncertain_answer("/sales")
        self.assertIn("Hmm", text)
        self.assertIn("Vendas", text)
        self.assertNotIn("/sales", text)


if __name__ == "__main__":
    unittest.main()
