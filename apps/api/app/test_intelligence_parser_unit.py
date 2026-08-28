"""Parser — perguntas básicas de cliente e financeiro."""

from __future__ import annotations

import unittest

from app.intelligence.parser import parse_intent


class IntelligenceParserBasicTests(unittest.TestCase):
    def test_cliente_devendo_open_titles(self):
        out = parse_intent("Quanto o cliente Alcione está me devendo?")
        self.assertEqual(out.intent_id, "customer.open_titles")
        self.assertEqual(out.action, "execute")
        self.assertEqual(out.slots.get("customer_name"), "Alcione")

    def test_contas_a_pagar_semana(self):
        out = parse_intent("Quanto tenho de contas a pagar essa semana?")
        self.assertEqual(out.intent_id, "finance.titles")
        self.assertEqual(out.slots.get("title_tipo"), 0)
        self.assertEqual(out.period.label, "esta semana")

    def test_faturamento_na_vr(self):
        out = parse_intent("faturamento hoje na VR 02")
        self.assertEqual(out.intent_id, "sales.overview")
        self.assertEqual(out.slots.get("filial_label"), "VR 02")


if __name__ == "__main__":
    unittest.main()
