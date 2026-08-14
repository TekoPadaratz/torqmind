"""Unit tests for commission PDF helpers and finance despesas source contract."""
from __future__ import annotations

import unittest
from pathlib import Path


class TestCommissionsPrintContract(unittest.TestCase):
    def test_commissions_tab_has_print_button_and_escape(self):
        src = (
            Path(__file__).resolve().parents[2]
            / "web"
            / "app"
            / "goals"
            / "CommissionsTab.tsx"
        )
        text = src.read_text(encoding="utf-8")
        self.assertIn("Imprimir / PDF", text)
        self.assertIn("escapeHtml", text)
        self.assertIn("A4 landscape", text)
        self.assertIn("buildCommissionsReportHtml", text)
        self.assertIn("Descontos e preços negociados", text)


class TestTrocaFilialLabel(unittest.TestCase):
    def test_troca_applies_filial_label(self):
        src = Path(__file__).resolve().parent / "repos_mart_realtime.py"
        text = src.read_text(encoding="utf-8")
        idx = text.find("def fraud_troca_forma_pgto(")
        end = text.find("\ndef fraud_troca_forma_pgto_kpis(", idx)
        chunk = text[idx:end]
        self.assertIn('r["filial_label"] = label', chunk)
        self.assertIn("_filial_label(fid", chunk)


class TestFinanceDespesasPublishSource(unittest.TestCase):
    def test_fetch_sql_uses_movlctos_not_contaspagar(self):
        src = Path(__file__).resolve().parent / "services" / "finance_despesas.py"
        text = src.read_text(encoding="utf-8")
        self.assertIn("stg.movlctos", text)
        self.assertIn("DTACONTA", text)
        self.assertNotIn("stg.contaspagar", text)
        self.assertIn("'entrada'", text)
        self.assertIn("'saida'", text)


if __name__ == "__main__":
    unittest.main()
