"""Unit tests for ANP compliance margin variance formulas."""
from __future__ import annotations

import unittest

from app.services.anp_compliance import classify_status, compute_variacao, events_to_csv


class AnpComplianceFormulaTests(unittest.TestCase):
    def test_variacao_aumento_margem(self):
        # preco 5→6, custo 4→4 → margem 1→2 → +100%
        m_ant, m_nova, var, sem = compute_variacao(5.0, 4.0, 6.0, 4.0)
        self.assertAlmostEqual(m_ant, 1.0)
        self.assertAlmostEqual(m_nova, 2.0)
        self.assertAlmostEqual(var or 0, 100.0)
        self.assertFalse(sem)
        self.assertEqual(classify_status(var, 50, 70), "RISCO_ABUSIVO")

    def test_alerta_faixa(self):
        # margem 1.0 → 1.55 = +55% → ALERTA
        _, _, var, _ = compute_variacao(5.0, 4.0, 5.55, 4.0)
        self.assertEqual(classify_status(var, 50, 70), "ALERTA")

    def test_ok_abaixo_alerta(self):
        _, _, var, _ = compute_variacao(5.0, 4.0, 5.2, 4.0)
        self.assertEqual(classify_status(var, 50, 70), "OK")

    def test_sem_lastro_margem_zero(self):
        m_ant, m_nova, var, sem = compute_variacao(4.0, 4.0, 5.0, 4.0)
        self.assertAlmostEqual(m_ant, 0.0)
        self.assertTrue(sem)
        self.assertIsNone(var)
        self.assertEqual(classify_status(var, 50, 70, sem_lastro=True), "SEM_LASTRO")

    def test_csv_headers(self):
        csv = events_to_csv(
            [
                {
                    "nome_resumido": "VR 01",
                    "id_filial": 14458,
                    "nome_produto": "ETANOL",
                    "data_alteracao": "2026-06-19",
                    "variacao_margem_pct": 55.5,
                    "status": "ALERTA",
                }
            ]
        )
        header = csv.splitlines()[0]
        self.assertIn("nome_resumido", header)
        self.assertIn("data_alteracao", header)
        self.assertNotIn("origem", header)
        self.assertIn("VR 01", csv)
        self.assertIn("2026-06-19", csv)


if __name__ == "__main__":
    unittest.main()
