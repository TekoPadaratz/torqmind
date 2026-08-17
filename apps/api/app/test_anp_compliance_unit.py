"""Unit tests for ANP compliance margin variance formulas."""
from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from app.services.anp_compliance import (
    ORIGEM_LMC,
    ORIGEM_MASH_MENSAL,
    ORIGEM_NFE,
    classify_status,
    compute_variacao,
    events_to_csv,
    mart_events_usable,
    overview_payload,
)


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


class AnpMartLastroTests(unittest.TestCase):
    def test_mash_mensal_sem_nfe_nao_serve_grid(self):
        self.assertFalse(
            mart_events_usable(
                [
                    {
                        "origem": ORIGEM_MASH_MENSAL,
                        "chave_nfe_nova": "",
                        "numero_nota_nova": "",
                    }
                ]
            )
        )

    def test_lmc_com_chave_serve_grid(self):
        self.assertTrue(
            mart_events_usable(
                [
                    {
                        "origem": ORIGEM_LMC,
                        "chave_nfe_nova": "35260112345678901234567890123456789012345678",
                        "numero_nota_nova": "12345",
                    }
                ]
            )
        )

    def test_nfe_asof_so_numero_serve_grid(self):
        self.assertTrue(
            mart_events_usable(
                [{"origem": ORIGEM_NFE, "chave_nfe_nova": "", "numero_nota_nova": "8891"}]
            )
        )

    @patch("app.services.anp_compliance.set_apelido_scope")
    @patch("app.services.anp_compliance.load_config")
    @patch("app.services.anp_compliance.compute_lmc_events")
    @patch("app.services.anp_compliance._stg_has_lmc", return_value=True)
    @patch("app.services.anp_compliance._events_from_ch_mart")
    def test_overview_ignora_mash_e_recalcula_lmc(
        self, mock_mart, _mock_stg, mock_live, mock_cfg, _mock_scope
    ):
        mock_mart.return_value = [
            {
                "origem": ORIGEM_MASH_MENSAL,
                "chave_nfe_nova": "",
                "numero_nota_nova": "",
                "id_filial": 14458,
                "status": "OK",
            }
        ]
        mock_live.return_value = [
            {
                "origem": ORIGEM_LMC,
                "chave_nfe_nova": "3526ABC",
                "numero_nota_nova": "4411",
                "id_filial": 14458,
                "nome_resumido": "VR 01",
                "status": "OK",
            }
        ]
        mock_cfg.return_value = {
            "limite_alerta_amarelo_perc": 50,
            "limite_abusivo_anp_perc": 70,
            "id_filial": 0,
        }
        payload = overview_payload(
            1, [14458], dt_ini=date(2026, 1, 1), dt_fim=date(2026, 8, 16)
        )
        self.assertEqual(payload["origem"], ORIGEM_LMC)
        self.assertEqual(payload["eventos"][0]["numero_nota_nova"], "4411")
        self.assertEqual(payload["eventos"][0]["chave_nfe_nova"], "3526ABC")
        mock_live.assert_called_once()


if __name__ == "__main__":
    unittest.main()
