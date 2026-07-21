"""Unit: status Ativa/Cancelada das trocas de forma de pagamento."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app import repos_mart_realtime as rt


class TrocaVendaStatusUnit(unittest.TestCase):
    def test_enrich_marca_cancelada_via_nfe_status_4(self) -> None:
        rows = [
            {
                "id_filial": 14122,
                "referencia": 2754653,
                "documento": "89477",
                "documento_raw": "Venda para VALDECIR SAVI/NFC-e 89477",
                "dt": "2026-05-09",
                "comprovante_situacao": 0,
            }
        ]
        nfe_rows = [
            {
                "id_filial": 14122,
                "numero_nfe": "89477",
                "dt_emissao": "2026-05-09",
                "nfe_status": 4,
                "id_comprovante": 3549682,
                "cancelado": 1,
                "situacao": 1,
            }
        ]
        with patch.object(rt, "query_dict", side_effect=[nfe_rows, []]):
            rt._enrich_troca_venda_status(1, rows)
        self.assertEqual(rows[0]["venda_status"], "Cancelada")
        self.assertTrue(rows[0]["venda_cancelada"])
        self.assertEqual(rows[0]["nfe_status"], 4)

    def test_enrich_ativa_quando_nfe_autorizada(self) -> None:
        rows = [
            {
                "id_filial": 14122,
                "referencia": 1,
                "documento": "100",
                "documento_raw": "NFC-e 100",
                "dt": "2026-05-09",
                "comprovante_situacao": 0,
            }
        ]
        nfe_rows = [
            {
                "id_filial": 14122,
                "numero_nfe": "100",
                "dt_emissao": "2026-05-09",
                "nfe_status": 3,
                "id_comprovante": 10,
                "cancelado": 0,
                "situacao": 1,
            }
        ]
        with patch.object(rt, "query_dict", side_effect=[nfe_rows, []]):
            rt._enrich_troca_venda_status(1, rows)
        self.assertEqual(rows[0]["venda_status"], "Ativa")
        self.assertFalse(rows[0]["venda_cancelada"])


if __name__ == "__main__":
    unittest.main()
