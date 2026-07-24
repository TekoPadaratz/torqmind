"""Unit tests for cliente preço fixo (desconto econômico implícito)."""
from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from app.services import cliente_preco_fixo as svc


class ClientePrecoFixoUnitTests(unittest.TestCase):
    def test_overview_maps_filial_apelido_and_totals(self) -> None:
        ch_rows = [
            {
                "id_filial": 10169,
                "id_entidade": 7527,
                "cliente_nome": "CLIENTE TESTE",
                "desconto_total": 55.64,
                "qtd_litros": 120.5,
                "qtd_itens": 3,
                "qtd_vendas": 2,
            }
        ]
        with patch.object(svc, "query_dict", side_effect=[
            [{"n": 1, "desconto_total_sum": 55.64, "qtd_litros_sum": 120.5}],
            ch_rows,
        ]), patch.object(svc, "apelido_for", return_value="VR01"):
            out = svc.overview(1, date(2026, 7, 1), date(2026, 7, 20), page=0, page_size=15)
        self.assertEqual(out["total"], 1)
        self.assertEqual(out["items"][0]["filial_label"], "VR01")
        self.assertEqual(out["items"][0]["cliente_nome"], "CLIENTE TESTE")
        self.assertAlmostEqual(out["items"][0]["desconto_total"], 55.64)
        self.assertAlmostEqual(out["items"][0]["qtd_litros"], 120.5)
        self.assertAlmostEqual(out["summary"]["desconto_total"], 55.64)
        self.assertAlmostEqual(out["summary"]["qtd_litros"], 120.5)
        self.assertEqual(out["source"], "clickhouse_mart")

    def test_detail_maps_document_and_prices_with_subtotal(self) -> None:
        ch_rows = [
            {
                "id_comprovante": 3649208,
                "id_itemcomprovante": 1,
                "id_produto": 5991,
                "dt_venda": date(2026, 7, 20),
                "dt_evento": "2026-07-20 17:09:31",
                "cliente_nome": "CLIENTE TESTE",
                "produto_nome": "GASOLINA COMUM",
                "documento_label": "325152",
                "qtd": 27.821,
                "preco_bomba": 6.52,
                "preco_pago": 6.32,
                "desconto_unitario": 0.20,
                "desconto_total": 5.56,
                "custo_unitario": 5.10,
                "margem_unitaria_pct": 19.3,
                "margem_bomba_pct": 21.8,
            },
            {
                "id_comprovante": 3649209,
                "id_itemcomprovante": 1,
                "id_produto": 5991,
                "dt_venda": date(2026, 7, 21),
                "dt_evento": "2026-07-21 10:00:00",
                "cliente_nome": "CLIENTE TESTE",
                "produto_nome": "GASOLINA COMUM",
                "documento_label": "325200",
                "qtd": 10.0,
                "preco_bomba": 6.52,
                "preco_pago": 6.32,
                "desconto_unitario": 0.20,
                "desconto_total": 2.0,
                "custo_unitario": 5.10,
                "margem_unitaria_pct": 19.3,
                "margem_bomba_pct": 21.8,
            },
        ]
        with patch.object(svc, "query_dict", side_effect=[
            [{"n": 2, "desconto_total_sum": 7.56, "qtd_litros_sum": 37.821}],
            ch_rows,
        ]), patch.object(svc, "apelido_for", return_value="VR01"):
            out = svc.detail(1, 10169, 7527, date(2026, 7, 1), date(2026, 7, 22))
        self.assertEqual(out["filial_label"], "VR01")
        items = out["items"]
        self.assertEqual(items[0]["row_kind"], "item")
        self.assertEqual(items[0]["documento_label"], "325152")
        self.assertAlmostEqual(items[0]["preco_bomba"], 6.52)
        self.assertAlmostEqual(items[0]["margem_unitaria_pct"], 19.3)
        self.assertEqual(items[-1]["row_kind"], "subtotal")
        self.assertAlmostEqual(items[-1]["qtd"], 37.821, places=3)
        self.assertAlmostEqual(items[-1]["desconto_total"], 7.56)
        self.assertAlmostEqual(out["summary"]["qtd_litros"], 37.821)

    def test_desconto_economico_formula(self) -> None:
        # bomba 6.52 − pago 6.32 = 0.20 × 27.821 ≈ 5.5642
        preco_bomba = 6.52
        preco_pago = 6.32
        qtd = 27.821
        desconto = (preco_bomba - preco_pago) * qtd
        self.assertGreater(preco_bomba - preco_pago, svc.DELTA_MIN)
        self.assertAlmostEqual(desconto, 5.5642, places=3)


if __name__ == "__main__":
    unittest.main()
