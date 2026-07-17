"""Unit: solvencia_detalhada mescla as-of de mart.liquidez_solvencia no mês."""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from app import repos_mart


def _rows(*dicts):
    return [dict(d) for d in dicts]


class SolvenciaDetalhadaAsofUnitTest(unittest.TestCase):
    def test_overlay_asof_substitui_dinheiro_cartoes_estoque(self) -> None:
        conn = MagicMock()

        def execute(sql, params=None):
            sql_l = " ".join(str(sql).lower().split())
            result = MagicMock()
            if "from mart.solvencia_item" in sql_l:
                result.fetchall.return_value = _rows(
                    {
                        "id_filial": 14458,
                        "grupo": "ativo_circulante",
                        "secao": "dinheiro",
                        "item_label": "Snapshot velho",
                        "valor": 1.0,
                        "qtd": None,
                        "ordem": 55,
                    },
                    {
                        "id_filial": 14458,
                        "grupo": "ativo_circulante",
                        "secao": "cartoes",
                        "item_label": "Cartão velho",
                        "valor": 2.0,
                        "qtd": None,
                        "ordem": 40,
                    },
                    {
                        "id_filial": 14458,
                        "grupo": "ativo_circulante",
                        "secao": "estoque",
                        "item_label": "Estoque velho",
                        "valor": 3.0,
                        "qtd": None,
                        "ordem": 20,
                    },
                    {
                        "id_filial": 14458,
                        "grupo": "ativo_circulante",
                        "secao": "aprazo",
                        "item_label": "Cliente X",
                        "valor": 100.0,
                        "qtd": None,
                        "ordem": 30,
                    },
                    {
                        "id_filial": 14458,
                        "grupo": "ativo_circulante",
                        "secao": "havel",
                        "item_label": "Havel clientes",
                        "valor": -4500.33,
                        "qtd": 12,
                        "ordem": 41,
                    },
                )
            elif "from mart.liquidez_solvencia" in sql_l and "distinct ano_mes" in sql_l:
                result.fetchall.return_value = _rows({"ano_mes": 202606}, {"ano_mes": 202607})
            elif "from mart.liquidez_solvencia" in sql_l:
                result.fetchall.return_value = _rows(
                    {
                        "id_filial": 14458,
                        "ativo_caixa": 34167.24,
                        "ativo_banco": 0,
                        "ativo_cartoes": 270527.0,
                        "ativo_cartoes_credito": 200000.0,
                        "ativo_cartoes_debito": 70527.0,
                        "ativo_cheques": 0,
                        "ativo_estoque": 522118.32,
                        "ativo_estoque_combustivel": 0,
                        "ativo_estoque_loja": 522118.32,
                        "passivo_contas_pagar": 50000.0,
                        "tem_ativo_dados": True,
                    }
                )
            elif "from app.solvencia_tipo_manual" in sql_l:
                result.fetchall.return_value = _rows(
                    {
                        "id_tipo": 1,
                        "chave": "banco",
                        "nome": "Bancos",
                        "grupo": "ativo_circulante",
                        "secao": "banco",
                        "ordem": 60,
                    },
                    {
                        "id_tipo": 2,
                        "chave": "investimento",
                        "nome": "Investimentos",
                        "grupo": "ativo_nao_circulante",
                        "secao": "investimento",
                        "ordem": 10,
                    },
                )
            elif "from app.solvencia_entrada_manual" in sql_l and "distinct" in sql_l:
                result.fetchall.return_value = []
            elif "from app.solvencia_entrada_manual" in sql_l:
                result.fetchall.return_value = []
            elif "from auth.filiais" in sql_l:
                result.fetchall.return_value = _rows({"id_filial": 14458, "nome": "VR01"})
            else:
                result.fetchall.return_value = []
            return result

        conn.execute.side_effect = execute
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        with (
            patch("app.repos_mart.get_conn", return_value=conn),
            patch("app.repos_mart.business_today", return_value=date(2026, 7, 13)),
        ):
            payload = repos_mart.solvencia_detalhada("MASTER", 1, 14458, 202606)

        self.assertEqual(payload["ano_mes"], 202606)
        self.assertEqual(payload["posicao"], "as_of_abertura_mes")
        self.assertIn(202606, payload["meses_disponiveis"])
        self.assertIn(202607, payload["meses_disponiveis"])

        filial = payload["filiais"][0]
        self.assertEqual(filial["id_filial"], 14458)
        secoes = {s["secao"]: s for s in filial["grupos"]["ativo_circulante"]["secoes"]}

        self.assertEqual(secoes["dinheiro"]["total"], 34167.24)
        self.assertEqual(secoes["dinheiro"]["itens"][0]["as_of"], True)
        self.assertNotIn("Snapshot velho", secoes["dinheiro"]["itens"][0]["label"])

        self.assertEqual(secoes["cartoes"]["total"], 270527.0)
        self.assertTrue(secoes["cartoes"].get("colapsado"))
        self.assertEqual(secoes["cartoes"]["hint_itens"][0]["label"], "Crédito")
        self.assertEqual(secoes["havel"]["total"], -4500.33)
        self.assertEqual(secoes["havel"]["itens"][0]["label"], "Havel Clientes")
        self.assertEqual(secoes["havel"]["label"], "Havel Clientes")

        self.assertEqual(secoes["estoque"]["total"], 522118.32)
        self.assertTrue(secoes["estoque"]["itens"][0].get("as_of"))

        # A Prazo permanece do snapshot (ainda sem as-of próprio).
        self.assertEqual(secoes["aprazo"]["total"], 100.0)

        passivo_sec = {s["secao"]: s for s in filial["grupos"]["passivo_circulante"]["secoes"]}
        self.assertEqual(passivo_sec["boleto"]["total"], 50000.0)


if __name__ == "__main__":
    unittest.main()
