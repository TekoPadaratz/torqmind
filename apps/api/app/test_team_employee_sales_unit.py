"""Unit tests for team employee sales attribution (id_funcionario, not caixa)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from app import repos_mart_realtime as rt


class TestTeamEmployeeSalesByFuncionario(unittest.TestCase):
    def test_sales_query_uses_item_funcionario_not_caixa_usuario(self):
        captured: list[str] = []

        def fake_query_dict(sql, parameters=None):
            captured.append(sql)
            if "mart_team_employees_rt" in sql:
                return [
                    {
                        "id_filial": 1,
                        "filial_nome": "Posto A",
                        "id_funcionario": 99,
                        "id_usuario": 7,
                        "nome": "Ana",
                        "funcao": "Frentista",
                        "salario_bruto": 2000,
                        "salario_total": 2000,
                        "vales": 0,
                        "horas_extras": 0,
                    }
                ]
            if "id_funcionario" in sql and "stg_itenscomprovantes_slim" in sql:
                return [{"id_filial": 1, "id_funcionario": 99, "vendas": 1500.0}]
            if "mart_finance_despesas_rt" in sql:
                return [{"total_pessoal": 0, "total_overhead": 100}]
            return []

        def fake_scalar(sql, parameters=None):
            return 1

        with patch.object(rt, "query_dict", side_effect=fake_query_dict), patch.object(
            rt, "query_scalar", side_effect=fake_scalar
        ):
            out = rt.team_employee_cost_overview(
                role="platform_master",
                id_empresa=1,
                id_filial=None,
                ano=2026,
                mes=7,
            )
        sales_sql = next(s for s in captured if "stg_itenscomprovantes_slim" in s)
        self.assertIn("i.id_funcionario", sales_sql)
        self.assertIn("nfe_bloqueada", sales_sql)
        self.assertIn("cancelado = 0", sales_sql)
        self.assertNotIn("GROUP BY i.id_filial, i.id_usuario", sales_sql)
        self.assertEqual(out["items"][0]["vendas"], 1500.0)
        self.assertEqual(out["items"][0]["id_funcionario"], 99)


if __name__ == "__main__":
    unittest.main()
