"""Unit tests for team employee sales attribution (id_funcionario, not caixa)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from app import repos_mart_realtime as rt


class TestTeamEmployeeSalesByFuncionario(unittest.TestCase):
    def test_sales_query_aliases_and_uses_funcionario(self):
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
            if "stg_itenscomprovantes_slim" in sql:
                return [{"id_filial": 1, "id_funcionario": 99, "vendas": 1500.0}]
            if "mart_finance_despesas_rt" in sql:
                return [{"total_pessoal": 0, "total_overhead": 100}]
            return []

        with patch.object(rt, "query_dict", side_effect=fake_query_dict), patch.object(
            rt, "query_scalar", return_value=1
        ):
            out = rt.team_employee_cost_overview(
                role="platform_master",
                id_empresa=1,
                id_filial=None,
                ano=2026,
                mes=7,
            )
        sales_sql = next(s for s in captured if "stg_itenscomprovantes_slim" in s)
        self.assertIn("i.id_funcionario AS id_funcionario", sales_sql)
        self.assertIn("i.id_filial AS id_filial", sales_sql)
        self.assertIn("nfe_bloqueada", sales_sql)
        self.assertIn("cancelado = 0", sales_sql)
        self.assertEqual(out["items"][0]["vendas"], 1500.0)
        self.assertEqual(out["items"][0]["id_funcionario"], 99)

    def test_sales_lookup_tolerates_prefixed_filial_key(self):
        """ClickHouse pode devolver i.id_filial — não pode zerar vendas."""

        def fake_query_dict(sql, parameters=None):
            if "mart_team_employees_rt" in sql:
                return [
                    {
                        "id_filial": 14458,
                        "filial_nome": "Posto",
                        "id_funcionario": 727,
                        "id_usuario": 0,
                        "nome": "Adilson",
                        "funcao": "Frentista",
                        "salario_bruto": 1000,
                        "salario_total": 1000,
                        "vales": 0,
                        "horas_extras": 0,
                    }
                ]
            if "stg_itenscomprovantes_slim" in sql:
                return [{"i.id_filial": 14458, "id_funcionario": 727, "vendas": 285183.41}]
            if "mart_finance_despesas_rt" in sql:
                return [{"total_pessoal": 0, "total_overhead": 0}]
            return []

        with patch.object(rt, "query_dict", side_effect=fake_query_dict), patch.object(
            rt, "query_scalar", return_value=1
        ):
            out = rt.team_employee_cost_overview(
                role="platform_master",
                id_empresa=1,
                id_filial=14458,
                ano=2026,
                mes=7,
            )
        self.assertEqual(out["items"][0]["vendas"], 285183.41)


if __name__ == "__main__":
    unittest.main()
