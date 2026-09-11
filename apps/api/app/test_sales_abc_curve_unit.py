"""Unit contracts for sales_abc_curve ranking cap (query-cost)."""

from __future__ import annotations

import inspect
import unittest
from datetime import date
from unittest.mock import patch

from app import repos_mart_realtime


class SalesAbcCurveRankingCapUnitTest(unittest.TestCase):
    def test_abc_ranking_cap_constant_and_sql(self) -> None:
        self.assertEqual(repos_mart_realtime._ABC_RANKING_CAP, 5000)
        src = inspect.getsource(repos_mart_realtime.sales_abc_curve)
        self.assertIn("posicao <= {_ABC_RANKING_CAP}", src)
        self.assertIn("_total_produtos", src)
        self.assertIn("countIf(classe_abc = 'A') OVER ()", src)

    def test_abc_empty_period_still_returns_empty_shape(self) -> None:
        with patch.object(repos_mart_realtime, "query_dict", side_effect=[[], []]):
            out = repos_mart_realtime.sales_abc_curve(
                "owner",
                1,
                None,
                date(2026, 9, 1),
                date(2026, 9, 10),
            )
        self.assertTrue(out.get("empty"))
        self.assertEqual(out["ranking"], [])
        self.assertEqual(out["summary"]["total_produtos"], 0)

    def test_abc_summary_uses_full_set_window_not_capped_row_count(self) -> None:
        ranking_rows = [
            {
                "id_produto": 1,
                "nome_produto": "Prod A",
                "nome_grupo": "Loja",
                "unidade": "UN",
                "quantity_kind": "unit",
                "faturamento": 100.0,
                "qtd": 1.0,
                "custo_total": 40.0,
                "margem": 60.0,
                "valor_unitario_medio": 100.0,
                "participacao_pct": 50.0,
                "acumulado_pct": 50.0,
                "classe_abc": "A",
                "posicao": 1,
                "total_produtos": 6000,
                "total_faturamento": 200.0,
                "total_metric": 200.0,
                "classe_a_count": 1,
                "classe_b_count": 0,
                "classe_c_count": 5999,
                "metric_a": 100.0,
                "metric_b": 0.0,
                "metric_c": 100.0,
            }
        ]

        def _qd(sql, parameters=None, **kwargs):
            if "sales_products_rt" in sql and "id_grupo_produto" in sql and "GROUP BY id_grupo_produto" in sql:
                return []
            return ranking_rows

        with patch.object(repos_mart_realtime, "query_dict", side_effect=_qd):
            out = repos_mart_realtime.sales_abc_curve(
                "owner",
                1,
                None,
                date(2026, 9, 1),
                date(2026, 9, 10),
            )

        self.assertEqual(out["summary"]["total_produtos"], 6000)
        self.assertEqual(out["summary"]["classe_c_count"], 5999)
        self.assertEqual(len(out["ranking"]), 1)


if __name__ == "__main__":
    unittest.main()
