from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from app import repos_mart


def _operational_range_bundle() -> dict:
    return {
        "kpis": {
            "faturamento": 600.0,
            "margem": 180.0,
            "ticket_medio": 200.0,
            "devolucoes": 40.0,
        },
        "commercial_kpis": {
            "saidas": 640.0,
            "qtd_saidas": 4,
            "entradas": 120.0,
            "qtd_entradas": 1,
            "cancelamentos": 75.0,
            "qtd_cancelamentos": 1,
        },
        "by_day": [
            {"data_key": 20260331, "id_filial": None, "faturamento": 100.0, "margem": 30.0},
            {"data_key": 20260401, "id_filial": None, "faturamento": 200.0, "margem": 60.0},
            {"data_key": 20260402, "id_filial": None, "faturamento": 300.0, "margem": 90.0},
        ],
        "by_hour": [],
        "commercial_by_hour": [{"hora": 9, "saidas": 320.0, "entradas": 0.0, "cancelamentos": 0.0}],
        "cfop_breakdown": [{"cfop_class": "saida_normal", "label": "Vendas normais", "documentos": 4, "valor_ativo": 640.0, "valor_cancelado": 75.0, "valor_total": 715.0}],
        "monthly_evolution": [{"month_ref": "2026-04-01", "ano": 2026, "mes": 4, "saidas": 640.0, "entradas": 120.0, "cancelamentos": 75.0}],
        "annual_comparison": {
            "current_year": 2026,
            "previous_year": 2025,
            "months": [
                {
                    "mes": 4,
                    "saidas_atual": 640.0,
                    "saidas_anterior": 0.0,
                    "entradas_atual": 120.0,
                    "entradas_anterior": 0.0,
                    "cancelamentos_atual": 75.0,
                    "cancelamentos_anterior": 0.0,
                    "month_ref_atual": "2026-04-01",
                    "month_ref_anterior": "2025-04-01",
                }
            ],
        },
        "top_products": [
            {
                "id_produto": 101,
                "produto_nome": "GASOLINA COMUM",
                "faturamento": 600.0,
                "custo_total": 420.0,
                "margem": 180.0,
                "qtd": 60.0,
                "valor_unitario_medio": 10.0,
            }
        ],
        "top_groups": [{"id_grupo_produto": 1, "grupo_nome": "COMBUSTIVEIS", "faturamento": 600.0, "margem": 180.0}],
        "top_employees": [],
        "stats": {"vendas": 3},
        "operational_sync": {
            "last_sync_at": "2026-04-02T15:34:00-03:00",
            "source": "dw.fact_venda",
            "dt_ref": "2026-04-02",
        },
        "freshness": {
            "mode": "live_range",
            "operational_day": "2026-04-02",
            "live_through_at": "2026-04-02T15:34:00-03:00",
            "historical_through_dt": "2026-04-02",
            "source": "dw.fact_venda",
        },
    }


class SalesOverviewBundleUnitTest(unittest.TestCase):
    def test_commercial_annual_comparison_fills_calendar_months_for_two_years(self) -> None:
        normalized, annual = repos_mart._commercial_annual_comparison(
            [
                {"ano": 2025, "mes": 2, "saidas": 50.0, "entradas": 10.0, "cancelamentos": 0.0},
                {"ano": 2026, "mes": 4, "saidas": 125.0, "entradas": 15.0, "cancelamentos": 5.0},
            ],
            current_year=2026,
        )

        self.assertEqual(len(normalized), 24)
        self.assertEqual(normalized[0]["month_ref"], "2025-01-01")
        self.assertEqual(normalized[-1]["month_ref"], "2026-12-01")
        self.assertEqual(float(normalized[1]["saidas"]), 50.0)
        self.assertEqual(float(normalized[12]["saidas"]), 0.0)
        self.assertEqual(annual["current_year"], 2026)
        self.assertEqual(annual["previous_year"], 2025)
        self.assertEqual(len(annual["months"]), 12)
        self.assertEqual(float(annual["months"][0]["saidas_atual"]), 0.0)
        self.assertEqual(float(annual["months"][1]["saidas_anterior"]), 50.0)
        self.assertEqual(float(annual["months"][3]["saidas_atual"]), 125.0)

    def test_turno_label_falls_back_to_numeric_identifier_when_payload_label_is_missing(self) -> None:
        self.assertEqual(repos_mart._turno_label(None, 356), "356")
        self.assertEqual(repos_mart._turno_label("", 0), "Turno sem cadastro")

    def test_commercial_docs_window_cte_uses_comprovantes_as_canonical_source(self) -> None:
        cte, _params, _branch = repos_mart._commercial_docs_window_cte(
            id_empresa=7,
            id_filial=11,
            date_predicate_sql="c.data_key BETWEEN %s AND %s",
            date_params=[20260414, 20260415],
        )

        self.assertIn("FROM dw.fact_comprovante c", cte)
        self.assertIn("etl.cfop_numeric_from_payload(c.payload)", cte)
        self.assertIn("etl.cfop_direction(etl.cfop_numeric_from_payload(c.payload)) IN ('saida', 'entrada')", cte)

    def test_sales_window_fact_cte_enforces_exact_statuses_and_cfop_rule(self) -> None:
        cte, _params, _branch = repos_mart._sales_window_fact_cte(
            id_empresa=7,
            id_filial=11,
            date_predicate_sql="v.data_key BETWEEN %s AND %s",
            date_params=[20260414, 20260415],
        )

        self.assertIn("COALESCE(v.situacao, 0) = 1", cte)
        self.assertIn("COALESCE(v.situacao, 0) = 3", cte)
        self.assertNotIn("COALESCE(v.situacao, 0) > 2", cte)
        self.assertIn("COALESCE(i.cfop, 0) > 5000", cte)
        self.assertNotIn("COALESCE(i.cfop, 0) >= 5000", cte)
        self.assertIn("v.id_comprovante AS doc_key", cte)
        self.assertNotIn("COALESCE(v.id_comprovante, v.id_movprodutos) AS doc_key", cte)

    def test_cash_sales_docs_cte_uses_canonical_comprovante_key(self) -> None:
        cte, _params = repos_mart._cash_sales_docs_cte(
            id_empresa=7,
            id_filial=11,
            date_key_sql="v.data_key BETWEEN %s AND %s",
            date_params=[20260414, 20260415],
        )

        self.assertIn("v.id_comprovante AS doc_key", cte)
        self.assertIn("GROUP BY", cte)
        self.assertIn("v.id_comprovante,", cte)
        self.assertNotIn("COALESCE(v.id_comprovante, v.id_movprodutos)", cte)

    def test_collapse_group_rank_rows_uses_canonical_bucket_for_convenience(self) -> None:
        collapsed = repos_mart._collapse_group_rank_rows(
            [
                {"id_grupo_produto": 10, "grupo_nome": "CIGARROS", "faturamento": 600.0, "margem": 60.0},
                {"id_grupo_produto": 14, "grupo_nome": "BEBIDAS ALCOOLICAS", "faturamento": 400.0, "margem": 90.0},
                {"id_grupo_produto": 1, "grupo_nome": "COMBUSTIVEIS", "faturamento": 5000.0, "margem": 700.0},
            ],
            limit=10,
        )

        self.assertEqual(collapsed[0]["grupo_nome"], "Combustíveis")
        self.assertEqual(collapsed[1]["grupo_nome"], "Conveniência")
        self.assertEqual(collapsed[1]["grupo_key"], "macro:conveniencia")
        self.assertEqual(collapsed[1]["id_grupo_produto"], repos_mart.CANONICAL_GROUP_BUCKET_IDS["macro:conveniencia"])
        self.assertEqual(float(collapsed[1]["faturamento"]), 1000.0)
        self.assertEqual(float(collapsed[1]["margem"]), 150.0)

    def test_collapse_group_rank_rows_keeps_filters_outside_fuel_bucket(self) -> None:
        collapsed = repos_mart._collapse_group_rank_rows(
            [
                {"id_grupo_produto": 10, "grupo_nome": "COMBUSTIVEIS", "faturamento": 5000.0, "margem": 700.0},
                {"id_grupo_produto": 11, "grupo_nome": "FILTROS DE COMBUSTIVEIS", "faturamento": 89.0, "margem": 49.0},
            ],
            limit=10,
        )

        self.assertEqual(collapsed[0]["grupo_nome"], "Combustíveis")
        self.assertEqual(float(collapsed[0]["faturamento"]), 5000.0)
        self.assertEqual(collapsed[1]["grupo_nome"], "FILTROS DE COMBUSTIVEIS")
        self.assertEqual(collapsed[1]["grupo_key"], "group:11")
        self.assertEqual(float(collapsed[1]["faturamento"]), 89.0)

    def test_uses_mart_bundle_for_fully_historical_windows(self) -> None:
        expected = _operational_range_bundle()
        expected["by_day"] = expected["by_day"][:2]
        expected["by_hour"] = expected["by_hour"][:0]
        expected["kpis"] = {
            "faturamento": 300.0,
            "margem": 90.0,
            "ticket_medio": 150.0,
            "devolucoes": 0.0,
        }
        expected["operational_sync"] = {
            "last_sync_at": None,
            "source": "mart.agg_vendas_diaria",
            "dt_ref": "2026-04-01",
        }
        expected["freshness"] = {
            "mode": "historical_snapshot",
            "operational_day": None,
            "live_through_at": None,
            "historical_through_dt": "2026-04-01",
            "source": "mart.agg_vendas_diaria",
        }

        with patch.object(repos_mart, "business_today", return_value=date(2026, 4, 2)), patch.object(
            repos_mart,
            "commercial_window_coverage",
            return_value={
                "mode": "exact",
                "effective_dt_ini": date(2026, 3, 31),
                "effective_dt_fim": date(2026, 4, 1),
            },
        ), patch.object(
            repos_mart,
            "_sales_historical_bundle_from_marts",
            return_value=expected,
        ) as historical_bundle, patch.object(
            repos_mart,
            "sales_commercial_overview",
            return_value={
                "kpis": expected["commercial_kpis"],
                "cfop_breakdown": expected["cfop_breakdown"],
                "by_hour": expected["commercial_by_hour"],
                "monthly_evolution": expected["monthly_evolution"],
                "annual_comparison": expected["annual_comparison"],
            },
        ) as commercial_bundle:
            payload = repos_mart.sales_overview_bundle(
                "MASTER",
                1,
                None,
                date(2026, 3, 31),
                date(2026, 4, 1),
                as_of=date(2026, 4, 2),
            )

        historical_bundle.assert_called_once_with(
            "MASTER",
            1,
            None,
            date(2026, 3, 31),
            date(2026, 4, 1),
            include_details=True,
        )
        commercial_bundle.assert_called_once()
        self.assertEqual(payload["reading_status"], "mart_snapshot")
        self.assertEqual([int(row["data_key"]) for row in payload["by_day"]], [20260331, 20260401])
        self.assertEqual(float(payload["kpis"]["faturamento"]), 300.0)
        self.assertEqual(float(payload["commercial_kpis"]["saidas"]), 640.0)
        self.assertEqual(payload["annual_comparison"]["current_year"], 2026)

    def test_uses_mart_semantics_when_window_includes_live_day(self) -> None:
        mart_bundle = _operational_range_bundle()
        with patch.object(repos_mart, "business_today", return_value=date(2026, 4, 2)), patch.object(
            repos_mart,
            "commercial_window_coverage",
            return_value={
                "mode": "exact",
                "effective_dt_ini": date(2026, 3, 31),
                "effective_dt_fim": date(2026, 4, 2),
            },
        ), patch.object(
            repos_mart,
            "_sales_historical_bundle_from_marts",
            return_value=mart_bundle,
        ) as historical_bundle, patch.object(
            repos_mart,
            "sales_commercial_overview",
            return_value={
                "kpis": mart_bundle["commercial_kpis"],
                "cfop_breakdown": mart_bundle["cfop_breakdown"],
                "by_hour": mart_bundle["commercial_by_hour"],
                "monthly_evolution": mart_bundle["monthly_evolution"],
                "annual_comparison": mart_bundle["annual_comparison"],
            },
        ) as commercial_bundle:
            payload = repos_mart.sales_overview_bundle(
                "MASTER",
                1,
                None,
                date(2026, 3, 31),
                date(2026, 4, 2),
                as_of=date(2026, 4, 2),
            )

        historical_bundle.assert_called_once_with(
            "MASTER",
            1,
            None,
            date(2026, 3, 31),
            date(2026, 4, 2),
            include_details=True,
        )
        commercial_bundle.assert_called_once()
        self.assertEqual(payload["reading_status"], "mart_snapshot")
        self.assertEqual([int(row["data_key"]) for row in payload["by_day"]], [20260331, 20260401, 20260402])
        self.assertEqual(float(payload["kpis"]["faturamento"]), 600.0)
        self.assertEqual(float(payload["kpis"]["devolucoes"]), 40.0)
        self.assertEqual(float(payload["commercial_kpis"]["cancelamentos"]), 75.0)
        self.assertEqual(float(payload["top_products"][0]["custo_total"]), 420.0)

    def test_sales_live_day_ignores_historical_simulated_reference(self) -> None:
        with patch.object(repos_mart, "business_today", return_value=date(2026, 4, 23)):
            live_day = repos_mart._sales_live_day_in_window(
                date(2026, 4, 22),
                date(2026, 4, 22),
                as_of=date(2026, 4, 22),
                tenant_id=1,
            )

        self.assertIsNone(live_day)

    def test_shifted_latest_window_stays_on_historical_bundle(self) -> None:
        expected = _operational_range_bundle()
        expected["freshness"] = {
            "mode": "historical_snapshot",
            "operational_day": None,
            "live_through_at": None,
            "historical_through_dt": "2026-04-22",
            "source": "mart.agg_vendas_diaria",
        }
        expected["operational_sync"] = {
            "last_sync_at": None,
            "source": "mart.agg_vendas_diaria",
            "dt_ref": "2026-04-22",
        }

        with patch.object(repos_mart, "business_today", return_value=date(2026, 4, 23)), patch.object(
            repos_mart,
            "commercial_window_coverage",
            return_value={
                "mode": "shifted_latest",
                "effective_dt_ini": date(2026, 4, 22),
                "effective_dt_fim": date(2026, 4, 22),
            },
        ), patch.object(
            repos_mart,
            "_sales_historical_bundle_from_marts",
            return_value=expected,
        ) as historical_bundle, patch.object(
            repos_mart,
            "sales_operational_range_bundle",
            side_effect=AssertionError("shifted latest should not use the operational sales window"),
        ), patch.object(
            repos_mart,
            "sales_commercial_overview",
            return_value={
                "kpis": expected["commercial_kpis"],
                "cfop_breakdown": expected["cfop_breakdown"],
                "by_hour": expected["commercial_by_hour"],
                "monthly_evolution": expected["monthly_evolution"],
                "annual_comparison": expected["annual_comparison"],
            },
        ):
            payload = repos_mart.sales_overview_bundle(
                "MASTER",
                1,
                14458,
                date(2026, 4, 23),
                date(2026, 4, 23),
                as_of=date(2026, 4, 23),
            )

        historical_bundle.assert_called_once_with(
            "MASTER",
            1,
            14458,
            date(2026, 4, 22),
            date(2026, 4, 22),
            include_details=True,
        )
        self.assertEqual(payload["reading_status"], "latest_compatible")


if __name__ == "__main__":
    unittest.main()
