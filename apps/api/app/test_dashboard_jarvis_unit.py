import unittest
from datetime import date
from unittest.mock import patch

from app import repos_mart


class DashboardJarvisUnitTests(unittest.TestCase):
    def test_sales_declining_products_signal_uses_closed_30_day_windows_from_mart(self) -> None:
        class _FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class _RecordingConn:
            def __init__(self, rows):
                self.rows = rows
                self.calls = []

            def execute(self, sql, params):
                self.calls.append((sql, list(params)))
                return _FakeResult(self.rows)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        conn = _RecordingConn([])
        dt_ref = date(2026, 4, 15)

        with patch("app.repos_mart.get_conn", return_value=conn):
            signal = repos_mart.sales_declining_products_signal(
                "MASTER",
                7,
                17,
                dt_ref=dt_ref,
            )

        self.assertEqual(
            signal["recent_window"],
            {"dt_ini": "2026-03-16", "dt_fim": "2026-04-14"},
        )
        self.assertEqual(
            signal["prior_window"],
            {"dt_ini": "2026-02-14", "dt_fim": "2026-03-15"},
        )
        self.assertEqual(signal["source_status"], "unavailable")

        executed_sql, executed_params = conn.calls[0]
        self.assertIn("FROM mart.agg_produtos_diaria a", executed_sql)
        self.assertIn("FROM dw.dim_produto p", executed_sql)
        self.assertEqual(
            executed_params,
            [
                20260316,
                20260414,
                20260316,
                20260414,
                20260214,
                20260315,
                20260214,
                20260315,
                7,
                20260214,
                20260414,
                17,
                7,
                17,
                7,
                3,
            ],
        )

    def test_jarvis_briefing_exposes_primary_and_secondary_shortcuts(self) -> None:
        dt_ref = date(2026, 4, 8)
        context = {
            "modeled_risk": {
                "impacto_total": 0.0,
                "eventos_alto_risco": 0,
                "total_eventos": 0,
            },
            "sales": {
                "freshness": {"mode": "hybrid_live"},
                "reading_status": "operational_overlay",
            },
            "cash_live": {
                "source_status": "ok",
                "freshness": {"mode": "operational_live"},
                "kpis": {
                    "caixas_criticos": 0,
                    "total_vendas_abertas": 0.0,
                },
                "open_boxes": [],
            },
            "finance_aging": {
                "receber_total_vencido": 900.0,
                "pagar_total_vencido": 100.0,
                "top5_concentration_pct": 58.0,
                "snapshot_status": "exact",
            },
            "churn": {
                "top_risk": [
                    {
                        "cliente_nome": "Cliente Alfa",
                        "revenue_at_risk_30d": 500.0,
                        "churn_score": 82,
                    }
                ],
                "snapshot_meta": {"snapshot_status": "exact"},
            },
            "payments": {
                "kpis": {
                    "source_status": "ok",
                    "unknown_valor": 0.0,
                },
                "anomalies": [],
            },
            "fraud_operational": {
                "valor_cancelado": 1200.0,
                "cancelamentos": 3,
            },
            "signals": {
                "peak_hours": {
                    "peak_hours": [{"hora": 7, "label": "07h"}, {"hora": 8, "label": "08h"}],
                    "off_peak_hours": [{"hora": 14, "label": "14h"}],
                    "recommendations": {
                        "peak": "Reforce atendimento de pista nas primeiras horas do dia.",
                        "off_peak": "Use a faixa para rotina e reposição.",
                    },
                },
                "declining_products": {
                    "items": [
                        {
                            "produto_nome": "Diesel S10",
                            "grupo_nome": "Combustíveis",
                            "delta_faturamento": 1400.0,
                            "variation_pct": -18.0,
                            "recent_faturamento": 5200.0,
                            "prior_faturamento": 6600.0,
                            "recommendation": "Revise preço de bomba, ruptura, mix de volume e posição na praça antes que a queda vire perda estrutural.",
                        }
                    ]
                },
            },
        }

        with patch("app.repos_mart.risk_by_turn_local", return_value=[]), patch(
            "app.repos_mart.business_today",
            return_value=dt_ref,
        ):
            briefing = repos_mart.jarvis_briefing(
                "MASTER",
                7,
                None,
                dt_ref=dt_ref,
                context=context,
            )

        self.assertEqual(briefing["primary_kind"], "fraud")
        self.assertEqual(briefing["primary_shortcut"]["path"], "/fraud")
        self.assertEqual(briefing["primary_shortcut"]["label"], "Abrir antifraude")
        self.assertEqual(briefing["secondary_focus"][0]["shortcut_path"], "/finance")
        self.assertEqual(briefing["secondary_focus"][0]["shortcut_label"], "Abrir financeiro")
        self.assertEqual(briefing["secondary_focus"][1]["shortcut_path"], "/customers")
        self.assertEqual(briefing["signals"]["peak_hours"]["peak_hours"][0]["label"], "07h")
        self.assertEqual(briefing["signals"]["declining_products"]["items"][0]["produto_nome"], "Diesel S10")

    def test_jarvis_briefing_returns_no_shortcut_when_operation_is_stable(self) -> None:
        dt_ref = date(2026, 4, 8)
        context = {
            "modeled_risk": {
                "impacto_total": 0.0,
                "eventos_alto_risco": 0,
                "total_eventos": 0,
            },
            "sales": {
                "freshness": {"mode": "hybrid_live"},
                "reading_status": "operational_overlay",
            },
            "cash_live": {
                "source_status": "ok",
                "freshness": {"mode": "operational_live"},
                "kpis": {
                    "caixas_criticos": 0,
                    "total_vendas_abertas": 0.0,
                },
                "open_boxes": [],
            },
            "finance_aging": {
                "receber_total_vencido": 0.0,
                "pagar_total_vencido": 0.0,
                "top5_concentration_pct": 0.0,
                "snapshot_status": "exact",
            },
            "churn": {
                "top_risk": [],
                "snapshot_meta": {"snapshot_status": "exact"},
            },
            "payments": {
                "kpis": {
                    "source_status": "ok",
                    "unknown_valor": 0.0,
                },
                "anomalies": [],
            },
            "fraud_operational": {
                "valor_cancelado": 0.0,
                "cancelamentos": 0,
            },
            "signals": {
                "peak_hours": {
                    "peak_hours": [{"hora": 6, "label": "06h"}],
                    "off_peak_hours": [{"hora": 15, "label": "15h"}],
                    "recommendations": {"peak": "Reforce cobertura na abertura.", "off_peak": "Use a janela para rotina."},
                },
                "declining_products": {"items": []},
            },
        }

        with patch("app.repos_mart.risk_by_turn_local", return_value=[]), patch(
            "app.repos_mart.business_today",
            return_value=dt_ref,
        ):
            briefing = repos_mart.jarvis_briefing(
                "MASTER",
                7,
                None,
                dt_ref=dt_ref,
                context=context,
            )

        self.assertIsNone(briefing["primary_shortcut"])
        self.assertEqual(briefing["secondary_focus"], [])
        self.assertEqual(briefing["status"], "ok")
        self.assertEqual(briefing["signals"]["peak_hours"]["peak_hours"][0]["label"], "06h")


if __name__ == "__main__":
    unittest.main()
