"""Tests for realtime mart bundle contracts.

Validates that dashboard_home_bundle and sales_overview_bundle return payloads
compatible with the Pydantic response models AND the frontend field expectations.
"""
from __future__ import annotations

import sys
import types
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

# Stub clickhouse_connect if not installed
try:
    import clickhouse_connect  # noqa: F401
except ModuleNotFoundError:
    fake_clickhouse = types.ModuleType("clickhouse_connect")
    fake_client_module = types.SimpleNamespace(Client=object)
    fake_clickhouse.driver = types.SimpleNamespace(
        client=fake_client_module,
        exceptions=types.SimpleNamespace(
            DatabaseError=Exception, OperationalError=Exception
        ),
    )
    fake_clickhouse.get_client = lambda **_kwargs: MagicMock()
    sys.modules["clickhouse_connect"] = fake_clickhouse
    sys.modules["clickhouse_connect.driver"] = fake_clickhouse.driver
    sys.modules["clickhouse_connect.driver.exceptions"] = fake_clickhouse.driver.exceptions

from app.schemas_bi import CashOverviewResponse, DashboardHomeResponse, SalesOverviewResponse


# ---------------------------------------------------------------------------
# Fake query_dict responses per table
# ---------------------------------------------------------------------------

_FAKE_SALES_DAILY_KPI = [{"faturamento": Decimal("100000.00"), "margem": Decimal("20000.00"), "qtd_vendas": 500, "ticket_medio": Decimal("200.00")}]
_FAKE_SALES_DAILY_KPI_FULL = [{"faturamento": Decimal("100000.00"), "qtd_vendas": 500, "qtd_itens": 2000, "qtd_canceladas": 5, "valor_cancelado": Decimal("1500.00"), "desconto_total": Decimal("800.00"), "margem": Decimal("20000.00"), "ticket_medio": Decimal("200.00")}]
_FAKE_SALES_BY_DAY = [{"dt": "2026-04-01", "faturamento": Decimal("5000.00"), "qtd_vendas": 20}]
_FAKE_SALES_BY_HOUR = [{"hora": 10, "faturamento": Decimal("3000.00"), "qtd_vendas": 15}]
_FAKE_TOP_PRODUCTS = [{"id_produto": 1, "nome_produto": "Produto A", "produto_nome": "Produto A", "nome_grupo": "Grupo X", "grupo_nome": "Grupo X", "unidade": "LT", "quantity_kind": "fuel", "faturamento": Decimal("5000.00"), "qtd": Decimal("100.000"), "margem": Decimal("1000.00"), "custo_total": Decimal("4000.00"), "valor_unitario_medio": 50.0}]
_FAKE_TOP_GROUPS = [{"id_grupo_produto": 1, "grupo_nome": "Grupo X", "faturamento": Decimal("8000.00"), "margem": Decimal("2000.00"), "qtd_itens": 300}]
_FAKE_FRAUD = [{"qtd_eventos": 10, "impacto_total": Decimal("500.00"), "score_medio": 0.45}]
_FAKE_CASH = [{"qtd_abertos": 3, "fat_aberto": Decimal("12000.00")}]
_FAKE_FINANCE = [{"tipo_titulo": 1, "faixa": "vencido_30d", "qtd_titulos": 5, "valor_total": Decimal("3000.00"), "valor_em_aberto": Decimal("2500.00")}]
_FAKE_CANCEL_KPIS = [{"qtd_canceladas": 5, "valor_cancelado": Decimal("1500.00")}]
_FAKE_MONTHLY = [
    {"ano": 2026, "mes": 1, "faturamento": Decimal("90000.00"), "qtd_vendas": 400, "valor_cancelado": Decimal("1000.00")},
    {"ano": 2025, "mes": 1, "faturamento": Decimal("80000.00"), "qtd_vendas": 350, "valor_cancelado": Decimal("900.00")},
]


def _make_query_dict_side_effect(fn_name: str):
    """Return appropriate fake data based on the SQL query content."""
    def side_effect(query: str, parameters=None):
        q = query.lower()
        if "sales_daily_rt" in q and "group by" not in q and "toYear" not in q and "s_cancel" in q:
            return _FAKE_CANCEL_KPIS
        if "sales_daily_rt" in q and "group by" not in q and "toYear" not in q:
            if fn_name == "sales":
                return _FAKE_SALES_DAILY_KPI_FULL
            return _FAKE_SALES_DAILY_KPI
        if "sales_daily_rt" in q and "toyear" in q:
            return _FAKE_MONTHLY
        if "sales_daily_rt" in q and "group by" in q:
            return _FAKE_SALES_BY_DAY
        if "sales_hourly_rt" in q:
            return _FAKE_SALES_BY_HOUR
        if "sales_products_rt" in q:
            return _FAKE_TOP_PRODUCTS
        if "sales_groups_rt" in q:
            return _FAKE_TOP_GROUPS
        if "fraud_daily_rt" in q:
            return _FAKE_FRAUD
        if "cash_overview_rt" in q:
            return _FAKE_CASH
        if "finance_overview_rt" in q:
            return _FAKE_FINANCE
        return []
    return side_effect


class TestDashboardHomeBundleContract(unittest.TestCase):
    """Validate dashboard_home_bundle returns the full frontend contract."""

    @patch("app.repos_mart_realtime.query_dict")
    def test_returns_full_contract(self, mock_qd: MagicMock):
        mock_qd.side_effect = _make_query_dict_side_effect("dashboard")

        from app.repos_mart_realtime import dashboard_home_bundle

        result = dashboard_home_bundle(
            role="ADMIN",
            id_empresa=1,
            id_filial=None,
            dt_ini=date(2026, 4, 1),
            dt_fim=date(2026, 4, 30),
        )

        # Top-level required by Pydantic
        self.assertIsInstance(result["kpis"], dict)
        self.assertIsInstance(result["series"], dict)
        self.assertEqual(result["source"], "realtime")
        self.assertEqual(result["realtime_source"], "stg")

        # overview.sales
        overview = result["overview"]
        self.assertIn("sales", overview)
        sales = overview["sales"]
        self.assertIn("kpis", sales)
        self.assertIn("by_day", sales)
        self.assertIn("by_hour", sales)
        self.assertIn("top_products", sales)
        self.assertIn("top_groups", sales)

        # overview.fraud
        self.assertIn("fraud", overview)
        self.assertIn("operational", overview["fraud"])
        self.assertIn("modeled_risk", overview["fraud"])

        # overview.risk
        self.assertIn("risk", overview)
        self.assertIn("kpis", overview["risk"])

        # overview.cash.live_now with proper kpis
        self.assertIn("cash", overview)
        live_now = overview["cash"]["live_now"]
        self.assertIn("kpis", live_now)
        cash_kpis = live_now["kpis"]
        self.assertIn("caixas_abertos", cash_kpis)
        self.assertIn("caixas_criticos", cash_kpis)
        self.assertIn("caixas_em_monitoramento", cash_kpis)
        self.assertIn("caixas_alto_risco", cash_kpis)
        self.assertIn("total_vendas_abertas", cash_kpis)

        # finance.aging as dict
        finance = result["finance"]
        self.assertIsInstance(finance["aging"], dict)
        self.assertIn("receber_total_vencido", finance["aging"])
        self.assertIn("pagar_total_vencido", finance["aging"])
        self.assertIn("top5_concentration_pct", finance["aging"])

        # cash top-level live_now with proper kpis
        cash = result["cash"]
        self.assertIn("live_now", cash)
        self.assertIn("kpis", cash["live_now"])
        self.assertIn("caixas_abertos", cash["live_now"]["kpis"])
        self.assertIn("total_vendas_abertas", cash["live_now"]["kpis"])

        # churn
        self.assertIn("churn", result)
        self.assertIn("top_risk", result["churn"])

        # notifications
        self.assertIn("notifications_unread", result)

    @patch("app.repos_mart_realtime.query_dict")
    def test_pydantic_validates_dashboard_response(self, mock_qd: MagicMock):
        mock_qd.side_effect = _make_query_dict_side_effect("dashboard")

        from app.repos_mart_realtime import dashboard_home_bundle

        result = dashboard_home_bundle(
            role="ADMIN", id_empresa=1, id_filial=None,
            dt_ini=date(2026, 4, 1), dt_fim=date(2026, 4, 30),
        )
        # Must not raise
        validated = DashboardHomeResponse.model_validate(result)
        self.assertIsInstance(validated.kpis, dict)
        self.assertIsInstance(validated.series, dict)


class TestSalesOverviewBundleContract(unittest.TestCase):
    """Validate sales_overview_bundle returns the full frontend contract."""

    @patch("app.repos_mart_realtime.query_dict")
    def test_returns_full_contract(self, mock_qd: MagicMock):
        mock_qd.side_effect = _make_query_dict_side_effect("sales")

        from app.repos_mart_realtime import sales_overview_bundle

        result = sales_overview_bundle(
            role="ADMIN",
            id_empresa=1,
            id_filial=None,
            dt_ini=date(2026, 4, 1),
            dt_fim=date(2026, 4, 30),
        )

        # Pydantic-declared fields
        self.assertIsInstance(result["kpis"], dict)
        self.assertIsInstance(result["series"], dict)
        self.assertIsInstance(result["ranking"], list)
        self.assertEqual(result["source"], "realtime")
        self.assertEqual(result["realtime_source"], "stg")

        # commercial_kpis
        ck = result["commercial_kpis"]
        self.assertIn("saidas", ck)
        self.assertIn("qtd_saidas", ck)
        self.assertIn("entradas", ck)
        self.assertIn("cancelamentos", ck)
        self.assertIn("qtd_cancelamentos", ck)

        # cfop_breakdown - no "CFOP>5000"
        cfop = result["cfop_breakdown"]
        self.assertIsInstance(cfop, list)
        self.assertTrue(len(cfop) >= 2)
        for item in cfop:
            self.assertNotIn("CFOP", item.get("label", ""))
        self.assertEqual(cfop[0]["label"], "Vendas normais")

        # commercial_by_hour
        cbh = result["commercial_by_hour"]
        self.assertIsInstance(cbh, list)
        if cbh:
            self.assertIn("saidas", cbh[0])

        # by_day, by_hour
        self.assertIsInstance(result["by_day"], list)
        self.assertIsInstance(result["by_hour"], list)

        # top_products with custo_total
        tp = result["top_products"]
        self.assertIsInstance(tp, list)
        if tp:
            self.assertIn("custo_total", tp[0])
            self.assertIn("produto_nome", tp[0])
            self.assertIn("grupo_nome", tp[0])
            self.assertIn("faturamento", tp[0])
            self.assertIn("margem", tp[0])
            self.assertIn("valor_unitario_medio", tp[0])
            self.assertIn("unidade", tp[0])
            self.assertIn("quantity_kind", tp[0])

        # top_groups
        self.assertIsInstance(result["top_groups"], list)

        # monthly_evolution with saidas
        me = result["monthly_evolution"]
        self.assertIsInstance(me, list)
        if me:
            self.assertIn("saidas", me[0])
            self.assertIn("month_ref", me[0])
            self.assertIn("cancelamentos", me[0])

        # annual_comparison with saidas_atual/saidas_anterior and 12 months
        ac = result["annual_comparison"]
        self.assertEqual(ac["current_year"], 2026)
        self.assertEqual(ac["previous_year"], 2025)
        months = ac["months"]
        self.assertEqual(len(months), 12)
        self.assertIn("saidas_atual", months[0])
        self.assertIn("saidas_anterior", months[0])
        self.assertIn("cancelamentos_atual", months[0])
        self.assertIn("cancelamentos_anterior", months[0])
        self.assertIn("month_ref_atual", months[0])
        self.assertIn("month_ref_anterior", months[0])


class TestFraudRealtimeContract(unittest.TestCase):
    """Validate antifraud operational KPIs match the frontend contract."""

    @patch("app.repos_mart_realtime.query_dict")
    def test_fraud_kpis_exposes_operational_keys(self, mock_qd: MagicMock):
        mock_qd.side_effect = _make_query_dict_side_effect("fraud")

        from app.repos_mart_realtime import fraud_kpis

        result = fraud_kpis(
            role="ADMIN",
            id_empresa=1,
            id_filial=None,
            dt_ini=date(2026, 4, 1),
            dt_fim=date(2026, 4, 30),
        )

        self.assertEqual(result["cancelamentos"], 10)
        self.assertEqual(result["valor_cancelado"], 500.0)
        self.assertEqual(result["qtd_eventos"], 10)
        self.assertEqual(result["impacto_total"], 500.0)
        self.assertEqual(result["score_medio"], 0.45)

    @patch("app.repos_mart_realtime.query_dict")
    def test_pydantic_validates_sales_response(self, mock_qd: MagicMock):
        mock_qd.side_effect = _make_query_dict_side_effect("sales")

        from app.repos_mart_realtime import sales_overview_bundle

        result = sales_overview_bundle(
            role="ADMIN", id_empresa=1, id_filial=None,
            dt_ini=date(2026, 4, 1), dt_fim=date(2026, 4, 30),
        )
        # Must not raise
        validated = SalesOverviewResponse.model_validate(result)
        self.assertIsInstance(validated.kpis, dict)
        self.assertIsInstance(validated.series, dict)

    @patch("app.repos_mart_realtime.query_dict")
    def test_annual_comparison_twelve_months(self, mock_qd: MagicMock):
        mock_qd.side_effect = _make_query_dict_side_effect("sales")

        from app.repos_mart_realtime import sales_overview_bundle

        result = sales_overview_bundle(
            role="ADMIN", id_empresa=1, id_filial=None,
            dt_ini=date(2026, 4, 1), dt_fim=date(2026, 4, 30),
        )
        months = result["annual_comparison"]["months"]
        self.assertEqual(len(months), 12)
        # Verify months are 1-12
        for i, m in enumerate(months):
            self.assertEqual(m["mes"], i + 1)


class TestFallbackDisabled(unittest.TestCase):
    """When REALTIME_MARTS_FALLBACK=false, errors must propagate."""

    @patch("app.repos_mart_realtime.query_dict")
    def test_dashboard_raises_on_clickhouse_error(self, mock_qd: MagicMock):
        mock_qd.side_effect = Exception("ClickHouse unavailable")

        from app.repos_mart_realtime import dashboard_home_bundle

        with self.assertRaises(Exception):
            dashboard_home_bundle(
                role="ADMIN", id_empresa=1, id_filial=None,
                dt_ini=date(2026, 4, 1), dt_fim=date(2026, 4, 30),
            )

    @patch("app.repos_mart_realtime.query_dict")
    def test_sales_raises_on_clickhouse_error(self, mock_qd: MagicMock):
        mock_qd.side_effect = Exception("ClickHouse unavailable")

        from app.repos_mart_realtime import sales_overview_bundle

        with self.assertRaises(Exception):
            sales_overview_bundle(
                role="ADMIN", id_empresa=1, id_filial=None,
                dt_ini=date(2026, 4, 1), dt_fim=date(2026, 4, 30),
            )


class TestCashOverviewRealtimeLabels(unittest.TestCase):
    @patch("app.repos_mart_realtime.query_dict")
    def test_cash_uses_real_labels_when_current_dimensions_exist(self, mock_qd: MagicMock):
        def side_effect(query: str, parameters=None):
            q = query.lower()
            if "from torqmind_mart_rt.cash_overview_rt" in q and "order by abertura_ts desc" in q:
                return [{
                    "id_filial": 10169,
                    "id_turno": 7134,
                    "id_usuario": 9,
                    "nome_operador": "Camila S",
                    "abertura_ts": datetime(2026, 4, 30, 10, 0, tzinfo=timezone.utc),
                    "fechamento_ts": None,
                    "is_aberto": 1,
                    "faturamento_turno": Decimal("950.00"),
                    "qtd_vendas_turno": 12,
                }]
            if "from torqmind_mart_rt.cash_overview_rt" in q and "order by faturamento_turno desc" in q:
                return [{
                    "id_filial": 10169,
                    "id_turno": 7134,
                    "id_usuario": 9,
                    "nome_operador": "Camila S",
                    "abertura_ts": datetime(2026, 4, 30, 10, 0, tzinfo=timezone.utc),
                    "fechamento_ts": None,
                    "is_aberto": 1,
                    "faturamento_turno": Decimal("950.00"),
                    "qtd_vendas_turno": 12,
                }]
            if "from torqmind_current.stg_filiais" in q:
                return [{"id_filial": 10169, "filial_nome": "AUTO POSTO VR 07"}]
            if "from torqmind_current.stg_turnos" in q:
                return [{"id_filial": 10169, "id_turno": 7134, "turno_value": "3"}]
            if "from torqmind_mart_rt.sales_daily_rt" in q and "group by dt" in q:
                return [{"dt": "2026-04-30", "faturamento": Decimal("950.00"), "qtd_vendas": 12}]
            if "from torqmind_mart_rt.sales_daily_rt" in q and "s_cancel_qtd" in q:
                return [{"total_vendas": Decimal("950.00"), "total_cancelamentos": Decimal("50.00"), "qtd_vendas": 12, "qtd_cancelamentos": 1}]
            if "from torqmind_mart_rt.payments_by_type_rt" in q:
                return [{"label": "DINHEIRO", "category": "DINHEIRO", "valor_total": Decimal("900.00"), "qtd_transacoes": 10}]
            return []

        mock_qd.side_effect = side_effect

        from app.repos_mart_realtime import cash_overview

        result = cash_overview(
            role="ADMIN",
            id_empresa=1,
            id_filial=None,
            dt_ini=date(2026, 4, 1),
            dt_fim=date(2026, 4, 30),
        )

        validated = CashOverviewResponse.model_validate(result)
        turno = validated.turnos[0]
        self.assertEqual(turno["filial_label"], "AUTO POSTO VR 07")
        self.assertEqual(turno["turno_label"], "3")
        self.assertEqual(turno["usuario_label"], "Camila S")
        self.assertEqual(float(result["kpis"]["recebimentos_periodo"]), 900.0)
        self.assertEqual(float(result["kpis"]["cancelamentos_periodo"]), 50.0)
        self.assertEqual(float(result["historical"]["kpis"]["recebimentos_periodo"]), 900.0)
        self.assertEqual(float(result["historical"]["kpis"]["cancelamentos_periodo"]), 50.0)
        self.assertEqual(result["payment_mix"], result["historical"]["payment_mix"])
        self.assertNotEqual(turno["filial_label"], "Filial 10169")
        self.assertNotEqual(turno["usuario_label"], "Operador #9")


if __name__ == "__main__":
    unittest.main()
