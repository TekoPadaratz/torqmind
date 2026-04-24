import unittest
from datetime import datetime

from agent.config import APIConfig, AppConfig, RuntimeConfig, SQLServerConfig
from agent.extractors.xpert import SQLServerExtractor


class _DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


class TestExtractorQuery(unittest.TestCase):
    def _cfg(self):
        return AppConfig(
            sqlserver=SQLServerConfig(),
            api=APIConfig(),
            runtime=RuntimeConfig(),
            datasets={
                "comprovantes": {
                    "enabled": True,
                    "table": "dbo.COMPROVANTES",
                    "watermark_column": "DATAREPL",
                }
            },
            id_empresa=1,
            id_db=1,
        )

    def test_query_plan_datetime_parametrized(self):
        ex = SQLServerExtractor(self._cfg(), _DummyLogger())
        plan = ex._build_query_plan(
            dataset="comprovantes",
            watermark_dt=datetime(2025, 9, 18, 10, 21, 25, 547000),
            dt_from=None,
            dt_to=None,
            watermark_type_detected="datetime",
            watermark_style=None,
        )
        self.assertIn("DATAREPL > ?", plan.sql)
        self.assertEqual(plan.query_mode, "param")
        self.assertEqual(len(plan.params), 1)
        self.assertNotIn("2025-09-18", plan.sql)

    def test_query_plan_text_try_convert(self):
        ex = SQLServerExtractor(self._cfg(), _DummyLogger())
        plan = ex._build_query_plan(
            dataset="comprovantes",
            watermark_dt=datetime(2025, 9, 18, 10, 21, 25, 547000),
            dt_from=None,
            dt_to=None,
            watermark_type_detected="text",
            watermark_style=121,
        )
        self.assertIn("TRY_CONVERT(datetime2, DATAREPL, 121)", plan.sql)
        self.assertEqual(plan.query_mode, "try_convert")
        self.assertEqual(plan.watermark_style, 121)

    def test_watermark_style_fallback_default(self):
        styles = SQLServerExtractor._watermark_styles({})
        self.assertEqual(styles, [121, 103])

    def test_connection_string_includes_security_flags(self):
        cfg = self._cfg()
        cfg.sqlserver = SQLServerConfig(
            driver="ODBC Driver 18 for SQL Server",
            server="10.0.0.10,1433",
            database="atxdados",
            user="u",
            password="p",
            encrypt=False,
            trust_server_certificate=True,
            login_timeout_seconds=15,
        )
        ex = SQLServerExtractor(cfg, _DummyLogger())
        conn = ex._connection_string()
        self.assertIn("Encrypt=no", conn)
        self.assertIn("TrustServerCertificate=yes", conn)
        self.assertIn("LoginTimeout=15", conn)

    def test_quote_ident_escapes_brackets(self):
        self.assertEqual(SQLServerExtractor._quote_ident("ab]cd"), "[ab]]cd]")

    def test_query_plan_can_revisit_recent_parent_window_when_watermark_stalls(self):
        cfg = self._cfg()
        cfg.datasets["itensmovprodutos"] = {
            "enabled": True,
            "table": "dbo.ITENSMOVPRODUTOS",
            "query": (
                "SELECT i.* FROM dbo.ITENSMOVPRODUTOS i "
                "JOIN dbo.MOVPRODUTOS m ON m.ID_FILIAL = i.ID_FILIAL "
                "AND m.ID_DB = i.ID_DB AND m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS"
            ),
            "watermark_column": "DATAREPL",
            "watermark_expression": "i.DATAREPL",
            "revisit_open_clause": "CAST(m.DATA AS date) >= CAST(DATEADD(day,-7,GETDATE()) AS date)",
        }
        ex = SQLServerExtractor(cfg, _DummyLogger())
        plan = ex._build_query_plan(
            dataset="itensmovprodutos",
            watermark_dt=datetime(2026, 3, 31, 10, 0, 0),
            dt_from=None,
            dt_to=None,
            watermark_type_detected="datetime",
            watermark_style=None,
        )
        self.assertIn("i.DATAREPL > ?", plan.sql)
        self.assertIn("CAST(m.DATA AS date) >= CAST(DATEADD(day,-7,GETDATE()) AS date)", plan.sql)
        self.assertEqual(plan.query_mode, "param")
        self.assertEqual(len(plan.params), 1)


if __name__ == "__main__":
    unittest.main()
