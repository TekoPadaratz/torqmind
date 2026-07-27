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

    def test_nfe_query_uses_data_only_for_event_date_and_watermark(self):
        cfg = self._cfg()
        cfg.datasets["nfe"] = {
            "enabled": True,
            "table": "dbo.NFE",
            "watermark_column": "TORQMIND_WATERMARK",
            "event_date_column": "TORQMIND_DT_EVENTO",
            "watermark_order_by": "TORQMIND_WATERMARK, ID_FILIAL, ID_DB, ID_COMPROVANTE, ID_NFE",
            "cursor_pk_columns": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "ID_NFE"],
            "query": (
                "SELECT n.*, "
                "CAST(n.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
                "CAST(n.DATA AS datetime2) AS TORQMIND_WATERMARK "
                "FROM dbo.NFE n"
            ),
        }

        ex = SQLServerExtractor(cfg, _DummyLogger())
        plan = ex._build_query_plan(
            dataset="nfe",
            watermark_dt=datetime(2026, 5, 8, 0, 0, 0),
            dt_from=datetime(2026, 5, 8, 0, 0, 0),
            dt_to=datetime(2026, 5, 9, 0, 0, 0),
            watermark_type_detected="datetime",
            watermark_style=None,
        )

        self.assertIn("TORQMIND_WATERMARK > ?", plan.sql)
        self.assertIn("TORQMIND_DT_EVENTO >= ?", plan.sql)
        self.assertIn("TORQMIND_DT_EVENTO < ?", plan.sql)
        legacy_missing_date_column = "DATA" + "EMISSAO"
        self.assertNotIn(legacy_missing_date_column, plan.sql)
        self.assertNotIn("DATAREPL", plan.sql)
        self.assertEqual(plan.watermark_expr, "TORQMIND_WATERMARK")
        self.assertEqual(plan.event_date_expr, "TORQMIND_DT_EVENTO")
        self.assertEqual(len(plan.params), 3)

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
            "query": (
                "SELECT i.*, "
                "CAST(m.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
                "CAST(i.DATAREPL AS datetime2) AS TORQMIND_WATERMARK "
                "FROM dbo.ITENSMOVPRODUTOS i "
                "JOIN dbo.MOVPRODUTOS m ON m.ID_FILIAL = i.ID_FILIAL "
                "AND m.ID_DB = i.ID_DB AND m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS"
            ),
            "watermark_column": "TORQMIND_WATERMARK",
            "event_date_column": "TORQMIND_DT_EVENTO",
            "revisit_open_clause": "CAST(TORQMIND_DT_EVENTO AS date) >= CAST(DATEADD(day,-7,GETDATE()) AS date)",
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
        self.assertIn("TORQMIND_WATERMARK > ?", plan.sql)
        self.assertIn("CAST(TORQMIND_DT_EVENTO AS date) >= CAST(DATEADD(day,-7,GETDATE()) AS date)", plan.sql)
        self.assertEqual(plan.query_mode, "param")
        self.assertEqual(len(plan.params), 1)

    def test_contasreceber_revisit_recaptures_recently_paid_titles(self):
        # Direct payment regression: a title PAID in CONTASRECEBER (DTAPGTO set)
        # with DATAREPL stuck at the sentinel and a poisoned watermark must still
        # be re-read. The revisit clause has to cover BOTH still-open titles and
        # recently-paid ones, otherwise paid titles freeze as "open" in STG.
        from agent.config import DEFAULT_DATASETS

        cr = DEFAULT_DATASETS["contasreceber"]
        cfg = self._cfg()
        cfg.datasets["contasreceber"] = {**cr, "enabled": True}
        ex = SQLServerExtractor(cfg, _DummyLogger())
        plan = ex._build_query_plan(
            dataset="contasreceber",
            watermark_dt=datetime(2026, 6, 1, 0, 0, 0),
            dt_from=None,
            dt_to=None,
            watermark_type_detected="datetime",
            watermark_style=None,
        )
        # still-open titles (existing safety net)
        self.assertIn("DTAPGTO IS NULL AND CAST(DTACONTA AS date) >=", plan.sql)
        # recently-paid titles (the fix) — no table alias prefix on columns
        self.assertIn("DTAPGTO IS NOT NULL AND CAST(DTAPGTO AS date) >=", plan.sql)
        self.assertNotIn("c.DTAPGTO", plan.sql)
        # DEFINITIVO: DTAPGTO não pode entrar no watermark do cursor (data futura envenena).
        self.assertNotIn("(CAST(c.DTAPGTO AS datetime2))", plan.sql)
        self.assertIn("(CAST(c.DTACONTA AS datetime2))", plan.sql)

    def test_contaspagar_watermark_excludes_future_prone_dates(self):
        from agent.config import DEFAULT_DATASETS

        cp = DEFAULT_DATASETS["contaspagar"]
        cfg = self._cfg()
        cfg.datasets["contaspagar"] = {**cp, "enabled": True}
        ex = SQLServerExtractor(cfg, _DummyLogger())
        plan = ex._build_query_plan(
            dataset="contaspagar",
            watermark_dt=datetime(2026, 6, 1, 0, 0, 0),
            dt_from=None,
            dt_to=None,
            watermark_type_detected="datetime",
            watermark_style=None,
        )
        self.assertNotIn("(CAST(c.DTAPGTO AS datetime2))", plan.sql)
        self.assertNotIn("(CAST(c.DTAVCTO AS datetime2))", plan.sql)
        self.assertIn("(CAST(c.DTACONTA AS datetime2))", plan.sql)
        self.assertIn("DTAPGTO IS NOT NULL AND CAST(DTAPGTO AS date) >=", plan.sql)


if __name__ == "__main__":
    unittest.main()
