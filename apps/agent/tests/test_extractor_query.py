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


if __name__ == "__main__":
    unittest.main()
