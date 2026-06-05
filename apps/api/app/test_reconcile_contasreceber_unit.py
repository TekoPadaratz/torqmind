"""Unit tests for the CONTASRECEBER reconciliation script.

These lock in the data-truth behaviour that heals the "paid title still shown as
overdue" bug (cliente 999 / VR09 regression), without requiring a live database:

* The Xpert hot-window queries only pull OPEN or RECENTLY-PAID titles (covering
  the direct-payment case where DTAPGTO is set but DATAREPL stays at the
  sentinel and the watermark is poisoned).
* The STG upsert only bumps ``received_at`` when a payment field (DTAPGTO /
  VLRPAGO) actually changed, so the canonical DW loader reprocesses only healed
  titles.
* The script carries NO hardcoded credentials.
"""
from __future__ import annotations

import importlib.util
import os
import unittest
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "fix_contasreceber_sync.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("reconcile_contasreceber", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class ReconcileContasreceberScriptTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module()

    # --- security: no hardcoded secrets -------------------------------------
    def test_no_hardcoded_credentials(self):
        text = _SCRIPT.read_text(encoding="utf-8")
        # The previous version hardcoded these. They must never come back.
        self.assertNotIn("XPT2000", text)
        self.assertNotIn("172.30.0.12", text)
        # No literal password assignment (only os.getenv lookups).
        self.assertNotIn('"password": "', text)
        self.assertIn("os.getenv", text)

    def test_config_requires_env(self):
        # Missing SQL Server env must fail fast (SystemExit), never connect blank.
        for key in ("SQLSERVER_HOST", "SQLSERVER_USER", "SQLSERVER_PASSWORD", "SQLSERVER_DATABASE"):
            os.environ.pop(key, None)
        with self.assertRaises(SystemExit):
            self.mod._mssql_config()

    # --- hot-window queries -------------------------------------------------
    def test_extract_query_covers_open_and_recently_paid(self):
        # We don't hit SQL Server; we assert the SQL the script would run by
        # monkeypatching the connector to capture the executed statement.
        captured = {}

        class _Cur:
            def execute(self, sql):
                captured["sql"] = sql

            def fetchall(self):
                return []

        class _Conn:
            def cursor(self, as_dict=False):
                return _Cur()

            def close(self):
                pass

        self.mod._mssql_connect = lambda cfg: _Conn()  # type: ignore
        self.mod.extract_contasreceber({}, paid_days=120)
        sql = captured["sql"]
        # still-open titles
        self.assertIn("c.DTAPGTO IS NULL", sql)
        # recently-paid titles (the fix) — direct payment recapture
        self.assertIn("CAST(c.DTAPGTO AS date) >= CAST(DATEADD(day, -120", sql)

    # --- change-detection predicate ----------------------------------------
    def test_upsert_only_bumps_received_at_on_payment_change(self):
        captured = {"sql": None, "rows": None}

        class _Cur:
            rowcount = 0

            def execute(self, *a, **k):
                pass

            def fetchone(self):
                return [0]

        class _Pg:
            def cursor(self):
                return _Cur()

            def commit(self):
                pass

            def rollback(self):
                pass

        # Patch execute_values to capture the SQL template the upsert builds.
        import psycopg2.extras as _ex

        orig = _ex.execute_values

        def _fake_execute_values(cur, sql, values, template=None, page_size=None):
            captured["sql"] = sql
            captured["rows"] = list(values)

        _ex.execute_values = _fake_execute_values
        try:
            rows = [{"ID_FILIAL": 14122, "ID_DB": 14122, "ID_CONTASRECEBER": 379252,
                     "DTAPGTO": "2026-06-03", "VLRPAGO": 105.04, "DTACONTA": "2026-05-26"}]
            self.mod.upsert_stg(
                _Pg(), rows, "contasreceber", "id_contasreceber", 1,
                change_keys=("DTAPGTO", "VLRPAGO"), dt_evento_key="DTACONTA", dry_run=False,
            )
        finally:
            _ex.execute_values = orig

        sql = captured["sql"]
        # received_at is updated and guarded by the payment-field change predicate
        self.assertIn("received_at = EXCLUDED.received_at", sql)
        self.assertIn("DTAPGTO", sql)
        self.assertIn("VLRPAGO", sql)
        self.assertIn("IS DISTINCT FROM", sql)

    def test_refresh_uses_canonical_etl_functions(self):
        calls = []

        class _Cur:
            def execute(self, sql, params=None):
                calls.append(sql)

            def fetchone(self):
                return [0]

        class _Pg:
            def cursor(self):
                return _Cur()

            def commit(self):
                pass

        self.mod.refresh_dw_and_mart(_Pg(), 1)
        joined = " ".join(calls)
        # Must delegate to the canonical functions, never TRUNCATE or inline mart SQL.
        self.assertIn("etl.load_fact_financeiro", joined)
        self.assertIn("etl.refresh_customer_delinquency_summary", joined)
        self.assertNotIn("TRUNCATE", joined.upper())


if __name__ == "__main__":
    unittest.main()
