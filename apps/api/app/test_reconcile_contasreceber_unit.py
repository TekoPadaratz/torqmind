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
        try:
            import psycopg2.extras as _ex
        except ModuleNotFoundError:
            self.skipTest("psycopg2 is not installed in the API runtime")

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
            rowcount = 0

            def execute(self, sql, params=None):
                calls.append(sql)

            def fetchone(self):
                return [0]

        class _Pg:
            def cursor(self):
                return _Cur()

            def commit(self):
                pass

            def rollback(self):
                pass

        self.mod.refresh_dw_and_mart(_Pg(), 1)
        joined = " ".join(calls)
        # Must delegate to the canonical functions, never TRUNCATE or inline mart SQL.
        self.assertIn("etl.load_fact_financeiro", joined)
        self.assertIn("etl.refresh_customer_delinquency_summary", joined)
        self.assertNotIn("TRUNCATE", joined.upper())
        # Race-proofing: full-scan + a direct DW<-STG payload sync guarded by
        # "payload IS DISTINCT" (the shared 'financeiro' watermark can race us).
        self.assertIn("etl.force_full_scan", joined)
        self.assertIn("UPDATE dw.fact_financeiro", joined)
        self.assertIn("payload IS DISTINCT FROM s.payload", joined)

    def test_refresh_mart_retries_on_transient_error(self):
        # First mart-refresh attempt fails (e.g. transient lock timeout); the
        # script must roll back and retry so the mart never stays stale after
        # STG/DW were healed.
        state = {"mart_attempts": 0, "rolled_back": 0}

        class _Cur:
            rowcount = 0

            def execute(self, sql, params=None):
                if "refresh_customer_delinquency_summary" in sql:
                    state["mart_attempts"] += 1
                    if state["mart_attempts"] == 1:
                        raise RuntimeError("deadlock detected")

            def fetchone(self):
                return [1]

        class _Pg:
            def cursor(self):
                return _Cur()

            def commit(self):
                pass

            def rollback(self):
                state["rolled_back"] += 1

        # Avoid real sleeps between retries.
        orig_sleep = self.mod.time.sleep
        self.mod.time.sleep = lambda *_a, **_k: None
        try:
            self.mod.refresh_dw_and_mart(_Pg(), 1)
        finally:
            self.mod.time.sleep = orig_sleep
        self.assertEqual(state["mart_attempts"], 2)  # failed once, succeeded on retry
        self.assertGreaterEqual(state["rolled_back"], 1)

    # --- phantom closure (deleted/renumbered titles) ------------------------
    def test_key_db_idc_uses_id_db_not_filial(self):
        # ID_CONTASRECEBER is unique per ID_DB, not per ID_FILIAL. The phantom
        # key must use ID_DB (falling back to ID_FILIAL only when absent).
        self.assertEqual(self.mod._key_db_idc({"ID_DB": 14126, "ID_FILIAL": 15172, "ID_CONTASRECEBER": 377040}),
                         (14126, 377040))
        self.assertEqual(self.mod._key_db_idc({"ID_FILIAL": 14122, "ID_CONTASRECEBER": 378285}),
                         (14122, 378285))

    def test_close_phantoms_tombstones_deleted_and_reupserts_paid(self):
        # STG has two open titles; Xpert's open set has neither. One still exists
        # in Xpert (paid straggler) -> re-upsert; the other is gone -> tombstone.
        cr_rows = [
            {"ID_DB": 14122, "ID_FILIAL": 14122, "ID_CONTASRECEBER": 999001, "DTAPGTO": None},  # an unrelated open title
        ]
        stg_open = [(14122, 14122, 378285), (18339, 18339, 54)]
        # Xpert re-fetch by (ID_DB, ID_CONTASRECEBER): 54 still exists (paid), 378285 deleted.
        xpert_found = [{"ID_DB": 18339, "ID_FILIAL": 18339, "ID_CONTASRECEBER": 54,
                        "DTAPGTO": "2025-10-30", "VLRPAGO": 2.59, "DTACONTA": "2025-10-25"}]

        upsert_calls = {"rows": None}

        class _Cur:
            def execute(self, sql, params=None):
                self._last = sql

            def fetchall(self):
                return stg_open

        class _Pg:
            def cursor(self):
                return _Cur()

            def commit(self):
                pass

        mod = self.mod
        orig_fetch = mod._fetch_titles_by_keys
        orig_upsert = mod.upsert_stg
        mod._fetch_titles_by_keys = lambda cfg, keys: xpert_found
        mod.upsert_stg = lambda pg, rows, *a, **k: (upsert_calls.__setitem__("rows", rows) or len(rows))
        try:
            reup, tomb = mod.close_phantoms(_Pg(), {}, cr_rows, 1, dry_run=False)
        finally:
            mod._fetch_titles_by_keys = orig_fetch
            mod.upsert_stg = orig_upsert

        self.assertEqual(reup, 1)   # title 54 re-upserted (paid straggler)
        self.assertEqual(tomb, 1)   # title 378285 tombstoned (deleted)
        self.assertEqual(upsert_calls["rows"], xpert_found)

    def test_close_phantoms_noop_when_all_open(self):
        # Every STG-open title is still open in Xpert -> nothing to close.
        cr_rows = [{"ID_DB": 14122, "ID_FILIAL": 14122, "ID_CONTASRECEBER": 378285, "DTAPGTO": None}]
        stg_open = [(14122, 14122, 378285)]

        class _Cur:
            def execute(self, sql, params=None):
                pass

            def fetchall(self):
                return stg_open

        class _Pg:
            def cursor(self):
                return _Cur()

            def commit(self):
                pass

        reup, tomb = self.mod.close_phantoms(_Pg(), {}, cr_rows, 1, dry_run=False)
        self.assertEqual((reup, tomb), (0, 0))


class DelinquencyMartMigrationTest(unittest.TestCase):
    """Lock in the two server-side guards that keep the mart correct."""

    _MIG_DIR = Path(__file__).resolve().parents[3] / "sql" / "migrations"

    def test_093_serializes_refresh_with_advisory_lock(self):
        # Concurrency race (orchestrator vs reconcile cron) used to abort the
        # DELETE+INSERT refresh and leave the mart stale. The function must take
        # a per-empresa advisory lock so callers serialize.
        sql = (self._MIG_DIR / "093_delinquency_refresh_advisory_lock.sql").read_text(encoding="utf-8", errors="ignore")
        self.assertIn("pg_advisory_xact_lock", sql)
        self.assertIn("refresh_customer_delinquency_summary", sql)

    def test_094_excludes_phantom_tombstoned_titles(self):
        # Titles deleted/renumbered in Xpert get a TORQMIND_RECONCILED_ABSENT
        # marker; the open-receivables CTE must exclude them so they stop
        # showing as overdue.
        sql = (self._MIG_DIR / "094_delinquency_exclude_phantom_titles.sql").read_text(encoding="utf-8", errors="ignore")
        self.assertIn("TORQMIND_RECONCILED_ABSENT", sql)
        self.assertIn("pg_advisory_xact_lock", sql)  # keeps the 093 guard too
        # The exclusion sits on the open-receivables source (payload marker).
        self.assertIn("NOT (f.payload ? 'TORQMIND_RECONCILED_ABSENT')", sql)


if __name__ == "__main__":
    unittest.main()
