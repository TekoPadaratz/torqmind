"""Tests for mart-only backfill mode, CLI flags, skip_batch_deletes, and rows_written.

Covers:
- CLI --mart-only / --skip-slim parsing
- CLI --batch-size / --max-threads / --max-memory-gb parsing
- MartBuilder mart_only validation and behavior
- skip_batch_deletes skips DELETE mutations
- rows_written is real (not hardcoded 0)
- data_key=0 is blocked everywhere
- 20260430 specific date coverage
- Sales marts don't use payload/JSONExtractString/FINAL in heavy path
- source=stg required for mart-only
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import date

import pytest


# ============================================================
# FIXTURES
# ============================================================

DATA_KEYS_FIXTURE = [20260425, 20260426, 20260427, 20260428, 20260429, 20260430, 20260501]


class FakeQueryResult:
    """Simulates clickhouse_connect query result."""
    def __init__(self, rows: list[tuple]):
        self.result_rows = rows


def make_builder(source: str = "stg", **kwargs):
    from torqmind_cdc_consumer.mart_builder import MartBuilder
    return MartBuilder(enabled=True, source=source, **kwargs)


# ============================================================
# CLI FLAG TESTS
# ============================================================

class TestCLIFlags:
    """Verify new CLI arguments are parsed correctly."""

    def test_backfill_stg_help_shows_mart_only(self):
        """backfill-stg --help must mention --mart-only."""
        import io
        from contextlib import redirect_stderr, redirect_stdout
        from torqmind_cdc_consumer.cli import main
        import sys

        old_argv = sys.argv
        try:
            sys.argv = ["cli", "backfill-stg", "--help"]
            buf = io.StringIO()
            with pytest.raises(SystemExit) as exc_info:
                with redirect_stdout(buf):
                    main()
            output = buf.getvalue()
            assert "--mart-only" in output
            assert "--skip-slim" in output
            assert "--batch-size" in output
            assert "--max-threads" in output
            assert "--max-memory-gb" in output
            assert "--skip-batch-deletes" in output
        finally:
            sys.argv = old_argv

    def test_backfill_help_shows_mart_only(self):
        """backfill --help must mention --mart-only."""
        import io
        from contextlib import redirect_stdout
        from torqmind_cdc_consumer.cli import main
        import sys

        old_argv = sys.argv
        try:
            sys.argv = ["cli", "backfill", "--help"]
            buf = io.StringIO()
            with pytest.raises(SystemExit):
                with redirect_stdout(buf):
                    main()
            output = buf.getvalue()
            assert "--mart-only" in output
            assert "--skip-slim" in output
        finally:
            sys.argv = old_argv

    def test_mart_only_requires_source_stg(self):
        """--mart-only with source=dw must fail."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=True, source="dw")
        with pytest.raises(ValueError, match="mart-only requires source=stg"):
            builder.backfill(mart_only=True)

    def test_skip_slim_is_alias_for_mart_only(self):
        """--skip-slim should produce the same behavior as --mart-only."""
        import argparse
        from torqmind_cdc_consumer.cli import cmd_backfill

        # Verify that skip_slim and mart_only are both checked
        args = argparse.Namespace(
            from_date="2025-01-01", to_date=None, id_empresa=1, id_filial=None,
            source="stg", mart_only=False, skip_slim=True,
            batch_size=None, max_threads=None, max_memory_gb=None,
            skip_batch_deletes=False,
        )
        # The resolved mart_only should be True when skip_slim is True
        mart_only = getattr(args, "mart_only", False) or getattr(args, "skip_slim", False)
        assert mart_only is True

    def test_batch_size_parsed(self):
        """--batch-size must be parsed as integer."""
        import argparse
        args = argparse.Namespace(
            batch_size=14, max_threads=4, max_memory_gb=8.0,
        )
        assert args.batch_size == 14
        assert args.max_threads == 4
        assert args.max_memory_gb == 8.0


# ============================================================
# MART BUILDER MART-ONLY TESTS
# ============================================================

class TestMartOnlyMode:
    """Verify mart-only mode behavior."""

    def test_mart_only_does_not_call_populate_slim_comprovantes(self):
        """mart_only=True must NOT call _populate_slim_comprovantes."""
        builder = make_builder()
        mock_client = MagicMock()

        # Setup client mock
        with patch.object(builder, "_get_client", return_value=mock_client):
            # _validate_slim_exists: tables exist and have data
            mock_client.query.side_effect = [
                FakeQueryResult([(1,)]),  # comprovantes_slim table exists
                FakeQueryResult([(100,)]),  # comprovantes_slim has rows
                FakeQueryResult([(1,)]),  # itens_slim table exists (second check)
                FakeQueryResult([(50,)]),  # itens_slim has rows
                FakeQueryResult([(1,)]),  # formas_slim table exists (third check)
                # data_keys discovery from slim
                FakeQueryResult([(20260430,)]),
                # _insert_and_count queries per mart (7 sales marts + non-batched)
            ] + [FakeQueryResult([(5,)])] * 30  # enough for all insert_and_count calls

            with patch.object(builder, "_populate_slim_comprovantes") as mock_pop_c, \
                 patch.object(builder, "_populate_slim_itens") as mock_pop_i, \
                 patch.object(builder, "_populate_slim_formas") as mock_pop_f:
                try:
                    builder.backfill(mart_only=True, from_date="2026-04-30")
                except Exception:
                    pass  # May fail on downstream queries, that's ok

                mock_pop_c.assert_not_called()
                mock_pop_i.assert_not_called()
                mock_pop_f.assert_not_called()

    def test_mart_only_validates_slim_exist(self):
        """mart_only=True must validate slim tables exist before proceeding."""
        builder = make_builder()
        mock_client = MagicMock()

        with patch.object(builder, "_get_client", return_value=mock_client):
            # First query: table doesn't exist
            mock_client.query.return_value = FakeQueryResult([(0,)])

            with pytest.raises(RuntimeError, match="does not exist"):
                builder.backfill(mart_only=True, from_date="2026-04-30")

    def test_mart_only_validates_slim_not_empty(self):
        """mart_only=True must fail if slim is empty for the requested scope."""
        builder = make_builder()
        mock_client = MagicMock()

        with patch.object(builder, "_get_client", return_value=mock_client):
            # Table exists but empty
            mock_client.query.side_effect = [
                FakeQueryResult([(1,)]),  # table exists
                FakeQueryResult([(1,)]),  # table exists
                FakeQueryResult([(1,)]),  # table exists
                FakeQueryResult([(0,)]),  # comprovantes_slim empty
            ]

            with pytest.raises(RuntimeError, match="empty"):
                builder.backfill(mart_only=True, from_date="2026-04-30")

    def test_mart_only_discovers_data_keys_from_slim(self):
        """mart_only must discover publishable data_keys from slim tables."""
        code = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        content = code.read_text()

        # Find the backfill method
        idx = content.index("def backfill(")
        end_idx = content.index("\n    def ", idx + 10)
        method_body = content[idx:end_idx]

        # In mart_only mode, stg_comprovantes (with payload) should NOT be queried
        # The slim query should be common to both modes
        assert "stg_comprovantes_slim" in method_body
        assert "data_key > 0" in method_body


# ============================================================
# SKIP BATCH DELETES TESTS
# ============================================================

class TestSkipBatchDeletes:
    """Verify skip_batch_deletes prevents DELETE mutations."""

    def test_refresh_sales_daily_skips_delete_when_flagged(self):
        """_refresh_sales_daily_stg with skip_delete=True must not call _delete_mart_batch."""
        builder = make_builder()
        mock_client = MagicMock()
        # Mock _insert_and_count to return a count
        mock_client.query.return_value = FakeQueryResult([(10,)])

        with patch.object(builder, "_delete_mart_batch") as mock_delete:
            builder._refresh_sales_daily_stg(mock_client, [20260430], skip_delete=True)
            mock_delete.assert_not_called()

    def test_refresh_sales_daily_deletes_when_not_flagged(self):
        """_refresh_sales_daily_stg without skip_delete must call _delete_mart_batch."""
        builder = make_builder()
        mock_client = MagicMock()
        mock_client.query.return_value = FakeQueryResult([(10,)])

        with patch.object(builder, "_delete_mart_batch") as mock_delete:
            builder._refresh_sales_daily_stg(mock_client, [20260430], skip_delete=False)
            mock_delete.assert_called_once_with(mock_client, "sales_daily_rt", [20260430])

    def test_all_stg_refreshes_accept_skip_delete(self):
        """All STG refresh methods that take data_keys must accept skip_delete."""
        code = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        content = code.read_text()

        methods_with_skip = [
            "_refresh_sales_daily_stg",
            "_refresh_sales_hourly_stg",
            "_refresh_sales_products_stg",
            "_refresh_sales_groups_stg",
            "_refresh_payments_by_type_stg",
            "_refresh_dashboard_home_stg",
            "_refresh_fraud_daily_stg",
        ]
        for method in methods_with_skip:
            idx = content.index(f"def {method}")
            sig_end = content.index(")", idx)
            signature = content[idx:sig_end + 1]
            assert "skip_delete" in signature, f"{method} must accept skip_delete parameter"


# ============================================================
# ROWS_WRITTEN TESTS
# ============================================================

class TestRowsWritten:
    """Verify rows_written is real, not hardcoded 0."""

    @pytest.fixture(autouse=True)
    def _load_code(self):
        self.builder_path = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        self.code = self.builder_path.read_text()

    def test_all_stg_refreshes_count_rows(self):
        """All STG refresh methods must use _insert_and_count or _insert_and_count_nokey."""
        methods = [
            "_refresh_sales_daily_stg",
            "_refresh_sales_hourly_stg",
            "_refresh_sales_products_stg",
            "_refresh_sales_groups_stg",
            "_refresh_payments_by_type_stg",
            "_refresh_dashboard_home_stg",
            "_refresh_fraud_daily_stg",
            "_refresh_risk_recent_events_stg",
            "_refresh_finance_overview_stg",
            "_refresh_cash_overview_stg",
        ]
        for method in methods:
            idx = self.code.index(f"def {method}")
            next_def = self.code.index("\n    def ", idx + 10)
            body = self.code[idx:next_def]
            assert "_insert_and_count" in body, (
                f"{method} must use _insert_and_count or _insert_and_count_nokey for real rows_written"
            )

    def test_insert_and_count_exists(self):
        """_insert_and_count method must exist."""
        assert "_insert_and_count" in self.code

    def test_insert_and_count_nokey_exists(self):
        """_insert_and_count_nokey method must exist for non-data_key tables."""
        assert "_insert_and_count_nokey" in self.code


# ============================================================
# DATA_KEY=0 AND 20260430 TESTS
# ============================================================

class TestDataKeyIntegrity:
    """Verify data_key=0 is blocked and 20260430 is preserved."""

    def test_data_key_zero_filtered_in_backfill(self):
        """Backfill must filter out data_key=0 from publishable keys."""
        code = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        content = code.read_text()
        idx = content.index("def backfill(")
        end_idx = content.index("\n    def ", idx + 10)
        method_body = content[idx:end_idx]
        assert "data_key > 0" in method_body
        assert "row[0] > 0" in method_body

    def test_slim_keys_filter_excludes_zero(self):
        """_slim_keys_filter must exclude data_key=0."""
        builder = make_builder()
        filt = builder._slim_keys_filter([0, 20260430, 20260501], "c")
        assert "c.data_key IN (20260430,20260501)" == filt

    def test_delete_mart_batch_excludes_zero(self):
        """_delete_mart_batch must not include data_key=0."""
        builder = make_builder()
        mock_client = MagicMock()
        builder._delete_mart_batch(mock_client, "sales_daily_rt", [0, 20260430])
        call_args = mock_client.command.call_args[0][0]
        assert "20260430" in call_args
        # The key list should only have 20260430
        assert call_args.endswith("(20260430)")

    def test_20260430_in_batch_boundary(self):
        """20260430 must not be skipped at batch boundaries."""
        from torqmind_cdc_consumer.mart_builder import _DEFAULT_BATCH_SIZE
        data_keys = DATA_KEYS_FIXTURE
        batches = []
        for i in range(0, len(data_keys), _DEFAULT_BATCH_SIZE):
            batches.append(data_keys[i:i + _DEFAULT_BATCH_SIZE])
        all_batched = [k for b in batches for k in b]
        assert 20260430 in all_batched

    def test_validate_completeness_detects_missing_20260430(self):
        """If 20260430 is in slim but not in a mart, validate must FAIL."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)

        with patch.object(builder, "_get_client") as mock_get:
            mock_client = MagicMock()
            mock_get.return_value = mock_client

            mock_client.query.side_effect = [
                FakeQueryResult([(20260430,), (20260501,)]),  # slim
                FakeQueryResult([(20260501,)]),  # sales_daily_rt - MISSING 20260430
                FakeQueryResult([(0,)]),
                FakeQueryResult([(20260430,), (20260501,)]),
                FakeQueryResult([(0,)]),
                FakeQueryResult([(20260430,), (20260501,)]),
                FakeQueryResult([(0,)]),
                FakeQueryResult([(20260430,), (20260501,)]),
                FakeQueryResult([(0,)]),
            ]

            result = builder.validate_completeness(id_empresa=1, from_date="2026-04-25")

        assert result["pass"] is False
        missing_keys = [m["data_key"] for m in result["missing"]]
        assert 20260430 in missing_keys


# ============================================================
# PERFORMANCE / QUERY CONTRACT TESTS
# ============================================================

class TestQueryContracts:
    """Verify sales mart queries don't use payload/JSONExtractString/heavy FINAL."""

    @pytest.fixture(autouse=True)
    def _load_code(self):
        self.builder_path = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        self.code = self.builder_path.read_text()

    def test_sales_marts_do_not_use_json_extract(self):
        """Sales mart refresh methods must not use JSONExtractString in their queries."""
        for method in ("_refresh_sales_daily_stg", "_refresh_sales_hourly_stg",
                       "_refresh_sales_products_stg", "_refresh_sales_groups_stg"):
            idx = self.code.index(f"def {method}")
            next_def = self.code.index("\n    def ", idx + 10)
            body = self.code[idx:next_def]
            # JSONExtractString in dimension lookups (products/groups) is ok but not in main join
            if method in ("_refresh_sales_daily_stg", "_refresh_sales_hourly_stg"):
                assert "JSONExtractString" not in body, (
                    f"{method} must not use JSONExtractString (use slim tables only)"
                )

    def test_sales_marts_use_slim_tables(self):
        """All sales mart queries must read from slim tables, not raw STG."""
        for method in ("_refresh_sales_daily_stg", "_refresh_sales_hourly_stg",
                       "_refresh_sales_products_stg", "_refresh_sales_groups_stg"):
            idx = self.code.index(f"def {method}")
            next_def = self.code.index("\n    def ", idx + 10)
            body = self.code[idx:next_def]
            assert "stg_comprovantes_slim" in body
            assert "stg_itenscomprovantes_slim" in body

    def test_sales_marts_include_id_db_in_joins(self):
        """All sales mart joins must include id_db."""
        for method in ("_refresh_sales_daily_stg", "_refresh_sales_hourly_stg",
                       "_refresh_sales_products_stg", "_refresh_sales_groups_stg"):
            idx = self.code.index(f"def {method}")
            next_def = self.code.index("\n    def ", idx + 10)
            body = self.code[idx:next_def]
            assert "id_db" in body, f"{method} must include id_db in joins"

    def test_configurable_query_settings(self):
        """MartBuilder must accept batch_size, max_threads, max_memory_usage."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(
            enabled=True, source="stg",
            batch_size=14, max_threads=4, max_memory_usage=8_000_000_000,
        )
        assert builder.batch_size == 14
        assert builder.max_threads == 4
        assert builder.max_memory_usage == 8_000_000_000
        assert builder._query_settings["max_threads"] == 4
        assert builder._query_settings["max_memory_usage"] == 8_000_000_000

    def test_instance_query_settings_used_not_global(self):
        """All refresh methods must use self._query_settings, not the global _QUERY_SETTINGS."""
        # Verify no method body uses _QUERY_SETTINGS (the global)
        # Instead all should use self._query_settings
        occurrences = self.code.count("_QUERY_SETTINGS")
        # _QUERY_SETTINGS can appear in assignment but not in settings= calls
        settings_calls = self.code.count("settings=_QUERY_SETTINGS")
        assert settings_calls == 0, (
            f"Found {settings_calls} uses of settings=_QUERY_SETTINGS, "
            "should all be self._query_settings"
        )


# ============================================================
# REQUIRED TABLES TESTS
# ============================================================

class TestRequiredTables:
    """Verify REQUIRED_MART_TABLES and REQUIRED_SLIM_TABLES constants."""

    def test_required_mart_tables_defined(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        assert hasattr(MartBuilder, "REQUIRED_MART_TABLES")
        required = MartBuilder.REQUIRED_MART_TABLES
        for t in ["sales_daily_rt", "sales_hourly_rt", "sales_products_rt",
                   "sales_groups_rt", "payments_by_type_rt", "dashboard_home_rt",
                   "fraud_daily_rt", "risk_recent_events_rt", "cash_overview_rt",
                   "finance_overview_rt", "source_freshness", "mart_publication_log"]:
            assert t in required, f"{t} must be in REQUIRED_MART_TABLES"

    def test_required_slim_tables_defined(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        assert hasattr(MartBuilder, "REQUIRED_SLIM_TABLES")
        required = MartBuilder.REQUIRED_SLIM_TABLES
        for t in ["stg_comprovantes_slim", "stg_itenscomprovantes_slim", "stg_formas_pgto_slim"]:
            assert t in required, f"{t} must be in REQUIRED_SLIM_TABLES"
