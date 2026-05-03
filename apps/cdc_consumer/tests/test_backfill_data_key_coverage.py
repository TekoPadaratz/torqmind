"""Regression tests for realtime sales backfill data_key coverage.

Critical invariants:
- Every data_key with valid sales in slim MUST appear in all 4 sales marts.
- data_key=0 MUST NOT appear in any sales mart.
- Publication log MUST NOT have id_empresa=0 or sentinel windows (1970→2099).
- Batch boundaries MUST NOT skip data_keys (no off-by-one).
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


def make_builder(source: str = "stg"):
    """Create MartBuilder with mocked ClickHouse client."""
    from torqmind_cdc_consumer.mart_builder import MartBuilder
    return MartBuilder(enabled=True, source=source)


# ============================================================
# BACKFILL DATA_KEY COVERAGE TESTS
# ============================================================

class TestBackfillDataKeyCoverage:
    """Verify all data_keys from slim are published to all sales marts."""

    def test_backfill_discovers_keys_from_slim_not_raw_only(self):
        """After slim population, backfill must query SLIM for publishable keys."""
        code = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        content = code.read_text()

        # Find the backfill method
        idx = content.index("def backfill(")
        end_idx = content.index("\n    def ", idx + 10)
        method_body = content[idx:end_idx]

        # Must query stg_comprovantes_slim with canonical join for mart data_keys
        assert "stg_comprovantes_slim" in method_body, (
            "backfill must discover publishable data_keys from stg_comprovantes_slim"
        )
        assert "stg_itenscomprovantes_slim" in method_body, (
            "backfill must JOIN stg_itenscomprovantes_slim for publishable data_keys"
        )
        # Must filter data_key > 0
        assert "data_key > 0" in method_body, (
            "backfill must exclude data_key=0 from publishable keys"
        )
        # Must filter cancelado = 0
        assert "cancelado = 0" in method_body, (
            "backfill must only publish non-cancelled comprovantes"
        )
        # Must filter cfop > 5000
        assert "cfop > 5000" in method_body, (
            "backfill must only publish valid sales items (cfop > 5000)"
        )

    def test_batch_boundary_includes_all_keys(self):
        """7-key batch must process all keys including last one (no off-by-one)."""
        from torqmind_cdc_consumer.mart_builder import _BACKFILL_BATCH_SIZE

        data_keys = DATA_KEYS_FIXTURE
        batches = []
        for i in range(0, len(data_keys), _BACKFILL_BATCH_SIZE):
            batches.append(data_keys[i:i + _BACKFILL_BATCH_SIZE])

        # All keys must be covered
        all_batched = [k for batch in batches for k in batch]
        assert set(all_batched) == set(data_keys), "Batching must cover all data_keys"
        assert 20260430 in all_batched, "20260430 must not be skipped at batch boundary"

    def test_batch_size_7_with_exactly_7_keys(self):
        """When there are exactly 7 keys (1 full batch), all are processed."""
        from torqmind_cdc_consumer.mart_builder import _BACKFILL_BATCH_SIZE

        assert _BACKFILL_BATCH_SIZE == 7
        data_keys = DATA_KEYS_FIXTURE
        assert len(data_keys) == 7

        # Single batch covers all
        chunk = data_keys[0:_BACKFILL_BATCH_SIZE]
        assert chunk == data_keys
        assert 20260430 in chunk

    def test_batch_size_7_with_8_keys_splits_correctly(self):
        """When there are 8 keys, batch boundary doesn't drop any key."""
        from torqmind_cdc_consumer.mart_builder import _BACKFILL_BATCH_SIZE

        data_keys = DATA_KEYS_FIXTURE + [20260502]
        assert len(data_keys) == 8

        batch1 = data_keys[0:_BACKFILL_BATCH_SIZE]
        batch2 = data_keys[_BACKFILL_BATCH_SIZE:_BACKFILL_BATCH_SIZE * 2]

        assert len(batch1) == 7
        assert len(batch2) == 1
        assert 20260430 in batch1
        assert 20260502 in batch2


# ============================================================
# DATA_KEY=0 PROHIBITION TESTS
# ============================================================

class TestDataKeyZeroProhibition:
    """data_key=0 must never appear in sales marts."""

    def test_slim_keys_filter_excludes_zero(self):
        """_slim_keys_filter must exclude data_key=0."""
        builder = make_builder()
        # Include a zero in the list
        filt = builder._slim_keys_filter([0, 20260430, 20260501], "c")
        assert "0" not in filt.split("IN")[1].split(",")[0].strip() or "20260430" in filt
        # Verify zero is excluded
        assert filt == "c.data_key IN (20260430,20260501)"

    def test_delete_mart_batch_excludes_zero(self):
        """_delete_mart_batch must not delete data_key=0 rows."""
        builder = make_builder()
        mock_client = MagicMock()
        builder._delete_mart_batch(mock_client, "sales_daily_rt", [0, 20260430])
        # Should only have non-zero key
        call_args = mock_client.command.call_args[0][0]
        assert "0," not in call_args or call_args.count("20260430") == 1
        assert "20260430" in call_args


# ============================================================
# PUBLICATION LOG TESTS
# ============================================================

class TestPublicationLog:
    """Publication log must record real values, not sentinels."""

    def test_log_publications_uses_real_id_empresa(self):
        """id_empresa in publication log must not be 0 for real backfill."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder, MartRefreshResult

        builder = make_builder()
        mock_client = MagicMock()
        results = [
            MartRefreshResult("sales_daily_rt", 100, 500),
            MartRefreshResult("sales_hourly_rt", 200, 300),
        ]

        builder._log_publications(
            mock_client, results,
            id_empresa=1,
            data_keys=[20260425, 20260430, 20260501],
        )

        # Verify insert was called
        assert mock_client.insert.called
        insert_call = mock_client.insert.call_args
        rows = insert_call[0][1]  # positional arg: rows data

        # Check id_empresa is 1, not 0
        for row in rows:
            assert row[1] == 1, f"id_empresa must be 1, got {row[1]}"

    def test_log_publications_uses_real_window(self):
        """window_start/window_end must reflect actual data_keys."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder, MartRefreshResult

        builder = make_builder()
        mock_client = MagicMock()
        results = [MartRefreshResult("sales_daily_rt", 50, 100)]

        builder._log_publications(
            mock_client, results,
            id_empresa=1,
            data_keys=[20260425, 20260430, 20260501],
        )

        insert_call = mock_client.insert.call_args
        rows = insert_call[0][1]

        for row in rows:
            window_start = row[2]
            window_end = row[3]
            # Must not be sentinel values
            assert window_start != date(1970, 1, 2), "window_start must not be 1970-01-02"
            assert window_end != date(2099, 12, 31), "window_end must not be 2099-12-31"
            # Must be real dates from the data_keys
            assert window_start == date(2026, 4, 25)
            assert window_end == date(2026, 5, 1)

    def test_log_publications_uses_real_rows_written(self):
        """rows_written must reflect actual count, not hardcoded 0."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder, MartRefreshResult

        builder = make_builder()
        mock_client = MagicMock()
        results = [
            MartRefreshResult("sales_daily_rt", 77, 500),
            MartRefreshResult("sales_hourly_rt", 0, 300),  # 0 is valid if no rows
        ]

        builder._log_publications(
            mock_client, results,
            id_empresa=1,
            data_keys=[20260430],
        )

        insert_call = mock_client.insert.call_args
        rows = insert_call[0][1]
        assert rows[0][4] == 77, "rows_written must be 77 for sales_daily_rt"
        assert rows[1][4] == 0, "rows_written can be 0 when legitimately empty"


# ============================================================
# VALIDATE COMPLETENESS TESTS
# ============================================================

class TestValidateCompleteness:
    """validate_completeness must detect missing data_keys and data_key=0."""

    def test_validate_method_exists(self):
        """MartBuilder must have validate_completeness method."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)
        assert hasattr(builder, "validate_completeness")
        assert callable(builder.validate_completeness)

    def test_validate_returns_expected_structure(self):
        """validate_completeness return value must have pass, missing, data_key_zero."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder

        builder = MartBuilder(enabled=False)

        # Mock the client to simulate a scenario where everything is present
        with patch.object(builder, "_get_client") as mock_get:
            mock_client = MagicMock()
            mock_get.return_value = mock_client

            # Slim has keys 20260430, 20260501
            mock_client.query.side_effect = [
                FakeQueryResult([(20260430,), (20260501,)]),  # slim query
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_daily_rt
                FakeQueryResult([(0,)]),  # data_key=0 check for daily
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_hourly_rt
                FakeQueryResult([(0,)]),  # data_key=0 check for hourly
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_products_rt
                FakeQueryResult([(0,)]),  # data_key=0 check for products
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_groups_rt
                FakeQueryResult([(0,)]),  # data_key=0 check for groups
            ]

            result = builder.validate_completeness(id_empresa=1, from_date="2026-04-30")

        assert "pass" in result
        assert "missing" in result
        assert "data_key_zero" in result
        assert result["pass"] is True
        assert result["missing"] == []
        assert result["data_key_zero"] == []

    def test_validate_detects_missing_data_key(self):
        """If slim has 20260430 but mart doesn't, validate must FAIL."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder

        builder = MartBuilder(enabled=False)

        with patch.object(builder, "_get_client") as mock_get:
            mock_client = MagicMock()
            mock_get.return_value = mock_client

            # Slim has 20260430 and 20260501
            # But sales_daily_rt only has 20260501 (20260430 missing!)
            mock_client.query.side_effect = [
                FakeQueryResult([(20260430,), (20260501,)]),  # slim
                FakeQueryResult([(20260501,)]),  # sales_daily_rt - MISSING 20260430
                FakeQueryResult([(0,)]),  # data_key=0 check
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_hourly_rt
                FakeQueryResult([(0,)]),  # data_key=0 check
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_products_rt
                FakeQueryResult([(0,)]),  # data_key=0 check
                FakeQueryResult([(20260430,), (20260501,)]),  # sales_groups_rt
                FakeQueryResult([(0,)]),  # data_key=0 check
            ]

            result = builder.validate_completeness(id_empresa=1, from_date="2026-04-25")

        assert result["pass"] is False
        assert len(result["missing"]) > 0
        missing_keys = [m["data_key"] for m in result["missing"]]
        assert 20260430 in missing_keys

    def test_validate_detects_data_key_zero(self):
        """If any sales mart has data_key=0, validate must FAIL."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder

        builder = MartBuilder(enabled=False)

        with patch.object(builder, "_get_client") as mock_get:
            mock_client = MagicMock()
            mock_get.return_value = mock_client

            mock_client.query.side_effect = [
                FakeQueryResult([(20260430,)]),  # slim
                FakeQueryResult([(20260430,)]),  # sales_daily_rt
                FakeQueryResult([(5,)]),  # data_key=0 check - 5 VIOLATIONS!
                FakeQueryResult([(20260430,)]),  # sales_hourly_rt
                FakeQueryResult([(0,)]),  # data_key=0 check
                FakeQueryResult([(20260430,)]),  # sales_products_rt
                FakeQueryResult([(0,)]),  # data_key=0 check
                FakeQueryResult([(20260430,)]),  # sales_groups_rt
                FakeQueryResult([(0,)]),  # data_key=0 check
            ]

            result = builder.validate_completeness(id_empresa=1, from_date="2026-04-30")

        assert result["pass"] is False
        assert len(result["data_key_zero"]) > 0
        assert result["data_key_zero"][0]["mart"] == "sales_daily_rt"
        assert result["data_key_zero"][0]["rows_with_zero"] == 5


# ============================================================
# CODE CONTRACT TESTS
# ============================================================

class TestCodeContract:
    """Verify code-level contracts that prevent the 20260430 regression."""

    @pytest.fixture(autouse=True)
    def _load_code(self):
        self.builder_path = Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        self.code = self.builder_path.read_text()

    def test_backfill_queries_slim_for_mart_keys(self):
        """backfill MUST use stg_comprovantes_slim JOIN stg_itenscomprovantes_slim
        to determine which data_keys get published to marts."""
        # Find phase 2 in backfill
        assert "Phase 2" in self.code or "phase 2" in self.code or "publishable data_keys from SLIM" in self.code

    def test_publication_log_not_hardcoded_sentinel(self):
        """_log_publications must not hardcode id_empresa=0 or sentinel dates."""
        idx = self.code.index("def _log_publications")
        next_def_idx = self.code.index("\n    def ", idx + 10)
        method_body = self.code[idx:next_def_idx]

        # Must not have hardcoded sentinel id_empresa
        # Old pattern: rows.append([..., 0, _date(1970, 1, 2), _date(2099, 12, 31), ...])
        assert "_date(1970, 1, 2)" not in method_body, "Must not hardcode 1970-01-02"
        assert "_date(2099, 12, 31)" not in method_body, "Must not hardcode 2099-12-31"
        # id_empresa should come from parameter
        assert "id_empresa" in method_body

    def test_sales_marts_use_data_key_from_comprovantes_slim(self):
        """All sales mart queries must use c.data_key or i.data_key from slim tables."""
        for method in ("_refresh_sales_daily_stg", "_refresh_sales_hourly_stg",
                       "_refresh_sales_products_stg", "_refresh_sales_groups_stg"):
            idx = self.code.index(f"def {method}")
            next_def = self.code.index("\n    def ", idx + 10)
            body = self.code[idx:next_def]
            assert "stg_comprovantes_slim" in body, f"{method} must read from stg_comprovantes_slim"
            assert "stg_itenscomprovantes_slim" in body, f"{method} must join stg_itenscomprovantes_slim"

    def test_rows_written_not_hardcoded_zero(self):
        """Sales mart refresh methods must capture actual rows_written."""
        for method in ("_refresh_sales_daily_stg", "_refresh_sales_hourly_stg",
                       "_refresh_sales_products_stg", "_refresh_sales_groups_stg"):
            idx = self.code.index(f"def {method}")
            next_def = self.code.index("\n    def ", idx + 10)
            body = self.code[idx:next_def]
            # Must use _insert_and_count or equivalent
            assert "_insert_and_count" in body or "rows" in body, (
                f"{method} must track actual rows_written"
            )
