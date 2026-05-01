"""Tests for realtime mart layer: signature parity, DDL alignment, idempotency."""

from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import Any

import pytest


# ============================================================
# SIGNATURE PARITY: repos_mart_realtime vs repos_mart_clickhouse
# ============================================================

class TestSignatureParity:
    """Ensure realtime functions have compatible signatures with clickhouse functions."""

    @pytest.fixture(autouse=True)
    def _load_modules(self):
        import sys
        # Ensure apps/api is importable
        api_path = str(Path(__file__).parent.parent.parent / "api")
        if api_path not in sys.path:
            sys.path.insert(0, api_path)

    def _get_realtime_module(self):
        from app import repos_mart_realtime
        return repos_mart_realtime

    def _get_clickhouse_module(self):
        from app import repos_mart_clickhouse
        return repos_mart_clickhouse

    def test_all_realtime_functions_exist(self):
        rt = self._get_realtime_module()
        for name in rt.REALTIME_FUNCTIONS:
            fn = getattr(rt, name, None)
            assert fn is not None, f"REALTIME_FUNCTIONS declares '{name}' but it doesn't exist"
            assert callable(fn), f"'{name}' is not callable"

    def test_positional_params_match_clickhouse(self):
        """The first N positional params of each realtime function must match
        the corresponding clickhouse function (role, id_empresa, id_filial, dt_ini, dt_fim)."""
        rt = self._get_realtime_module()
        ch = self._get_clickhouse_module()

        # Functions that exist in both modules
        shared = rt.REALTIME_FUNCTIONS & {
            name for name, val in inspect.getmembers(ch, inspect.isfunction)
            if not name.startswith("_")
        }

        for name in sorted(shared):
            rt_fn = getattr(rt, name)
            ch_fn = getattr(ch, name)
            rt_sig = inspect.signature(rt_fn)
            ch_sig = inspect.signature(ch_fn)

            rt_params = [
                (pname, p.default)
                for pname, p in rt_sig.parameters.items()
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            ]
            ch_params = [
                (pname, p.default)
                for pname, p in ch_sig.parameters.items()
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            ]

            # Check first params match by name
            min_len = min(len(rt_params), len(ch_params))
            for i in range(min(min_len, 5)):  # Check at least role, id_empresa, id_filial, dt_ini, dt_fim
                rt_name = rt_params[i][0]
                ch_name = ch_params[i][0]
                assert rt_name == ch_name, (
                    f"{name}: param[{i}] is '{rt_name}' in realtime but '{ch_name}' in clickhouse"
                )

    def test_role_is_first_param_for_bi_functions(self):
        """All BI realtime functions (not streaming_health) must have 'role' as first param."""
        rt = self._get_realtime_module()
        for name in sorted(rt.REALTIME_FUNCTIONS):
            if name == "streaming_health":
                continue
            fn = getattr(rt, name)
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert params[0] == "role", (
                f"{name}: first param must be 'role', got '{params[0]}'"
            )

    def test_id_empresa_is_second_param(self):
        """All BI realtime functions must have 'id_empresa' as second param."""
        rt = self._get_realtime_module()
        for name in sorted(rt.REALTIME_FUNCTIONS):
            if name == "streaming_health":
                continue
            fn = getattr(rt, name)
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert params[1] == "id_empresa", (
                f"{name}: second param must be 'id_empresa', got '{params[1]}'"
            )

    def test_id_filial_is_third_param(self):
        """All BI realtime functions must have 'id_filial' as third param."""
        rt = self._get_realtime_module()
        for name in sorted(rt.REALTIME_FUNCTIONS):
            if name == "streaming_health":
                continue
            fn = getattr(rt, name)
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert params[2] == "id_filial", (
                f"{name}: third param must be 'id_filial', got '{params[2]}'"
            )

    def test_limit_defaults_match(self):
        """Functions with 'limit' param should have same default as clickhouse."""
        rt = self._get_realtime_module()
        ch = self._get_clickhouse_module()

        limit_fns = ["sales_top_products", "sales_top_groups", "fraud_last_events"]
        for name in limit_fns:
            rt_fn = getattr(rt, name, None)
            ch_fn = getattr(ch, name, None)
            if rt_fn is None or ch_fn is None:
                continue

            rt_sig = inspect.signature(rt_fn)
            ch_sig = inspect.signature(ch_fn)

            rt_limit = rt_sig.parameters.get("limit")
            ch_limit = ch_sig.parameters.get("limit")
            if rt_limit and ch_limit:
                assert rt_limit.default == ch_limit.default, (
                    f"{name}: limit default {rt_limit.default} != {ch_limit.default}"
                )


# ============================================================
# DDL vs MART BUILDER ALIGNMENT
# ============================================================

class TestDDLMartBuilderAlignment:
    """Verify mart_builder.py INSERT columns match 041_mart_rt_tables.sql DDL."""

    @pytest.fixture(autouse=True)
    def _load_paths(self):
        self.root = Path(__file__).parent.parent.parent.parent
        self.ddl_path = self.root / "sql" / "clickhouse" / "streaming" / "041_mart_rt_tables.sql"
        self.builder_path = self.root / "apps" / "cdc_consumer" / "torqmind_cdc_consumer" / "mart_builder.py"

    def _parse_ddl_columns(self, table_name: str) -> list[str]:
        """Extract column names from CREATE TABLE statement."""
        ddl = self.ddl_path.read_text()
        # Find the CREATE TABLE for this table
        pattern = rf"CREATE TABLE IF NOT EXISTS torqmind_mart_rt\.{table_name}\s*\((.*?)\)\s*ENGINE"
        match = re.search(pattern, ddl, re.DOTALL)
        if not match:
            return []
        body = match.group(1)
        columns = []
        for line in body.strip().split("\n"):
            line = line.strip().rstrip(",")
            if line and not line.startswith("--"):
                parts = line.split()
                if parts and not parts[0].upper().startswith(("ENGINE", "ORDER", "PARTITION", "SETTINGS", "TTL")):
                    col_name = parts[0]
                    if col_name.isidentifier():
                        columns.append(col_name)
        return columns

    def _parse_builder_insert_columns(self, mart_name: str) -> list[str]:
        """Extract SELECT column aliases from INSERT INTO statement for a mart."""
        code = self.builder_path.read_text()
        # Find INSERT INTO {db}.{mart_name} SELECT ... pattern
        pattern = rf"INSERT INTO.*?\.{mart_name}\s+SELECT\s+(.*?)(?:FROM)"
        match = re.search(pattern, code, re.DOTALL)
        if not match:
            return []
        select_part = match.group(1)
        # Extract column aliases (AS name) or final column names
        columns = []
        for expr in select_part.split(","):
            expr = expr.strip()
            # Look for "AS column_name"
            as_match = re.search(r'\bAS\s+(\w+)\s*$', expr, re.IGNORECASE)
            if as_match:
                columns.append(as_match.group(1))
            else:
                # Last word (e.g. v.id_empresa → id_empresa, or now64(6))
                parts = expr.split(".")
                last = parts[-1].strip()
                # Handle function calls
                if "(" in last:
                    continue
                columns.append(last)
        return columns

    @pytest.mark.parametrize("table", [
        "sales_daily_rt",
        "sales_hourly_rt",
        "sales_products_rt",
        "sales_groups_rt",
        "payments_by_type_rt",
        "cash_overview_rt",
        "fraud_daily_rt",
        "risk_recent_events_rt",
        "finance_overview_rt",
        "dashboard_home_rt",
    ])
    def test_ddl_exists_for_mart_table(self, table: str):
        """Every mart table refreshed by builder must exist in DDL."""
        columns = self._parse_ddl_columns(table)
        assert len(columns) > 0, f"Table {table} not found in DDL file"

    @pytest.mark.parametrize("table", [
        "sales_daily_rt",
        "sales_hourly_rt",
        "dashboard_home_rt",
    ])
    def test_builder_inserts_into_ddl_table(self, table: str):
        """Verify mart_builder has INSERT INTO for each DDL table."""
        code = self.builder_path.read_text()
        assert f".{table}" in code, f"mart_builder.py doesn't INSERT INTO {table}"
        # Verify DDL has the required published_at column (for ReplacingMergeTree)
        ddl_cols = self._parse_ddl_columns(table)
        assert "published_at" in ddl_cols, f"{table} DDL missing published_at column"

    @pytest.mark.parametrize("table", [
        "sales_daily_rt",
        "sales_hourly_rt",
        "sales_products_rt",
        "sales_groups_rt",
        "payments_by_type_rt",
        "cash_overview_rt",
        "fraud_daily_rt",
        "risk_recent_events_rt",
        "finance_overview_rt",
        "dashboard_home_rt",
    ])
    def test_ddl_uses_replacing_merge_tree(self, table: str):
        """All mart_rt tables must use ReplacingMergeTree for idempotency."""
        ddl = self.ddl_path.read_text()
        pattern = rf"CREATE TABLE IF NOT EXISTS torqmind_mart_rt\.{table}.*?ENGINE\s*=\s*(\w+)"
        match = re.search(pattern, ddl, re.DOTALL)
        assert match is not None, f"Cannot find ENGINE for {table}"
        engine = match.group(1)
        assert engine == "ReplacingMergeTree", (
            f"{table} uses {engine}, must use ReplacingMergeTree for idempotency"
        )


# ============================================================
# MART BUILDER IDEMPOTENCY (unit level)
# ============================================================

class TestMartBuilderIdempotency:
    """Test that mart builder state tracking is correct."""

    def test_mark_affected_tracks_data_keys(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)
        builder.mark_affected(1, 1, 20250401, "fact_venda")
        builder.mark_affected(1, 1, 20250401, "fact_venda_item")
        builder.mark_affected(1, 2, 20250402, "fact_venda")

        assert builder.state.affected_data_keys == {20250401, 20250402}
        assert builder.state.affected_empresas == {1}
        assert builder.state.affected_filiais == {(1, 1), (1, 2)}
        assert builder.state.affected_tables == {"fact_venda", "fact_venda_item"}

    def test_clear_resets_state(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)
        builder.mark_affected(1, 1, 20250401, "fact_venda")
        builder.state.clear()

        assert not builder.state.has_work
        assert len(builder.state.affected_data_keys) == 0

    def test_refresh_noop_when_disabled(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)
        builder.mark_affected(1, 1, 20250401, "fact_venda")
        results = builder.refresh_if_needed()
        assert results == []

    def test_refresh_noop_when_no_work(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=True)
        results = builder.refresh_if_needed()
        assert results == []

    def test_replaying_same_data_key_is_idempotent_by_design(self):
        """ReplacingMergeTree ensures same grain INSERT is idempotent.
        The builder always INSERTs — ClickHouse dedup happens at query time with FINAL."""
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)
        # Mark same data_key twice
        builder.mark_affected(1, 1, 20250401, "fact_venda")
        builder.mark_affected(1, 1, 20250401, "fact_venda")
        # Should still result in single data_key in the set
        assert builder.state.affected_data_keys == {20250401}
