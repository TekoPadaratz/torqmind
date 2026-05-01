"""Tests for realtime mart layer: signature parity, DDL alignment, idempotency."""

from __future__ import annotations

import inspect
import json
import re
from datetime import date
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

    def test_facade_routes_to_realtime_without_argument_shift(self, monkeypatch: pytest.MonkeyPatch):
        """repos_analytics must pass role/id_empresa/id_filial through unchanged."""
        from app import repos_analytics
        from app import repos_mart_realtime

        repos_analytics._DISPATCH_CACHE.clear()
        monkeypatch.setattr(repos_analytics.settings, "use_realtime_marts", True)
        monkeypatch.setattr(repos_analytics.settings, "realtime_marts_fallback", False)

        def fake_dashboard_kpis(
            role: str,
            id_empresa: int,
            id_filial: Any,
            dt_ini: date,
            dt_fim: date,
            **kwargs: Any,
        ) -> dict[str, Any]:
            return {
                "role": role,
                "id_empresa": id_empresa,
                "id_filial": id_filial,
                "dt_ini": dt_ini,
                "dt_fim": dt_fim,
                "kwargs": kwargs,
            }

        monkeypatch.setattr(repos_mart_realtime, "dashboard_kpis", fake_dashboard_kpis)

        result = getattr(repos_analytics, "dashboard_kpis")(
            "admin",
            123,
            456,
            date(2026, 4, 1),
            date(2026, 4, 30),
            probe=True,
        )

        assert result["role"] == "admin"
        assert result["id_empresa"] == 123
        assert result["id_filial"] == 456
        assert result["role"] != result["id_empresa"]
        assert result["kwargs"] == {"probe": True}


# ============================================================
# DDL vs MART BUILDER ALIGNMENT
# ============================================================

class TestDDLMartBuilderAlignment:
    """Verify mart_builder.py INSERT columns match 041_mart_rt_tables.sql DDL."""

    @pytest.fixture(autouse=True)
    def _load_paths(self):
        self.root = Path(__file__).parent.parent.parent.parent
        self.ddl_db_path = self.root / "sql" / "clickhouse" / "streaming" / "040_mart_rt_database.sql"
        self.ddl_path = self.root / "sql" / "clickhouse" / "streaming" / "041_mart_rt_tables.sql"
        self.builder_path = self.root / "apps" / "cdc_consumer" / "torqmind_cdc_consumer" / "mart_builder.py"
        self.realtime_repo_path = self.root / "apps" / "api" / "app" / "repos_mart_realtime.py"

    def _ddl_text(self) -> str:
        return self.ddl_db_path.read_text() + "\n" + self.ddl_path.read_text()

    def _split_top_level_commas(self, text: str) -> list[str]:
        parts: list[str] = []
        start = 0
        depth = 0
        quote: str | None = None
        for i, ch in enumerate(text):
            if quote:
                if ch == quote:
                    quote = None
                continue
            if ch in {"'", '"'}:
                quote = ch
                continue
            if ch == "(":
                depth += 1
                continue
            if ch == ")":
                depth -= 1
                continue
            if ch == "," and depth == 0:
                parts.append(text[start:i].strip())
                start = i + 1
        tail = text[start:].strip()
        if tail:
            parts.append(tail)
        return parts

    def _parse_ddl_columns(self, table_name: str) -> list[str]:
        """Extract column names from CREATE TABLE statement."""
        ddl = self._ddl_text()
        # Find the CREATE TABLE for this table
        pattern = rf"CREATE TABLE IF NOT EXISTS torqmind_mart_rt\.{table_name}\s*\((.*?)\)\s*ENGINE"
        match = re.search(pattern, ddl, re.DOTALL)
        if not match:
            return []
        body = match.group(1)
        columns = []
        for expr in self._split_top_level_commas(body):
            line = expr.strip()
            if not line or line.startswith("--"):
                continue
            col_name = line.split()[0]
            if col_name.isidentifier():
                columns.append(col_name)
        return columns

    def _select_part_for_insert(self, mart_name: str) -> str:
        """Extract SELECT column aliases from INSERT INTO statement for a mart."""
        code = self.builder_path.read_text()
        pattern = rf"INSERT INTO\s+\{{self\.mart_rt_db\}}\.{mart_name}\s+SELECT\s+"
        match = re.search(pattern, code, re.DOTALL)
        if not match:
            return ""
        start = match.end()
        depth = 0
        quote: str | None = None
        i = start
        while i < len(code):
            ch = code[i]
            if quote:
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in {"'", '"'}:
                quote = ch
                i += 1
                continue
            if ch == "(":
                depth += 1
                i += 1
                continue
            if ch == ")":
                depth -= 1
                i += 1
                continue
            if depth == 0 and code[i : i + 4].upper() == "FROM":
                before = code[i - 1] if i > 0 else " "
                after = code[i + 4] if i + 4 < len(code) else " "
                if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
                    return code[start:i].strip()
            i += 1
        return ""

    def _parse_builder_insert_columns(self, mart_name: str) -> list[str]:
        """Extract SELECT output column names from an INSERT INTO ... SELECT."""
        select_part = self._select_part_for_insert(mart_name)
        columns = []
        for expr in self._split_top_level_commas(select_part):
            expr = expr.strip()
            # Look for "AS column_name"
            as_match = re.search(r'\bAS\s+(\w+)\s*$', expr, re.IGNORECASE)
            if as_match:
                columns.append(as_match.group(1))
            else:
                qualified = re.search(r"\b\w+\.(\w+)\s*$", expr)
                if qualified:
                    columns.append(qualified.group(1))
                    continue
                if re.match(r"^\w+$", expr):
                    columns.append(expr)
        return columns

    def _builder_insert_tables(self) -> set[str]:
        code = self.builder_path.read_text()
        return set(re.findall(r"\{self\.mart_rt_db\}\.([A-Za-z0-9_]+)", code))

    def _realtime_repo_tables(self) -> set[str]:
        code = self.realtime_repo_path.read_text()
        return set(re.findall(r"\{MART_RT_DB\}\.([A-Za-z0-9_]+)", code))

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

    def test_all_required_cutover_tables_exist_in_ddl(self):
        """The operational cutover table contract must exist in tracked DDL."""
        required = {
            "dashboard_home_rt",
            "sales_daily_rt",
            "sales_hourly_rt",
            "sales_products_rt",
            "sales_groups_rt",
            "payments_by_type_rt",
            "cash_overview_rt",
            "fraud_daily_rt",
            "risk_recent_events_rt",
            "finance_overview_rt",
            "source_freshness",
            "mart_publication_log",
        }
        for table in sorted(required):
            assert self._parse_ddl_columns(table), f"Required table {table} missing from DDL"

    def test_builder_tables_exist_in_ddl(self):
        """Every table written by MartBuilder must exist in mart_rt DDL."""
        for table in sorted(self._builder_insert_tables()):
            assert self._parse_ddl_columns(table), f"MartBuilder writes {table}, but DDL does not create it"

    def test_realtime_repo_tables_exist_in_ddl(self):
        """Every mart_rt table read by repos_mart_realtime.py must exist in DDL."""
        for table in sorted(self._realtime_repo_tables()):
            assert self._parse_ddl_columns(table), f"repos_mart_realtime reads {table}, but DDL does not create it"

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
        "source_freshness",
    ])
    def test_builder_insert_columns_match_ddl_order(self, table: str):
        """INSERT ... SELECT without a column list must match DDL columns by position."""
        ddl_cols = self._parse_ddl_columns(table)
        insert_cols = self._parse_builder_insert_columns(table)
        assert insert_cols == ddl_cols, (
            f"{table}: MartBuilder INSERT columns must match DDL order\n"
            f"insert={insert_cols}\n"
            f"ddl={ddl_cols}"
        )

    def test_publication_log_insert_columns_exist(self):
        """client.insert(column_names=...) into mart_publication_log must reference DDL columns."""
        code = self.builder_path.read_text()
        match = re.search(r"column_names=\[(.*?)\]", code, re.DOTALL)
        assert match is not None, "MartBuilder publication log insert must specify column_names"
        inserted = re.findall(r'"([A-Za-z0-9_]+)"', match.group(1))
        ddl_cols = set(self._parse_ddl_columns("mart_publication_log"))
        assert inserted
        assert set(inserted).issubset(ddl_cols)

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
        ddl = self._ddl_text()
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

    def test_default_source_is_stg(self):
        from torqmind_cdc_consumer.mart_builder import MartBuilder
        builder = MartBuilder(enabled=False)
        assert builder.source == "stg"


class TestSTGDirectContract:
    """Release contract for the STG-direct realtime path."""

    @pytest.fixture(autouse=True)
    def _load_paths(self):
        self.root = Path(__file__).parent.parent.parent.parent
        self.connector_path = self.root / "deploy" / "debezium" / "connectors" / "torqmind-postgres-cdc.json"
        self.builder_path = self.root / "apps" / "cdc_consumer" / "torqmind_cdc_consumer" / "mart_builder.py"

    def test_connector_includes_canonical_stg_tables(self):
        config = json.loads(self.connector_path.read_text())["config"]
        include_list = set(config["table.include.list"].split(","))
        required = {
            "stg.comprovantes",
            "stg.itenscomprovantes",
            "stg.formas_pgto_comprovantes",
            "stg.turnos",
            "stg.produtos",
            "stg.grupoprodutos",
            "stg.funcionarios",
            "stg.usuarios",
            "stg.localvendas",
            "stg.contaspagar",
            "stg.contasreceber",
            "app.payment_type_map",
        }
        assert required.issubset(include_list)

    def test_mart_builder_has_stg_direct_refreshes(self):
        code = self.builder_path.read_text()
        for table in ("stg_comprovantes", "stg_itenscomprovantes", "stg_formas_pgto_comprovantes"):
            assert f"{'{'}self.current_db{'}'}.{table}" in code
        assert "fact_venda FINAL" in code  # DW compatibility remains present.
        assert "self.source == \"stg\"" in code
