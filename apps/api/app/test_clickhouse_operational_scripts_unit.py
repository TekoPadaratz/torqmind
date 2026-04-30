from __future__ import annotations

import unittest
from pathlib import Path


def repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "deploy" / "scripts").is_dir():
            return parent
    raise unittest.SkipTest("repository deploy scripts are not available in this runtime")


def read(path: str) -> str:
    return (repo_root() / path).read_text(encoding="utf-8")


class ClickHouseOperationalScriptsTest(unittest.TestCase):
    def test_sync_dw_supports_full_and_incremental_without_incremental_drop(self) -> None:
        source = read("deploy/scripts/prod-clickhouse-sync-dw.sh")
        self.assertIn('MODE="${MODE:-full}"', source)
        self.assertIn('run_full_sync()', source)
        self.assertIn('run_incremental_sync()', source)
        self.assertIn("dim_forma_pagamento", source)
        self.assertIn("pg_app_table_fn payment_type_map", source)
        incremental_body = source.split("run_incremental_sync()", 1)[1]
        self.assertIn('load_incremental_table "$table"', incremental_body)
        self.assertIn("reload_payment_type_map", incremental_body)
        self.assertIn("ALTER TABLE torqmind_dw.${table} DELETE", source)
        self.assertNotIn("DROP DATABASE IF EXISTS torqmind_dw", incremental_body)
        self.assertNotIn("CREATE DATABASE torqmind_dw", incremental_body)
        self.assertIn("redact_secrets", source)

    def test_refresh_marts_incremental_is_windowed_and_records_publication(self) -> None:
        source = read("deploy/scripts/prod-clickhouse-refresh-marts.sh")
        self.assertIn('MODE="${MODE:-incremental}"', source)
        self.assertIn("run_full_refresh()", source)
        self.assertIn("run_incremental_refresh()", source)
        incremental_body = source.split("run_incremental_refresh()", 1)[1]
        self.assertIn('delete_data_key_window "$table"', incremental_body)
        self.assertIn("DELETE WHERE data_key BETWEEN", source)
        self.assertIn("mart_publication", incremental_body)
        self.assertIn("ensure_semantic_columns", incremental_body)
        self.assertIn("dim_forma_pagamento", source)
        self.assertNotIn("DROP DATABASE IF EXISTS torqmind_mart", incremental_body)

    def test_payment_marts_use_real_payment_type_map_labels(self) -> None:
        for path in (
            "sql/clickhouse/phase3_native_backfill.sql",
            "sql/clickhouse/phase2_mvs_streaming_triggers.sql",
            "deploy/scripts/prod-clickhouse-refresh-marts.sh",
        ):
            source = read(path)
            self.assertIn("dim_forma_pagamento", source, path)
            self.assertNotIn("concat('FORMA_', toString(p.tipo_forma))", source, path)

    def test_semantic_marts_audit_checks_human_labels(self) -> None:
        source = read("deploy/scripts/prod-semantic-marts-audit.sh")
        self.assertIn("startsWith(label, 'FORMA_')", source)
        self.assertIn("fraude_cancelamentos_eventos", source)
        self.assertIn("risco_eventos_recentes", source)
        self.assertIn("system.columns", source)
        self.assertIn("id_filial", source)
        self.assertIn("finance_aging_daily", source)
        self.assertIn("app.competitor_fuel_prices", source)

    def test_risk_recent_events_view_contract_keeps_filial_and_updated_at(self) -> None:
        design_source = read("sql/clickhouse/phase2_mvs_design.sql")
        refresh_source = read("deploy/scripts/prod-clickhouse-refresh-marts.sh")
        for source in (design_source, refresh_source):
            self.assertIn("CREATE OR REPLACE VIEW", source)
            self.assertIn("risco_eventos_recentes", source)
            self.assertIn("r.id_filial", source)
            self.assertIn("AS filial_nome", source)
            self.assertIn("event_type", source)
            self.assertIn("operador_caixa_nome", source)
            self.assertIn("r.created_at AS updated_at", source)

    def test_pipeline_runs_incremental_clickhouse_and_never_full_refresh(self) -> None:
        source = read("deploy/scripts/prod-etl-pipeline.sh")
        self.assertIn("flock -n 9", source)
        self.assertIn("PIPELINE_TIMEOUT_SECONDS", source)
        self.assertIn("MODE=incremental", source)
        self.assertIn("prod-clickhouse-sync-dw.sh", source)
        self.assertIn("prod-clickhouse-refresh-marts.sh", source)
        self.assertIn("mktemp", source)
        self.assertIn("PIPELINE_TRACK_LOG_MAX_BYTES", source)
        self.assertNotIn("MODE=full", source)
        self.assertNotIn("prod-clickhouse-init.sh", source)

        incremental_source = read("deploy/scripts/prod-etl-incremental.sh")
        self.assertIn("sys.stdin.read()", incremental_source)
        self.assertNotIn('"$TRACK" "$error_code" "$message"', incremental_source)

    def test_cron_defaults_to_two_minutes_and_risk_interval_is_configurable(self) -> None:
        source = read("deploy/scripts/prod-install-cron.sh")
        self.assertIn('OPERATIONAL_INTERVAL_MINUTES="${OPERATIONAL_INTERVAL_MINUTES:-2}"', source)
        self.assertIn('RISK_INTERVAL_MINUTES="${RISK_INTERVAL_MINUTES:-30}"', source)
        self.assertIn("*/$OPERATIONAL_INTERVAL_MINUTES", source)

    def test_reconcile_keeps_orphans_warn_only(self) -> None:
        source = read("deploy/scripts/prod-data-reconcile.sh")
        self.assertIn("orphan item(s); this is data quality debt", source)
        self.assertIn("warn \"PostgreSQL dw.fact_venda_item has", source)
        self.assertNotIn("error \"PostgreSQL dw.fact_venda_item has", source)


if __name__ == "__main__":
    unittest.main()
