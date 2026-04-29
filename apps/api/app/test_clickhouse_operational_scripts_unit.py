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
        incremental_body = source.split("run_incremental_sync()", 1)[1]
        self.assertIn('load_incremental_table "$table"', incremental_body)
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
        self.assertNotIn("DROP DATABASE IF EXISTS torqmind_mart", incremental_body)

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
