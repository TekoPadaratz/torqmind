from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("USE_CLICKHOUSE", "false")
os.environ.setdefault("DUAL_READ_MODE", "false")
os.environ.setdefault("USE_REALTIME_MARTS", "false")
os.environ.setdefault("REALTIME_MARTS_FALLBACK", "false")


DB_INTEGRATION_FILES = {
    "test_platform_backoffice.py",
    "test_release_hardening.py",
    "test_sales_retention.py",
    "test_smoke_api.py",
}

DB_INTEGRATION_PREFIXES = {
    "test_ingest_time_parsing.py": ("test_ingest_",),
}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-db-integration",
        action="store_true",
        default=False,
        help="run API tests that require a live PostgreSQL database",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration_db: requires a live PostgreSQL database with TorqMind migrations",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_db_integration = bool(config.getoption("--run-db-integration"))
    skip_db = pytest.mark.skip(
        reason="requires PostgreSQL; run with --run-db-integration inside Docker/Compose",
    )

    for item in items:
        filename = Path(str(item.fspath)).name
        is_db_integration = filename in DB_INTEGRATION_FILES or any(
            item.name.startswith(prefix) for prefix in DB_INTEGRATION_PREFIXES.get(filename, ())
        )
        if is_db_integration:
            item.add_marker(pytest.mark.integration_db)
            if not run_db_integration:
                item.add_marker(skip_db)
