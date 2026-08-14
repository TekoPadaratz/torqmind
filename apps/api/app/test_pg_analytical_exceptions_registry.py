from __future__ import annotations

import json
import sys
import types
import unittest
from pathlib import Path

try:
    import clickhouse_connect  # noqa: F401
except ModuleNotFoundError:
    fake_clickhouse = types.ModuleType("clickhouse_connect")
    fake_client_module = types.SimpleNamespace(Client=object)
    fake_clickhouse.driver = types.SimpleNamespace(client=fake_client_module)
    fake_clickhouse.get_client = lambda **_kwargs: None
    sys.modules["clickhouse_connect"] = fake_clickhouse

from app import repos_analytics

REQUIRED_FIELDS = {
    "function",
    "class",
    "endpoint",
    "screen",
    "source",
    "motivo",
    "responsavel",
    "risco",
    "date",
    "prazo_remocao",
    "teste",
}
ALLOWED_CLASSES = {
    "oltp_authorized",
    "write_authorized",
    "pipeline_authorized",
    "operational_non_analytical",
    "analytical_debt",
}


class PostgresAnalyticalExceptionsRegistryTest(unittest.TestCase):
    def test_registry_covers_owned_debt_and_forbids_unregistered_legacy(self) -> None:
        payload = json.loads(Path(repos_analytics.PG_EXCEPTIONS_PATH).read_text(encoding="utf-8"))
        exceptions = payload["exceptions"]
        names = [item["function"] for item in exceptions]
        self.assertEqual(len(names), len(set(names)), "duplicate function in PG exception registry")

        for item in exceptions:
            missing = REQUIRED_FIELDS - set(item)
            self.assertFalse(missing, f"{item.get('function')}: missing {missing}")
            self.assertIn(item["class"], ALLOWED_CLASSES)

        registered = set(names)
        owned = set(repos_analytics._POSTGRES_OWNED_FUNCTIONS)
        debt = set(repos_analytics._CLICKHOUSE_DEBT_FUNCTIONS)
        self.assertEqual(owned, {item["function"] for item in exceptions if item["class"] != "analytical_debt"})
        self.assertEqual(debt, {item["function"] for item in exceptions if item["class"] == "analytical_debt"})

        inventory = repos_analytics.analytics_backend_inventory()
        by_name = {row["function"]: row for row in inventory["functions"]}
        for name in registered:
            self.assertIn(name, by_name, f"registry function {name} is not a public repos_mart function")

        for row in inventory["functions"]:
            if row["source"] in {"postgres_app", "postgres_debt", "postgres_legacy"}:
                self.assertIn(
                    row["function"],
                    registered,
                    f"unregistered PostgreSQL analytics function {row['function']} source={row['source']}",
                )
