import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from agent.state.watermark import WatermarkStore
from agent.utils.timezone import business_datetime_iso


class TestWatermarkStore(unittest.TestCase):
    def test_set_get_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            store = WatermarkStore(root_dir=td, tenant_key="empresa_1")
            store.set("comprovantes", "2026-01-01 00:00:00", scope="db:1")
            got = store.get("comprovantes", scope="db:1")
            self.assertEqual(got, business_datetime_iso(datetime(2026, 1, 1, 0, 0, 0), timespec="microseconds"))

    def test_migrate_legacy(self):
        with tempfile.TemporaryDirectory() as td:
            legacy = Path(td) / "state.json"
            legacy.write_text('{"COMPROVANTES": "2026-01-02 01:00:00"}', encoding="utf-8")

            store = WatermarkStore(root_dir=td, tenant_key="empresa_1")
            moved = WatermarkStore.migrate_legacy_state(str(legacy), store, scope="db:1")
            self.assertEqual(moved, 1)
            self.assertEqual(
                store.get("comprovantes", scope="db:1"),
                business_datetime_iso(datetime(2026, 1, 2, 1, 0, 0), timespec="microseconds"),
            )

    def test_migrate_legacy_does_not_override_existing(self):
        with tempfile.TemporaryDirectory() as td:
            legacy = Path(td) / "state.json"
            legacy.write_text('{"COMPROVANTES": "2026-01-02 01:00:00"}', encoding="utf-8")
            store = WatermarkStore(root_dir=td, tenant_key="empresa_1")
            store.set("comprovantes", "2026-01-03 01:00:00", scope="db:1")

            moved = WatermarkStore.migrate_legacy_state(str(legacy), store, scope="db:1")
            self.assertEqual(moved, 0)
            self.assertEqual(
                store.get("comprovantes", scope="db:1"),
                business_datetime_iso(datetime(2026, 1, 3, 1, 0, 0), timespec="microseconds"),
            )

    def test_normalize_to_iso(self):
        iso = WatermarkStore.normalize_watermark("2025-09-18 10:21:25.547000")
        self.assertEqual(iso, business_datetime_iso(datetime(2025, 9, 18, 10, 21, 25, 547000), timespec="microseconds"))


if __name__ == "__main__":
    unittest.main()
