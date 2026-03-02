import tempfile
import unittest
from pathlib import Path

from agent.state.watermark import WatermarkStore


class TestWatermarkStore(unittest.TestCase):
    def test_set_get_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            store = WatermarkStore(root_dir=td, tenant_key="empresa_1")
            store.set("comprovantes", "2026-01-01 00:00:00", scope="db:1")
            got = store.get("comprovantes", scope="db:1")
            self.assertEqual(got, "2026-01-01 00:00:00")

    def test_migrate_legacy(self):
        with tempfile.TemporaryDirectory() as td:
            legacy = Path(td) / "state.json"
            legacy.write_text('{"COMPROVANTES": "2026-01-02 01:00:00"}', encoding="utf-8")

            store = WatermarkStore(root_dir=td, tenant_key="empresa_1")
            moved = WatermarkStore.migrate_legacy_state(str(legacy), store, scope="db:1")
            self.assertEqual(moved, 1)
            self.assertEqual(store.get("comprovantes", scope="db:1"), "2026-01-02 01:00:00")


if __name__ == "__main__":
    unittest.main()
