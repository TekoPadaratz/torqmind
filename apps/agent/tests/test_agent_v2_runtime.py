"""Agent 2.0 unit tests — scheduler, watermark guard, update semver/hash."""

from __future__ import annotations

import json
import sys
import types
import unittest
from pathlib import Path

sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.runtime.scheduler import filter_datasets_for_cycle, should_run_dataset  # noqa: E402
from agent.state.watermark_guard import sanitize_temporal_watermark  # noqa: E402
from agent.update.manifest import is_newer_version, sha256_file, verify_sha256  # noqa: E402
from agent.update.apply import write_apply_script  # noqa: E402


class SchedulerTests(unittest.TestCase):
    def test_hot_every_cycle(self):
        self.assertTrue(should_run_dataset("comprovantes", 1))
        self.assertTrue(should_run_dataset("comprovantes", 17))

    def test_warm_every_5(self):
        self.assertTrue(should_run_dataset("contasreceber", 5))
        self.assertFalse(should_run_dataset("contasreceber", 1))
        self.assertFalse(should_run_dataset("contasreceber", 4))

    def test_cold_every_15(self):
        self.assertTrue(should_run_dataset("produtos", 15))
        self.assertFalse(should_run_dataset("produtos", 1))

    def test_filter_enabled_only(self):
        cfg = {
            "comprovantes": {"enabled": True},
            "produtos": {"enabled": True},
            "clientes": {"enabled": False},
        }
        out = filter_datasets_for_cycle(
            ["comprovantes", "produtos", "clientes"],
            15,
            cfg_datasets=cfg,
        )
        self.assertEqual(out, ["comprovantes", "produtos"])


class WatermarkGuardTests(unittest.TestCase):
    def test_future_clamped(self):
        safe, clamped = sanitize_temporal_watermark("2033-01-01T00:00:00")
        self.assertTrue(clamped)
        self.assertIsNotNone(safe)
        self.assertFalse(str(safe).startswith("2033"))

    def test_sentinel_rejected(self):
        safe, clamped = sanitize_temporal_watermark("1970-01-01T00:00:00")
        self.assertTrue(clamped)
        self.assertIsNone(safe)


class UpdateManifestTests(unittest.TestCase):
    def test_semver_newer(self):
        self.assertTrue(is_newer_version("2.0.0", "1.9.9"))
        self.assertTrue(is_newer_version("2.0.0", "2026.07.28+turnos"))
        self.assertFalse(is_newer_version("2.0.0", "2.0.0"))
        self.assertFalse(is_newer_version("1.9.0", "2.0.0"))

    def test_sha256_roundtrip(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bin"
            path.write_bytes(b"torqmind-agent-v2")
            digest = sha256_file(path)
            self.assertTrue(verify_sha256(path, digest))
            self.assertFalse(verify_sha256(path, "0" * 64))

    def test_write_apply_script(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            bat = write_apply_script(base)
            self.assertTrue(bat.is_file())
            text = bat.read_text(encoding="utf-8")
            self.assertIn("TorqMindAgent", text)
            self.assertIn("backup", text)


if __name__ == "__main__":
    unittest.main()
