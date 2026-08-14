from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.cli.migrate import (
    accepted_checksums,
    assert_not_protected_bootstrap,
    checksum_v1_raw,
    checksum_v2_normalized,
    is_protected_environment,
    load_migration_manifest,
    resolve_migrations_dir,
    validate_migration_chain,
)


class MigrationGuardUnitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.migrations_dir = resolve_migrations_dir()

    def test_manifest_matches_disk_and_frozen_duplicates(self) -> None:
        validate_migration_chain(self.migrations_dir)
        manifest = load_migration_manifest(self.migrations_dir)
        self.assertEqual(
            set(manifest["historical_duplicate_prefixes"]),
            {"012", "013", "135"},
        )
        self.assertEqual(manifest["bootstrap_destructive_files"], ["003_mart_demo.sql"])

    def test_v1_and_v2_checksums_are_both_accepted(self) -> None:
        legacy = next(
            path
            for path in self.migrations_dir.glob("*.sql")
            if path.name.startswith("080_")
        )
        v1 = checksum_v1_raw(legacy)
        v2 = checksum_v2_normalized(legacy)
        accepted = accepted_checksums(legacy)
        self.assertIn(v1, accepted)
        self.assertIn(v2, accepted)

    def test_new_duplicate_prefix_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            for src in (self.migrations_dir / "001_auth.sql", self.migrations_dir / "002_app_rls.sql"):
                (tmp_path / src.name).write_bytes(src.read_bytes())
            (tmp_path / "001_extra.sql").write_text("SELECT 1;\n", encoding="utf-8")
            (tmp_path / "MANIFEST.json").write_text(
                json.dumps(
                    {
                        "historical_duplicate_prefixes": {},
                        "files": [
                            {"filename": "001_auth.sql", "kind": "incremental"},
                            {"filename": "001_extra.sql", "kind": "incremental"},
                            {"filename": "002_app_rls.sql", "kind": "incremental"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "New duplicate migration prefix 001"):
                validate_migration_chain(tmp_path)

    def test_utf8_required_for_new_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "200_new.sql").write_bytes("SELECT 'ação';\n".encode("cp1252"))
            (tmp_path / "MANIFEST.json").write_text(
                json.dumps(
                    {
                        "historical_duplicate_prefixes": {},
                        "files": [{"filename": "200_new.sql", "kind": "incremental"}],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "must be UTF-8"):
                validate_migration_chain(tmp_path)

    def test_manifest_detects_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "001_auth.sql").write_text("SELECT 1;\n", encoding="utf-8")
            (tmp_path / "MANIFEST.json").write_text(
                json.dumps(
                    {
                        "historical_duplicate_prefixes": {},
                        "files": [
                            {"filename": "001_auth.sql", "kind": "incremental"},
                            {"filename": "002_missing.sql", "kind": "incremental"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "missing="):
                validate_migration_chain(tmp_path)

    def test_protected_environments_refuse_bootstrap(self) -> None:
        self.assertTrue(is_protected_environment("homolog"))
        self.assertTrue(is_protected_environment("prod"))
        self.assertFalse(is_protected_environment("dev"))
        with patch("app.cli.migrate.current_app_env", return_value="homolog"):
            with self.assertRaisesRegex(RuntimeError, "protected"):
                assert_not_protected_bootstrap()

    def test_reset_script_refuses_homolog_and_prod(self) -> None:
        reset = (self.migrations_dir.parent / "torqmind_reset_db_v2.sql").read_text(
            encoding="utf-8", errors="replace"
        )
        self.assertIn("TM_EPHEMERAL_LOCAL", reset)
        self.assertIn("must be dev", reset)
        self.assertNotIn("v_reset_env NOT IN ('dev', 'homolog')", reset)


if __name__ == "__main__":
    unittest.main()
