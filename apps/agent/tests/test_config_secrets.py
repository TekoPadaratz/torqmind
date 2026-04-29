import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent.config import (
    AgentConfigError,
    build_default_raw_config,
    load_config,
    load_raw_config,
    migrate_yaml_to_encrypted_config,
    save_encrypted_config,
)
from agent.secrets import load_encrypted_json_file


class TestEncryptedConfig(unittest.TestCase):
    def test_build_default_raw_config_prefers_secure_sqlserver_defaults(self):
        raw = build_default_raw_config()
        self.assertEqual(raw["sqlserver"]["driver"], "ODBC Driver 18 for SQL Server")
        self.assertTrue(raw["sqlserver"]["encrypt"])
        self.assertFalse(raw["sqlserver"]["trust_server_certificate"])

    def test_save_and_load_encrypted_config_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "config.enc"
            raw = build_default_raw_config()
            raw["sqlserver"]["server"] = "sql.internal"
            raw["sqlserver"]["database"] = "torq"
            raw["sqlserver"]["user"] = "sa"
            raw["sqlserver"]["password"] = "super-secret"
            raw["api"]["base_url"] = "https://api.example.com"
            raw["api"]["ingest_key"] = "ingest-123"

            with patch("agent.secrets._protect_data", side_effect=lambda value: b"enc:" + value), patch(
                "agent.secrets._unprotect_data",
                side_effect=lambda value: value[4:],
            ):
                save_encrypted_config(target, raw)
                loaded = load_encrypted_json_file(target)

            self.assertEqual(loaded["sqlserver"]["password"], "super-secret")
            self.assertEqual(loaded["api"]["ingest_key"], "ingest-123")

    def test_load_config_from_encrypted_file(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "config.enc"
            raw = build_default_raw_config()
            raw["sqlserver"]["server"] = "sql.internal"
            raw["sqlserver"]["database"] = "torq"
            raw["sqlserver"]["user"] = "sa"
            raw["sqlserver"]["password"] = "pw-1"
            raw["api"]["base_url"] = "https://api.example.com"
            raw["api"]["ingest_key"] = "ingest-1"

            with patch("agent.secrets._protect_data", side_effect=lambda value: b"enc:" + value), patch(
                "agent.secrets._unprotect_data",
                side_effect=lambda value: value[4:],
            ):
                save_encrypted_config(target, raw)
                cfg = load_config(str(target))

            self.assertEqual(cfg.sqlserver.password, "pw-1")
            self.assertEqual(cfg.api.ingest_key, "ingest-1")
            self.assertEqual(cfg.sqlserver.port, 1433)

    def test_migrate_yaml_to_encrypted_config_removes_source(self):
        with tempfile.TemporaryDirectory() as td:
            yaml_path = Path(td) / "config.local.yaml"
            yaml_path.write_text(
                """
sqlserver:
  server: sql.internal
  port: 1433
  database: torq
  user: sa
  password: legacy-password
api:
  base_url: https://api.example.com
  ingest_key: legacy-ingest
runtime:
  interval_seconds: 60
""".strip(),
                encoding="utf-8",
            )
            target = Path(td) / "config.enc"

            with patch("agent.secrets._protect_data", side_effect=lambda value: b"enc:" + value), patch(
                "agent.secrets._unprotect_data",
                side_effect=lambda value: value[4:],
            ):
                result = migrate_yaml_to_encrypted_config(yaml_path, target)
                raw, meta = load_raw_config(target)

            self.assertFalse(yaml_path.exists())
            self.assertEqual(result["target"], str(target))
            self.assertEqual(meta["kind"], "encrypted")
            self.assertEqual(raw["sqlserver"]["password"], "legacy-password")
            self.assertEqual(raw["api"]["ingest_key"], "legacy-ingest")

    def test_missing_required_secret_raises_friendly_error(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "config.enc"
            raw = build_default_raw_config()
            raw["sqlserver"]["server"] = "sql.internal"
            raw["sqlserver"]["database"] = "torq"
            raw["sqlserver"]["user"] = "sa"
            raw["sqlserver"]["password"] = ""
            raw["api"]["base_url"] = "https://api.example.com"
            raw["api"]["ingest_key"] = ""
            raw["api"]["empresa_id"] = None

            with patch("agent.secrets._protect_data", side_effect=lambda value: b"enc:" + value):
                with self.assertRaises(AgentConfigError):
                    save_encrypted_config(target, raw)


if __name__ == "__main__":
    unittest.main()
