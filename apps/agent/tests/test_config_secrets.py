import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent.config import (
    AgentConfigError,
    DEFAULT_FULL_REFRESH_MIN_INTERVAL_SECONDS,
    build_default_raw_config,
    load_config,
    load_raw_config,
    migrate_yaml_to_encrypted_config,
    normalize_ingest_key,
    save_encrypted_config,
)
from agent.secrets import load_encrypted_json_file


class TestEncryptedConfig(unittest.TestCase):
    def test_build_default_raw_config_prefers_secure_sqlserver_defaults(self):
        raw = build_default_raw_config()
        self.assertEqual(raw["sqlserver"]["driver"], "ODBC Driver 18 for SQL Server")
        self.assertTrue(raw["sqlserver"]["encrypt"])
        self.assertFalse(raw["sqlserver"]["trust_server_certificate"])

    def test_full_refresh_datasets_get_default_throttle_interval(self):
        # Defaults are applied at merge/load time; build_default only stores enabled toggles.
        with tempfile.TemporaryDirectory() as td:
            yaml_path = Path(td) / "config.local.yaml"
            yaml_path.write_text(
                """
sqlserver:
  server: sql.internal
  database: torq
  user: sa
  password: x
api:
  base_url: https://api.example.com
  ingest_key: k
""",
                encoding="utf-8",
            )
            cfg = load_config(str(yaml_path))
        self.assertTrue(cfg.datasets["estoque"].get("full_refresh"))
        self.assertEqual(
            int(cfg.datasets["estoque"].get("full_refresh_min_interval_seconds")),
            DEFAULT_FULL_REFRESH_MIN_INTERVAL_SECONDS,
        )
        self.assertEqual(
            int(cfg.datasets["funcionarios"].get("full_refresh_min_interval_seconds")),
            DEFAULT_FULL_REFRESH_MIN_INTERVAL_SECONDS,
        )

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

    def test_stale_dataset_query_in_config_does_not_override_builtin_sql(self):
        """config.enc from an older build must not keep TRY_CONVERT after exe upgrade."""
        with tempfile.TemporaryDirectory() as td:
            yaml_path = Path(td) / "config.local.yaml"
            yaml_path.write_text(
                """
sqlserver:
  server: sql.internal
  database: torq
  user: sa
  password: x
api:
  base_url: https://api.example.com
  ingest_key: k
datasets:
  formas_pgto_comprovantes:
    enabled: true
    query: "SELECT 1 AS broken WHERE TRY_CONVERT(int, '1') = 1"
""",
                encoding="utf-8",
            )
            cfg = load_config(str(yaml_path))
        query = str(cfg.datasets["formas_pgto_comprovantes"].get("query") or "")
        self.assertNotIn("TRY_CONVERT", query)
        self.assertIn("FORMAS_PGTO_COMPROVANTES", query.upper())
        self.assertIn("CASE", query.upper())

    def test_build_default_raw_config_does_not_embed_dataset_sql(self):
        raw = build_default_raw_config()
        formas = raw["datasets"]["formas_pgto_comprovantes"]
        self.assertIn("enabled", formas)
        self.assertNotIn("query", formas)

    def test_normalize_ingest_key_recovers_duplicated_uuid_paste(self):
        duplicated = (
            "505aee7c-3651-4a1f-877f-ae96772caac5"
            "505aee7c-3651-4a1f-877f-ae96772caac5"
        )
        self.assertEqual(
            normalize_ingest_key(duplicated),
            "505aee7c-3651-4a1f-877f-ae96772caac5",
        )


if __name__ == "__main__":
    unittest.main()
