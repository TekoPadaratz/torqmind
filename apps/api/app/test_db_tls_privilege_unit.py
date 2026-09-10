"""Prompt 6 — PostgreSQL conninfo TLS preservation + purpose pools (no live Hom/Prod)."""
from __future__ import annotations

import ssl
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from psycopg.conninfo import conninfo_to_dict

from app import db as db_mod
from app import db_clickhouse
from app.db import build_conninfo, db_purpose, redacted_conn_summary


class ConninfoTlsTests(unittest.TestCase):
    def test_preserves_ssl_query_params_from_url(self) -> None:
        url = (
            "postgresql://appuser:p%40ss@db.example:5432/torqmind"
            "?sslmode=verify-full&sslrootcert=/etc/ssl/certs/ca.pem&application_name=keep-me"
        )
        info = build_conninfo(purpose="api", database_url=url)
        parsed = conninfo_to_dict(info)
        self.assertEqual(parsed.get("sslmode"), "verify-full")
        self.assertEqual(parsed.get("sslrootcert"), "/etc/ssl/certs/ca.pem")
        self.assertEqual(parsed.get("user"), "appuser")
        self.assertEqual(parsed.get("password"), "p@ss")
        self.assertEqual(parsed.get("host"), "db.example")
        self.assertEqual(parsed.get("application_name"), "keep-me")

    def test_password_special_chars_escaped_via_make_conninfo(self) -> None:
        url = "postgresql://u:p%20w'ord@localhost:5432/db"
        info = build_conninfo(purpose="api", database_url=url)
        parsed = conninfo_to_dict(info)
        self.assertEqual(parsed.get("password"), "p w'ord")

    def test_discrete_sslmode_fills_when_url_has_none(self) -> None:
        url = "postgresql://u:p@localhost:5432/db"
        with patch.object(db_mod.settings, "pg_sslmode", "require"):
            with patch.object(db_mod.settings, "pg_sslrootcert", ""):
                info = build_conninfo(purpose="api", database_url=url)
        parsed = conninfo_to_dict(info)
        self.assertEqual(parsed.get("sslmode"), "require")

    def test_url_sslmode_not_overridden_by_settings(self) -> None:
        url = "postgresql://u:p@localhost:5432/db?sslmode=disable"
        with patch.object(db_mod.settings, "pg_sslmode", "verify-full"):
            info = build_conninfo(purpose="api", database_url=url)
        self.assertEqual(conninfo_to_dict(info).get("sslmode"), "disable")

    def test_purpose_application_name(self) -> None:
        url = "postgresql://u:p@localhost:5432/db"
        info = build_conninfo(purpose="ingest", database_url=url)
        self.assertEqual(conninfo_to_dict(info).get("application_name"), "torqmind-ingest")

    def test_db_purpose_context(self) -> None:
        with db_purpose("etl"):
            self.assertEqual(db_mod._default_purpose.get(), "etl")
        self.assertEqual(db_mod._default_purpose.get(), "api")

    def test_redacted_summary_hides_password(self) -> None:
        url = "postgresql://u:secret@localhost:5432/db?sslmode=require"
        summary = redacted_conn_summary(purpose="api", conninfo=build_conninfo(database_url=url))
        self.assertTrue(summary["password_set"])
        self.assertNotIn("secret", str(summary))
        self.assertEqual(summary["sslmode"], "require")

    def test_invalid_ca_path_still_builds_conninfo(self) -> None:
        url = "postgresql://u:p@127.0.0.1:1/db?sslmode=verify-full&sslrootcert=/no/such/ca.pem"
        info = build_conninfo(purpose="api", database_url=url)
        parsed = conninfo_to_dict(info)
        self.assertEqual(parsed.get("sslrootcert"), "/no/such/ca.pem")
        self.assertEqual(parsed.get("sslmode"), "verify-full")

    def test_ssl_context_rejects_bad_ca_file(self) -> None:
        """Isolated ssl module check — no network and no prod services."""
        ctx = ssl.create_default_context()
        self.assertTrue(ctx.check_hostname)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        with tempfile.TemporaryDirectory() as tmp:
            bogus = Path(tmp) / "empty.pem"
            bogus.write_text("not-a-cert\n", encoding="utf-8")
            with self.assertRaises((ssl.SSLError, PermissionError, OSError)):
                ssl.create_default_context(cafile=str(bogus))


class ClickHouseTlsKwargsTests(unittest.TestCase):
    def test_default_secure_false(self) -> None:
        with patch.object(db_clickhouse.settings, "clickhouse_secure", False):
            with patch.object(db_clickhouse.settings, "clickhouse_verify", True):
                with patch.object(db_clickhouse.settings, "clickhouse_ca_cert", ""):
                    kwargs = db_clickhouse.clickhouse_client_kwargs()
        self.assertFalse(kwargs["secure"])
        self.assertTrue(kwargs["verify"])
        self.assertNotIn("ca_cert", kwargs)

    def test_secure_with_ca(self) -> None:
        with patch.object(db_clickhouse.settings, "clickhouse_secure", True):
            with patch.object(db_clickhouse.settings, "clickhouse_verify", True):
                with patch.object(db_clickhouse.settings, "clickhouse_ca_cert", "/ca.pem"):
                    with patch.object(db_clickhouse.settings, "clickhouse_server_host_name", "ch.internal"):
                        kwargs = db_clickhouse.clickhouse_client_kwargs()
        self.assertTrue(kwargs["secure"])
        self.assertEqual(kwargs["ca_cert"], "/ca.pem")
        self.assertEqual(kwargs["server_host_name"], "ch.internal")


class PrivilegeTemplateTests(unittest.TestCase):
    def test_role_template_has_no_password_literals(self) -> None:
        root = Path(__file__).resolve().parents[3] / "deploy" / "sql" / "privilege_separation"
        text = (root / "01_roles_postgresql.sql.template").read_text(encoding="utf-8")
        self.assertNotIn("PASSWORD '", text)
        self.assertIn("CREATE ROLE torqmind_api", text)
        self.assertIn("NOSUPERUSER", text)


if __name__ == "__main__":
    unittest.main()
