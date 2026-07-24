"""Unit: status SMTP público e resolução DB/env."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app import email_service
from app.email_service import SmtpRuntime


def _env_settings(**overrides):
    class S:
        smtp_enabled = False
        smtp_host = "mail.example.com"
        smtp_port = 465
        smtp_user = "user@example.com"
        smtp_password = "SECRET_MUST_NOT_LEAK"
        smtp_use_ssl = True
        smtp_use_tls = False
        smtp_from_email = "user@example.com"
        smtp_from = ""
        smtp_from_name = "TorqMind"
        smtp_timeout_seconds = 20

    for k, v in overrides.items():
        setattr(S, k, v)
    return S


class SmtpStatusPublicTest(unittest.TestCase):
    def test_status_sem_senha_e_configured_false(self) -> None:
        with (
            patch.object(email_service, "settings", _env_settings()),
            patch.object(email_service, "_load_db_smtp_row", return_value=None),
        ):
            status = email_service.smtp_status_public()
        self.assertFalse(status["configured"])
        self.assertEqual(status["host"], "mail.example.com")
        self.assertEqual(status["port"], 465)
        self.assertTrue(status["use_ssl"])
        self.assertEqual(status["source"], "environment")
        blob = str(status)
        self.assertNotIn("SECRET_MUST_NOT_LEAK", blob)
        self.assertIn("password_configured", status)

    def test_configured_true_com_ssl_465(self) -> None:
        with (
            patch.object(email_service, "settings", _env_settings(smtp_enabled=True)),
            patch.object(email_service, "_load_db_smtp_row", return_value=None),
        ):
            self.assertTrue(email_service.is_email_configured())
            status = email_service.smtp_status_public()
        self.assertTrue(status["configured"])
        self.assertTrue(status["user_configured"])
        self.assertTrue(status["password_configured"])

    def test_db_host_override_env(self) -> None:
        row = {
            "channel_name": "TorqMind",
            "from_email": "db@example.com",
            "smtp_enabled": True,
            "smtp_host": "smtp.db.local",
            "smtp_port": 587,
            "smtp_user": "dbuser",
            "smtp_password_encrypted": "",
            "smtp_use_ssl": False,
            "smtp_use_tls": True,
            "smtp_from_name": "DB Name",
            "smtp_timeout_seconds": 30,
        }
        with (
            patch.object(email_service, "settings", _env_settings(smtp_enabled=True)),
            patch.object(email_service, "_load_db_smtp_row", return_value=row),
            patch.object(email_service, "_decrypt_db_password", return_value=""),
        ):
            cfg = email_service.resolve_smtp_runtime()
        self.assertIsInstance(cfg, SmtpRuntime)
        self.assertEqual(cfg.source, "database")
        self.assertEqual(cfg.host, "smtp.db.local")
        self.assertEqual(cfg.from_email, "db@example.com")
        self.assertEqual(cfg.password, "SECRET_MUST_NOT_LEAK")  # fallback env
        self.assertEqual(cfg.timeout_seconds, 30)


if __name__ == "__main__":
    unittest.main()
