"""Unit tests — Prompt 4 Parte A (MFA hardening). Sem TestClient/PG real.

Cobre: policy privilegiada (default OFF), TOTP counter/replay, recovery one-shot
mock, rate-limit em memória (APP_ENV=test).
"""
from __future__ import annotations

import base64
import unittest
from unittest.mock import patch

from app import repos_mfa, totp
from app.mfa_policy import (
    PRIVILEGED_MFA_ROLES,
    effective_totp_required,
    enforcement_enabled,
    is_privileged_mfa_role,
    policy_snapshot,
)


class MfaPolicyTests(unittest.TestCase):
    def test_privileged_roles_known(self) -> None:
        self.assertTrue(is_privileged_mfa_role("platform_master"))
        self.assertTrue(is_privileged_mfa_role("tenant_admin"))
        self.assertFalse(is_privileged_mfa_role("tenant_manager"))
        self.assertIn("platform_master", PRIVILEGED_MFA_ROLES)

    def test_enforcement_default_off(self) -> None:
        self.assertFalse(enforcement_enabled())
        # Privileged without column must NOT be forced while flag is off.
        self.assertFalse(
            effective_totp_required(
                {"totp_required": False, "totp_enabled": False},
                user_role="platform_master",
            )
        )

    def test_column_totp_required_still_wins(self) -> None:
        self.assertTrue(
            effective_totp_required({"totp_required": True}, user_role="tenant_viewer")
        )

    def test_enforcement_flag_would_force_privileged(self) -> None:
        with patch("app.mfa_policy.settings") as s:
            s.mfa_enforce_privileged = True
            self.assertTrue(
                effective_totp_required(
                    {"totp_required": False, "totp_enabled": False},
                    user_role="tenant_admin",
                )
            )
            self.assertFalse(
                effective_totp_required(
                    {"totp_required": False, "totp_enabled": False},
                    user_role="tenant_viewer",
                )
            )

    def test_policy_snapshot_informational(self) -> None:
        snap = policy_snapshot("platform_master", {"totp_enabled": False, "totp_required": False})
        self.assertTrue(snap["privileged_role"])
        self.assertFalse(snap["enforcement_enabled"])
        self.assertTrue(snap["policy_would_require_mfa"])


class MfaReplayAndThrottleTests(unittest.TestCase):
    def setUp(self) -> None:
        from app import security_attempts

        security_attempts.reset_memory_store_for_tests()
        repos_mfa._test_counters.clear()
        key = base64.urlsafe_b64encode(b"k" * 32).decode()
        self._key_patch = patch.object(totp.settings, "totp_encryption_key", key)
        self._key_patch.start()
        self._env_patch = patch.object(repos_mfa.settings, "app_env", "test")
        self._env_patch.start()
        self._env_sa = patch.object(security_attempts.settings, "app_env", "test")
        self._env_sa.start()

    def tearDown(self) -> None:
        from app import security_attempts

        self._key_patch.stop()
        self._env_patch.stop()
        self._env_sa.stop()
        security_attempts.reset_memory_store_for_tests()
        repos_mfa._test_counters.clear()

    def test_totp_counter_replay_rejected(self) -> None:
        uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        secret = totp.generate_secret()
        enc = totp.encrypt_secret(secret)
        code = totp.now_code(secret)
        counter = totp.match_code(secret, code)
        self.assertIsNotNone(counter)

        with patch.object(repos_mfa, "get_encrypted_secret", return_value=enc):
            first = repos_mfa.verify_totp_or_recovery(uid, code, purpose="verify")
            second = repos_mfa.verify_totp_or_recovery(uid, code, purpose="verify")
        self.assertTrue(first.ok)
        self.assertFalse(second.ok)
        self.assertEqual(second.error, "replay")

    def test_rate_limit_after_max_failures(self) -> None:
        uid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        secret = totp.generate_secret()
        enc = totp.encrypt_secret(secret)
        with patch.object(repos_mfa, "get_encrypted_secret", return_value=enc):
            with patch.object(repos_mfa, "consume_recovery_code", return_value=False):
                with patch.object(repos_mfa.settings, "mfa_max_attempts", 3):
                    for _ in range(3):
                        bad = repos_mfa.verify_totp_or_recovery(uid, "000000", purpose="verify")
                        self.assertEqual(bad.error, "invalid_code")
                    limited = repos_mfa.verify_totp_or_recovery(uid, "000000", purpose="verify")
        self.assertEqual(limited.error, "too_many_attempts")

    def test_recovery_consume_one_shot(self) -> None:
        uid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        secret = totp.generate_secret()
        enc = totp.encrypt_secret(secret)
        calls = {"n": 0}

        def _consume(user_id: str, code_hash: str) -> bool:
            calls["n"] += 1
            return calls["n"] == 1

        with patch.object(repos_mfa, "get_encrypted_secret", return_value=enc):
            with patch.object(repos_mfa, "consume_recovery_code", side_effect=_consume):
                with patch.object(repos_mfa, "mark_used"):
                    first = repos_mfa.verify_totp_or_recovery(
                        uid, "ABCDEfghij", purpose="verify"
                    )
                    # Force TOTP miss: use non-digit so match_code fails, recovery path only.
                    second = repos_mfa.verify_totp_or_recovery(
                        uid, "ABCDEfghij", purpose="verify"
                    )
        self.assertTrue(first.ok)
        self.assertTrue(first.used_recovery)
        self.assertFalse(second.ok)


class TotpMatchTests(unittest.TestCase):
    def test_match_code_returns_counter(self) -> None:
        secret = totp.generate_secret()
        at = 1_700_000_000.0
        code = totp.now_code(secret, at=at)
        counter = totp.match_code(secret, code, at=at)
        self.assertEqual(counter, int(at // 30))
        self.assertTrue(totp.verify_code(secret, code, at=at))


if __name__ == "__main__":
    unittest.main()
