"""Unit tests for recovery dual-verify overlay (old token contract)."""
from __future__ import annotations

import importlib.util
import inspect
import sys
import unittest
from pathlib import Path

from passlib.context import CryptContext


def _load_dual_verify():
    root = Path(__file__).resolve().parents[3]
    path = root / "deploy" / "docker" / "security_dual_verify.py"
    # Provide minimal app.config.settings stub before load.
    import types

    if "app" not in sys.modules:
        sys.modules["app"] = types.ModuleType("app")
    if "app.config" not in sys.modules:
        cfg = types.ModuleType("app.config")

        class _S:
            api_jwt_secret = "unit-test-jwt-secret-32chars-min!!"
            api_jwt_issuer = "torqmind-api"
            api_access_token_minutes = 30

        cfg.settings = _S()
        sys.modules["app.config"] = cfg

    spec = importlib.util.spec_from_file_location("security_dual_verify_under_test", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class RecoveryDualVerifyUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sec = _load_dual_verify()

    def test_create_access_token_signature_is_pre_hardening(self) -> None:
        params = list(inspect.signature(self.sec.create_access_token).parameters)
        self.assertEqual(params, ["payload", "minutes"])

    def test_challenge_markers_survive_create_access_token(self) -> None:
        token = self.sec.create_access_token(
            {"sub": "u", "scope": "mfa_challenge", "mfa_pending": True},
            minutes=5,
        )
        payload = self.sec.decode_token(token)
        self.assertTrue(payload.get("mfa_pending"))
        self.assertEqual(payload.get("scope"), "mfa_challenge")
        self.assertNotIn("token_use", payload)

    def test_plain_access_does_not_invent_token_use(self) -> None:
        payload = self.sec.decode_token(self.sec.create_access_token({"sub": "u"}, minutes=10))
        self.assertNotIn("token_use", payload)
        self.assertNotIn("mfa_pending", payload)

    def test_bcrypt_and_argon2_verify(self) -> None:
        pw = "UnitRecovery!pass"
        bcrypt_hash = CryptContext(schemes=["bcrypt"], deprecated="auto").hash(pw)
        self.assertEqual(self.sec.identify_password_scheme(bcrypt_hash), "bcrypt")
        self.assertTrue(self.sec.verify_password(pw, bcrypt_hash))
        if not self.sec._argon2_available():
            self.skipTest("argon2-cffi not installed in this environment")
        argon_hash = self.sec.hash_password(pw)
        self.assertEqual(self.sec.identify_password_scheme(argon_hash), "argon2")
        self.assertTrue(self.sec.verify_password(pw, argon_hash))
        self.assertFalse(self.sec.verify_password("nope", argon_hash))


if __name__ == "__main__":
    unittest.main()
