"""Unit tests — Prompt 4 Parte B (Argon2id + bcrypt legado). Sem PG.

Não instala pacotes no host: testes de hash Argon2id fazem skip se argon2-cffi
não estiver disponível. A ambiguidade bcrypt >72 bytes é exercitada só com bcrypt.
"""
from __future__ import annotations

import concurrent.futures
import time
import unittest

from passlib.context import CryptContext

from app.password_policy import PASSWORD_MAX_BYTES, validate_password
from app.security import (
    BCRYPT_MAX_PASSWORD_BYTES,
    _argon2_available,
    hash_password,
    identify_password_scheme,
    password_byte_length,
    verify_password,
    verify_password_detailed,
)


class BcryptLegacyAmbiguityTests(unittest.TestCase):
    def test_suffix_collision_over_72_bytes(self) -> None:
        bcrypt_only = CryptContext(schemes=["bcrypt"], deprecated="auto")
        base = "A" * BCRYPT_MAX_PASSWORD_BYTES
        pw_a = base + "XXXX"
        pw_b = base + "YYYY"
        self.assertNotEqual(pw_a, pw_b)
        digest = bcrypt_only.hash(pw_a)
        # passlib/bcrypt truncates — both verify against the same hash.
        self.assertTrue(bcrypt_only.verify(pw_a, digest))
        self.assertTrue(bcrypt_only.verify(pw_b, digest))

        check_a = verify_password_detailed(pw_a, digest)
        check_b = verify_password_detailed(pw_b, digest)
        self.assertTrue(check_a.ok)
        self.assertTrue(check_b.ok)
        self.assertTrue(check_a.ambiguous_bcrypt_long)
        self.assertTrue(check_b.ambiguous_bcrypt_long)
        self.assertFalse(check_a.upgradeable_to_argon2)

    def test_short_bcrypt_not_ambiguous(self) -> None:
        bcrypt_only = CryptContext(schemes=["bcrypt"], deprecated="auto")
        pw = "ShortPass1!"
        digest = bcrypt_only.hash(pw)
        check = verify_password_detailed(pw, digest)
        self.assertTrue(check.ok)
        self.assertFalse(check.ambiguous_bcrypt_long)
        self.assertEqual(check.scheme, "bcrypt")
        if _argon2_available():
            self.assertTrue(check.upgradeable_to_argon2)

    def test_multibyte_byte_length(self) -> None:
        # "á" is 2 UTF-8 bytes — length in chars ≠ bytes.
        pw = "á" * 40
        self.assertEqual(len(pw), 40)
        self.assertEqual(password_byte_length(pw), 80)
        self.assertGreater(password_byte_length(pw), BCRYPT_MAX_PASSWORD_BYTES)


class PasswordPolicyBytesTests(unittest.TestCase):
    def test_rejects_over_max_bytes(self) -> None:
        # 200 non-ASCII chars → 400 bytes > 256.
        pw = "Áa1!" + ("á" * 200)
        errors = validate_password(pw)
        self.assertTrue(any("bytes" in e for e in errors))
        self.assertEqual(PASSWORD_MAX_BYTES, 256)


@unittest.skipUnless(_argon2_available(), "argon2-cffi not installed on this host")
class Argon2idHashTests(unittest.TestCase):
    def test_hash_and_verify_roundtrip(self) -> None:
        pw = "Argon2id-Ok1!"
        digest = hash_password(pw)
        self.assertEqual(identify_password_scheme(digest), "argon2")
        self.assertTrue(verify_password(pw, digest))
        self.assertFalse(verify_password(pw + "x", digest))
        check = verify_password_detailed(pw, digest)
        self.assertTrue(check.ok)
        self.assertFalse(check.upgradeable_to_argon2)

    def test_long_password_not_truncated(self) -> None:
        base = "B" * 80
        pw_a = base + "ONE"
        pw_b = base + "TWO"
        digest = hash_password(pw_a)
        self.assertTrue(verify_password(pw_a, digest))
        self.assertFalse(verify_password(pw_b, digest))

    def test_cost_single_and_concurrent(self) -> None:
        """Light cost probe — documents latency/memory params, not a CI gate."""
        pw = "CostProbe1!"
        t0 = time.perf_counter()
        digest = hash_password(pw)
        single_s = time.perf_counter() - t0
        self.assertTrue(verify_password(pw, digest))

        def _one(_: int) -> float:
            started = time.perf_counter()
            hash_password(pw)
            return time.perf_counter() - started

        workers = 4
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            times = list(pool.map(_one, range(workers)))
        # Soft bound: single hash should stay interactive (< 2s on typical HW).
        self.assertLess(single_s, 2.0)
        self.assertLess(max(times), 5.0)
        # Keep the measurement visible in verbose runs.
        print(
            f"argon2id_cost single={single_s:.3f}s "
            f"concurrent_n={workers} max={max(times):.3f}s avg={sum(times)/len(times):.3f}s"
        )


class ConditionalArgon2UpgradeTests(unittest.TestCase):
    """Login must not overwrite a concurrent password reset with a stale bcrypt upgrade."""

    def test_upgrade_skipped_when_hash_changed_concurrently(self) -> None:
        from unittest.mock import patch

        from app import repos_auth
        from app.security import PasswordCheck

        old_digest = "$2b$12$legacyhashplaceholderxxxxxxxxxxxxxuYYYYYYYYYYYYYYYYYYY"
        pw = "LegacyOk1!"
        uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        user = {
            "id": uid,
            "email": "u@example.com",
            "username": "u",
            "password_hash": old_digest,
            "is_active": True,
            "nome": "U",
            "role": "tenant_admin",
            "must_change_password": False,
            "locked_until": None,
            "password_changed_at": None,
        }
        check = PasswordCheck(
            ok=True,
            scheme="bcrypt",
            upgradeable_to_argon2=True,
            ambiguous_bcrypt_long=False,
        )

        with patch.object(repos_auth, "get_user_by_identifier", return_value=dict(user)):
            with patch.object(repos_auth, "verify_password_detailed", return_value=check):
                with patch.object(repos_auth, "hash_password", return_value="$argon2id$new"):
                    with patch.object(repos_auth, "_upgrade_password_hash", return_value=False) as upgrade:
                        with patch.object(repos_auth, "_list_user_access_rows", return_value=[]):
                            with patch.object(repos_auth, "_build_session_context", return_value={"sub": uid}):
                                with patch.object(repos_auth, "_record_successful_login"):
                                    with self.assertRaises(repos_auth.AuthError) as ctx:
                                        repos_auth.verify_login("u@example.com", pw)
        self.assertEqual(ctx.exception.error, "credentials_changed")
        upgrade.assert_called_once()
        self.assertEqual(upgrade.call_args.kwargs.get("expected_old_hash"), old_digest)

    def test_upgrade_succeeds_when_hash_still_matches(self) -> None:
        from unittest.mock import patch

        from app import repos_auth
        from app.security import PasswordCheck

        old_digest = "$2b$12$legacyhashplaceholderxxxxxxxxxxxxxuZZZZZZZZZZZZZZZZZZZ"
        pw = "LegacyOk2!"
        uid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        user = {
            "id": uid,
            "email": "v@example.com",
            "username": "v",
            "password_hash": old_digest,
            "is_active": True,
            "nome": "V",
            "role": "tenant_admin",
            "must_change_password": False,
            "locked_until": None,
            "password_changed_at": None,
        }
        check = PasswordCheck(
            ok=True,
            scheme="bcrypt",
            upgradeable_to_argon2=True,
            ambiguous_bcrypt_long=False,
        )

        with patch.object(repos_auth, "get_user_by_identifier", return_value=dict(user)):
            with patch.object(repos_auth, "verify_password_detailed", return_value=check):
                with patch.object(repos_auth, "hash_password", return_value="$argon2id$new"):
                    with patch.object(repos_auth, "_upgrade_password_hash", return_value=True) as upgrade:
                        with patch.object(repos_auth, "_list_user_access_rows", return_value=[]):
                            with patch.object(
                                repos_auth,
                                "_build_session_context",
                                return_value={"sub": uid, "email": "v@example.com"},
                            ):
                                with patch.object(repos_auth, "_record_successful_login"):
                                    session = repos_auth.verify_login("v@example.com", pw)
        self.assertEqual(session["sub"], uid)
        upgrade.assert_called_once()
        self.assertEqual(upgrade.call_args.kwargs.get("expected_old_hash"), old_digest)


class ConditionalUpgradeSqlRaceTests(unittest.TestCase):
    """Conditional UPDATE race — no Argon2 dependency."""

    def test_login_vs_reset_race_controlled(self) -> None:
        """Simulated race: reset updates hash between verify and upgrade UPDATE."""
        from unittest.mock import MagicMock, patch

        from app import repos_auth

        bcrypt_only = CryptContext(schemes=["bcrypt"], deprecated="auto")
        pw = "RacePass1!"
        old_digest = bcrypt_only.hash(pw)
        new_digest = "$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$bm90LWEtcmVhbC1oYXNo"
        uid = "cccccccc-cccc-cccc-cccc-cccccccccccc"

        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchone.return_value = None  # no RETURNING row

        with patch.object(repos_auth, "get_conn", return_value=conn):
            ok = repos_auth._upgrade_password_hash(uid, new_digest, expected_old_hash=old_digest)
        self.assertFalse(ok)
        sql = conn.execute.call_args.args[0]
        self.assertIn("password_hash = %s", sql)
        self.assertIn("AND password_hash = %s", sql)
        self.assertEqual(conn.execute.call_args.args[1], (new_digest, uid, old_digest))


if __name__ == "__main__":
    unittest.main()
