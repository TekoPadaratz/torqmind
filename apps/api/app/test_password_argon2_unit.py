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


if __name__ == "__main__":
    unittest.main()
