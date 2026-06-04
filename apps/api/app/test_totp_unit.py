"""Unit tests for TOTP 2FA primitives and helpers.

These cover the algorithm and encryption helpers without a database. The
full login/reset/admin flows are covered by integration tests that require a
DB and are skipped when one is not configured.
"""
from __future__ import annotations

import base64
import os

import pytest

from app import totp


def _set_key(monkeypatch):
    # Deterministic Fernet key for encryption round-trip tests.
    key = base64.urlsafe_b64encode(b"0" * 32).decode()
    monkeypatch.setattr(totp.settings, "totp_encryption_key", key, raising=False)
    return key


def test_generate_secret_is_base32_and_unique():
    s1 = totp.generate_secret()
    s2 = totp.generate_secret()
    assert s1 != s2
    # base32 alphabet only
    assert all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567" for c in s1)


def test_verify_code_accepts_current_and_rejects_wrong():
    secret = totp.generate_secret()
    code = totp.now_code(secret)
    assert totp.verify_code(secret, code) is True
    wrong = "000000" if code != "000000" else "111111"
    assert totp.verify_code(secret, wrong) is False


def test_verify_code_rejects_malformed():
    secret = totp.generate_secret()
    assert totp.verify_code(secret, "") is False
    assert totp.verify_code(secret, "abc") is False
    assert totp.verify_code(secret, "12345") is False  # wrong length


def test_verify_code_window_tolerance():
    secret = totp.generate_secret()
    import time as _t

    now = _t.time()
    # A code from the previous 30s step should still validate (window=1).
    prev_code = totp.now_code(secret, at=now - 30)
    assert totp.verify_code(secret, prev_code, valid_window=1, at=now) is True
    # But not from 5 steps ago.
    old_code = totp.now_code(secret, at=now - 150)
    assert totp.verify_code(secret, old_code, valid_window=1, at=now) is False


def test_encrypt_decrypt_round_trip(monkeypatch):
    _set_key(monkeypatch)
    secret = totp.generate_secret()
    enc = totp.encrypt_secret(secret)
    assert enc != secret  # actually encrypted
    assert totp.decrypt_secret(enc) == secret


def test_encrypt_requires_key(monkeypatch):
    monkeypatch.setattr(totp.settings, "totp_encryption_key", "", raising=False)
    assert totp.is_totp_configured() is False
    with pytest.raises(totp.TOTPNotConfigured):
        totp.encrypt_secret("ABC")


def test_provisioning_uri_format():
    secret = totp.generate_secret()
    uri = totp.provisioning_uri(secret, "user@example.com", issuer="TorqMind")
    assert uri.startswith("otpauth://totp/")
    assert f"secret={secret}" in uri
    assert "issuer=TorqMind" in uri
    assert "digits=6" in uri and "period=30" in uri


def test_recovery_codes_unique_and_hashable():
    codes = totp.generate_recovery_codes(8)
    assert len(codes) == 8
    assert len(set(codes)) == 8
    # Hash is stable regardless of separators/case.
    c = codes[0]
    assert totp.hash_recovery_code(c) == totp.hash_recovery_code(c.lower())
    assert totp.hash_recovery_code(c) == totp.hash_recovery_code(c.replace("-", ""))


def test_secret_never_equals_encrypted(monkeypatch):
    _set_key(monkeypatch)
    secret = totp.generate_secret()
    enc = totp.encrypt_secret(secret)
    # Encrypted token must not leak the plaintext secret substring.
    assert secret not in enc
