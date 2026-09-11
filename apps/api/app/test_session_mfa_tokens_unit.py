"""Unit tests for session revocation + MFA token classification (Prompt 3).

No TestClient / PG pool — pure JWT + dependency helpers with mocks.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from fastapi import HTTPException

from app import deps
from app.security import (
    TOKEN_USE_ACCESS,
    TOKEN_USE_MFA_CHALLENGE,
    TOKEN_USE_MFA_SETUP,
    assert_not_revoked_by_password_change,
    assert_session_not_absolutely_expired,
    classify_token_use,
    create_access_token,
    decode_token,
    resolve_session_exp,
)


class TokenClassificationUnitTest(unittest.TestCase):
    def test_explicit_token_use_and_legacy_scope(self) -> None:
        self.assertEqual(classify_token_use({"token_use": TOKEN_USE_ACCESS}), TOKEN_USE_ACCESS)
        self.assertEqual(classify_token_use({"scope": "mfa_challenge", "mfa_pending": True}), TOKEN_USE_MFA_CHALLENGE)
        self.assertEqual(classify_token_use({"scope": "mfa_setup", "mfa_pending": True}), TOKEN_USE_MFA_SETUP)
        self.assertEqual(classify_token_use({"mfa_pending": True}), TOKEN_USE_MFA_CHALLENGE)
        self.assertEqual(classify_token_use({"sub": "u"}), TOKEN_USE_ACCESS)

    def test_issue_access_vs_mfa_tokens(self) -> None:
        access = decode_token(create_access_token({"sub": "u1", "user_role": "tenant_admin"}, token_use=TOKEN_USE_ACCESS))
        challenge = decode_token(
            create_access_token({"sub": "u1"}, minutes=5, token_use=TOKEN_USE_MFA_CHALLENGE)
        )
        setup = decode_token(create_access_token({"sub": "u1"}, minutes=5, token_use=TOKEN_USE_MFA_SETUP))

        self.assertEqual(access["token_use"], TOKEN_USE_ACCESS)
        self.assertIn("session_exp", access)
        self.assertNotIn("mfa_pending", access)

        self.assertEqual(challenge["token_use"], TOKEN_USE_MFA_CHALLENGE)
        self.assertTrue(challenge["mfa_pending"])
        self.assertEqual(challenge["scope"], "mfa_challenge")
        self.assertNotIn("session_exp", challenge)

        self.assertEqual(setup["token_use"], TOKEN_USE_MFA_SETUP)
        self.assertEqual(setup["scope"], "mfa_setup")


class PasswordRevocationUnitTest(unittest.TestCase):
    def test_token_before_password_change_is_revoked(self) -> None:
        issued = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
        changed = datetime(2026, 9, 10, 12, 5, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            assert_not_revoked_by_password_change({"iat": issued}, changed)

    def test_token_after_password_change_survives(self) -> None:
        changed = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
        issued = datetime(2026, 9, 10, 12, 0, 0, 500000, tzinfo=timezone.utc)
        assert_not_revoked_by_password_change({"iat": issued}, changed)  # within skew

    def test_missing_password_changed_at_is_noop(self) -> None:
        assert_not_revoked_by_password_change({"iat": datetime.now(timezone.utc)}, None)


class AbsoluteSessionUnitTest(unittest.TestCase):
    def test_refresh_preserves_session_exp(self) -> None:
        first = decode_token(create_access_token({"sub": "u", "user_role": "tenant_admin"}, minutes=30))
        session_exp = resolve_session_exp(first)
        refreshed = decode_token(
            create_access_token(
                {"sub": "u", "user_role": "tenant_admin"},
                minutes=30,
                session_exp=session_exp,
            )
        )
        self.assertEqual(resolve_session_exp(refreshed), session_exp)

    def test_absolute_expiry_blocks_renewal(self) -> None:
        past = datetime.now(timezone.utc) - timedelta(minutes=1)
        with self.assertRaises(ValueError):
            create_access_token({"sub": "u"}, session_exp=past)

    def test_legacy_token_without_session_exp_derives_from_iat(self) -> None:
        iat = datetime(2026, 1, 1, tzinfo=timezone.utc)
        exp = resolve_session_exp({"iat": iat, "user_role": "tenant_admin"})
        self.assertGreater(exp, iat)

    def test_assert_session_expired(self) -> None:
        payload = {"session_exp": datetime.now(timezone.utc) - timedelta(seconds=5)}
        with self.assertRaises(ValueError):
            assert_session_not_absolutely_expired(payload)


class DepsAccessGateUnitTest(unittest.TestCase):
    def test_me_path_rejects_mfa_challenge(self) -> None:
        token = create_access_token({"sub": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}, minutes=5, token_use=TOKEN_USE_MFA_CHALLENGE)
        with self.assertRaises(HTTPException) as exc:
            deps.resolve_access_session(f"Bearer {token}")
        self.assertEqual(exc.exception.status_code, 401)
        self.assertEqual(exc.exception.detail["error"], "mfa_required")

    def test_me_path_rejects_mfa_setup(self) -> None:
        token = create_access_token({"sub": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}, minutes=5, token_use=TOKEN_USE_MFA_SETUP)
        with self.assertRaises(HTTPException) as exc:
            deps.resolve_access_session(f"Bearer {token}")
        self.assertEqual(exc.exception.detail["error"], "mfa_required")

    def test_access_token_requires_sub(self) -> None:
        token = create_access_token({"user_role": "tenant_admin"}, token_use=TOKEN_USE_ACCESS)
        # create always needs sub for session — empty sub after decode
        bad = decode_token(token)
        bad.pop("sub", None)
        # forge by patching decode
        with patch.object(deps, "decode_token", return_value={"token_use": TOKEN_USE_ACCESS, "session_exp": datetime.now(timezone.utc) + timedelta(hours=1)}):
            with self.assertRaises(HTTPException) as exc:
                deps.resolve_access_session("Bearer x")
        self.assertEqual(exc.exception.detail["error"], "invalid_token")

    def test_password_change_revokes_previous_access_token(self) -> None:
        uid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        token = create_access_token({"sub": uid, "user_role": "tenant_admin"}, token_use=TOKEN_USE_ACCESS)
        payload = decode_token(token)
        changed = _as_future(payload["iat"], seconds=10)

        session = {
            "sub": uid,
            "must_change_password": False,
            "password_changed_at": changed,
        }
        with patch.object(deps.repos_auth, "get_session_context", return_value=session):
            with self.assertRaises(HTTPException) as exc:
                deps.resolve_access_session(f"Bearer {token}")
        self.assertEqual(exc.exception.detail["error"], "token_revoked")

    def test_refresh_style_reload_keeps_live_permissions(self) -> None:
        uid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        token = create_access_token({"sub": uid, "user_role": "tenant_admin", "id_empresa": 1}, token_use=TOKEN_USE_ACCESS)
        session = {
            "sub": uid,
            "user_role": "tenant_admin",
            "role": "OWNER",
            "must_change_password": False,
            "password_changed_at": None,
            "allowed_screens": ["dashboard_home"],
        }
        with patch.object(deps.repos_auth, "get_session_context", return_value=session) as get_session:
            out, payload = deps.resolve_access_session(f"Bearer {token}")
        get_session.assert_called_once()
        self.assertEqual(get_session.call_args.kwargs.get("include_default_scope"), False)
        self.assertEqual(out["allowed_screens"], ["dashboard_home"])
        self.assertEqual(classify_token_use(payload), TOKEN_USE_ACCESS)

    def test_resolve_access_session_default_skips_product_scope(self) -> None:
        uid = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        token = create_access_token({"sub": uid, "user_role": "tenant_admin"}, token_use=TOKEN_USE_ACCESS)
        session = {"sub": uid, "must_change_password": False, "password_changed_at": None}
        with patch.object(deps.repos_auth, "get_session_context", return_value=session) as get_session:
            deps.resolve_access_session(f"Bearer {token}")
        self.assertEqual(get_session.call_args.kwargs.get("include_default_scope"), False)

    def test_get_current_claims_skips_product_scope_hotpath(self) -> None:
        from unittest.mock import MagicMock

        request = MagicMock()
        session = {
            "sub": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
            "must_change_password": False,
            "allowed_screens": ["dashboard_home"],
        }
        with patch.object(
            deps,
            "resolve_access_session",
            return_value=(session, {"token_use": TOKEN_USE_ACCESS}),
        ) as resolve:
            out = deps.get_current_claims(request, authorization="Bearer x")
        resolve.assert_called_once()
        self.assertEqual(resolve.call_args.kwargs.get("include_default_scope"), False)
        self.assertEqual(out["allowed_screens"], ["dashboard_home"])

    def test_missing_bearer(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            deps.resolve_access_session(None)
        self.assertEqual(exc.exception.detail["error"], "missing_bearer")

    def test_kiosk_absolute_at_least_24h(self) -> None:
        token = create_access_token({"sub": "k", "user_role": "tenant_kiosk"}, minutes=1440, token_use=TOKEN_USE_ACCESS)
        payload = decode_token(token)
        exp = resolve_session_exp(payload)
        iat = datetime.fromtimestamp(payload["iat"], tz=timezone.utc) if isinstance(payload["iat"], (int, float)) else payload["iat"]
        if getattr(iat, "tzinfo", None) is None:
            iat = iat.replace(tzinfo=timezone.utc)
        self.assertGreaterEqual((exp - iat).total_seconds(), 24 * 3600 - 1)


def _as_future(iat_value, *, seconds: int) -> datetime:
    if isinstance(iat_value, (int, float)):
        base = datetime.fromtimestamp(float(iat_value), tz=timezone.utc)
    else:
        base = iat_value if iat_value.tzinfo else iat_value.replace(tzinfo=timezone.utc)
    return base + timedelta(seconds=seconds)


class ConcurrentRefreshUnitTest(unittest.TestCase):
    def test_repeated_refresh_cannot_extend_absolute_cap(self) -> None:
        token = create_access_token({"sub": "u", "user_role": "tenant_admin"}, minutes=5, token_use=TOKEN_USE_ACCESS)
        payload = decode_token(token)
        original_exp = resolve_session_exp(payload)
        for _ in range(5):
            token = create_access_token(
                {"sub": "u", "user_role": "tenant_admin"},
                minutes=5,
                token_use=TOKEN_USE_ACCESS,
                session_exp=original_exp,
            )
            payload = decode_token(token)
            self.assertEqual(resolve_session_exp(payload), original_exp)


if __name__ == "__main__":
    unittest.main()
