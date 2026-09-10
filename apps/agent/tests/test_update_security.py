"""Ed25519 authenticity + secure-mode update policy tests (ephemeral keys only)."""

from __future__ import annotations

import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.transport import (  # noqa: E402
    TransportPolicyError,
    assert_download_origin_allowed,
    assert_secure_transport,
    strip_auth_if_foreign_host,
)
from agent.update.manifest import ReleaseManifest  # noqa: E402
from agent.update.policy import (  # noqa: E402
    UpdatePolicyError,
    assert_not_unauthorized_rollback,
    effective_floor_version,
    enforce_signature_policy,
    record_successful_stage,
    write_floor_version,
)
from agent.update.signing import (  # noqa: E402
    TrustedPublicKey,
    canonical_signed_bytes,
    generate_ephemeral_keypair_for_tests,
    sign_canonical,
)


def _window(days_back: int = 0, days_fwd: int = 7):
    now = datetime.now(timezone.utc)
    nbf = (now - timedelta(days=days_back)).isoformat().replace("+00:00", "Z")
    exp = (now + timedelta(days=days_fwd)).isoformat().replace("+00:00", "Z")
    return nbf, exp


class Ed25519SecureModeTests(unittest.TestCase):
    def setUp(self):
        self.priv_b64, self.pub_b64, self.private = generate_ephemeral_keypair_for_tests()
        self.key_id = "test-key-a"
        self.trusted = [
            TrustedPublicKey(key_id=self.key_id, public_key_b64=self.pub_b64, status="active")
        ]
        self.nbf, self.exp = _window()
        self.base = {
            "product": "torqmind-agent",
            "channel": "stable",
            "key_id": self.key_id,
            "version": "2.0.13",
            "sha256": "a" * 64,
            "size": 100,
            "url": "https://www.torqmind.com.br/api/agent/update/download/2.0.13",
            "valid_not_before": self.nbf,
            "valid_not_after": self.exp,
        }

    def _sign(self, data: dict) -> ReleaseManifest:
        payload = canonical_signed_bytes(
            product=data["product"],
            channel=data["channel"],
            key_id=data["key_id"],
            version=data["version"],
            sha256=data["sha256"],
            size=data["size"],
            url=data["url"],
            valid_not_before=data["valid_not_before"],
            valid_not_after=data["valid_not_after"],
        )
        data = dict(data)
        data["signature_ed25519"] = sign_canonical(self.private, payload)
        return ReleaseManifest.from_dict(data)

    def test_secure_accepts_valid(self):
        m = self._sign(self.base)
        enforce_signature_policy(m, secure_mode=True, trusted_keys=self.trusted)

    def test_secure_rejects_unsigned(self):
        m = ReleaseManifest.from_dict(self.base)
        with self.assertRaises(UpdatePolicyError):
            enforce_signature_policy(m, secure_mode=True, trusted_keys=self.trusted)

    def test_secure_no_hmac_fallback(self):
        m = ReleaseManifest.from_dict(
            {**self.base, "signature_hmac_sha256": "not-a-real-hmac"}
        )
        with self.assertRaises(UpdatePolicyError) as ctx:
            enforce_signature_policy(
                m, secure_mode=True, trusted_keys=self.trusted, hmac_key="anything"
            )
        self.assertIn("signature_ed25519", str(ctx.exception))

    def test_tamper_each_bound_field(self):
        m = self._sign(self.base)
        for field, value in [
            ("version", "9.9.9"),
            ("sha256", "b" * 64),
            ("size", 999),
            ("url", "https://www.torqmind.com.br/api/agent/update/download/evil"),
            ("channel", "evil"),
            ("product", "other"),
            ("key_id", "other-key"),
            ("valid_not_after", self.exp.replace("Z", "")),  # still may parse; use far past
        ]:
            data = {**self.base, "signature_ed25519": m.signature_ed25519}
            if field == "valid_not_after":
                data["valid_not_after"] = "2000-01-01T00:00:00Z"
            else:
                data[field] = value
            bad = ReleaseManifest.from_dict(data)
            with self.assertRaises(UpdatePolicyError):
                enforce_signature_policy(bad, secure_mode=True, trusted_keys=self.trusted)

    def test_wrong_key(self):
        m = self._sign(self.base)
        _, other_pub, _ = generate_ephemeral_keypair_for_tests()
        wrong = [TrustedPublicKey(key_id=self.key_id, public_key_b64=other_pub, status="active")]
        with self.assertRaises(UpdatePolicyError):
            enforce_signature_policy(m, secure_mode=True, trusted_keys=wrong)

    def test_unknown_key_id(self):
        m = self._sign(self.base)
        with self.assertRaises(UpdatePolicyError):
            enforce_signature_policy(
                m,
                secure_mode=True,
                trusted_keys=[
                    TrustedPublicKey(key_id="other", public_key_b64=self.pub_b64, status="active")
                ],
            )

    def test_expired(self):
        nbf, _ = _window(days_back=10, days_fwd=-1)
        data = {**self.base, "valid_not_before": nbf, "valid_not_after": "2000-01-01T00:00:00Z"}
        m = self._sign(data)
        with self.assertRaises(UpdatePolicyError):
            enforce_signature_policy(m, secure_mode=True, trusted_keys=self.trusted)

    def test_replay_old_version_blocked_by_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            updates = Path(tmp)
            record_successful_stage(updates, "2.0.13", key_id=self.key_id)
            floor = effective_floor_version(updates, "2.0.13")
            with self.assertRaises(UpdatePolicyError):
                assert_not_unauthorized_rollback(
                    "2.0.12", local_version="2.0.13", floor_version=floor
                )

    def test_floor_deletion_not_authorized_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            updates = Path(tmp)
            record_successful_stage(updates, "2.0.13", key_id=self.key_id)
            # Attacker deletes floor file only — last_applied + local still protect.
            (updates / "floor_version.json").unlink()
            floor = effective_floor_version(updates, "2.0.13")
            self.assertEqual(floor, "2.0.13")
            with self.assertRaises(UpdatePolicyError):
                assert_not_unauthorized_rollback(
                    "2.0.10", local_version="2.0.13", floor_version=floor
                )

    def test_legacy_unsigned_allowed_outside_secure(self):
        m = ReleaseManifest.from_dict(self.base)
        enforce_signature_policy(m, secure_mode=False, trusted_keys=[])


class OriginAndTlsTests(unittest.TestCase):
    def test_origin_scheme_host_port(self):
        assert_download_origin_allowed(
            "https://www.torqmind.com.br/api/agent/update/download/2.0.13",
            api_base_url="https://www.torqmind.com.br/api",
        )
        with self.assertRaises(TransportPolicyError):
            assert_download_origin_allowed(
                "http://www.torqmind.com.br/api/agent/update/download/2.0.13",
                api_base_url="https://www.torqmind.com.br/api",
            )

    def test_private_dns_does_not_authorize_ingest_key(self):
        # Same private IP hostname mismatch / different authority → strip key.
        headers = {"X-Ingest-Key": "uuid", "X-Agent-Version": "2.0.13"}
        out = strip_auth_if_foreign_host(
            headers,
            request_url="https://evil.internal/api/x",
            api_base_url="https://www.torqmind.com.br/api",
        )
        self.assertNotIn("X-Ingest-Key", out)

    def test_secure_tls_required(self):
        with self.assertRaises(TransportPolicyError):
            assert_secure_transport("https://www.torqmind.com.br/api", tls_verify=False)
        with self.assertRaises(TransportPolicyError):
            assert_secure_transport("http://172.30.0.10/api", tls_verify=True)


class DownloadRedirectTests(unittest.TestCase):
    def test_redirect_refused(self):
        from agent.update.downloader import UpdateDownloadError, download_release

        manifest = ReleaseManifest.from_dict(
            {
                "version": "2.0.13",
                "sha256": "a" * 64,
                "size": 4,
                "url": "https://www.torqmind.com.br/api/agent/update/download/2.0.13",
            }
        )
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 302
        resp.is_redirect = True
        resp.headers = {"Location": "http://evil/x"}
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        session.get.return_value = resp

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(UpdateDownloadError):
                download_release(
                    manifest,
                    dest_dir=Path(tmp),
                    api_base_url="https://www.torqmind.com.br/api",
                    session=session,
                    secure_mode=True,
                    tls_verify=True,
                )


if __name__ == "__main__":
    unittest.main()
