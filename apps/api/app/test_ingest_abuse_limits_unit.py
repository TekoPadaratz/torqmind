"""Prompt 5 — ingest streaming limits + client IP trust + shared attempt buckets.

Synthetic stream tests (no live PG/nginx). Does not prove Hom/Prod readiness.
"""
from __future__ import annotations

import gzip
import io
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from app import security_attempts
from app.client_ip import client_ip_for_rate_limit, peer_ip
from app.config import settings
from app.routes_ingest import IngestStreamLimitError, _stream_ndjson_objects


class _FakeRequest:
    def __init__(self, chunks: list[bytes], headers: dict | None = None):
        self._chunks = chunks
        self.headers = headers or {}
        self.client = MagicMock(host="10.0.0.9")

    async def stream(self):
        for chunk in self._chunks:
            yield chunk


async def _collect(request, is_gzip: bool = False):
    out = []
    async for obj in _stream_ndjson_objects(request, is_gzip=is_gzip):
        out.append(obj)
    return out


class ClientIpTrustTests(unittest.TestCase):
    def test_default_hops_zero_ignores_spoofed_xff(self) -> None:
        req = MagicMock()
        req.client = MagicMock(host="172.18.0.1")
        req.headers = {"x-forwarded-for": "1.2.3.4, 5.6.7.8"}
        with patch.object(settings, "api_trusted_proxy_hops", 0):
            self.assertEqual(client_ip_for_rate_limit(req), "peer:172.18.0.1")
            self.assertEqual(peer_ip(req), "172.18.0.1")

    def test_hops_one_uses_rightmost_client_not_left_spoof(self) -> None:
        req = MagicMock()
        req.client = MagicMock(host="172.18.0.1")
        # Attacker prepends 9.9.9.9; nginx appends real client 203.0.113.10
        req.headers = {"x-forwarded-for": "9.9.9.9, 203.0.113.10"}
        with patch.object(settings, "api_trusted_proxy_hops", 1):
            # hops=1 → take parts[-1] when nginx replaced? 
            # len=2, idx = 2-1 = 1 → 203.0.113.10
            self.assertEqual(client_ip_for_rate_limit(req), "xff:203.0.113.10")


class SecurityAttemptsReuseTests(unittest.TestCase):
    def setUp(self) -> None:
        security_attempts.reset_memory_store_for_tests()
        self._env = patch.object(security_attempts.settings, "app_env", "test")
        self._env.start()

    def tearDown(self) -> None:
        self._env.stop()
        security_attempts.reset_memory_store_for_tests()

    def test_shared_bucket_keys_namespaced(self) -> None:
        k1 = security_attempts.bucket_key("auth", "login", "id", "abc")
        k2 = security_attempts.bucket_key("ingest", "post", 1)
        k3 = security_attempts.bucket_key("mfa", "verify", "u1")
        self.assertTrue(k1.startswith("auth:"))
        self.assertTrue(k2.startswith("ingest:"))
        self.assertTrue(k3.startswith("mfa:"))
        security_attempts.record_attempt(k1, window_seconds=60)
        self.assertTrue(security_attempts.is_rate_limited(k1, max_attempts=1, window_seconds=60))
        self.assertFalse(security_attempts.is_rate_limited(k2, max_attempts=1, window_seconds=60))


class IngestStreamLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path_chunk_split(self) -> None:
        line = json.dumps({"a": 1}, ensure_ascii=False).encode() + b"\n"
        # Split across chunks mid-line
        req = _FakeRequest([line[:5], line[5:]])
        rows = await _collect(req)
        self.assertEqual(rows, [{"a": 1}])

    async def test_invalid_json(self) -> None:
        req = _FakeRequest([b"{not-json}\n"])
        with self.assertRaises(HTTPException) as ctx:
            await _collect(req)
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_line_without_newline_at_eof(self) -> None:
        req = _FakeRequest([b'{"a":2}'])
        rows = await _collect(req)
        self.assertEqual(rows, [{"a": 2}])

    async def test_gzip_high_expansion_rejected(self) -> None:
        # Tiny gzip of highly repetitive payload → expansion bomb ratio.
        raw = (b'{"x":"' + (b"A" * 200_000) + b'"}\n')
        compressed = gzip.compress(raw)
        self.assertGreater(len(raw) / max(len(compressed), 1), 20)
        req = _FakeRequest([compressed], headers={"content-encoding": "gzip"})
        with patch.object(settings, "ingest_max_gzip_expansion_ratio", 10.0):
            with patch.object(settings, "ingest_max_decoded_bytes", 50_000_000):
                with patch.object(settings, "ingest_max_wire_bytes", 50_000_000):
                    with self.assertRaises(IngestStreamLimitError) as ctx:
                        await _collect(req, is_gzip=True)
        self.assertIn(ctx.exception.detail["error"], {"ingest_gzip_bomb", "ingest_decoded_too_large"})

    async def test_partial_line_too_large(self) -> None:
        huge = b"{" + (b"a" * 1000)
        req = _FakeRequest([huge])
        with patch.object(settings, "ingest_max_line_bytes", 100):
            with self.assertRaises(IngestStreamLimitError) as ctx:
                await _collect(req)
        self.assertEqual(ctx.exception.detail["error"], "ingest_line_too_large")

    async def test_too_many_records(self) -> None:
        body = b"".join(json.dumps({"i": i}).encode() + b"\n" for i in range(10))
        req = _FakeRequest([body])
        with patch.object(settings, "ingest_max_records_per_request", 3):
            with self.assertRaises(IngestStreamLimitError) as ctx:
                await _collect(req)
        self.assertEqual(ctx.exception.detail["error"], "ingest_too_many_records")

    async def test_wire_budget(self) -> None:
        req = _FakeRequest([b"x" * 200])
        with patch.object(settings, "ingest_max_wire_bytes", 50):
            with self.assertRaises(IngestStreamLimitError) as ctx:
                await _collect(req)
        self.assertEqual(ctx.exception.detail["error"], "ingest_wire_too_large")

    async def test_disconnect_stops_iteration(self) -> None:
        """Simulated client abort: stream ends mid-body without final newline."""
        req = _FakeRequest([b'{"ok":1}\n', b'{"partial":'])
        with patch.object(settings, "ingest_max_line_bytes", 10_000):
            with self.assertRaises(HTTPException):
                # incomplete JSON at EOF → 400 invalid NDJSON (honest disconnect handling)
                await _collect(req)


if __name__ == "__main__":
    unittest.main()
