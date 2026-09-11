from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.client_ip import client_ip_for_rate_limit, peer_ip
from app.config import settings


class LoginClientIpTest(unittest.TestCase):
    """Client IP for abuse controls — never blind first-XFF trust (Prompt 5)."""

    def test_default_hops_zero_uses_peer_not_spoofed_xff(self) -> None:
        request = SimpleNamespace(
            headers={"x-forwarded-for": "177.55.57.5, 172.18.0.1"},
            client=SimpleNamespace(host="172.18.0.1"),
        )
        with patch.object(settings, "api_trusted_proxy_hops", 0):
            self.assertEqual(peer_ip(request), "172.18.0.1")
            self.assertEqual(client_ip_for_rate_limit(request), "peer:172.18.0.1")

    def test_falls_back_to_direct_client_host(self) -> None:
        request = SimpleNamespace(
            headers={},
            client=SimpleNamespace(host="127.0.0.1"),
        )
        with patch.object(settings, "api_trusted_proxy_hops", 0):
            self.assertEqual(client_ip_for_rate_limit(request), "peer:127.0.0.1")

    def test_hops_one_uses_rightmost_trusted_client(self) -> None:
        request = SimpleNamespace(
            headers={"x-forwarded-for": "9.9.9.9, 203.0.113.10"},
            client=SimpleNamespace(host="172.18.0.1"),
        )
        with patch.object(settings, "api_trusted_proxy_hops", 1):
            self.assertEqual(client_ip_for_rate_limit(request), "xff:203.0.113.10")
