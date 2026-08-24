from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.main import _client_ip


class LoginClientIpTest(unittest.TestCase):
    def test_prefers_x_forwarded_for_first_hop(self) -> None:
        request = SimpleNamespace(
            headers={"x-forwarded-for": "177.55.57.5, 172.18.0.1"},
            client=SimpleNamespace(host="172.18.0.1"),
        )
        self.assertEqual(_client_ip(request), "177.55.57.5")

    def test_falls_back_to_direct_client_host(self) -> None:
        request = SimpleNamespace(
            headers={},
            client=SimpleNamespace(host="127.0.0.1"),
        )
        self.assertEqual(_client_ip(request), "127.0.0.1")
