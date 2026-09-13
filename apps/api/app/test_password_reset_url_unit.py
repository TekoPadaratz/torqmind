"""URL pública dos links de recuperação de senha — só o host/path, sem e-mail nem token real."""
from __future__ import annotations

from unittest.mock import patch

from app.routes_auth import _build_reset_url


def test_build_reset_url_uses_web_public_url_and_path():
    with patch("app.routes_auth.settings") as settings:
        settings.web_public_url = "https://www.torqmind.com.br/"
        settings.password_reset_link_path = "reset-password"
        url = _build_reset_url("probe-token")
    assert url.startswith("https://www.torqmind.com.br/reset-password?token=")
    assert "redevr.ddns.me" not in url
    assert url.endswith("probe-token")


def test_build_reset_url_keeps_hom_domain():
    with patch("app.routes_auth.settings") as settings:
        settings.web_public_url = "https://hom.torqmind.com.br"
        settings.password_reset_link_path = "/reset-password"
        url = _build_reset_url("probe-token")
    assert url.startswith("https://hom.torqmind.com.br/reset-password?token=")
