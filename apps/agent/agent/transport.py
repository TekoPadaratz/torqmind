"""Transport policy for Agent → API (HTTPS, LAN HTTP, no silent downgrade).

Gradual transition (Prompt 8):
- Default still allows HTTP for LAN/postos that hit ``172.30.0.10`` / private IPs.
- HTTPS uses certificate validation (``tls_verify=True``) unless explicitly disabled.
- Never silently follow HTTPS→HTTP redirects or leak ``X-Ingest-Key`` off the API host.
"""
from __future__ import annotations

import ipaddress
import socket
from typing import Any, Mapping, MutableMapping, Optional
from urllib.parse import urlparse, urlunparse

import requests


class TransportPolicyError(ValueError):
    """Raised when a URL violates agent transport policy."""


def _hostname(url: str) -> str:
    return (urlparse(str(url or "").strip()).hostname or "").lower()


def _scheme(url: str) -> str:
    return (urlparse(str(url or "").strip()).scheme or "").lower()


def is_private_or_loopback_host(host: str) -> bool:
    """True for RFC1918 / loopback / link-local (LAN agent → TorqMind App)."""
    text = (host or "").strip().lower().rstrip(".")
    if not text:
        return False
    if text in {"localhost", "localhost.localdomain"}:
        return True
    try:
        addr = ipaddress.ip_address(text)
        return bool(addr.is_private or addr.is_loopback or addr.is_link_local)
    except ValueError:
        # Hostnames: resolve only for classification hints — failure ⇒ not LAN.
        try:
            infos = socket.getaddrinfo(text, None, type=socket.SOCK_STREAM)
        except OSError:
            return False
        for info in infos:
            try:
                addr = ipaddress.ip_address(info[4][0])
            except (ValueError, IndexError, TypeError):
                continue
            if addr.is_private or addr.is_loopback or addr.is_link_local:
                return True
        return False


def authority_key(url: str) -> str:
    """scheme://host:port — comparable origin for update downloads."""
    parsed = urlparse(str(url or "").strip())
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()
    port = parsed.port
    if port is None:
        if scheme == "https":
            port = 443
        elif scheme == "http":
            port = 80
        else:
            port = 0
    return f"{scheme}://{host}:{port}"


def same_origin(url_a: str, url_b: str) -> bool:
    return authority_key(url_a) == authority_key(url_b)


def assert_no_https_downgrade(*, from_url: str, to_url: str) -> None:
    if _scheme(from_url) == "https" and _scheme(to_url) == "http":
        raise TransportPolicyError(
            f"HTTPS downgrade blocked: {from_url!r} → {to_url!r}"
        )


def assert_transport_allowed(
    url: str,
    *,
    allow_insecure_http: bool = True,
    require_https: bool = False,
) -> None:
    """Enforce scheme policy without cutting LAN HTTP by default."""
    scheme = _scheme(url)
    host = _hostname(url)
    if not scheme or not host:
        raise TransportPolicyError(f"Invalid URL (missing scheme/host): {url!r}")
    if scheme not in {"http", "https"}:
        raise TransportPolicyError(f"Unsupported URL scheme: {scheme}")
    if require_https and scheme != "https":
        raise TransportPolicyError(
            f"require_https=true rejects non-HTTPS URL: {url!r}"
        )
    if scheme == "http" and not allow_insecure_http:
        raise TransportPolicyError(
            f"HTTP blocked (allow_insecure_http=false): {url!r}"
        )
    # Soft guidance: HTTP outside LAN is discouraged but still allowed while
    # allow_insecure_http remains true (postos may use public NAT during cutover).


def strip_auth_if_foreign_host(
    headers: Mapping[str, str],
    *,
    request_url: str,
    api_base_url: str,
) -> dict[str, str]:
    """Drop ingest credentials unless scheme+hostname+port match api.base_url.

    Private DNS / same IP is **not** authorization — only the configured authority.
    """
    out = {str(k): str(v) for k, v in dict(headers or {}).items()}
    if same_origin(request_url, api_base_url):
        return out
    for key in list(out):
        lower = key.lower()
        if lower in {"x-ingest-key", "authorization", "cookie"}:
            out.pop(key, None)
    return out


def assert_download_origin_allowed(
    download_url: str,
    *,
    api_base_url: str,
    allow_foreign_download: bool = False,
) -> None:
    """Manifest download URL must match API scheme+hostname+port unless explicitly allowed."""
    assert_no_https_downgrade(from_url=api_base_url, to_url=download_url)
    if allow_foreign_download:
        return
    if not same_origin(download_url, api_base_url):
        raise TransportPolicyError(
            f"Download origin {authority_key(download_url)!r} differs from "
            f"API origin {authority_key(api_base_url)!r}"
        )


def assert_secure_transport(
    url: str,
    *,
    tls_verify: bool,
) -> None:
    """Secure update mode: HTTPS + certificate validation required."""
    if not tls_verify:
        raise TransportPolicyError("update_secure_mode requires tls_verify=true")
    assert_transport_allowed(url, allow_insecure_http=False, require_https=True)


def build_http_session(*, tls_verify: bool = True) -> requests.Session:
    session = requests.Session()
    session.verify = bool(tls_verify)
    # Never auto-follow redirects with credentials attached.
    session.max_redirects = 0
    return session


def redact_url(url: str) -> str:
    """Log-safe URL (no query secrets)."""
    parsed = urlparse(str(url or ""))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def session_request_kwargs() -> dict[str, Any]:
    return {"allow_redirects": False}
