"""Client address resolution for abuse controls (Prompt 5).

Trust model (must be proven at the edge before relying on XFF):

1. The immediate TCP peer (``request.client.host``) is always known.
2. ``X-Forwarded-For`` is attacker-controlled unless every hop from the
   client to the API is a **trusted** reverse proxy that **appends** (or
   overwrites) the chain correctly.
3. ``api_trusted_proxy_hops`` counts how many rightmost XFF entries are
   added by trusted proxies. With hops=1 (typical nginx → API), the client
   IP is the entry just left of the last hop.

Default hops=0: **do not trust XFF** for rate-limit identity. Behind Docker
published nginx this collapses many users onto the bridge gateway IP — that
is an intentional residual until ops sets hops and proves the chain. Account
/ ingest-key buckets (Postgres ``security_attempt_buckets``) remain the
cross-worker control that does not depend on XFF.
"""
from __future__ import annotations

from typing import Optional

from fastapi import Request

from app.config import settings


def peer_ip(request: Request) -> str:
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def client_ip_for_rate_limit(request: Request, *, trusted_proxy_hops: Optional[int] = None) -> str:
    """Return the address key used for IP-scoped abuse buckets.

    Never uses "the first X-Forwarded-For value" blindly (spoofable).
    """
    hops = (
        int(trusted_proxy_hops)
        if trusted_proxy_hops is not None
        else int(getattr(settings, "api_trusted_proxy_hops", 0) or 0)
    )
    peer = peer_ip(request)
    if hops <= 0:
        return f"peer:{peer}"

    raw = (request.headers.get("x-forwarded-for") or "").strip()
    if not raw:
        return f"peer:{peer}"
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return f"peer:{peer}"
    # Append model: the rightmost ``hops`` entries were added by trusted proxies.
    # Client address is at index len(parts) - hops (0-based). Example hops=1 → last entry.
    idx = len(parts) - hops
    if idx < 0 or idx >= len(parts):
        return f"peer:{peer}"
    return f"xff:{parts[idx]}"
