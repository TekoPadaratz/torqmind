"""Additive Ingest Key migration plan (Prompt 8) — **not applied** to Hom/Prod.

Current consumers (plaintext UUID in ``app.tenants.ingest_key``):
- ``routes_ingest`` — ``X-Ingest-Key`` header
- ``routes_agent_update`` — manifest/download/hello
- ``routes_etl`` — micro_risk optional ingest path
- Agent sink + auto-update client

Goal: stop storing/comparing only the raw key long-term, **without regenerating**
existing keys or breaking agents mid-field.

Proposed additive columns (future migration — do not invent here):
- ``ingest_key_hash`` TEXT — SHA-256 hex of the UUID string (lookup)
- ``ingest_hmac_secret`` TEXT NULL — per-tenant secret for request signing (optional)

Verification order (when enabled behind a flag):
1. Accept legacy ``X-Ingest-Key: <uuid>`` and resolve via hash or existing column.
2. Accept ``X-Ingest-Key-Id: <tenant|key-id>`` + ``X-Ingest-Signature`` HMAC over
   method+path+body-sha256 with ``ingest_hmac_secret`` (agent-side later).
3. Never log the raw key; never return it except platform_master create flows.

This module only documents the contract. Call sites continue using the current
UUID compare until a reviewed SQL migration + dual-read deploy.
"""
from __future__ import annotations

import hashlib
from typing import Optional


def hash_ingest_key(raw_key: str) -> str:
    """Stable hash for future indexed lookup (does not replace current column)."""
    return hashlib.sha256(str(raw_key or "").strip().encode("utf-8")).hexdigest()


def migration_status() -> dict:
    return {
        "status": "proposed_not_applied",
        "regenerate_existing_keys": False,
        "dual_accept_legacy_header": True,
        "next_steps": [
            "review SQL additive columns",
            "backfill ingest_key_hash from existing ingest_key",
            "deploy API dual-read",
            "agent optional HMAC signing after update channel HMAC is live",
        ],
    }


def redact_ingest_key(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if len(text) <= 8:
        return "***"
    return text[:4] + "…" + text[-4:]
