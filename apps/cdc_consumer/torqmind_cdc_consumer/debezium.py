"""Debezium event parsing and extraction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import orjson


@dataclass(slots=True)
class DebeziumEvent:
    """Parsed Debezium CDC event."""

    topic: str
    partition: int
    offset: int
    op: str  # c, u, d, r
    source_ts_ms: int
    table_schema: str
    table_name: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    key: dict[str, Any]
    id_empresa: int
    data_key: int


def parse_debezium_event(
    topic: str,
    partition: int,
    offset: int,
    key_bytes: bytes | None,
    value_bytes: bytes | None,
) -> DebeziumEvent | None:
    """Parse a raw Kafka message into a DebeziumEvent.

    Returns None if the message is a tombstone or unparseable.
    """
    if value_bytes is None:
        return None

    try:
        value = orjson.loads(value_bytes)
    except (orjson.JSONDecodeError, TypeError):
        return None

    payload = value.get("payload") or value
    source = payload.get("source", {})

    op = payload.get("op", "")
    if not op:
        return None

    before = payload.get("before")
    after = payload.get("after")

    # Extract table info from source metadata
    table_schema = source.get("schema", "")
    table_name = source.get("table", "")
    source_ts_ms = source.get("ts_ms", 0)

    # Parse key
    key: dict[str, Any] = {}
    if key_bytes:
        try:
            key_parsed = orjson.loads(key_bytes)
            key = key_parsed.get("payload", key_parsed) if isinstance(key_parsed, dict) else {}
        except (orjson.JSONDecodeError, TypeError):
            pass

    # Extract id_empresa and data_key from the active record
    record = after if after else before
    id_empresa = _extract_int(record, "id_empresa")
    data_key = _extract_int(record, "data_key")

    return DebeziumEvent(
        topic=topic,
        partition=partition,
        offset=offset,
        op=op,
        source_ts_ms=source_ts_ms,
        table_schema=table_schema,
        table_name=table_name,
        before=before,
        after=after,
        key=key,
        id_empresa=id_empresa,
        data_key=data_key,
    )


def _extract_int(record: dict[str, Any] | None, field: str) -> int:
    """Safely extract integer field from a record."""
    if not record:
        return 0
    val = record.get(field)
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0
