"""Debezium event parsing and extraction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
    data_key = _extract_data_key(record)

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


def _extract_data_key(record: dict[str, Any] | None) -> int:
    """Extract YYYYMMDD from DW data_key or from STG event timestamps."""
    direct = _extract_int(record, "data_key")
    if direct > 0:
        return direct
    if not record:
        return 0

    for field in ("data_key_shadow", "DATA_KEY", "dataKey"):
        val = _extract_int(record, field)
        if val > 0:
            return val

    payload = record.get("payload")
    if isinstance(payload, str):
        try:
            payload = orjson.loads(payload)
        except (orjson.JSONDecodeError, TypeError):
            payload = {}
    if not isinstance(payload, dict):
        payload = {}

    candidates = [
        record.get("dt_evento"),
        record.get("datarepl"),
        payload.get("TORQMIND_DT_EVENTO"),
        payload.get("DT_EVENTO"),
        payload.get("DATAHORA"),
        payload.get("DATA"),
        payload.get("DTHR"),
        payload.get("DATAREPL"),
    ]
    for candidate in candidates:
        key = _date_key_from_any(candidate)
        if key > 0:
            return key
    return 0


def _date_key_from_any(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, datetime):
        return int(value.strftime("%Y%m%d"))
    if isinstance(value, (int, float)):
        # Debezium may expose timestamps as millis or micros depending on type.
        raw = float(value)
        if raw <= 0:
            return 0
        if raw > 10_000_000_000_000:
            raw = raw / 1_000_000
        elif raw > 10_000_000_000:
            raw = raw / 1_000
        try:
            return int(datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y%m%d"))
        except (OverflowError, OSError, ValueError):
            return 0

    text = str(value).strip()
    if not text:
        return 0
    if len(text) >= 8 and text[:8].isdigit():
        return int(text[:8])

    normalized = text.replace("Z", "+00:00")
    for parser in (
        lambda s: datetime.fromisoformat(s),
        lambda s: datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S"),
        lambda s: datetime.strptime(s[:10], "%Y-%m-%d"),
        lambda s: datetime.strptime(s[:10], "%d/%m/%Y"),
    ):
        try:
            parsed = parser(normalized)
            return int(parsed.strftime("%Y%m%d"))
        except (TypeError, ValueError):
            continue
    return 0
