from __future__ import annotations

from datetime import date, datetime
import json
from decimal import Decimal
from typing import Dict, Iterable, List


def _sanitize_value(obj):
    if isinstance(obj, str):
        return obj.replace("\x00", "")
    if isinstance(obj, list):
        return [_sanitize_value(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _sanitize_value(value) for key, value in obj.items()}
    return obj


def _default_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def to_ndjson_lines(rows: Iterable[Dict]) -> List[str]:
    return [json.dumps(_sanitize_value(row), ensure_ascii=False, default=_default_serializer) for row in rows]


def to_ndjson_bytes(rows: Iterable[Dict]) -> bytes:
    lines = to_ndjson_lines(rows)
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")
