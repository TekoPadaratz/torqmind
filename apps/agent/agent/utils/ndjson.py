from __future__ import annotations

from datetime import date, datetime
import json
from decimal import Decimal
from typing import Dict, Iterable, List

from agent.utils.timezone import business_datetime_iso


def _default_serializer(obj):
    if isinstance(obj, datetime):
        return business_datetime_iso(obj, timespec="microseconds")
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def to_ndjson_lines(rows: Iterable[Dict]) -> List[str]:
    return [json.dumps(row, ensure_ascii=False, default=_default_serializer) for row in rows]


def to_ndjson_bytes(rows: Iterable[Dict]) -> bytes:
    lines = to_ndjson_lines(rows)
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")
