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
        if obj == obj.to_integral_value():
            return int(obj)
        return float(obj)
    return str(obj)


def _normalize_row_for_json(row: Dict) -> Dict:
    """Evita IDs floatish no payload (282384.0), que quebram etl.safe_int/CH joins."""
    out: Dict = {}
    for key, value in row.items():
        if isinstance(value, bool) or value is None:
            out[key] = value
        elif isinstance(value, float) and value.is_integer():
            out[key] = int(value)
        elif isinstance(value, Decimal) and value == value.to_integral_value():
            out[key] = int(value)
        else:
            out[key] = value
    return out


def to_ndjson_lines(rows: Iterable[Dict]) -> List[str]:
    return [
        json.dumps(_normalize_row_for_json(row), ensure_ascii=False, default=_default_serializer)
        for row in rows
    ]


def to_ndjson_bytes(rows: Iterable[Dict]) -> bytes:
    lines = to_ndjson_lines(rows)
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")
