"""Serialização JSON segura para contexto do assistente (Decimal, date, etc.)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def json_ready(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(v) for v in value]
    return str(value)


def dumps_json(value: Any, **kwargs: Any) -> str:
    return json.dumps(json_ready(value), ensure_ascii=False, default=str, **kwargs)
