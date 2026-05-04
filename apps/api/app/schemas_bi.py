from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

GoalType = Literal['FATURAMENTO']

class GoalTargetRequest(BaseModel):
    target_value: float = Field(..., gt=0)
    goal_month: Optional[date] = None
    goal_type: GoalType = 'FATURAMENTO'

    @field_validator('goal_month', mode='before')
    @classmethod
    def normalize_goal_month(cls, value: Optional[date | str]) -> Optional[date]:
        if value is None:
            return None
        parsed = date.fromisoformat(str(value)) if isinstance(value, str) else value
        return parsed.replace(day=1)


# ------------------------------------------------------------------
# BI Overview response models (typed envelope, flexible payload)
# ------------------------------------------------------------------

def _sanitize_nan(obj: Any) -> Any:
    """Recursively replace NaN/Inf float values with None (JSON-safe)."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]
    return obj


class CacheMetadata(BaseModel):
    cached: bool = False
    cached_at: Optional[datetime] = None
    scope_key: Optional[str] = None

    model_config = {"extra": "allow"}


class DashboardHomeResponse(CacheMetadata):
    kpis: Dict[str, Any] = Field(default_factory=dict)
    alerts: List[Dict[str, Any]] = Field(default_factory=list)
    series: Dict[str, Any] = Field(default_factory=dict)
    insights: Optional[Dict[str, Any]] = None

    model_config = {"extra": "allow"}

    @field_validator("series", mode="before")
    @classmethod
    def coerce_series(cls, v: Any) -> Dict[str, Any]:
        if isinstance(v, list):
            return {}
        return v if isinstance(v, dict) else {}

    @model_validator(mode="before")
    @classmethod
    def sanitize_payload(cls, values: Any) -> Any:
        if isinstance(values, dict):
            return _sanitize_nan(values)
        return values


class SalesOverviewResponse(CacheMetadata):
    kpis: Dict[str, Any] = Field(default_factory=dict)
    series: Dict[str, Any] = Field(default_factory=dict)
    ranking: List[Dict[str, Any]] = Field(default_factory=list)
    filters: Optional[Dict[str, Any]] = None

    model_config = {"extra": "allow"}

    @field_validator("series", mode="before")
    @classmethod
    def coerce_series(cls, v: Any) -> Dict[str, Any]:
        if isinstance(v, list):
            return {}
        return v if isinstance(v, dict) else {}

    @model_validator(mode="before")
    @classmethod
    def sanitize_payload(cls, values: Any) -> Any:
        if isinstance(values, dict):
            return _sanitize_nan(values)
        return values


class CashOverviewResponse(CacheMetadata):
    kpis: Dict[str, Any] = Field(default_factory=dict)
    series: Dict[str, Any] = Field(default_factory=dict)
    turnos: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    @field_validator("series", mode="before")
    @classmethod
    def coerce_series(cls, v: Any) -> Dict[str, Any]:
        if isinstance(v, list):
            return {}
        return v if isinstance(v, dict) else {}

    @model_validator(mode="before")
    @classmethod
    def sanitize_payload(cls, values: Any) -> Any:
        if isinstance(values, dict):
            return _sanitize_nan(values)
        return values


class FraudOverviewResponse(CacheMetadata):
    kpis: Dict[str, Any] = Field(default_factory=dict)
    risk_events: List[Dict[str, Any]] = Field(default_factory=list)
    series: Dict[str, Any] = Field(default_factory=dict)
    risk_coverage: Optional[Dict[str, Any]] = Field(default=None, alias="model_coverage")

    model_config = {"extra": "allow", "populate_by_name": True}

    @field_validator("series", mode="before")
    @classmethod
    def coerce_series(cls, v: Any) -> Dict[str, Any]:
        if isinstance(v, list):
            return {}
        return v if isinstance(v, dict) else {}

    @model_validator(mode="before")
    @classmethod
    def sanitize_payload(cls, values: Any) -> Any:
        if isinstance(values, dict):
            return _sanitize_nan(values)
        return values


class FinanceOverviewResponse(CacheMetadata):
    kpis: Dict[str, Any] = Field(default_factory=dict)
    aging: Dict[str, Any] = Field(default_factory=dict)
    series: Dict[str, Any] = Field(default_factory=dict)
    definitions: Optional[Dict[str, Any]] = None

    model_config = {"extra": "allow"}

    @field_validator("series", mode="before")
    @classmethod
    def coerce_series(cls, v: Any) -> Dict[str, Any]:
        if isinstance(v, list):
            return {}
        return v if isinstance(v, dict) else {}

    @model_validator(mode="before")
    @classmethod
    def sanitize_payload(cls, values: Any) -> Any:
        if isinstance(values, dict):
            return _sanitize_nan(values)
        return values
