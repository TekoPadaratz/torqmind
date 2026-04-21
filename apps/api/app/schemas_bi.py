from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

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
