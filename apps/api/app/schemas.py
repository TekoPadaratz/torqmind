from __future__ import annotations

from pydantic import BaseModel
from typing import Optional, Literal, List

Role = Literal["MASTER", "OWNER", "MANAGER"]


class LoginRequest(BaseModel):
    """Login payload.

    PT-BR:
      - Não usamos EmailStr aqui de propósito.
      - Alguns domínios internos (.local) falham na validação do email-validator,
        gerando erro 422 e quebrando o login no front.

    EN:
      - We intentionally avoid EmailStr.
      - Internal domains (.local) can fail email-validator, producing 422 errors.
    """

    email: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: Role
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None


class DashboardKpisResponse(BaseModel):
    faturamento: float = 0
    margem: float = 0
    ticket_medio: float = 0
    itens: int = 0


class DashboardSeriesPoint(BaseModel):
    data_key: int
    id_filial: int
    faturamento: float
    margem: float


class DashboardSeriesResponse(BaseModel):
    points: List[DashboardSeriesPoint]


class InsightsPoint(BaseModel):
    data_key: int
    id_filial: int
    faturamento_dia: float
    faturamento_mes_acum: float
    comparativo_mes_anterior: float


class InsightsResponse(BaseModel):
    points: List[InsightsPoint]
