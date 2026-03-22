from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field

PlatformRole = Literal[
    "platform_master",
    "platform_admin",
    "product_global",
    "channel_admin",
    "tenant_admin",
    "tenant_manager",
    "tenant_viewer",
]


class TenantUpsertRequest(BaseModel):
    nome: str = Field(min_length=2)
    cnpj: Optional[str] = None
    is_enabled: bool = True
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    status: Optional[str] = None
    billing_status: Optional[str] = None
    grace_until: Optional[date] = None
    suspended_reason: Optional[str] = None
    channel_id: Optional[int] = None
    plan_name: Optional[str] = None
    monthly_amount: Optional[Decimal] = None
    billing_day: Optional[int] = Field(default=None, ge=1, le=31)
    issue_day: Optional[int] = Field(default=None, ge=1, le=31)
    sales_history_days: Optional[int] = Field(default=None, ge=1, le=3650)
    default_product_scope_days: Optional[int] = Field(default=None, ge=1, le=365)


class BranchUpsertRequest(BaseModel):
    nome: str = Field(min_length=2)
    cnpj: Optional[str] = None
    is_enabled: bool = True
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    blocked_reason: Optional[str] = None


class UserAccessInput(BaseModel):
    role: PlatformRole
    channel_id: Optional[int] = None
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None
    is_enabled: bool = True
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None


class UserUpsertRequest(BaseModel):
    nome: str = Field(min_length=2)
    email: str = Field(min_length=3)
    password: Optional[str] = Field(default=None, min_length=8)
    role: PlatformRole
    is_enabled: bool = True
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    must_change_password: bool = False
    locked_until: Optional[datetime] = None
    reset_failed_login: bool = False
    accesses: list[UserAccessInput] = Field(default_factory=list)


class UserContactRequest(BaseModel):
    telegram_chat_id: Optional[str] = None
    telegram_username: Optional[str] = None
    telegram_enabled: bool = False
    email: Optional[str] = None
    phone: Optional[str] = None


class NotificationSubscriptionRequest(BaseModel):
    user_id: str
    tenant_id: Optional[int] = None
    branch_id: Optional[int] = None
    event_type: str = Field(min_length=2)
    channel: Literal["telegram", "email", "phone", "in_app"]
    severity_min: Optional[Literal["INFO", "WARN", "CRITICAL"]] = None
    is_enabled: bool = True


class ChannelUpsertRequest(BaseModel):
    name: str = Field(min_length=2)
    contact_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    is_enabled: bool = True
    notes: Optional[str] = None


class ContractUpsertRequest(BaseModel):
    tenant_id: int
    channel_id: Optional[int] = None
    plan_name: str = Field(min_length=2)
    monthly_amount: Decimal = Field(ge=0)
    billing_day: int = Field(ge=1, le=31)
    issue_day: int = Field(ge=1, le=31)
    start_date: date
    end_date: Optional[date] = None
    is_enabled: bool = True
    commission_first_year_pct: Decimal = Field(ge=0, le=100)
    commission_recurring_pct: Decimal = Field(ge=0, le=100)
    notes: Optional[str] = None


class ReceivableGenerationRequest(BaseModel):
    competence_month: Optional[date] = None
    as_of: Optional[date] = None
    months_ahead: int = Field(default=0, ge=0, le=6)
    tenant_id: Optional[int] = None


class ReceivableMarkEmittedRequest(BaseModel):
    emitted_at: Optional[datetime] = None
    notes: Optional[str] = None


class ReceivableMarkPaidRequest(BaseModel):
    paid_at: Optional[datetime] = None
    received_amount: Optional[Decimal] = Field(default=None, ge=0)
    payment_method: Optional[str] = None
    notes: Optional[str] = None


class StatusNoteRequest(BaseModel):
    notes: Optional[str] = None


class PayableMarkPaidRequest(BaseModel):
    paid_at: Optional[datetime] = None
    notes: Optional[str] = None
