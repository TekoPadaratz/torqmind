from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from app.usernames import normalize_email, normalize_username

PlatformRole = Literal[
    "platform_master",
    "platform_admin",
    "product_global",
    "channel_admin",
    "tenant_admin",
    "tenant_manager",
    "tenant_viewer",
    "tenant_kiosk",
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
    apelido: Optional[str] = Field(default=None, max_length=40)
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
    username: str = Field(min_length=3, max_length=32)
    password: Optional[str] = Field(default=None, min_length=8)
    role: PlatformRole
    is_enabled: bool = True
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    must_change_password: bool = False
    locked_until: Optional[datetime] = None
    reset_failed_login: bool = False
    accesses: list[UserAccessInput] = Field(default_factory=list)
    screen_permissions: Optional[List[str]] = Field(default=None, description="Allowed screen keys for manager/kiosk roles")

    @field_validator("email")
    @classmethod
    def normalize_email_value(cls, value: str) -> str:
        normalized = normalize_email(value)
        if not normalized:
            raise ValueError("Email é obrigatório.")
        return normalized

    @field_validator("username")
    @classmethod
    def normalize_username_value(cls, value: str) -> str:
        normalized = normalize_username(value)
        if not normalized:
            raise ValueError("Nome de usuário é obrigatório.")
        return normalized


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


class PlatformEmailProfileUpdateRequest(BaseModel):
    channel_name: str = Field(min_length=1, max_length=120)
    contact_name: Optional[str] = Field(default=None, max_length=120)
    from_email: Optional[str] = Field(default=None, max_length=320)
    smtp_enabled: bool = False
    smtp_host: Optional[str] = Field(default=None, max_length=255)
    smtp_port: int = Field(default=587, ge=1, le=65535)
    smtp_user: Optional[str] = Field(default=None, max_length=320)
    smtp_password: Optional[str] = Field(default=None, max_length=512)
    smtp_use_ssl: bool = False
    smtp_use_tls: bool = True
    smtp_from_name: Optional[str] = Field(default=None, max_length=120)
    smtp_timeout_seconds: int = Field(default=20, ge=5, le=120)


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
