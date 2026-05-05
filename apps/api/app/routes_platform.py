from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.deps import get_current_claims
from app import repos_auth, repos_platform
from app.schemas_platform import (
    BranchUpsertRequest,
    ChannelUpsertRequest,
    ContractUpsertRequest,
    NotificationSubscriptionRequest,
    PayableMarkPaidRequest,
    ReceivableGenerationRequest,
    ReceivableMarkEmittedRequest,
    ReceivableMarkPaidRequest,
    StatusNoteRequest,
    TenantUpsertRequest,
    UserContactRequest,
    UserUpsertRequest,
)

router = APIRouter(prefix="/platform", tags=["platform"])


def _ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def _raise(exc: repos_platform.AuthError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


@router.get("/companies")
def companies_list(
    search: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_companies(claims, search=search, status=status, limit=limit, offset=offset)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/companies")
def companies_create(body: TenantUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_company(claims, body.model_dump(), ip=_ip(request), tenant_id=None)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/companies/{tenant_id}")
def companies_detail(tenant_id: int, claims=Depends(get_current_claims)):
    try:
        return repos_platform.get_company_detail(claims, tenant_id)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.patch("/companies/{tenant_id}")
def companies_update(tenant_id: int, body: TenantUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_company(claims, body.model_dump(), ip=_ip(request), tenant_id=tenant_id)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/companies/{tenant_id}/branches")
def branches_create(tenant_id: int, body: BranchUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_branch(claims, tenant_id, body.model_dump(), ip=_ip(request), branch_id=None)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.patch("/companies/{tenant_id}/branches/{branch_id}")
def branches_update(
    tenant_id: int,
    branch_id: int,
    body: BranchUpsertRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.upsert_branch(claims, tenant_id, body.model_dump(), ip=_ip(request), branch_id=branch_id)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/users")
def users_list(
    tenant_id: int | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_users(claims, tenant_id=tenant_id, search=search, limit=limit, offset=offset)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/users")
def users_create(body: UserUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_user(claims, body.model_dump(), ip=_ip(request), user_id=None)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.patch("/users/{user_id}")
def users_update(user_id: str, body: UserUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_user(claims, body.model_dump(), ip=_ip(request), user_id=user_id)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.put("/users/{user_id}/contacts")
def users_contacts_update(user_id: str, body: UserContactRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_user_contacts(claims, user_id, body.model_dump(), ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/notifications/subscriptions")
def subscriptions_list(
    tenant_id: int | None = Query(None),
    user_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_notification_subscriptions(
            claims,
            tenant_id=tenant_id,
            user_id=user_id,
            limit=limit,
            offset=offset,
        )
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/notifications/subscriptions")
def subscriptions_create(body: NotificationSubscriptionRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_notification_subscription(claims, body.model_dump(), ip=_ip(request), subscription_id=None)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.patch("/notifications/subscriptions/{subscription_id}")
def subscriptions_update(
    subscription_id: int,
    body: NotificationSubscriptionRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.upsert_notification_subscription(
            claims,
            body.model_dump(),
            ip=_ip(request),
            subscription_id=subscription_id,
        )
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/channels")
def channels_list(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_channels(claims, limit=limit, offset=offset)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/channels")
def channels_create(body: ChannelUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_channel(claims, body.model_dump(), ip=_ip(request), channel_id=None)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.patch("/channels/{channel_id}")
def channels_update(channel_id: int, body: ChannelUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_channel(claims, body.model_dump(), ip=_ip(request), channel_id=channel_id)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/contracts")
def contracts_list(
    tenant_id: int | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_contracts(claims, tenant_id=tenant_id, limit=limit, offset=offset)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/contracts")
def contracts_create(body: ContractUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_contract(claims, body.model_dump(), ip=_ip(request), contract_id=None)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.patch("/contracts/{contract_id}")
def contracts_update(contract_id: int, body: ContractUpsertRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.upsert_contract(claims, body.model_dump(), ip=_ip(request), contract_id=contract_id)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/receivables")
def receivables_list(
    tenant_id: int | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_receivables(claims, tenant_id=tenant_id, status=status, limit=limit, offset=offset)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/generate")
def receivables_generate(body: ReceivableGenerationRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.generate_receivables(
            claims,
            ip=_ip(request),
            competence_month=body.competence_month,
            as_of=body.as_of,
            months_ahead=body.months_ahead,
            tenant_id=body.tenant_id,
        )
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/{receivable_id}/emit")
def receivables_emit(
    receivable_id: int,
    body: ReceivableMarkEmittedRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.mark_receivable_emitted(claims, receivable_id, body.model_dump(), ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/{receivable_id}/unemit")
def receivables_unemit(
    receivable_id: int,
    body: StatusNoteRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.unmark_receivable_emitted(claims, receivable_id, body.notes, ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/{receivable_id}/pay")
def receivables_pay(
    receivable_id: int,
    body: ReceivableMarkPaidRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.mark_receivable_paid(claims, receivable_id, body.model_dump(), ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/{receivable_id}/undo-payment")
def receivables_undo_payment(
    receivable_id: int,
    body: StatusNoteRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.undo_receivable_payment(claims, receivable_id, body.notes, ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/{receivable_id}/cancel")
def receivables_cancel(
    receivable_id: int,
    body: StatusNoteRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.cancel_receivable(claims, receivable_id, body.notes, ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/receivables/{receivable_id}/reopen")
def receivables_reopen(
    receivable_id: int,
    body: StatusNoteRequest,
    request: Request,
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.reopen_receivable(claims, receivable_id, body.notes, ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/channel-payables")
def payables_list(
    channel_id: int | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    claims=Depends(get_current_claims),
):
    try:
        return repos_platform.list_channel_payables(claims, channel_id=channel_id, status=status, limit=limit, offset=offset)
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/channel-payables/{payable_id}/pay")
def payables_pay(payable_id: int, body: PayableMarkPaidRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.mark_channel_payable_paid(claims, payable_id, body.model_dump(), ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.post("/channel-payables/{payable_id}/cancel")
def payables_cancel(payable_id: int, body: StatusNoteRequest, request: Request, claims=Depends(get_current_claims)):
    try:
        return repos_platform.cancel_channel_payable(claims, payable_id, body.notes, ip=_ip(request))
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/audit")
def audit_list(
    tenant_id: int | None = Query(None),
    entity_type: str | None = Query(None),
    action: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    entity_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    claims=Depends(get_current_claims),
):
    try:
        return {
            "items": repos_platform.list_audit(
                claims,
                tenant_id=tenant_id,
                entity_type=entity_type,
                action=action,
                date_from=date_from,
                date_to=date_to,
                entity_id=entity_id,
                limit=limit,
            )
        }
    except repos_platform.AuthError as exc:
        _raise(exc)


@router.get("/streaming-health")
def streaming_health(claims=Depends(get_current_claims)):
    """Streaming/realtime pipeline health for platform admins."""
    try:
        repos_auth.assert_platform_access(claims)
    except repos_auth.AuthError as exc:
        _raise(exc)

    from app import repos_mart_realtime
    from app.config import settings as _settings

    try:
        health = repos_mart_realtime.streaming_health(id_empresa=0)
    except Exception as e:
        health = {"error": str(e), "source_freshness": [], "cdc_state": [], "recent_errors": [], "lag": [], "mart_publications": []}

    health["use_realtime_marts"] = _settings.use_realtime_marts
    health["realtime_marts_source"] = _settings.realtime_marts_source
    health["realtime_marts_domains"] = _settings.realtime_marts_domains
    health["realtime_marts_fallback"] = _settings.realtime_marts_fallback
    return health
