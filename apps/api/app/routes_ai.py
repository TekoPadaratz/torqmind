"""Rotas /ai — Assistente TorqMind (determinístico, somente leitura)."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from threading import Lock
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.deps import get_current_claims
from app.intelligence import list_capabilities, process_message
from app.intelligence.authz import permission_hash
from app.intelligence.limits import get_limits
from app.permissions import require_not_kiosk, require_screen
from app.scope import resolve_scope
from app import repos_ai



router = APIRouter(prefix="/ai", tags=["ai"])

_rate_lock = Lock()
_rate_buckets: dict[str, list[float]] = defaultdict(list)


class CreateConversationBody(BaseModel):
    title: Optional[str] = None
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None


class PostMessageBody(BaseModel):
    text: str = Field(..., min_length=1)
    id_empresa: Optional[int] = None
    id_filial: Optional[int] = None


class FeedbackBody(BaseModel):
    rating: int
    reason_code: Optional[str] = None
    note: Optional[str] = None


def _ensure_enabled() -> None:
    if not bool(getattr(settings, "ai_chat_enabled", False)):
        raise HTTPException(
            status_code=503,
            detail={
                "error": "ai_chat_disabled",
                "message": "O Assistente TorqMind está desligado neste ambiente. Ative AI_CHAT_ENABLED após validar em homologação.",
            },
        )


def _rate_limit(claims: dict[str, Any]) -> None:
    limits = get_limits()
    key = str(claims.get("sub") or claims.get("user_id") or "anon")
    now = time.time()
    with _rate_lock:
        bucket = [t for t in _rate_buckets[key] if now - t < 60]
        if len(bucket) >= limits.rate_limit_per_minute:
            raise HTTPException(
                status_code=429,
                detail={"error": "rate_limited", "message": "Muitas mensagens. Aguarde um instante."},
            )
        bucket.append(now)
        _rate_buckets[key] = bucket


def _is_admin(claims: dict[str, Any]) -> bool:
    role = str(claims.get("user_role") or "")
    return role in {"platform_master", "platform_admin", "tenant_admin", "product_global"}


def _resolve_ai_scope(claims: dict[str, Any], id_empresa_q: int | None, id_filial_q: int | None) -> dict[str, Any]:
    # resolve_scope valida tenant a partir dos claims — nunca confiar só no texto
    id_empresa, id_filial = resolve_scope(claims, id_empresa_q, id_filial_q)
    return {"id_empresa": int(id_empresa), "id_filial": id_filial}


def _scoped_claims(claims: dict[str, Any], scope: dict[str, Any]) -> dict[str, Any]:
    """Cópia dos claims com id_empresa efetivo (platform_master pode vir sem empresa)."""
    out = dict(claims)
    out["id_empresa"] = int(scope["id_empresa"])
    if scope.get("id_filial") is not None:
        out["id_filial"] = scope.get("id_filial")
    return out


def _scope_required() -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "error": "scope_required",
            "message": "Selecione a empresa no topo da tela para usar o assistente.",
        },
    )


@router.get("/capabilities")
def ai_capabilities(
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
    _screen=Depends(require_screen("assistant")),
):
    _ensure_enabled()
    return {"items": list_capabilities(claims), "disclaimer": "Somente leitura. O assistente não altera informações."}


@router.get("/capability-coverage")
def ai_capability_coverage(
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
):
    _ensure_enabled()
    if not _is_admin(claims):
        raise HTTPException(status_code=403, detail={"error": "admin_only", "message": "Acesso restrito."})
    return {"items": repos_ai.list_capability_coverage()}


@router.post("/conversations")
def ai_create_conversation(
    body: CreateConversationBody | None = None,
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
    _screen=Depends(require_screen("assistant")),
):
    _ensure_enabled()
    body = body or CreateConversationBody()
    try:
        scope = _resolve_ai_scope(claims, body.id_empresa, body.id_filial)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "scope_required",
                "message": "Selecione a empresa no topo da tela para usar o assistente.",
            },
        )
    branch_scope: list[Any] = []
    if scope.get("id_filial") is not None:
        branch_scope = [scope.get("id_filial")]
    try:
        row = repos_ai.create_conversation(
            claims,
            title=body.title,
            permission_hash=permission_hash(claims),
            branch_scope=branch_scope,
            id_empresa=int(scope["id_empresa"]),
        )
    except ValueError as exc:
        code = str(exc)
        if code == "max_active_conversations":
            raise HTTPException(
                status_code=409,
                detail={"error": "limit", "message": "Limite de conversas ativas atingido."},
            )
        if code in {"missing_id_empresa", "missing_user_id"}:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "scope_required",
                    "message": "Selecione a empresa no topo da tela para usar o assistente.",
                },
            )
        raise HTTPException(
            status_code=400,
            detail={"error": "validation_failed", "message": "Não foi possível abrir a conversa."},
        )
    return row


@router.get("/conversations")
def ai_list_conversations(
    id_empresa: Optional[int] = None,
    id_filial: Optional[int] = None,
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
    _screen=Depends(require_screen("assistant")),
):
    _ensure_enabled()
    try:
        scope = _resolve_ai_scope(claims, id_empresa, id_filial)
    except HTTPException:
        raise
    except Exception:
        raise _scope_required()
    try:
        return {"items": repos_ai.list_conversations(claims, id_empresa=int(scope["id_empresa"]))}
    except ValueError as exc:
        if str(exc) in {"missing_id_empresa", "missing_user_id"}:
            raise _scope_required()
        raise


@router.get("/conversations/{conversation_id}/messages")
def ai_list_messages(
    conversation_id: str,
    id_empresa: Optional[int] = None,
    id_filial: Optional[int] = None,
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
    _screen=Depends(require_screen("assistant")),
):
    _ensure_enabled()
    try:
        scope = _resolve_ai_scope(claims, id_empresa, id_filial)
    except HTTPException:
        raise
    except Exception:
        raise _scope_required()
    scoped = _scoped_claims(claims, scope)
    try:
        conv = repos_ai.get_conversation(scoped, conversation_id, id_empresa=int(scope["id_empresa"]))
    except ValueError as exc:
        if str(exc) in {"missing_id_empresa", "missing_user_id"}:
            raise _scope_required()
        raise
    if not conv:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Conversa não encontrada."})
    return {"items": repos_ai.list_messages(scoped, conversation_id, id_empresa=int(scope["id_empresa"]))}


@router.post("/conversations/{conversation_id}/messages")
async def ai_post_message(
    conversation_id: str,
    request: Request,
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
    _screen=Depends(require_screen("assistant")),
    accept: Optional[str] = Header(default=None),
):
    _ensure_enabled()
    _rate_limit(claims)

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail={"error": "invalid_json", "message": "JSON inválido."})
    body = PostMessageBody(**payload)
    limits = get_limits()
    if len(body.text) > limits.max_message_chars:
        raise HTTPException(
            status_code=422,
            detail={"error": "message_too_long", "message": f"Mensagem excede {limits.max_message_chars} caracteres."},
        )

    try:
        scope = _resolve_ai_scope(claims, body.id_empresa, body.id_filial)
    except HTTPException:
        raise
    except Exception:
        raise _scope_required()

    scoped = _scoped_claims(claims, scope)
    try:
        conv = repos_ai.get_conversation(scoped, conversation_id, id_empresa=int(scope["id_empresa"]))
    except ValueError as exc:
        if str(exc) in {"missing_id_empresa", "missing_user_id"}:
            raise _scope_required()
        raise
    if not conv:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Conversa não encontrada."})

    context = conv.get("context_opaque") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except Exception:
            context = {}

    result = process_message(scoped, body.text, conversation_context=context, scope=scope)

    if result.get("status") == "unknown":
        try:
            repos_ai.enqueue_unknown_question(
                scoped,
                body.text,
                permission_hash=permission_hash(scoped),
                id_empresa=int(scope["id_empresa"]),
            )
        except Exception:
            pass

    try:
        saved = repos_ai.add_message_pair(
            scoped,
            conversation_id,
            user_text=body.text,
            assistant=result,
            tool_calls_meta=result.get("tool_calls_meta") or [],
            id_empresa=int(scope["id_empresa"]),
        )
    except ValueError as exc:
        if str(exc) == "max_messages_per_conversation":
            raise HTTPException(status_code=409, detail={"error": "limit", "message": "Limite de mensagens da conversa."})
        if str(exc) in {"missing_id_empresa", "missing_user_id"}:
            raise _scope_required()
        raise HTTPException(
            status_code=400,
            detail={"error": "validation_failed", "message": "Não foi possível gravar a mensagem."},
        )
    except LookupError:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Conversa não encontrada."})

    response_body = {
        **result,
        "conversation_id": conversation_id,
        "user_message_id": saved.get("user_message_id"),
        "assistant_message_id": str((saved.get("assistant_message") or {}).get("id") or ""),
    }

    wants_sse = accept and "text/event-stream" in accept.lower()
    if wants_sse:

        def _gen():
            yield f"event: message\ndata: {json.dumps(response_body, ensure_ascii=False, default=str)}\n\n"

        return StreamingResponse(_gen(), media_type="text/event-stream")

    return response_body


@router.post("/conversations/{conversation_id}/messages/{message_id}/feedback")
def ai_feedback(
    conversation_id: str,
    message_id: str,
    body: FeedbackBody,
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
    _screen=Depends(require_screen("assistant")),
):
    _ensure_enabled()
    try:
        return repos_ai.add_feedback(
            claims,
            conversation_id,
            message_id,
            rating=body.rating,
            reason_code=body.reason_code,
            note=body.note,
        )
    except ValueError:
        raise HTTPException(status_code=422, detail={"error": "invalid_rating", "message": "rating deve ser -1 ou 1."})
    except LookupError:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Conversa não encontrada."})


@router.get("/unknown-questions")
def ai_unknown_questions(
    claims=Depends(get_current_claims),
    _kiosk=Depends(require_not_kiosk()),
):
    _ensure_enabled()
    if not _is_admin(claims):
        raise HTTPException(status_code=403, detail={"error": "admin_only", "message": "Acesso restrito."})
    return {"items": repos_ai.list_unknown_questions(claims)}
