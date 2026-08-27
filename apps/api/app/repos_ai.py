"""Persistência PG do Assistente (conversas/mensagens/feedback) — tenant + user."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Optional

from app.db import get_conn
from app.intelligence.json_util import dumps_json
from app.intelligence.limits import get_limits


def _hash_text(value: str | None) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _user_id(claims: dict[str, Any]) -> str:
    uid = str(claims.get("sub") or claims.get("user_id") or "").strip()
    if not uid:
        raise ValueError("missing_user_id")
    return uid


def _empresa(claims: dict[str, Any], scope: dict[str, Any] | None = None) -> int:
    """Resolve id_empresa from explicit scope, claims or accesses — never assume None."""
    if scope and scope.get("id_empresa") is not None:
        return int(scope["id_empresa"])
    raw = claims.get("id_empresa")
    if raw is not None and str(raw).strip() != "":
        return int(raw)
    for row in claims.get("accesses") or []:
        if row.get("id_empresa") is not None:
            return int(row["id_empresa"])
    raise ValueError("missing_id_empresa")


def create_conversation(
    claims: dict[str, Any],
    *,
    title: str | None = None,
    permission_hash: str = "",
    branch_scope: list[Any] | None = None,
    context_opaque: dict[str, Any] | None = None,
    id_empresa: int | None = None,
) -> dict[str, Any]:
    limits = get_limits()
    id_empresa = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    user_id = _user_id(claims)
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        active = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM app.ai_conversations
            WHERE id_empresa = %s AND user_id = %s::uuid AND status = 'active'
            """,
            (id_empresa, user_id),
        ).fetchone()
        if int((active or {}).get("n") or 0) >= limits.max_active_conversations:
            raise ValueError("max_active_conversations")

        row = conn.execute(
            """
            INSERT INTO app.ai_conversations (
              id_empresa, user_id, title, permission_hash, branch_scope, context_opaque
            ) VALUES (
              %s, %s::uuid, %s, %s, %s::jsonb, %s::jsonb
            )
            RETURNING id, id_empresa, user_id, status, title, permission_hash,
                      branch_scope, context_opaque, message_count, created_at, updated_at
            """,
            (
                id_empresa,
                user_id,
                (title or "")[:120] or None,
                permission_hash or "",
                dumps_json(branch_scope or []),
                dumps_json(context_opaque or {}),
            ),
        ).fetchone()
        conn.commit()
    return dict(row)


def get_conversation(
    claims: dict[str, Any],
    conversation_id: str,
    *,
    id_empresa: int | None = None,
) -> dict[str, Any] | None:
    user_id = _user_id(claims)
    id_empresa = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        row = conn.execute(
            """
            SELECT id, id_empresa, user_id, status, title, permission_hash, branch_scope,
                   context_opaque, message_count, created_at, updated_at, last_message_at
            FROM app.ai_conversations
            WHERE id = %s::uuid AND id_empresa = %s AND user_id = %s::uuid
            """,
            (conversation_id, id_empresa, user_id),
        ).fetchone()
    return dict(row) if row else None


def list_conversations(
    claims: dict[str, Any],
    *,
    limit: int = 30,
    id_empresa: int | None = None,
) -> list[dict[str, Any]]:
    id_empresa = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    user_id = _user_id(claims)
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        rows = conn.execute(
            """
            SELECT id, status, title, message_count, created_at, updated_at, last_message_at
            FROM app.ai_conversations
            WHERE id_empresa = %s AND user_id = %s::uuid AND status = 'active'
            ORDER BY COALESCE(last_message_at, updated_at) DESC
            LIMIT %s
            """,
            (id_empresa, user_id, max(1, min(100, int(limit)))),
        ).fetchall()
    return [dict(r) for r in rows]


def list_messages(
    claims: dict[str, Any],
    conversation_id: str,
    *,
    limit: int = 100,
    id_empresa: int | None = None,
) -> list[dict[str, Any]]:
    id_empresa_resolved = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    user_id = _user_id(claims)
    # ownership check
    if not get_conversation(claims, conversation_id, id_empresa=id_empresa_resolved):
        return []
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa_resolved) as conn:
        rows = conn.execute(
            """
            SELECT id, role, status, content_text, intent_id, confidence, evidence_ids,
                   deep_link_key, answer_id, request_id, created_at
            FROM app.ai_messages
            WHERE conversation_id = %s::uuid AND id_empresa = %s AND user_id = %s::uuid
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (conversation_id, id_empresa_resolved, user_id, max(1, min(200, int(limit)))),
        ).fetchall()
    return [dict(r) for r in rows]


def update_conversation_context(
    claims: dict[str, Any],
    conversation_id: str,
    context_opaque: dict[str, Any],
    *,
    permission_hash: str | None = None,
    id_empresa: int | None = None,
) -> None:
    id_empresa = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    user_id = _user_id(claims)
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        conn.execute(
            """
            UPDATE app.ai_conversations
            SET context_opaque = %s::jsonb,
                permission_hash = COALESCE(%s, permission_hash),
                updated_at = now()
            WHERE id = %s::uuid AND id_empresa = %s AND user_id = %s::uuid
            """,
            (dumps_json(context_opaque or {}), permission_hash, conversation_id, id_empresa, user_id),
        )
        conn.commit()


def add_message_pair(
    claims: dict[str, Any],
    conversation_id: str,
    *,
    user_text: str,
    assistant: dict[str, Any],
    tool_calls_meta: list[dict[str, Any]] | None = None,
    id_empresa: int | None = None,
) -> dict[str, Any]:
    """Persiste user+assistant. Não grava resultados brutos de tools."""
    limits = get_limits()
    id_empresa = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    user_id = _user_id(claims)
    conv = get_conversation(claims, conversation_id, id_empresa=id_empresa)
    if not conv:
        raise LookupError("conversation_not_found")
    if int(conv.get("message_count") or 0) >= limits.max_messages_per_conversation:
        raise ValueError("max_messages_per_conversation")

    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        user_row = conn.execute(
            """
            INSERT INTO app.ai_messages (
              conversation_id, id_empresa, user_id, role, status, content_text, content_hash, request_id
            ) VALUES (
              %s::uuid, %s, %s::uuid, 'user', 'ok', %s, %s, %s::uuid
            )
            RETURNING id, created_at
            """,
            (
                conversation_id,
                id_empresa,
                user_id,
                user_text[: limits.max_message_chars],
                _hash_text(user_text),
                assistant.get("request_id") or str(uuid.uuid4()),
            ),
        ).fetchone()

        asst_row = conn.execute(
            """
            INSERT INTO app.ai_messages (
              conversation_id, id_empresa, user_id, role, status, content_text, content_hash,
              intent_id, confidence, evidence_ids, deep_link_key, answer_id, request_id, slots_opaque
            ) VALUES (
              %s::uuid, %s, %s::uuid, 'assistant', %s, %s, %s,
              %s, %s, %s::jsonb, %s, %s::uuid, %s::uuid, %s::jsonb
            )
            RETURNING id, created_at, status, content_text, intent_id, confidence, evidence_ids,
                      deep_link_key, answer_id, request_id
            """,
            (
                conversation_id,
                id_empresa,
                user_id,
                assistant.get("status") or "ok",
                (assistant.get("answer_text") or "")[: limits.max_response_chars],
                _hash_text(assistant.get("answer_text")),
                assistant.get("intent_id"),
                assistant.get("confidence"),
                dumps_json(assistant.get("evidence_ids") or []),
                assistant.get("deep_link"),
                assistant.get("answer_id") or str(uuid.uuid4()),
                assistant.get("request_id") or str(uuid.uuid4()),
                dumps_json({}),  # slots opacos mínimos — sem PII extra
            ),
        ).fetchone()

        for meta in tool_calls_meta or []:
            conn.execute(
                """
                INSERT INTO app.ai_tool_calls (
                  conversation_id, message_id, id_empresa, user_id,
                  tool_name, tool_version, args_minimized, result_hash,
                  result_row_count, latency_ms, status
                ) VALUES (
                  %s::uuid, %s::uuid, %s, %s::uuid,
                  %s, %s, %s::jsonb, %s,
                  %s, %s, %s
                )
                """,
                (
                    conversation_id,
                    asst_row["id"],
                    id_empresa,
                    user_id,
                    meta.get("tool_name") or "unknown",
                    str(meta.get("tool_version") or "1"),
                    dumps_json({"minimized": True}),
                    meta.get("result_hash"),
                    meta.get("row_count"),
                    meta.get("latency_ms"),
                    meta.get("status") or "ok",
                ),
            )

        conn.execute(
            """
            UPDATE app.ai_conversations
            SET message_count = message_count + 2,
                last_message_at = now(),
                updated_at = now(),
                context_opaque = COALESCE(%s::jsonb, context_opaque)
            WHERE id = %s::uuid AND id_empresa = %s AND user_id = %s::uuid
            """,
            (
                dumps_json(assistant.get("conversation_context"))
                if assistant.get("conversation_context") is not None
                else None,
                conversation_id,
                id_empresa,
                user_id,
            ),
        )
        conn.commit()

    return {
        "user_message_id": str(user_row["id"]),
        "assistant_message": dict(asst_row),
    }


def add_feedback(
    claims: dict[str, Any],
    conversation_id: str,
    message_id: str,
    *,
    rating: int,
    reason_code: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    if rating not in (-1, 1):
        raise ValueError("invalid_rating")
    id_empresa = _empresa(claims)
    user_id = _user_id(claims)
    if not get_conversation(claims, conversation_id):
        raise LookupError("conversation_not_found")
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        row = conn.execute(
            """
            INSERT INTO app.ai_feedback (
              conversation_id, message_id, id_empresa, user_id, rating, reason_code, note_hash
            ) VALUES (
              %s::uuid, %s::uuid, %s, %s::uuid, %s, %s, %s
            )
            ON CONFLICT (message_id, user_id) DO UPDATE
              SET rating = EXCLUDED.rating,
                  reason_code = EXCLUDED.reason_code,
                  note_hash = EXCLUDED.note_hash
            RETURNING id, rating, reason_code, created_at
            """,
            (
                conversation_id,
                message_id,
                id_empresa,
                user_id,
                int(rating),
                reason_code,
                _hash_text(note) if note else None,
            ),
        ).fetchone()
        conn.commit()
    return dict(row)


def enqueue_unknown_question(
    claims: dict[str, Any],
    question: str,
    permission_hash: str | None = None,
    *,
    id_empresa: int | None = None,
) -> None:
    id_empresa = _empresa(claims, {"id_empresa": id_empresa} if id_empresa is not None else None)
    user_id = _user_id(claims)
    from app.intelligence.normalize import fold_key

    normalized = fold_key(question)
    qhash = _hash_text(normalized)
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        conn.execute(
            """
            INSERT INTO app.ai_unknown_questions_queue (
              id_empresa, user_id, question_text, question_normalized, question_hash, permission_hash
            ) VALUES (%s, %s::uuid, %s, %s, %s, %s)
            ON CONFLICT (id_empresa, question_hash) DO UPDATE
              SET frequency = app.ai_unknown_questions_queue.frequency + 1,
                  last_seen_at = now()
            """,
            (id_empresa, user_id, question[:2000], normalized[:2000], qhash, permission_hash or ""),
        )
        conn.commit()


def list_unknown_questions(claims: dict[str, Any], *, limit: int = 50) -> list[dict[str, Any]]:
    id_empresa = _empresa(claims)
    with get_conn(role=str(claims.get("user_role") or "tenant_viewer"), tenant_id=id_empresa) as conn:
        rows = conn.execute(
            """
            SELECT id, question_text, question_normalized, status, frequency, last_seen_at, created_at
            FROM app.ai_unknown_questions_queue
            WHERE id_empresa = %s
            ORDER BY last_seen_at DESC
            LIMIT %s
            """,
            (id_empresa, max(1, min(200, int(limit)))),
        ).fetchall()
    return [dict(r) for r in rows]


def list_capability_coverage(*, limit: int = 200) -> list[dict[str, Any]]:
    with get_conn(role="MASTER", tenant_id=None) as conn:
        try:
            rows = conn.execute(
                """
                SELECT domain, subdomain, intent_id, tool_name, screen_key, coverage_status, notes, catalog_version, updated_at
                FROM app.ai_capability_coverage
                ORDER BY domain, intent_id
                LIMIT %s
                """,
                (max(1, min(500, int(limit))),),
            ).fetchall()
        except Exception:
            return []
    return [dict(r) for r in rows]
