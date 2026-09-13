"""Orquestração determinística do Assistente TorqMind Intelligence."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from app.filial_apelido import apelido_for, load_apelido_map
from app.intelligence.authz import reauthorize
from app.intelligence.branch_resolve import BranchResolveResult, apply_resolved_branch, resolve_branch_hint
from app.intelligence.capability_map.loader import get_intent, list_intents, suggestions_for_claims
from app.intelligence.conversation import invalidate_if_scope_changed, update_after_turn
from app.intelligence.disambiguation import consume_pending_disambiguation
from app.intelligence.entity_resolve import search_customers
from app.intelligence.evidence import EvidenceStore
from app.intelligence.guards import detect_injection, detect_mutation_request, detect_sensitive_probe
from app.intelligence.limits import get_limits
from app.intelligence.normalize import normalize_text
from app.intelligence.parser import ensure_period, parse_intent
from app.intelligence.periods import PeriodResult
from app.intelligence.deep_links import deep_link_for_intent
from app.intelligence.templates.responses import build_answer, uncertain_answer
from app.intelligence.tools.executor import execute_tool
from app.intelligence.tools.registry import get_tool
from app.permissions import can_view_sensitive_financials, is_kiosk_user
from datetime import date


VALID_STATUSES = frozenset(
    {
        "ok",
        "clarification_required",
        "forbidden",
        "unsupported",
        "mutation_denied",
        "stale_data",
        "no_data",
        "timeout",
        "overloaded",
        "validation_failed",
        "unknown",
    }
)


def _ui_period(scope: dict[str, Any]) -> PeriodResult | None:
    """Período enviado pela tela (escopo atual)."""
    raw_ini, raw_fim = scope.get("dt_ini"), scope.get("dt_fim")
    if not raw_ini or not raw_fim:
        return None
    try:
        dt_ini = date.fromisoformat(str(raw_ini)[:10])
        dt_fim = date.fromisoformat(str(raw_fim)[:10])
    except ValueError:
        return None
    if dt_fim < dt_ini:
        return None
    return PeriodResult(dt_ini, dt_fim, f"{dt_ini.isoformat()} → {dt_fim.isoformat()}")


def _resolve_period(parsed: Any, scope: dict[str, Any]) -> PeriodResult:
    """Período da pergunta > período da tela > default do parser."""
    if getattr(parsed, "period", None):
        return parsed.period
    ui = _ui_period(scope)
    if ui:
        return ui
    return ensure_period(parsed)


@dataclass
class EngineResult:
    status: str
    answer_text: str
    intent_id: str | None = None
    confidence: float = 0.0
    clarification_options: list[Any] = field(default_factory=list)
    clarification_kind: str | None = None
    suggestions: list[str] = field(default_factory=list)
    deep_link: str | None = None
    evidence_ids: list[str] = field(default_factory=list)
    tool_calls_meta: list[dict[str, Any]] = field(default_factory=list)
    answer_id: str = ""
    request_id: str = ""
    conversation_context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if data["status"] not in VALID_STATUSES:
            data["status"] = "validation_failed"
        return data


def list_capabilities(claims: dict[str, Any]) -> list[dict[str, Any]]:
    sensitive = can_view_sensitive_financials(claims)
    out: list[dict[str, Any]] = []
    for intent in list_intents():
        if intent.get("hidden_from_kiosk") and is_kiosk_user(claims):
            continue
        if (intent.get("requires_sensitive_role") or intent.get("requires_sensitive")) and not sensitive:
            continue
        syns = intent.get("synonyms") or []
        out.append(
            {
                "intent_id": intent.get("intent_id"),
                "domain": intent.get("domain"),
                "label": syns[0] if syns else intent.get("intent_id"),
                "examples": syns[:3],
                "deep_link": intent.get("deep_link_key"),
                "screen_key": intent.get("screen_key"),
            }
        )
    return out


def _scope_label(scope: dict[str, Any]) -> str:
    if scope.get("filial_label"):
        return f"filial {scope['filial_label']}"
    emp = scope.get("id_empresa")
    fil = scope.get("id_filial")
    apelidos = load_apelido_map(int(emp)) if emp else {}
    if scope.get("branch_scope") == "all" or (
        isinstance(fil, list) and len(fil) > 1
    ):
        count = len(fil) if isinstance(fil, list) else len(scope.get("id_filiais") or [])
        if count > 1:
            return f"empresa {emp}, todas as filiais ({count})"
        return f"empresa {emp} (todas as filiais do escopo)"
    if isinstance(fil, list) and len(fil) == 1:
        fil = fil[0]
    if fil is None:
        return f"empresa {emp} (todas as filiais do escopo)"
    label = apelidos.get(int(fil)) or apelido_for(fil)
    if label:
        return f"filial {label}"
    return "filial selecionada"


def _intent_label(intent_id: str | None) -> str:
    if not intent_id:
        return ""
    meta = get_intent(intent_id) or {}
    syns = meta.get("synonyms") or []
    return str(syns[0] if syns else intent_id)


def _follow_up_suggestions(claims: dict[str, Any], intent_id: str | None) -> list[str]:
    if intent_id:
        meta = get_intent(intent_id) or {}
        follow = [str(x) for x in (meta.get("follow_ups") or []) if x]
        if follow:
            return follow[:4]
    screens = claims.get("allowed_screens") or []
    chips = suggestions_for_claims(
        screens,
        can_view_sensitive_financials(claims),
        limit=4,
    )
    return [str(c.get("text") or "").replace("Sobre ", "").replace("?", "") for c in chips if c.get("text")]


def _resolve_branch_from_slots(
    parsed_slots: dict[str, Any],
    scope: dict[str, Any],
    claims: dict[str, Any],
) -> tuple[dict[str, Any], Optional[BranchResolveResult]]:
    hint = parsed_slots.get("filial_label")
    if not hint:
        return scope, None
    branch = resolve_branch_hint(str(hint), scope, claims)
    if branch.status == "resolved":
        return apply_resolved_branch(scope, branch), branch
    if branch.status in {"ambiguous", "not_found"}:
        return scope, branch
    return scope, None


def _default_suggestions(claims: dict[str, Any]) -> list[str]:
    caps = list_capabilities(claims)
    return [str(c.get("label")) for c in caps[:5] if c.get("label")]


def process_message(
    claims: dict[str, Any],
    text: str,
    conversation_context: dict[str, Any] | None = None,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    answer_id = str(uuid.uuid4())
    limits = get_limits()
    evidence = EvidenceStore()
    tool_meta: list[dict[str, Any]] = []

    if is_kiosk_user(claims):
        result = EngineResult(
            status="forbidden",
            answer_text=build_answer(status="forbidden", intent_id=None, custom_lead="Assistente indisponível no modo TV."),
            request_id=request_id,
            answer_id=answer_id,
        )
        return result.to_dict()

    scope = dict(scope or {})
    if "id_empresa" not in scope:
        scope["id_empresa"] = claims.get("id_empresa")
    try:
        scope["id_empresa"] = int(scope["id_empresa"])
    except (TypeError, ValueError):
        result = EngineResult(
            status="forbidden",
            answer_text="Escopo de empresa inválido.",
            request_id=request_id,
            answer_id=answer_id,
        )
        return result.to_dict()

    branch_scope = scope.get("branch_scope") or scope.get("filiais") or []
    if scope.get("id_filial") is not None and not branch_scope:
        raw = scope.get("id_filial")
        branch_scope = raw if isinstance(raw, list) else [raw]
    if scope.get("id_filiais") and not branch_scope:
        branch_scope = scope.get("id_filiais")
    ctx = invalidate_if_scope_changed(conversation_context, claims, list(branch_scope))
    # Mudança de período na tela invalida follow-up da investigação anterior.
    ui_period_early = _ui_period(scope)
    if ui_period_early and ctx.get("last_investigation"):
        prev = ((ctx.get("last_investigation") or {}).get("params") or {})
        if prev.get("dt_ini") and (
            str(prev.get("dt_ini"))[:10] != ui_period_early.dt_ini.isoformat()
            or str(prev.get("dt_fim"))[:10] != ui_period_early.dt_fim.isoformat()
        ):
            ctx = {**ctx, "last_investigation": None}

    norm = normalize_text(text)
    if len(norm.display) > limits.max_message_chars:
        result = EngineResult(
            status="validation_failed",
            answer_text=f"Mensagem muito longa (máx. {limits.max_message_chars} caracteres).",
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=ctx,
        )
        return result.to_dict()

    if detect_injection(norm.display):
        result = EngineResult(
            status="forbidden",
            answer_text=build_answer(
                status="forbidden",
                intent_id=None,
                custom_lead="Não posso atender a esse tipo de pedido.",
            ),
            confidence=1.0,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=ctx,
        )
        return result.to_dict()

    if detect_mutation_request(norm.display):
        result = EngineResult(
            status="mutation_denied",
            answer_text=build_answer(status="mutation_denied", intent_id=None),
            confidence=1.0,
            deep_link="/goals",
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=ctx,
        )
        return result.to_dict()

    if detect_sensitive_probe(norm.display) and not can_view_sensitive_financials(claims):
        result = EngineResult(
            status="forbidden",
            answer_text=build_answer(
                status="forbidden",
                intent_id="profit.overview",
                custom_lead="Lucro, margem e custo não estão disponíveis para o seu perfil.",
            ),
            intent_id="profit.overview",
            confidence=0.95,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=ctx,
        )
        return result.to_dict()

    pending_override, scope, consumed = consume_pending_disambiguation(norm.display, ctx, scope)
    if consumed and pending_override:
        parsed = pending_override
    else:
        parsed = parse_intent(norm.display)

    # Phase 3 — follow-up de investigação (usa escopo vigente, não o histórico como auth).
    from app.intelligence.investigation import (
        answer_followup,
        build_investigation_context,
        detect_followup_action,
        detect_investigation_intent,
        format_deterministic_answer,
        maybe_narrate_with_jarvis,
        run_finance_investigation,
        run_sales_investigation,
    )

    follow_action = detect_followup_action(norm.display, ctx.get("last_investigation"))
    if follow_action:
        last_inv = ctx.get("last_investigation") or {}
        follow_intent = (
            "finance.investigate_portfolio"
            if str(last_inv.get("domain") or "") == "finance_portfolio"
            else "sales.investigate_variation"
        )
        follow_tool = get_tool(follow_intent) or {
            "name": follow_intent,
            "screens": ["finance"] if follow_intent.startswith("finance") else ["sales"],
        }
        ok_follow, _reason_follow = reauthorize(claims, follow_tool, follow_tool.get("screens"))
        if not ok_follow:
            result = EngineResult(
                status="forbidden",
                answer_text=build_answer(status="forbidden", intent_id=follow_intent),
                intent_id=follow_intent,
                confidence=0.9,
                request_id=request_id,
                answer_id=answer_id,
                conversation_context={**ctx, "last_investigation": None},
            )
            return result.to_dict()
        inv = answer_followup(
            follow_action, last_inv, claims, scope, evidence
        )
        tool_meta.append(
            {
                "tool_name": f"followup:{follow_action}",
                "status": inv.get("status") or "ok",
                "latency_ms": inv.get("_latency_ms"),
                "evidence_id": inv.get("evidence_id"),
            }
        )
        det = format_deterministic_answer(inv)
        narr = (
            maybe_narrate_with_jarvis(inv)
            if inv.get("status") in {None, "ok"}
            else {"used_llm": False, "text": None, "reason": "skipped"}
        )
        answer_text = det
        if narr.get("used_llm") and narr.get("text"):
            answer_text = f"{det}\n\n—\n{narr['text']}"
        elif narr.get("reason") and narr.get("reason") not in {None, "openai_not_configured", "skipped"}:
            answer_text = (
                f"{det}\n\nA explicação adicional não ficou disponível. "
                "Os números acima continuam válidos."
            )
        period_ctx = (inv.get("scope") or {})
        if not period_ctx.get("dt_ini") and ctx.get("last_period"):
            period_ctx = ctx.get("last_period") or {}
        status = "ok" if inv.get("status") in {None, "ok"} else str(inv.get("status") or "ok")
        if status not in VALID_STATUSES:
            status = "ok" if inv.get("headline") else "unsupported"
        result = EngineResult(
            status=status if status in VALID_STATUSES else "ok",
            answer_text=answer_text,
            intent_id=follow_intent,
            confidence=0.9,
            suggestions=list(inv.get("follow_ups") or [])[:4],
            deep_link="/sales" if inv.get("domain") == "sales_variation" else "/finance",
            evidence_ids=[eid for eid in [inv.get("evidence_id")] if eid],
            tool_calls_meta=tool_meta,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=update_after_turn(
                ctx,
                intent_id="sales.investigate_variation"
                if inv.get("domain") == "sales_variation"
                else "finance.investigate_portfolio",
                slots={},
                period=period_ctx if isinstance(period_ctx, dict) else None,
                entities=[],
                pending=None,
                last_investigation=build_investigation_context(inv, period_ctx if isinstance(period_ctx, dict) else None),
            ),
        )
        out = result.to_dict()
        out["investigation"] = {
            "domain": inv.get("domain"),
            "totals": inv.get("totals"),
            "used_llm": bool(narr.get("used_llm")),
            "llm_reason": narr.get("reason"),
            "follow_ups": inv.get("follow_ups") or [],
            "additive_warning": inv.get("additive_warning"),
            "latency_ms": inv.get("_latency_ms"),
        }
        return out

    # Boost: perguntas naturais de investigação (sempre executar, não clarificar)
    forced = detect_investigation_intent(norm.display)
    if forced and (
        parsed.action in {"unknown", "clarify"}
        or not parsed.intent_id
        or parsed.intent_id != forced
        or parsed.confidence < 0.85
    ):
        from app.intelligence.parser import ParseResult

        parsed = ParseResult(intent_id=forced, confidence=0.92, slots={}, action="execute")

    if parsed.action == "unknown" or not parsed.intent_id:
        suggestions = _default_suggestions(claims)
        result = EngineResult(
            status="unknown",
            answer_text=build_answer(
                status="unknown",
                intent_id=None,
                suggestions=suggestions,
            ),
            confidence=parsed.confidence,
            suggestions=suggestions,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=update_after_turn(
                ctx, intent_id=None, slots={}, period=None, entities=[], pending=None
            ),
        )
        return result.to_dict()

    intent_meta = get_intent(parsed.intent_id) or {}
    tool_name = str(intent_meta.get("tool") or parsed.intent_id)
    tool_spec = get_tool(tool_name) or {
        "name": tool_name,
        "screens": [intent_meta.get("screen_key")] if intent_meta.get("screen_key") else [],
        "requires_sensitive": bool(
            intent_meta.get("requires_sensitive_role") or intent_meta.get("requires_sensitive")
        ),
    }

    parsed_slots_with_intent = {**parsed.slots, "intent_id": parsed.intent_id}
    scope, branch_result = _resolve_branch_from_slots(parsed_slots_with_intent, scope, claims)
    if branch_result and branch_result.status == "ambiguous":
        options = branch_result.candidates
        answer = build_answer(
            status="clarification_required",
            intent_id=parsed.intent_id,
            custom_lead=branch_result.message or "Qual filial você quer consultar?",
            clarification_options=options,
        )
        pending = {"kind": "branch", "intent_id": parsed.intent_id, "options": options}
        result = EngineResult(
            status="clarification_required",
            answer_text=answer,
            intent_id=parsed.intent_id,
            confidence=parsed.confidence,
            clarification_options=options,
            clarification_kind="branch",
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=update_after_turn(
                ctx,
                intent_id=parsed.intent_id,
                slots=parsed.slots,
                period=None,
                entities=[],
                pending=pending,
            ),
        )
        return result.to_dict()
    if branch_result and branch_result.status == "not_found" and branch_result.message:
        parsed.slots["filial_resolve_warning"] = branch_result.message

    ok, reason = reauthorize(claims, tool_spec, tool_spec.get("screens"))
    if not ok:
        status = "forbidden"
        result = EngineResult(
            status=status,
            answer_text=build_answer(status=status, intent_id=parsed.intent_id),
            intent_id=parsed.intent_id,
            confidence=parsed.confidence,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=ctx,
        )
        return result.to_dict()

    period = _resolve_period(parsed, scope)
    deep_link = deep_link_for_intent(parsed.intent_id, intent_meta)

    # Clarificação: mês ambíguo / candidatos de intent
    if parsed.action == "clarify" and parsed.intent_id == "sales.overview" and parsed.slots.get("ambiguous_year"):
        year = period.dt_ini.year
        options = [
            {"label": f"{period.label} / {year}", "value": f"{year}"},
            {"label": f"{period.label} / {year - 1}", "value": f"{year - 1}"},
        ]
        answer = build_answer(
            status="clarification_required",
            intent_id=parsed.intent_id,
            period_label=period.label,
            clarification_options=options,
            custom_lead=f"Você quer o faturamento de {period.label} de {year} ou de {year - 1}?",
            deep_link=deep_link,
        )
        pending = {"kind": "period_year", "intent_id": parsed.intent_id, "options": options}
        result = EngineResult(
            status="clarification_required",
            answer_text=answer,
            intent_id=parsed.intent_id,
            confidence=parsed.confidence,
            clarification_options=options,
            clarification_kind="period_year",
            deep_link=deep_link,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=update_after_turn(
                ctx,
                intent_id=parsed.intent_id,
                slots=parsed.slots,
                period={"dt_ini": period.dt_ini.isoformat(), "dt_fim": period.dt_fim.isoformat(), "label": period.label},
                entities=[],
                pending=pending,
            ),
        )
        return result.to_dict()

    # Cliente com devedor: resolver nome completo quando há um candidato claro
    if parsed.intent_id == "customer.open_titles" and parsed.slots.get("customer_name"):
        search_meta = search_customers(claims, scope, str(parsed.slots["customer_name"]), evidence=evidence)
        tool_meta.append(
            {k: search_meta.get(k) for k in ("tool_name", "status", "latency_ms", "evidence_id", "result_hash")}
        )
        candidates = ((search_meta.get("result") or {}).get("candidates")) or []
        unique: dict[str, dict[str, Any]] = {}
        for c in candidates:
            label = str(c.get("nome") or "").strip()
            if label:
                unique.setdefault(label, c)
        if len(unique) == 1:
            parsed.slots["customer_name"] = next(iter(unique.keys()))
        elif len(unique) > 1:
            options = [
                {"label": nome, "value": c.get("ref"), "documento_masked": c.get("documento_masked")}
                for nome, c in list(unique.items())[:5]
            ]
            answer = build_answer(
                status="clarification_required",
                intent_id=parsed.intent_id,
                tool_result=search_meta.get("result"),
                deep_link=deep_link,
                custom_lead="Encontrei mais de um cliente com esse nome. Qual deles?",
            )
            result = EngineResult(
                status="clarification_required",
                answer_text=answer,
                intent_id=parsed.intent_id,
                confidence=parsed.confidence,
                clarification_options=options,
                clarification_kind="customer",
                deep_link=deep_link,
                evidence_ids=[eid for eid in [search_meta.get("evidence_id")] if eid],
                tool_calls_meta=tool_meta,
                request_id=request_id,
                answer_id=answer_id,
                conversation_context=update_after_turn(
                    ctx,
                    intent_id=parsed.intent_id,
                    slots=parsed.slots,
                    period=None,
                    entities=list(unique.values()),
                    pending={"kind": "customer", "intent_id": parsed.intent_id, "options": options},
                ),
            )
            return result.to_dict()

    # Cliente: sempre desambiguar se múltiplos
    if parsed.intent_id in {"customer.search", "customer.overview"} and parsed.slots.get("customer_name"):
        search_meta = search_customers(claims, scope, str(parsed.slots["customer_name"]), evidence=evidence)
        tool_meta.append({k: search_meta.get(k) for k in ("tool_name", "status", "latency_ms", "evidence_id", "result_hash")})
        candidates = ((search_meta.get("result") or {}).get("candidates")) or []
        if len(candidates) != 1:
            answer = build_answer(
                status="clarification_required",
                intent_id=parsed.intent_id,
                tool_result=search_meta.get("result"),
                deep_link=deep_link,
            )
            options = [
                {"label": c.get("nome"), "value": c.get("ref"), "documento_masked": c.get("documento_masked")}
                for c in candidates[:5]
            ]
            result = EngineResult(
                status="clarification_required",
                answer_text=answer,
                intent_id=parsed.intent_id,
                confidence=parsed.confidence,
                clarification_options=options,
                clarification_kind="customer",
                deep_link=deep_link,
                evidence_ids=[eid for eid in [search_meta.get("evidence_id")] if eid],
                tool_calls_meta=tool_meta,
                request_id=request_id,
                answer_id=answer_id,
                conversation_context=update_after_turn(
                    ctx,
                    intent_id=parsed.intent_id,
                    slots=parsed.slots,
                    period=None,
                    entities=candidates,
                    pending={"kind": "customer", "intent_id": parsed.intent_id, "options": options},
                ),
            )
            return result.to_dict()

    if parsed.action == "clarify":
        options = [
            {
                "label": _intent_label(c.get("intent_id")),
                "value": c.get("intent_id"),
            }
            for c in parsed.candidates[:3]
        ]
        answer = build_answer(
            status="clarification_required",
            intent_id=parsed.intent_id,
            clarification_options=options,
            suggestions=_default_suggestions(claims),
            deep_link=deep_link,
            custom_lead="Entendi parte da pergunta — qual dessas consultas você quer?",
        )
        result = EngineResult(
            status="clarification_required",
            answer_text=answer,
            intent_id=parsed.intent_id,
            confidence=parsed.confidence,
            clarification_options=options,
            clarification_kind="intent",
            suggestions=_default_suggestions(claims),
            deep_link=deep_link,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=update_after_turn(
                ctx,
                intent_id=parsed.intent_id,
                slots=parsed.slots,
                period=None,
                entities=[],
                pending={"kind": "intent", "options": options},
            ),
        )
        return result.to_dict()

    # Execução — Phase 3 investigações (formato + narrativa + continuidade)
    if parsed.intent_id in {"sales.investigate_variation", "finance.investigate_portfolio"}:
        ok, reason = reauthorize(claims, tool_spec, tool_spec.get("screens"))
        if not ok:
            result = EngineResult(
                status="forbidden",
                answer_text=build_answer(status="forbidden", intent_id=parsed.intent_id),
                intent_id=parsed.intent_id,
                confidence=parsed.confidence,
                request_id=request_id,
                answer_id=answer_id,
                conversation_context=ctx,
            )
            return result.to_dict()
        period = _resolve_period(parsed, scope)
        deep_link = deep_link_for_intent(parsed.intent_id, intent_meta)
        if parsed.intent_id == "sales.investigate_variation":
            inv = run_sales_investigation(
                claims,
                scope,
                {"dt_ini": period.dt_ini.isoformat(), "dt_fim": period.dt_fim.isoformat()},
                evidence,
            )
        else:
            inv = run_finance_investigation(claims, scope, evidence)
        tool_meta.append(
            {
                "tool_name": parsed.intent_id,
                "status": inv.get("status") or "ok",
                "latency_ms": inv.get("_latency_ms"),
                "evidence_id": inv.get("evidence_id"),
            }
        )
        map_status = {
            "ok": "ok",
            "no_data": "no_data",
            "unavailable": "unsupported",
            "forbidden_scope": "forbidden",
            "period_too_long": "validation_failed",
            "validation_failed": "validation_failed",
        }
        status = map_status.get(str(inv.get("status") or "ok"), "ok")
        det = format_deterministic_answer(inv)
        narr = maybe_narrate_with_jarvis(inv) if status == "ok" else {"used_llm": False, "text": None, "reason": "skipped"}
        answer_text = det
        if narr.get("used_llm") and narr.get("text"):
            answer_text = f"{det}\n\n—\n{narr['text']}"
        elif status == "ok" and narr.get("reason") and narr.get("reason") not in {
            None,
            "openai_not_configured",
            "skipped",
        }:
            answer_text = (
                f"{det}\n\nA explicação adicional não ficou disponível. "
                "Os números acima continuam válidos."
            )
        period_payload = {
            "dt_ini": period.dt_ini.isoformat(),
            "dt_fim": period.dt_fim.isoformat(),
            "label": period.label,
        }
        suggestions = list(inv.get("follow_ups") or [])[:4] or _follow_up_suggestions(claims, parsed.intent_id)
        result = EngineResult(
            status=status,
            answer_text=answer_text,
            intent_id=parsed.intent_id,
            confidence=parsed.confidence,
            suggestions=suggestions,
            deep_link=deep_link,
            evidence_ids=[eid for eid in [inv.get("evidence_id")] if eid],
            tool_calls_meta=tool_meta,
            request_id=request_id,
            answer_id=answer_id,
            conversation_context=update_after_turn(
                ctx,
                intent_id=parsed.intent_id,
                slots=parsed.slots,
                period=period_payload,
                entities=[],
                pending=None,
                last_investigation=build_investigation_context(inv, period_payload),
            ),
        )
        out = result.to_dict()
        out["investigation"] = {
            "domain": inv.get("domain"),
            "totals": inv.get("totals"),
            "dimension_views": {
                k: {
                    "shown_count": (v or {}).get("shown_count"),
                    "truncated": (v or {}).get("truncated"),
                    "residual_delta": (v or {}).get("residual_delta"),
                }
                for k, v in (inv.get("dimension_views") or {}).items()
            },
            "used_llm": bool(narr.get("used_llm")),
            "llm_reason": narr.get("reason"),
            "follow_ups": inv.get("follow_ups") or [],
            "additive_warning": inv.get("additive_warning"),
            "latency_ms": inv.get("_latency_ms"),
            "freshness": inv.get("freshness"),
        }
        return out

    # Execução
    args = {
        "dt_ini": period.dt_ini,
        "dt_fim": period.dt_fim,
        **{k: v for k, v in parsed.slots.items() if k not in {"ambiguous_year", "period_label"}},
    }
    exec_result = execute_tool(tool_name, args, claims, scope, evidence=evidence)
    tool_meta.append(
        {k: exec_result.get(k) for k in ("tool_name", "status", "latency_ms", "evidence_id", "result_hash", "row_count")}
    )

    status = "ok"
    if exec_result.get("status") == "forbidden":
        status = "forbidden"
    elif exec_result.get("status") == "unsupported":
        status = "unsupported"
    elif exec_result.get("status") == "error":
        status = "unsupported"
    elif exec_result.get("status") == "navigate_only":
        status = "ok"

    payload = exec_result.get("result")
    if isinstance(payload, dict) and payload.get("deep_link"):
        deep_link = str(payload["deep_link"])

    answer = build_answer(
        status=status,
        intent_id=parsed.intent_id,
        period_label=period.label,
        scope_label=_scope_label(scope),
        tool_result=payload,
        deep_link=deep_link,
        suggestions=_default_suggestions(claims) if status in {"unknown", "unsupported"} else None,
        slots=parsed.slots,
    )
    warning = parsed.slots.get("filial_resolve_warning")
    if warning and status == "ok":
        answer = f"{answer}\n\n({warning})"

    if status == "ok" and not evidence.validate_numbers(answer, evidence.ids()):
        answer = uncertain_answer(deep_link)

    follow_ups = _follow_up_suggestions(claims, parsed.intent_id) if status == "ok" else []
    ctx2 = update_after_turn(
        ctx,
        intent_id=parsed.intent_id,
        slots=parsed.slots,
        period={"dt_ini": period.dt_ini.isoformat(), "dt_fim": period.dt_fim.isoformat(), "label": period.label},
        entities=[],
        pending=None,
    )
    result = EngineResult(
        status=status,
        answer_text=answer,
        intent_id=parsed.intent_id,
        confidence=parsed.confidence,
        deep_link=deep_link,
        evidence_ids=evidence.ids(),
        tool_calls_meta=tool_meta,
        request_id=request_id,
        answer_id=answer_id,
        conversation_context=ctx2,
        suggestions=follow_ups if follow_ups else (_default_suggestions(claims) if status == "unknown" else []),
    )
    return result.to_dict()
