"""Consumo de pending_disambiguation — respostas curtas após clarificação."""

from __future__ import annotations

from typing import Any, Optional

from app.intelligence.normalize import fold_key, normalize_text
from app.intelligence.parser import ParseResult, parse_intent
from app.intelligence.periods import parse_period


def _match_option(text_fold: str, options: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    for opt in options:
        val = str(opt.get("value") or "").strip()
        label = str(opt.get("label") or "").strip()
        if val and (val.casefold() in text_fold or text_fold == val.casefold()):
            return opt
        if label and (fold_key(label) in text_fold or text_fold in fold_key(label)):
            return opt
    return None


def consume_pending_disambiguation(
    text: str,
    context: dict[str, Any],
    scope: dict[str, Any],
) -> tuple[Optional[ParseResult], dict[str, Any], bool]:
    """
    Se há clarificação pendente e a mensagem parece uma escolha, devolve ParseResult
  pronto para execução e scope ajustado.
    Retorna (parsed_override, scope_out, consumed).
    """
    pending = context.get("pending_disambiguation")
    if not pending or not isinstance(pending, dict):
        return None, scope, False

    norm = normalize_text(text)
    fold = norm.fold
    if not fold:
        return None, scope, False

    kind = str(pending.get("kind") or "")
    options = list(pending.get("options") or [])
    last_intent = pending.get("intent_id") or context.get("last_intent")
    last_slots = dict(context.get("last_slots") or {})
    last_period = context.get("last_period")

    matched = _match_option(fold, options) if options else None
    # Resposta curta sem lista de opções: usa texto direto para customer/branch
    direct = fold if len(fold.split()) <= 4 else None

    scope_out = dict(scope)

    if kind == "period_year" and last_intent:
        year_opt = matched
        if not year_opt and direct and direct.isdigit() and len(direct) == 4:
            year_opt = {"value": direct}
        if year_opt:
            try:
                year = int(year_opt.get("value"))
            except (TypeError, ValueError):
                return None, scope, False
            period_label = str(last_slots.get("period_label") or "mês")
            rebuilt = f"faturamento {period_label} {year}"
            parsed = parse_intent(rebuilt)
            if parsed.intent_id:
                parsed.slots.update(last_slots)
                parsed.slots["period_label"] = period_label
                parsed.action = "execute"
                return parsed, scope_out, True

    if kind == "customer" and last_intent in {"customer.search", "customer.open_titles", "customer.overview"}:
        pick = matched or ({"label": norm.display.strip()} if direct else None)
        if pick:
            name = str(pick.get("label") or pick.get("value") or norm.display).strip()
            slots = dict(last_slots)
            slots["customer_name"] = name
            slots["customer_query"] = name
            if pick.get("value") and not str(pick.get("value")).isdigit():
                slots["customer_ref"] = pick.get("value")
            period = None
            if last_period and isinstance(last_period, dict):
                period = parse_period(
                    f"{last_period.get('label') or ''} {last_period.get('dt_ini') or ''}"
                )
            parsed = ParseResult(
                intent_id=str(last_intent),
                confidence=0.94,
                slots=slots,
                period=period,
                action="execute",
            )
            return parsed, scope_out, True

    if kind == "branch":
        pick = matched
        if not pick and direct:
            pick = _match_option(direct, options)
        if pick and (pick.get("id_filial") or pick.get("value")):
            try:
                fid = int(pick.get("id_filial") or pick.get("value"))
            except (TypeError, ValueError):
                fid = None
            if fid:
                scope_out["id_filial"] = fid
                scope_out["id_filiais"] = [fid]
                scope_out["branch_scope"] = "selected"
                if pick.get("label"):
                    scope_out["filial_label"] = str(pick["label"])
                rebuilt = norm.display
                if last_intent == "sales.overview" and "faturamento" not in fold:
                    rebuilt = f"faturamento hoje na {pick.get('label') or fid}"
                parsed = parse_intent(rebuilt) if last_intent != "sales.overview" else parse_intent(rebuilt)
                if not parsed.intent_id and last_intent:
                    parsed = ParseResult(
                        intent_id=str(last_intent),
                        confidence=0.9,
                        slots=dict(last_slots),
                        period=parse_period(rebuilt) or None,
                        action="execute",
                    )
                parsed.action = "execute"
                parsed.slots.update(last_slots)
                if pick.get("label"):
                    parsed.slots["filial_label"] = str(pick["label"])
                return parsed, scope_out, True

    if kind == "intent" and matched:
        intent_id = str(matched.get("value") or matched.get("label") or "")
        if intent_id:
            parsed = parse_intent(str(matched.get("label") or intent_id))
            if not parsed.intent_id:
                parsed = ParseResult(intent_id=intent_id, confidence=0.85, slots=dict(last_slots), action="execute")
            else:
                parsed.action = "execute"
            return parsed, scope_out, True

    return None, scope, False
