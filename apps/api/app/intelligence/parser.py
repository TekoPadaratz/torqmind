"""Parser determinístico de intenções (sem LLM)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Optional

from app.intelligence.capability_map.loader import list_intents
from app.intelligence.limits import CONFIDENCE_CLARIFY, CONFIDENCE_EXECUTE
from app.intelligence.normalize import fold_key, normalize_text
from app.intelligence.periods import PeriodResult, default_period, parse_period


_METRIC_OPS = {
    "total": ("total", "soma", "faturamento total"),
    "media": ("media", "médio", "medio", "ticket medio", "ticket médio"),
    "ranking": ("ranking", "rank"),
    "top": ("top", "mais vendidos", "maiores"),
    "comparacao": ("comparacao", "comparação", "versus", "vs", "comparar"),
}

_ORDINAL_RE = re.compile(r"\b(o|a)\s+(primeiro|segundo|terceiro|quarto|quinto)\b", re.I)
_FILIAL_RE = re.compile(r"\bfilial\s+([a-z0-9][\w\s\-]{1,40})", re.I)
_CUSTOMER_RE = re.compile(
    r"\b(?:saldo|cliente|do cliente|da cliente|de)\s+(?:do\s+|da\s+|de\s+)?([a-zà-ú0-9][\wà-ú\s\.]{1,60})",
    re.I,
)


@dataclass
class ParseResult:
    intent_id: Optional[str]
    confidence: float
    slots: dict[str, Any] = field(default_factory=dict)
    period: Optional[PeriodResult] = None
    candidates: list[dict[str, Any]] = field(default_factory=list)
    action: str = "unknown"  # execute | clarify | unknown


def _synonym_score(fold: str, synonym: str) -> float:
    syn = fold_key(synonym)
    if not syn:
        return 0.0
    if fold == syn:
        return 1.0
    if syn in fold:
        # frase contém o sinônimo completo
        bonus = min(0.08, 0.02 * syn.count(" "))
        return 0.96 + bonus
    # fuzzy só depois de exact/prefix
    if fold.startswith(syn) or syn.startswith(fold):
        return 0.9
    ratio = SequenceMatcher(None, fold, syn).ratio()
    if ratio >= 0.92:
        return 0.85 + (ratio - 0.92) * 0.5
    if ratio >= 0.82:
        return 0.72
    return 0.0


def _extract_metric_op(fold: str) -> Optional[str]:
    for op, keys in _METRIC_OPS.items():
        for k in keys:
            if fold_key(k) in fold:
                return op
    return None


def _extract_customer_name(display: str, fold: str) -> Optional[str]:
    m = _CUSTOMER_RE.search(display)
    if m:
        name = m.group(1).strip(" .,!?")
        # corta stopwords de período/intent
        stop = {"hoje", "ontem", "agosto", "mes", "mês", "filial", "da", "do", "de"}
        parts = [p for p in name.split() if p.casefold() not in stop]
        if parts:
            return " ".join(parts[:4])
    # "saldo junior"
    if "saldo" in fold:
        rest = fold.split("saldo", 1)[-1].strip()
        if rest and len(rest.split()) <= 4:
            return rest.title() if rest else None
    return None


def _extract_ordinal(fold: str) -> Optional[int]:
    m = _ORDINAL_RE.search(fold)
    if not m:
        return None
    mapping = {"primeiro": 1, "segundo": 2, "terceiro": 3, "quarto": 4, "quinto": 5}
    return mapping.get(m.group(2).casefold())


def parse_intent(text: str | None) -> ParseResult:
    norm = normalize_text(text)
    fold = norm.fold
    if not fold:
        return ParseResult(intent_id=None, confidence=0.0, action="unknown")

    # Regras explícitas de alta confiança (preferidas a fuzzy)
    if any(k in fold for k in ("o que posso perguntar", "what can i ask", "capacidades", "o que voce faz")):
        return ParseResult(
            intent_id="assistant.capabilities",
            confidence=0.99,
            slots={},
            period=parse_period(norm.display),
            candidates=[{"intent_id": "assistant.capabilities", "confidence": 0.99, "matched": "capabilities"}],
            action="execute",
        )

    customer = _extract_customer_name(norm.display, fold)
    if "saldo" in fold and customer:
        period = parse_period(norm.display) or parse_period(fold)
        slots: dict[str, Any] = {"customer_name": customer, "customer_query": customer}
        if period:
            slots["period_label"] = period.label
            slots["ambiguous_year"] = period.ambiguous_year
        return ParseResult(
            intent_id="customer.search",
            confidence=0.95,
            slots=slots,
            period=period,
            candidates=[{"intent_id": "customer.search", "confidence": 0.95, "matched": "saldo"}],
            action="execute",
        )

    scored: list[tuple[float, dict[str, Any], str]] = []
    for intent in list_intents():
        best = 0.0
        best_syn = ""
        for syn in intent.get("synonyms") or []:
            s = _synonym_score(fold, str(syn))
            if s > best:
                best = s
                best_syn = str(syn)
        # boost por intent_id token
        iid = str(intent.get("intent_id") or "")
        if iid.split(".")[-1] in fold:
            best = max(best, 0.75)
        if best > 0:
            scored.append((best, intent, best_syn))

    scored.sort(key=lambda x: x[0], reverse=True)
    candidates = [
        {"intent_id": i.get("intent_id"), "confidence": round(s, 4), "matched": syn}
        for s, i, syn in scored[:5]
    ]

    if not scored:
        return ParseResult(intent_id=None, confidence=0.0, candidates=[], action="unknown")

    top_score, top_intent, _ = scored[0]
    second = scored[1][0] if len(scored) > 1 else 0.0
    # se empate próximo, clarificar
    if top_score >= CONFIDENCE_CLARIFY and (top_score - second) < 0.05 and second >= CONFIDENCE_CLARIFY:
        top_score = min(top_score, 0.88)

    period = parse_period(norm.display) or parse_period(fold)
    slots = {}
    metric_op = _extract_metric_op(fold)
    if metric_op:
        slots["metric_op"] = metric_op
    filial_m = _FILIAL_RE.search(norm.display)
    if filial_m:
        slots["filial_label"] = filial_m.group(1).strip()
    if customer:
        slots["customer_name"] = customer
        slots["customer_query"] = customer
    ordinal = _extract_ordinal(fold)
    if ordinal is not None:
        slots["ordinal"] = ordinal
    if period:
        slots["period_label"] = period.label
        slots["ambiguous_year"] = period.ambiguous_year

    intent_id = str(top_intent.get("intent_id"))

    if top_score >= CONFIDENCE_EXECUTE:
        action = "execute"
    elif top_score >= CONFIDENCE_CLARIFY:
        action = "clarify"
    else:
        action = "unknown"
        intent_id = None

    # mês nomeado ambíguo → clarify suave mantendo intent
    if action == "execute" and period and period.ambiguous_year and "faturamento" in fold:
        action = "clarify"
        # mantém confidence na faixa clarify
        top_score = min(top_score, 0.9)

    return ParseResult(
        intent_id=intent_id,
        confidence=round(float(top_score), 4),
        slots=slots,
        period=period,
        candidates=candidates,
        action=action,
    )


def ensure_period(parsed: ParseResult) -> PeriodResult:
    return parsed.period or default_period()
