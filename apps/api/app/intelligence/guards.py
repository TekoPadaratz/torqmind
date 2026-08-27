"""Guardas determinísticas: mutação, injeção e sondagem sensível."""

from __future__ import annotations

import base64
import re
from typing import Iterable

from app.intelligence.normalize import fold_key, normalize_text


_MUTATION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\b(mude|mudar|alter(e|ar)|definir|defina|aument(e|ar)|reduz(a|ir)|diminu(a|ir))\b",
        r"\b(cri(e|ar)|excluir|exclu(a|i)|apagar|delet(e|ar)|remover|cancele|cancelar)\b",
        r"\b(baixar\s+t[ií]tulo|quitar|liquidar|estornar)\b",
        r"\b(comiss[aã]o|meta|pre[cç]o|or[cç]amento)\b.{0,40}\b(mude|alter|defina|aument|reduz|criar|excluir)",
        r"\b(mude|alter|defina|aument|reduz).{0,40}\b(meta|comiss[aã]o|pre[cç]o|or[cç]amento|usu[aá]rio)",
        r"\bcriar\s+usu[aá]rio\b",
        r"\b(registrar|salvar|gravar|atualizar)\s+(pre[cç]o|meta|comiss[aã]o|or[cç]amento)\b",
        r"\b(upsert|write|insert|update|delete)\b",
    )
)

_INJECTION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"ignore\s+(all\s+)?(previous|prior|above)\s+(instructions?|prompts?)",
        r"ignor(e|ar)\s+(as\s+)?(instru[cç][oõ]es|regras)\s+(anteriores|acima)",
        r"system\s+prompt",
        r"voc[eê]\s+[eé]\s+(agora\s+)?(um\s+)?admin",
        r"role\s*play.*(admin|root|system)",
        r"mostre\s+(o\s+)?(sql|schema|segredo|secret|api[_\s-]?key|token)",
        r"show\s+(me\s+)?(the\s+)?(sql|schema|secrets?|api[_\s-]?key)",
        r"<script[\s>]",
        r"javascript:",
        r"\bDAN\b|\bjailbreak\b",
        r"developer\s+mode",
    )
)

_SENSITIVE_PROBE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\b(lucro|margem|cmv|custo|rentabilidade|markup)\b",
        r"\b(profit|margin|cost)\b",
        r"\bdre\b",
        r"\bsolv[eê]ncia\b",
        r"\bcusto\s+do\s+funcion[aá]rio\b",
        r"\bsal[aá]rio\b",
    )
)

_B64_BLOB_RE = re.compile(r"(?:[A-Za-z0-9+/]{40,}={0,2})")


def _any_match(patterns: Iterable[re.Pattern[str]], text: str) -> bool:
    return any(p.search(text) for p in patterns)


def detect_mutation_request(text: str | None) -> bool:
    norm = normalize_text(text)
    hay = f"{norm.lower} {norm.fold}"
    return _any_match(_MUTATION_PATTERNS, hay)


def _base64_decodes_to_injection(blob: str) -> bool:
    padded = blob + ("=" * ((4 - len(blob) % 4) % 4))
    try:
        raw = base64.b64decode(padded, validate=False)
        decoded = raw.decode("utf-8", errors="ignore")
    except Exception:
        return False
    if not decoded or len(decoded) < 8:
        return False
    lower = decoded.lower()
    return _any_match(_INJECTION_PATTERNS, lower) or "system prompt" in lower or "ignore previous" in lower


def detect_injection(text: str | None) -> bool:
    norm = normalize_text(text)
    hay = f"{norm.original} {norm.lower}"
    if _any_match(_INJECTION_PATTERNS, hay):
        return True
    for match in _B64_BLOB_RE.finditer(norm.original or ""):
        if _base64_decodes_to_injection(match.group(0)):
            return True
    return False


def detect_sensitive_probe(text: str | None) -> bool:
    """True quando o texto pede inferência de lucro/custo/margem."""
    key = fold_key(text)
    return _any_match(_SENSITIVE_PROBE_PATTERNS, key)
