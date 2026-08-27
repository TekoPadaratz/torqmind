"""Templates de resposta pt-BR (somente leitura, sem HTML)."""

from __future__ import annotations

import html
import re
from typing import Any

from app.intelligence.limits import MAX_RESPONSE_CHARS


_TAG_RE = re.compile(r"<[^>]+>")


def sanitize_text(value: str | None) -> str:
    text = str(value or "")
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    return text.strip()


def _fmt_money(value: Any) -> str | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _extract_faturamento(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("faturamento", "total_vendas", "s_fat", "receita"):
        if key in payload:
            return _fmt_money(payload.get(key))
    kpis = payload.get("kpis") if isinstance(payload.get("kpis"), dict) else None
    if kpis:
        for key in ("faturamento", "total_vendas"):
            if key in kpis:
                return _fmt_money(kpis.get(key))
    return None


def build_answer(
    *,
    status: str,
    intent_id: str | None,
    display_question: str | None = None,
    period_label: str | None = None,
    scope_label: str | None = None,
    tool_result: Any = None,
    evidence_summary: list[dict[str, Any]] | None = None,
    freshness: str | None = None,
    limitations: list[str] | None = None,
    deep_link: str | None = None,
    clarification_options: list[Any] | None = None,
    suggestions: list[str] | None = None,
    custom_lead: str | None = None,
) -> str:
    parts: list[str] = []

    if custom_lead:
        parts.append(sanitize_text(custom_lead))
    elif status == "mutation_denied":
        parts.append(
            "Não posso alterar dados pelo chat. Metas, preços, comissões e títulos só mudam nas telas do TorqMind."
        )
    elif status == "forbidden":
        parts.append("Você não tem permissão para esta consulta.")
    elif status == "unsupported":
        parts.append("Essa pergunta ainda não é suportada pelo Assistente.")
    elif status == "unknown":
        parts.append("Não entendi a pergunta com segurança o bastante para consultar os dados.")
    elif status == "clarification_required":
        parts.append("Preciso de um detalhe para continuar.")
    elif status == "no_data":
        parts.append("Não encontrei dados para o período e escopo informados.")
    else:
        fat = _extract_faturamento(tool_result)
        if fat and intent_id == "sales.overview":
            parts.append(f"Faturamento: {fat}.")
        elif isinstance(tool_result, dict) and tool_result.get("message"):
            parts.append(sanitize_text(str(tool_result["message"])))
        elif isinstance(tool_result, dict) and tool_result.get("capabilities"):
            caps = tool_result["capabilities"]
            lines = ["Posso ajudar com consultas somente leitura, por exemplo:"]
            for item in caps[:8]:
                label = item.get("label") or item.get("intent_id")
                lines.append(f"• {label}")
            parts.append("\n".join(lines))
        elif isinstance(tool_result, dict) and tool_result.get("candidates") is not None:
            cands = tool_result.get("candidates") or []
            if not cands:
                parts.append("Não encontrei clientes com esse nome.")
            else:
                lines = ["Encontrei mais de uma possibilidade. Qual cliente?"]
                for idx, c in enumerate(cands[:5], start=1):
                    nome = c.get("nome") or "—"
                    doc = c.get("documento_masked") or "—"
                    lines.append(f"{idx}) {nome} (doc {doc})")
                parts.append("\n".join(lines))
        else:
            parts.append("Consulta concluída com base nos dados do TorqMind.")

    if period_label:
        parts.append(f"Período: {sanitize_text(period_label)}.")
    if scope_label:
        parts.append(f"Escopo: {sanitize_text(scope_label)}.")

    if clarification_options:
        opts = []
        for opt in clarification_options[:5]:
            if isinstance(opt, dict):
                opts.append(str(opt.get("label") or opt.get("value") or opt))
            else:
                opts.append(str(opt))
        if opts and status == "clarification_required":
            parts.append("Opções: " + " | ".join(opts))

    if evidence_summary:
        parts.append(f"Evidências: {len(evidence_summary)} registro(s) consultado(s).")
    if freshness:
        parts.append(f"Atualização: {sanitize_text(freshness)}.")
    if limitations:
        parts.append("Limitações: " + "; ".join(sanitize_text(x) for x in limitations[:3]) + ".")

    if deep_link:
        parts.append(f"Ver na tela: {sanitize_text(deep_link)}")

    if suggestions and status in {"unknown", "clarification_required", "ok"}:
        sug = [sanitize_text(s) for s in suggestions[:4] if s]
        if sug:
            parts.append("Sugestões: " + " · ".join(sug))

    parts.append("Respostas baseadas nos dados do TorqMind. O assistente não altera informações.")
    text = "\n".join(p for p in parts if p)
    if len(text) > MAX_RESPONSE_CHARS:
        text = text[: MAX_RESPONSE_CHARS - 20].rstrip() + "…"
    return text
