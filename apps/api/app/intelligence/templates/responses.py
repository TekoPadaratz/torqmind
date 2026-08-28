"""Templates de resposta pt-BR — conversa operacional, sem jargão de pipeline."""

from __future__ import annotations

import html
import re
from typing import Any

from app.intelligence.limits import MAX_RESPONSE_CHARS


_TAG_RE = re.compile(r"<[^>]+>")


_SCREEN_LABELS: dict[str, str] = {
    "/sales": "Vendas",
    "/customers": "Clientes",
    "/finance": "Financeiro",
    "/cash": "Caixa",
    "/fraud": "Antifraude",
    "/goals": "Metas",
    "/team": "Equipe",
    "/inventory": "Estoque",
    "/pricing": "Precificação",
    "/profit-management": "Gestão de Lucro",
}


def sanitize_text(value: str | None) -> str:
    text = str(value or "")
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    return text.strip()


def _screen_label(deep_link: str | None) -> str | None:
    if not deep_link:
        return None
    path = str(deep_link).split("?")[0].rstrip("/") or "/"
    return _SCREEN_LABELS.get(path) or _SCREEN_LABELS.get(f"/{path.strip('/')}")


def _fmt_money(value: Any) -> str | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_int(value: Any) -> str | None:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return f"{n:,}".replace(",", ".")


def _extract_faturamento(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("faturamento", "total_vendas", "s_fat", "receita", "saidas"):
        if key in payload:
            return _fmt_money(payload.get(key))
    kpis = payload.get("kpis") if isinstance(payload.get("kpis"), dict) else None
    if kpis:
        for key in ("faturamento", "total_vendas", "saidas"):
            if key in kpis:
                return _fmt_money(kpis.get(key))
    commercial = payload.get("commercial_kpis") if isinstance(payload.get("commercial_kpis"), dict) else None
    if commercial and "saidas" in commercial:
        return _fmt_money(commercial.get("saidas"))
    return None


def _extract_qtd_vendas(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("qtd_vendas", "qtd_saidas"):
        if key in payload:
            try:
                return int(payload.get(key) or 0)
            except (TypeError, ValueError):
                pass
    commercial = payload.get("commercial_kpis") if isinstance(payload.get("commercial_kpis"), dict) else None
    if commercial and "qtd_saidas" in commercial:
        try:
            return int(commercial.get("qtd_saidas") or 0)
        except (TypeError, ValueError):
            return None
    stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else None
    if stats and "vendas" in stats:
        try:
            return int(stats.get("vendas") or 0)
        except (TypeError, ValueError):
            return None
    return None


def _finance_titles_line(tool_result: Any, period_label: str | None, title_tipo: int | None) -> str | None:
    totals = (tool_result or {}).get("totals") if isinstance(tool_result, dict) else None
    if not isinstance(totals, dict):
        return None
    aberto = _fmt_money(totals.get("valor_aberto"))
    if not aberto:
        return None
    when = _period_phrase(period_label)
    kind = "a pagar" if int(title_tipo or 0) == 0 else "a receber"
    total = int((tool_result or {}).get("total") or 0)
    suffix = f" ({total} título(s) em aberto)" if total else ""
    return f"Você tem {aberto} em contas {kind} {when}{suffix}."


def _customer_debt_line(tool_result: Any, customer_name: str | None) -> str | None:
    totals = (tool_result or {}).get("totals") if isinstance(tool_result, dict) else None
    if not isinstance(totals, dict):
        return None
    label = sanitize_text(customer_name or "o cliente")
    total = int((tool_result or {}).get("total") or 0)
    if total <= 0:
        return f"Não encontrei títulos em aberto para {label}."
    aberto = _fmt_money(totals.get("valor_aberto"))
    if not aberto:
        return f"Encontrei {total} título(s) em aberto para {label}, mas sem valor consolidado."
    return f"{label} tem {aberto} em aberto ({total} título(s))."


def _scope_phrase(scope_label: str | None) -> str | None:
    if not scope_label:
        return None
    text = sanitize_text(scope_label)
    lower = text.lower()
    if "todas as filiais" in lower:
        return "em todas as filiais do escopo"
    if lower.startswith("filial "):
        apelido = text.split("filial", 1)[-1].strip()
        if apelido:
            return f"na {apelido}"
    return None


def _period_phrase(period_label: str | None) -> str:
    label = sanitize_text(period_label or "").lower()
    if not label:
        return "no período consultado"
    if label == "hoje":
        return "hoje"
    if label == "ontem":
        return "ontem"
    if label in {"esta semana", "esse mês", "este mês", "este ano", "esse ano"}:
        return label
    if label.startswith("mês"):
        return label
    return f"em {label}"


def uncertain_answer(deep_link: str | None = None) -> str:
    screen = _screen_label(deep_link)
    if screen:
        return (
            f"Hmm… não consigo ter certeza sobre essa informação agora. "
            f"Confira na tela de {screen} para validar com os dados atualizados."
        )
    return (
        "Hmm… não consigo ter certeza sobre essa informação agora. "
        "Abra a tela correspondente no TorqMind para confirmar."
    )


def _sales_overview_line(tool_result: Any, period_label: str | None) -> str | None:
    fat = _extract_faturamento(tool_result)
    if not fat:
        return None
    qtd = _extract_qtd_vendas(tool_result)
    when = _period_phrase(period_label)
    if when == "hoje":
        lead = f"Hoje o faturamento está em {fat}"
    elif when == "ontem":
        lead = f"Ontem o faturamento foi de {fat}"
    else:
        lead = f"O faturamento {when} está em {fat}"
    if qtd is not None and qtd > 0:
        lead += f", com {_fmt_int(qtd)} vendas"
    return lead + "."


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
    slots: dict[str, Any] | None = None,
) -> str:
  # scope_label / evidence_summary mantidos na assinatura para compatibilidade interna
    parts: list[str] = []

    if custom_lead:
        parts.append(sanitize_text(custom_lead))
    elif status == "mutation_denied":
        parts.append(
            "Não posso alterar dados por aqui. Metas, preços e títulos só mudam nas telas do TorqMind."
        )
    elif status == "forbidden":
        parts.append("Esse tipo de consulta não está liberada para o seu perfil.")
    elif status == "unsupported":
        screen = _screen_label(deep_link)
        if screen:
            parts.append(f"Essa pergunta ainda não está no assistente. Tente abrir {screen} no menu.")
        else:
            parts.append("Essa pergunta ainda não está disponível no assistente.")
    elif status == "unknown":
        parts.append(
            "Hmm… não entendi bem o suficiente para buscar nos dados. "
            "Tente algo como “faturamento de hoje”, “vendas por hora” ou “quanto o cliente X está devendo”."
        )
    elif status == "clarification_required":
        parts.append("Só preciso confirmar um detalhe para seguir com a consulta.")
    elif status == "no_data":
        screen = _screen_label(deep_link) or "Vendas"
        parts.append(
            f"Não encontrei dados para { _period_phrase(period_label) }. "
            f"Vale conferir em {screen}."
        )
    elif status in {"timeout", "overloaded"}:
        parts.append("A consulta demorou mais que o esperado. Tente novamente em instantes.")
    elif status == "validation_failed":
        parts.append(uncertain_answer(deep_link))
    else:
        if intent_id == "sales.overview":
            sales_line = _sales_overview_line(tool_result, period_label)
            if sales_line:
                scope_hint = _scope_phrase(scope_label)
                parts.append(f"{sales_line}{f' {scope_hint}' if scope_hint else ''}")
            else:
                parts.append(uncertain_answer(deep_link))
        elif intent_id == "finance.titles":
            line = _finance_titles_line(tool_result, period_label, (slots or {}).get("title_tipo"))
            parts.append(line or uncertain_answer(deep_link))
        elif intent_id == "customer.open_titles":
            line = _customer_debt_line(tool_result, (slots or {}).get("customer_name"))
            parts.append(line or uncertain_answer(deep_link))
        elif isinstance(tool_result, dict) and tool_result.get("message"):
            parts.append(sanitize_text(str(tool_result["message"])))
        elif isinstance(tool_result, dict) and tool_result.get("capabilities"):
            caps = tool_result["capabilities"]
            lines = ["Posso ajudar com consultas de leitura, por exemplo:"]
            for item in caps[:6]:
                label = item.get("label") or item.get("intent_id")
                lines.append(f"• {label}")
            parts.append("\n".join(lines))
        elif isinstance(tool_result, dict) and tool_result.get("candidates") is not None:
            cands = tool_result.get("candidates") or []
            if not cands:
                parts.append("Não encontrei clientes com esse nome.")
            else:
                lines = ["Encontrei mais de um cliente. Qual deles?"]
                for idx, c in enumerate(cands[:5], start=1):
                    nome = c.get("nome") or "—"
                    doc = c.get("documento_masked") or "—"
                    lines.append(f"{idx}) {nome} (doc {doc})")
                parts.append("\n".join(lines))
        else:
            fat = _extract_faturamento(tool_result)
            if fat:
                parts.append(f"Encontrei {fat} no período consultado.")
            else:
                parts.append("Consulta concluída. Abra a tela indicada para ver o detalhe.")

    if clarification_options:
        opts = []
        for opt in clarification_options[:5]:
            if isinstance(opt, dict):
                opts.append(str(opt.get("label") or opt.get("value") or opt))
            else:
                opts.append(str(opt))
        if opts and status == "clarification_required":
            parts.append("Opções: " + " · ".join(opts))

    if suggestions and status in {"unknown", "clarification_required"}:
        sug = [sanitize_text(s) for s in suggestions[:4] if s]
        if sug:
            parts.append("Sugestões: " + " · ".join(sug))

    text = "\n".join(p for p in parts if p)
    if len(text) > MAX_RESPONSE_CHARS:
        text = text[: MAX_RESPONSE_CHARS - 20].rstrip() + "…"
    return text
