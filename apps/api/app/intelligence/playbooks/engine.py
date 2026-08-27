"""Playbooks determinísticos versionados (hipóteses, não causalidade)."""

from __future__ import annotations

from typing import Any


PLAYBOOK_VERSION = "1"


def _action(
    title: str,
    *,
    owner: str,
    deadline: str,
    effort: str,
    risk: str,
    success: str,
    screen: str,
) -> dict[str, Any]:
    return {
        "title": title,
        "suggested_owner": owner,
        "deadline": deadline,
        "effort": effort,
        "risk": risk,
        "success_criteria": success,
        "screen": screen,
    }


def run_playbook(
    playbook_id: str,
    *,
    context: dict[str, Any] | None = None,
    can_view_profit: bool = False,
) -> dict[str, Any] | None:
    ctx = context or {}
    builders = {
        "revenue_drop": _revenue_drop,
        "delinquency_priority": _delinquency,
        "mix_shift": _mix_shift,
        "goals_pace": _goals_pace,
        "idle_hours": _idle_hours,
    }
    fn = builders.get(playbook_id)
    if not fn:
        return None
    result = fn(ctx)
    if not can_view_profit:
        # remove menções a lucro/custo/margem
        result["hypotheses"] = [h for h in result.get("hypotheses", []) if not _sensitive_text(h)]
        result["actions"] = [
            a for a in result.get("actions", []) if not _sensitive_text(str(a.get("title") or ""))
        ][:5]
        result["diagnosis"] = _strip_sensitive(str(result.get("diagnosis") or ""))
    result["playbook_id"] = playbook_id
    result["version"] = PLAYBOOK_VERSION
    result["actions"] = (result.get("actions") or [])[:5]
    return result


def _sensitive_text(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in ("lucro", "margem", "cmv", "custo", "rentab"))


def _strip_sensitive(text: str) -> str:
    return text


def _revenue_drop(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "diagnosis": "Queda de faturamento no período comparado ao referência.",
        "hypotheses": [
            "Menor volume em horários de pico.",
            "Mix deslocado para produtos de ticket menor.",
            "Filial específica puxando a média para baixo.",
            "Aumento de cancelamentos impactando o líquido.",
        ],
        "actions": [
            _action(
                "Revisar vendas por hora e reforçar cobertura nos picos",
                owner="Gerente de pista",
                deadline="48h",
                effort="médio",
                risk="baixo",
                success="Recuperar ≥50% da queda nos horários críticos",
                screen="/sales",
            ),
            _action(
                "Checar top produtos e rupturas de estoque",
                owner="Comercial",
                deadline="24h",
                effort="baixo",
                risk="baixo",
                success="Itens A sem ruptura",
                screen="/sales",
            ),
            _action(
                "Validar cancelamentos e operadores outliers",
                owner="Supervisor",
                deadline="24h",
                effort="médio",
                risk="médio",
                success="Cancelamentos dentro da média",
                screen="/fraud",
            ),
        ],
        "confidence": 0.7,
        "note": "Hipóteses operacionais — não afirmam causa única.",
    }


def _delinquency(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "diagnosis": "Priorização de inadimplência / títulos em atraso.",
        "hypotheses": [
            "Concentração em poucos clientes.",
            "Títulos recém-vencidos ainda sem cobrança.",
            "Divergência de baixa entre STG e mart (sincronização).",
        ],
        "actions": [
            _action(
                "Listar top inadimplentes e acionar cobrança",
                owner="Financeiro",
                deadline="24h",
                effort="médio",
                risk="baixo",
                success="Contato registrado nos 10 maiores saldos",
                screen="/customers",
            ),
            _action(
                "Conferir títulos abertos na tela de receber",
                owner="Financeiro",
                deadline="48h",
                effort="baixo",
                risk="baixo",
                success="Saldo reconciliado com a operação",
                screen="/finance?view=receivable",
            ),
        ],
        "confidence": 0.75,
        "note": "Hipóteses — validar no Xpert antes de ação jurídica.",
    }


def _mix_shift(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "diagnosis": "Deslocamento de mix de produtos/grupos.",
        "hypotheses": [
            "Combustível cedeu espaço para conveniência (ou o inverso).",
            "Promoção pontual distorceu o ranking.",
            "Ruptura em item âncora.",
        ],
        "actions": [
            _action(
                "Comparar top grupos vs período anterior",
                owner="Comercial",
                deadline="48h",
                effort="baixo",
                risk="baixo",
                success="Mix documentado com hipótese principal",
                screen="/sales",
            ),
            _action(
                "Checar curva ABC",
                owner="Comercial",
                deadline="48h",
                effort="baixo",
                risk="baixo",
                success="Itens A revisados",
                screen="/sales/abc",
            ),
        ],
        "confidence": 0.65,
        "note": "Hipóteses de mix — sem inferir margem sem permissão.",
    }


def _goals_pace(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "diagnosis": "Ritmo de meta abaixo ou acima do esperado.",
        "hypotheses": [
            "Ritmo diário insuficiente para fechar o mês.",
            "Filial específica abaixo do pace.",
            "Meta desatualizada frente ao histórico.",
        ],
        "actions": [
            _action(
                "Abrir projeção mensal de meta",
                owner="Gerente",
                deadline="hoje",
                effort="baixo",
                risk="baixo",
                success="Gap diário conhecido",
                screen="/goals",
            ),
            _action(
                "Alinhar equipe nos horários de maior conversão",
                owner="Gerente de pista",
                deadline="48h",
                effort="médio",
                risk="baixo",
                success="Pace diário recuperado por 3 dias",
                screen="/sales",
            ),
        ],
        "confidence": 0.7,
        "note": "O assistente não altera metas.",
    }


def _idle_hours(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "diagnosis": "Janelas ociosas relevantes na curva horária.",
        "hypotheses": [
            "Subdimensionamento de equipe em faixa específica.",
            "Demanda real baixa (padrão histórico).",
            "Fila / demora reduz conversão no pico vizinho.",
        ],
        "actions": [
            _action(
                "Revisar escala nos horários ociosos e de pico",
                owner="Gerente de pista",
                deadline="72h",
                effort="médio",
                risk="baixo",
                success="Cobertura ajustada sem overtime excessivo",
                screen="/sales",
            ),
            _action(
                "Testar ação de pista (combo) em 1 faixa ociosa",
                owner="Comercial",
                deadline="7d",
                effort="médio",
                risk="baixo",
                success="Lift mensurável na faixa testada",
                screen="/sales",
            ),
        ],
        "confidence": 0.6,
        "note": "Hipóteses — validar com curva horária da tela de Vendas.",
    }
