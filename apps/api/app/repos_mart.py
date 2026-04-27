from __future__ import annotations

"""Repositories (SQL access) for MART/DW.

PT-BR: Este módulo concentra queries de leitura para dashboards.
EN   : This module centralizes read queries for dashboards.

Design:
- Prefer reading from `mart.*` (materialized views) for performance.
- When something is not in MART yet, we read from `dw.*` facts/dims.
"""

from datetime import date, timedelta
from typing import Optional, List, Dict, Any
import logging
import unicodedata

from app.business_time import business_clock_payload, business_timezone_name, business_today
from app.cash_operational_truth import (
    CASH_OPEN_RELATION,
    cash_open_schema_mode,
    cash_open_source_sql,
    cash_payment_relation_exists,
    relation_exists,
)
from app.db_compat import SNAPSHOT_FALLBACK_ERRORS
from app.db import get_conn
from app.sales_semantics import (
    CANCELLATION_STATUS,
    RETURN_STATUS,
    SALE_STATUS,
    cash_net_value,
    comercial_cfop_class_sql,
    comercial_cfop_direction_sql,
    comercial_cfop_numeric_sql,
    sales_cfop_filter_sql,
    sales_status_filter_sql,
    sales_status_sql,
)

logger = logging.getLogger(__name__)


LOCAL_VENDA_LABELS = {
    -1: "Canal não identificado",
    1: "Pista",
    2: "Loja de conveniência",
    3: "Serviços",
}

COMMERCIAL_CFOP_LABELS = {
    "saida_normal": "Saída normal",
    "entrada_normal": "Entrada normal",
    "devolucao_saida": "Devolução de saída",
    "devolucao_entrada": "Devolução de entrada",
    "outro": "Outros CFOPs",
}

EVENT_TYPE_LABELS = {
    "CANCELAMENTO": "Cancelamento fora do padrão",
    "CANCELAMENTO_SEGUIDO_VENDA": "Cancelamento seguido de nova venda",
    "DESCONTO_ALTO": "Desconto acima do padrão",
    "FUNCIONARIO_OUTLIER": "Comportamento fora do padrão",
}

SNAPSHOT_TABLES = {
    "customer_churn_risk_daily": "mart.customer_churn_risk_daily",
    "finance_aging_daily": "mart.finance_aging_daily",
    "health_score_daily": "mart.health_score_daily",
}

CASH_STALE_WINDOW_HOURS = 96
CASH_CANCEL_EVENT_TYPES = frozenset({"CANCELAMENTO", "CANCELAMENTO_SEGUIDO_VENDA"})
SALES_OPERATIONAL_FALLBACK_TIMEOUT_MS = 2500

CANONICAL_GROUP_BUCKET_IDS = {
    "macro:combustiveis": 900000001,
    "macro:servicos": 900000002,
    "macro:conveniencia": 900000003,
    "group:unknown": 900000099,
}

CANONICAL_GROUP_COMBUSTIVEIS_EXACT = frozenset({
    "COMBUSTIVEIS",
    "COMBUSTIVEL",
    "COMBUSTIVEIS ESPECIAIS",
    "GASOLINA",
    "ETANOL",
    "DIESEL",
    "GNV",
})
CANONICAL_GROUP_COMBUSTIVEIS_PREFIXES = (
    "COMBUSTIV",
    "GASOL",
    "ETANOL",
    "DIESEL",
    "GNV",
)
CANONICAL_GROUP_COMBUSTIVEIS_EXCLUDES = frozenset({
    "FILTRO",
    "OLEO",
    "LUBR",
    "ADITIV",
    "GRAXA",
    "ARLA",
    "CARRO",
    "UTILIDADE",
    "LIMPEZA",
})
CANONICAL_GROUP_SERVICOS_EXACT = frozenset({
    "SERVICOS",
    "SERVICOS AUTOMOTIVOS",
    "OFICINA",
    "LAVAGEM",
    "DUCHA",
    "TROCA DE OLEO",
})
CANONICAL_GROUP_SERVICOS_PREFIXES = (
    "SERVIC",
    "OFIC",
    "LAVAG",
    "DUCHA",
    "TROCA",
)
CANONICAL_GROUP_CONVENIENCIA_EXACT = frozenset({
    "CONVENIENCIA",
    "LOJA DE CONVENIENCIA",
    "CIGARROS",
    "TABACARIA",
    "BEBIDAS ALCOOLICAS",
    "BEBIDAS NAO ALCOOLICAS",
    "FRENTE DE CAIXA",
    "FRENTE DE CAIXA COMISSAO",
    "MERCEARIA",
    "CHOCOLATES",
    "SALGADINHOS",
    "SALGADOS",
    "SORVETES",
    "LANCHONETE",
    "PADARIA",
    "DOCES",
    "BOMBONIERE",
    "ALIMENTOS",
    "ALIMENTACAO",
})
CANONICAL_GROUP_CONVENIENCIA_PREFIXES = (
    "CONVENI",
    "LOJA DE CONVENI",
    "CIGAR",
    "TABAC",
    "BEBID",
    "FRENTE DE CAIXA",
    "MERCE",
    "CHOCOL",
    "SALG",
    "SORVET",
    "LANCH",
    "PADAR",
    "DOC",
    "BOMBON",
    "ALIMENT",
)


def _format_brl(value: Any) -> str:
    return f"R$ {float(value or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _normalized_text_expression(expr: str) -> str:
    return (
        f"TRANSLATE(UPPER(COALESCE(NULLIF({expr}, ''), '')), "
        "'ÁÀÃÂÉÈÊÍÌÎÓÒÕÔÚÙÛÇ', 'AAAAEEEIIIOOOOUUUC')"
    )


def _filial_label(id_filial: Any, filial_nome: Any = None) -> str:
    if isinstance(id_filial, (list, tuple, set)):
        branch_ids = _branch_ids(id_filial)
        if not branch_ids:
            return "Todas as filiais"
        if len(branch_ids) == 1:
            return _filial_label(branch_ids[0], filial_nome)
        return f"{len(branch_ids)} filiais selecionadas"
    nome = str(filial_nome or "").strip()
    if nome:
        return nome
    if id_filial is None:
        return "Todas as filiais"
    return "Filial não identificada"


def _jarvis_shortcut(kind: Any) -> Optional[Dict[str, str]]:
    mapping = {
        "cash": {"path": "/cash", "label": "Abrir caixa"},
        "churn": {"path": "/customers", "label": "Abrir clientes"},
        "finance": {"path": "/finance", "label": "Abrir financeiro"},
        "fraud": {"path": "/fraud", "label": "Abrir antifraude"},
        "payments": {"path": "/finance", "label": "Abrir financeiro"},
        "pricing": {"path": "/pricing", "label": "Abrir preço concorrente"},
        "sales": {"path": "/sales", "label": "Abrir vendas"},
    }
    shortcut = mapping.get(str(kind or "").lower())
    return dict(shortcut) if shortcut else None


def _local_venda_label(id_local_venda: Any, local_nome: Any = None) -> str:
    nome = str(local_nome or "").strip()
    if nome:
        return nome
    if id_local_venda is None:
        return "Canal não informado"
    try:
        return LOCAL_VENDA_LABELS.get(int(id_local_venda), f"Canal #{int(id_local_venda)}")
    except Exception:
        return "Canal não informado"


def _turno_value_sql(payload_expr: str, id_turno_expr: str) -> str:
    return f"""
      COALESCE(
        NULLIF(trim({payload_expr}->>'TURNO'), ''),
        NULLIF(trim({payload_expr}->>'NO_TURNO'), ''),
        NULLIF(trim({payload_expr}->>'NUMTURNO'), ''),
        NULLIF(trim({payload_expr}->>'NR_TURNO'), ''),
        NULLIF(trim({payload_expr}->>'NROTURNO'), ''),
        NULLIF(trim({payload_expr}->>'TURNO_CAIXA'), ''),
        NULLIF(trim({payload_expr}->>'TURNOCAIXA'), ''),
        CASE
          WHEN {id_turno_expr} IS NOT NULL AND {id_turno_expr} > 0 THEN {id_turno_expr}::text
          ELSE NULL
        END
      )
    """


def _turno_label(turno_value: Any, id_turno: Any = None) -> str:
    value = str(turno_value or "").strip()
    if value:
        return value
    try:
        if id_turno is not None and int(id_turno) > 0:
            return str(int(id_turno))
    except Exception:
        pass
    return "Turno não identificado"


def _event_type_label(event_type: Any) -> str:
    key = str(event_type or "").strip().upper()
    return EVENT_TYPE_LABELS.get(key, key.replace("_", " ").title() or "Evento de risco")


def _humanize_risk_reasons(reasons: Any, event_type: Any) -> List[str]:
    payload = reasons if isinstance(reasons, dict) else {}
    items: List[str] = []

    if str(payload.get("pattern") or "") == "cancelamento_seguido_venda_rapida":
        items.append("Nova venda registrada logo após o cancelamento.")
    if float(payload.get("high_value_p90") or 0) > 0:
        items.append("Valor acima da faixa normal para a operação.")
    if float(payload.get("quick_resale_lt_2m") or 0) > 0:
        items.append("Recompra muito próxima após o cancelamento.")
    if float(payload.get("user_outlier_ratio") or 0) > 0:
        items.append("Colaborador acima do padrão histórico de cancelamentos.")
    if float(payload.get("risk_hour_bonus") or 0) > 0:
        items.append("Ocorrência em horário de maior risco.")
    if float(payload.get("discount_p95_bonus") or 0) > 0:
        items.append("Desconto acima da faixa normal do dia.")
    if float(payload.get("unit_price_outlier_bonus") or 0) > 0:
        items.append("Preço unitário fora da curva recente.")
    if float(payload.get("base_desconto") or 0) > 0 and not items:
        items.append("Desconto relevante para a operação.")
    if float(payload.get("base_cancelamento") or 0) > 0 and not items:
        items.append("Cancelamento acima do padrão operacional.")

    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    valor_total = float(metrics.get("valor_total") or 0)
    desconto_total = float(metrics.get("desconto_total") or 0)
    if desconto_total > 0 and not any("Desconto" in item for item in items):
        items.append(f"Desconto total de R$ {desconto_total:,.2f} na operação.".replace(",", "X").replace(".", ",").replace("X", "."))
    if valor_total > 0 and not any("Valor acima" in item for item in items) and str(event_type or "").upper() == "CANCELAMENTO":
        items.append(f"Valor envolvido de R$ {valor_total:,.2f} no cancelamento.".replace(",", "X").replace(".", ",").replace("X", "."))

    if not items:
        items.append(f"{_event_type_label(event_type)} identificado pela leitura de risco.")

    return items[:3]


def _group_name_expression(group_alias: str, product_alias: str) -> str:
    normalized = _normalized_text_expression(f"COALESCE(NULLIF({group_alias}.nome, ''), NULLIF({product_alias}.nome, ''), '')")
    return f"""
      CASE
        WHEN {normalized} LIKE '%%GASOL%%'
          OR {normalized} LIKE '%%ETANOL%%'
          OR {normalized} LIKE '%%DIESEL%%'
          OR {normalized} LIKE '%%GNV%%'
          OR {normalized} LIKE '%%COMBUST%%'
          THEN 'Combustíveis'
        WHEN {normalized} LIKE '%%TROCA%%'
          OR {normalized} LIKE '%%LAVAG%%'
          OR {normalized} LIKE '%%DUCHA%%'
          OR {normalized} LIKE '%%SERV%%'
          OR {normalized} LIKE '%%OFIC%%'
          THEN 'Serviços'
        WHEN {normalized} LIKE '%%CONVENI%%'
          OR {normalized} LIKE '%%BEBID%%'
          OR {normalized} LIKE '%%ALIMENT%%'
          OR {normalized} LIKE '%%SALG%%'
          OR {normalized} LIKE '%%CIGAR%%'
          OR {normalized} LIKE '%%LOJA%%'
          OR {normalized} LIKE '%%MERCE%%'
          THEN 'Conveniência'
        WHEN COALESCE(NULLIF({group_alias}.nome, ''), '') <> '' THEN {group_alias}.nome
        ELSE 'Outros da operação'
      END
    """


def _group_display_name_expression(group_alias: str, product_alias: str) -> str:
    return f"COALESCE(NULLIF({group_alias}.nome, ''), NULLIF({product_alias}.nome, ''), 'Outros da operação')"


def _normalize_group_bucket_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .upper()
    )


def _matches_group_bucket(
    normalized: str,
    *,
    exact_names: frozenset[str],
    prefixes: tuple[str, ...],
    excluded_tokens: frozenset[str] = frozenset(),
) -> bool:
    if not normalized:
        return False
    if excluded_tokens and any(token in normalized for token in excluded_tokens):
        return False
    if normalized in exact_names:
        return True
    return any(normalized.startswith(prefix) for prefix in prefixes)


def _canonical_group_identity(group_id: Any, group_name: Any) -> tuple[int, str, str]:
    label = str(group_name or "").strip()
    normalized = _normalize_group_bucket_text(label)

    if _matches_group_bucket(
        normalized,
        exact_names=CANONICAL_GROUP_COMBUSTIVEIS_EXACT,
        prefixes=CANONICAL_GROUP_COMBUSTIVEIS_PREFIXES,
        excluded_tokens=CANONICAL_GROUP_COMBUSTIVEIS_EXCLUDES,
    ):
        return (
            CANONICAL_GROUP_BUCKET_IDS["macro:combustiveis"],
            "Combustíveis",
            "macro:combustiveis",
        )
    if _matches_group_bucket(
        normalized,
        exact_names=CANONICAL_GROUP_SERVICOS_EXACT,
        prefixes=CANONICAL_GROUP_SERVICOS_PREFIXES,
    ):
        return (
            CANONICAL_GROUP_BUCKET_IDS["macro:servicos"],
            "Serviços",
            "macro:servicos",
        )
    if _matches_group_bucket(
        normalized,
        exact_names=CANONICAL_GROUP_CONVENIENCIA_EXACT,
        prefixes=CANONICAL_GROUP_CONVENIENCIA_PREFIXES,
    ):
        return (
            CANONICAL_GROUP_BUCKET_IDS["macro:conveniencia"],
            "Conveniência",
            "macro:conveniencia",
        )

    try:
        raw_id = int(group_id)
        if raw_id >= 0:
            fallback_label = label or f"Grupo #{raw_id}"
            return raw_id, fallback_label, f"group:{raw_id}"
    except Exception:
        pass

    fallback_label = label or "Outros da operação"
    return (
        CANONICAL_GROUP_BUCKET_IDS["group:unknown"],
        fallback_label,
        "group:unknown",
    )


def _collapse_group_rank_rows(rows: List[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
    combined: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        canonical_id, canonical_label, canonical_key = _canonical_group_identity(
            row.get("id_grupo_produto"),
            row.get("grupo_nome"),
        )
        current = combined.setdefault(
            canonical_key,
            {
                "id_grupo_produto": canonical_id,
                "grupo_key": canonical_key,
                "grupo_nome": canonical_label,
                "faturamento": 0.0,
                "margem": 0.0,
            },
        )
        current["grupo_nome"] = canonical_label
        current["faturamento"] = float(current.get("faturamento") or 0) + float(row.get("faturamento") or 0)
        current["margem"] = float(current.get("margem") or 0) + float(row.get("margem") or 0)

    ordered = sorted(combined.values(), key=lambda row: float(row.get("faturamento") or 0), reverse=True)
    return [
        {
            **row,
            "faturamento": round(float(row.get("faturamento") or 0), 2),
            "margem": round(float(row.get("margem") or 0), 2),
        }
        for row in ordered[:limit]
    ]


def _fuel_group_signal_expression(group_alias: str) -> str:
    group_name = _normalized_text_expression(f"{group_alias}.nome")
    return f"""
      (
        (
          {group_name} LIKE '%%COMBUST%%'
          OR {group_name} LIKE '%%GNV%%'
        )
        AND {group_name} NOT LIKE '%%FILTRO%%'
        AND {group_name} NOT LIKE '%%ADITIV%%'
        AND {group_name} NOT LIKE '%%LUBR%%'
        AND {group_name} NOT LIKE '%%CARRO%%'
        AND {group_name} NOT LIKE '%%UTILIDADE%%'
        AND {group_name} NOT LIKE '%%LIMPEZA%%'
      )
    """


def _fuel_family_case_expression(group_alias: str, product_alias: str) -> str:
    product_name = _normalized_text_expression(f"{product_alias}.nome")
    group_name = _normalized_text_expression(f"{group_alias}.nome")
    unit_name = _normalized_text_expression(f"{product_alias}.unidade")
    fuel_group_signal = _fuel_group_signal_expression(group_alias)
    liquid_units = f"{unit_name} IN ('LT', 'L', 'LITRO', 'LITROS')"
    gas_units = f"{unit_name} IN ('M3', 'MTS3') OR {unit_name} = ''"
    fuel_scope = f"({fuel_group_signal} OR COALESCE(NULLIF({group_alias}.nome, ''), '') = '')"
    return f"""
      CASE
        WHEN (
          {fuel_scope}
          AND {liquid_units}
          AND (
            {product_name} LIKE '%%GASOL%%'
            OR {group_name} LIKE '%%GASOL%%'
          )
        ) THEN 'GASOLINA'
        WHEN (
          {fuel_scope}
          AND {liquid_units}
          AND (
            {product_name} LIKE '%%ETANOL%%'
            OR ({fuel_group_signal} AND {product_name} LIKE '%%ALCOOL%%')
            OR {group_name} LIKE '%%ETANOL%%'
            OR ({fuel_group_signal} AND {group_name} LIKE '%%ALCOOL%%')
          )
        ) THEN 'ETANOL'
        WHEN (
          {fuel_scope}
          AND {liquid_units}
          AND (
            {product_name} LIKE '%%DIESEL S10%%'
            OR {product_name} LIKE '%%DIESEL-S10%%'
            OR {product_name} LIKE '%% S10%%'
            OR {product_name} LIKE '%%BS10%%'
            OR ({group_name} LIKE '%%DIESEL%%' AND {product_name} LIKE '%%S10%%')
          )
        ) THEN 'DIESEL S10'
        WHEN (
          {fuel_scope}
          AND {liquid_units}
          AND (
            {product_name} LIKE '%%DIESEL S500%%'
            OR {product_name} LIKE '%%DIESEL-S500%%'
            OR {product_name} LIKE '%% S500%%'
            OR {product_name} LIKE '%%BS500%%'
            OR ({product_name} LIKE '%%DIESEL%%' AND {product_name} NOT LIKE '%%S10%%' AND {product_name} NOT LIKE '%%BS10%%')
          )
        ) THEN 'DIESEL S500'
        WHEN (
          ({fuel_group_signal} OR {group_name} LIKE '%%GNV%%')
          AND ({gas_units})
          AND (
            {product_name} LIKE '%%GNV%%'
            OR {group_name} LIKE '%%GNV%%'
          )
        ) THEN 'GNV'
        ELSE NULL
      END
    """


def _fuel_filter_expression(group_alias: str, product_alias: str) -> str:
    product_name = _normalized_text_expression(f"{product_alias}.nome")
    family_case = _fuel_family_case_expression(group_alias, product_alias)
    return f"""
      (
        {family_case} IS NOT NULL
        AND {product_name} NOT LIKE 'ADITIVO%%'
        AND {product_name} NOT LIKE '%% ADITIVO%%'
        AND {product_name} NOT LIKE '%% INJECTOR %%'
        AND {product_name} NOT LIKE '%% FUEL TREATMENT%%'
        AND {product_name} NOT LIKE '%%BOMBA%%'
        AND {product_name} NOT LIKE '%%FILTRO%%'
        AND {product_name} NOT LIKE '%%KIT%%'
        AND {product_name} NOT LIKE '%%MANGUEIRA%%'
        AND {product_name} NOT LIKE '%%BICO%%'
        AND {product_name} NOT LIKE '%%MEDIDORA%%'
        AND {product_name} NOT LIKE '%%LEITOR%%'
        AND {product_name} NOT LIKE '%%CODIGO%%'
        AND {product_name} NOT LIKE '%%BARRAS%%'
        AND {product_name} NOT LIKE '%%BEMATECH%%'
        AND {product_name} NOT LIKE '%%ARLA%%'
        AND {product_name} NOT LIKE '%%LUBRIFICANTE%%'
        AND {product_name} NOT LIKE '%%FLUID%%'
        AND {product_name} NOT LIKE '%%15W%%'
        AND {product_name} NOT LIKE '%%10W%%'
        AND {product_name} NOT LIKE '%%5W%%'
        AND {product_name} NOT LIKE '%%200ML%%'
        AND {product_name} NOT LIKE '%%236ML%%'
        AND {product_name} NOT LIKE '%%250ML%%'
        AND {product_name} NOT LIKE '%%354ML%%'
        AND {product_name} NOT LIKE '%%500ML%%'
        AND {product_name} NOT LIKE '%%1KG%%'
        AND {product_name} NOT LIKE '%%20KG%%'
        AND {product_name} NOT LIKE '%% 1L%%'
        AND {product_name} NOT LIKE '%% 5L%%'
        AND {product_name} NOT LIKE '%% 20L%%'
      )
    """


def _active_product_filter_expression(product_alias: str) -> str:
    # Legacy dimensions may still have NULL status until the canonical product ETL
    # repopulates them; treat NULL as active and hide only explicitly inactive rows.
    return f"COALESCE({product_alias}.situacao, 1) = 1"


def _sales_status_expression(sale_alias: str) -> str:
    return sales_status_sql(sale_alias)


def _employee_label(funcionario_nome: Any, id_funcionario: Any = None) -> str:
    nome = str(funcionario_nome or "").strip()
    if nome and nome.lower() not in {"(sem funcionário)", "sem funcionário", "sem funcionario"}:
        return nome
    return "Equipe não identificada"


def _cash_operator_label(usuario_nome: Any, id_usuario: Any = None) -> str:
    nome = str(usuario_nome or "").strip()
    if nome:
        return nome
    return "Operador não identificado"


def cash_definitions() -> Dict[str, str]:
    return {
        "historical": "O histórico do caixa preserva a trilha de reconciliação por turno, enquanto a camada comercial principal usa comprovantes ativos e cancelados com CFOP comercial para fechar vendas e cancelamentos do período.",
        "live_now": (
            f"O monitor ao vivo mostra apenas turnos que seguem abertos e tiveram movimento recente nas últimas {CASH_STALE_WINDOW_HOURS} horas. "
            "Turnos antigos sem atividade ficam separados para investigação, sem poluir o agora."
        ),
        "operator": (
            "O nome exibido é o operador logado responsável pelo turno. Caixa e Antifraude usam essa mesma referência para evitar divergência de responsável."
        ),
        "closing_rule": "Um turno deixa de aparecer como aberto quando o fechamento foi confirmado e não houve nova movimentação depois disso.",
        "aggregates": "A visão principal do caixa parte de comprovantes com CFOP comercial e flag de cancelamento; a reconciliação detalhada continua exposta separadamente quando necessário.",
        "net_cash": "Saldo comercial do período = vendas ativas de saída - cancelamentos. Recebimentos e componentes financeiros seguem expostos separadamente.",
    }


def fraud_definitions() -> Dict[str, str]:
    return {
        "operational_cancelamentos": (
            "Cancelamento operacional é a venda cancelada que ainda precisa de revisão, sempre reconciliada com o turno real do caixa para não gerar leitura duplicada ou fora de contexto."
        ),
        "cashier_operator": (
            "Sempre mostramos o operador logado responsável pela operação do caixa. O usuário gravado no documento só entra como apoio quando o turno não consegue resolver o responsável."
        ),
        "high_risk_events": (
            "Evento de alto risco é um comportamento que foge do padrão esperado e merece revisão prioritária, como sequência incomum de cancelamentos, desconto fora da curva ou operação em contexto atípico."
        ),
        "estimated_impact": (
            "Impacto estimado é o valor potencial exposto no evento, usado para priorizar auditoria. Em cancelamento modelado usamos 70% do valor da operação; em desconto alto usamos o maior entre o desconto total e 8% da venda. Não é perda confirmada."
        ),
        "score_meaning": (
            "O score médio resume o nível de alerta dos eventos do período numa escala de 0 a 100. Quanto maior o score, maior a concentração de sinais que pedem investigação."
        ),
        "coverage": (
            "Leitura operacional mostra o que realmente ocorreu no período. Leitura modelada depende da janela coberta pelo motor de risco; quando a cobertura é parcial, a tela avisa isso sem apagar os eventos operacionais."
        ),
        "impact_formulas": (
            "Cancelamento modelado: 70% do valor da venda cancelada. Desconto alto: maior entre o desconto concedido e 8% do valor da venda. Pagamentos fora do padrão usam a exposição monetária do próprio evento."
        ),
    }


def finance_definitions() -> Dict[str, Dict[str, str]]:
    return {
        "receber_aberto": {
            "label": "Receber em aberto",
            "formula": "Soma dos títulos a receber ainda não quitados na data-base.",
            "source": "mart.financeiro_vencimentos_diaria / dw.fact_financeiro",
            "impact": "Mostra o caixa que ainda deve entrar.",
        },
        "receber_vencido": {
            "label": "Receber vencido",
            "formula": "Parcela do contas a receber cujo vencimento já passou e segue em aberto.",
            "source": "mart.finance_aging_daily ou leitura operacional equivalente",
            "impact": "Mostra caixa atrasado e necessidade de cobrança.",
        },
        "pagar_aberto": {
            "label": "Pagar em aberto",
            "formula": "Soma dos compromissos a pagar ainda não liquidados na data-base.",
            "source": "mart.financeiro_vencimentos_diaria / dw.fact_financeiro",
            "impact": "Mostra obrigação futura que ainda pressiona o caixa.",
        },
        "pagar_vencido": {
            "label": "Pagar vencido",
            "formula": "Parcela do contas a pagar cujo vencimento já passou e segue em aberto.",
            "source": "mart.finance_aging_daily ou leitura operacional equivalente",
            "impact": "Mostra pressão imediata e risco de atraso com fornecedor.",
        },
        "cash_pressure": {
            "label": "Pressão imediata de caixa",
            "formula": "Receber vencido + pagar vencido.",
            "source": "Cálculo da tela a partir do aging.",
            "impact": "Resume quanto do caixa está pressionado por atraso hoje.",
        },
        "top5_concentration": {
            "label": "Concentração da carteira",
            "formula": "Participação dos 5 maiores títulos vencidos no total vencido a receber.",
            "source": "mart.finance_aging_daily ou leitura operacional equivalente",
            "impact": "Ajuda a ver dependência excessiva de poucos títulos.",
        },
        "payments_total": {
            "label": "Leitura dos pagamentos",
            "formula": "Soma dos pagamentos conciliados no recorte.",
            "source": "mart.agg_pagamentos_turno / dw.fact_pagamento_comprovante",
            "impact": "Mostra por onde o dinheiro entrou e sustenta conferência com caixa.",
        },
        "payments_unknown_share": {
            "label": "Pagamentos não identificados",
            "formula": "Valor sem mapeamento oficial dividido pelo valor total conciliado de pagamentos.",
            "source": "app.payment_type_map + mart.agg_pagamentos_turno",
            "impact": "Indica perda de explicabilidade do recebimento.",
        },
    }


def _payment_category_label(category: Any, label: Any = None) -> str:
    category_value = str(category or "").strip().upper()
    label_value = str(label or "").strip()
    if label_value and label_value.upper() != "NÃO IDENTIFICADO":
        return label_value
    if category_value and category_value != "NAO_IDENTIFICADO":
        return category_value.replace("_", " ").title()
    return "NÃO IDENTIFICADO"


def _resolved_cash_eligible_sql(
    cash_column: str,
    data_column: str,
    data_conta_column: str,
    id_turno_column: str,
) -> str:
    return f"etl.resolve_cash_eligible({cash_column}, {data_column}, {data_conta_column}, {id_turno_column})"


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _date_from_key(value: Any) -> Optional[date]:
    digits = str(value or "").strip()
    if len(digits) != 8 or not digits.isdigit():
        return None
    try:
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    except Exception:
        return None


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _next_month_start(value: date) -> date:
    month_start = _month_start(value)
    return (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)


def _shift_months(value: date, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + month_index // 12
    month = (month_index % 12) + 1
    return date(year, month, 1)


def _days_in_month(value: date) -> int:
    month_start = _month_start(value)
    return (_next_month_start(month_start) - month_start).days


def _iso_or_none(value: Any) -> Optional[str]:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _month_ref(year: int, month: int) -> date:
    return date(year, month, 1)


def _window_coverage_payload(
    *,
    requested_dt_ini: date,
    requested_dt_fim: date,
    min_data_key: Any,
    max_data_key: Any,
    source_label: str,
) -> Dict[str, Any]:
    requested_days = max((requested_dt_fim - requested_dt_ini).days + 1, 1)
    earliest_available_dt = _date_from_key(min_data_key)
    latest_available_dt = _date_from_key(max_data_key)

    if earliest_available_dt is None or latest_available_dt is None:
        return {
            "mode": "missing",
            "source": source_label,
            "requested_dt_ini": requested_dt_ini,
            "requested_dt_fim": requested_dt_fim,
            "effective_dt_ini": None,
            "effective_dt_fim": None,
            "earliest_available_dt": None,
            "latest_available_dt": None,
            "requested_days": requested_days,
            "covered_days_in_requested": 0,
            "requested_has_coverage": False,
            "is_stale": False,
            "message": "A trilha comercial canônica ainda não publicou base suficiente para este escopo.",
        }

    overlap_start = max(requested_dt_ini, earliest_available_dt)
    overlap_end = min(requested_dt_fim, latest_available_dt)
    covered_days = (
        max((overlap_end - overlap_start).days + 1, 0)
        if overlap_end >= overlap_start
        else 0
    )

    if requested_dt_ini > latest_available_dt:
        effective_dt_fim = latest_available_dt
        effective_dt_ini = max(
            earliest_available_dt,
            latest_available_dt - timedelta(days=requested_days - 1),
        )
        mode = "shifted_latest"
        message = (
            f"O recorte pedido vai até {requested_dt_fim.isoformat()}, mas a última base comercial disponível "
            f"vai até {latest_available_dt.isoformat()}. A tela usa o último período comparável entre "
            f"{effective_dt_ini.isoformat()} e {effective_dt_fim.isoformat()}."
        )
    elif requested_dt_fim > latest_available_dt:
        effective_dt_ini = requested_dt_ini
        effective_dt_fim = latest_available_dt
        mode = "partial_requested"
        message = (
            f"A base comercial canônica cobre este recorte apenas até {latest_available_dt.isoformat()}. "
            "Os valores posteriores ainda não chegaram da origem."
        )
    else:
        effective_dt_ini = requested_dt_ini
        effective_dt_fim = requested_dt_fim
        mode = "exact"
        message = None

    return {
        "mode": mode,
        "source": source_label,
        "requested_dt_ini": requested_dt_ini,
        "requested_dt_fim": requested_dt_fim,
        "effective_dt_ini": effective_dt_ini,
        "effective_dt_fim": effective_dt_fim,
        "earliest_available_dt": earliest_available_dt,
        "latest_available_dt": latest_available_dt,
        "requested_days": requested_days,
        "covered_days_in_requested": covered_days,
        "requested_has_coverage": covered_days > 0,
        "is_stale": requested_dt_fim > latest_available_dt,
        "message": message,
    }


def commercial_window_coverage(
    role: str,
    id_empresa: int,
    id_filial: Any,
    requested_dt_ini: date,
    requested_dt_fim: date,
) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql = f"""
      SELECT
        MIN(data_key)::int AS min_data_key,
        MAX(data_key)::int AS max_data_key
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s
        {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, [id_empresa] + branch_params).fetchone() or {}

    return _window_coverage_payload(
        requested_dt_ini=requested_dt_ini,
        requested_dt_fim=requested_dt_fim,
        min_data_key=row.get("min_data_key"),
        max_data_key=row.get("max_data_key"),
        source_label="mart.agg_vendas_diaria",
    )


def _dashboard_home_modeled_risk_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    try:
        return {
            "source_status": "ok",
            "message": None,
            "insights": risk_insights(role, id_empresa, id_filial, dt_ini, dt_fim, limit=20),
            "kpis": risk_kpis(role, id_empresa, id_filial, dt_ini, dt_fim),
            "window": risk_data_window(role, id_empresa, id_filial),
        }
    except SNAPSHOT_FALLBACK_ERRORS as exc:
        logger.warning(
            "Dashboard home modeled risk unavailable tenant=%s filial=%s: %s",
            id_empresa,
            id_filial,
            exc.__class__.__name__,
            exc_info=exc,
        )
    except TimeoutError as exc:
        logger.warning(
            "Dashboard home modeled risk timed out tenant=%s filial=%s",
            id_empresa,
            id_filial,
            exc_info=exc,
        )

    return {
        "source_status": "unavailable",
        "message": "A leitura modelada de risco ainda não ficou pronta neste ambiente restaurado.",
        "insights": [],
        "kpis": {
            "total_eventos": None,
            "eventos_alto_risco": None,
            "impacto_total": None,
            "score_medio": None,
        },
        "window": {
            "min_data_key": None,
            "max_data_key": None,
            "rows": None,
        },
    }


def _commercial_annual_comparison(
    monthly_rows: List[Dict[str, Any]],
    *,
    current_year: int,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    by_month = {
        (int(row.get("ano") or 0), int(row.get("mes") or 0)): row
        for row in monthly_rows
    }
    normalized_rows: List[Dict[str, Any]] = []
    comparison_months: List[Dict[str, Any]] = []
    previous_year = current_year - 1

    for year in (previous_year, current_year):
        for month in range(1, 13):
            source = by_month.get((year, month), {})
            normalized_rows.append(
                {
                    "month_ref": _month_ref(year, month).isoformat(),
                    "ano": year,
                    "mes": month,
                    "saidas": round(float(source.get("saidas") or 0), 2),
                    "entradas": round(float(source.get("entradas") or 0), 2),
                    "cancelamentos": round(float(source.get("cancelamentos") or 0), 2),
                }
            )

    for month in range(1, 13):
        current = by_month.get((current_year, month), {})
        previous = by_month.get((previous_year, month), {})
        comparison_months.append(
            {
                "mes": month,
                "saidas_atual": round(float(current.get("saidas") or 0), 2),
                "saidas_anterior": round(float(previous.get("saidas") or 0), 2),
                "entradas_atual": round(float(current.get("entradas") or 0), 2),
                "entradas_anterior": round(float(previous.get("entradas") or 0), 2),
                "cancelamentos_atual": round(float(current.get("cancelamentos") or 0), 2),
                "cancelamentos_anterior": round(float(previous.get("cancelamentos") or 0), 2),
                "month_ref_atual": _month_ref(current_year, month).isoformat(),
                "month_ref_anterior": _month_ref(previous_year, month).isoformat(),
            }
        )

    return normalized_rows, {
        "current_year": current_year,
        "previous_year": previous_year,
        "months": comparison_months,
    }


def risk_model_coverage(dt_ini: date, dt_fim: date, risk_window: Dict[str, Any]) -> Dict[str, Any]:
    requested_start_key = _date_key(dt_ini)
    requested_end_key = _date_key(dt_fim)
    requested_days = max((dt_fim - dt_ini).days + 1, 0)
    window_start_key = int(risk_window.get("min_data_key") or 0)
    window_end_key = int(risk_window.get("max_data_key") or 0)
    if window_start_key <= 0 or window_end_key <= 0:
        return {
            "status": "unavailable",
            "covered_fully": False,
            "requested_days": requested_days,
            "covered_days": 0,
            "requested_start_key": requested_start_key,
            "requested_end_key": requested_end_key,
            "covered_start_key": None,
            "covered_end_key": None,
            "message": "A leitura modelada ainda não tem janela pronta para este escopo. A leitura operacional segue válida no período.",
        }

    covered_start_key = max(requested_start_key, window_start_key)
    covered_end_key = min(requested_end_key, window_end_key)
    covered_start = _date_from_key(covered_start_key)
    covered_end = _date_from_key(covered_end_key)
    covered_days = (
        max((covered_end - covered_start).days + 1, 0)
        if covered_start is not None and covered_end is not None and covered_end >= covered_start
        else 0
    )
    covered_fully = window_start_key <= requested_start_key and window_end_key >= requested_end_key
    if covered_fully:
        status = "covered"
        message = "A leitura modelada cobre todo o período selecionado."
    elif covered_days > 0:
        status = "partial"
        message = (
            f"A leitura modelada cobre de {covered_start.strftime('%d/%m/%Y')} a {covered_end.strftime('%d/%m/%Y')}. "
            "Fora dessa janela, use a leitura operacional como verdade do período."
        )
    else:
        status = "not_covered"
        message = "A leitura modelada não cobre este recorte. Os eventos operacionais continuam válidos para o período."

    return {
        "status": status,
        "covered_fully": covered_fully,
        "requested_days": requested_days,
        "covered_days": covered_days,
        "requested_start_key": requested_start_key,
        "requested_end_key": requested_end_key,
        "covered_start_key": covered_start_key if covered_days > 0 else None,
        "covered_end_key": covered_end_key if covered_days > 0 else None,
        "window_start_key": window_start_key,
        "window_end_key": window_end_key,
        "message": message,
    }


def _branch_ids(id_filial: Any) -> Optional[List[int]]:
    if id_filial is None:
        return None
    if isinstance(id_filial, (list, tuple, set)):
        values = sorted({int(value) for value in id_filial if value is not None})
        return values
    return [int(id_filial)]


def _conn_branch_id(id_filial: Any) -> Optional[int]:
    branch_ids = _branch_ids(id_filial)
    if not branch_ids or len(branch_ids) != 1:
        return None
    return int(branch_ids[0])


def _branch_scope_clause(column: str, id_filial: Any) -> tuple[str, list[Any]]:
    branch_ids = _branch_ids(id_filial)
    if branch_ids is None:
        return "", []
    if not branch_ids:
        return "AND 1 = 0", []
    if len(branch_ids) == 1:
        return f"AND {column} = %s", [branch_ids[0]]
    return f"AND {column} = ANY(%s)", [branch_ids]


def _snapshot_meta(
    role: str,
    table_name: str,
    id_empresa: int,
    id_filial: Optional[int],
    requested_dt_ref: Optional[date],
    precision_mode: str,
) -> Dict[str, Any]:
    table = SNAPSHOT_TABLES[table_name]
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [requested_dt_ref, requested_dt_ref, requested_dt_ref, id_empresa] + branch_params
    sql = f"""
      SELECT
        MIN(dt_ref) AS coverage_start_dt_ref,
        MAX(dt_ref) AS coverage_end_dt_ref,
        COUNT(*)::int AS row_count,
        COALESCE(BOOL_OR(dt_ref = %s), false) AS has_exact,
        MAX(CASE WHEN %s::date IS NULL OR dt_ref <= %s::date THEN dt_ref END) AS effective_dt_ref,
        MAX(updated_at) AS latest_updated_at
      FROM {table}
      WHERE id_empresa = %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone() or {}

    start_dt = row.get("coverage_start_dt_ref")
    end_dt = row.get("coverage_end_dt_ref")
    has_exact = bool(row.get("has_exact"))
    effective_dt_ref = row.get("effective_dt_ref")
    snapshot_status = "exact" if has_exact else ("best_effort" if effective_dt_ref else "missing")
    return {
        "requested_dt_ref": requested_dt_ref,
        "effective_dt_ref": effective_dt_ref,
        "coverage_start_dt_ref": start_dt,
        "coverage_end_dt_ref": end_dt,
        "precision_mode": "exact" if has_exact else precision_mode,
        "snapshot_status": snapshot_status,
        "source_table": table,
        "source_kind": "snapshot" if effective_dt_ref else "missing",
        "latest_updated_at": row.get("latest_updated_at"),
        "row_count": int(row.get("row_count") or 0),
    }


def list_filiais(role: str, id_empresa: int) -> List[Dict[str, Any]]:
    sql = """
      SELECT id_filial, nome
      FROM auth.filiais
      WHERE id_empresa = %s AND is_active = true
      ORDER BY id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        return list(conn.execute(sql, (id_empresa,)).fetchall())


# ========================
# Dashboard (existing)
# ========================

def dashboard_home_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    dt_ref: date,
) -> Dict[str, Any]:
    modeled_risk_bundle = _dashboard_home_modeled_risk_bundle(role, id_empresa, id_filial, dt_ini, dt_fim)
    insights_rows = modeled_risk_bundle.get("insights") or []
    sales_coverage = commercial_window_coverage(role, id_empresa, id_filial, dt_ini, dt_fim)
    sales_dt_ini = sales_coverage.get("effective_dt_ini") or dt_ini
    sales_dt_fim = sales_coverage.get("effective_dt_fim") or dt_fim
    signal_dt_ref = sales_coverage.get("effective_dt_fim") or dt_ref
    # 2026-04-29: marts are now refreshed every operational cycle (TRACK_OPERATIONAL
    # includes global refresh). No need for live-day overlay from dw.fact_*.
    # Always read from marts for consistent, fast performance.
    sales = _sales_historical_bundle_from_marts(
        role,
        id_empresa,
        id_filial,
        sales_dt_ini,
        sales_dt_fim,
        include_details=False,
    )
    sales["commercial_coverage"] = sales_coverage
    sales["reading_status"] = (
        "latest_compatible"
        if sales_coverage.get("mode") == "shifted_latest"
        else str(sales.get("reading_status") or "mart_snapshot")
    )
    peak_hours_signal = sales_peak_hours_signal(role, id_empresa, id_filial, signal_dt_ref)
    declining_products_signal = sales_declining_products_signal(role, id_empresa, id_filial, signal_dt_ref)
    fraud_operational = {
        "kpis": fraud_kpis(role, id_empresa, id_filial, dt_ini, dt_fim),
        "window": fraud_data_window(role, id_empresa, id_filial),
    }
    modeled_risk = {
        "source_status": modeled_risk_bundle.get("source_status"),
        "message": modeled_risk_bundle.get("message"),
        "kpis": modeled_risk_bundle.get("kpis"),
        "window": modeled_risk_bundle.get("window"),
    }
    churn = customers_churn_bundle(role, id_empresa, id_filial, as_of=dt_ref, min_score=40, limit=10)
    finance_aging = finance_aging_overview(role, id_empresa, id_filial, as_of=dt_ref)
    cash_live = _cash_live_now(role, id_empresa, id_filial)
    payments = payments_overview(role, id_empresa, id_filial, sales_dt_ini, sales_dt_fim, anomaly_limit=5)
    notifications_unread = notifications_unread_count(role, id_empresa, id_filial)
    operational_sync = sales.get("operational_sync") or cash_live.get("operational_sync")
    freshness = {
        "mode": "hybrid_operational_home",
        "sales": sales.get("freshness"),
        "cash": cash_live.get("freshness"),
        "live_through_at": (operational_sync or {}).get("last_sync_at"),
        "source": "operational_truth",
    }

    filial_name = None
    branch_id = _conn_branch_id(id_filial)
    if branch_id is not None:
        with get_conn(role=role, tenant_id=id_empresa, branch_id=branch_id) as conn:
            filial_name_row = conn.execute(
                """
                SELECT nome
                FROM auth.filiais
                WHERE id_empresa = %s
                  AND id_filial = %s
                """,
                (id_empresa, branch_id),
            ).fetchone()
            filial_name = filial_name_row.get("nome") if filial_name_row else None

    return {
        "scope": {
            "id_empresa": id_empresa,
            "id_filial": branch_id,
            "id_filiais": _branch_ids(id_filial) or [],
            "filial_label": _filial_label(id_filial, filial_name),
            "dt_ini": dt_ini,
            "dt_fim": dt_fim,
            "requested_dt_ref": dt_ref,
        },
        "overview": {
            "sales": sales,
            "insights_generated": insights_rows,
            "fraud": {
                "operational": fraud_operational,
                "modeled_risk": modeled_risk,
            },
            "risk": modeled_risk,
            "cash": {
                "live_now": cash_live,
            },
            "jarvis": jarvis_briefing(
                role,
                id_empresa,
                id_filial,
                dt_ref=dt_ref,
                context={
                    "fraud_operational": fraud_operational.get("kpis"),
                    "modeled_risk": modeled_risk.get("kpis"),
                    "cash_live": cash_live,
                    "finance_aging": finance_aging,
                    "churn": churn,
                    "payments": payments,
                    "sales": sales,
                    "signals": {
                        "peak_hours": peak_hours_signal,
                        "declining_products": declining_products_signal,
                    },
                },
            ),
        },
        "churn": churn,
        "finance": {
            "aging": finance_aging,
        },
        "cash": {
            "live_now": cash_live,
            "operational_sync": cash_live.get("operational_sync"),
            "freshness": cash_live.get("freshness"),
        },
        "notifications_unread": notifications_unread,
        "operational_sync": operational_sync,
        "freshness": freshness,
        "commercial_coverage": sales_coverage,
    }

def dashboard_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    # 2026-04-29: marts are refreshed every operational cycle — read exclusively
    # from mart.agg_vendas_diaria, no live-day overlay from dw.fact_*.
    sql = f"""
      SELECT
        COALESCE(SUM(faturamento),0) AS faturamento,
        COALESCE(SUM(margem),0) AS margem,
        COALESCE(AVG(ticket_medio),0) AS ticket_medio,
        COALESCE(SUM(quantidade_itens),0) AS itens
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = dict(conn.execute(sql, params).fetchone() or {})

    return row or {"faturamento": 0, "margem": 0, "ticket_medio": 0, "itens": 0}


def dashboard_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params
    # 2026-04-29: marts refreshed every operational cycle — no live-day overlay.
    sql = f"""
      SELECT data_key, id_filial, faturamento, margem
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _sales_live_day_in_window(
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
    tenant_id: Optional[int] = None,
) -> Optional[date]:
    actual_business_today = business_today(tenant_id)
    if as_of is not None and as_of != actual_business_today:
        return None
    live_day = actual_business_today
    if dt_ini <= live_day <= dt_fim:
        return live_day
    return None


def _sales_historical_window_end(dt_ini: date, dt_fim: date, live_day: Optional[date]) -> Optional[date]:
    if live_day is None:
        return dt_fim
    if live_day <= dt_ini:
        return None
    return min(dt_fim, live_day - timedelta(days=1))


def _merge_sales_kpis(historical: Dict[str, Any], live: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = {
        "faturamento": float(historical.get("faturamento") or 0),
        "margem": float(historical.get("margem") or 0),
        "ticket_medio": float(historical.get("ticket_medio") or 0),
        "itens": float(historical.get("itens") or 0),
    }
    if not live:
        return merged

    live_kpis = live.get("kpis") or {}
    total_faturamento = merged["faturamento"] + float(live_kpis.get("faturamento") or 0)
    total_margem = merged["margem"] + float(live_kpis.get("margem") or 0)
    total_itens = merged["itens"] + float(live_kpis.get("itens") or 0)
    historical_sales = float(historical.get("sales_count") or 0)
    live_sales = float((live.get("stats") or {}).get("vendas") or 0)
    total_sales = historical_sales + live_sales
    return {
        "faturamento": round(total_faturamento, 2),
        "margem": round(total_margem, 2),
        "ticket_medio": round(total_faturamento / total_sales, 2) if total_sales > 0 else 0.0,
        "itens": round(total_itens, 3),
    }


def _merge_series_rows(
    historical_rows: List[Dict[str, Any]],
    live_row: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: Dict[tuple[int, int], Dict[str, Any]] = {}
    for row in historical_rows:
        key = (int(row.get("data_key") or 0), int(row.get("id_filial") or -1))
        merged[key] = dict(row)
    if live_row:
        key = (int(live_row.get("data_key") or 0), int(live_row.get("id_filial") or -1))
        merged[key] = dict(live_row)
    return sorted(merged.values(), key=lambda row: (int(row.get("data_key") or 0), int(row.get("id_filial") or -1)))


def _merge_rank_rows(
    historical_rows: List[Dict[str, Any]],
    live_rows: List[Dict[str, Any]],
    *,
    id_key: str,
    name_key: str,
    limit: int,
    numeric_fields: tuple[str, ...],
) -> List[Dict[str, Any]]:
    combined: Dict[int, Dict[str, Any]] = {}
    for source_rows in (historical_rows, live_rows):
        for row in source_rows:
            entity_id = int(row.get(id_key) or -1)
            if entity_id < 0:
                continue
            current = combined.setdefault(entity_id, {id_key: entity_id})
            name_value = str(row.get(name_key) or "").strip()
            if name_value:
                current[name_key] = name_value
            for field in numeric_fields:
                current[field] = float(current.get(field) or 0) + float(row.get(field) or 0)

    rows = list(combined.values())
    rows.sort(key=lambda row: float(row.get("faturamento") or 0), reverse=True)
    normalized: List[Dict[str, Any]] = []
    for row in rows[:limit]:
        item = dict(row)
        for field in numeric_fields:
            if field == "vendas":
                item[field] = int(round(float(item.get(field) or 0)))
            elif field == "qtd":
                item[field] = round(float(item.get(field) or 0), 3)
            else:
                item[field] = round(float(item.get(field) or 0), 2)
        normalized.append(item)
    return normalized


def insights_base(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params
    sql = f"""
      SELECT data_key, id_filial, faturamento_dia, faturamento_mes_acum, comparativo_mes_anterior
      FROM mart.insights_base_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Vendas & Stores
# ========================

def _commercial_cfop_label(value: Any) -> str:
    key = str(value or "").strip().lower()
    return COMMERCIAL_CFOP_LABELS.get(key, COMMERCIAL_CFOP_LABELS["outro"])


def _commercial_docs_window_cte(
    *,
    id_empresa: int,
    id_filial: Optional[int],
    date_predicate_sql: str,
    date_params: List[Any],
) -> tuple[str, List[Any], Optional[int]]:
    where_filial, branch_params = _branch_scope_clause("c.id_filial", id_filial)
    params = [id_empresa] + date_params + branch_params
    conn_branch_id = _conn_branch_id(id_filial)
    cte = f"""
      WITH commercial_docs AS MATERIALIZED (
        SELECT
          c.id_empresa,
          c.id_filial,
          c.id_db,
          c.id_comprovante,
          c.id_turno,
          c.id_cliente,
          c.data,
          c.data_key,
          COALESCE(c.valor_total, 0)::numeric(18,2) AS valor_total,
          COALESCE(c.cancelado, false) AS cancelado,
          COALESCE(c.situacao, 0)::int AS situacao,
          {comercial_cfop_numeric_sql('c')} AS cfop_num,
          {comercial_cfop_direction_sql('c')} AS cfop_direction,
          {comercial_cfop_class_sql('c')} AS cfop_class,
          c.updated_at,
          c.created_at
        FROM dw.fact_comprovante c
        WHERE c.id_empresa = %s
          AND {date_predicate_sql}
          AND {comercial_cfop_direction_sql('c')} IN ('saida', 'entrada')
          {where_filial}
      )
    """
    return cte, params, conn_branch_id


def sales_commercial_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    commercial_cte, params, conn_branch_id = _commercial_docs_window_cte(
        id_empresa=id_empresa,
        id_filial=id_filial,
        date_predicate_sql="c.data_key BETWEEN %s AND %s",
        date_params=[ini, fim],
    )
    where_filial, branch_params = _branch_scope_clause("c.id_filial", id_filial)
    mart_where_filial, mart_branch_params = _branch_scope_clause("m.id_filial", id_filial)
    comparison_year = dt_fim.year
    comparison_start_key = _date_key(date(comparison_year - 1, 1, 1))
    comparison_end_key = _date_key(date(comparison_year, 12, 31))
    combined_params = params + [id_empresa, comparison_start_key, comparison_end_key] + mart_branch_params
    sql_combined = commercial_cte + f"""
      , monthly AS MATERIALIZED (
        SELECT
          m.month_key,
          make_date((m.month_key / 100)::int, (m.month_key %% 100)::int, 1) AS month_ref,
          (m.month_key / 100)::int AS ano,
          (m.month_key %% 100)::int AS mes,
          COALESCE(SUM(m.faturamento), 0)::numeric(18,2) AS saidas,
          0::numeric(18,2) AS entradas,
          0::numeric(18,2) AS cancelamentos
        FROM (
          SELECT
            (m.data_key / 100)::int AS month_key,
            m.faturamento
          FROM mart.agg_vendas_diaria m
          WHERE m.id_empresa = %s
            AND m.data_key BETWEEN %s AND %s
            {mart_where_filial}
        ) m
        GROUP BY m.month_key
      ), kpis AS (
        SELECT
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'saida'), 0)::numeric(18,2) AS saidas,
          COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = false AND cfop_direction = 'saida')::int AS qtd_saidas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'entrada'), 0)::numeric(18,2) AS entradas,
          COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = false AND cfop_direction = 'entrada')::int AS qtd_entradas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada')), 0)::numeric(18,2) AS cancelamentos,
          COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada'))::int AS qtd_cancelamentos
        FROM commercial_docs
      ),
      breakdown AS (
        SELECT
          cfop_class,
          COUNT(*)::int AS documentos,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false), 0)::numeric(18,2) AS valor_ativo,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true), 0)::numeric(18,2) AS valor_cancelado,
          COALESCE(SUM(valor_total), 0)::numeric(18,2) AS valor_total
        FROM commercial_docs
        GROUP BY cfop_class
      ),
      by_hour AS (
        SELECT
          EXTRACT(HOUR FROM data)::int AS hora,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'saida'), 0)::numeric(18,2) AS saidas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'entrada'), 0)::numeric(18,2) AS entradas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true), 0)::numeric(18,2) AS cancelamentos
        FROM commercial_docs
        WHERE data IS NOT NULL
        GROUP BY 1
      )
      SELECT
        to_jsonb(kpis) AS kpis,
        COALESCE(
          (
            SELECT jsonb_agg(to_jsonb(b) ORDER BY b.valor_total DESC, b.cfop_class)
            FROM breakdown b
          ),
          '[]'::jsonb
        ) AS breakdown,
        COALESCE(
          (
            SELECT jsonb_agg(to_jsonb(h) ORDER BY h.hora)
            FROM by_hour h
          ),
          '[]'::jsonb
        ) AS by_hour,
        COALESCE(
          (
            SELECT jsonb_agg(to_jsonb(m) ORDER BY m.month_key)
            FROM monthly m
          ),
          '[]'::jsonb
        ) AS monthly
      FROM kpis
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        combined_row = dict(conn.execute(sql_combined, combined_params).fetchone() or {})
    kpis = dict(combined_row.get("kpis") or {})
    breakdown_rows = [dict(row) for row in (combined_row.get("breakdown") or [])]
    by_hour_rows = [dict(row) for row in (combined_row.get("by_hour") or [])]
    monthly_rows = [dict(row) for row in (combined_row.get("monthly") or [])]
    monthly_series, annual_comparison = _commercial_annual_comparison(
        monthly_rows,
        current_year=comparison_year,
    )

    return {
        "kpis": {
            "saidas": round(float(kpis.get("saidas") or 0), 2),
            "qtd_saidas": int(kpis.get("qtd_saidas") or 0),
            "entradas": round(float(kpis.get("entradas") or 0), 2),
            "qtd_entradas": int(kpis.get("qtd_entradas") or 0),
            "cancelamentos": round(float(kpis.get("cancelamentos") or 0), 2),
            "qtd_cancelamentos": int(kpis.get("qtd_cancelamentos") or 0),
        },
        "cfop_breakdown": [
            {
                "cfop_class": str(row.get("cfop_class") or "outro"),
                "label": _commercial_cfop_label(row.get("cfop_class")),
                "documentos": int(row.get("documentos") or 0),
                "valor_ativo": round(float(row.get("valor_ativo") or 0), 2),
                "valor_cancelado": round(float(row.get("valor_cancelado") or 0), 2),
                "valor_total": round(float(row.get("valor_total") or 0), 2),
            }
            for row in breakdown_rows
        ],
        "by_hour": [
            {
                "hora": int(row.get("hora") or 0),
                "saidas": round(float(row.get("saidas") or 0), 2),
                "entradas": round(float(row.get("entradas") or 0), 2),
                "cancelamentos": round(float(row.get("cancelamentos") or 0), 2),
            }
            for row in by_hour_rows
        ],
        "monthly_evolution": monthly_series,
        "annual_comparison": annual_comparison,
    }


def cash_commercial_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    commercial_cte, params, conn_branch_id = _commercial_docs_window_cte(
        id_empresa=id_empresa,
        id_filial=id_filial,
        date_predicate_sql="c.data_key BETWEEN %s AND %s",
        date_params=[ini, fim],
    )
    where_filial_pay, pay_branch_params = _branch_scope_clause("p.id_filial", id_filial)
    params_pay = [id_empresa, ini, fim] + pay_branch_params

    sql_summary = commercial_cte + f"""
      , pagamentos AS (
        SELECT
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
      )
      SELECT
        COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'saida'), 0)::numeric(18,2) AS total_vendas,
        COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = false AND cfop_direction = 'saida')::int AS qtd_vendas,
        COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada')), 0)::numeric(18,2) AS total_cancelamentos,
        COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada'))::int AS qtd_cancelamentos,
        COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'entrada'), 0)::numeric(18,2) AS total_entradas,
        COUNT(DISTINCT (id_filial::text || ':' || COALESCE(id_turno, -1)::text))::int AS caixas_periodo,
        COALESCE(MAX(p.total_pagamentos), 0)::numeric(18,2) AS total_pagamentos
      FROM commercial_docs
      CROSS JOIN pagamentos p
    """
    sql_by_day = commercial_cte + f"""
      , pagamentos AS (
        SELECT
          p.data_key,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
        GROUP BY p.data_key
      ), comercial AS (
        SELECT
          data_key,
          COUNT(DISTINCT (id_filial::text || ':' || COALESCE(id_turno, -1)::text))::int AS caixas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'saida'), 0)::numeric(18,2) AS total_vendas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada')), 0)::numeric(18,2) AS total_cancelamentos,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'entrada'), 0)::numeric(18,2) AS total_entradas
        FROM commercial_docs
        GROUP BY data_key
      )
      SELECT
        COALESCE(c.data_key, p.data_key)::int AS data_key,
        COALESCE(c.caixas, 0)::int AS caixas,
        COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
        COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
        COALESCE(c.total_entradas, 0)::numeric(18,2) AS total_entradas,
        COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos
      FROM comercial c
      FULL OUTER JOIN pagamentos p
        ON p.data_key = c.data_key
      ORDER BY COALESCE(c.data_key, p.data_key)
    """
    sql_top_turnos = commercial_cte + f"""
      , pagamentos AS (
        SELECT
          p.id_filial,
          p.id_turno,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
        GROUP BY p.id_filial, p.id_turno
      ), comercial AS (
        SELECT
          id_filial,
          id_turno,
          MIN(data_key)::int AS min_data_key,
          MAX(data_key)::int AS max_data_key,
          MIN(data) AS first_event_at,
          MAX(data) AS last_event_at,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'saida'), 0)::numeric(18,2) AS total_vendas,
          COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = false AND cfop_direction = 'saida')::int AS qtd_vendas,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada')), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(DISTINCT id_comprovante) FILTER (WHERE cancelado = true AND cfop_direction IN ('saida', 'entrada'))::int AS qtd_cancelamentos,
          COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND cfop_direction = 'entrada'), 0)::numeric(18,2) AS total_entradas
        FROM commercial_docs
        GROUP BY id_filial, id_turno
      )
      SELECT
        c.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        c.id_turno,
        {_turno_value_sql('t.payload', 'c.id_turno')} AS turno_value,
        t.id_usuario,
        COALESCE(
          NULLIF(u.nome, ''),
          NULLIF(t.payload->>'NOMEUSUARIOS', ''),
          NULLIF(t.payload->>'NOME_USUARIOS', ''),
          NULLIF(t.payload->>'NOMEUSUARIO', ''),
          NULLIF(t.payload->>'NOME_USUARIO', ''),
          CASE WHEN t.id_usuario IS NOT NULL THEN format('Operador %%s', t.id_usuario) ELSE NULL END
        ) AS usuario_nome,
        c.first_event_at,
        c.last_event_at,
        c.total_vendas,
        c.qtd_vendas,
        c.total_cancelamentos,
        c.qtd_cancelamentos,
        c.total_entradas,
        COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos
      FROM comercial c
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = %s
       AND t.id_filial = c.id_filial
       AND t.id_turno = c.id_turno
       AND (t.data_key_abertura IS NULL OR t.data_key_abertura <= c.max_data_key)
       AND (
             t.data_key_fechamento IS NULL
             OR t.data_key_fechamento >= c.min_data_key
             OR t.is_aberto = true
           )
      LEFT JOIN dw.dim_usuario_caixa u
        ON u.id_empresa = %s
       AND u.id_filial = c.id_filial
       AND u.id_usuario = t.id_usuario
      LEFT JOIN auth.filiais f
        ON f.id_empresa = %s
       AND f.id_filial = c.id_filial
      LEFT JOIN pagamentos p
        ON p.id_filial = c.id_filial
       AND p.id_turno = c.id_turno
      ORDER BY c.total_vendas DESC, c.total_cancelamentos DESC, c.last_event_at DESC
      LIMIT 12
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        summary_row = conn.execute(sql_summary, params + params_pay).fetchone() or {}
        by_day_rows = [dict(row) for row in conn.execute(sql_by_day, params + params_pay).fetchall()]
        top_turnos_rows = [
            dict(row)
            for row in conn.execute(
                sql_top_turnos,
                params + params_pay + [id_empresa, id_empresa, id_empresa],
            ).fetchall()
        ]

    total_vendas = round(float(summary_row.get("total_vendas") or 0), 2)
    total_cancelamentos = round(float(summary_row.get("total_cancelamentos") or 0), 2)
    total_entradas = round(float(summary_row.get("total_entradas") or 0), 2)
    total_pagamentos = round(float(summary_row.get("total_pagamentos") or 0), 2)
    saldo_comercial = round(total_vendas - total_cancelamentos, 2)

    for row in by_day_rows:
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["total_entradas"] = round(float(row.get("total_entradas") or 0), 2)
        row["total_pagamentos"] = round(float(row.get("total_pagamentos") or 0), 2)
        row["saldo_comercial"] = round(row["total_vendas"] - row["total_cancelamentos"], 2)

    for row in top_turnos_rows:
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["total_entradas"] = round(float(row.get("total_entradas") or 0), 2)
        row["total_pagamentos"] = round(float(row.get("total_pagamentos") or 0), 2)
        row["saldo_comercial"] = round(row["total_vendas"] - row["total_cancelamentos"], 2)
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))

    return {
        "summary": (
            f"{int(summary_row.get('caixas_periodo') or 0)} caixa(s) concentraram "
            f"{_format_brl(total_vendas)} em vendas ativas, "
            f"{_format_brl(total_cancelamentos)} em cancelamentos e "
            f"{_format_brl(total_pagamentos)} em recebimentos no período."
            if total_vendas > 0 or total_cancelamentos > 0 or total_pagamentos > 0
            else "Não houve fluxo comercial relevante no período selecionado."
        ),
        "kpis": {
            "total_vendas": total_vendas,
            "qtd_vendas": int(summary_row.get("qtd_vendas") or 0),
            "total_cancelamentos": total_cancelamentos,
            "qtd_cancelamentos": int(summary_row.get("qtd_cancelamentos") or 0),
            "total_entradas": total_entradas,
            "total_pagamentos": total_pagamentos,
            "saldo_comercial": saldo_comercial,
            "caixas_periodo": int(summary_row.get("caixas_periodo") or 0),
        },
        "by_day": by_day_rows,
        "top_turnos": top_turnos_rows,
    }


def sales_operational_current(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date],
) -> Optional[Dict[str, Any]]:
    if as_of is None or dt_ini != dt_fim or dt_fim != as_of:
        return None
    bundle = sales_operational_day_bundle(role, id_empresa, id_filial, as_of)
    if bundle is None:
        return None
    commercial = sales_commercial_overview(role, id_empresa, id_filial, as_of, as_of)
    bundle["commercial_kpis"] = commercial.get("kpis") or {}
    bundle["commercial_by_hour"] = commercial.get("by_hour") or []
    bundle["cfop_breakdown"] = commercial.get("cfop_breakdown") or []
    bundle["monthly_evolution"] = commercial.get("monthly_evolution") or []
    return bundle | {"reading_status": "operational_current"}


def _empty_sales_overview_bundle() -> Dict[str, Any]:
    return {
        "kpis": {
            "faturamento": 0.0,
            "margem": 0.0,
            "ticket_medio": 0.0,
            "devolucoes": 0.0,
        },
        "commercial_kpis": {
            "saidas": 0.0,
            "qtd_saidas": 0,
            "entradas": 0.0,
            "qtd_entradas": 0,
            "cancelamentos": 0.0,
            "qtd_cancelamentos": 0,
        },
        "by_day": [],
        "by_hour": [],
        "commercial_by_hour": [],
        "cfop_breakdown": [],
        "monthly_evolution": [],
        "annual_comparison": {"current_year": None, "previous_year": None, "months": []},
        "top_products": [],
        "top_groups": [],
        "top_employees": [],
        "stats": {"vendas": 0},
        "operational_sync": {
            "last_sync_at": None,
            "source": "dw.fact_venda",
            "dt_ref": None,
        },
        "freshness": {
            "mode": "operational_range",
            "operational_day": None,
            "live_through_at": None,
            "historical_through_dt": None,
            "source": "dw.fact_venda",
        },
    }


def _sales_window_fact_cte(
    *,
    id_empresa: int,
    id_filial: Optional[int],
    date_predicate_sql: str,
    date_params: List[Any],
) -> tuple[str, List[Any], Optional[int]]:
    where_filial_venda, branch_params = _branch_scope_clause("v.id_filial", id_filial)
    where_filial_item, item_branch_params = _branch_scope_clause("i.id_filial", id_filial)
    params = (
        [id_empresa] + date_params + branch_params
        + [id_empresa] + item_branch_params
        + [id_empresa] + date_params + branch_params
        + [id_empresa] + item_branch_params
    )
    conn_branch_id = _conn_branch_id(id_filial)
    cte = f"""
      WITH sale_headers AS MATERIALIZED (
        SELECT
          v.id_empresa,
          v.id_filial,
          v.id_db,
          v.id_comprovante,
          v.id_comprovante AS doc_key,
          v.data,
          v.data_key,
          v.updated_at AS venda_updated_at,
          v.created_at AS venda_created_at
        FROM dw.fact_venda v
        WHERE v.id_empresa = %s
          AND {date_predicate_sql}
          AND {_sales_status_expression('v')} = 1
          {where_filial_venda}
      ), sale_items AS MATERIALIZED (
        SELECT
          v.id_empresa,
          v.id_filial,
          v.id_db,
          v.id_comprovante,
          v.doc_key,
          v.data,
          v.data_key,
          v.venda_updated_at,
          v.venda_created_at,
          i.id_produto,
          i.id_grupo_produto,
          i.id_funcionario,
          i.valor_unitario,
          i.total,
          i.custo_total,
          i.margem,
          i.qtd,
          i.updated_at AS item_updated_at,
          i.created_at AS item_created_at
        FROM sale_headers v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa
         AND i.id_filial = v.id_filial
         AND i.id_db = v.id_db
         AND i.id_comprovante = v.id_comprovante
        WHERE i.id_empresa = %s
          {where_filial_item}
          AND {sales_cfop_filter_sql('i')}
      ), return_headers AS MATERIALIZED (
        SELECT
          v.id_empresa,
          v.id_filial,
          v.id_db,
          v.id_comprovante,
          v.data,
          v.data_key,
          v.updated_at AS venda_updated_at,
          v.created_at AS venda_created_at
        FROM dw.fact_venda v
        WHERE v.id_empresa = %s
          AND {date_predicate_sql}
          AND {sales_status_filter_sql('v', RETURN_STATUS)}
          {where_filial_venda}
      ), return_items AS MATERIALIZED (
        SELECT
          v.id_empresa,
          v.id_filial,
          v.id_db,
          v.id_comprovante,
          v.data,
          v.data_key,
          v.venda_updated_at,
          v.venda_created_at,
          i.total,
          i.updated_at AS item_updated_at,
          i.created_at AS item_created_at
        FROM return_headers v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa
         AND i.id_filial = v.id_filial
         AND i.id_db = v.id_db
         AND i.id_comprovante = v.id_comprovante
        WHERE i.id_empresa = %s
          {where_filial_item}
          AND {sales_cfop_filter_sql('i')}
      )
    """
    return cte, params, conn_branch_id


def _normalize_sales_top_products_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                **row,
                "faturamento": round(float(row.get("faturamento") or 0), 2),
                "custo_total": round(float(row.get("custo_total") or 0), 2),
                "margem": round(float(row.get("margem") or 0), 2),
                "qtd": round(float(row.get("qtd") or 0), 3),
                "valor_unitario_medio": round(float(row.get("valor_unitario_medio") or 0), 4),
            }
        )
    return normalized


def sales_operational_day_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    day_ref: date,
    *,
    include_rankings: bool = True,
    canonicalize_groups: bool = True,
) -> Optional[Dict[str, Any]]:
    day_key = _date_key(day_ref)
    sales_window_cte, params, conn_branch_id = _sales_window_fact_cte(
        id_empresa=id_empresa,
        id_filial=id_filial,
        date_predicate_sql="v.data_key = %s",
        date_params=[day_key],
    )

    sql_kpis = sales_window_cte + """
      SELECT
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem,
        CASE
          WHEN COUNT(DISTINCT si.doc_key) = 0 THEN 0::numeric(18,2)
          ELSE (SUM(si.total) / COUNT(DISTINCT si.doc_key))::numeric(18,2)
        END AS ticket_medio,
        COALESCE((SELECT SUM(ri.total) FROM return_items ri), 0)::numeric(18,2) AS devolucoes,
        COUNT(DISTINCT si.doc_key)::int AS vendas,
        (
          SELECT MAX(sync_ts)
          FROM (
            SELECT MAX(COALESCE(venda_updated_at, item_updated_at, venda_created_at, item_created_at, data)) AS sync_ts
            FROM sale_items
            UNION ALL
            SELECT MAX(COALESCE(venda_updated_at, item_updated_at, venda_created_at, item_created_at, data)) AS sync_ts
            FROM return_items
          ) sync_points
        ) AS latest_sync_at
      FROM sale_items si
    """
    sql_by_hour = sales_window_cte + """
      SELECT
        EXTRACT(HOUR FROM si.data)::int AS hora,
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem,
        COUNT(DISTINCT si.doc_key)::int AS vendas
      FROM sale_items si
      WHERE si.data IS NOT NULL
      GROUP BY 1
      ORDER BY 1
    """
    active_filter = _active_product_filter_expression("p")
    sql_top_products = sales_window_cte + """
      SELECT
        si.id_produto,
        MAX(COALESCE(NULLIF(p.nome, ''), '#ID ' || si.id_produto::text)) AS produto_nome,
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.custo_total), 0)::numeric(18,2) AS custo_total,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem,
        COALESCE(SUM(si.qtd), 0)::numeric(18,3) AS qtd,
        CASE
          WHEN COALESCE(SUM(si.qtd), 0) = 0 THEN 0::numeric(18,4)
          ELSE (SUM(si.total) / NULLIF(SUM(si.qtd), 0))::numeric(18,4)
        END AS valor_unitario_medio
      FROM sale_items si
      LEFT JOIN dw.dim_produto p
        ON p.id_empresa = si.id_empresa
       AND p.id_filial = si.id_filial
       AND p.id_produto = si.id_produto
      WHERE """ + active_filter + """
      GROUP BY si.id_produto
      ORDER BY faturamento DESC
      LIMIT 15
    """
    sql_top_groups = sales_window_cte + f"""
      SELECT
        COALESCE(si.id_grupo_produto, -1) AS id_grupo_produto,
        MAX({_group_display_name_expression('g', 'p').replace('i.', 'si.')}) AS grupo_nome,
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem
      FROM sale_items si
      LEFT JOIN dw.dim_produto p
        ON p.id_empresa = si.id_empresa
       AND p.id_filial = si.id_filial
       AND p.id_produto = si.id_produto
      LEFT JOIN dw.dim_grupo_produto g
        ON g.id_empresa = si.id_empresa
       AND g.id_filial = si.id_filial
       AND g.id_grupo_produto = si.id_grupo_produto
      WHERE """ + active_filter + """
      GROUP BY COALESCE(si.id_grupo_produto, -1)
      ORDER BY faturamento DESC
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        conn.execute(f"SET LOCAL statement_timeout = {int(SALES_OPERATIONAL_FALLBACK_TIMEOUT_MS)}")
        kpis = dict(conn.execute(sql_kpis, params).fetchone() or {})
        by_hour = [dict(row) for row in conn.execute(sql_by_hour, params).fetchall()] if include_rankings else []
        top_products = [dict(row) for row in conn.execute(sql_top_products, params).fetchall()] if include_rankings else []
        top_groups_raw = [dict(row) for row in conn.execute(sql_top_groups, params).fetchall()] if include_rankings else []
    top_groups = (
        _collapse_group_rank_rows(top_groups_raw, limit=10)
        if include_rankings and canonicalize_groups
        else top_groups_raw
    )
    top_products = _normalize_sales_top_products_rows(top_products)

    faturamento = float(kpis.get("faturamento") or 0)
    margem = float(kpis.get("margem") or 0)
    devolucoes = float(kpis.get("devolucoes") or 0)
    vendas = int(kpis.get("vendas") or 0)
    if faturamento <= 0 and margem <= 0 and devolucoes <= 0 and vendas <= 0 and not by_hour:
        return None

    latest_sync_at = (
        kpis["latest_sync_at"].isoformat()
        if hasattr(kpis.get("latest_sync_at"), "isoformat")
        else None
    )
    return {
        "kpis": {
            "faturamento": round(faturamento, 2),
            "margem": round(margem, 2),
            "ticket_medio": round(float(kpis.get("ticket_medio") or 0), 2),
            "devolucoes": round(devolucoes, 2),
        },
        "by_day": [{
            "data_key": day_key,
            "id_filial": conn_branch_id,
            "faturamento": round(faturamento, 2),
            "margem": round(margem, 2),
        }],
        "by_hour": by_hour,
        "top_products": top_products,
        "top_groups": top_groups,
        "top_employees": [],
        "stats": {
            "vendas": vendas,
            "data_key": day_key,
        },
        "operational_sync": {
            "last_sync_at": latest_sync_at,
            "source": "dw.fact_venda",
            "dt_ref": day_ref.isoformat(),
        },
        "freshness": {
            "mode": "live_day",
            "operational_day": day_ref.isoformat(),
            "live_through_at": latest_sync_at,
            "historical_through_dt": None,
            "source": "dw.fact_venda",
        },
    }


def sales_operational_range_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    *,
    include_rankings: bool = True,
    canonicalize_groups: bool = True,
) -> Optional[Dict[str, Any]]:
    if dt_fim < dt_ini:
        return None

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    sales_window_cte, params, conn_branch_id = _sales_window_fact_cte(
        id_empresa=id_empresa,
        id_filial=id_filial,
        date_predicate_sql="v.data_key BETWEEN %s AND %s",
        date_params=[ini, fim],
    )
    branch_select = "NULL::int AS id_filial" if conn_branch_id is None else f"{int(conn_branch_id)}::int AS id_filial"

    sql_kpis = sales_window_cte + """
      SELECT
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem,
        CASE
          WHEN COUNT(DISTINCT si.doc_key) = 0 THEN 0::numeric(18,2)
          ELSE (SUM(si.total) / COUNT(DISTINCT si.doc_key))::numeric(18,2)
        END AS ticket_medio,
        COALESCE((SELECT SUM(ri.total) FROM return_items ri), 0)::numeric(18,2) AS devolucoes,
        COUNT(DISTINCT si.doc_key)::int AS vendas,
        (
          SELECT MAX(sync_ts)
          FROM (
            SELECT MAX(COALESCE(venda_updated_at, item_updated_at, venda_created_at, item_created_at, data)) AS sync_ts
            FROM sale_items
            UNION ALL
            SELECT MAX(COALESCE(venda_updated_at, item_updated_at, venda_created_at, item_created_at, data)) AS sync_ts
            FROM return_items
          ) sync_points
        ) AS latest_sync_at
      FROM sale_items si
    """
    sql_by_day = sales_window_cte + f"""
      SELECT
        si.data_key,
        {branch_select},
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem
      FROM sale_items si
      WHERE si.data_key IS NOT NULL
      GROUP BY si.data_key
      ORDER BY si.data_key
    """
    sql_by_hour = sales_window_cte + f"""
      SELECT
        si.data_key,
        {branch_select},
        EXTRACT(HOUR FROM si.data)::int AS hora,
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem,
        COUNT(DISTINCT si.doc_key)::int AS vendas
      FROM sale_items si
      WHERE si.data IS NOT NULL
      GROUP BY si.data_key, hora
      ORDER BY si.data_key, hora
    """
    active_filter = _active_product_filter_expression("p")
    sql_top_products = sales_window_cte + """
      SELECT
        si.id_produto,
        MAX(COALESCE(NULLIF(p.nome, ''), '#ID ' || si.id_produto::text)) AS produto_nome,
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.custo_total), 0)::numeric(18,2) AS custo_total,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem,
        COALESCE(SUM(si.qtd), 0)::numeric(18,3) AS qtd,
        CASE
          WHEN COALESCE(SUM(si.qtd), 0) = 0 THEN 0::numeric(18,4)
          ELSE (SUM(si.total) / NULLIF(SUM(si.qtd), 0))::numeric(18,4)
        END AS valor_unitario_medio
      FROM sale_items si
      LEFT JOIN dw.dim_produto p
        ON p.id_empresa = si.id_empresa
       AND p.id_filial = si.id_filial
       AND p.id_produto = si.id_produto
      WHERE """ + active_filter + """
      GROUP BY si.id_produto
      ORDER BY faturamento DESC
      LIMIT 15
    """
    sql_top_groups = sales_window_cte + f"""
      SELECT
        COALESCE(si.id_grupo_produto, -1) AS id_grupo_produto,
        MAX({_group_display_name_expression('g', 'p').replace('i.', 'si.')}) AS grupo_nome,
        COALESCE(SUM(si.total), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(si.margem), 0)::numeric(18,2) AS margem
      FROM sale_items si
      LEFT JOIN dw.dim_produto p
        ON p.id_empresa = si.id_empresa
       AND p.id_filial = si.id_filial
       AND p.id_produto = si.id_produto
      LEFT JOIN dw.dim_grupo_produto g
        ON g.id_empresa = si.id_empresa
       AND g.id_filial = si.id_filial
       AND g.id_grupo_produto = si.id_grupo_produto
      WHERE """ + active_filter + """
      GROUP BY COALESCE(si.id_grupo_produto, -1)
      ORDER BY faturamento DESC
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        kpis = dict(conn.execute(sql_kpis, params).fetchone() or {})
        by_day = [dict(row) for row in conn.execute(sql_by_day, params).fetchall()]
        by_hour = [dict(row) for row in conn.execute(sql_by_hour, params).fetchall()] if include_rankings else []
        top_products = [dict(row) for row in conn.execute(sql_top_products, params).fetchall()] if include_rankings else []
        top_groups_raw = [dict(row) for row in conn.execute(sql_top_groups, params).fetchall()] if include_rankings else []
    top_groups = (
        _collapse_group_rank_rows(top_groups_raw, limit=10)
        if include_rankings and canonicalize_groups
        else top_groups_raw
    )
    top_products = _normalize_sales_top_products_rows(top_products)

    faturamento = float(kpis.get("faturamento") or 0)
    margem = float(kpis.get("margem") or 0)
    devolucoes = float(kpis.get("devolucoes") or 0)
    vendas = int(kpis.get("vendas") or 0)
    if faturamento <= 0 and margem <= 0 and devolucoes <= 0 and vendas <= 0 and not by_day:
        return None

    latest_sync_at = (
        kpis["latest_sync_at"].isoformat()
        if hasattr(kpis.get("latest_sync_at"), "isoformat")
        else None
    )
    return {
        "kpis": {
            "faturamento": round(faturamento, 2),
            "margem": round(margem, 2),
            "ticket_medio": round(float(kpis.get("ticket_medio") or 0), 2),
            "devolucoes": round(devolucoes, 2),
        },
        "by_day": by_day,
        "by_hour": by_hour,
        "top_products": top_products,
        "top_groups": top_groups,
        "top_employees": [],
        "stats": {
            "vendas": vendas,
        },
        "operational_sync": {
            "last_sync_at": latest_sync_at,
            "source": "dw.fact_venda",
            "dt_ref": dt_fim.isoformat(),
        },
        "freshness": {
            "mode": "live_range",
            "operational_day": dt_fim.isoformat(),
            "live_through_at": latest_sync_at,
            "historical_through_dt": dt_fim.isoformat(),
            "source": "dw.fact_venda",
        },
    }


def _sales_data_keys(rows: List[Dict[str, Any]]) -> set[int]:
    return {
        int(row.get("data_key") or 0)
        for row in rows
        if int(row.get("data_key") or 0) > 0
    }


def sales_by_hour(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params
    sql = f"""
      SELECT data_key, id_filial, hora, faturamento, margem, vendas
      FROM mart.agg_vendas_hora
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, hora
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def sales_top_products(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]
    sql = f"""
      SELECT
        id_produto,
        MAX(produto_nome) AS produto_nome,
        COALESCE(SUM(faturamento), 0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(custo_total), 0)::numeric(18,2) AS custo_total,
        COALESCE(SUM(margem), 0)::numeric(18,2) AS margem,
        COALESCE(SUM(qtd), 0)::numeric(18,3) AS qtd,
        CASE
          WHEN COALESCE(SUM(qtd), 0) = 0 THEN 0::numeric(18,4)
          ELSE ROUND((SUM(faturamento) / NULLIF(SUM(qtd), 0))::numeric, 4)
        END AS valor_unitario_medio
      FROM mart.agg_produtos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY id_produto
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def sales_top_groups(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]
    sql = f"""
      SELECT
        id_grupo_produto,
        MAX(grupo_nome) AS grupo_nome,
        SUM(faturamento)::numeric(18,2) AS faturamento,
        SUM(margem)::numeric(18,2) AS margem
      FROM mart.agg_grupos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY id_grupo_produto
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def sales_top_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]
    sql = f"""
      SELECT
        id_funcionario,
        MAX(funcionario_nome) AS funcionario_nome,
        SUM(faturamento) AS faturamento,
        SUM(margem) AS margem,
        SUM(vendas)::int AS vendas
      FROM mart.agg_funcionarios_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      AND COALESCE(id_funcionario, -1) <> -1
      AND COALESCE(NULLIF(funcionario_nome, ''), '') <> ''
      {where_filial}
      GROUP BY id_funcionario
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _sales_historical_bundle_from_marts(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    *,
    include_details: bool = True,
) -> Dict[str, Any]:
    kpis = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim) or {}
    by_day = dashboard_series(role, id_empresa, id_filial, dt_ini, dt_fim)
    by_hour = sales_by_hour(role, id_empresa, id_filial, dt_ini, dt_fim) if include_details else []
    top_products = sales_top_products(role, id_empresa, id_filial, dt_ini, dt_fim, limit=15) if include_details else []
    top_groups = sales_top_groups(role, id_empresa, id_filial, dt_ini, dt_fim, limit=10) if include_details else []
    top_employees = sales_top_employees(role, id_empresa, id_filial, dt_ini, dt_fim, limit=10) if include_details else []

    return {
        "kpis": {
            "faturamento": round(float(kpis.get("faturamento") or 0), 2),
            "margem": round(float(kpis.get("margem") or 0), 2),
            "ticket_medio": round(float(kpis.get("ticket_medio") or 0), 2),
            "devolucoes": 0.0,
        },
        "by_day": by_day,
        "by_hour": by_hour,
        "top_products": _normalize_sales_top_products_rows(top_products),
        "top_groups": top_groups,
        "top_employees": top_employees,
        "stats": {
            "vendas": int(sum(int(row.get("vendas") or 0) for row in by_hour)),
        },
        "operational_sync": {
            "last_sync_at": None,
            "source": "mart.agg_vendas_diaria",
            "dt_ref": dt_fim.isoformat(),
        },
        "freshness": {
            "mode": "mart_snapshot",
            "operational_day": None,
            "live_through_at": None,
            "historical_through_dt": dt_fim.isoformat(),
            "source": "mart.agg_vendas_diaria",
        },
    }


def sales_overview_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
    *,
    include_details: bool = True,
) -> Dict[str, Any]:
    sales_coverage = commercial_window_coverage(role, id_empresa, id_filial, dt_ini, dt_fim)
    effective_dt_ini = sales_coverage.get("effective_dt_ini") or dt_ini
    effective_dt_fim = sales_coverage.get("effective_dt_fim") or dt_fim
    # 2026-04-29: marts refreshed every operational cycle — always use marts.
    bundle = _sales_historical_bundle_from_marts(
        role,
        id_empresa,
        id_filial,
        effective_dt_ini,
        effective_dt_fim,
        include_details=include_details,
    )
    commercial = sales_commercial_overview(role, id_empresa, id_filial, effective_dt_ini, effective_dt_fim)
    bundle["commercial_kpis"] = commercial.get("kpis") or _empty_sales_overview_bundle()["commercial_kpis"]
    bundle["cfop_breakdown"] = commercial.get("cfop_breakdown") or []
    bundle["commercial_by_hour"] = commercial.get("by_hour") or []
    bundle["monthly_evolution"] = commercial.get("monthly_evolution") or []
    bundle["annual_comparison"] = commercial.get("annual_comparison") or _empty_sales_overview_bundle()["annual_comparison"]
    bundle["commercial_coverage"] = sales_coverage

    freshness = dict(bundle.get("freshness") or {})
    operational_sync = dict(bundle.get("operational_sync") or {})
    reading_status = "mart_snapshot"

    freshness.update(
        {
            "mode": "mart_snapshot",
            "operational_day": None,
            "historical_through_dt": effective_dt_fim.isoformat(),
            "source": "mart.agg_vendas_diaria",
        }
    )
    operational_sync["dt_ref"] = effective_dt_fim.isoformat()

    if sales_coverage.get("mode") == "shifted_latest":
        reading_status = "latest_compatible"
        freshness["mode"] = "latest_compatible"
        operational_sync["dt_ref"] = _iso_or_none(sales_coverage.get("effective_dt_fim"))

    bundle["freshness"] = freshness
    bundle["operational_sync"] = operational_sync
    bundle["reading_status"] = reading_status
    return bundle


# ========================
# Pricing (competitor simulation)
# ========================

def competitor_pricing_overview(
    role: str,
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    days_simulation: int = 10,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    days_window = max((dt_fim - dt_ini).days + 1, 1)
    days_sim = max(days_simulation, 1)
    fuel_filter = _fuel_filter_expression("g", "p")
    active_filter = _active_product_filter_expression("p")

    sql = f"""
      WITH sales AS (
        SELECT
          id_produto,
          COALESCE(SUM(faturamento),0)::numeric(18,2) AS faturamento_periodo,
          COALESCE(SUM(qtd),0)::numeric(18,3) AS qtd_periodo
        FROM mart.agg_produtos_diaria
        WHERE id_empresa = %s
          AND id_filial = %s
          AND data_key BETWEEN %s AND %s
        GROUP BY id_produto
      ),
      fuel_products AS (
        SELECT
          p.id_produto,
          COALESCE(NULLIF(p.nome, ''), '#ID ' || p.id_produto::text) AS produto_nome,
          {_group_name_expression("g", "p")} AS grupo_nome,
          {_fuel_family_case_expression("g", "p")} AS familia_combustivel,
          COALESCE(p.custo_medio, 0)::numeric(18,4) AS custo_medio
        FROM dw.dim_produto p
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = p.id_empresa
         AND g.id_filial = p.id_filial
         AND g.id_grupo_produto = p.id_grupo_produto
        WHERE p.id_empresa = %s
          AND p.id_filial = %s
          AND {fuel_filter}
          AND {active_filter}
      ),
      comp AS (
        SELECT
          id_produto,
          competitor_price::numeric(18,4) AS competitor_price,
          updated_at
        FROM app.competitor_fuel_prices
        WHERE id_empresa = %s
          AND id_filial = %s
      )
      SELECT
        fp.id_produto,
        fp.produto_nome,
        fp.grupo_nome,
        fp.familia_combustivel,
        fp.custo_medio,
        COALESCE(s.qtd_periodo, 0)::numeric(18,3) AS qtd_periodo,
        COALESCE(s.faturamento_periodo, 0)::numeric(18,2) AS faturamento_periodo,
        CASE
          WHEN COALESCE(s.qtd_periodo, 0) > 0 THEN (s.faturamento_periodo / NULLIF(s.qtd_periodo,0))::numeric(18,4)
          ELSE 0::numeric(18,4)
        END AS avg_price_current,
        COALESCE(c.competitor_price, 0)::numeric(18,4) AS competitor_price,
        c.updated_at AS competitor_updated_at
      FROM fuel_products fp
      LEFT JOIN sales s ON s.id_produto = fp.id_produto
      LEFT JOIN comp c ON c.id_produto = fp.id_produto
      ORDER BY fp.produto_nome
    """
    params = [id_empresa, id_filial, ini, fim, id_empresa, id_filial, id_empresa, id_filial]
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = list(conn.execute(sql, params).fetchall())
        if not rows:
            fallback_sql = f"""
              SELECT
                p.id_produto,
                COALESCE(NULLIF(p.nome, ''), '#ID ' || p.id_produto::text) AS produto_nome,
                {_group_name_expression("g", "p")} AS grupo_nome,
                {_fuel_family_case_expression("g", "p")} AS familia_combustivel,
                COALESCE(p.custo_medio, 0)::numeric(18,4) AS custo_medio,
                0::numeric(18,3) AS qtd_periodo,
                0::numeric(18,2) AS faturamento_periodo,
                0::numeric(18,4) AS avg_price_current,
                COALESCE(c.competitor_price, 0)::numeric(18,4) AS competitor_price,
                c.updated_at AS competitor_updated_at
              FROM dw.dim_produto p
              LEFT JOIN dw.dim_grupo_produto g
                ON g.id_empresa = p.id_empresa
               AND g.id_filial = p.id_filial
               AND g.id_grupo_produto = p.id_grupo_produto
              LEFT JOIN app.competitor_fuel_prices c
                ON c.id_empresa = p.id_empresa
               AND c.id_filial = p.id_filial
               AND c.id_produto = p.id_produto
              WHERE p.id_empresa = %s
                AND p.id_filial = %s
                AND {fuel_filter}
                AND {active_filter}
              ORDER BY p.nome
            """
            rows = list(conn.execute(fallback_sql, (id_empresa, id_filial)).fetchall())

    items: List[Dict[str, Any]] = []
    total_current_revenue_10d = 0.0
    total_no_change_revenue_10d = 0.0
    total_match_revenue_10d = 0.0
    total_lost_if_no_change_10d = 0.0
    total_match_vs_current_10d = 0.0
    total_match_vs_no_change_10d = 0.0

    for row in rows:
        avg_daily_volume = float(row.get("qtd_periodo") or 0) / float(days_window)
        current_price = float(row.get("avg_price_current") or 0)
        competitor_price = float(row.get("competitor_price") or 0)
        custo_medio = float(row.get("custo_medio") or 0)

        baseline_revenue_10d = current_price * avg_daily_volume * days_sim
        baseline_margin_10d = (current_price - custo_medio) * avg_daily_volume * days_sim

        price_gap = 0.0
        volume_loss_rate = 0.0
        if current_price > 0 and competitor_price > 0:
            price_gap = current_price - competitor_price
            # Conservative elasticity proxy: bigger positive gap vs competitor => likely lower conversion.
            if price_gap > 0:
                volume_loss_rate = min(0.35, max(0.0, (price_gap / current_price) * 1.5))

        no_change_daily_volume = avg_daily_volume * (1.0 - volume_loss_rate)
        no_change_revenue_10d = current_price * no_change_daily_volume * days_sim
        no_change_margin_10d = (current_price - custo_medio) * no_change_daily_volume * days_sim

        matched_price = competitor_price if competitor_price > 0 else current_price
        match_revenue_10d = matched_price * avg_daily_volume * days_sim
        match_margin_10d = (matched_price - custo_medio) * avg_daily_volume * days_sim

        lost_if_no_change_10d = baseline_revenue_10d - no_change_revenue_10d
        impact_match_vs_current_10d = match_revenue_10d - baseline_revenue_10d
        impact_match_vs_no_change_10d = match_revenue_10d - no_change_revenue_10d

        total_current_revenue_10d += baseline_revenue_10d
        total_no_change_revenue_10d += no_change_revenue_10d
        total_match_revenue_10d += match_revenue_10d
        total_lost_if_no_change_10d += lost_if_no_change_10d
        total_match_vs_current_10d += impact_match_vs_current_10d
        total_match_vs_no_change_10d += impact_match_vs_no_change_10d

        items.append(
            {
                "id_produto": row.get("id_produto"),
                "produto_nome": row.get("produto_nome"),
                "grupo_nome": row.get("grupo_nome"),
                "familia_combustivel": row.get("familia_combustivel"),
                "avg_daily_volume": round(avg_daily_volume, 3),
                "avg_price_current": round(current_price, 4),
                "competitor_price": round(competitor_price, 4),
                "station_price_gap": round(price_gap, 4),
                "volume_loss_rate_no_change": round(volume_loss_rate, 4),
                "competitor_updated_at": row.get("competitor_updated_at"),
                "scenario_current": {
                    "revenue_10d": round(baseline_revenue_10d, 2),
                    "margin_10d": round(baseline_margin_10d, 2),
                },
                "scenario_no_change": {
                    "expected_volume_10d": round(no_change_daily_volume * days_sim, 3),
                    "revenue_10d": round(no_change_revenue_10d, 2),
                    "margin_10d": round(no_change_margin_10d, 2),
                    "lost_revenue_10d": round(lost_if_no_change_10d, 2),
                },
                "scenario_match_competitor": {
                    "revenue_10d": round(match_revenue_10d, 2),
                    "margin_10d": round(match_margin_10d, 2),
                    "impact_vs_current_10d": round(impact_match_vs_current_10d, 2),
                    "impact_vs_no_change_10d": round(impact_match_vs_no_change_10d, 2),
                },
                "recommendation": (
                    "Ajustar preço para defender volume"
                    if competitor_price > 0 and impact_match_vs_no_change_10d > 0
                    else "Manter preço atual e monitorar a praça"
                ),
            }
        )

    items_sorted = sorted(
        items,
        key=lambda x: abs(float((x.get("scenario_match_competitor") or {}).get("impact_vs_no_change_10d") or 0)),
        reverse=True,
    )

    return {
        "meta": {
            "dt_ini": dt_ini.isoformat(),
            "dt_fim": dt_fim.isoformat(),
            "days_window": days_window,
            "days_simulation": days_sim,
        },
        "summary": {
            "fuel_types": len(items_sorted),
            "total_current_revenue_10d": round(total_current_revenue_10d, 2),
            "total_no_change_revenue_10d": round(total_no_change_revenue_10d, 2),
            "total_match_revenue_10d": round(total_match_revenue_10d, 2),
            "total_lost_if_no_change_10d": round(total_lost_if_no_change_10d, 2),
            "total_match_vs_current_10d": round(total_match_vs_current_10d, 2),
            "total_match_vs_no_change_10d": round(total_match_vs_no_change_10d, 2),
        },
        "items": items_sorted,
    }


def competitor_pricing_upsert(
    role: str,
    id_empresa: int,
    id_filial: int,
    items: List[Dict[str, Any]],
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    if not items:
        return {"saved": 0}

    sql = """
      INSERT INTO app.competitor_fuel_prices
        (id_empresa, id_filial, id_produto, competitor_price, updated_by, updated_at)
      VALUES (%s, %s, %s, %s, %s, now())
      ON CONFLICT (id_empresa, id_filial, id_produto)
      DO UPDATE
        SET competitor_price = EXCLUDED.competitor_price,
            updated_by = EXCLUDED.updated_by,
            updated_at = now()
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        for item in items:
            conn.execute(
                sql,
                (
                    id_empresa,
                    id_filial,
                    int(item["id_produto"]),
                    float(item["competitor_price"]),
                    updated_by,
                ),
            )
        conn.commit()

    return {"saved": len(items)}


def competitor_fuel_product_ids(role: str, id_empresa: int, id_filial: int, product_ids: List[int]) -> set[int]:
    if not product_ids:
        return set()
    normalized_ids = [int(value) for value in product_ids]
    fuel_filter = _fuel_filter_expression("g", "p")
    active_filter = _active_product_filter_expression("p")
    sql = f"""
      SELECT p.id_produto
      FROM dw.dim_produto p
      LEFT JOIN dw.dim_grupo_produto g
        ON g.id_empresa = p.id_empresa
       AND g.id_filial = p.id_filial
       AND g.id_grupo_produto = p.id_grupo_produto
      WHERE p.id_empresa = %s
        AND p.id_filial = %s
        AND p.id_produto = ANY(%s)
        AND {fuel_filter}
        AND {active_filter}
    """
    params = [id_empresa, id_filial, normalized_ids]
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = conn.execute(sql, params).fetchall()
    return {int(row["id_produto"]) for row in rows}


# ========================
# Anti-fraude
# ========================

def fraud_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql = f"""
      SELECT
        COALESCE(SUM(cancelamentos),0)::int AS cancelamentos,
        COALESCE(SUM(valor_cancelado),0)::numeric(18,2) AS valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"cancelamentos": 0, "valor_cancelado": 0}


def fraud_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql = f"""
      SELECT data_key, id_filial, cancelamentos, valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def fraud_data_window(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa] + branch_params
    sql = f"""
      SELECT
        MIN(data_key)::int AS min_data_key,
        MAX(data_key)::int AS max_data_key,
        COUNT(*)::int AS rows
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"min_data_key": None, "max_data_key": None, "rows": 0}


def fraud_last_events(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("e.id_filial", id_filial)
    params = [id_empresa, id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        e.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        e.id_db,
        e.id_comprovante,
        e.data,
        e.data_key,
        e.id_usuario,
        e.id_usuario_documento,
        e.usuario_source,
        e.usuario_nome,
        e.id_turno,
        {_turno_value_sql('t.payload', 'e.id_turno')} AS turno_value,
        e.valor_total
      FROM mart.fraude_cancelamentos_eventos e
      LEFT JOIN auth.filiais f
        ON f.id_empresa = %s
       AND f.id_filial = e.id_filial
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = e.id_empresa
       AND t.id_filial = e.id_filial
       AND t.id_turno = e.id_turno
      WHERE e.id_empresa = %s
        AND e.data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY e.data DESC NULLS LAST
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    return rows


def fraud_top_users(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        id_usuario,
        MAX(usuario_nome) AS usuario_nome,
        COUNT(*)::int AS cancelamentos,
        COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_cancelado,
        COUNT(*) FILTER (WHERE usuario_source = 'turno')::int AS resolvidos_por_turno,
        COUNT(*) FILTER (WHERE usuario_source = 'comprovante')::int AS fallback_comprovante
      FROM mart.fraude_cancelamentos_eventos
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY id_usuario
      ORDER BY valor_cancelado DESC, cancelamentos DESC, id_usuario
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
    return rows


# ========================
# Risk Scoring / Insights
# ========================

def risk_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql = f"""
      SELECT
        COALESCE(SUM(eventos_risco_total),0)::int AS total_eventos,
        COALESCE(SUM(eventos_alto_risco),0)::int AS eventos_alto_risco,
        COALESCE(SUM(impacto_estimado_total),0)::numeric(18,2) AS impacto_total,
        COALESCE(AVG(score_medio),0)::numeric(10,2) AS score_medio
      FROM mart.agg_risco_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"total_eventos": 0, "eventos_alto_risco": 0, "impacto_total": 0, "score_medio": 0}


def risk_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql = f"""
      SELECT
        data_key,
        id_filial,
        eventos_risco_total,
        eventos_alto_risco,
        impacto_estimado_total,
        score_medio,
        p95_score
      FROM mart.agg_risco_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def risk_data_window(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa] + branch_params
    sql = f"""
      SELECT
        MIN(data_key)::int AS min_data_key,
        MAX(data_key)::int AS max_data_key,
        COUNT(*)::int AS rows
      FROM mart.agg_risco_diaria
      WHERE id_empresa = %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"min_data_key": None, "max_data_key": None, "rows": 0}


def risk_top_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        id_funcionario,
        MAX(funcionario_nome) AS funcionario_nome,
        SUM(eventos)::int AS eventos,
        SUM(alto_risco)::int AS alto_risco,
        SUM(impacto_estimado)::numeric(18,2) AS impacto_estimado,
        AVG(score_medio)::numeric(10,2) AS score_medio
      FROM mart.risco_top_funcionarios_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      AND COALESCE(id_funcionario, -1) <> -1
      AND COALESCE(NULLIF(funcionario_nome, ''), '') <> ''
      AND UPPER(COALESCE(funcionario_nome, '')) NOT IN ('(SEM FUNCIONÁRIO)', '(SEM FUNCIONARIO)', 'SEM FUNCIONÁRIO', 'SEM FUNCIONARIO')
      {where_filial}
      GROUP BY id_funcionario
      ORDER BY impacto_estimado DESC, score_medio DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def risk_last_events(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("e.id_filial", id_filial)
    params = [id_empresa, id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        e.id,
        e.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        e.data_key,
        e.data,
        e.event_type,
        e.id_db,
        e.id_comprovante,
        e.id_movprodutos,
        e.id_usuario,
        e.id_funcionario,
        e.funcionario_nome,
        fo.id_usuario AS operador_caixa_id,
        fo.usuario_nome AS operador_caixa_nome,
        fo.usuario_source AS operador_caixa_source,
        e.id_turno,
        {_turno_value_sql('t.payload', 'e.id_turno')} AS turno_value,
        e.valor_total,
        e.impacto_estimado,
        e.score_risco,
        e.score_level,
        e.reasons
      FROM mart.risco_eventos_recentes
      e
      LEFT JOIN auth.filiais f
        ON f.id_empresa = %s
       AND f.id_filial = e.id_filial
      LEFT JOIN mart.fraude_cancelamentos_eventos fo
        ON fo.id_empresa = e.id_empresa
       AND fo.id_filial = e.id_filial
       AND fo.id_db = e.id_db
       AND fo.id_comprovante = e.id_comprovante
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = e.id_empresa
       AND t.id_filial = e.id_filial
       AND t.id_turno = e.id_turno
      WHERE e.id_empresa = %s
        AND e.data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY e.data DESC NULLS LAST, e.id DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["event_label"] = _event_type_label(row.get("event_type"))
        row["funcionario_label"] = _employee_label(row.get("funcionario_nome"), row.get("id_funcionario"))
        row["operador_caixa_label"] = _cash_operator_label(row.get("operador_caixa_nome"), row.get("operador_caixa_id"))
        row["reasons_humanized"] = _humanize_risk_reasons(row.get("reasons"), row.get("event_type"))
        row["reason_summary"] = " ".join(row["reasons_humanized"])
        if str(row.get("event_type") or "").strip().upper() in CASH_CANCEL_EVENT_TYPES and row.get("operador_caixa_id") is not None:
            row["responsavel_label"] = row["operador_caixa_label"]
            row["responsavel_kind"] = "operador_caixa"
        else:
            row["responsavel_label"] = row["funcionario_label"]
            row["responsavel_kind"] = "colaborador_venda"
    return rows


def risk_insights(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    status: Optional[str] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    where_status = "" if not status else "AND status = %s"
    params = [id_empresa, dt_ini, dt_fim] + branch_params + ([] if not status else [status]) + [limit]

    sql = f"""
      SELECT
        id,
        created_at,
        id_filial,
        insight_type,
        severity,
        dt_ref,
        impacto_estimado,
        title,
        message,
        recommendation,
        status,
        meta,
        ai_plan,
        ai_model,
        ai_prompt_tokens,
        ai_completion_tokens,
        ai_generated_at,
        ai_cache_hit,
        ai_error
      FROM app.insights_gerados
      WHERE id_empresa = %s
        AND dt_ref BETWEEN %s AND %s
        {where_filial}
        {where_status}
      ORDER BY dt_ref DESC, created_at DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def risk_by_turn_local(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("rtl.id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        rtl.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        rtl.id_turno,
        {_turno_value_sql('t.payload', 'rtl.id_turno')} AS turno_value,
        rtl.id_local_venda,
        COALESCE(MAX(lv.nome), '') AS local_nome,
        SUM(rtl.eventos)::int AS eventos,
        SUM(rtl.alto_risco)::int AS alto_risco,
        SUM(rtl.impacto_estimado)::numeric(18,2) AS impacto_estimado,
        AVG(rtl.score_medio)::numeric(10,2) AS score_medio
      FROM mart.risco_turno_local_diaria rtl
      LEFT JOIN auth.filiais f
        ON f.id_empresa = rtl.id_empresa
       AND f.id_filial = rtl.id_filial
      LEFT JOIN dw.dim_local_venda lv
        ON lv.id_empresa = rtl.id_empresa
       AND lv.id_filial = rtl.id_filial
       AND lv.id_local_venda = rtl.id_local_venda
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = rtl.id_empresa
       AND t.id_filial = rtl.id_filial
       AND t.id_turno = rtl.id_turno
      WHERE rtl.id_empresa = %s
        AND rtl.data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY rtl.id_filial, f.nome, rtl.id_turno, t.payload, rtl.id_local_venda
      ORDER BY impacto_estimado DESC, score_medio DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["local_label"] = _local_venda_label(row.get("id_local_venda"), row.get("local_nome"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    return rows


def operational_score(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params_sales = [id_empresa, ini, fim] + branch_params
    params_risk = [id_empresa, ini, fim] + branch_params

    sql_sales = f"""
      SELECT
        COALESCE(SUM(faturamento),0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(margem),0)::numeric(18,2) AS margem,
        COALESCE(AVG(ticket_medio),0)::numeric(18,2) AS ticket_medio
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    sql_risk = f"""
      SELECT
        COALESCE(SUM(eventos_alto_risco),0)::int AS eventos_alto_risco,
        COALESCE(SUM(eventos_risco_total),0)::int AS eventos_risco_total,
        COALESCE(SUM(impacto_estimado_total),0)::numeric(18,2) AS impacto_estimado_total
      FROM mart.agg_risco_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        sales = conn.execute(sql_sales, params_sales).fetchone() or {}
        risk = conn.execute(sql_risk, params_risk).fetchone() or {}

    faturamento = float(sales.get("faturamento", 0) or 0)
    margem = float(sales.get("margem", 0) or 0)
    ticket = float(sales.get("ticket_medio", 0) or 0)
    eventos_alto = int(risk.get("eventos_alto_risco", 0) or 0)
    eventos_total = int(risk.get("eventos_risco_total", 0) or 0)
    impacto = float(risk.get("impacto_estimado_total", 0) or 0)

    margem_ratio = (margem / faturamento) if faturamento > 0 else 0.0
    margem_score = min(100.0, max(0.0, (margem_ratio / 0.15) * 100))
    risk_density = (eventos_alto / eventos_total) if eventos_total > 0 else 0.0
    risk_score = max(0.0, 100.0 - min(100.0, risk_density * 120.0 + (impacto / max(faturamento, 1.0)) * 100.0))
    ticket_score = min(100.0, max(0.0, (ticket / 120.0) * 100.0))

    score = round((margem_score * 0.45) + (risk_score * 0.40) + (ticket_score * 0.15), 2)

    return {
        "score": max(0, min(100, score)),
        "components": {
            "margem_score": round(margem_score, 2),
            "risk_score": round(risk_score, 2),
            "ticket_score": round(ticket_score, 2),
        },
    }


# ========================
# Clientes
# ========================

def customers_top(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    """Top customers by valid outbound sales for the selected period."""

    where_mart_filial, mart_branch_params = _branch_scope_clause("s.id_filial", id_filial)
    mart_params = [id_empresa, id_empresa, dt_ini, dt_fim] + mart_branch_params + [id_empresa, limit]
    mart_sql = f"""
      WITH names AS (
        SELECT DISTINCT ON (d.id_empresa, d.id_cliente)
          d.id_empresa,
          d.id_cliente,
          d.nome
        FROM dw.dim_cliente d
        WHERE d.id_empresa = %s
        ORDER BY d.id_empresa, d.id_cliente, d.updated_at DESC, d.id_filial
      ), ranked AS (
        SELECT
          s.id_cliente,
          COALESCE(SUM(s.valor_dia),0)::numeric(18,2) AS faturamento,
          COALESCE(SUM(s.compras_dia),0)::int AS compras,
          MAX(s.dt_ref) AS ultima_compra
        FROM mart.customer_sales_daily s
        WHERE s.id_empresa = %s
          AND s.id_cliente <> -1
          AND s.dt_ref BETWEEN %s::date AND %s::date
          {where_mart_filial}
        GROUP BY s.id_cliente
      )
      SELECT
        r.id_cliente,
        COALESCE(NULLIF(n.nome, ''), '#ID ' || r.id_cliente::text) AS cliente_nome,
        r.faturamento,
        r.compras,
        r.ultima_compra,
        CASE
          WHEN r.compras = 0 THEN 0::numeric(18,2)
          ELSE (r.faturamento / r.compras)::numeric(18,2)
        END AS ticket_medio
      FROM ranked r
      LEFT JOIN names n
        ON n.id_empresa = %s
       AND n.id_cliente = r.id_cliente
      ORDER BY r.faturamento DESC, r.compras DESC, r.id_cliente
      LIMIT %s
    """

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_dw_filial, dw_branch_params = _branch_scope_clause("v.id_filial", id_filial)
    dw_params = [id_empresa, ini, fim] + dw_branch_params + [limit]
    dw_sql = f"""
      SELECT
        v.id_cliente,
        COALESCE(NULLIF(dc.nome, ''), '#ID ' || v.id_cliente::text) AS cliente_nome,
        COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
        COALESCE(COUNT(DISTINCT v.id_comprovante),0)::int AS compras,
        MAX(v.data)::date AS ultima_compra,
        CASE
          WHEN COUNT(DISTINCT v.id_comprovante) = 0 THEN 0::numeric(18,2)
          ELSE (SUM(i.total) / COUNT(DISTINCT v.id_comprovante))::numeric(18,2)
        END AS ticket_medio
      FROM dw.fact_venda v
      JOIN dw.fact_venda_item i
        ON i.id_empresa = v.id_empresa
       AND i.id_filial = v.id_filial
       AND i.id_db = v.id_db
       AND i.id_comprovante = v.id_comprovante
      LEFT JOIN LATERAL (
        SELECT d.nome
        FROM dw.dim_cliente d
        WHERE d.id_empresa = v.id_empresa
          AND d.id_cliente = v.id_cliente
        ORDER BY
          CASE WHEN d.id_filial = v.id_filial THEN 0 ELSE 1 END,
          d.updated_at DESC,
          d.id_filial
        LIMIT 1
      ) dc ON true
      WHERE v.id_empresa = %s
        AND v.id_cliente IS NOT NULL
        AND v.id_cliente <> -1
        AND v.data_key BETWEEN %s AND %s
        AND COALESCE(v.cancelado, false) = false
        AND COALESCE(i.cfop, 0) >= 5000
        {where_dw_filial}
      GROUP BY v.id_cliente, dc.nome
      ORDER BY faturamento DESC, compras DESC, v.id_cliente
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        mart_rows = list(conn.execute(mart_sql, mart_params).fetchall())
        if mart_rows:
            return mart_rows
        return list(conn.execute(dw_sql, dw_params).fetchall())


def customers_rfm_snapshot(role: str, id_empresa: int, id_filial: Optional[int], as_of: date) -> Dict[str, Any]:
    """Very lightweight RFM-like snapshot for *today* (rule-based, no ML yet)."""

    # Last 90 days window
    dt_ini = as_of - timedelta(days=90)
    ini = _date_key(dt_ini)
    fim = _date_key(as_of)

    where_filial, branch_params = _branch_scope_clause("v.id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql = f"""
      WITH base AS (
        SELECT
          COALESCE(v.id_cliente, -1) AS id_cliente,
          MAX(v.data)::date AS last_purchase,
          COUNT(DISTINCT v.id_comprovante)::int AS freq,
          SUM(v.total_venda)::numeric(18,2) AS monetary
        FROM dw.fact_venda v
        WHERE v.id_empresa = %s
          AND v.data_key BETWEEN %s AND %s
          AND COALESCE(v.cancelado,false) = false
          {where_filial}
        GROUP BY COALESCE(v.id_cliente, -1)
      )
      SELECT
        COUNT(*) FILTER (WHERE id_cliente <> -1)::int AS clientes_identificados,
        COUNT(*) FILTER (WHERE last_purchase >= (%s::date - interval '7 days'))::int AS ativos_7d,
        COUNT(*) FILTER (WHERE last_purchase < (%s::date - interval '30 days'))::int AS em_risco_30d,
        COALESCE(SUM(monetary),0)::numeric(18,2) AS faturamento_90d
      FROM base
    """

    params2 = params + [as_of, as_of]
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params2).fetchone()
        return row or {
            "clientes_identificados": 0,
            "ativos_7d": 0,
            "em_risco_30d": 0,
            "faturamento_90d": 0,
        }


def customers_churn_risk(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    min_score: int = 60,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, min_score] + branch_params + [limit]

    sql = f"""
      SELECT
        id_cliente,
        COALESCE(NULLIF(cliente_nome, ''), '#ID ' || id_cliente::text) AS cliente_nome,
        churn_score,
        last_purchase,
        compras_30d,
        compras_60_30,
        faturamento_30d,
        faturamento_60_30,
        reasons
      FROM mart.clientes_churn_risco
      WHERE id_empresa = %s
        AND id_cliente <> -1
        AND churn_score >= %s
        {where_filial}
      ORDER BY churn_score DESC, faturamento_60_30 DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def _customers_churn_operational_current(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date],
    min_score: int,
    limit: int,
    id_cliente: Optional[int] = None,
) -> List[Dict[str, Any]]:
    effective_as_of = as_of or business_today(id_empresa)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    where_customer = "" if id_cliente is None else "AND id_cliente = %s"
    params = (
        [effective_as_of, effective_as_of, id_empresa, min_score]
        + branch_params
        + ([] if id_cliente is None else [id_cliente])
        + [limit]
    )
    sql = f"""
      SELECT
        COALESCE((reasons->>'ref_date')::date, %s::date) AS dt_ref,
        id_cliente,
        COALESCE(NULLIF(cliente_nome, ''), '#ID ' || id_cliente::text) AS cliente_nome,
        last_purchase,
        GREATEST(0, COALESCE((reasons->>'ref_date')::date, %s::date) - last_purchase)::int AS recency_days,
        30::numeric(10,2) AS expected_cycle_days,
        compras_30d AS frequency_30,
        (compras_30d + compras_60_30)::int AS frequency_90,
        faturamento_30d::numeric(18,2) AS monetary_30,
        (faturamento_30d + faturamento_60_30)::numeric(18,2) AS monetary_90,
        CASE
          WHEN compras_30d > 0 THEN (faturamento_30d / compras_30d)::numeric(18,2)
          ELSE 0::numeric(18,2)
        END AS ticket_30,
        churn_score,
        GREATEST(faturamento_60_30, 0)::numeric(18,2) AS revenue_at_risk_30d,
        'Leitura operacional corrente do churn; snapshot diário exato indisponível para a data solicitada.' AS recommendation,
        reasons,
        updated_at
      FROM mart.clientes_churn_risco
      WHERE id_empresa = %s
        AND id_cliente <> -1
        AND churn_score >= %s
        {where_filial}
        {where_customer}
      ORDER BY churn_score DESC, faturamento_60_30 DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def customers_churn_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
    min_score: int = 60,
    limit: int = 20,
) -> Dict[str, Any]:
    snapshot_meta = _snapshot_meta(role, "customer_churn_risk_daily", id_empresa, id_filial, as_of, "latest_leq_ref")
    rows: List[Dict[str, Any]] = []

    effective_dt_ref = snapshot_meta.get("effective_dt_ref")
    if effective_dt_ref:
        where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
        params = [id_empresa, min_score] + branch_params + [effective_dt_ref, limit]
        sql = f"""
          SELECT
            dt_ref,
            id_cliente,
            COALESCE(NULLIF(cliente_nome,''), '#ID ' || id_cliente::text) AS cliente_nome,
            last_purchase,
            recency_days,
            expected_cycle_days,
            frequency_30,
            frequency_90,
            monetary_30,
            monetary_90,
            ticket_30,
            churn_score,
            revenue_at_risk_30d,
            recommendation,
            reasons,
            updated_at
          FROM mart.customer_churn_risk_daily
          WHERE id_empresa = %s
            AND churn_score >= %s
            AND id_cliente <> -1
            {where_filial}
            AND dt_ref = %s
          ORDER BY churn_score DESC, revenue_at_risk_30d DESC
          LIMIT %s
        """
        with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]

    if not rows:
        rows = _customers_churn_operational_current(role, id_empresa, id_filial, as_of=as_of, min_score=min_score, limit=limit)
        if rows:
            snapshot_meta = {
                **snapshot_meta,
                "snapshot_status": "operational_current",
                "precision_mode": "operational_current",
                "effective_dt_ref": rows[0].get("dt_ref"),
                "source_table": "mart.clientes_churn_risco",
                "source_kind": "operational_current",
                "latest_updated_at": max((row.get("updated_at") for row in rows), default=None),
                "row_count": len(rows),
            }

    total_revenue_at_risk = float(sum(float(row.get("revenue_at_risk_30d") or 0) for row in rows))
    avg_churn_score = round(sum(float(row.get("churn_score") or 0) for row in rows) / len(rows), 2) if rows else 0.0

    return {
        "top_risk": rows,
        "summary": {
            "total_top_risk": len(rows),
            "avg_churn_score": avg_churn_score,
            "revenue_at_risk_30d": round(total_revenue_at_risk, 2),
        },
        "snapshot_meta": snapshot_meta,
    }


def customers_churn_diamond(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
    min_score: int = 60,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    return customers_churn_bundle(
        role,
        id_empresa,
        id_filial,
        as_of=as_of,
        min_score=min_score,
        limit=limit,
    )["top_risk"]


def customers_churn_snapshot_meta(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date],
) -> Dict[str, Any]:
    snapshot_meta = _snapshot_meta(role, "customer_churn_risk_daily", id_empresa, id_filial, as_of, "latest_leq_ref")
    if snapshot_meta.get("snapshot_status") != "missing":
        return snapshot_meta

    fallback_rows = _customers_churn_operational_current(role, id_empresa, id_filial, as_of=as_of, min_score=0, limit=1)
    if not fallback_rows:
        return snapshot_meta

    return {
        **snapshot_meta,
        "snapshot_status": "operational_current",
        "precision_mode": "operational_current",
        "effective_dt_ref": fallback_rows[0].get("dt_ref"),
        "source_table": "mart.clientes_churn_risco",
        "source_kind": "operational_current",
        "latest_updated_at": fallback_rows[0].get("updated_at"),
        "row_count": int(snapshot_meta.get("row_count") or 0),
    }


def customer_churn_drilldown(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    id_cliente: int,
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("v.id_filial", id_filial)
    params = [id_empresa, id_cliente, ini, fim] + branch_params

    sql_series = f"""
      SELECT
        v.data_key,
        COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
        COUNT(DISTINCT v.id_comprovante)::int AS compras
      FROM dw.fact_venda v
      JOIN dw.fact_venda_item i
        ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_comprovante=v.id_comprovante
      WHERE v.id_empresa = %s
        AND v.id_cliente = %s
        AND v.data_key BETWEEN %s AND %s
        AND COALESCE(v.cancelado,false) = false
        AND COALESCE(i.cfop,0) >= 5000
        {where_filial}
      GROUP BY v.data_key
      ORDER BY v.data_key
    """

    snapshot_meta = customers_churn_snapshot_meta(role, id_empresa, id_filial, as_of)
    snapshot: Dict[str, Any] = {}
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        series = list(conn.execute(sql_series, params).fetchall())

        if snapshot_meta.get("snapshot_status") in {"exact", "best_effort"} and snapshot_meta.get("effective_dt_ref"):
            where_snapshot_filial, snapshot_branch_params = _branch_scope_clause("id_filial", id_filial)
            sql_snapshot = f"""
              SELECT
                dt_ref,
                id_cliente,
                COALESCE(NULLIF(cliente_nome,''), '#ID ' || id_cliente::text) AS cliente_nome,
                recency_days,
                expected_cycle_days,
                frequency_30,
                frequency_90,
                monetary_30,
                monetary_90,
                ticket_30,
                churn_score,
                revenue_at_risk_30d,
                recommendation,
                reasons
              FROM mart.customer_churn_risk_daily
              WHERE id_empresa = %s
                AND id_cliente = %s
                {where_snapshot_filial}
                AND dt_ref = %s
              ORDER BY dt_ref DESC
              LIMIT 1
            """
            params_snapshot = [id_empresa, id_cliente] + snapshot_branch_params + [snapshot_meta["effective_dt_ref"]]
            snap = conn.execute(sql_snapshot, params_snapshot).fetchone()
            snapshot = dict(snap) if snap else {}
        elif snapshot_meta.get("snapshot_status") == "operational_current":
            fallback_rows = _customers_churn_operational_current(
                role,
                id_empresa,
                id_filial,
                as_of=as_of,
                min_score=0,
                limit=1,
                id_cliente=id_cliente,
            )
            snapshot = fallback_rows[0] if fallback_rows else {}
    return {
        "snapshot": snapshot,
        "series": series,
        "snapshot_meta": snapshot_meta,
    }


def anonymous_retention_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql_series = f"""
      SELECT
        to_char(dt_ref, 'YYYYMMDD')::int AS data_key,
        id_filial,
        anon_faturamento_7d,
        anon_faturamento_prev_28d,
        trend_pct,
        anon_share_pct_7d,
        repeat_proxy_idx,
        impact_estimated_7d
      FROM mart.anonymous_retention_daily
      WHERE id_empresa = %s
        AND to_char(dt_ref, 'YYYYMMDD')::int BETWEEN %s AND %s
        {where_filial}
      ORDER BY dt_ref, id_filial
    """

    sql_latest = f"""
      SELECT
        dt_ref,
        id_filial,
        anon_faturamento_7d,
        anon_faturamento_prev_28d,
        trend_pct,
        anon_share_pct_7d,
        repeat_proxy_idx,
        impact_estimated_7d,
        details
      FROM mart.anonymous_retention_daily
      WHERE id_empresa = %s
        AND dt_ref = (
          SELECT MAX(dt_ref)
          FROM mart.anonymous_retention_daily
          WHERE id_empresa = %s
            AND dt_ref <= %s
          {where_filial}
        )
        {where_filial}
      ORDER BY id_filial
    """
    params_latest = [id_empresa, id_empresa, dt_fim] + branch_params + branch_params

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        latest_rows = list(conn.execute(sql_latest, params_latest).fetchall())
        series = list(conn.execute(sql_series, params).fetchall())

    agg_impact = sum(float(r.get("impact_estimated_7d") or 0) for r in latest_rows)
    avg_trend = (sum(float(r.get("trend_pct") or 0) for r in latest_rows) / len(latest_rows)) if latest_rows else 0.0
    avg_repeat = (sum(float(r.get("repeat_proxy_idx") or 0) for r in latest_rows) / len(latest_rows)) if latest_rows else 0.0

    recommendation = (
        "Recorrência anônima caiu. Ajuste a operação por horário/dia, reveja o mix de produtos e acione promoções de retorno."
        if avg_trend < -8
        else "Recorrência anônima estável. Monitore horários de maior queda e mantenha ações de fidelização."
    )

    return {
        "kpis": {
            "impact_estimated_7d": round(agg_impact, 2),
            "trend_pct": round(avg_trend, 2),
            "repeat_proxy_idx": round(avg_repeat, 2),
            "severity": "CRITICAL" if avg_trend <= -15 else ("WARN" if avg_trend <= -8 else "OK"),
            "recommendation": recommendation,
        },
        "latest": latest_rows,
        "series": series,
        "breakdown_dow": [],
        "breakdown_hour": [],
        "mix": [],
    }


def customers_delinquency_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: date,
    *,
    limit: int = 20,
) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("f.id_filial", id_filial)
    params = [as_of, id_empresa] + branch_params + [as_of, as_of, id_empresa, limit]
    sql = f"""
      WITH base AS (
        SELECT
          f.id_filial,
          COALESCE(f.id_entidade, -1) AS id_cliente,
          GREATEST(0::numeric, COALESCE(f.valor, 0) - COALESCE(f.valor_pago, 0))::numeric(18,2) AS valor_aberto,
          GREATEST(0, %s::date - COALESCE(f.vencimento, f.data_emissao))::int AS dias_atraso
        FROM dw.fact_financeiro f
        WHERE f.id_empresa = %s
          AND f.tipo_titulo = 1
          {where_filial}
          AND COALESCE(f.vencimento, f.data_emissao) < %s
          AND (
            f.data_pagamento IS NULL
            OR f.data_pagamento > %s
            OR (COALESCE(f.valor, 0) - COALESCE(f.valor_pago, 0)) > 0
          )
      ), open_rows AS (
        SELECT *
        FROM base
        WHERE valor_aberto > 0
          AND id_cliente <> -1
      ), per_branch_customer AS (
        SELECT
          o.id_filial,
          o.id_cliente,
          COUNT(*)::int AS titulos,
          MAX(o.dias_atraso)::int AS max_dias_atraso,
          COALESCE(SUM(o.valor_aberto), 0)::numeric(18,2) AS valor_aberto,
          COUNT(*) FILTER (WHERE o.dias_atraso BETWEEN 1 AND 30)::int AS titulos_30,
          COUNT(*) FILTER (WHERE o.dias_atraso BETWEEN 31 AND 60)::int AS titulos_60,
          COUNT(*) FILTER (WHERE o.dias_atraso > 60)::int AS titulos_90_plus,
          COALESCE(SUM(o.valor_aberto) FILTER (WHERE o.dias_atraso BETWEEN 1 AND 30), 0)::numeric(18,2) AS valor_30,
          COALESCE(SUM(o.valor_aberto) FILTER (WHERE o.dias_atraso BETWEEN 31 AND 60), 0)::numeric(18,2) AS valor_60,
          COALESCE(SUM(o.valor_aberto) FILTER (WHERE o.dias_atraso > 60), 0)::numeric(18,2) AS valor_90_plus
        FROM open_rows o
        GROUP BY o.id_filial, o.id_cliente
      ), named AS (
        SELECT
          p.id_cliente,
          COALESCE(NULLIF(d.nome, ''), '#ID ' || p.id_cliente::text) AS cliente_nome,
          p.titulos,
          p.max_dias_atraso,
          p.valor_aberto,
          p.titulos_30,
          p.titulos_60,
          p.titulos_90_plus,
          p.valor_30,
          p.valor_60,
          p.valor_90_plus
        FROM per_branch_customer p
        LEFT JOIN LATERAL (
          SELECT d.nome
          FROM dw.dim_cliente d
          WHERE d.id_empresa = %s
            AND d.id_cliente = p.id_cliente
          ORDER BY
            CASE WHEN d.id_filial = p.id_filial THEN 0 ELSE 1 END,
            d.updated_at DESC,
            d.id_filial
          LIMIT 1
        ) d ON true
      ), ranked AS (
        SELECT
          n.id_cliente,
          MAX(n.cliente_nome) AS cliente_nome,
          COALESCE(SUM(n.titulos), 0)::int AS titulos,
          MAX(n.max_dias_atraso)::int AS max_dias_atraso,
          COALESCE(SUM(n.valor_aberto), 0)::numeric(18,2) AS valor_aberto,
          COALESCE(SUM(n.titulos_30), 0)::int AS titulos_30,
          COALESCE(SUM(n.titulos_60), 0)::int AS titulos_60,
          COALESCE(SUM(n.titulos_90_plus), 0)::int AS titulos_90_plus,
          COALESCE(SUM(n.valor_30), 0)::numeric(18,2) AS valor_30,
          COALESCE(SUM(n.valor_60), 0)::numeric(18,2) AS valor_60,
          COALESCE(SUM(n.valor_90_plus), 0)::numeric(18,2) AS valor_90_plus
        FROM named n
        GROUP BY n.id_cliente
      ), totals AS (
        SELECT
          COUNT(DISTINCT id_cliente)::int AS clientes_em_aberto,
          COUNT(*)::int AS titulos_em_aberto,
          COALESCE(SUM(valor_aberto), 0)::numeric(18,2) AS valor_total,
          COUNT(*) FILTER (WHERE dias_atraso BETWEEN 1 AND 30)::int AS titulos_30,
          COUNT(*) FILTER (WHERE dias_atraso BETWEEN 31 AND 60)::int AS titulos_60,
          COUNT(*) FILTER (WHERE dias_atraso > 60)::int AS titulos_90_plus,
          COALESCE(SUM(valor_aberto) FILTER (WHERE dias_atraso BETWEEN 1 AND 30), 0)::numeric(18,2) AS valor_30,
          COALESCE(SUM(valor_aberto) FILTER (WHERE dias_atraso BETWEEN 31 AND 60), 0)::numeric(18,2) AS valor_60,
          COALESCE(SUM(valor_aberto) FILTER (WHERE dias_atraso > 60), 0)::numeric(18,2) AS valor_90_plus,
          COALESCE(MAX(dias_atraso), 0)::int AS max_dias_atraso
        FROM open_rows
      )
      SELECT
        jsonb_build_object(
          'clientes_em_aberto', t.clientes_em_aberto,
          'titulos_em_aberto', t.titulos_em_aberto,
          'valor_total', t.valor_total,
          'titulos_30', t.titulos_30,
          'titulos_60', t.titulos_60,
          'titulos_90_plus', t.titulos_90_plus,
          'valor_30', t.valor_30,
          'valor_60', t.valor_60,
          'valor_90_plus', t.valor_90_plus,
          'max_dias_atraso', t.max_dias_atraso
        ) AS summary,
        jsonb_build_array(
          jsonb_build_object('bucket', '1_30', 'label', '1-30 dias', 'valor', t.valor_30, 'titulos', t.titulos_30),
          jsonb_build_object('bucket', '31_60', 'label', '31-60 dias', 'valor', t.valor_60, 'titulos', t.titulos_60),
          jsonb_build_object('bucket', '61_plus', 'label', '61+ dias', 'valor', t.valor_90_plus, 'titulos', t.titulos_90_plus)
        ) AS buckets,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'id_cliente', r.id_cliente,
                'cliente_nome', r.cliente_nome,
                'titulos', r.titulos,
                'max_dias_atraso', r.max_dias_atraso,
                'valor_aberto', r.valor_aberto,
                'titulos_30', r.titulos_30,
                'titulos_60', r.titulos_60,
                'titulos_90_plus', r.titulos_90_plus,
                'valor_30', r.valor_30,
                'valor_60', r.valor_60,
                'valor_90_plus', r.valor_90_plus,
                'titulos_totais', r.titulos,
                'valor_total', r.valor_aberto,
                'bucket_label',
                  CASE
                    WHEN r.valor_90_plus > 0 THEN '61+ dias'
                    WHEN r.valor_60 > 0 THEN '31-60 dias'
                    ELSE '1-30 dias'
                  END
              )
              ORDER BY r.valor_aberto DESC, r.max_dias_atraso DESC, r.id_cliente
            )
            FROM (
              SELECT *
              FROM ranked
              ORDER BY valor_aberto DESC, max_dias_atraso DESC, id_cliente
              LIMIT %s
            ) r
          ),
          '[]'::jsonb
        ) AS customers
      FROM totals t
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone() or {}

    summary = dict(row.get("summary") or {})
    buckets = list(row.get("buckets") or [])
    customers = list(row.get("customers") or [])
    return {
        "summary": {
            "clientes_em_aberto": int(summary.get("clientes_em_aberto") or 0),
            "titulos_em_aberto": int(summary.get("titulos_em_aberto") or 0),
            "valor_total": round(float(summary.get("valor_total") or 0), 2),
            "titulos_30": int(summary.get("titulos_30") or 0),
            "titulos_60": int(summary.get("titulos_60") or 0),
            "titulos_90_plus": int(summary.get("titulos_90_plus") or 0),
            "valor_30": round(float(summary.get("valor_30") or 0), 2),
            "valor_60": round(float(summary.get("valor_60") or 0), 2),
            "valor_90_plus": round(float(summary.get("valor_90_plus") or 0), 2),
            "max_dias_atraso": int(summary.get("max_dias_atraso") or 0),
        },
        "buckets": [
            {
                "bucket": item.get("bucket"),
                "label": item.get("label"),
                "valor": round(float(item.get("valor") or 0), 2),
                "titulos": int(item.get("titulos") or 0),
            }
            for item in buckets
        ],
        "customers": [
            {
                "id_cliente": int(item.get("id_cliente") or 0),
                "cliente_nome": item.get("cliente_nome"),
                "titulos": int(item.get("titulos") or 0),
                "max_dias_atraso": int(item.get("max_dias_atraso") or 0),
                "valor_aberto": round(float(item.get("valor_aberto") or 0), 2),
                "titulos_30": int(item.get("titulos_30") or 0),
                "titulos_60": int(item.get("titulos_60") or 0),
                "titulos_90_plus": int(item.get("titulos_90_plus") or 0),
                "valor_30": round(float(item.get("valor_30") or 0), 2),
                "valor_60": round(float(item.get("valor_60") or 0), 2),
                "valor_90_plus": round(float(item.get("valor_90_plus") or 0), 2),
                "titulos_totais": int(item.get("titulos_totais") or item.get("titulos") or 0),
                "valor_total": round(float(item.get("valor_total") or item.get("valor_aberto") or 0), 2),
                "bucket_label": item.get("bucket_label"),
            }
            for item in customers
        ],
        "dt_ref": as_of.isoformat(),
    }


def stock_position_summary(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
) -> Dict[str, Any]:
    fuel_filter = _fuel_filter_expression("g", "p")
    local_name = _normalized_text_expression("lv.nome")
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        if relation_exists(conn, "mart", "agg_estoque_posicao_atual"):
            row = conn.execute(
                f"""
                  SELECT
                    COALESCE(SUM(rows), 0)::int AS rows,
                    MAX(updated_at) AS last_sync_at,
                    MAX(dt_ref) AS dt_ref,
                    COALESCE(SUM(qtd_total) FILTER (WHERE estoque_bucket = 'tanques'), 0)::numeric(18,3) AS qtd_tanques,
                    COALESCE(SUM(valor_estimado) FILTER (WHERE estoque_bucket = 'tanques'), 0)::numeric(18,2) AS valor_tanques,
                    COALESCE(SUM(qtd_total) FILTER (WHERE estoque_bucket = 'loja'), 0)::numeric(18,3) AS qtd_loja,
                    COALESCE(SUM(valor_estimado) FILTER (WHERE estoque_bucket = 'loja'), 0)::numeric(18,2) AS valor_loja
                  FROM mart.agg_estoque_posicao_atual
                  WHERE id_empresa = %s
                    {where_filial}
                """,
                [id_empresa] + branch_params,
            ).fetchone() or {}
        elif relation_exists(conn, "dw", "fact_estoque_atual"):
            where_dw_filial, dw_branch_params = _branch_scope_clause("e.id_filial", id_filial)
            sql = f"""
              WITH enriched AS (
                SELECT
                  e.id_filial,
                  e.id_produto,
                  e.id_local_venda,
                  COALESCE(e.qtd_atual, 0)::numeric(18,3) AS qtd_atual,
                  COALESCE(p.custo_medio, 0)::numeric(18,6) AS custo_unitario,
                  (COALESCE(e.qtd_atual, 0) * COALESCE(p.custo_medio, 0))::numeric(18,2) AS valor_estimado,
                  CASE
                    WHEN ({fuel_filter})
                      OR {local_name} LIKE '%%PISTA%%'
                      OR {local_name} LIKE '%%TANQUE%%'
                      OR {local_name} LIKE '%%BICO%%'
                    THEN 'tanques'
                    ELSE 'loja'
                  END AS estoque_bucket,
                  e.data_ref,
                  e.updated_at
                FROM dw.fact_estoque_atual e
                LEFT JOIN dw.dim_produto p
                  ON p.id_empresa = e.id_empresa
                 AND p.id_filial = e.id_filial
                 AND p.id_produto = e.id_produto
                LEFT JOIN dw.dim_grupo_produto g
                  ON g.id_empresa = p.id_empresa
                 AND g.id_filial = p.id_filial
                 AND g.id_grupo_produto = p.id_grupo_produto
                LEFT JOIN dw.dim_local_venda lv
                  ON lv.id_empresa = e.id_empresa
                 AND lv.id_filial = e.id_filial
                 AND lv.id_local_venda = e.id_local_venda
                WHERE e.id_empresa = %s
                  {where_dw_filial}
              )
              SELECT
                COUNT(*)::int AS rows,
                MAX(updated_at) AS last_sync_at,
                MAX(data_ref) AS dt_ref,
                COALESCE(SUM(qtd_atual) FILTER (WHERE estoque_bucket = 'tanques'), 0)::numeric(18,3) AS qtd_tanques,
                COALESCE(SUM(valor_estimado) FILTER (WHERE estoque_bucket = 'tanques'), 0)::numeric(18,2) AS valor_tanques,
                COALESCE(SUM(qtd_atual) FILTER (WHERE estoque_bucket = 'loja'), 0)::numeric(18,3) AS qtd_loja,
                COALESCE(SUM(valor_estimado) FILTER (WHERE estoque_bucket = 'loja'), 0)::numeric(18,2) AS valor_loja
              FROM enriched
            """
            row = conn.execute(sql, [id_empresa] + dw_branch_params).fetchone() or {}
        else:
            return {
                "source_status": "unavailable",
                "summary": "A trilha de estoque ainda não foi publicada no DW desta base.",
                "cards": [],
                "dt_ref": None,
                "last_sync_at": None,
                "rows": 0,
            }

    rows = int(row.get("rows") or 0)
    dt_ref = row.get("dt_ref")
    last_sync_at = row.get("last_sync_at")
    if rows <= 0:
        return {
            "source_status": "unavailable",
            "summary": "Nenhum snapshot de estoque foi ingerido na trilha canônica desta empresa.",
            "cards": [
                {
                    "key": "estoque_tanques",
                    "label": "Estoque de tanques",
                    "status": "unavailable",
                    "amount": None,
                    "quantity": None,
                    "detail": "Sem posição canônica de estoque publicada para combustíveis e tanques.",
                },
                {
                    "key": "estoque_loja",
                    "label": "Estoque de loja",
                    "status": "unavailable",
                    "amount": None,
                    "quantity": None,
                    "detail": "Sem posição canônica de estoque publicada para a loja e itens de conveniência.",
                },
            ],
            "dt_ref": None,
            "last_sync_at": None,
            "rows": 0,
        }

    return {
        "source_status": "ok",
        "summary": (
            f"Posição de estoque canônica com {rows} item(ns), atualizada até "
            f"{dt_ref.isoformat() if hasattr(dt_ref, 'isoformat') else dt_ref}."
        ),
        "cards": [
            {
                "key": "estoque_tanques",
                "label": "Estoque de tanques",
                "status": "ready",
                "amount": round(float(row.get("valor_tanques") or 0), 2),
                "quantity": round(float(row.get("qtd_tanques") or 0), 3),
                "detail": "Valor estimado pela posição atual multiplicada pelo custo médio dos produtos de combustível.",
            },
            {
                "key": "estoque_loja",
                "label": "Estoque de loja",
                "status": "ready",
                "amount": round(float(row.get("valor_loja") or 0), 2),
                "quantity": round(float(row.get("qtd_loja") or 0), 3),
                "detail": "Valor estimado da posição de conveniência e demais itens fora do bucket de tanques.",
            },
        ],
        "dt_ref": _iso_or_none(dt_ref),
        "last_sync_at": _iso_or_none(last_sync_at),
        "rows": rows,
    }


def cash_dre_summary(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: date,
) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("f.id_filial", id_filial)
    params = [id_empresa] + branch_params + [as_of, as_of, as_of]
    sql = f"""
      WITH open_titles AS (
        SELECT
          f.tipo_titulo,
          COALESCE(f.vencimento, f.data_emissao) AS vencimento,
          GREATEST(0::numeric, COALESCE(f.valor, 0) - COALESCE(f.valor_pago, 0))::numeric(18,2) AS valor_aberto
        FROM dw.fact_financeiro f
        WHERE f.id_empresa = %s
          {where_filial}
          AND (
            f.data_pagamento IS NULL
            OR f.data_pagamento > %s
            OR (COALESCE(f.valor, 0) - COALESCE(f.valor_pago, 0)) > 0
          )
      )
      SELECT
        COALESCE(SUM(valor_aberto) FILTER (WHERE tipo_titulo = 0 AND vencimento > %s), 0)::numeric(18,2) AS pagar_futuro,
        COUNT(*) FILTER (WHERE tipo_titulo = 0 AND vencimento > %s)::int AS pagar_futuro_titulos,
        COALESCE(SUM(valor_aberto) FILTER (WHERE tipo_titulo = 1), 0)::numeric(18,2) AS receber_aberto,
        COUNT(*) FILTER (WHERE tipo_titulo = 1)::int AS receber_aberto_titulos
      FROM open_titles
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone() or {}

    pagar_futuro = round(float(row.get("pagar_futuro") or 0), 2)
    receber_aberto = round(float(row.get("receber_aberto") or 0), 2)
    saldo_liquido = round(receber_aberto - pagar_futuro, 2)
    stock_summary = stock_position_summary(role, id_empresa, id_filial)

    return {
        "cards": [
            {
                "key": "contas_pagar_futuro_banco",
                "label": "Contas a pagar futuras",
                "status": "ready",
                "amount": pagar_futuro,
                "titles": int(row.get("pagar_futuro_titulos") or 0),
                "detail": "Títulos a pagar com vencimento após a data-base.",
            },
            {
                "key": "contas_receber",
                "label": "Contas a receber",
                "status": "ready",
                "amount": receber_aberto,
                "titles": int(row.get("receber_aberto_titulos") or 0),
                "detail": "Recebíveis ainda em aberto na rede.",
            },
            {
                "key": "saldo_liquido_aberto",
                "label": "Saldo líquido aberto",
                "status": "ready",
                "amount": saldo_liquido,
                "titles": None,
                "detail": "Contas a receber menos contas a pagar futuras.",
            },
        ]
        + list(stock_summary.get("cards") or []),
        "pending": [
            {
                "key": "notas_lancadas",
                "label": "Notas lançadas",
                "status": "pending",
                "detail": "Base confiável ainda não foi publicada no DW para esta visão.",
            },
            {
                "key": "pagamento_carga_antecipada",
                "label": "Pagamento de carga antecipada",
                "status": "pending",
                "detail": "Sem base confiável publicada no DW para este componente.",
            },
            {
                "key": "saldo_bancos",
                "label": "Saldo nos bancos",
                "status": "pending",
                "detail": "Sem base bancária consolidada publicada no DW.",
            },
            {
                "key": "dinheiro_posto",
                "label": "Dinheiro no posto",
                "status": "pending",
                "detail": "Sem leitura financeira operacional consolidada para caixa físico.",
            },
        ],
        "stock": stock_summary,
        "dt_ref": as_of.isoformat(),
    }


# ========================
# Financeiro
# ========================

def finance_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    """Finance KPIs by due date (vencimento) within the selected range."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)

    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    # tipo_titulo: 0 pagar, 1 receber
    sql = f"""
      SELECT
        COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_total ELSE 0 END),0)::numeric(18,2) AS receber_total,
        COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_pago  ELSE 0 END),0)::numeric(18,2) AS receber_pago,
        COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS receber_aberto,

        COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_total ELSE 0 END),0)::numeric(18,2) AS pagar_total,
        COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_pago  ELSE 0 END),0)::numeric(18,2) AS pagar_pago,
        COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS pagar_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {
            "receber_total": 0,
            "receber_pago": 0,
            "receber_aberto": 0,
            "pagar_total": 0,
            "pagar_pago": 0,
            "pagar_aberto": 0,
        }


def finance_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)

    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

    sql = f"""
      SELECT data_key, id_filial, tipo_titulo, valor_total, valor_pago, valor_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, tipo_titulo
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def _finance_aging_operational_as_of(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: date,
) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("f.id_filial", id_filial)
    params = [as_of, id_empresa] + branch_params + [
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
        as_of,
    ]
    sql = f"""
      WITH base AS (
        SELECT
          f.tipo_titulo,
          COALESCE(f.vencimento, f.data_emissao) AS vencimento,
          CASE
            WHEN f.data_pagamento IS NULL THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            WHEN f.data_pagamento > %s THEN GREATEST(0::numeric, COALESCE(f.valor,0))
            ELSE GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
          END::numeric(18,2) AS valor_aberto
        FROM dw.fact_financeiro f
        WHERE f.id_empresa = %s
          {where_filial}
          AND COALESCE(f.vencimento, f.data_emissao) IS NOT NULL
          AND COALESCE(f.vencimento, f.data_emissao) <= %s
          AND (
            f.data_pagamento IS NULL
            OR f.data_pagamento > %s
            OR (COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) > 0
          )
      ), open_titles AS (
        SELECT *
        FROM base
        WHERE valor_aberto > 0
      ), totals AS (
        SELECT
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS receber_total_aberto,
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 AND vencimento < %s THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS receber_total_vencido,
          COALESCE(SUM(CASE WHEN tipo_titulo = 0 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS pagar_total_aberto,
          COALESCE(SUM(CASE WHEN tipo_titulo = 0 AND vencimento < %s THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS pagar_total_vencido,
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 AND (%s - vencimento) BETWEEN 0 AND 7 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS bucket_0_7,
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 AND (%s - vencimento) BETWEEN 8 AND 15 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS bucket_8_15,
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 AND (%s - vencimento) BETWEEN 16 AND 30 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS bucket_16_30,
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 AND (%s - vencimento) BETWEEN 31 AND 60 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS bucket_31_60,
          COALESCE(SUM(CASE WHEN tipo_titulo = 1 AND (%s - vencimento) > 60 THEN valor_aberto ELSE 0 END),0)::numeric(18,2) AS bucket_60_plus,
          COUNT(*)::int AS open_rows
        FROM open_titles
      ), overdue_rank AS (
        SELECT
          valor_aberto,
          ROW_NUMBER() OVER (ORDER BY valor_aberto DESC) AS rn
        FROM open_titles
        WHERE tipo_titulo = 1
          AND vencimento < %s
      ), top5 AS (
        SELECT COALESCE(SUM(valor_aberto),0)::numeric(18,2) AS top5_vencido
        FROM overdue_rank
        WHERE rn <= 5
      )
      SELECT
        %s::date AS dt_ref,
        t.receber_total_aberto,
        t.receber_total_vencido,
        t.pagar_total_aberto,
        t.pagar_total_vencido,
        t.bucket_0_7,
        t.bucket_8_15,
        t.bucket_16_30,
        t.bucket_31_60,
        t.bucket_60_plus,
        CASE
          WHEN t.receber_total_vencido > 0 THEN (top5.top5_vencido / NULLIF(t.receber_total_vencido, 0) * 100)::numeric(10,2)
          ELSE 0::numeric(10,2)
        END AS top5_concentration_pct,
        (t.receber_total_aberto = 0 AND t.pagar_total_aberto = 0) AS data_gaps,
        t.open_rows AS snapshot_rows
      FROM totals t
      CROSS JOIN top5
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else {}


def finance_aging_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    requested_as_of = as_of or business_today(id_empresa)
    snapshot_meta = _snapshot_meta(role, "finance_aging_daily", id_empresa, id_filial, requested_as_of, "latest_leq_ref")
    effective_dt_ref = snapshot_meta.get("effective_dt_ref")

    if effective_dt_ref:
        where_filial, branch_params = _branch_scope_clause("f.id_filial", id_filial)
        branch_ids = _branch_ids(id_filial)
        if not branch_ids:
            sql = f"""
              SELECT
                %s::date AS dt_ref,
                COALESCE(SUM(f.receber_total_aberto),0)::numeric(18,2) AS receber_total_aberto,
                COALESCE(SUM(f.receber_total_vencido),0)::numeric(18,2) AS receber_total_vencido,
                COALESCE(SUM(f.pagar_total_aberto),0)::numeric(18,2) AS pagar_total_aberto,
                COALESCE(SUM(f.pagar_total_vencido),0)::numeric(18,2) AS pagar_total_vencido,
                COALESCE(SUM(f.bucket_0_7),0)::numeric(18,2) AS bucket_0_7,
                COALESCE(SUM(f.bucket_8_15),0)::numeric(18,2) AS bucket_8_15,
                COALESCE(SUM(f.bucket_16_30),0)::numeric(18,2) AS bucket_16_30,
                COALESCE(SUM(f.bucket_31_60),0)::numeric(18,2) AS bucket_31_60,
                COALESCE(SUM(f.bucket_60_plus),0)::numeric(18,2) AS bucket_60_plus,
                COALESCE(AVG(f.top5_concentration_pct),0)::numeric(10,2) AS top5_concentration_pct,
                COALESCE(BOOL_OR(f.data_gaps), true) AS data_gaps,
                COUNT(*)::int AS snapshot_rows
              FROM mart.finance_aging_daily f
              WHERE f.id_empresa = %s
                AND f.dt_ref = %s
            """
            params = [effective_dt_ref, id_empresa, effective_dt_ref]
        else:
            sql = f"""
              SELECT
                dt_ref,
                receber_total_aberto,
                receber_total_vencido,
                pagar_total_aberto,
                pagar_total_vencido,
                bucket_0_7,
                bucket_8_15,
                bucket_16_30,
                bucket_31_60,
                bucket_60_plus,
                top5_concentration_pct,
                data_gaps,
                1::int AS snapshot_rows
              FROM mart.finance_aging_daily f
              WHERE f.id_empresa = %s
                {where_filial}
                AND f.dt_ref = %s
              ORDER BY f.dt_ref DESC
              LIMIT 1
            """
            params = [id_empresa] + branch_params + [effective_dt_ref]

        with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            row = conn.execute(sql, params).fetchone()
            if row and int(row.get("snapshot_rows") or 0) > 0:
                payload = dict(row)
                payload.update(snapshot_meta)
                payload["dt_ref"] = effective_dt_ref
                payload["source_table"] = "mart.finance_aging_daily"
                payload["source_kind"] = "snapshot"
                return payload

    payload = _finance_aging_operational_as_of(role, id_empresa, id_filial, requested_as_of)
    if payload:
        payload.update(
            {
                **snapshot_meta,
                "snapshot_status": "operational",
                "precision_mode": "operational_as_of",
                "effective_dt_ref": requested_as_of,
                "source_table": "dw.fact_financeiro",
                "source_kind": "operational_as_of",
            }
        )
        return payload

    return {
        "dt_ref": requested_as_of,
        "receber_total_aberto": 0,
        "receber_total_vencido": 0,
        "pagar_total_aberto": 0,
        "pagar_total_vencido": 0,
        "bucket_0_7": 0,
        "bucket_8_15": 0,
        "bucket_16_30": 0,
        "bucket_31_60": 0,
        "bucket_60_plus": 0,
        "top5_concentration_pct": 0,
        "data_gaps": True,
        **snapshot_meta,
    }


def payments_overview_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    days = max((dt_fim - dt_ini).days + 1, 1)
    prev_fim = ini - 1
    prev_ini = _date_key(dt_ini - timedelta(days=days))
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)

    sql_curr = f"""
      SELECT
        COALESCE(SUM(total_valor),0)::numeric(18,2) AS total_valor,
        COALESCE(SUM(CASE WHEN category = 'NAO_IDENTIFICADO' THEN total_valor ELSE 0 END),0)::numeric(18,2) AS unknown_valor,
        COALESCE(SUM(qtd_comprovantes),0)::int AS qtd_comprovantes,
        COUNT(*)::int AS row_count,
        COUNT(*) FILTER (WHERE total_valor > 0)::int AS nonzero_rows
      FROM mart.agg_pagamentos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
    """
    sql_prev = f"""
      SELECT COALESCE(SUM(total_valor),0)::numeric(18,2) AS total_valor
      FROM mart.agg_pagamentos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
    """
    sql_mix = f"""
      SELECT
        category,
        label,
        COALESCE(SUM(total_valor),0)::numeric(18,2) AS total_valor
      FROM mart.agg_pagamentos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY category, label
      ORDER BY total_valor DESC
    """
    params_curr = [id_empresa, ini, fim] + branch_params
    params_prev = [id_empresa, prev_ini, prev_fim] + branch_params
    params_mix = [id_empresa, ini, fim] + branch_params

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        curr = conn.execute(sql_curr, params_curr).fetchone() or {}
        prev = conn.execute(sql_prev, params_prev).fetchone() or {}
        mix = list(conn.execute(sql_mix, params_mix).fetchall())

    total_curr = float(curr.get("total_valor") or 0)
    total_prev = float(prev.get("total_valor") or 0)
    unknown_val = float(curr.get("unknown_valor") or 0)
    row_count = int(curr.get("row_count") or 0)
    nonzero_rows = int(curr.get("nonzero_rows") or 0)
    unknown_share = (unknown_val / total_curr * 100.0) if total_curr > 0 else 0.0
    delta_pct = ((total_curr - total_prev) / total_prev * 100.0) if total_prev > 0 else (100.0 if total_curr > 0 else 0.0)
    mix_labeled = []
    for item in mix:
        row = dict(item)
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
        mix_labeled.append(row)

    if row_count == 0:
        source_status = "unavailable"
        summary = "Sem movimento de formas de pagamento no recorte selecionado."
    elif total_curr <= 0 and nonzero_rows == 0:
        source_status = "value_gap"
        summary = "Os registros de pagamento chegaram, mas os valores ainda precisam de validação da carga para leitura executiva."
    elif unknown_share > 0:
        source_status = "partial"
        summary = "A taxonomia oficial está aplicada, mas ainda existem pagamentos não identificados no recorte."
    else:
        source_status = "ok"
        summary = "Leitura de meios de pagamento alinhada à taxonomia oficial da Xpert."

    return {
        "total_valor": round(total_curr, 2),
        "total_valor_prev": round(total_prev, 2),
        "delta_pct": round(delta_pct, 2),
        "qtd_comprovantes": int(curr.get("qtd_comprovantes") or 0),
        "row_count": row_count,
        "nonzero_rows": nonzero_rows,
        "unknown_valor": round(unknown_val, 2),
        "unknown_share_pct": round(unknown_share, 2),
        "source_status": source_status,
        "summary": summary,
        "mix": mix_labeled,
    }


def payments_by_day(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params
    sql = f"""
      SELECT
        data_key,
        id_filial,
        category,
        label,
        total_valor,
        qtd_comprovantes,
        share_percent
      FROM mart.agg_pagamentos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      ORDER BY data_key, total_valor DESC
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
    return rows


def payments_by_turno(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    limit: int = 18,
) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("p.id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]
    sql = f"""
      SELECT
        p.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        p.id_turno,
        {_turno_value_sql('t.payload', 'p.id_turno')} AS turno_value,
        p.category,
        p.label,
        COALESCE(SUM(p.total_valor), 0)::numeric(18,2) AS total_valor,
        COALESCE(SUM(p.qtd_comprovantes), 0)::int AS qtd_comprovantes,
        COUNT(DISTINCT p.data_key)::int AS dias_com_movimento
      FROM mart.agg_pagamentos_turno p
      LEFT JOIN auth.filiais f
        ON f.id_empresa = p.id_empresa
       AND f.id_filial = p.id_filial
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = p.id_empresa
       AND t.id_filial = p.id_filial
       AND t.id_turno = p.id_turno
      WHERE p.id_empresa = %s
        AND p.data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY p.id_filial, f.nome, p.id_turno, t.payload, p.category, p.label
      ORDER BY total_valor DESC, qtd_comprovantes DESC, p.id_filial, p.id_turno
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    return rows


def payments_anomalies(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("p.id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]
    sql = f"""
      SELECT
        p.data_key,
        p.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        p.id_turno,
        {_turno_value_sql('t.payload', 'p.id_turno')} AS turno_value,
        p.event_type,
        p.severity,
        p.score,
        p.impacto_estimado,
        p.reasons,
        p.insight_id,
        p.insight_id_hash
      FROM mart.pagamentos_anomalias_diaria p
      LEFT JOIN auth.filiais f
        ON f.id_empresa = p.id_empresa
       AND f.id_filial = p.id_filial
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = p.id_empresa
       AND t.id_filial = p.id_filial
       AND t.id_turno = p.id_turno
      WHERE p.id_empresa = %s
        AND p.data_key BETWEEN %s AND %s
        {where_filial}
      ORDER BY p.score DESC, p.impacto_estimado DESC, p.data_key DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["event_label"] = _event_type_label(row.get("event_type"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    return rows


def payments_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    anomaly_limit: int = 20,
) -> Dict[str, Any]:
    kpis = payments_overview_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    by_day = payments_by_day(role, id_empresa, id_filial, dt_ini, dt_fim)
    by_turno = payments_by_turno(role, id_empresa, id_filial, dt_ini, dt_fim)
    anomalies = payments_anomalies(role, id_empresa, id_filial, dt_ini, dt_fim, limit=anomaly_limit)
    return {
        "kpis": kpis,
        "by_day": by_day,
        "by_turno": by_turno,
        "anomalies": anomalies,
    }


def _cash_live_now_live_query(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial_dw, dw_branch_params = _branch_scope_clause("t.id_filial", id_filial)
    where_filial_live, live_branch_params = _branch_scope_clause("a.id_filial", id_filial)
    where_filial_payment, payment_branch_params = _branch_scope_clause("live_turns.id_filial", id_filial)
    sql_total_turnos = f"""
      SELECT COUNT(*)::int AS total_turnos
      FROM dw.fact_caixa_turno t
      WHERE t.id_empresa = %s
      {where_filial_dw}
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        cash_from_sql, cash_schema_mode = cash_open_source_sql(
            conn,
            id_empresa=id_empresa,
            id_filial=id_filial,
            alias="a",
        )
        cash_payment_sql, _ = cash_open_source_sql(
            conn,
            id_empresa=id_empresa,
            id_filial=id_filial,
            alias="live_turns",
        )
        sql_summary = f"""
          SELECT
            COUNT(*)::int AS caixas_abertos_fonte,
            COUNT(*) FILTER (WHERE a.is_operational_live)::int AS caixas_abertos,
            COUNT(*) FILTER (WHERE a.is_stale)::int AS caixas_stale,
            COUNT(*) FILTER (WHERE a.is_operational_live AND a.severity = 'CRITICAL')::int AS caixas_criticos,
            COUNT(*) FILTER (WHERE a.is_operational_live AND a.severity = 'HIGH')::int AS caixas_alto_risco,
            COUNT(*) FILTER (WHERE a.is_operational_live AND a.severity = 'WARN')::int AS caixas_em_monitoramento,
            COALESCE(SUM(a.total_vendas) FILTER (WHERE a.is_operational_live), 0)::numeric(18,2) AS total_vendas_abertas,
            COALESCE(SUM(a.total_cancelamentos) FILTER (WHERE a.is_operational_live), 0)::numeric(18,2) AS total_cancelamentos_abertas,
            COALESCE(SUM(a.total_devolucoes) FILTER (WHERE a.is_operational_live), 0)::numeric(18,2) AS total_devolucoes_abertas,
            MAX(a.snapshot_ts) AS snapshot_ts,
            MAX(a.last_activity_ts) FILTER (WHERE a.is_operational_live) AS latest_activity_ts
          FROM {cash_from_sql}
          WHERE a.id_empresa = %s
          {where_filial_live}
        """
        sql_open = f"""
          SELECT
            a.id_filial,
            a.filial_nome,
            a.id_turno,
            a.turno_value,
            a.id_usuario,
            a.usuario_nome,
            a.usuario_source,
            a.abertura_ts,
            a.last_activity_ts,
            a.snapshot_ts,
            a.horas_aberto,
            a.horas_sem_movimento,
            a.severity,
            a.status_label,
            a.total_vendas,
            a.qtd_vendas,
            a.total_cancelamentos,
            a.qtd_cancelamentos,
            a.total_devolucoes,
            a.qtd_devolucoes,
            a.total_pagamentos
          FROM {cash_from_sql}
          WHERE a.id_empresa = %s
            {where_filial_live}
            AND a.is_operational_live = true
          ORDER BY
            CASE a.severity
              WHEN 'CRITICAL' THEN 0
              WHEN 'HIGH' THEN 1
              WHEN 'WARN' THEN 2
              ELSE 3
            END,
            a.horas_aberto DESC,
            a.last_activity_ts DESC NULLS LAST,
            a.id_turno DESC
          LIMIT 20
        """
        sql_stale = f"""
          SELECT
            a.id_filial,
            a.filial_nome,
            a.id_turno,
            a.turno_value,
            a.id_usuario,
            a.usuario_nome,
            a.usuario_source,
            a.abertura_ts,
            a.last_activity_ts,
            a.snapshot_ts,
            a.horas_aberto,
            a.horas_sem_movimento,
            a.total_vendas,
            a.total_cancelamentos,
            a.total_devolucoes
          FROM {cash_from_sql}
          WHERE a.id_empresa = %s
            {where_filial_live}
            AND a.is_stale = true
          ORDER BY a.last_activity_ts DESC NULLS LAST, a.horas_aberto DESC, a.id_turno DESC
          LIMIT 10
        """
        sql_payments = f"""
          SELECT
            COALESCE(m.label, 'NÃO IDENTIFICADO') AS forma_label,
            COALESCE(m.category, 'NAO_IDENTIFICADO') AS forma_category,
            COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
            COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
            COUNT(DISTINCT (live_turns.id_filial::text || ':' || live_turns.id_turno::text))::int AS qtd_turnos
          FROM {cash_payment_sql}
          JOIN dw.fact_pagamento_comprovante p
            ON p.id_empresa = live_turns.id_empresa
           AND p.id_filial = live_turns.id_filial
           AND p.id_turno = live_turns.id_turno
           AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
          LEFT JOIN LATERAL (
            SELECT label, category
            FROM app.payment_type_map m
            WHERE m.tipo_forma = p.tipo_forma
              AND m.active = true
              AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
            ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
            LIMIT 1
          ) m ON true
          WHERE live_turns.id_empresa = %s
            {where_filial_payment}
            AND live_turns.is_operational_live = true
          GROUP BY COALESCE(m.label, 'NÃO IDENTIFICADO'), COALESCE(m.category, 'NAO_IDENTIFICADO')
          ORDER BY total_valor DESC
        """
        total_turnos_row = conn.execute(sql_total_turnos, [id_empresa] + dw_branch_params).fetchone() or {"total_turnos": 0}
        summary_row = conn.execute(sql_summary, [id_empresa] + live_branch_params).fetchone() or {}
        open_rows = [dict(row) for row in conn.execute(sql_open, [id_empresa] + live_branch_params).fetchall()]
        stale_rows = [dict(row) for row in conn.execute(sql_stale, [id_empresa] + live_branch_params).fetchall()]
        payment_rows = [dict(row) for row in conn.execute(sql_payments, [id_empresa] + payment_branch_params).fetchall()]

    total_turnos = int(total_turnos_row.get("total_turnos") or 0)
    source_open_total = int(summary_row.get("caixas_abertos_fonte") or 0)
    operational_open_total = int(summary_row.get("caixas_abertos") or 0)
    stale_open_total = int(summary_row.get("caixas_stale") or 0)
    critical_count = int(summary_row.get("caixas_criticos") or 0)
    high_count = int(summary_row.get("caixas_alto_risco") or 0)
    warn_count = int(summary_row.get("caixas_em_monitoramento") or 0)
    total_vendas = round(float(summary_row.get("total_vendas_abertas") or 0), 2)
    total_cancelamentos = round(float(summary_row.get("total_cancelamentos_abertas") or 0), 2)
    total_devolucoes = round(float(summary_row.get("total_devolucoes_abertas") or 0), 2)
    caixa_liquido = cash_net_value(total_vendas, total_cancelamentos, total_devolucoes)
    snapshot_ts = summary_row.get("snapshot_ts")
    latest_activity_ts = summary_row.get("latest_activity_ts")
    snapshot_ts_iso = _iso_or_none(snapshot_ts)
    latest_activity_iso = _iso_or_none(latest_activity_ts)

    for row in open_rows:
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["qtd_vendas"] = int(row.get("qtd_vendas") or 0)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["qtd_cancelamentos"] = int(row.get("qtd_cancelamentos") or 0)
        row["total_devolucoes"] = round(float(row.get("total_devolucoes") or 0), 2)
        row["qtd_devolucoes"] = int(row.get("qtd_devolucoes") or 0)
        row["total_pagamentos"] = round(float(row.get("total_pagamentos") or 0), 2)
        row["caixa_liquido"] = cash_net_value(
            row.get("total_vendas"),
            row.get("total_cancelamentos"),
            row.get("total_devolucoes"),
        )
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["alert_message"] = (
            f"O turno {row['turno_label']} da {row['filial_label']} segue aberto há {row.get('horas_aberto') or 0} horas."
        )

    for row in stale_rows:
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["total_devolucoes"] = round(float(row.get("total_devolucoes") or 0), 2)
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))

    payment_mix = [
        {
            "label": str(row.get("forma_label") or "NÃO IDENTIFICADO").strip() or "NÃO IDENTIFICADO",
            "category": row.get("forma_category"),
            "total_valor": round(float(row.get("total_valor") or 0), 2),
            "qtd_comprovantes": int(row.get("qtd_comprovantes") or 0),
            "qtd_turnos": int(row.get("qtd_turnos") or 0),
        }
        for row in payment_rows
    ]

    cancelamentos = [
        {
            "id_filial": row.get("id_filial"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "usuario_label": row.get("usuario_label"),
            "total_cancelamentos": round(float(row.get("total_cancelamentos") or 0), 2),
            "qtd_cancelamentos": int(row.get("qtd_cancelamentos") or 0),
        }
        for row in open_rows
        if float(row.get("total_cancelamentos") or 0) > 0
    ]
    cancelamentos.sort(key=lambda item: float(item.get("total_cancelamentos") or 0), reverse=True)

    alert_rows = [
        {
            "id_filial": row.get("id_filial"),
            "filial_nome": row.get("filial_nome"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "id_usuario": row.get("id_usuario"),
            "usuario_nome": row.get("usuario_nome"),
            "usuario_label": row.get("usuario_label"),
            "abertura_ts": row.get("abertura_ts"),
            "last_activity_ts": row.get("last_activity_ts"),
            "horas_aberto": row.get("horas_aberto"),
            "severity": row.get("severity"),
            "title": row.get("alert_message"),
            "body": row.get("alert_message"),
            "url": "/cash",
            "insight_id_hash": None,
        }
        for row in open_rows
        if str(row.get("severity") or "").upper() in {"CRITICAL", "HIGH", "WARN"}
    ][:10]

    if total_turnos == 0:
        source_status = "unavailable"
        summary = "A visão operacional em tempo real ainda não possui turnos carregados no DW."
    elif source_open_total == 0:
        source_status = "ok"
        summary = "Nenhum caixa permanece aberto na fonte operacional atual."
    elif operational_open_total == 0 and stale_open_total > 0:
        source_status = "ok"
        summary = (
            f"Nenhum caixa ficou ativo na janela operacional recente. "
            f"{stale_open_total} turno(s) ainda marcados abertos na fonte foram isolados como stale."
        )
    elif critical_count > 0:
        source_status = "ok"
        summary = f"{critical_count} caixa(s) aberto(s) há mais de 24 horas exigem ação imediata."
    elif high_count > 0:
        source_status = "ok"
        summary = f"{high_count} caixa(s) aberto(s) já ultrapassaram a janela segura de operação."
    elif warn_count > 0:
        source_status = "ok"
        summary = f"{warn_count} caixa(s) aberto(s) merecem monitoramento antes do fim do dia."
    else:
        source_status = "ok"
        summary = f"{operational_open_total} caixa(s) permanecem abertos na leitura operacional recente."

    if stale_open_total > 0 and source_status == "ok" and operational_open_total > 0:
        summary = f"{summary} Mais {stale_open_total} turno(s) abertos na fonte ficaram fora do ao vivo por estarem stale."

    return {
        "source_status": source_status,
        "summary": summary,
        "kpis": {
            "total_turnos": total_turnos,
            "caixas_abertos_fonte": source_open_total,
            "caixas_abertos": operational_open_total,
            "caixas_stale": stale_open_total,
            "caixas_criticos": critical_count,
            "caixas_alto_risco": high_count,
            "caixas_em_monitoramento": warn_count,
            "total_vendas_abertas": total_vendas,
            "total_cancelamentos_abertos": total_cancelamentos,
            "total_devolucoes_abertas": total_devolucoes,
            "caixa_liquido_aberto": caixa_liquido,
            "snapshot_ts": snapshot_ts,
            "latest_activity_ts": latest_activity_ts,
            "stale_window_hours": CASH_STALE_WINDOW_HOURS,
            "schema_mode": cash_schema_mode,
        },
        "operational_sync": {
            "last_sync_at": latest_activity_iso or snapshot_ts_iso,
            "snapshot_generated_at": snapshot_ts_iso,
            "source": "dw.fact_caixa_turno_live",
        },
        "freshness": {
            "mode": "live_monitor",
            "live_through_at": latest_activity_iso or snapshot_ts_iso,
            "snapshot_generated_at": snapshot_ts_iso,
            "source": "dw.fact_caixa_turno + dw.fact_pagamento_comprovante",
        },
        "open_boxes": open_rows,
        "stale_boxes": stale_rows,
        "payment_mix": payment_mix,
        "cancelamentos": cancelamentos[:10],
        "alerts": alert_rows,
    }


def _cash_live_now_from_marts(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial_dw, dw_branch_params = _branch_scope_clause("t.id_filial", id_filial)
    where_filial_open, open_branch_params = _branch_scope_clause("a.id_filial", id_filial)
    where_filial_payment, payment_branch_params = _branch_scope_clause("p.id_filial", id_filial)
    conn_branch_id = _conn_branch_id(id_filial)
    sql_total_turnos = f"""
      SELECT COUNT(*)::int AS total_turnos
      FROM dw.fact_caixa_turno t
      WHERE t.id_empresa = %s
      {where_filial_dw}
    """
    sql_summary = f"""
      SELECT
        COUNT(*)::int AS caixas_abertos_fonte,
        COUNT(*) FILTER (WHERE a.is_operational_live)::int AS caixas_abertos,
        COUNT(*) FILTER (WHERE a.is_stale)::int AS caixas_stale,
        COUNT(*) FILTER (WHERE a.is_operational_live AND a.severity = 'CRITICAL')::int AS caixas_criticos,
        COUNT(*) FILTER (WHERE a.is_operational_live AND a.severity = 'HIGH')::int AS caixas_alto_risco,
        COUNT(*) FILTER (WHERE a.is_operational_live AND a.severity = 'WARN')::int AS caixas_em_monitoramento,
        COALESCE(SUM(a.total_vendas) FILTER (WHERE a.is_operational_live), 0)::numeric(18,2) AS total_vendas_abertas,
        COALESCE(SUM(a.total_cancelamentos) FILTER (WHERE a.is_operational_live), 0)::numeric(18,2) AS total_cancelamentos_abertas,
        MAX(a.snapshot_ts) AS snapshot_ts,
        MAX(a.last_activity_ts) FILTER (WHERE a.is_operational_live) AS latest_activity_ts
      FROM mart.agg_caixa_turno_aberto a
      WHERE a.id_empresa = %s
      {where_filial_open}
    """
    sql_open = f"""
      SELECT
        a.id_filial,
        a.filial_nome,
        a.id_turno,
        a.id_turno::text AS turno_value,
        a.id_usuario,
        a.usuario_nome,
        a.usuario_source,
        a.abertura_ts,
        a.last_activity_ts,
        a.snapshot_ts,
        a.horas_aberto,
        a.horas_sem_movimento,
        a.severity,
        a.status_label,
        a.total_vendas,
        a.qtd_vendas,
        a.total_cancelamentos,
        a.qtd_cancelamentos,
        a.total_pagamentos
      FROM mart.agg_caixa_turno_aberto a
      WHERE a.id_empresa = %s
        {where_filial_open}
        AND a.is_operational_live = true
      ORDER BY
        CASE a.severity
          WHEN 'CRITICAL' THEN 0
          WHEN 'HIGH' THEN 1
          WHEN 'WARN' THEN 2
          ELSE 3
        END,
        a.horas_aberto DESC,
        a.last_activity_ts DESC NULLS LAST,
        a.id_turno DESC
      LIMIT 20
    """
    sql_stale = f"""
      SELECT
        a.id_filial,
        a.filial_nome,
        a.id_turno,
        a.id_turno::text AS turno_value,
        a.id_usuario,
        a.usuario_nome,
        a.usuario_source,
        a.abertura_ts,
        a.last_activity_ts,
        a.snapshot_ts,
        a.horas_aberto,
        a.horas_sem_movimento,
        a.total_vendas,
        a.total_cancelamentos,
        a.total_pagamentos
      FROM mart.agg_caixa_turno_aberto a
      WHERE a.id_empresa = %s
        {where_filial_open}
        AND a.is_stale = true
      ORDER BY a.last_activity_ts DESC NULLS LAST, a.horas_aberto DESC, a.id_turno DESC
      LIMIT 10
    """
    sql_payments = f"""
      SELECT
        p.forma_label,
        p.forma_category,
        COALESCE(SUM(p.total_valor), 0)::numeric(18,2) AS total_valor,
        COALESCE(SUM(p.qtd_comprovantes), 0)::int AS qtd_comprovantes,
        COUNT(DISTINCT (p.id_filial::text || ':' || p.id_turno::text))::int AS qtd_turnos
      FROM mart.agg_caixa_forma_pagamento p
      WHERE p.id_empresa = %s
        {where_filial_payment}
      GROUP BY p.forma_label, p.forma_category
      ORDER BY total_valor DESC
    """
    sql_returns = f"""
      WITH relevant_turns AS (
        SELECT
          a.id_empresa,
          a.id_filial,
          a.id_turno,
          a.is_operational_live
        FROM mart.agg_caixa_turno_aberto a
        WHERE a.id_empresa = %s
          {where_filial_open}
      )
      SELECT
        t.id_filial,
        t.id_turno,
        t.is_operational_live,
        COALESCE(
          SUM(c.valor_total) FILTER (
            WHERE COALESCE(c.cancelado, false) = false
              AND {comercial_cfop_class_sql('c')} IN ('devolucao_saida', 'devolucao_entrada')
          ),
          0
        )::numeric(18,2) AS total_devolucoes,
        COUNT(DISTINCT c.id_comprovante) FILTER (
          WHERE COALESCE(c.cancelado, false) = false
            AND {comercial_cfop_class_sql('c')} IN ('devolucao_saida', 'devolucao_entrada')
        )::int AS qtd_devolucoes
      FROM relevant_turns t
      LEFT JOIN dw.fact_comprovante c
        ON c.id_empresa = t.id_empresa
       AND c.id_filial = t.id_filial
       AND c.id_turno = t.id_turno
       AND {_resolved_cash_eligible_sql('c.cash_eligible', 'c.data', 'c.data_conta', 'c.id_turno')}
      GROUP BY t.id_filial, t.id_turno, t.is_operational_live
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        if not relation_exists(conn, *CASH_OPEN_RELATION) or not cash_payment_relation_exists(conn):
            return _cash_live_now_live_query(role, id_empresa, id_filial)

        cash_schema_mode = cash_open_schema_mode(conn)
        total_turnos_row = conn.execute(sql_total_turnos, [id_empresa] + dw_branch_params).fetchone() or {"total_turnos": 0}
        summary_row = conn.execute(sql_summary, [id_empresa] + open_branch_params).fetchone() or {}
        open_rows = [dict(row) for row in conn.execute(sql_open, [id_empresa] + open_branch_params).fetchall()]
        stale_rows = [dict(row) for row in conn.execute(sql_stale, [id_empresa] + open_branch_params).fetchall()]
        payment_rows = [dict(row) for row in conn.execute(sql_payments, [id_empresa] + payment_branch_params).fetchall()]
        return_rows = [dict(row) for row in conn.execute(sql_returns, [id_empresa] + open_branch_params).fetchall()]

    return_map = {
        (int(row.get("id_filial") or 0), int(row.get("id_turno") or 0)): {
            "total_devolucoes": round(float(row.get("total_devolucoes") or 0), 2),
            "qtd_devolucoes": int(row.get("qtd_devolucoes") or 0),
            "is_operational_live": bool(row.get("is_operational_live")),
        }
        for row in return_rows
    }

    total_turnos = int(total_turnos_row.get("total_turnos") or 0)
    source_open_total = int(summary_row.get("caixas_abertos_fonte") or 0)
    operational_open_total = int(summary_row.get("caixas_abertos") or 0)
    stale_open_total = int(summary_row.get("caixas_stale") or 0)
    critical_count = int(summary_row.get("caixas_criticos") or 0)
    high_count = int(summary_row.get("caixas_alto_risco") or 0)
    warn_count = int(summary_row.get("caixas_em_monitoramento") or 0)
    total_vendas = round(float(summary_row.get("total_vendas_abertas") or 0), 2)
    total_cancelamentos = round(float(summary_row.get("total_cancelamentos_abertas") or 0), 2)
    snapshot_ts = summary_row.get("snapshot_ts")
    latest_activity_ts = summary_row.get("latest_activity_ts")
    snapshot_ts_iso = _iso_or_none(snapshot_ts)
    latest_activity_iso = _iso_or_none(latest_activity_ts)

    for row in open_rows:
        return_info = return_map.get((int(row.get("id_filial") or 0), int(row.get("id_turno") or 0)), {})
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["qtd_vendas"] = int(row.get("qtd_vendas") or 0)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["qtd_cancelamentos"] = int(row.get("qtd_cancelamentos") or 0)
        row["total_devolucoes"] = round(float(return_info.get("total_devolucoes") or 0), 2)
        row["qtd_devolucoes"] = int(return_info.get("qtd_devolucoes") or 0)
        row["total_pagamentos"] = round(float(row.get("total_pagamentos") or 0), 2)
        row["caixa_liquido"] = cash_net_value(
            row.get("total_vendas"),
            row.get("total_cancelamentos"),
            row.get("total_devolucoes"),
        )
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["alert_message"] = (
            f"O turno {row['turno_label']} da {row['filial_label']} segue aberto há {row.get('horas_aberto') or 0} horas."
        )

    for row in stale_rows:
        return_info = return_map.get((int(row.get("id_filial") or 0), int(row.get("id_turno") or 0)), {})
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["total_devolucoes"] = round(float(return_info.get("total_devolucoes") or 0), 2)
        row["qtd_devolucoes"] = int(return_info.get("qtd_devolucoes") or 0)
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))

    total_devolucoes = round(sum(float(row.get("total_devolucoes") or 0) for row in open_rows), 2)
    caixa_liquido = cash_net_value(total_vendas, total_cancelamentos, total_devolucoes)

    payment_mix = [
        {
            "label": str(row.get("forma_label") or "NÃO IDENTIFICADO").strip() or "NÃO IDENTIFICADO",
            "category": row.get("forma_category"),
            "total_valor": round(float(row.get("total_valor") or 0), 2),
            "qtd_comprovantes": int(row.get("qtd_comprovantes") or 0),
            "qtd_turnos": int(row.get("qtd_turnos") or 0),
        }
        for row in payment_rows
    ]

    cancelamentos = [
        {
            "id_filial": row.get("id_filial"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "usuario_label": row.get("usuario_label"),
            "total_cancelamentos": round(float(row.get("total_cancelamentos") or 0), 2),
            "qtd_cancelamentos": int(row.get("qtd_cancelamentos") or 0),
        }
        for row in open_rows
        if float(row.get("total_cancelamentos") or 0) > 0
    ]
    cancelamentos.sort(key=lambda item: float(item.get("total_cancelamentos") or 0), reverse=True)

    alert_rows = [
        {
            "id_filial": row.get("id_filial"),
            "filial_nome": row.get("filial_nome"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "id_usuario": row.get("id_usuario"),
            "usuario_nome": row.get("usuario_nome"),
            "usuario_label": row.get("usuario_label"),
            "abertura_ts": row.get("abertura_ts"),
            "last_activity_ts": row.get("last_activity_ts"),
            "horas_aberto": row.get("horas_aberto"),
            "severity": row.get("severity"),
            "title": row.get("alert_message"),
            "body": row.get("alert_message"),
            "url": "/cash",
            "insight_id_hash": None,
        }
        for row in open_rows
        if str(row.get("severity") or "").upper() in {"CRITICAL", "HIGH", "WARN"}
    ][:10]

    if total_turnos == 0:
        source_status = "unavailable"
        summary = "A visão operacional em tempo real ainda não possui turnos carregados no DW."
    elif source_open_total == 0:
        source_status = "ok"
        summary = "Nenhum caixa permanece aberto na fonte operacional atual."
    elif operational_open_total == 0 and stale_open_total > 0:
        source_status = "ok"
        summary = (
            f"Nenhum caixa ficou ativo na janela operacional recente. "
            f"{stale_open_total} turno(s) ainda marcados abertos na fonte foram isolados como stale."
        )
    elif critical_count > 0:
        source_status = "ok"
        summary = f"{critical_count} caixa(s) aberto(s) há mais de 24 horas exigem ação imediata."
    elif high_count > 0:
        source_status = "ok"
        summary = f"{high_count} caixa(s) aberto(s) já ultrapassaram a janela segura de operação."
    elif warn_count > 0:
        source_status = "ok"
        summary = f"{warn_count} caixa(s) aberto(s) merecem monitoramento antes do fim do dia."
    else:
        source_status = "ok"
        summary = f"{operational_open_total} caixa(s) permanecem abertos na leitura operacional recente."

    if stale_open_total > 0 and source_status == "ok" and operational_open_total > 0:
        summary = f"{summary} Mais {stale_open_total} turno(s) abertos na fonte ficaram fora do ao vivo por estarem stale."

    return {
        "source_status": source_status,
        "summary": summary,
        "kpis": {
            "total_turnos": total_turnos,
            "caixas_abertos_fonte": source_open_total,
            "caixas_abertos": operational_open_total,
            "caixas_stale": stale_open_total,
            "caixas_criticos": critical_count,
            "caixas_alto_risco": high_count,
            "caixas_em_monitoramento": warn_count,
            "total_vendas_abertas": total_vendas,
            "total_cancelamentos_abertos": total_cancelamentos,
            "total_devolucoes_abertas": total_devolucoes,
            "caixa_liquido_aberto": caixa_liquido,
            "snapshot_ts": snapshot_ts,
            "latest_activity_ts": latest_activity_ts,
            "stale_window_hours": CASH_STALE_WINDOW_HOURS,
            "schema_mode": cash_schema_mode,
        },
        "operational_sync": {
            "last_sync_at": latest_activity_iso or snapshot_ts_iso,
            "snapshot_generated_at": snapshot_ts_iso,
            "source": "mart.agg_caixa_turno_aberto",
        },
        "freshness": {
            "mode": "live_monitor",
            "live_through_at": latest_activity_iso or snapshot_ts_iso,
            "snapshot_generated_at": snapshot_ts_iso,
            "source": "mart.agg_caixa_turno_aberto + mart.agg_caixa_forma_pagamento",
        },
        "open_boxes": open_rows,
        "stale_boxes": stale_rows,
        "payment_mix": payment_mix,
        "cancelamentos": cancelamentos[:10],
        "alerts": alert_rows,
    }


def _cash_live_now(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    return _cash_live_now_from_marts(role, id_empresa, id_filial)


def _cash_sales_docs_cte(
    id_empresa: int,
    id_filial: Optional[int],
    *,
    date_key_sql: str,
    date_params: List[Any],
) -> tuple[str, List[Any]]:
    where_filial, branch_params = _branch_scope_clause("v.id_filial", id_filial)
    params = [id_empresa] + date_params + branch_params
    cte = f"""
      WITH sales_docs AS (
        SELECT
          v.id_filial,
          COALESCE(c.id_turno, v.id_turno) AS id_turno,
          COALESCE(v.data_key, c.data_key) AS data_key,
          COALESCE(v.data, c.data) AS data,
          v.id_comprovante AS doc_key,
          {_sales_status_expression('v')} AS situacao,
          COALESCE(SUM(i.total), 0)::numeric(18,2) AS total
        FROM dw.fact_venda v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa
         AND i.id_filial = v.id_filial
         AND i.id_db = v.id_db
         AND i.id_comprovante = v.id_comprovante
        JOIN dw.fact_comprovante c
          ON c.id_empresa = v.id_empresa
         AND c.id_filial = v.id_filial
         AND c.id_db = v.id_db
         AND c.id_comprovante = v.id_comprovante
        WHERE v.id_empresa = %s
          AND {date_key_sql}
          {where_filial}
          AND {_sales_status_expression('v')} IN ({SALE_STATUS}, {CANCELLATION_STATUS}, {RETURN_STATUS})
          AND {sales_cfop_filter_sql('i')}
          AND {_resolved_cash_eligible_sql('c.cash_eligible', 'c.data', 'c.data_conta', 'c.id_turno')}
        GROUP BY
          v.id_filial,
          COALESCE(c.id_turno, v.id_turno),
          COALESCE(v.data_key, c.data_key),
          COALESCE(v.data, c.data),
          v.id_comprovante,
          {_sales_status_expression('v')}
      )
    """
    return cte, params


def _cash_historical_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial_pay, pay_branch_params = _branch_scope_clause("p.id_filial", id_filial)
    sales_docs_cte, params_sales = _cash_sales_docs_cte(
        id_empresa,
        id_filial,
        date_key_sql="v.data_key BETWEEN %s AND %s",
        date_params=[ini, fim],
    )
    params_pay = [id_empresa, ini, fim] + pay_branch_params

    sql_summary = sales_docs_cte + f"""
      , vendas AS (
        SELECT
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text))::int AS caixas_periodo,
          COUNT(DISTINCT data_key)::int AS dias_com_movimento,
          COALESCE(SUM(total) FILTER (WHERE situacao = {SALE_STATUS}), 0)::numeric(18,2) AS total_vendas,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {SALE_STATUS})::int AS qtd_vendas,
          COALESCE(SUM(total) FILTER (WHERE situacao = {CANCELLATION_STATUS}), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {CANCELLATION_STATUS})::int AS qtd_cancelamentos,
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text)) FILTER (WHERE situacao = {CANCELLATION_STATUS})::int AS caixas_com_cancelamento,
          COALESCE(SUM(total) FILTER (WHERE situacao = {RETURN_STATUS}), 0)::numeric(18,2) AS total_devolucoes,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {RETURN_STATUS})::int AS qtd_devolucoes,
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text)) FILTER (WHERE situacao = {RETURN_STATUS})::int AS caixas_com_devolucao,
          MIN(data_key)::int AS min_data_key,
          MAX(data_key)::int AS max_data_key
        FROM sales_docs
      ), pagamentos AS (
        SELECT
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
      )
      SELECT
        v.caixas_periodo,
        v.dias_com_movimento,
        v.total_vendas,
        v.qtd_vendas,
        v.total_cancelamentos,
        v.qtd_cancelamentos,
        v.caixas_com_cancelamento,
        v.total_devolucoes,
        v.qtd_devolucoes,
        v.caixas_com_devolucao,
        v.min_data_key,
        v.max_data_key,
        p.total_pagamentos
      FROM vendas v
      CROSS JOIN pagamentos p
    """
    sql_by_day = sales_docs_cte + f"""
      , vendas AS (
        SELECT
          data_key,
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text))::int AS caixas,
          COALESCE(SUM(total) FILTER (WHERE situacao = {SALE_STATUS}), 0)::numeric(18,2) AS total_vendas,
          COALESCE(SUM(total) FILTER (WHERE situacao = {CANCELLATION_STATUS}), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {CANCELLATION_STATUS})::int AS qtd_cancelamentos,
          COALESCE(SUM(total) FILTER (WHERE situacao = {RETURN_STATUS}), 0)::numeric(18,2) AS total_devolucoes,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {RETURN_STATUS})::int AS qtd_devolucoes
        FROM sales_docs
        GROUP BY data_key
      ), pagamentos AS (
        SELECT
          p.data_key,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
        GROUP BY p.data_key
      )
      SELECT
        COALESCE(v.data_key, p.data_key)::int AS data_key,
        COALESCE(v.caixas, 0)::int AS caixas,
        COALESCE(v.total_vendas, 0)::numeric(18,2) AS total_vendas,
        COALESCE(v.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
        COALESCE(v.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
        COALESCE(v.total_devolucoes, 0)::numeric(18,2) AS total_devolucoes,
        COALESCE(v.qtd_devolucoes, 0)::int AS qtd_devolucoes,
        COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos
      FROM vendas v
      FULL OUTER JOIN pagamentos p
        ON p.data_key = v.data_key
      ORDER BY COALESCE(v.data_key, p.data_key)
    """
    sql_payment_mix = f"""
      SELECT
        COALESCE(m.label, 'NÃO IDENTIFICADO') AS label,
        COALESCE(m.category, 'NAO_IDENTIFICADO') AS category,
        COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
        COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
        COUNT(DISTINCT (p.id_filial::text || ':' || COALESCE(p.id_turno, -1)::text))::int AS qtd_turnos
      FROM dw.fact_pagamento_comprovante p
      LEFT JOIN LATERAL (
        SELECT label, category
        FROM app.payment_type_map m
        WHERE m.tipo_forma = p.tipo_forma
          AND m.active = true
          AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
        ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
        LIMIT 1
      ) m ON true
      WHERE p.id_empresa = %s
        AND p.data_key BETWEEN %s AND %s
        {where_filial_pay}
        AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
      GROUP BY COALESCE(m.label, 'NÃO IDENTIFICADO'), COALESCE(m.category, 'NAO_IDENTIFICADO')
      ORDER BY total_valor DESC
    """
    sql_top_turnos = sales_docs_cte + f"""
      , turnos AS (
        SELECT
          id_filial,
          id_turno,
          MIN(data_key)::int AS min_data_key,
          MAX(data_key)::int AS max_data_key,
          MIN(data) AS first_event_at,
          MAX(data) AS last_event_at,
          COALESCE(SUM(total) FILTER (WHERE situacao = {SALE_STATUS}), 0)::numeric(18,2) AS total_vendas,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {SALE_STATUS})::int AS qtd_vendas,
          COALESCE(SUM(total) FILTER (WHERE situacao = {CANCELLATION_STATUS}), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {CANCELLATION_STATUS})::int AS qtd_cancelamentos,
          COALESCE(SUM(total) FILTER (WHERE situacao = {RETURN_STATUS}), 0)::numeric(18,2) AS total_devolucoes,
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {RETURN_STATUS})::int AS qtd_devolucoes
        FROM sales_docs
        GROUP BY id_filial, id_turno
      ), pagamentos AS (
        SELECT
          p.id_filial,
          p.id_turno,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND {_resolved_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
        GROUP BY p.id_filial, p.id_turno
      )
      SELECT
        c.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        c.id_turno,
        {_turno_value_sql('t.payload', 'c.id_turno')} AS turno_value,
        t.id_usuario,
        COALESCE(
          NULLIF(u.nome, ''),
          NULLIF(t.payload->>'NOMEUSUARIOS', ''),
          NULLIF(t.payload->>'NOME_USUARIOS', ''),
          NULLIF(t.payload->>'NOMEUSUARIO', ''),
          NULLIF(t.payload->>'NOME_USUARIO', ''),
          CASE WHEN t.id_usuario IS NOT NULL THEN format('Operador %%s', t.id_usuario) ELSE NULL END
        ) AS usuario_nome,
        t.abertura_ts,
        t.fechamento_ts,
        t.is_aberto,
        c.first_event_at,
        c.last_event_at,
        c.total_vendas,
        c.qtd_vendas,
        c.total_cancelamentos,
        c.qtd_cancelamentos,
        c.total_devolucoes,
        c.qtd_devolucoes,
        COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos
      FROM turnos c
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = %s
       AND t.id_filial = c.id_filial
       AND t.id_turno = c.id_turno
       AND (t.data_key_abertura IS NULL OR t.data_key_abertura <= c.max_data_key)
       AND (
             t.data_key_fechamento IS NULL
             OR t.data_key_fechamento >= c.min_data_key
             OR t.is_aberto = true
           )
      LEFT JOIN dw.dim_usuario_caixa u
        ON u.id_empresa = %s
       AND u.id_filial = c.id_filial
       AND u.id_usuario = t.id_usuario
      LEFT JOIN auth.filiais f
        ON f.id_empresa = %s
       AND f.id_filial = c.id_filial
      LEFT JOIN pagamentos p
        ON p.id_filial = c.id_filial
       AND p.id_turno = c.id_turno
      ORDER BY c.total_vendas DESC, c.total_cancelamentos DESC, c.total_devolucoes DESC, c.last_event_at DESC
      LIMIT 12
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        summary_row = conn.execute(sql_summary, params_sales + params_pay).fetchone() or {}
        by_day_rows = [dict(row) for row in conn.execute(sql_by_day, params_sales + params_pay).fetchall()]
        payment_mix_rows = [dict(row) for row in conn.execute(sql_payment_mix, params_pay).fetchall()]
        top_turnos_rows = [
            dict(row)
            for row in conn.execute(
                sql_top_turnos,
                params_sales + params_pay + [id_empresa, id_empresa, id_empresa],
            ).fetchall()
        ]

    total_vendas = round(float(summary_row.get("total_vendas") or 0), 2)
    qtd_vendas = int(summary_row.get("qtd_vendas") or 0)
    total_cancelamentos = round(float(summary_row.get("total_cancelamentos") or 0), 2)
    total_devolucoes = round(float(summary_row.get("total_devolucoes") or 0), 2)
    total_pagamentos = round(float(summary_row.get("total_pagamentos") or 0), 2)
    caixas_periodo = int(summary_row.get("caixas_periodo") or 0)
    qtd_cancelamentos = int(summary_row.get("qtd_cancelamentos") or 0)
    qtd_devolucoes = int(summary_row.get("qtd_devolucoes") or 0)
    caixa_liquido = cash_net_value(total_vendas, total_cancelamentos, total_devolucoes)
    payment_mix = [
        {
            "label": row.get("label"),
            "category": row.get("category"),
            "total_valor": round(float(row.get("total_valor") or 0), 2),
            "qtd_comprovantes": int(row.get("qtd_comprovantes") or 0),
            "qtd_turnos": int(row.get("qtd_turnos") or 0),
        }
        for row in payment_mix_rows
    ]

    for row in by_day_rows:
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["qtd_cancelamentos"] = int(row.get("qtd_cancelamentos") or 0)
        row["total_devolucoes"] = round(float(row.get("total_devolucoes") or 0), 2)
        row["qtd_devolucoes"] = int(row.get("qtd_devolucoes") or 0)
        row["total_pagamentos"] = round(float(row.get("total_pagamentos") or 0), 2)
        row["caixa_liquido"] = cash_net_value(
            row.get("total_vendas"),
            row.get("total_cancelamentos"),
            row.get("total_devolucoes"),
        )

    for row in top_turnos_rows:
        row["total_vendas"] = round(float(row.get("total_vendas") or 0), 2)
        row["qtd_vendas"] = int(row.get("qtd_vendas") or 0)
        row["total_cancelamentos"] = round(float(row.get("total_cancelamentos") or 0), 2)
        row["qtd_cancelamentos"] = int(row.get("qtd_cancelamentos") or 0)
        row["total_devolucoes"] = round(float(row.get("total_devolucoes") or 0), 2)
        row["qtd_devolucoes"] = int(row.get("qtd_devolucoes") or 0)
        row["total_pagamentos"] = round(float(row.get("total_pagamentos") or 0), 2)
        row["caixa_liquido"] = cash_net_value(
            row.get("total_vendas"),
            row.get("total_cancelamentos"),
            row.get("total_devolucoes"),
        )
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))

    cancelamentos = [
        {
            "id_filial": row.get("id_filial"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "usuario_label": row.get("usuario_label"),
            "total_cancelamentos": round(float(row.get("total_cancelamentos") or 0), 2),
            "qtd_cancelamentos": int(row.get("qtd_cancelamentos") or 0),
        }
        for row in sorted(top_turnos_rows, key=lambda item: float(item.get("total_cancelamentos") or 0), reverse=True)
        if float(row.get("total_cancelamentos") or 0) > 0
    ][:10]

    if caixas_periodo == 0 and total_pagamentos == 0:
        source_status = "unavailable"
        summary = "Não houve movimentos de caixa vinculados ao período selecionado."
    elif caixas_periodo == 0:
        source_status = "partial"
        summary = "Há pagamentos vinculados ao período, mas sem turnos históricos suficientes para fechar a visão completa."
    else:
        source_status = "ok" if payment_mix else "partial"
        summary = (
            f"{caixas_periodo} caixa(s) movimentaram { _format_brl(total_vendas) } em vendas válidas "
            f"entre {dt_ini.isoformat()} e {dt_fim.isoformat()}, com {qtd_cancelamentos} cancelamento(s) somando { _format_brl(total_cancelamentos) }, "
            f"{qtd_devolucoes} devolução(ões) somando { _format_brl(total_devolucoes) } e caixa líquido de { _format_brl(caixa_liquido) }."
        )

    return {
        "source_status": source_status,
        "summary": summary,
        "requested_window": {
            "dt_ini": dt_ini,
            "dt_fim": dt_fim,
        },
        "coverage": {
            "min_data_key": summary_row.get("min_data_key"),
            "max_data_key": summary_row.get("max_data_key"),
        },
        "kpis": {
            "caixas_periodo": caixas_periodo,
            "dias_com_movimento": int(summary_row.get("dias_com_movimento") or 0),
            "ticket_medio": round(total_vendas / qtd_vendas, 2) if qtd_vendas else 0.0,
            "total_vendas": total_vendas,
            "total_pagamentos": total_pagamentos,
            "total_cancelamentos": total_cancelamentos,
            "qtd_cancelamentos": qtd_cancelamentos,
            "caixas_com_cancelamento": int(summary_row.get("caixas_com_cancelamento") or 0),
            "total_devolucoes": total_devolucoes,
            "qtd_devolucoes": qtd_devolucoes,
            "caixas_com_devolucao": int(summary_row.get("caixas_com_devolucao") or 0),
            "caixa_liquido": caixa_liquido,
        },
        "by_day": by_day_rows,
        "payment_mix": payment_mix,
        "top_turnos": top_turnos_rows[:10],
        "cancelamentos": cancelamentos,
    }


def _cash_historical_overview_from_marts(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    commercial = cash_commercial_overview(role, id_empresa, id_filial, dt_ini, dt_fim)
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)

    sql_payment_mix = f"""
      SELECT
        label,
        category,
        COALESCE(SUM(total_valor), 0)::numeric(18,2) AS total_valor,
        COALESCE(SUM(qtd_comprovantes), 0)::int AS qtd_comprovantes,
        COUNT(DISTINCT data_key)::int AS qtd_turnos
      FROM mart.agg_pagamentos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY label, category
      ORDER BY total_valor DESC, label
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        payment_mix_rows = [
            dict(row)
            for row in conn.execute(sql_payment_mix, [id_empresa, ini, fim] + branch_params).fetchall()
        ]

    payment_mix = [
        {
            "label": row.get("label"),
            "category": row.get("category"),
            "total_valor": round(float(row.get("total_valor") or 0), 2),
            "qtd_comprovantes": int(row.get("qtd_comprovantes") or 0),
            "qtd_turnos": int(row.get("qtd_turnos") or 0),
        }
        for row in payment_mix_rows
    ]
    commercial_top_turnos = commercial.get("top_turnos") or []
    cancelamentos = [
        {
            "id_filial": row.get("id_filial"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "usuario_label": row.get("usuario_label"),
            "total_cancelamentos": round(float(row.get("total_cancelamentos") or 0), 2),
            "qtd_cancelamentos": int(row.get("qtd_cancelamentos") or 0),
        }
        for row in sorted(commercial_top_turnos, key=lambda item: float(item.get("total_cancelamentos") or 0), reverse=True)
        if float(row.get("total_cancelamentos") or 0) > 0
    ]

    commercial_kpis = dict(commercial.get("kpis") or {})
    total_vendas = round(float(commercial_kpis.get("total_vendas") or 0), 2)
    total_cancelamentos = round(float(commercial_kpis.get("total_cancelamentos") or 0), 2)
    total_pagamentos = round(float(commercial_kpis.get("total_pagamentos") or 0), 2)
    caixas_periodo = int(commercial_kpis.get("caixas_periodo") or 0)
    qtd_vendas = int(commercial_kpis.get("qtd_vendas") or 0)
    qtd_cancelamentos = int(sum(int(row.get("qtd_cancelamentos") or 0) for row in cancelamentos))

    if caixas_periodo == 0 and total_pagamentos == 0:
        source_status = "unavailable"
    elif total_vendas == 0 and total_pagamentos == 0 and total_cancelamentos == 0:
        source_status = "partial"
    else:
        source_status = "ok"

    return {
        "source_status": source_status,
        "summary": commercial.get("summary"),
        "requested_window": {
            "dt_ini": dt_ini,
            "dt_fim": dt_fim,
        },
        "coverage": {
            "min_data_key": _date_key(dt_ini),
            "max_data_key": _date_key(dt_fim),
        },
        "kpis": {
            "caixas_periodo": caixas_periodo,
            "dias_com_movimento": len(commercial.get("by_day") or []),
            "ticket_medio": round(total_vendas / qtd_vendas, 2) if qtd_vendas else 0.0,
            "total_vendas": total_vendas,
            "total_pagamentos": total_pagamentos,
            "total_cancelamentos": total_cancelamentos,
            "qtd_cancelamentos": qtd_cancelamentos,
            "caixas_com_cancelamento": len(cancelamentos),
            "total_devolucoes": 0.0,
            "qtd_devolucoes": 0,
            "caixas_com_devolucao": 0,
            "caixa_liquido": cash_net_value(total_vendas, total_cancelamentos, 0.0),
        },
        "by_day": commercial.get("by_day") or [],
        "payment_mix": payment_mix,
        "top_turnos": commercial.get("top_turnos") or [],
        "cancelamentos": cancelamentos,
    }


def cash_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
) -> Dict[str, Any]:
    effective_dt_fim = dt_fim or business_today(id_empresa)
    effective_dt_ini = dt_ini or (effective_dt_fim - timedelta(days=29))
    commercial_coverage = commercial_window_coverage(role, id_empresa, id_filial, effective_dt_ini, effective_dt_fim)
    historical_dt_ini = commercial_coverage.get("effective_dt_ini") or effective_dt_ini
    historical_dt_fim = commercial_coverage.get("effective_dt_fim") or effective_dt_fim
    historical = _cash_historical_overview_from_marts(
        role,
        id_empresa,
        id_filial,
        dt_ini=historical_dt_ini,
        dt_fim=historical_dt_fim,
    )
    commercial = cash_commercial_overview(role, id_empresa, id_filial, dt_ini=historical_dt_ini, dt_fim=historical_dt_fim)
    commercial["commercial_coverage"] = commercial_coverage
    dre_summary = cash_dre_summary(role, id_empresa, id_filial, as_of=historical_dt_fim)
    live_now = _cash_live_now(role, id_empresa, id_filial)
    return {
        "source_status": historical.get("source_status"),
        "summary": commercial.get("summary") or historical.get("summary"),
        "kpis": historical.get("kpis"),
        "commercial": commercial,
        "dre_summary": dre_summary,
        "definitions": cash_definitions(),
        "operational_sync": live_now.get("operational_sync"),
        "freshness": {
            "mode": "latest_compatible" if commercial_coverage.get("mode") == "shifted_latest" else "historical_plus_live",
            "historical_through_dt": historical_dt_fim.isoformat(),
            "live_through_at": (live_now.get("operational_sync") or {}).get("last_sync_at"),
            "source": "dw.cash_historical + dw.cash_live",
        },
        "historical": historical,
        "live_now": live_now,
        "open_boxes": live_now.get("open_boxes") or [],
        "stale_boxes": live_now.get("stale_boxes") or [],
        "payment_mix": historical.get("payment_mix") or [],
        "cancelamentos": historical.get("cancelamentos") or [],
        "alerts": live_now.get("alerts") or [],
        "commercial_coverage": commercial_coverage,
    }


def open_cash_monitor(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    cash = _cash_live_now(role, id_empresa, id_filial)
    kpis = cash.get("kpis") or {}
    severity = "OK"
    if int(kpis.get("caixas_criticos") or 0) > 0:
        severity = "CRITICAL"
    elif int(kpis.get("caixas_alto_risco") or 0) > 0:
        severity = "HIGH"
    elif int(kpis.get("caixas_em_monitoramento") or 0) > 0:
        severity = "WARN"
    elif int(kpis.get("caixas_stale") or 0) > 0:
        severity = "WARN"
    elif cash.get("source_status") == "unavailable":
        severity = "UNAVAILABLE"

    return {
        "source_status": cash.get("source_status"),
        "severity": severity,
        "summary": cash.get("summary"),
        "total_turnos": int(kpis.get("total_turnos") or 0),
        "mapped_rows": int(kpis.get("total_turnos") or 0),
        "total_open": int(kpis.get("caixas_abertos") or 0),
        "source_open_total": int(kpis.get("caixas_abertos_fonte") or 0),
        "stale_count": int(kpis.get("caixas_stale") or 0),
        "warn_count": int(kpis.get("caixas_em_monitoramento") or 0),
        "high_count": int(kpis.get("caixas_alto_risco") or 0),
        "critical_count": int(kpis.get("caixas_criticos") or 0),
        "snapshot_ts": kpis.get("snapshot_ts"),
        "items": cash.get("open_boxes") or [],
    }


def health_score_latest(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    where_as_of = "AND dt_ref <= %s" if as_of is not None else ""
    branch_ids = _branch_ids(id_filial)
    snapshot_meta = _snapshot_meta(role, "health_score_daily", id_empresa, id_filial, as_of, "latest_leq_ref")
    if branch_ids is not None and len(branch_ids) == 1:
        sql = f"""
          SELECT
            dt_ref,
            score_total,
            components,
            reasons
          FROM mart.health_score_daily
          WHERE id_empresa = %s
          {where_filial}
          {where_as_of}
          ORDER BY dt_ref DESC
          LIMIT 1
        """
        params = [id_empresa] + branch_params + ([] if as_of is None else [as_of])
    else:
        sql = f"""
          WITH scoped AS (
            SELECT
              dt_ref,
              AVG(comp_margem)::numeric(10,2) AS comp_margem,
              AVG(comp_fraude)::numeric(10,2) AS comp_fraude,
              AVG(comp_churn)::numeric(10,2) AS comp_churn,
              AVG(comp_finance)::numeric(10,2) AS comp_finance,
              AVG(comp_operacao)::numeric(10,2) AS comp_operacao,
              AVG(comp_dados)::numeric(10,2) AS comp_dados,
              AVG(score_total)::numeric(10,2) AS score_total
            FROM mart.health_score_daily
            WHERE id_empresa = %s
            {where_filial}
            {where_as_of}
            GROUP BY dt_ref
            ORDER BY dt_ref DESC
            LIMIT 1
          )
          SELECT
            dt_ref,
            score_total,
            jsonb_build_object(
              'margem', comp_margem,
              'fraude', comp_fraude,
              'churn', comp_churn,
              'finance', comp_finance,
              'operacao', comp_operacao,
              'dados', comp_dados
            ) AS components,
            jsonb_build_object(
              'scope_mode', CASE WHEN %s::int[] IS NULL THEN 'all_branches' ELSE 'multi_branch' END,
              'selected_branches', COALESCE(to_jsonb(%s::int[]), '[]'::jsonb)
            ) AS reasons
          FROM scoped
        """
        params = [id_empresa] + branch_params + ([] if as_of is None else [as_of]) + [branch_ids, branch_ids]

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        if row:
            payload = dict(row)
            payload.update(snapshot_meta)
            payload["snapshot_status"] = "exact" if as_of is None or payload.get("dt_ref") == as_of else "best_effort"
            payload["precision_mode"] = "exact" if payload["snapshot_status"] == "exact" else "latest_leq_ref"
            payload["source_kind"] = "snapshot"
            return payload
        payload = {
            "dt_ref": as_of,
            "score_total": 0,
            "components": {},
            "reasons": {},
        }
        payload.update(snapshot_meta)
        return payload


# ========================
# Metas & Equipe
# ========================

def goals_today(role: str, id_empresa: int, id_filial: Any, goal_date: date) -> List[Dict[str, Any]]:
    """Goals configured for the current month within the selected scope."""

    month_start = _month_start(goal_date)
    month_end = _next_month_start(month_start) - timedelta(days=1)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql = f"""
      SELECT
        goal_type,
        SUM(target_value)::numeric(18,2) AS target_value,
        COUNT(*)::int AS branch_goal_count,
        MIN(goal_date)::date AS goal_month
      FROM app.goals
      WHERE id_empresa = %s
        AND goal_date BETWEEN %s AND %s
        {where_filial}
      GROUP BY goal_type
      ORDER BY goal_type
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, [id_empresa, month_start, month_end] + branch_params).fetchall())


def upsert_goal(
    role: str,
    id_empresa: int,
    id_filial: int,
    goal_date: date,
    goal_type: str,
    target_value: float,
) -> Dict[str, Any]:
    month_ref = _month_start(goal_date)
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(
            """
            INSERT INTO app.goals (id_empresa, id_filial, goal_date, goal_type, target_value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_empresa, id_filial, goal_date, goal_type)
            DO UPDATE
              SET target_value = EXCLUDED.target_value
            RETURNING
              id,
              id_empresa,
              id_filial,
              goal_date,
              goal_type,
              target_value,
              created_at
            """,
            (
                id_empresa,
                id_filial,
                month_ref,
                goal_type,
                round(float(target_value or 0), 2),
            ),
        ).fetchone()
        conn.commit()
    payload = dict(row or {})
    payload["month_ref"] = _iso_or_none(payload.get("goal_date"))
    return payload


def _sales_daily_totals(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
) -> List[Dict[str, Any]]:
    if dt_fim < dt_ini:
        return []

    branch_clause, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql = f"""
      SELECT
        data_key,
        COALESCE(SUM(total_venda), 0)::numeric(18,2) AS faturamento
      FROM dw.fact_venda
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        AND {_sales_status_expression('dw.fact_venda')} = 1
        {branch_clause}
      GROUP BY data_key
      ORDER BY data_key
    """
    params = [id_empresa, _date_key(dt_ini), _date_key(dt_fim)] + branch_params
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _sales_month_summaries(
    role: str,
    id_empresa: int,
    id_filial: Any,
    month_ref: date,
    lookback_months: int = 6,
) -> List[Dict[str, Any]]:
    last_closed_month = _shift_months(_month_start(month_ref), -1)
    first_month = _shift_months(last_closed_month, -(max(lookback_months, 1) - 1))
    branch_clause, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql = f"""
      SELECT
        date_trunc('month', to_date(data_key::text, 'YYYYMMDD'))::date AS month_ref,
        COUNT(DISTINCT data_key)::int AS observed_days,
        COALESCE(SUM(total_venda), 0)::numeric(18,2) AS faturamento
      FROM dw.fact_venda
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        AND {_sales_status_expression('dw.fact_venda')} = 1
        {branch_clause}
      GROUP BY 1
      ORDER BY month_ref DESC
    """
    params = [
        id_empresa,
        _date_key(first_month),
        _date_key(_next_month_start(last_closed_month) - timedelta(days=1)),
    ] + branch_params
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        raw_rows = [dict(row) for row in conn.execute(sql, params).fetchall()]

    raw_map: Dict[date, Dict[str, Any]] = {
        row.get("month_ref"): row
        for row in raw_rows
        if isinstance(row.get("month_ref"), date)
    }

    calendar_rows: List[Dict[str, Any]] = []
    for offset in range(max(lookback_months, 1)):
        current_month = _shift_months(last_closed_month, -offset)
        expected_days = _days_in_month(current_month)
        row = raw_map.get(current_month, {})
        observed_days = int(row.get("observed_days") or 0)
        faturamento = round(float(row.get("faturamento") or 0), 2)
        completeness_pct = round((observed_days / expected_days) * 100, 1) if expected_days else 0.0
        calendar_rows.append(
            {
                "month_ref": current_month.isoformat(),
                "faturamento": faturamento,
                "observed_days": observed_days,
                "expected_days": expected_days,
                "completeness_pct": completeness_pct,
                "has_data": observed_days > 0,
                "is_partial": observed_days > 0 and observed_days < expected_days,
                "is_complete": observed_days >= expected_days,
            }
        )
    return calendar_rows


def leaderboard_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    """Employee leaderboard for gamification."""

    if dt_fim < dt_ini:
        return []
    return sales_top_employees(role, id_empresa, id_filial, dt_ini, dt_fim, limit=limit)


def monthly_goal_projection(
    role: str,
    id_empresa: int,
    id_filial: Any,
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    requested_as_of = as_of or business_today(id_empresa)
    commercial_coverage = commercial_window_coverage(
        role,
        id_empresa,
        id_filial,
        requested_as_of,
        requested_as_of,
    )
    effective_as_of = commercial_coverage.get("effective_dt_fim") or requested_as_of
    month_start = _month_start(effective_as_of)
    month_end = _next_month_start(month_start) - timedelta(days=1)
    total_days = (month_end - month_start).days + 1
    days_elapsed = (effective_as_of - month_start).days + 1
    remaining_days = max(total_days - days_elapsed, 0)

    historical_end = effective_as_of
    live_bundle = None
    if effective_as_of == requested_as_of == business_today(id_empresa):
        historical_end = effective_as_of - timedelta(days=1)
        live_bundle = sales_operational_day_bundle(role, id_empresa, id_filial, effective_as_of, include_rankings=False)

    daily_rows: List[Dict[str, Any]] = (
        _sales_daily_totals(role, id_empresa, id_filial, month_start, historical_end)
        if historical_end >= month_start
        else []
    )

    daily_map: Dict[date, float] = {
        _date_from_key(row.get("data_key")): float(row.get("faturamento") or 0)
        for row in daily_rows
        if _date_from_key(row.get("data_key")) is not None
    }
    if live_bundle:
        live_value = float((live_bundle.get("kpis") or {}).get("faturamento") or 0)
        daily_map[effective_as_of] = live_value

    series: List[Dict[str, Any]] = []
    cursor = month_start
    while cursor <= effective_as_of:
        value = round(float(daily_map.get(cursor) or 0), 2)
        series.append(
            {
                "date": cursor.isoformat(),
                "data_key": _date_key(cursor),
                "weekday": cursor.strftime("%A"),
                "weekday_index": cursor.weekday(),
                "faturamento": value,
            }
        )
        cursor += timedelta(days=1)

    mtd_actual = round(sum(float(item.get("faturamento") or 0) for item in series), 2)
    avg_daily_mtd = round(mtd_actual / days_elapsed, 2) if days_elapsed > 0 else 0.0
    projection_base = round(mtd_actual + (avg_daily_mtd * remaining_days), 2)

    weekday_history_start = month_start - timedelta(days=84)
    weekday_rows: List[Dict[str, Any]] = (
        _sales_daily_totals(role, id_empresa, id_filial, weekday_history_start, effective_as_of - timedelta(days=1))
        if effective_as_of > weekday_history_start
        else []
    )

    weekday_totals: Dict[int, List[float]] = {}
    for row in weekday_rows:
        row_date = _date_from_key(row.get("data_key"))
        if row_date is None:
            continue
        weekday_totals.setdefault(row_date.weekday(), []).append(float(row.get("faturamento") or 0))

    weekday_observations = sum(len(values) for values in weekday_totals.values())
    weekday_avg: Dict[int, float] = {
        weekday: (sum(values) / len(values))
        for weekday, values in weekday_totals.items()
        if values
    }
    overall_weekday_avg = (
        sum(sum(values) for values in weekday_totals.values()) / weekday_observations
        if weekday_observations > 0
        else 0.0
    )
    weekday_factor: Dict[int, float] = {}
    if overall_weekday_avg > 0 and weekday_observations >= 21:
        for weekday in range(7):
            factor = (weekday_avg.get(weekday) or overall_weekday_avg) / overall_weekday_avg
            weekday_factor[weekday] = max(0.7, min(1.3, factor))

    adjusted_remaining = 0.0
    future_cursor = effective_as_of + timedelta(days=1)
    while future_cursor <= month_end:
        factor = weekday_factor.get(future_cursor.weekday(), 1.0)
        adjusted_remaining += avg_daily_mtd * factor
        future_cursor += timedelta(days=1)
    projection_adjusted = round(mtd_actual + adjusted_remaining, 2) if weekday_factor else projection_base

    branch_clause, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql_goal = f"""
      SELECT
        COALESCE(SUM(target_value), 0)::numeric(18,2) AS target_value,
        COUNT(*)::int AS goal_rows
      FROM app.goals
      WHERE id_empresa = %s
        AND goal_type = 'FATURAMENTO'
        AND goal_date BETWEEN %s AND %s
        {branch_clause}
    """
    goal_row: Dict[str, Any] = {}
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        goal_row = dict(conn.execute(sql_goal, [id_empresa, month_start, month_end] + branch_params).fetchone() or {})

    recent_closed_months = _sales_month_summaries(role, id_empresa, id_filial, month_start, lookback_months=4)
    last_month_rows = recent_closed_months[:3]
    comparison_months = recent_closed_months[:3]
    complete_comparison = len([item for item in comparison_months if bool(item.get("is_complete"))]) >= 3
    comparison_mode = "last_3_complete_months" if complete_comparison else "last_3_available_months"

    target_value = round(float(goal_row.get("target_value") or 0), 2)
    goal_configured = int(goal_row.get("goal_rows") or 0) > 0 and target_value > 0
    average_last_3_months = (
        round(sum(float(row.get("faturamento") or 0) for row in comparison_months) / len(comparison_months), 2)
        if comparison_months
        else 0.0
    )
    required_daily_to_goal = round(max(target_value - mtd_actual, 0) / remaining_days, 2) if remaining_days > 0 and goal_configured else 0.0
    gap_to_goal = round(projection_adjusted - target_value, 2) if goal_configured else None
    variation_vs_goal_pct = round(((projection_adjusted / target_value) - 1) * 100, 2) if goal_configured and target_value > 0 else None
    variation_vs_last_3m_pct = (
        round(((projection_adjusted / average_last_3_months) - 1) * 100, 2)
        if average_last_3_months > 0
        else None
    )

    if commercial_coverage.get("mode") == "shifted_latest":
        status = "latest_compatible"
        headline = (
            f"A base comercial ainda não chegou em {requested_as_of.strftime('%m/%Y')}. "
            f"A projeção mostra a última referência disponível de {effective_as_of.strftime('%m/%Y')}."
        )
    elif goal_configured and projection_adjusted >= target_value:
        status = "above_goal"
        headline = "O ritmo atual projeta fechamento acima da meta mensal."
    elif goal_configured and projection_adjusted < target_value:
        status = "below_goal"
        headline = "O ritmo atual projeta fechamento abaixo da meta mensal."
    elif average_last_3_months > 0 and projection_adjusted >= average_last_3_months:
        status = "above_history"
        headline = "O ritmo atual projeta fechamento acima da média recente."
    else:
        status = "tracking"
        headline = "A projeção usa o ritmo atual do mês como referência principal."

    if weekday_factor and weekday_observations >= 28:
        confidence_level = "high"
        confidence_label = "Alta"
        confidence_reason = "Há base recente suficiente para ajustar o restante do mês pelo padrão de dia da semana."
    elif days_elapsed >= 5:
        confidence_level = "medium"
        confidence_label = "Moderada"
        confidence_reason = "A projeção já usa uma base razoável do mês, mas ainda com pouca profundidade sazonal."
    else:
        confidence_level = "low"
        confidence_label = "Baixa"
        confidence_reason = "O mês ainda tem poucos dias observados; a projeção é mais sensível a oscilações diárias."

    return {
        "month_ref": month_start.isoformat(),
        "month_label": month_start.strftime("%m/%Y"),
        "requested_as_of": requested_as_of.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "requested_month_ref": _month_start(requested_as_of).isoformat(),
        "business_clock": business_clock_payload(id_empresa),
        "status": status,
        "headline": headline,
        "commercial_coverage": commercial_coverage,
        "summary": {
            "mtd_actual": mtd_actual,
            "avg_daily_mtd": avg_daily_mtd,
            "projection_base": projection_base,
            "projection_adjusted": projection_adjusted,
            "remaining_days": remaining_days,
            "days_elapsed": days_elapsed,
            "total_days": total_days,
        },
        "goal": {
            "configured": goal_configured,
            "target_value": target_value,
            "gap_to_goal": gap_to_goal,
            "variation_pct": variation_vs_goal_pct,
            "required_daily_to_goal": required_daily_to_goal,
            "goal_month": month_start.isoformat(),
            "scope_branch_count": int(goal_row.get("goal_rows") or 0),
        },
        "history": {
            "last_3_months": [
                {
                    "month_ref": row.get("month_ref"),
                    "faturamento": round(float(row.get("faturamento") or 0), 2),
                    "observed_days": int(row.get("observed_days") or 0),
                    "expected_days": int(row.get("expected_days") or 0),
                    "completeness_pct": float(row.get("completeness_pct") or 0),
                    "has_data": bool(row.get("has_data")),
                    "is_partial": bool(row.get("is_partial")),
                    "is_complete": bool(row.get("is_complete")),
                }
                for row in last_month_rows
            ],
            "comparison_months": [
                {
                    "month_ref": row.get("month_ref"),
                    "faturamento": round(float(row.get("faturamento") or 0), 2),
                    "observed_days": int(row.get("observed_days") or 0),
                    "expected_days": int(row.get("expected_days") or 0),
                    "is_complete": bool(row.get("is_complete")),
                }
                for row in comparison_months
            ],
            "average_last_3_months": average_last_3_months,
            "variation_vs_last_3m_pct": variation_vs_last_3m_pct,
            "average_basis": comparison_mode,
            "average_basis_note": (
                "A média comparativa usou apenas meses fechados completos para evitar distorção por histórico parcial."
                if comparison_mode == "last_3_complete_months"
                else "A média comparativa precisou usar os meses disponíveis porque não havia três fechamentos completos."
            ),
        },
        "forecast": {
            "method": "mtd_with_weekday_adjustment" if weekday_factor else "mtd_average",
            "weekday_adjustment_applied": bool(weekday_factor),
            "weekday_observations": weekday_observations,
            "weekday_factors": {str(key): round(float(value), 3) for key, value in sorted(weekday_factor.items())},
            "confidence_level": confidence_level,
            "confidence_label": confidence_label,
            "confidence_reason": confidence_reason,
        },
        "series_mtd": series,
        "drivers": [
            f"MTD atual em {_format_brl(mtd_actual)}.",
            f"Ritmo médio de {_format_brl(avg_daily_mtd)} por dia corrido do mês até agora.",
            (
                "Projeção ajustada por padrão de dia da semana."
                if weekday_factor
                else "Projeção linear simples porque ainda não há base sazonal suficiente."
            ),
        ],
    }


# ========================
# Jarvis (rule-based briefing)
# ========================

def _jarvis_hour_label(hour_value: Any) -> str:
    hour = int(hour_value or 0)
    return f"{hour:02d}h"


def _jarvis_peak_guidance(hours: List[int]) -> str:
    if not hours:
        return "Sem janela de pico material na base recente."

    earliest = min(hours)
    latest = max(hours)
    if latest <= 9:
        return "Reforce pista, caixa e atendimento no começo da manhã para absorver a abertura com fila curta e execução limpa."
    if earliest >= 17:
        return "Reforce cobertura de pista, troca de turno e frente de loja no fim do dia, quando o fluxo acelera de novo."
    if earliest <= 11 <= latest:
        return "Garanta cobertura contínua de pista e caixa na virada de almoço, evitando fila e perda de ritmo comercial."
    return "Ajuste escala, atenção de pista e conferência operacional nas horas de maior média recente."


def _jarvis_off_peak_guidance(hours: List[int]) -> str:
    if not hours:
        return "Sem janela ociosa relevante na base recente."

    earliest = min(hours)
    latest = max(hours)
    if latest <= 6:
        return "Use essa janela para checklist, conferência e rotina de abastecimento interno, sem depender de promoção fora de contexto."
    if earliest >= 21:
        return "Reserve essa faixa para fechamento gradual, conferência e rotina operacional, sem criar ação comercial artificial."
    return "Use as horas de menor fluxo para reposição, rotina operacional e ofertas leves que não distorçam margem."


def _jarvis_product_decline_guidance(item: Dict[str, Any]) -> str:
    group_name = _normalize_group_bucket_text(item.get("grupo_nome"))
    product_name = _normalize_group_bucket_text(item.get("produto_nome"))
    if "COMBUST" in group_name or "GASOL" in product_name or "ETANOL" in product_name or "DIESEL" in product_name or "GNV" in product_name:
        return "Revise preço de bomba, ruptura, mix de volume e posição na praça antes que a queda vire perda estrutural."
    return "Revise ruptura, exposição, mix e disciplina comercial do produto antes de perder recorrência da conveniência."


def sales_peak_hours_signal(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ref: date,
) -> Dict[str, Any]:
    effective_ref = commercial_window_coverage(role, id_empresa, id_filial, dt_ref, dt_ref).get("effective_dt_fim") or dt_ref
    closed_end = effective_ref - timedelta(days=1)
    closed_start = closed_end - timedelta(days=29)
    if closed_end < closed_start:
        return {
            "source_status": "unavailable",
            "window_days": 0,
            "dt_ini": None,
            "dt_fim": None,
            "peak_hours": [],
            "off_peak_hours": [],
            "recommendations": {"peak": None, "off_peak": None},
        }

    closed_days = max((closed_end - closed_start).days + 1, 1)
    start_key = _date_key(closed_start)
    end_key = _date_key(closed_end)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, start_key, end_key] + branch_params + [closed_days, closed_days]
    conn_branch_id = _conn_branch_id(id_filial)
    sql = f"""
      WITH hour_dim AS (
        SELECT generate_series(0, 23)::int AS hora
      ), hourly AS (
        SELECT
          hora,
          COALESCE(SUM(faturamento), 0)::numeric(18,2) AS faturamento_total,
          COALESCE(SUM(vendas), 0)::int AS vendas_total
        FROM mart.agg_vendas_hora
        WHERE id_empresa = %s
          AND data_key BETWEEN %s AND %s
          {where_filial}
        GROUP BY hora
      )
      SELECT
        h.hora,
        COALESCE(hourly.faturamento_total, 0)::numeric(18,2) AS faturamento_total,
        COALESCE(hourly.vendas_total, 0)::int AS vendas_total,
        ROUND((COALESCE(hourly.faturamento_total, 0) / %s)::numeric, 2) AS avg_faturamento_dia,
        ROUND((COALESCE(hourly.vendas_total, 0)::numeric / %s), 2) AS avg_vendas_dia
      FROM hour_dim h
      LEFT JOIN hourly
        ON hourly.hora = h.hora
      ORDER BY h.hora
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]

    active_rows = [row for row in rows if float(row.get("avg_faturamento_dia") or 0) > 0]
    baseline_avg = (
        sum(float(row.get("avg_faturamento_dia") or 0) for row in rows) / len(rows)
        if rows
        else 0.0
    )

    def _normalize_hour_row(row: Dict[str, Any]) -> Dict[str, Any]:
        avg_faturamento = round(float(row.get("avg_faturamento_dia") or 0), 2)
        avg_vendas = round(float(row.get("avg_vendas_dia") or 0), 2)
        return {
            "hora": int(row.get("hora") or 0),
            "label": _jarvis_hour_label(row.get("hora")),
            "avg_faturamento_dia": avg_faturamento,
            "avg_vendas_dia": avg_vendas,
            "relative_index": round((avg_faturamento / baseline_avg), 2) if baseline_avg > 0 else 0.0,
        }

    peak_rows = sorted(
        active_rows,
        key=lambda row: (float(row.get("avg_faturamento_dia") or 0), float(row.get("avg_vendas_dia") or 0), -int(row.get("hora") or 0)),
        reverse=True,
    )[:3]
    peak_hours = [_normalize_hour_row(row) for row in peak_rows]
    excluded_hours = {item["hora"] for item in peak_hours}
    off_peak_rows = sorted(
        [row for row in rows if int(row.get("hora") or 0) not in excluded_hours],
        key=lambda row: (float(row.get("avg_faturamento_dia") or 0), float(row.get("avg_vendas_dia") or 0), int(row.get("hora") or 0)),
    )[:3]
    off_peak_hours = [_normalize_hour_row(row) for row in off_peak_rows]

    return {
        "source_status": "ok" if peak_hours or off_peak_hours else "unavailable",
        "window_days": closed_days,
        "dt_ini": closed_start.isoformat(),
        "dt_fim": closed_end.isoformat(),
        "peak_hours": peak_hours,
        "off_peak_hours": off_peak_hours,
        "recommendations": {
            "peak": _jarvis_peak_guidance([item["hora"] for item in peak_hours]),
            "off_peak": _jarvis_off_peak_guidance([item["hora"] for item in off_peak_hours]),
        },
    }


def sales_declining_products_signal(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ref: date,
    *,
    limit: int = 3,
) -> Dict[str, Any]:
    effective_ref = commercial_window_coverage(role, id_empresa, id_filial, dt_ref, dt_ref).get("effective_dt_fim") or dt_ref
    recent_end = effective_ref - timedelta(days=1)
    recent_start = recent_end - timedelta(days=29)
    prior_end = recent_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=29)
    if prior_end < prior_start:
        return {
            "source_status": "unavailable",
            "recent_window": {"dt_ini": None, "dt_fim": None},
            "prior_window": {"dt_ini": None, "dt_fim": None},
            "thresholds": {"min_prior_revenue": 1000.0, "min_absolute_drop": 300.0, "min_decline_pct": -15.0},
            "items": [],
        }

    recent_start_key = _date_key(recent_start)
    recent_end_key = _date_key(recent_end)
    prior_start_key = _date_key(prior_start)
    prior_end_key = _date_key(prior_end)
    where_filial, branch_params = _branch_scope_clause("a.id_filial", id_filial)
    dim_where_filial, dim_branch_params = _branch_scope_clause("p.id_filial", id_filial)
    conn_branch_id = _conn_branch_id(id_filial)
    active_filter = _active_product_filter_expression("p")
    params = [
        recent_start_key,
        recent_end_key,
        recent_start_key,
        recent_end_key,
        prior_start_key,
        prior_end_key,
        prior_start_key,
        prior_end_key,
        id_empresa,
        prior_start_key,
        recent_end_key,
        *branch_params,
        id_empresa,
        *dim_branch_params,
        id_empresa,
        limit,
    ]
    sql = f"""
      WITH aggregated AS (
        SELECT
          a.id_produto,
          MAX(COALESCE(NULLIF(a.produto_nome, ''), '#ID ' || a.id_produto::text)) AS produto_nome,
          COALESCE(SUM(a.faturamento) FILTER (WHERE a.data_key BETWEEN %s AND %s), 0)::numeric(18,2) AS recent_faturamento,
          COALESCE(SUM(a.qtd) FILTER (WHERE a.data_key BETWEEN %s AND %s), 0)::numeric(18,3) AS recent_qtd,
          COALESCE(SUM(a.faturamento) FILTER (WHERE a.data_key BETWEEN %s AND %s), 0)::numeric(18,2) AS prior_faturamento,
          COALESCE(SUM(a.qtd) FILTER (WHERE a.data_key BETWEEN %s AND %s), 0)::numeric(18,3) AS prior_qtd
        FROM mart.agg_produtos_diaria a
        WHERE a.id_empresa = %s
          AND a.data_key BETWEEN %s AND %s
          {where_filial}
        GROUP BY a.id_produto
      ), latest_products AS (
        SELECT DISTINCT ON (p.id_empresa, p.id_produto)
          p.id_empresa,
          p.id_produto,
          {_group_name_expression('g', 'p')} AS grupo_nome
        FROM dw.dim_produto p
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = p.id_empresa
         AND g.id_filial = p.id_filial
         AND g.id_grupo_produto = p.id_grupo_produto
        WHERE p.id_empresa = %s
          {dim_where_filial}
          AND {active_filter}
        ORDER BY
          p.id_empresa,
          p.id_produto,
          p.updated_at DESC NULLS LAST,
          p.created_at DESC NULLS LAST,
          p.id_filial
      )
      SELECT
        a.id_produto,
        a.produto_nome,
        COALESCE(lp.grupo_nome, '(Sem grupo)') AS grupo_nome,
        a.recent_faturamento,
        a.recent_qtd,
        a.prior_faturamento,
        a.prior_qtd,
        (a.prior_faturamento - a.recent_faturamento)::numeric(18,2) AS delta_faturamento,
        CASE
          WHEN a.prior_faturamento <= 0 THEN 0::numeric(18,2)
          ELSE ROUND((((a.recent_faturamento / NULLIF(a.prior_faturamento, 0)) - 1) * 100)::numeric, 2)
        END AS variation_pct
      FROM aggregated a
      LEFT JOIN latest_products lp
        ON lp.id_empresa = %s
       AND lp.id_produto = a.id_produto
      WHERE a.prior_faturamento >= 1000
        AND (a.prior_faturamento - a.recent_faturamento) >= 300
        AND a.recent_faturamento <= (a.prior_faturamento * 0.85)
      ORDER BY delta_faturamento DESC, a.prior_faturamento DESC, a.produto_nome
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=conn_branch_id) as conn:
        rows = [
            dict(row)
            for row in conn.execute(sql, params).fetchall()
        ]

    items = [
        {
            "id_produto": row.get("id_produto"),
            "produto_nome": row.get("produto_nome"),
            "grupo_nome": row.get("grupo_nome"),
            "recent_faturamento": round(float(row.get("recent_faturamento") or 0), 2),
            "prior_faturamento": round(float(row.get("prior_faturamento") or 0), 2),
            "recent_qtd": round(float(row.get("recent_qtd") or 0), 3),
            "prior_qtd": round(float(row.get("prior_qtd") or 0), 3),
            "delta_faturamento": round(float(row.get("delta_faturamento") or 0), 2),
            "variation_pct": round(float(row.get("variation_pct") or 0), 2),
            "recommendation": _jarvis_product_decline_guidance(row),
        }
        for row in rows
    ]

    return {
        "source_status": "ok" if items else "unavailable",
        "recent_window": {"dt_ini": recent_start.isoformat(), "dt_fim": recent_end.isoformat()},
        "prior_window": {"dt_ini": prior_start.isoformat(), "dt_fim": prior_end.isoformat()},
        "thresholds": {"min_prior_revenue": 1000.0, "min_absolute_drop": 300.0, "min_decline_pct": -15.0},
        "items": items,
    }


def jarvis_briefing(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ref: date,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a premium rule-based operational copilot for the home."""

    dt_ini = dt_ref - timedelta(days=6)
    risk = context.get("modeled_risk") if context else None
    try:
        if not isinstance(risk, dict):
            risk = risk_kpis(role, id_empresa, id_filial, dt_ini, dt_ref)
    except SNAPSHOT_FALLBACK_ERRORS:
        risk = {}
    except TimeoutError:
        risk = {}

    try:
        risk_focus = (risk_by_turn_local(role, id_empresa, id_filial, dt_ini, dt_ref, limit=1) or [None])[0]
    except SNAPSHOT_FALLBACK_ERRORS:
        risk_focus = None
    except TimeoutError:
        risk_focus = None
    sales = context.get("sales") if context else None
    if not isinstance(sales, dict):
        sales = sales_overview_bundle(role, id_empresa, id_filial, dt_ini, dt_ref, as_of=dt_ref)
    cash_live = context.get("cash_live") if context else None
    if not isinstance(cash_live, dict):
        cash_live = _cash_live_now(role, id_empresa, id_filial)

    finance = context.get("finance_aging") if context else None
    if not isinstance(finance, dict):
        finance = finance_aging_overview(role, id_empresa, id_filial, as_of=dt_ref)

    churn_bundle = context.get("churn") if context else None
    if isinstance(churn_bundle, dict):
        churn = churn_bundle.get("top_risk") or []
    else:
        churn = customers_churn_diamond(role, id_empresa, id_filial, as_of=dt_ref, min_score=40, limit=5)

    payments = context.get("payments") if context else None
    if not isinstance(payments, dict):
        payments = payments_overview(role, id_empresa, id_filial, dt_ini, dt_ref, anomaly_limit=5)

    fraud_operational = context.get("fraud_operational") if context else None
    if not isinstance(fraud_operational, dict):
        fraud_operational = fraud_kpis(role, id_empresa, id_filial, dt_ini, dt_ref)

    pricing_branch_id = _conn_branch_id(id_filial)
    pricing = (
        competitor_pricing_overview(role, id_empresa, pricing_branch_id, dt_ini=dt_ini, dt_fim=dt_ref, days_simulation=10)
        if pricing_branch_id is not None
        else None
    )

    cash_kpis = cash_live.get("kpis") or {}
    receiving_overdue = float(finance.get("receber_total_vencido") or 0)
    paying_overdue = float(finance.get("pagar_total_vencido") or 0)
    overdue_pressure = receiving_overdue + paying_overdue
    top_churn = churn[0] if churn else None
    churn_impact = sum(float(item.get("revenue_at_risk_30d") or 0) for item in churn[:5])
    payments_kpis = payments.get("kpis") or {}
    payment_anomaly = (payments.get("anomalies") or [None])[0]
    fraud_impact = float(fraud_operational.get("valor_cancelado") or 0)
    fraud_cancelamentos = int(fraud_operational.get("cancelamentos") or 0)
    pricing_summary = pricing.get("summary") if isinstance(pricing, dict) else {}
    pricing_items = pricing.get("items") if isinstance(pricing, dict) else []
    pricing_impact = float(pricing_summary.get("total_lost_if_no_change_10d") or 0)
    pricing_focus = None
    if pricing_items:
        pricing_focus = max(
            pricing_items,
            key=lambda item: float(item.get("scenario_no_change", {}).get("lost_revenue_10d") or 0),
        )
    signal_context = context.get("signals") if context else None
    if isinstance(signal_context, dict):
        peak_hours_signal = signal_context.get("peak_hours") if isinstance(signal_context.get("peak_hours"), dict) else {}
        declining_products_signal = (
            signal_context.get("declining_products")
            if isinstance(signal_context.get("declining_products"), dict)
            else {}
        )
    else:
        peak_hours_signal = sales_peak_hours_signal(role, id_empresa, id_filial, dt_ref)
        declining_products_signal = sales_declining_products_signal(role, id_empresa, id_filial, dt_ref)
    signals = {
        "peak_hours": peak_hours_signal,
        "declining_products": declining_products_signal,
    }

    candidates: List[Dict[str, Any]] = []

    if int(cash_kpis.get("caixas_criticos") or 0) > 0:
        focus_box = (cash_live.get("open_boxes") or [None])[0]
        candidates.append(
            {
                "kind": "cash",
                "weight": 1000 + float(cash_kpis.get("total_vendas_abertas") or 0),
                "impact_value": float(cash_kpis.get("total_vendas_abertas") or 0),
                "priority": "Imediatamente",
                "headline": f"Revisar imediatamente {int(cash_kpis.get('caixas_criticos') or 0)} caixa(s) aberto(s) fora da janela segura.",
                "cause": "Caixa aberto há mais de 24 horas aumenta risco operacional, posterga fechamento e expõe cancelamentos sem revisão.",
                "action": "Validar fechamento do caixa mais antigo, confirmar operador responsável e conciliar vendas e cancelamentos ainda hoje.",
                "evidence": [
                    _filial_label(focus_box.get("id_filial"), focus_box.get("filial_nome")) if focus_box else None,
                    (
                        focus_box.get("turno_label")
                        if focus_box and str(focus_box.get("turno_label") or "").lower().startswith("turno ")
                        else (f"Turno {focus_box.get('turno_label')}" if focus_box and focus_box.get("turno_label") else None)
                    ),
                    f"{round(float(focus_box.get('horas_aberto') or 0), 1)}h aberto" if focus_box else None,
                    f"Vendas expostas: {_format_brl(cash_kpis.get('total_vendas_abertas'))}",
                ],
            }
        )

    if overdue_pressure > 0:
        priority = "Hoje" if receiving_overdue > 0 else "Acompanhar"
        headline = (
            "Cobrar hoje os vencidos mais concentrados para aliviar a pressão de caixa."
            if receiving_overdue >= paying_overdue
            else "Reprogramar compromissos vencidos antes que a pressão financeira avance."
        )
        cause = (
            "A carteira vencida concentra recursos que já deveriam estar no caixa."
            if receiving_overdue >= paying_overdue
            else "As obrigações vencidas já consomem capacidade de caixa e aumentam a pressão financeira do período."
        )
        action = (
            "Ativar régua de cobrança nos maiores títulos vencidos, priorizando a filial com maior concentração e clientes de maior valor."
            if receiving_overdue >= paying_overdue
            else "Renegociar os maiores vencidos e reordenar pagamentos para proteger o caixa operacional desta semana."
        )
        candidates.append(
            {
                "kind": "finance",
                "weight": overdue_pressure,
                "impact_value": overdue_pressure,
                "priority": priority,
                "headline": headline,
                "cause": cause,
                "action": action,
                "evidence": [
                    f"Receber vencido: {_format_brl(receiving_overdue)}",
                    f"Pagar vencido: {_format_brl(paying_overdue)}",
                    f"Top 5 concentram {float(finance.get('top5_concentration_pct') or 0):.1f}% da carteira",
                ],
            }
        )

    if float(payments_kpis.get("unknown_valor") or 0) > 0 or payment_anomaly:
        candidates.append(
            {
                "kind": "payments",
                "weight": float(payment_anomaly.get("impacto_estimado") or 0) if payment_anomaly else float(payments_kpis.get("unknown_valor") or 0),
                "impact_value": float(payment_anomaly.get("impacto_estimado") or 0) if payment_anomaly else float(payments_kpis.get("unknown_valor") or 0),
                "priority": "Hoje" if payment_anomaly else "Acompanhar",
                "headline": "Revisar meios de pagamento fora do padrão antes do próximo fechamento.",
                "cause": "A taxonomia oficial de pagamentos já foi aplicada, mas o recorte ainda mostra anomalia ou valores sem identificação comercial.",
                "action": "Abrir o bloco de pagamentos, validar o turno mais exposto e corrigir a origem dos meios não identificados ainda neste ciclo.",
                "evidence": [
                    f"Não identificado: {_format_brl(payments_kpis.get('unknown_valor'))}",
                    payment_anomaly.get("event_label") if payment_anomaly else None,
                    payment_anomaly.get("turno_label") if payment_anomaly else None,
                ],
            }
        )

    if fraud_impact > 0 or float(risk.get("impacto_total") or 0) > 0:
        modeled_impact = float(risk.get("impacto_total") or 0)
        candidates.append(
            {
                "kind": "fraud",
                "weight": fraud_impact + modeled_impact + (int(risk.get("eventos_alto_risco") or 0) * 500),
                "impact_value": max(fraud_impact, modeled_impact),
                "priority": "Imediatamente" if int(risk.get("eventos_alto_risco") or 0) >= 5 else "Hoje",
                "headline": "Auditar cancelamentos e descontos relevantes antes do próximo fechamento.",
                "cause": (
                    "Os cancelamentos operacionais do período já são materiais e pedem auditoria de turno, operador e justificativa."
                    if fraud_impact >= modeled_impact
                    else "A modelagem de risco encontrou concentração relevante em cancelamentos, descontos e recompras rápidas."
                ),
                "action": "Abrir o antifraude, revisar o turno mais sensível e validar o colaborador mais exposto ainda neste ciclo.",
                "evidence": [
                    f"{fraud_cancelamentos} cancelamento(s) somando {_format_brl(fraud_impact)}",
                    f"{int(risk.get('eventos_alto_risco') or 0)} evento(s) de alto risco" if modeled_impact > 0 else None,
                    _filial_label(risk_focus.get("id_filial"), risk_focus.get("filial_nome")) if risk_focus else None,
                    risk_focus.get("turno_label") if risk_focus else None,
                ],
            }
        )

    if churn_impact > 0:
        candidates.append(
            {
                "kind": "churn",
                "weight": churn_impact,
                "impact_value": churn_impact,
                "priority": "Hoje",
                "headline": "Ativar a recuperação dos clientes que já saíram do padrão de retorno.",
                "cause": "A queda de frequência e o intervalo acima do ciclo esperado já colocam receita recorrente em risco.",
                "action": "Acionar os clientes mais relevantes com contato comercial e oferta aderente antes do próximo ciclo de compra.",
                "evidence": [
                    top_churn.get("cliente_nome") if top_churn else None,
                    f"Receita em risco: {_format_brl(churn_impact)}",
                    f"{len(churn)} cliente(s) prioritário(s) na fila de reativação",
                ],
            }
        )

    if pricing_impact > 0 and pricing_focus:
        candidates.append(
            {
                "kind": "pricing",
                "weight": pricing_impact,
                "impact_value": pricing_impact,
                "priority": "Acompanhar",
                "headline": f"Ajustar o preço de {pricing_focus.get('produto_nome')} para reduzir perda competitiva.",
                "cause": "O cenário competitivo indica perda de volume ou margem se o preço atual continuar desalinhado com a praça.",
                "action": "Revisar o preço do combustível líder da simulação e decidir se vale igualar, proteger margem ou reposicionar a oferta.",
                "evidence": [
                    _filial_label(pricing_branch_id),
                    pricing_focus.get("produto_nome"),
                    f"Perda em 10 dias: {_format_brl(pricing_focus.get('scenario_no_change', {}).get('lost_revenue_10d'))}",
                ],
            }
        )

    decline_items = declining_products_signal.get("items") if isinstance(declining_products_signal, dict) else []
    top_decline = decline_items[0] if decline_items else None
    if top_decline:
        delta_faturamento = float(top_decline.get("delta_faturamento") or 0)
        variation_pct = float(top_decline.get("variation_pct") or 0)
        candidates.append(
            {
                "kind": "sales",
                "weight": round(delta_faturamento * 0.25, 2),
                "impact_value": delta_faturamento,
                "priority": "Hoje" if delta_faturamento >= 1500 or variation_pct <= -25 else "Acompanhar",
                "headline": f"Revisar a queda recente de {top_decline.get('produto_nome')} antes que a perda ganhe escala.",
                "cause": (
                    f"O produto saiu de {_format_brl(top_decline.get('prior_faturamento'))} para {_format_brl(top_decline.get('recent_faturamento'))} "
                    f"na comparação das últimas duas janelas de 30 dias."
                ),
                "action": top_decline.get("recommendation"),
                "evidence": [
                    top_decline.get("produto_nome"),
                    top_decline.get("grupo_nome"),
                    f"Queda de {_format_brl(delta_faturamento)}",
                    f"{variation_pct:.1f}% vs janela anterior",
                ],
            }
        )

    churn_snapshot_meta = churn_bundle.get("snapshot_meta") if isinstance(churn_bundle, dict) else {}
    finance_status = str(finance.get("snapshot_status") or "").lower()
    churn_status = str(churn_snapshot_meta.get("snapshot_status") or "").lower()
    payments_status = str(payments_kpis.get("source_status") or "").lower()
    cash_live_status = str(cash_live.get("source_status") or "").lower()
    sales_reading_status = str(sales.get("reading_status") or "").lower()
    sales_freshness_mode = str((sales.get("freshness") or {}).get("mode") or "").lower()
    confidence_score = 3
    confidence_reasons: List[str] = []

    if sales_freshness_mode not in {"hybrid_live", "snapshot_only"}:
        confidence_score -= 2
        confidence_reasons.append("vendas ainda não confirmaram a trilha operacional")
    elif sales_reading_status != "operational_overlay" and dt_ref == business_today(id_empresa):
        confidence_score -= 1
        confidence_reasons.append("vendas do dia ainda dependem só da publicação analítica")

    if finance_status in {"missing", ""}:
        confidence_score -= 2
        confidence_reasons.append("financeiro ainda está sendo atualizado")
    elif finance_status not in {"exact", "best_effort", "operational"}:
        confidence_score -= 1
        confidence_reasons.append("financeiro ainda usa a melhor base disponível")

    if churn_status in {"missing", ""}:
        confidence_score -= 2
        confidence_reasons.append("clientes ainda estão em atualização")
    elif churn_status == "operational_current":
        confidence_score -= 1
        confidence_reasons.append("clientes usam a leitura mais recente disponível")

    if payments_status in {"unavailable", "value_gap"}:
        confidence_score -= 1
        confidence_reasons.append("pagamentos ainda estão fechando")

    if cash_live_status == "unavailable":
        confidence_score -= 1
        confidence_reasons.append("monitor de caixa ainda não fechou a leitura")

    if confidence_score >= 3:
        confidence_label = "Alta"
        confidence_level = "high"
        confidence_reason = "Base pronta e coerente para orientar a decisão deste recorte."
    elif confidence_score >= 1:
        confidence_label = "Moderada"
        confidence_level = "medium"
        confidence_reason = "A leitura já orienta a prioridade, mas alguns blocos ainda usam a melhor base disponível."
    else:
        confidence_label = "Baixa"
        confidence_level = "low"
        confidence_reason = (
            "Parte da leitura ainda está em atualização; valide a prioridade com a operação local antes de agir."
            if not confidence_reasons
            else f"Parte da leitura ainda está em atualização: {', '.join(confidence_reasons)}."
        )

    if not candidates:
        return {
            "title": "Copiloto operacional",
            "data_ref": dt_ref.isoformat(),
            "status": "ok",
            "headline": "Operação estável no recorte atual, sem foco crítico acima da linha de corte.",
            "summary": "O momento pede disciplina de execução e acompanhamento dos indicadores líderes, sem ruptura relevante no período.",
            "priority": "Acompanhar",
            "impact_value": 0.0,
            "impact_label": "Sem exposição crítica material",
            "problem": "Sem frente crítica acima da linha de corte.",
            "cause": "Fraude, caixa, clientes e financeiro seguiram dentro da faixa esperada.",
            "action": "Sustentar o ritmo comercial, proteger margem e manter a rotina de acompanhamento diário.",
            "confidence_label": confidence_label,
            "confidence_level": confidence_level,
            "confidence_reason": confidence_reason,
            "data_freshness": {
                "sales": sales.get("freshness"),
                "cash": cash_live.get("freshness"),
            },
            "primary_kind": None,
            "primary_shortcut": None,
            "evidence": ["Sem alertas críticos acima do corte", "Ciclo operacional dentro da faixa esperada"],
            "secondary_focus": [],
            "signals": signals,
            "highlights": ["A operação seguiu estável no recorte.", "Nenhum risco material superou a linha de intervenção imediata."],
        }

    candidates.sort(key=lambda item: float(item.get("weight") or 0), reverse=True)
    primary = candidates[0]
    secondary = candidates[1:3]
    status = "critical" if primary.get("priority") == "Imediatamente" else ("warn" if primary.get("priority") == "Hoje" else "ok")

    return {
        "title": "Copiloto operacional",
        "data_ref": dt_ref.isoformat(),
        "status": status,
        "headline": primary["headline"],
        "summary": primary["cause"],
        "priority": primary["priority"],
        "impact_value": round(float(primary.get("impact_value") or 0), 2),
        "impact_label": f"{_format_brl(primary.get('impact_value'))} em jogo",
        "problem": primary["headline"],
        "cause": primary["cause"],
        "action": primary["action"],
        "confidence_label": confidence_label,
        "confidence_level": confidence_level,
        "confidence_reason": confidence_reason,
        "data_freshness": {
            "sales": sales.get("freshness"),
            "cash": cash_live.get("freshness"),
        },
        "primary_kind": primary.get("kind"),
        "primary_shortcut": _jarvis_shortcut(primary.get("kind")),
        "evidence": [item for item in primary.get("evidence", []) if item],
        "secondary_focus": [
            {
                "kind": item.get("kind"),
                "label": item["headline"],
                "impact_label": _format_brl(item.get("impact_value")),
                "priority": item["priority"],
                "shortcut_path": (_jarvis_shortcut(item.get("kind")) or {}).get("path"),
                "shortcut_label": (_jarvis_shortcut(item.get("kind")) or {}).get("label"),
            }
            for item in secondary
        ],
        "signals": signals,
        "highlights": [
            primary["action"],
            *[item["headline"] for item in secondary],
        ][:3],
    }


# ========================
# Notifications
# ========================

def notifications_list(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    limit: int = 30,
    unread_only: bool = False,
) -> List[Dict[str, Any]]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    where_unread = "AND read_at IS NULL" if unread_only else ""
    params = [id_empresa] + branch_params + [limit]
    sql = f"""
      SELECT id, id_filial, severity, title, body, url, created_at, read_at
      FROM app.notifications
      WHERE id_empresa = %s
        {where_filial}
        {where_unread}
      ORDER BY created_at DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def notifications_unread_count(role: str, id_empresa: int, id_filial: Optional[int]) -> int:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa] + branch_params
    sql = f"""
      SELECT COALESCE(COUNT(*),0)::int AS total
      FROM app.notifications
      WHERE id_empresa = %s
        {where_filial}
        AND read_at IS NULL
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone() or {"total": 0}
    return int(row["total"])


def notification_mark_read(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    notification_id: int,
) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, notification_id] + branch_params
    sql = f"""
      UPDATE app.notifications
      SET read_at = COALESCE(read_at, now())
      WHERE id_empresa = %s
        AND id = %s
        {where_filial}
      RETURNING id, read_at
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()
    return row or {"id": notification_id, "read_at": None}
