from __future__ import annotations

"""Repositories (SQL access) for MART/DW.

PT-BR: Este módulo concentra queries de leitura para dashboards.
EN   : This module centralizes read queries for dashboards.

Design:
- Prefer reading from `mart.*` (materialized views) for performance.
- When something is not in MART yet, we read from `dw.*` facts/dims.
"""

from datetime import date, timedelta
from typing import Optional, List, Dict, Any, Tuple
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
from app.filial_apelido import apelido_for
from app.sales_semantics import (
    CANCELLATION_STATUS,
    COMMERCIAL_STATUSES,
    IGNORED_BUSINESS_STATUS,
    RETURN_STATUS,
    SALE_STATUS,
    cash_net_value,
    comercial_cfop_class_sql,
    comercial_cfop_direction_sql,
    comercial_cfop_numeric_sql,
    commercial_eligible_sql,
    sales_cfop_filter_sql,
    sales_status_filter_sql,
    sales_status_sql,
)

logger = logging.getLogger(__name__)


LOCAL_VENDA_LABELS = {
    -1: "Canal sem cadastro",
    1: "Pista",
    2: "Loja de conveniência",
    3: "Serviços",
}

COMMERCIAL_CFOP_LABELS = {
    "saida_normal": "Vendas normais",
    "entrada_normal": "Entradas registradas",
    "devolucao_saida": "Devoluções de venda",
    "devolucao_entrada": "Devoluções de entrada",
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
    apelido = apelido_for(id_filial)
    if apelido:
        return apelido
    nome = str(filial_nome or "").strip()
    if nome:
        return nome
    if id_filial is None:
        return "Todas as filiais"
    return "Filial sem cadastro"


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
        NULLIF(NULLIF(trim({payload_expr}->>'TURNO'), ''), '0'),
        NULLIF(NULLIF(trim({payload_expr}->>'NO_TURNO'), ''), '0'),
        NULLIF(NULLIF(trim({payload_expr}->>'NUMTURNO'), ''), '0'),
        NULLIF(NULLIF(trim({payload_expr}->>'NR_TURNO'), ''), '0'),
        NULLIF(NULLIF(trim({payload_expr}->>'NROTURNO'), ''), '0'),
        NULLIF(NULLIF(trim({payload_expr}->>'TURNO_CAIXA'), ''), '0'),
        NULLIF(NULLIF(trim({payload_expr}->>'TURNOCAIXA'), ''), '0')
      )
    """


def _turno_label(turno_value: Any, id_turno: Any = None) -> str:
    value = str(turno_value or "").strip()
    if value and value != "0":
        return value
    return "Turno sem cadastro"


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
    return "Equipe sem cadastro"


def _cash_operator_label(usuario_nome: Any, id_usuario: Any = None) -> str:
    nome = str(usuario_nome or "").strip()
    if nome:
        return nome
    return "Operador sem cadastro"


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
            "formula": "Soma dos pagamentos conciliados no período.",
            "source": "mart.agg_pagamentos_turno / dw.fact_pagamento_comprovante",
            "impact": "Mostra por onde o dinheiro entrou e sustenta conferência com caixa.",
        },
        "payments_unknown_share": {
            "label": "Pagamentos sem classificação",
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
            f"O período solicitado vai até {requested_dt_fim.isoformat()}, mas a última base comercial disponível "
            f"vai até {latest_available_dt.isoformat()}. A tela usa o último período comparável entre "
            f"{effective_dt_ini.isoformat()} e {effective_dt_fim.isoformat()}."
        )
    elif requested_dt_fim > latest_available_dt:
        effective_dt_ini = requested_dt_ini
        effective_dt_fim = latest_available_dt
        mode = "partial_requested"
        message = (
            f"A base comercial canônica cobre este período apenas até {latest_available_dt.isoformat()}. "
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


def risk_model_coverage(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    risk_window = risk_data_window(role, id_empresa, id_filial)
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
        message = "A leitura modelada não cobre este período. Os eventos operacionais continuam válidos para o período."

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
      SELECT id_filial,
             COALESCE(NULLIF(btrim(apelido), ''), nome) AS nome,
             nome AS nome_completo,
             apelido
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
            # DISABLED (2026-05-05): jarvis_briefing calls competitor_pricing_overview which takes 2-4s,
            # causing dashboard_home to hang indefinitely. Will re-enable after caching competitor_pricing.
            "jarvis": {},
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
          "cancelamentos_periodo": total_cancelamentos,
            "qtd_cancelamentos": int(summary_row.get("qtd_cancelamentos") or 0),
            "total_entradas": total_entradas,
            "total_pagamentos": total_pagamentos,
          "recebimentos_periodo": total_pagamentos,
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
        0::numeric(18,2) AS devolucoes,
        COUNT(DISTINCT si.doc_key)::int AS vendas,
        (
          SELECT MAX(COALESCE(venda_updated_at, item_updated_at, venda_created_at, item_created_at, data))
            FROM sale_items
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
        0::numeric(18,2) AS devolucoes,
        COUNT(DISTINCT si.doc_key)::int AS vendas,
        (
          SELECT MAX(COALESCE(venda_updated_at, item_updated_at, venda_created_at, item_created_at, data))
            FROM sale_items
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


def sales_ticket_combustivel(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, **kwargs: Any) -> Dict[str, Any]:
    """Ticket medio de COMBUSTIVEL — lê ClickHouse ``mart_ticket_combustivel_diaria``."""
    from app.db_clickhouse import query_dict

    branch_ids = None
    if id_filial is not None and int(id_filial) != -1:
        branch_ids = [int(id_filial)] if not isinstance(id_filial, (list, tuple, set)) else [
            int(v) for v in id_filial if v is not None and int(v) != -1
        ]
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
    }
    filial_sql = ""
    if branch_ids:
        if len(branch_ids) == 1:
            filial_sql = "AND id_filial = %(id_filial)s"
            params["id_filial"] = branch_ids[0]
        else:
            filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)
    try:
        rows = query_dict(
            f"""
            SELECT
              sum(valor_total) AS valor_total,
              sum(qtd_abastecimentos) AS qtd_abastecimentos,
              sum(litros_total) AS litros_total
            FROM torqmind_mart_rt.mart_ticket_combustivel_diaria FINAL
            WHERE id_empresa = %(id_empresa)s
              AND data_ref BETWEEN toDate(%(dt_ini)s) AND toDate(%(dt_fim)s)
              {filial_sql}
            """,
            params,
        )
        row = rows[0] if rows else {}
        if float(row.get("qtd_abastecimentos") or 0) > 0 or float(row.get("valor_total") or 0) > 0:
            valor = float(row.get("valor_total") or 0)
            qtd = int(row.get("qtd_abastecimentos") or 0)
            litros = float(row.get("litros_total") or 0)
            return {
                "ticket_medio": round(valor / qtd, 2) if qtd else 0.0,
                "valor_total": round(valor, 2),
                "qtd_abastecimentos": qtd,
                "litros_total": round(litros, 3),
                "preco_medio_litro": round(valor / litros, 3) if litros else 0.0,
                "source": "clickhouse",
            }
    except Exception as exc:
        logging.getLogger(__name__).warning("sales_ticket_combustivel CH failed: %s", str(exc)[:200])

    # Fallback legado PG (só se CH vazio)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql = f"""
      SELECT
        COALESCE(SUM(valor_total), 0)::numeric(18,2) AS valor_total,
        COALESCE(SUM(qtd_abastecimentos), 0)::int AS qtd_abastecimentos,
        COALESCE(SUM(litros_total), 0)::numeric(18,3) AS litros_total
      FROM mart.ticket_combustivel_diaria
      WHERE id_empresa = %s AND data_ref BETWEEN %s AND %s
        {where_filial}
    """
    params_pg = [id_empresa, dt_ini, dt_fim] + branch_params
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, params_pg).fetchone() or {}
    valor = float(row.get("valor_total") or 0)
    qtd = int(row.get("qtd_abastecimentos") or 0)
    litros = float(row.get("litros_total") or 0)
    return {
        "ticket_medio": round(valor / qtd, 2) if qtd else 0.0,
        "valor_total": round(valor, 2),
        "qtd_abastecimentos": qtd,
        "litros_total": round(litros, 3),
        "preco_medio_litro": round(valor / litros, 3) if litros else 0.0,
        "source": "postgres",
    }


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


def sales_abc_curve(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, **kwargs) -> Dict[str, Any]:
    """ABC curve from PostgreSQL mart (fallback).

    ``id_grupos`` (opcional): lista de id_grupo_produto para restringir a curva
    a grupos específicos escolhidos pelo usuário (multi-seleção na tela). Quando
    vazio/None, considera todos os grupos que já compõem a curva. A resposta
    inclui ``groups`` (grupos disponíveis no período) para montar o seletor.
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    id_grupos = kwargs.get("id_grupos") or None
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)

    group_filter = ""
    if id_grupos:
        group_filter = (
            " AND id_produto IN ("
            " SELECT DISTINCT id_produto FROM dw.dim_produto"
            " WHERE id_empresa = %s AND id_grupo_produto = ANY(%s))"
        )
        params = [id_empresa, ini, fim] + branch_params + [id_empresa, list(id_grupos)]
    else:
        params = [id_empresa, ini, fim] + branch_params
    sql = f"""
      WITH base AS (
        SELECT
          id_produto,
          MAX(produto_nome) AS nome_produto,
          MAX(grupo_nome) AS nome_grupo,
          SUM(faturamento)::numeric(18,2) AS faturamento,
          SUM(qtd)::numeric(18,3) AS qtd,
          SUM(custo_total)::numeric(18,2) AS custo_total,
          (SUM(faturamento) - SUM(custo_total))::numeric(18,2) AS margem,
          CASE WHEN SUM(qtd) > 0 THEN (SUM(faturamento) / SUM(qtd))::numeric(18,4) ELSE 0 END AS valor_unitario_medio
        FROM mart.agg_produtos_diaria
        WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
        {where_filial}
        {group_filter}
        AND faturamento > 0
        GROUP BY id_produto
        HAVING SUM(faturamento) > 0
      ),
      ranked AS (
        SELECT *,
          ROW_NUMBER() OVER (ORDER BY faturamento DESC, id_produto) AS posicao,
          faturamento / NULLIF(SUM(faturamento) OVER (), 0) * 100 AS participacao_pct,
          SUM(faturamento) OVER (ORDER BY faturamento DESC, id_produto ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(faturamento) OVER (), 0) * 100 AS acumulado_pct
        FROM base
      )
      SELECT *,
        CASE WHEN acumulado_pct <= 80 THEN 'A' WHEN acumulado_pct <= 95 THEN 'B' ELSE 'C' END AS classe_abc
      FROM ranked
      ORDER BY posicao
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
        groups_params = [id_empresa, ini, fim] + branch_params
        groups_sql = f"""
          SELECT id_grupo_produto, MAX(grupo_nome) AS grupo_nome, SUM(faturamento)::numeric(18,2) AS faturamento
          FROM mart.agg_grupos_diaria
          WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
          {where_filial}
          AND id_grupo_produto <> -1
          GROUP BY id_grupo_produto
          HAVING SUM(faturamento) > 0
          ORDER BY faturamento DESC
        """
        try:
            available_groups = [
                {
                    "id_grupo_produto": int(g["id_grupo_produto"]),
                    "grupo_nome": g.get("grupo_nome") or "(Sem grupo)",
                    "faturamento": float(g.get("faturamento") or 0),
                }
                for g in conn.execute(groups_sql, groups_params).fetchall()
            ]
        except Exception:
            available_groups = []

    if not rows:
        return {
            "summary": {"total_produtos": 0, "total_faturamento": 0, "classe_a_count": 0, "classe_a_pct": 0, "classe_b_count": 0, "classe_b_pct": 0, "classe_c_count": 0, "classe_c_pct": 0, "produto_lider": "", "produto_lider_pct": 0, "produto_lider_faturamento": 0, "concentration": "empty", "concentration_text": ""},
            "chart_data": [], "ranking": [], "insights": [], "thresholds": {"a": 80, "b": 95, "c": 100}, "groups": available_groups, "selected_groups": list(id_grupos) if id_grupos else [], "source": "postgres", "empty": True,
        }

    total_fat = sum(float(r.get("faturamento") or 0) for r in rows)
    class_a = [r for r in rows if r.get("classe_abc") == "A"]
    class_b = [r for r in rows if r.get("classe_abc") == "B"]
    class_c = [r for r in rows if r.get("classe_abc") == "C"]
    pct_a = sum(float(r.get("faturamento") or 0) for r in class_a) / total_fat * 100 if total_fat else 0
    pct_b = sum(float(r.get("faturamento") or 0) for r in class_b) / total_fat * 100 if total_fat else 0
    pct_c = sum(float(r.get("faturamento") or 0) for r in class_c) / total_fat * 100 if total_fat else 0
    leader = rows[0]
    leader_pct = float(leader.get("participacao_pct") or 0)
    top5_pct = sum(float(r.get("participacao_pct") or 0) for r in rows[:5])

    if top5_pct >= 70:
        conc, conc_text = "high", f"Alta concentração: 5 produtos representam {top5_pct:.1f}% do faturamento."
    elif len(class_c) > 50 and pct_c < 10:
        conc, conc_text = "dispersed", f"Mix pulverizado: Classe C tem {len(class_c)} produtos com apenas {pct_c:.1f}% do faturamento."
    else:
        conc, conc_text = "healthy", "Concentração saudável do portfólio de produtos."

    def _row_to_chart(r):
        return {"posicao": int(r.get("posicao") or 0), "nome_produto": r.get("nome_produto") or "", "nome_grupo": r.get("nome_grupo") or "", "faturamento": float(r.get("faturamento") or 0), "qtd": float(r.get("qtd") or 0), "valor_unitario_medio": float(r.get("valor_unitario_medio") or 0), "participacao_pct": round(float(r.get("participacao_pct") or 0), 2), "acumulado_pct": round(float(r.get("acumulado_pct") or 0), 2), "classe_abc": r.get("classe_abc") or "C"}

    def _row_to_ranking(r):
        return {**_row_to_chart(r), "id_produto": r.get("id_produto"), "unidade": "", "quantity_kind": "unit", "custo_total": float(r.get("custo_total") or 0), "margem": float(r.get("margem") or 0)}

    return {
        "summary": {"total_produtos": len(rows), "total_faturamento": total_fat, "classe_a_count": len(class_a), "classe_a_pct": round(pct_a, 1), "classe_b_count": len(class_b), "classe_b_pct": round(pct_b, 1), "classe_c_count": len(class_c), "classe_c_pct": round(pct_c, 1), "produto_lider": leader.get("nome_produto") or "", "produto_lider_pct": round(leader_pct, 1), "produto_lider_faturamento": float(leader.get("faturamento") or 0), "concentration": conc, "concentration_text": conc_text},
        "chart_data": [_row_to_chart(r) for r in rows[:40]],
        "ranking": [_row_to_ranking(r) for r in rows],
        "insights": [],
        "thresholds": {"a": 80, "b": 95, "c": 100},
        "groups": available_groups,
        "selected_groups": list(id_grupos) if id_grupos else [],
        "source": "postgres",
    }


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

def _competitor_fuel_family(produto_nome: str, grupo_nome: str) -> Optional[str]:
    text = f"{produto_nome or ''} {grupo_nome or ''}".upper()
    if "GASOL" in text:
        return "GASOLINA"
    if "ETANOL" in text or "ALCOOL" in text or "ÁLCOOL" in text:
        return "ETANOL"
    if "DIESEL" in text or "S10" in text or "S500" in text:
        return "DIESEL"
    if "GNV" in text or "GAS NATURAL" in text:
        return "GNV"
    return None


def competitor_pricing_overview(
    role: str,
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    days_simulation: int = 10,
) -> Dict[str, Any]:
    """Simulação de preço vs concorrente.

    Vendas/produto: ClickHouse ``sales_products_rt`` (+ custo em ``dim_produto``).
    Preço concorrente: ``app.competitor_fuel_prices`` (OLTP — escrita do usuário).
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    days_window = max((dt_fim - dt_ini).days + 1, 1)
    days_sim = max(days_simulation, 1)
    fuel_filter = _fuel_filter_expression("g", "p")
    active_filter = _active_product_filter_expression("p")

    rows: list = []
    source = "postgres"

    # Preços digitados pelo usuário (OLTP) — sempre PG.
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        comp_rows = list(
            conn.execute(
                """
                SELECT id_produto, competitor_price::numeric(18,4) AS competitor_price, updated_at
                FROM app.competitor_fuel_prices
                WHERE id_empresa = %s AND id_filial = %s
                """,
                [id_empresa, id_filial],
            ).fetchall()
        )
    comp_by_prod = {
        int(r["id_produto"]): r for r in comp_rows if r.get("id_produto") is not None
    }

    try:
        from app.db_clickhouse import query_dict

        sales_rows = query_dict(
            """
            SELECT
              id_produto,
              any(nome_produto) AS produto_nome,
              any(nome_grupo) AS grupo_nome,
              sum(qtd) AS qtd_periodo,
              sum(faturamento) AS faturamento_periodo
            FROM torqmind_mart_rt.sales_products_rt FINAL
            WHERE id_empresa = %(id_empresa)s
              AND id_filial = %(id_filial)s
              AND data_key BETWEEN %(ini)s AND %(fim)s
              AND (
                lowerUTF8(nome_grupo) LIKE '%%combusti%%'
                OR lowerUTF8(nome_grupo) LIKE '%%gasolina%%'
                OR lowerUTF8(nome_grupo) LIKE '%%diesel%%'
                OR lowerUTF8(nome_grupo) LIKE '%%etanol%%'
                OR lowerUTF8(nome_grupo) LIKE '%%gnv%%'
                OR lowerUTF8(nome_grupo) LIKE '%%gas natural%%'
                OR lowerUTF8(nome_grupo) LIKE '%%alcool%%'
              )
              AND NOT (
                lowerUTF8(nome_produto) LIKE '%%aditivo%%'
                OR lowerUTF8(nome_produto) LIKE '%%injector%%'
                OR lowerUTF8(nome_produto) LIKE '%%arla%%'
                OR lowerUTF8(nome_produto) LIKE '%%lubrificante%%'
                OR lowerUTF8(nome_produto) LIKE '%%filtro%%'
                OR lowerUTF8(nome_grupo) LIKE '%%filtro%%'
              )
            GROUP BY id_produto
            HAVING sum(qtd) > 0 OR sum(faturamento) > 0
            ORDER BY produto_nome
            """,
            {
                "id_empresa": int(id_empresa),
                "id_filial": int(id_filial),
                "ini": int(ini),
                "fim": int(fim),
            },
        )
        # Catálogo combustível no CH (mesmo sem venda no período) — evita fallback lento PG.
        fuel_catalog = query_dict(
            """
            SELECT
              p.id_produto,
              if(p.nome = '', concat('#ID ', toString(p.id_produto)), p.nome) AS produto_nome,
              ifNull(g.nome, '') AS grupo_nome,
              toFloat64(ifNull(p.custo_medio, 0)) AS custo_medio
            FROM torqmind_current.dim_produto AS p FINAL
            LEFT JOIN torqmind_current.dim_grupo_produto AS g FINAL
              ON g.id_empresa = p.id_empresa
             AND g.id_filial = p.id_filial
             AND g.id_grupo_produto = p.id_grupo_produto
             AND g.is_deleted = 0
            WHERE p.id_empresa = %(id_empresa)s
              AND p.id_filial = %(id_filial)s
              AND p.is_deleted = 0
              AND (
                lowerUTF8(ifNull(g.nome, '')) LIKE '%%combusti%%'
                OR lowerUTF8(ifNull(g.nome, '')) LIKE '%%gasolina%%'
                OR lowerUTF8(ifNull(g.nome, '')) LIKE '%%diesel%%'
                OR lowerUTF8(ifNull(g.nome, '')) LIKE '%%etanol%%'
                OR lowerUTF8(ifNull(g.nome, '')) LIKE '%%gnv%%'
                OR lowerUTF8(p.nome) LIKE '%%gasolina%%'
                OR lowerUTF8(p.nome) LIKE '%%etanol%%'
                OR lowerUTF8(p.nome) LIKE '%%diesel%%'
              )
              AND NOT (
                lowerUTF8(p.nome) LIKE '%%aditivo%%'
                OR lowerUTF8(p.nome) LIKE '%%filtro%%'
                OR lowerUTF8(ifNull(g.nome, '')) LIKE '%%filtro%%'
              )
            ORDER BY produto_nome
            """,
            {"id_empresa": int(id_empresa), "id_filial": int(id_filial)},
        )
        sales_by_prod = {
            int(r["id_produto"]): r for r in (sales_rows or []) if r.get("id_produto") is not None
        }
        catalog = fuel_catalog or []
        # Se o catálogo veio vazio mas há vendas, use as vendas como base.
        if not catalog and sales_by_prod:
            catalog = [
                {
                    "id_produto": pid,
                    "produto_nome": s.get("produto_nome"),
                    "grupo_nome": s.get("grupo_nome"),
                    "custo_medio": 0.0,
                }
                for pid, s in sales_by_prod.items()
            ]
        if catalog or sales_by_prod:
            for fr in catalog:
                pid = int(fr["id_produto"])
                sr = sales_by_prod.get(pid) or {}
                qtd = float(sr.get("qtd_periodo") or 0)
                fat = float(sr.get("faturamento_periodo") or 0)
                comp = comp_by_prod.get(pid) or {}
                nome = str(fr.get("produto_nome") or sr.get("produto_nome") or f"#ID {pid}")
                grupo = str(fr.get("grupo_nome") or sr.get("grupo_nome") or "")
                rows.append(
                    {
                        "id_produto": pid,
                        "produto_nome": nome,
                        "grupo_nome": grupo,
                        "familia_combustivel": _competitor_fuel_family(nome, grupo),
                        "custo_medio": float(fr.get("custo_medio") or 0),
                        "qtd_periodo": qtd,
                        "faturamento_periodo": fat,
                        "avg_price_current": (fat / qtd) if qtd > 0 else 0.0,
                        "competitor_price": float(comp.get("competitor_price") or 0),
                        "competitor_updated_at": comp.get("updated_at"),
                    }
                )
            if rows:
                source = "clickhouse"
    except Exception as exc:
        logging.getLogger(__name__).warning("competitor_pricing CH failed: %s", str(exc)[:200])
        rows = []

    if source != "clickhouse":
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
        source = "postgres"

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
        "source": source,
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
      SELECT data_key,
             SUM(cancelamentos)::int AS cancelamentos,
             SUM(valor_cancelado)::numeric(18,2) AS valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY data_key
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
    fetch_limit = max(int(limit) * 4, int(limit))
    params = [id_empresa, id_empresa, ini, fim] + branch_params + [fetch_limit]

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
    out: List[Dict[str, Any]] = []
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        tv = str(row.get("turno_value") or "").strip()
        # Exclui caixa geral (0), nulo e turno sem resolução operacional
        if not tv or tv == "0":
            continue
        try:
            if int(tv) < 1:
                continue
        except (TypeError, ValueError):
            continue
        row["turno_label"] = _turno_label(tv, row.get("id_turno"))
        row["turno_numero"] = int(tv) if str(tv).isdigit() else None
        label_l = str(row.get("turno_label") or "").lower()
        if "sem cadastro" in label_l or "não resolvido" in label_l or "nao resolvido" in label_l:
            continue
        if not row.get("data"):
            continue
        doc = row.get("id_comprovante")
        if not doc:
            continue
        row["documento_label"] = str(doc)
        row["documento_venda"] = doc
        out.append(row)
        if len(out) >= int(limit):
            break
    return out


def fraud_top_users(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    """Top operadores por cancelamento — mesmo universo de fraud_last_events (turno ≥ 1)."""
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("e.id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        e.id_filial,
        MAX(e.usuario_nome) AS usuario_nome,
        e.id_usuario,
        COUNT(*)::int AS cancelamentos,
        COALESCE(SUM(e.valor_total),0)::numeric(18,2) AS valor_cancelado,
        COUNT(*) FILTER (WHERE e.usuario_source = 'turno')::int AS resolvidos_por_turno,
        COUNT(*) FILTER (WHERE e.usuario_source = 'comprovante')::int AS fallback_comprovante
      FROM mart.fraude_cancelamentos_eventos e
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = e.id_empresa
       AND t.id_filial = e.id_filial
       AND t.id_turno = e.id_turno
      WHERE e.id_empresa = %s
        AND e.data_key BETWEEN %s AND %s
        {where_filial}
        AND NULLIF(TRIM({_turno_value_sql('t.payload', 'e.id_turno')}), '') IS NOT NULL
        AND NULLIF(TRIM({_turno_value_sql('t.payload', 'e.id_turno')}), '') <> '0'
        AND (
          CASE
            WHEN {_turno_value_sql('t.payload', 'e.id_turno')} ~ '^[0-9]+$'
            THEN ({_turno_value_sql('t.payload', 'e.id_turno')})::int >= 1
            ELSE true
          END
        )
      GROUP BY e.id_filial, e.id_usuario
      ORDER BY cancelamentos DESC, valor_cancelado DESC, e.id_usuario
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        filial_nome_map = {
            int(r["id_filial"]): r.get("nome")
            for r in conn.execute(
                "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s",
                [id_empresa],
            ).fetchall()
            if r.get("id_filial") is not None
        }
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        fid = int(row.get("id_filial") or 0)
        row["filial_label"] = _filial_label(fid, filial_nome_map.get(fid))
    return rows


def fraud_troca_forma_pgto(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    only_suspeita: bool = True,
    limit: int = 200,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Payment-form-change antifraud events.

    There is no PostgreSQL mart for this contract: the canonical source lives in
    the realtime ClickHouse mart (``torqmind_mart_rt.mart_troca_forma_pgto_rt``)
    served via ``repos_mart_realtime.fraud_troca_forma_pgto``. This legacy stub
    exists only so the analytics facade can register the function and dispatch to
    realtime; on the legacy path it safely returns no rows.
    """
    return []


def fraud_troca_forma_pgto_kpis(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Period-wide totals for payment-form-change events (antifraud).

    Like :func:`fraud_troca_forma_pgto`, the canonical source is the realtime
    ClickHouse mart served via ``repos_mart_realtime``. This legacy stub returns
    zeroed totals so the analytics facade can register the function and dispatch
    to realtime.
    """
    return {
        "suspeitas_qtd": 0,
        "suspeitas_valor": 0.0,
        "todas_qtd": 0,
        "todas_valor": 0.0,
    }


def fraud_lancamentos_creditos(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    limit: int = 500,
    risco: str = "suspeitas",
    cliente_q: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Lancamentos de credito de clientes (antifraude).

    Golpe: o operador INJETA um credito no cliente (ENTRADAS) e depois aplica
    esse credito em vendas/pagamentos (SAIDAS). A injecao MANUAL ("Credito
    adicionado manualmente"), sem contrapartida de troco/cheque/fatura, e o
    sinal de risco.

    Hora: só quando ``DATA`` traz HH:MM ≠ 00:00 (injeções manuais no Xpert
    quase sempre vêm sem hora). Sem DATAREPL/DTACONTA no payload.
    ``saldo_operacao``: saldo do cliente imediatamente após aquele lançamento,
    reconstruído de trás pra frente a partir do saldo atual + ledger ordenado
    por DATA + ID_MOVCREDITOENTIDADES.
    ``cliente_q``: filtro por trecho do nome (resolvido via dim_cliente).
    Cada injecao traz ``consumos`` (SAIDAS posteriores do mesmo cliente/filial).
    """
    import re

    where_filial, branch_params = _branch_scope_clause("m.id_filial", id_filial)
    ini = dt_ini.isoformat()
    fim = dt_fim.isoformat()
    risco_key = str(risco or "suspeitas").strip().lower()
    if risco_key in ("suspeita", "suspeitas", "manual", "manuais"):
        risco_sql = "AND (m.payload->>'HISTORICO' ILIKE '%%adicionado manualmente%%')"
    elif risco_key in ("normal", "normais"):
        risco_sql = "AND NOT (m.payload->>'HISTORICO' ILIKE '%%adicionado manualmente%%')"
    else:
        risco_sql = ""

    summary_sql = f"""
      SELECT
        COUNT(*) FILTER (WHERE ent > 0)::int AS injecoes_qtd,
        COALESCE(SUM(ent) FILTER (WHERE ent > 0), 0)::numeric(18,2) AS injetado,
        COUNT(*) FILTER (WHERE ent > 0 AND manual)::int AS manuais_qtd,
        COALESCE(SUM(ent) FILTER (WHERE ent > 0 AND manual), 0)::numeric(18,2) AS injetado_manual,
        COALESCE(SUM(sai), 0)::numeric(18,2) AS aplicado
      FROM (
        SELECT
          COALESCE((m.payload->>'ENTRADAS')::numeric, 0) AS ent,
          COALESCE((m.payload->>'SAIDAS')::numeric, 0) AS sai,
          (m.payload->>'HISTORICO' ILIKE '%%adicionado manualmente%%') AS manual
        FROM stg.movcreditoentidades m
        WHERE m.id_empresa = %s
          AND LEFT(m.payload->>'DATA', 10) BETWEEN %s AND %s
          {where_filial}
      ) t
    """
    summary_params = [id_empresa, ini, fim] + branch_params

    list_sql = f"""
      WITH saldo AS (
        SELECT id_filial, (payload->>'ID_ENTIDADE')::int AS id_entidade,
               SUM(COALESCE((payload->>'SALDO')::numeric, 0)) AS saldo
        FROM stg.credito
        WHERE id_empresa = %s
        GROUP BY 1, 2
      )
      SELECT
        LEFT(m.payload->>'DATA', 10) AS data,
        m.payload->>'DATA' AS data_raw,
        NULLIF(TRIM(m.payload->>'DATAREPL'), '') AS datarepl,
        NULLIF(TRIM(m.payload->>'DTACONTA'), '') AS dtaconta,
        m.id_filial,
        NULLIF(m.payload->>'ID_ENTIDADE', '')::int AS id_entidade,
        COALESCE(u.nome, '') AS operador,
        NULLIF(m.payload->>'ID_USUARIOS', '')::int AS id_usuario,
        COALESCE((m.payload->>'ENTRADAS')::numeric, 0)::numeric(18,2) AS injetado,
        COALESCE(s.saldo, 0)::numeric(18,2) AS saldo_cliente,
        COALESCE(NULLIF(TRIM(m.payload->>'HISTORICO'), ''), '') AS historico,
        NULLIF(TRIM(m.payload->>'REFERENCIA'), '') AS referencia,
        NULLIF(m.payload->>'ID_MOVCREDITOENTIDADES', '')::bigint AS id_mov,
        (m.payload->>'HISTORICO' ILIKE '%%adicionado manualmente%%') AS suspeita
      FROM stg.movcreditoentidades m
      LEFT JOIN LATERAL (
        SELECT COALESCE(NULLIF(TRIM(u2.payload->>'NOMEUSUARIOS'), ''), NULLIF(TRIM(u2.payload->>'NOME'), '')) AS nome
        FROM stg.usuarios u2
        WHERE u2.id_empresa = m.id_empresa
          AND u2.id_usuario = NULLIF(m.payload->>'ID_USUARIOS', '')::int
          AND COALESCE(NULLIF(TRIM(u2.payload->>'NOMEUSUARIOS'), ''), NULLIF(TRIM(u2.payload->>'NOME'), '')) IS NOT NULL
        LIMIT 1
      ) u ON true
      LEFT JOIN saldo s
        ON s.id_filial = m.id_filial AND s.id_entidade = NULLIF(m.payload->>'ID_ENTIDADE', '')::int
      WHERE m.id_empresa = %s
        AND LEFT(m.payload->>'DATA', 10) BETWEEN %s AND %s
        AND COALESCE((m.payload->>'ENTRADAS')::numeric, 0) > 0
        {risco_sql}
        {where_filial}
      ORDER BY m.id_filial ASC,
               LEFT(m.payload->>'DATA', 10) DESC,
               COALESCE(NULLIF(m.payload->>'ID_MOVCREDITOENTIDADES', '')::bigint, 0) DESC
      LIMIT %s
    """
    list_params = [id_empresa, id_empresa, ini, fim] + branch_params + [int(limit)]

    def _forma_from_historico(hist: str) -> Optional[str]:
        text = str(hist or "").strip()
        if not text:
            return None
        m = re.search(r"(?i)adicionado manualmente\s+(.+)$", text)
        if m:
            tail = m.group(1).strip()
            if tail:
                return re.sub(r"\s+\d+$", "", tail).strip() or tail
        for token in ("CHEQUE PRE", "CHEQUE", "DINHEIRO", "PIX", "CARTÃO", "CARTAO", "PRAZO", "CONVÊNIO", "CONVENIO"):
            if re.search(rf"(?i)\b{re.escape(token)}\b", text):
                return token.title() if token != "CHEQUE PRE" else "Cheque Pre"
        return None

    def _parse_data_parts(raw: Any) -> Tuple[Optional[str], Optional[str], bool]:
        s = str(raw or "").strip()
        if not s:
            return None, None, False
        day = s[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", s) else None
        if not day:
            return None, None, False
        m = re.search(r"[T ](\d{2}):(\d{2})", s)
        if not m:
            return day, None, False
        hh, mm = int(m.group(1)), int(m.group(2))
        if hh == 0 and mm == 0:
            return day, None, False
        ts = f"{day}T{hh:02d}:{mm:02d}:00-03:00"
        return day, ts, True

    source = "clickhouse"
    srow: Dict[str, Any] = {}
    rows: list = []
    filial_nome_map: Dict[int, Any] = {}
    consumos_by_key: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
    saldo_apos_by_mov: Dict[Tuple[int, int, int], float] = {}

    def _load_ledger_and_rebuild(ledger_rows: List[Dict[str, Any]], rows_in: list) -> None:
        nonlocal consumos_by_key, saldo_apos_by_mov
        from datetime import timedelta as _td
        cons_fim = (dt_fim + _td(days=120)).isoformat()
        ids_cli = sorted({int(r["id_entidade"]) for r in rows_in if r.get("id_entidade")})
        if not ids_cli:
            return
        saldo_atual_map: Dict[Tuple[int, int], float] = {}
        for r in rows_in:
            if r.get("id_entidade") is None:
                continue
            key = (int(r["id_filial"]), int(r["id_entidade"]))
            saldo_atual_map[key] = round(float(r.get("saldo_cliente") or 0), 2)
        by_cli: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        for lr in ledger_rows:
            if lr.get("id_entidade") is None:
                continue
            by_cli.setdefault((int(lr["id_filial"]), int(lr["id_entidade"])), []).append(lr)
        for key, movs in by_cli.items():
            running = float(saldo_atual_map.get(key) or 0)
            for lr in reversed(movs):
                id_mov = int(lr.get("id_mov") or 0)
                running = round(running, 2)
                if id_mov:
                    saldo_apos_by_mov[(key[0], key[1], id_mov)] = running
                ent = float(lr.get("entradas") or 0)
                sai = float(lr.get("saidas") or 0)
                running = round(running - ent + sai, 2)
        for lr in ledger_rows:
            sai = float(lr.get("saidas") or 0)
            if sai <= 0:
                continue
            day, data_ts, hora_ok = _parse_data_parts(lr.get("data_raw") or lr.get("data"))
            if day and day > cons_fim:
                continue
            if day and day < ini:
                continue
            key = (int(lr["id_filial"]), int(lr["id_entidade"]))
            hist = str(lr.get("historico") or "")
            tipo = "baixa_manual"
            low = hist.lower()
            if "venda" in low or "cupom" in low or "nfc" in low or "comprovante" in low:
                tipo = "venda"
            elif "fatura" in low or "titulo" in low or "receber" in low:
                tipo = "pagamento_fatura"
            id_mov = int(lr.get("id_mov") or 0)
            consumos_by_key.setdefault(key, []).append(
                {
                    "data": day,
                    "data_ts": data_ts if hora_ok else None,
                    "hora_conhecida": hora_ok,
                    "valor": round(sai, 2),
                    "historico": hist,
                    "referencia": lr.get("referencia"),
                    "id_mov": id_mov or None,
                    "saldo_operacao": saldo_apos_by_mov.get((key[0], key[1], id_mov)) if id_mov else None,
                    "tipo": tipo,
                    "tipo_label": {
                        "venda": "Venda",
                        "pagamento_fatura": "Pagamento de fatura",
                        "baixa_manual": "Baixa / uso do crédito",
                    }.get(tipo, "Uso do crédito"),
                }
            )

    try:
        from app.db_clickhouse import query_dict

        branch_ids = _branch_ids(id_filial)
        params_ch: Dict[str, Any] = {
            "id_empresa": int(id_empresa),
            "ini": ini,
            "fim": fim,
            "lim": int(limit),
        }
        filial_sql = ""
        if branch_ids:
            if len(branch_ids) == 1:
                filial_sql = "AND id_filial = %(id_filial)s"
                params_ch["id_filial"] = branch_ids[0]
            else:
                filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)
        risco_ch = ""
        if risco_key in ("suspeita", "suspeitas", "manual", "manuais"):
            risco_ch = "AND manual_suspeita = 1"
        elif risco_key in ("normal", "normais"):
            risco_ch = "AND manual_suspeita = 0"

        sum_rows = query_dict(
            f"""
            SELECT
              countIf(entradas > 0) AS injecoes_qtd,
              sumIf(entradas, entradas > 0) AS injetado,
              countIf(entradas > 0 AND manual_suspeita = 1) AS manuais_qtd,
              sumIf(entradas, entradas > 0 AND manual_suspeita = 1) AS injetado_manual,
              sum(saidas) AS aplicado
            FROM torqmind_mart_rt.mart_fraud_credito_cliente_mov FINAL
            WHERE id_empresa = %(id_empresa)s
              AND data_dia BETWEEN toDate(%(ini)s) AND toDate(%(fim)s)
              {filial_sql}
            """,
            params_ch,
        )
        srow = sum_rows[0] if sum_rows else {}
        rows = query_dict(
            f"""
            SELECT
              toString(m.data_dia) AS data,
              m.data_raw,
              m.id_filial,
              m.id_entidade,
              m.operador,
              m.id_usuario,
              m.entradas AS injetado,
              coalesce(s.saldo, 0) AS saldo_cliente,
              m.historico,
              m.referencia,
              m.id_mov,
              m.manual_suspeita AS suspeita
            FROM torqmind_mart_rt.mart_fraud_credito_cliente_mov AS m FINAL
            LEFT JOIN (
              SELECT id_empresa, id_filial, id_entidade, argMax(saldo, published_at) AS saldo
              FROM torqmind_mart_rt.mart_fraud_credito_cliente_saldo
              GROUP BY id_empresa, id_filial, id_entidade
            ) AS s
              ON s.id_empresa = m.id_empresa AND s.id_filial = m.id_filial AND s.id_entidade = m.id_entidade
            WHERE m.id_empresa = %(id_empresa)s
              AND m.data_dia BETWEEN toDate(%(ini)s) AND toDate(%(fim)s)
              AND m.entradas > 0
              {risco_ch}
              {filial_sql}
            ORDER BY m.id_filial ASC, m.data_dia DESC, m.id_mov DESC
            LIMIT %(lim)s
            """,
            params_ch,
        )
        if not rows and float(srow.get("injecoes_qtd") or 0) == 0:
            raise RuntimeError("CH empty — fallback PG")
        for r in rows:
            fid = int(r.get("id_filial") or 0)
            filial_nome_map[fid] = apelido_for(fid)
        ids_cli = sorted({int(r["id_entidade"]) for r in rows if r.get("id_entidade")})
        ledger_rows: List[Dict[str, Any]] = []
        if ids_cli:
            ids_csv = ", ".join(str(i) for i in ids_cli)
            ledger_rows = query_dict(
                f"""
                SELECT
                  id_filial, id_entidade,
                  toString(data_dia) AS data,
                  data_raw, id_mov, entradas, saidas, historico, referencia
                FROM torqmind_mart_rt.mart_fraud_credito_cliente_mov FINAL
                WHERE id_empresa = %(id_empresa)s
                  AND id_entidade IN ({ids_csv})
                  AND (entradas > 0 OR saidas > 0)
                  {filial_sql}
                ORDER BY id_filial, id_entidade, data_dia, id_mov
                """,
                params_ch,
            )
        _load_ledger_and_rebuild(ledger_rows, rows)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "fraud_lancamentos_creditos CH path failed (%s); using PG", str(exc)[:180]
        )
        source = "postgres"
        with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            srow = conn.execute(summary_sql, summary_params).fetchone() or {}
            filial_nome_map = {
                int(r["id_filial"]): r.get("nome")
                for r in conn.execute(
                    "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s",
                    [id_empresa],
                ).fetchall()
                if r.get("id_filial") is not None
            }
            rows = list(conn.execute(list_sql, list_params).fetchall())
            ids_cli = sorted({int(r["id_entidade"]) for r in rows if r.get("id_entidade")})
            ledger_rows = []
            if ids_cli:
                from datetime import timedelta as _td
                ledger_sql = f"""
                  SELECT
                    m.id_filial,
                    NULLIF(m.payload->>'ID_ENTIDADE', '')::int AS id_entidade,
                    LEFT(m.payload->>'DATA', 10) AS data,
                    m.payload->>'DATA' AS data_raw,
                    NULLIF(m.payload->>'ID_MOVCREDITOENTIDADES', '')::bigint AS id_mov,
                    COALESCE((m.payload->>'ENTRADAS')::numeric, 0)::numeric(18,2) AS entradas,
                    COALESCE((m.payload->>'SAIDAS')::numeric, 0)::numeric(18,2) AS saidas,
                    COALESCE(NULLIF(TRIM(m.payload->>'HISTORICO'), ''), '') AS historico,
                    NULLIF(TRIM(m.payload->>'REFERENCIA'), '') AS referencia
                  FROM stg.movcreditoentidades m
                  WHERE m.id_empresa = %s
                    AND NULLIF(m.payload->>'ID_ENTIDADE', '')::int = ANY(%s)
                    AND (
                      COALESCE((m.payload->>'ENTRADAS')::numeric, 0) > 0
                      OR COALESCE((m.payload->>'SAIDAS')::numeric, 0) > 0
                    )
                    {where_filial}
                  ORDER BY m.id_filial,
                           NULLIF(m.payload->>'ID_ENTIDADE', '')::int,
                           LEFT(m.payload->>'DATA', 10),
                           COALESCE(NULLIF(m.payload->>'ID_MOVCREDITOENTIDADES', '')::bigint, 0)
                """
                ledger_rows = [dict(x) for x in conn.execute(ledger_sql, [id_empresa, ids_cli] + branch_params).fetchall()]
            _load_ledger_and_rebuild(ledger_rows, rows)


    nome_map: Dict[int, str] = {}
    ids = sorted({int(r.get("id_entidade")) for r in rows if r.get("id_entidade")})
    if ids:
        try:
            from app.db_clickhouse import query_dict as ch_query

            values = ", ".join(str(i) for i in ids)
            ch_rows = ch_query(
                f"""
                SELECT id_cliente, nome
                FROM (
                  SELECT id_cliente, argMax(nome, updated_at) AS nome
                  FROM torqmind_current.dim_cliente FINAL
                  WHERE id_empresa = {{id_empresa:Int32}}
                    AND id_cliente IN ({values})
                    AND is_deleted = 0
                  GROUP BY id_cliente
                )
                WHERE nome != ''
                """,
                parameters={"id_empresa": id_empresa},
            )
            for cr in ch_rows:
                cid = int(cr.get("id_cliente") or 0)
                nome = str(cr.get("nome") or "").strip()
                if cid and nome:
                    nome_map[cid] = nome
        except Exception:
            nome_map = {}

    q = str(cliente_q or "").strip().lower()

    def _fmt(r: Dict[str, Any]) -> Dict[str, Any]:
        fid = int(r.get("id_filial") or 0)
        ident = r.get("id_entidade")
        ident_i = int(ident) if ident is not None else None
        nome_cli = nome_map.get(ident_i) if ident_i else None
        operador = r.get("operador") or (f"Operador #{r.get('id_usuario')}" if r.get("id_usuario") else "Operador sem cadastro")
        day, data_ts, hora_ok = _parse_data_parts(r.get("data_raw") or r.get("data"))
        hist = r.get("historico") or ""
        id_mov = int(r.get("id_mov") or 0) or None
        saldo_atual = round(float(r.get("saldo_cliente") or 0), 2)
        saldo_op = None
        if ident_i and fid and id_mov:
            saldo_op = saldo_apos_by_mov.get((fid, ident_i, id_mov))
        consumos = []
        if ident_i and fid:
            for c in consumos_by_key.get((fid, ident_i), []):
                # Só consumos na data da injecao ou posteriores
                if day and c.get("data") and str(c["data"]) < day:
                    continue
                # No mesmo dia: só usos com id_mov > id da injeção (ordem canônica)
                if day and c.get("data") == day and id_mov and c.get("id_mov"):
                    if int(c["id_mov"]) < int(id_mov):
                        continue
                consumos.append(c)
        return {
            "data": day,
            "data_ts": data_ts if hora_ok else None,
            "hora_conhecida": bool(hora_ok),
            "id_mov": id_mov,
            "id_filial": fid,
            "filial_label": _filial_label(fid, filial_nome_map.get(fid)),
            "id_cliente": ident_i,
            "cliente": nome_cli or (f"Cliente #{ident_i}" if ident_i else "Cliente sem cadastro"),
            "operador": operador,
            "injetado": round(float(r.get("injetado") or 0), 2),
            "saldo_cliente": saldo_atual,
            "saldo_atual": saldo_atual,
            "saldo_operacao": round(float(saldo_op), 2) if saldo_op is not None else None,
            "historico": hist,
            "forma_pagamento": _forma_from_historico(hist),
            "referencia": r.get("referencia"),
            "suspeita": bool(r.get("suspeita")),
            "consumos": consumos,
            "consumos_qtd": len(consumos),
            "consumos_valor": round(sum(float(c.get("valor") or 0) for c in consumos), 2),
        }

    lancamentos = [_fmt(r) for r in rows]
    if q:
        lancamentos = [
            x for x in lancamentos
            if q in str(x.get("cliente") or "").lower()
            or q in str(x.get("id_cliente") or "")
        ]

    return {
        "summary": {
            "injecoes_qtd": int(srow.get("injecoes_qtd") or 0),
            "injetado": round(float(srow.get("injetado") or 0), 2),
            "manuais_qtd": int(srow.get("manuais_qtd") or 0),
            "injetado_manual": round(float(srow.get("injetado_manual") or 0), 2),
            "aplicado": round(float(srow.get("aplicado") or 0), 2),
        },
        "lancamentos": lancamentos,
        "risco_filtro": risco_key if risco_key in ("suspeitas", "normais", "todas") else "suspeitas",
        "cliente_q": q or None,
        "source": source,
    }


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

    mart_where_filial, mart_branch_params = _branch_scope_clause("id_filial", id_filial)
    latest_snapshot_sql = f"""
      SELECT MAX(dt_ref)::date AS dt_ref
      FROM mart.customer_rfm_daily
      WHERE id_empresa = %s
        AND dt_ref <= %s::date
        {mart_where_filial}
    """
    snapshot_sql = f"""
      SELECT
        COUNT(*) FILTER (WHERE id_cliente <> -1)::int AS clientes_identificados,
        COUNT(*) FILTER (WHERE last_purchase >= (%s::date - interval '7 days'))::int AS ativos_7d,
        COUNT(*) FILTER (WHERE last_purchase < (%s::date - interval '30 days'))::int AS em_risco_30d,
        COALESCE(SUM(monetary_90),0)::numeric(18,2) AS faturamento_90d
      FROM mart.customer_rfm_daily
      WHERE id_empresa = %s
        AND dt_ref = %s::date
        {mart_where_filial}
    """

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
        latest_snapshot = conn.execute(
            latest_snapshot_sql,
            [id_empresa, as_of] + mart_branch_params,
        ).fetchone()
        snapshot_dt = latest_snapshot.get("dt_ref") if latest_snapshot else None
        if snapshot_dt:
            row = conn.execute(
                snapshot_sql,
                [as_of, as_of, id_empresa, snapshot_dt] + mart_branch_params,
            ).fetchone()
            if row:
                return row

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
    limit: int = 0,
    sort_by: str = "gravity",
) -> Dict[str, Any]:
    """Delinquency overview from mart.customer_delinquency_summary (fast indexed read).

    sort_by options:
      - gravity (default): 30+ titles DESC, then 30d DESC, then total value
      - valor: valor_total_aberto DESC
      - atraso: max_dias_atraso DESC
      - comprando: compras_30d DESC (only customers still buying), then valor_total_vencido DESC
    """
    where_filial, branch_params = _branch_scope_clause("m.id_filial", id_filial)

    # Summary aggregation from mart
    summary_sql = f"""
      SELECT
        COUNT(*)::int AS clientes_em_aberto,
        COALESCE(SUM(m.titulos_ate_30d + m.titulos_acima_30d), 0)::int AS titulos_em_aberto,
        COALESCE(SUM(m.valor_total_vencido), 0)::numeric(18,2) AS valor_total,
        COALESCE(SUM(m.titulos_ate_30d), 0)::int AS titulos_ate_30d,
        COALESCE(SUM(m.valor_ate_30d), 0)::numeric(18,2) AS valor_ate_30d,
        COALESCE(SUM(m.titulos_acima_30d), 0)::int AS titulos_acima_30d,
        COALESCE(SUM(m.valor_acima_30d), 0)::numeric(18,2) AS valor_acima_30d,
        COALESCE(SUM(m.titulos_a_vencer), 0)::int AS titulos_a_vencer,
        COALESCE(SUM(m.valor_a_vencer), 0)::numeric(18,2) AS valor_a_vencer,
        COALESCE(MAX(m.max_dias_atraso), 0)::int AS max_dias_atraso,
        COUNT(*) FILTER (WHERE m.titulos_a_vencer > 0)::int AS clientes_a_vencer,
        COALESCE(SUM(m.valor_total_aberto), 0)::numeric(18,2) AS valor_total_aberto
      FROM mart.customer_delinquency_summary m
      WHERE m.id_empresa = %s
        {where_filial}
    """
    summary_params = [id_empresa] + branch_params

    # Sort order based on sort_by parameter
    order_clauses = {
        "gravity": "m.titulos_acima_30d DESC, m.titulos_ate_30d DESC, m.valor_total_vencido DESC, m.id_cliente",
        "valor": "m.valor_total_aberto DESC, m.valor_total_vencido DESC, m.id_cliente",
        "atraso": "m.max_dias_atraso DESC, m.valor_total_vencido DESC, m.id_cliente",
        "comprando": "m.compras_30d DESC, m.valor_total_vencido DESC, m.id_cliente",
    }
    order_clause = order_clauses.get(sort_by, order_clauses["gravity"])

    # Customer list ordered by selected criterion
    customers_sql = f"""
      SELECT
        m.id_cliente,
        m.id_filial,
        m.cliente_nome,
        m.titulos_ate_30d,
        m.valor_ate_30d,
        m.titulos_acima_30d,
        m.valor_acima_30d,
        m.titulos_a_vencer,
        m.valor_a_vencer,
        m.max_dias_atraso,
        m.valor_total_vencido,
        m.valor_total_aberto,
        m.compras_30d,
        m.ultima_compra_dt
      FROM mart.customer_delinquency_summary m
      WHERE m.id_empresa = %s
        {where_filial}
      ORDER BY {order_clause}
    """
    customers_params = [id_empresa] + branch_params

    # Total da dívida POR FILIAL (o mesmo cliente pode dever em vários postos).
    by_filial_sql = f"""
      SELECT
        m.id_filial,
        COUNT(*)::int AS clientes,
        COALESCE(SUM(m.valor_total_vencido), 0)::numeric(18,2) AS valor_vencido,
        COALESCE(SUM(m.valor_total_aberto), 0)::numeric(18,2) AS valor_aberto
      FROM mart.customer_delinquency_summary m
      WHERE m.id_empresa = %s
        {where_filial}
      GROUP BY m.id_filial
      ORDER BY valor_vencido DESC
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        summary_row = conn.execute(summary_sql, summary_params).fetchone() or {}
        customers = list(conn.execute(customers_sql, customers_params).fetchall())
        by_filial_rows = list(conn.execute(by_filial_sql, summary_params).fetchall())
        filial_nome_map = {
            int(r["id_filial"]): r.get("nome")
            for r in conn.execute(
                "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s",
                [id_empresa],
            ).fetchall()
            if r.get("id_filial") is not None
        }

    by_filial = [
        {
            "id_filial": int(r.get("id_filial") or 0),
            "filial_label": _filial_label(int(r.get("id_filial") or 0), filial_nome_map.get(int(r.get("id_filial") or 0))),
            "clientes": int(r.get("clientes") or 0),
            "valor_vencido": round(float(r.get("valor_vencido") or 0), 2),
            "valor_aberto": round(float(r.get("valor_aberto") or 0), 2),
        }
        for r in by_filial_rows
        if r.get("id_filial") is not None
    ]

    if not customers:
        # Mart may not be populated yet — return empty structure
        return {
            "summary": {
                "clientes_em_aberto": 0,
                "titulos_em_aberto": 0,
                "valor_total": 0,
                "titulos_ate_30d": 0,
                "valor_ate_30d": 0,
                "titulos_acima_30d": 0,
                "valor_acima_30d": 0,
                "titulos_a_vencer": 0,
                "valor_a_vencer": 0,
                "max_dias_atraso": 0,
            },
            "buckets": [
                {"bucket": "1_30", "label": "Até 30 dias", "valor": 0, "titulos": 0},
                {"bucket": "31_plus", "label": "30+ dias", "valor": 0, "titulos": 0},
            ],
            "customers": [],
            "by_filial": [],
            "dt_ref": as_of.isoformat(),
        }

    return {
        "summary": {
            "clientes_em_aberto": int(summary_row.get("clientes_em_aberto") or 0),
            "titulos_em_aberto": int(summary_row.get("titulos_em_aberto") or 0),
            "valor_total": round(float(summary_row.get("valor_total") or 0), 2),
            "titulos_ate_30d": int(summary_row.get("titulos_ate_30d") or 0),
            "valor_ate_30d": round(float(summary_row.get("valor_ate_30d") or 0), 2),
            "titulos_acima_30d": int(summary_row.get("titulos_acima_30d") or 0),
            "valor_acima_30d": round(float(summary_row.get("valor_acima_30d") or 0), 2),
            "titulos_a_vencer": int(summary_row.get("titulos_a_vencer") or 0),
            "valor_a_vencer": round(float(summary_row.get("valor_a_vencer") or 0), 2),
            "max_dias_atraso": int(summary_row.get("max_dias_atraso") or 0),
            "valor_total_aberto": round(float(summary_row.get("valor_total_aberto") or 0), 2),
        },
        "buckets": [
            {
                "bucket": "1_30",
                "label": "Até 30 dias",
                "valor": round(float(summary_row.get("valor_ate_30d") or 0), 2),
                "titulos": int(summary_row.get("titulos_ate_30d") or 0),
            },
            {
                "bucket": "31_plus",
                "label": "30+ dias",
                "valor": round(float(summary_row.get("valor_acima_30d") or 0), 2),
                "titulos": int(summary_row.get("titulos_acima_30d") or 0),
            },
        ],
        "customers": [
            {
                "id_cliente": int(c.get("id_cliente") or 0),
                "id_filial": int(c.get("id_filial") or 0),
                "filial_label": _filial_label(int(c.get("id_filial") or 0), filial_nome_map.get(int(c.get("id_filial") or 0))),
                "cliente_nome": c.get("cliente_nome"),
                "titulos_ate_30d": int(c.get("titulos_ate_30d") or 0),
                "valor_ate_30d": round(float(c.get("valor_ate_30d") or 0), 2),
                "titulos_acima_30d": int(c.get("titulos_acima_30d") or 0),
                "valor_acima_30d": round(float(c.get("valor_acima_30d") or 0), 2),
                "titulos_a_vencer": int(c.get("titulos_a_vencer") or 0),
                "valor_a_vencer": round(float(c.get("valor_a_vencer") or 0), 2),
                "max_dias_atraso": int(c.get("max_dias_atraso") or 0),
                "valor_total_vencido": round(float(c.get("valor_total_vencido") or 0), 2),
                "valor_total_aberto": round(float(c.get("valor_total_aberto") or 0), 2),
                "compras_30d": int(c.get("compras_30d") or 0),
                "ultima_compra_dt": str(c.get("ultima_compra_dt") or "") if c.get("ultima_compra_dt") else None,
            }
            for c in customers
        ],
        "by_filial": by_filial,
        "sort_by": sort_by,
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


def finance_receipts_by_day(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Recebimentos de CONTAS A RECEBER por dia (baixas totais e parciais).

    Legacy PostgreSQL. Lê as baixas de títulos a receber (stg.contasreceberbaixa)
    pela DATA DA BAIXA (DATABAIXA). É o "recebimentos" da tela Financeiro: o
    dinheiro que entrou de títulos a receber — distinto do mix de formas de
    pagamento das vendas.
    """
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa] + branch_params + [dt_ini, dt_fim]
    sql = f"""
      SELECT
        to_char(left(payload->>'DATABAIXA', 10)::date, 'YYYYMMDD')::int AS data_key,
        round(SUM((payload->>'VALORBAIXA')::numeric), 2) AS valor,
        COUNT(*) AS qtd
      FROM stg.contasreceberbaixa
      WHERE id_empresa = %s
        {where_filial}
        AND payload->>'DATABAIXA' IS NOT NULL
        AND left(payload->>'DATABAIXA', 10) <> ''
        AND left(payload->>'DATABAIXA', 10)::date BETWEEN %s AND %s
      GROUP BY 1
      HAVING SUM((payload->>'VALORBAIXA')::numeric) > 0
      ORDER BY 1
    """
    try:
        with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            rows = list(conn.execute(sql, params).fetchall())
    except Exception:
        rows = []
    by_day = [
        {"data_key": int(r["data_key"]), "valor": round(float(r["valor"] or 0), 2), "qtd": int(r["qtd"] or 0)}
        for r in rows
        if r.get("data_key")
    ]
    total = round(sum(d["valor"] for d in by_day), 2)
    return {
        "by_day": by_day,
        "total_recebido": total,
        "qtd_baixas": sum(d["qtd"] for d in by_day),
        "source": "postgres",
    }


def cheques_pendentes_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    status: str = "",
    limit: int = 3000,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Cheques recebidos por status (tela Financeiro / Controle de Cheques).

    Traz cheques a VISTA e a PRAZO com todos os status: ``a_compensar`` |
    ``depositado`` | ``devolvido`` (com motivo) | ``compensado``. O parametro
    ``status`` e uma lista separada por virgula; vazio ou ``todos`` mostra a
    visao padrao (tudo MENOS compensado). Os cards de resumo sempre refletem o
    quadro completo. Fonte: mart.cheques_pendentes (Xpert dbo.CHEQUESRECEBIDOS
    + dbo.SITUACOES).
    """
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    today = business_today(id_empresa)

    _valid = {"a_compensar", "depositado", "devolvido", "compensado"}
    raw = str(status or "").strip().lower()
    if not raw or raw == "todos":
        req = {"a_compensar", "depositado", "devolvido"}
    else:
        req = {s.strip() for s in raw.split(",") if s.strip() in _valid}
        if not req:
            req = {"a_compensar", "depositado", "devolvido"}

    summary_sql = f"""
      SELECT
        status_cheque,
        COUNT(*)::int AS qtd,
        COALESCE(SUM(valor), 0)::numeric(18,2) AS valor,
        COUNT(*) FILTER (WHERE status_cheque <> 'compensado' AND dt_vencimento < %s)::int AS venc_qtd,
        COALESCE(SUM(valor) FILTER (WHERE status_cheque <> 'compensado' AND dt_vencimento < %s), 0)::numeric(18,2) AS venc_valor,
        COUNT(*) FILTER (WHERE avista)::int AS avista_qtd,
        COUNT(*) FILTER (WHERE NOT avista)::int AS aprazo_qtd
      FROM mart.cheques_pendentes
      WHERE id_empresa = %s
        {where_filial}
      GROUP BY status_cheque
    """
    summary_params = [today, today, id_empresa] + branch_params

    list_sql = f"""
      SELECT id_filial, id_db, id_cheque, id_entidade, cliente_nome, cpf, valor,
             dt_recebido, dt_vencimento, dt_compensado, situacao_cheque, avista,
             motivo_devolucao, status_cheque, banco, agencia, nroconta, numero
      FROM mart.cheques_pendentes
      WHERE id_empresa = %s
        {where_filial}
        AND status_cheque = ANY(%s)
      ORDER BY (status_cheque = 'devolvido') DESC, dt_vencimento NULLS LAST, valor DESC
      LIMIT %s
    """
    list_params = [id_empresa] + branch_params + [list(req), int(limit)]

    srows: list = []
    rows: list = []
    filial_nome_map: Dict[int, Any] = {}
    source = "postgres"
    try:
        from app.db_clickhouse import query_dict

        branch_ids = _branch_ids(id_filial)
        params_ch: Dict[str, Any] = {
            "id_empresa": int(id_empresa),
            "today": today.isoformat(),
            "lim": int(limit),
        }
        filial_sql = ""
        if branch_ids:
            if len(branch_ids) == 1:
                filial_sql = "AND id_filial = %(id_filial)s"
                params_ch["id_filial"] = branch_ids[0]
            else:
                filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)
        status_list = ", ".join("'" + s.replace("'", "") + "'" for s in sorted(req))
        srows = query_dict(
            f"""
            SELECT
              status_cheque,
              count() AS qtd,
              sum(v) AS valor,
              countIf(status_cheque != 'compensado' AND isNotNull(dt_venc) AND dt_venc < toDate(%(today)s)) AS venc_qtd,
              sumIf(v, status_cheque != 'compensado' AND isNotNull(dt_venc) AND dt_venc < toDate(%(today)s)) AS venc_valor,
              countIf(av = 1) AS avista_qtd,
              countIf(av = 0) AS aprazo_qtd
            FROM (
              SELECT
                status_cheque,
                valor AS v,
                dt_vencimento AS dt_venc,
                avista AS av
              FROM torqmind_mart_rt.mart_cheques_pendentes FINAL
              WHERE id_empresa = %(id_empresa)s
                {filial_sql}
            )
            GROUP BY status_cheque
            """,
            params_ch,
        )
        rows = query_dict(
            f"""
            SELECT id_filial, id_db, id_cheque, id_entidade, cliente_nome, cpf, valor,
                   dt_recebido, dt_vencimento, dt_compensado, situacao_cheque, avista,
                   motivo_devolucao, status_cheque, banco, agencia, nroconta, numero
            FROM torqmind_mart_rt.mart_cheques_pendentes FINAL
            WHERE id_empresa = %(id_empresa)s
              {filial_sql}
              AND status_cheque IN ({status_list})
            ORDER BY (status_cheque = 'devolvido') DESC, dt_vencimento ASC, valor DESC
            LIMIT %(lim)s
            """,
            params_ch,
        )
        if srows or rows:
            source = "clickhouse"
            # labels via apelido
            for r in rows:
                fid = int(r.get("id_filial") or 0)
                filial_nome_map[fid] = apelido_for(fid) or filial_nome_map.get(fid)
    except Exception as exc:
        logging.getLogger(__name__).warning("cheques CH failed: %s", str(exc)[:200])
        srows = []
        rows = []

    if source != "clickhouse":
        with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            srows = list(conn.execute(summary_sql, summary_params).fetchall())
            filial_nome_map = {
                int(r["id_filial"]): r.get("nome")
                for r in conn.execute(
                    "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s",
                    [id_empresa],
                ).fetchall()
                if r.get("id_filial") is not None
            }
            rows = list(conn.execute(list_sql, list_params).fetchall())
        source = "postgres"

    por_status: Dict[str, Dict[str, Any]] = {
        k: {"qtd": 0, "valor": 0.0} for k in _valid
    }
    venc_qtd = venc_valor = avista_qtd = aprazo_qtd = 0
    pend_qtd = 0
    pend_valor = 0.0
    for s in srows:
        st = s.get("status_cheque") or "a_compensar"
        por_status.setdefault(st, {"qtd": 0, "valor": 0.0})
        por_status[st]["qtd"] = int(s.get("qtd") or 0)
        por_status[st]["valor"] = round(float(s.get("valor") or 0), 2)
        venc_qtd += int(s.get("venc_qtd") or 0)
        venc_valor += float(s.get("venc_valor") or 0)
        avista_qtd += int(s.get("avista_qtd") or 0)
        aprazo_qtd += int(s.get("aprazo_qtd") or 0)
        if st != "compensado":
            pend_qtd += int(s.get("qtd") or 0)
            pend_valor += float(s.get("valor") or 0)

    def _fmt(c: Dict[str, Any]) -> Dict[str, Any]:
        fid = int(c.get("id_filial") or 0)
        dt_venc = c.get("dt_vencimento")
        return {
            "id_cheque": int(c.get("id_cheque") or 0),
            "id_filial": fid,
            "filial_label": _filial_label(fid, filial_nome_map.get(fid)),
            "cliente_nome": c.get("cliente_nome") or "",
            "cpf": c.get("cpf") or "",
            "valor": round(float(c.get("valor") or 0), 2),
            "dt_recebido": str(c.get("dt_recebido")) if c.get("dt_recebido") else None,
            "dt_vencimento": str(dt_venc) if dt_venc else None,
            "dt_compensado": str(c.get("dt_compensado")) if c.get("dt_compensado") else None,
            "vencido": bool(dt_venc and dt_venc < today and (c.get("status_cheque") != "compensado")),
            "avista": bool(int(c.get("avista") or 0)) if not isinstance(c.get("avista"), bool) else bool(c.get("avista")),
            "status": c.get("status_cheque") or "a_compensar",
            "motivo_devolucao": c.get("motivo_devolucao") or "",
            "banco": c.get("banco") or "",
            "agencia": c.get("agencia") or "",
            "nroconta": c.get("nroconta") or "",
            "numero": c.get("numero") or "",
        }

    return {
        "summary": {
            "por_status": por_status,
            "total_qtd": pend_qtd,
            "total_valor": round(pend_valor, 2),
            "vencidos_qtd": venc_qtd,
            "vencidos_valor": round(venc_valor, 2),
            "devolvidos_qtd": por_status.get("devolvido", {}).get("qtd", 0),
            "devolvidos_valor": por_status.get("devolvido", {}).get("valor", 0.0),
            "avista_qtd": avista_qtd,
            "aprazo_qtd": aprazo_qtd,
        },
        "status": sorted(req),
        "cheques": [_fmt(c) for c in rows],
        "dt_ref": today.isoformat(),
        "source": source,
    }


# ================================================================
# GESTAO ORCAMENTARIA (orcamento de despesas por conta gerencial)
# ================================================================

def _budget_status(realizado: float, orcado: float, alerta_pct: int) -> tuple[float, str]:
    pct = (realizado / orcado * 100.0) if orcado > 0 else 0.0
    if pct >= 100.0:
        status = "estourado"
    elif pct >= float(alerta_pct or 90):
        status = "alerta"
    else:
        status = "ok"
    return round(pct, 1), status


def budget_config_overview(role: str, id_empresa: int, id_filial: Optional[int], **kwargs: Any) -> Dict[str, Any]:
    """Contas gerenciais + orcamento configurado (tela Metas & Equipe, 1 filial).

    Sincroniza com o Xpert: lista TODAS as contas gerenciais da filial (catalogo
    mart.plano_contas_gerencial) com o teto/alerta ja definido (app.budget_conta).
    Exige 1 filial, como as telas de comissao.
    """
    fid = id_filial if isinstance(id_filial, int) else None
    if fid is None:
        return {"requires_single_filial": True, "id_filial": None, "accounts": []}
    sql = """
      SELECT
        g.id_plano_conta,
        g.codigo,
        g.nome_conta,
        COALESCE(b.valor_max, 0)::numeric(18,2) AS valor_max,
        COALESCE(b.alerta_pct, 90)::int AS alerta_pct,
        (b.id_plano_conta IS NOT NULL) AS configurado
      FROM mart.plano_contas_gerencial g
      LEFT JOIN app.budget_conta b
        ON b.id_empresa = g.id_empresa AND b.id_filial = g.id_filial AND b.id_plano_conta = g.id_plano_conta
      WHERE g.id_empresa = %s AND g.id_filial = %s
      ORDER BY g.nome_conta
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(fid)) as conn:
        rows = [dict(r) for r in conn.execute(sql, [id_empresa, fid]).fetchall()]
    accounts = [
        {
            "id_plano_conta": int(r.get("id_plano_conta") or 0),
            "codigo": r.get("codigo") or "",
            "nome_conta": r.get("nome_conta") or "",
            "valor_max": round(float(r.get("valor_max") or 0), 2),
            "alerta_pct": int(r.get("alerta_pct") or 90),
            "configurado": bool(r.get("configurado")),
        }
        for r in rows
    ]
    return {"requires_single_filial": False, "id_filial": fid, "accounts": accounts}


def budget_config_upsert(role: str, id_empresa: int, id_filial: Optional[int], items: List[Dict[str, Any]], **kwargs: Any) -> Dict[str, Any]:
    """Salva o orcamento (teto + % alerta) por conta. Exige 1 filial.

    Conta com teto <= 0 e removida da configuracao (deixa de ser orcada).
    """
    fid = id_filial if isinstance(id_filial, int) else None
    if fid is None:
        raise ValueError("budget_config_upsert requires a single filial")
    saved = 0
    removed = 0
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(fid)) as conn:
        for it in items or []:
            try:
                idpc = int(it.get("id_plano_conta"))
            except (TypeError, ValueError):
                continue
            vmax = float(it.get("valor_max") or 0)
            apct = int(it.get("alerta_pct") or 90)
            apct = max(1, min(100, apct))
            if vmax <= 0:
                conn.execute(
                    "DELETE FROM app.budget_conta WHERE id_empresa=%s AND id_filial=%s AND id_plano_conta=%s",
                    [id_empresa, fid, idpc],
                )
                removed += 1
            else:
                conn.execute(
                    """
                    INSERT INTO app.budget_conta (id_empresa, id_filial, id_plano_conta, valor_max, alerta_pct, updated_at)
                    VALUES (%s, %s, %s, %s, %s, now())
                    ON CONFLICT (id_empresa, id_filial, id_plano_conta) DO UPDATE SET
                      valor_max = EXCLUDED.valor_max, alerta_pct = EXCLUDED.alerta_pct, updated_at = now()
                    """,
                    [id_empresa, fid, idpc, round(vmax, 2), apct],
                )
                saved += 1
        conn.commit()
    return {"saved": saved, "removed": removed}


def budget_overview(role: str, id_empresa: int, id_filial: Optional[int], ano: int, mes: int, **kwargs: Any) -> Dict[str, Any]:
    """Realizado x orcado — tetos em app.budget_conta (OLTP); realizado no ClickHouse."""
    from app.db_clickhouse import query_dict

    where_filial, branch_params = _branch_scope_clause("b.id_filial", id_filial)
    source = "clickhouse"
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        budget_rows = [dict(r) for r in conn.execute(
            f"""
            SELECT b.id_filial, b.id_plano_conta, b.valor_max::numeric(18,2) AS valor_max,
                   b.alerta_pct::int AS alerta_pct,
                   COALESCE(g.codigo, '') AS codigo,
                   COALESCE(g.nome_conta, '') AS nome_conta
            FROM app.budget_conta b
            LEFT JOIN mart.plano_contas_gerencial g
              ON g.id_empresa = b.id_empresa AND g.id_filial = b.id_filial
             AND g.id_plano_conta = b.id_plano_conta
            WHERE b.id_empresa = %s
              {where_filial}
              AND b.valor_max > 0
            ORDER BY b.id_filial, g.nome_conta
            """,
            [id_empresa] + branch_params,
        ).fetchall()]
        filial_nome_map = {
            int(r["id_filial"]): r.get("nome")
            for r in conn.execute("SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s", [id_empresa]).fetchall()
            if r.get("id_filial") is not None
        }

    realizado_map: Dict[Tuple[int, int], float] = {}
    try:
        branch_ids = _branch_ids(id_filial)
        params_ch: Dict[str, Any] = {"id_empresa": int(id_empresa), "ano": int(ano), "mes": int(mes)}
        filial_sql = ""
        if branch_ids:
            if len(branch_ids) == 1:
                filial_sql = "AND id_filial = %(id_filial)s"
                params_ch["id_filial"] = branch_ids[0]
            else:
                filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)
        ch_rows = query_dict(
            f"""
            SELECT id_filial, id_plano_conta, sum(valor_realizado) AS valor_realizado
            FROM torqmind_mart_rt.mart_despesa_conta_mensal FINAL
            WHERE id_empresa = %(id_empresa)s AND ano = %(ano)s AND mes = %(mes)s
              {filial_sql}
            GROUP BY id_filial, id_plano_conta
            """,
            params_ch,
        )
        for r in ch_rows:
            realizado_map[(int(r["id_filial"]), int(r["id_plano_conta"]))] = float(r.get("valor_realizado") or 0)
        if not ch_rows and budget_rows:
            # CH vazio: fallback PG realizado
            source = "postgres"
            with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
                for r in conn.execute(
                    f"""
                    SELECT id_filial, id_plano_conta, COALESCE(valor_realizado,0) AS valor_realizado
                    FROM mart.despesa_conta_mensal
                    WHERE id_empresa = %s AND ano = %s AND mes = %s
                      {_branch_scope_clause("id_filial", id_filial)[0]}
                    """,
                    [id_empresa, int(ano), int(mes)] + _branch_scope_clause("id_filial", id_filial)[1],
                ).fetchall():
                    realizado_map[(int(r["id_filial"]), int(r["id_plano_conta"]))] = float(r.get("valor_realizado") or 0)
    except Exception as exc:
        logging.getLogger(__name__).warning("budget_overview CH failed: %s", str(exc)[:200])
        source = "postgres"

    contas = []
    total_orcado = 0.0
    total_realizado = 0.0
    for r in budget_rows:
        fidr = int(r.get("id_filial") or 0)
        idpc = int(r.get("id_plano_conta") or 0)
        orcado = round(float(r.get("valor_max") or 0), 2)
        realizado = round(float(realizado_map.get((fidr, idpc), 0)), 2)
        alerta_pct = int(r.get("alerta_pct") or 90)
        pct, status = _budget_status(realizado, orcado, alerta_pct)
        total_orcado += orcado
        total_realizado += realizado
        contas.append({
            "id_filial": fidr,
            "filial_label": _filial_label(fidr, filial_nome_map.get(fidr)),
            "id_plano_conta": idpc,
            "codigo": r.get("codigo") or "",
            "nome_conta": r.get("nome_conta") or "",
            "orcado": orcado,
            "realizado": realizado,
            "saldo": round(orcado - realizado, 2),
            "consumo_pct": pct,
            "alerta_pct": alerta_pct,
            "status": status,
        })

    return {
        "ano": int(ano),
        "mes": int(mes),
        "contas": contas,
        "summary": {
            "total_orcado": round(total_orcado, 2),
            "total_realizado": round(total_realizado, 2),
            "saldo": round(total_orcado - total_realizado, 2),
            "contas_em_alerta": sum(1 for c in contas if c["status"] in ("alerta", "estourado")),
            "contas_estouradas": sum(1 for c in contas if c["status"] == "estourado"),
        },
        "source": source,
    }


def budget_alerts(role: str, id_empresa: int, id_filial: Optional[int], ano: int, mes: int, **kwargs: Any) -> Dict[str, Any]:
    """Contas de despesa chegando/passando do teto no mes (alerta do Dashboard)."""
    overview = budget_overview(role, id_empresa, id_filial, ano, mes)
    alerts = [
        {
            "id_filial": c["id_filial"],
            "filial_label": c["filial_label"],
            "nome_conta": c["nome_conta"],
            "orcado": c["orcado"],
            "realizado": c["realizado"],
            "consumo_pct": c["consumo_pct"],
            "status": c["status"],
        }
        for c in overview.get("contas", [])
        if c["status"] in ("alerta", "estourado")
    ]
    alerts.sort(key=lambda a: a["consumo_pct"], reverse=True)
    return {
        "ano": int(ano),
        "mes": int(mes),
        "alerts": alerts,
        "total_alertas": len(alerts),
        "source": overview.get("source") or "clickhouse",
    }


_MESES_PTBR = [
    "", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]


def _month_label_ptbr(ano_mes: int) -> str:
    ano = ano_mes // 100
    mes = ano_mes % 100
    return f"{_MESES_PTBR[mes]}/{ano}" if 1 <= mes <= 12 else str(ano_mes)


_SOLVENCIA_SECAO_LABEL = {
    "combustivel": "Combustível",
    "estoque": "Estoque Loja",
    "aprazo": "A Prazo",
    "cartoes": "Cartões",
    "cheques": "Cheques",
    "dinheiro": "Dinheiro em Espécie",
    "banco": "Bancos",
    "investimento": "Investimentos",
    "boleto": "Contas a Pagar",
    "havel": "Havel Clientes",
    "despesas": "Despesas",
    "outro": "Outros",
}
_SOLVENCIA_GRUPO_LABEL = {
    "ativo_circulante": "Ativo Circulante",
    "ativo_nao_circulante": "Ativo Não Circulante",
    "passivo_circulante": "Passivo Circulante",
}

# Seções auto substituídas pela posição as-of do mês (mart.liquidez_solvencia).
# Ordem alinhada ao refresh_solvencia_itens / tela Fechamento de Caixa.
_SOLVENCIA_ASOF_ORDEM = {
    "combustivel": 10,
    "estoque": 20,
    "aprazo": 30,
    "cartoes": 40,
    "havel": 42,
    "cheques": 50,
    "dinheiro": 55,
    "banco": 60,
    "boleto": 90,
    "despesas": 92,
}

_BANCO_FEBRABAN = {
    "1": "Banco do Brasil",
    "33": "Santander",
    "104": "Caixa Econômica",
    "237": "Bradesco",
    "341": "Itaú",
    "399": "HSBC",
    "748": "Sicredi",
    "756": "Sicoob",
    "84": "Uniprime",
    "85": "Ailos",
    "133": "Cresol",
}


def _nome_banco(codigo: Optional[str]) -> str:
    raw = (codigo or "").strip()
    if not raw:
        return "Banco não informado"
    return _BANCO_FEBRABAN.get(raw, f"Banco {raw}")


def _collapse_secao(s: Dict[str, Any], *, label: Optional[str] = None) -> None:
    """Mantém só o total na linha; detalhes vão para hint_itens (hover)."""
    if label:
        s["label"] = label
    detalhes = list(s.get("itens") or [])
    if not detalhes and s.get("hint_itens"):
        return
    if detalhes:
        s["hint_itens"] = [
            {
                "label": it.get("label"),
                "valor": round(float(it.get("valor") or 0), 2),
                "qtd": it.get("qtd"),
            }
            for it in detalhes
        ]
    s["itens"] = []
    s["colapsado"] = True



def solvencia_detalhada(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    ano_mes: int,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Aba Solvência detalhada (formato "Fechamento de Caixa Geral").

    Monta, por filial, os grupos Ativo Circulante / Ativo Não Circulante /
    Passivo Circulante. Itens AUTO de snapshot (`mart.solvencia_item`) são
    sobrescritos, no mês selecionado, pela posição as-of de abertura
    (`mart.liquidez_solvencia`: dinheiro D-1, bancos as-of, cartões a receber, estoque loja /
    combustível, passivo do mês). Manuais (investimentos/outros e override de bancos/dinheiro)
    continuam por mês em `app.solvencia_entrada_manual`.

    ``ativos_do_mes=True`` (default): cheques e a prazo só com vencimento no
    mês e ainda abertos. ``False``: posição aberta completa (snapshot).
    """
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    target = int(ano_mes)
    ano, mes = target // 100, target % 100
    ativos_do_mes = bool(kwargs.get("ativos_do_mes", True))
    mes_ini = f"{ano:04d}-{mes:02d}-01"
    source = "clickhouse"

    from app.db_clickhouse import query_dict

    branch_ids = _branch_ids(id_filial)
    params_ch: Dict[str, Any] = {"id_empresa": int(id_empresa), "ano_mes": target}
    filial_sql = ""
    if branch_ids:
        if len(branch_ids) == 1:
            filial_sql = "AND id_filial = %(id_filial)s"
            params_ch["id_filial"] = branch_ids[0]
        else:
            filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)

    auto_rows: list = []
    asof_rows: list = []
    banco_conta_rows: list = []
    meses_asof_rows: list = []
    cheques_mes_rows: list = []
    aprazo_mes_rows: list = []
    try:
        auto_rows = query_dict(
            f"""
            SELECT id_filial, grupo, secao, item_label, valor, qtd, ordem
            FROM torqmind_mart_rt.mart_solvencia_item FINAL
            WHERE id_empresa = %(id_empresa)s {filial_sql}
            ORDER BY id_filial, grupo, ordem, valor DESC
            """,
            params_ch,
        )
        asof_rows = query_dict(
            f"""
            SELECT
              id_filial, ativo_caixa, ativo_banco, ativo_cartoes, ativo_cheques,
              ativo_estoque, ativo_estoque_combustivel, ativo_estoque_loja,
              ativo_cartoes_credito, ativo_cartoes_debito,
              passivo_contas_pagar, tem_ativo_dados
            FROM torqmind_mart_rt.mart_liquidez_solvencia FINAL
            WHERE id_empresa = %(id_empresa)s AND ano_mes = %(ano_mes)s {filial_sql}
            ORDER BY id_filial
            """,
            params_ch,
        )
        banco_conta_rows = query_dict(
            f"""
            SELECT id_filial, id_contasbancarias, banco_nome, agencia, nro_conta,
                   descricao, ativo, saldo
            FROM torqmind_mart_rt.mart_solvencia_banco_conta FINAL
            WHERE id_empresa = %(id_empresa)s AND ano_mes = %(ano_mes)s {filial_sql}
            ORDER BY id_filial, abs(saldo) DESC, id_contasbancarias
            """,
            params_ch,
        )
        meses_asof_rows = query_dict(
            f"""
            SELECT DISTINCT ano_mes FROM torqmind_mart_rt.mart_liquidez_solvencia FINAL
            WHERE id_empresa = %(id_empresa)s {filial_sql} ORDER BY ano_mes
            """,
            params_ch,
        )
        if ativos_do_mes:
            params_ch["mes_ini"] = mes_ini
            cheques_mes_rows = query_dict(
                f"""
                SELECT id_filial, if(banco = '', '?', banco) AS banco,
                       sum(v) AS valor, count() AS qtd
                FROM (
                  SELECT id_filial, banco, valor AS v
                  FROM torqmind_mart_rt.mart_cheques_pendentes FINAL
                  WHERE id_empresa = %(id_empresa)s {filial_sql}
                    AND status_cheque IN ('a_compensar', 'depositado')
                    AND dt_vencimento IS NOT NULL
                    AND dt_vencimento >= toDate(%(mes_ini)s)
                    AND dt_vencimento < addMonths(toDate(%(mes_ini)s), 1)
                )
                GROUP BY id_filial, banco
                ORDER BY id_filial, valor DESC
                """,
                params_ch,
            )
            aprazo_mes_rows = query_dict(
                f"""
                SELECT id_filial, valor, qtd
                FROM torqmind_mart_rt.mart_solvencia_aprazo_mes FINAL
                WHERE id_empresa = %(id_empresa)s {filial_sql}
                  AND ano_mes = %(ano_mes)s
                ORDER BY id_filial
                """,
                params_ch,
            )
        if not auto_rows and not asof_rows:
            raise RuntimeError("CH solvencia empty")
    except Exception as exc:
        logging.getLogger(__name__).warning("solvencia_detalhada CH failed: %s", str(exc)[:200])
        source = "postgres"
        auto_rows = asof_rows = banco_conta_rows = meses_asof_rows = []
        cheques_mes_rows = aprazo_mes_rows = []

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        if source != "clickhouse":
            auto_rows = conn.execute(
                f"""
                SELECT id_filial, grupo, secao, item_label, valor, qtd, ordem
                FROM mart.solvencia_item
                WHERE id_empresa = %s {where_filial}
                ORDER BY id_filial, grupo, ordem, valor DESC
                """,
                [id_empresa] + branch_params,
            ).fetchall()
            asof_rows = conn.execute(
                f"""
                SELECT
                  id_filial,
                  ativo_caixa, ativo_banco, ativo_cartoes, ativo_cheques,
                  ativo_estoque, ativo_estoque_combustivel, ativo_estoque_loja,
                  COALESCE(ativo_cartoes_credito, 0) AS ativo_cartoes_credito,
                  COALESCE(ativo_cartoes_debito, 0) AS ativo_cartoes_debito,
                  passivo_contas_pagar, tem_ativo_dados
                FROM mart.liquidez_solvencia
                WHERE id_empresa = %s AND ano_mes = %s {where_filial}
                ORDER BY id_filial
                """,
                [id_empresa, target] + branch_params,
            ).fetchall()
            banco_conta_rows = conn.execute(
                f"""
                SELECT id_filial, id_contasbancarias, banco_nome, agencia, nro_conta,
                       descricao, ativo, saldo
                FROM mart.solvencia_banco_conta
                WHERE id_empresa = %s AND ano_mes = %s {where_filial}
                ORDER BY id_filial, ABS(saldo) DESC, id_contasbancarias
                """,
                [id_empresa, target] + branch_params,
            ).fetchall()
            meses_asof_rows = conn.execute(
                f"""
                SELECT DISTINCT ano_mes FROM mart.liquidez_solvencia
                WHERE id_empresa = %s {where_filial} ORDER BY 1
                """,
                [id_empresa] + branch_params,
            ).fetchall()
            if ativos_do_mes:
                cheques_mes_rows = conn.execute(
                    f"""
                    SELECT id_filial, COALESCE(NULLIF(banco, ''), '?') AS banco,
                           SUM(valor)::numeric(18,2) AS valor, COUNT(*)::numeric AS qtd
                    FROM mart.cheques_pendentes
                    WHERE id_empresa = %s {where_filial}
                      AND status_cheque IN ('a_compensar', 'depositado')
                      AND dt_vencimento IS NOT NULL
                      AND dt_vencimento >= %s::date
                      AND dt_vencimento < (%s::date + interval '1 month')
                    GROUP BY id_filial, COALESCE(NULLIF(banco, ''), '?')
                    ORDER BY id_filial, SUM(valor) DESC
                    """,
                    [id_empresa] + branch_params + [mes_ini, mes_ini],
                ).fetchall()
                aprazo_mes_rows = conn.execute(
                    f"""
                    SELECT id_filial,
                           SUM(GREATEST(
                             etl.safe_numeric(payload->>'VALOR')
                               - COALESCE(etl.safe_numeric(payload->>'VLRPAGO'), 0), 0
                           ))::numeric(18,2) AS valor,
                           COUNT(*)::numeric AS qtd
                    FROM stg.contasreceber
                    WHERE id_empresa = %s {where_filial}
                      AND payload->>'DTAPGTO' IS NULL
                      AND etl.safe_timestamp(payload->>'DTAVCTO') IS NOT NULL
                      AND (etl.safe_timestamp(payload->>'DTAVCTO'))::date >= %s::date
                      AND (etl.safe_timestamp(payload->>'DTAVCTO'))::date < (%s::date + interval '1 month')
                    GROUP BY id_filial
                    """,
                    [id_empresa] + branch_params + [mes_ini, mes_ini],
                ).fetchall()

        tipos = {
            int(t["id_tipo"]): dict(t)
            for t in conn.execute(
                "SELECT id_tipo, chave, nome, grupo, secao, ordem FROM app.solvencia_tipo_manual"
            ).fetchall()
        }

        manual_rows = conn.execute(
            f"""
            SELECT id_filial, id_tipo, descricao, valor, ordem, id
            FROM app.solvencia_entrada_manual
            WHERE id_empresa = %s AND ano = %s AND mes = %s AND ativo {where_filial}
            ORDER BY id_filial, id_tipo, ordem, id
            """,
            [id_empresa, ano, mes] + branch_params,
        ).fetchall()

        filial_nome_map = {
            int(r["id_filial"]): r.get("nome")
            for r in conn.execute(
                "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s",
                [id_empresa],
            ).fetchall()
            if r.get("id_filial") is not None
        }

        meses_rows = conn.execute(
            f"""
            SELECT DISTINCT (ano * 100 + mes) AS ano_mes
            FROM app.solvencia_entrada_manual
            WHERE id_empresa = %s {where_filial}
            ORDER BY 1
            """,
            [id_empresa] + branch_params,
        ).fetchall()

    # secao -> (grupo, editavel, id_tipo) para os painéis manuais
    manual_secao = {}
    for tid, t in tipos.items():
        manual_secao[t["secao"]] = {"grupo": t["grupo"], "id_tipo": tid, "nome": t["nome"], "ordem": int(t.get("ordem") or 0)}

    # Estrutura por filial: {id_filial: {grupo: {secao: {label, total, itens, editavel, id_tipo, ordem}}}}
    filiais: Dict[int, Dict[str, Any]] = {}

    def _secao(fid: int, grupo: str, secao: str) -> Dict[str, Any]:
        f = filiais.setdefault(fid, {})
        g = f.setdefault(grupo, {})
        if secao not in g:
            ms = manual_secao.get(secao)
            g[secao] = {
                "secao": secao,
                "label": _SOLVENCIA_SECAO_LABEL.get(secao, secao.title()),
                "total": 0.0,
                "itens": [],
                "editavel": bool(ms) and secao != "combustivel",
                "id_tipo": (ms or {}).get("id_tipo"),
                "ordem": 0,
            }
        return g[secao]

    def _set_asof_secao(
        fid: int,
        grupo: str,
        secao: str,
        item_label: str,
        valor: float,
        *,
        qtd: Optional[float] = None,
    ) -> None:
        """Substitui a seção auto pela posição as-of do mês (um item consolidado)."""
        s = _secao(fid, grupo, secao)
        s["editavel"] = False
        s["id_tipo"] = None
        s["ordem"] = int(_SOLVENCIA_ASOF_ORDEM.get(secao, s.get("ordem") or 0))
        s["itens"] = [{
            "label": item_label,
            "valor": round(float(valor or 0), 2),
            "qtd": float(qtd) if qtd is not None else None,
            "origem": "auto",
            "editavel": False,
            "as_of": True,
        }]
        s["total"] = round(float(valor or 0), 2)

    # Snapshot corrente. Cheques: renomeia código FEBRABAN → nome do banco.
    havel_by_fid: Dict[int, Dict[str, float]] = {}
    for r in auto_rows:
        fid = int(r["id_filial"])
        if r["secao"] == "havel":
            bucket = havel_by_fid.setdefault(fid, {"valor": 0.0, "qtd": 0.0})
            bucket["valor"] = round(bucket["valor"] + float(r["valor"] or 0), 2)
            if r.get("qtd") is not None:
                bucket["qtd"] = float(bucket["qtd"]) + float(r["qtd"])
            continue
        s = _secao(fid, r["grupo"], r["secao"])
        s["ordem"] = int(r["ordem"] or 0)
        val = float(r["valor"] or 0)
        label = r["item_label"]
        if r["secao"] == "cheques":
            # "Banco 237" / código puro → nome FEBRABAN
            codigo = str(label or "").replace("Banco ", "").strip()
            label = _nome_banco(codigo)
        s["itens"].append({
            "label": label,
            "valor": round(val, 2),
            "qtd": float(r["qtd"]) if r.get("qtd") is not None else None,
            "origem": "auto",
            "editavel": False,
        })
        s["total"] = round(s["total"] + val, 2)

    # Ativos do mês: cheques/a prazo só com vencimento no mês e ainda abertos.
    if ativos_do_mes:
        fids_cheques = {
            int(r["id_filial"]) for r in auto_rows if r.get("secao") == "cheques"
        } | {int(r["id_filial"]) for r in cheques_mes_rows}
        for fid in fids_cheques:
            s = _secao(fid, "ativo_circulante", "cheques")
            s["itens"] = []
            s["total"] = 0.0
            s["ordem"] = int(_SOLVENCIA_ASOF_ORDEM.get("cheques", 50))
            s["editavel"] = False
            s["id_tipo"] = None
        for r in cheques_mes_rows:
            fid = int(r["id_filial"])
            s = _secao(fid, "ativo_circulante", "cheques")
            val = float(r["valor"] or 0)
            s["itens"].append({
                "label": _nome_banco(str(r.get("banco") or "")),
                "valor": round(val, 2),
                "qtd": float(r["qtd"]) if r.get("qtd") is not None else None,
                "origem": "auto",
                "editavel": False,
                "ativos_do_mes": True,
            })
            s["total"] = round(s["total"] + val, 2)

        fids_aprazo = {
            int(r["id_filial"]) for r in auto_rows if r.get("secao") == "aprazo"
        } | {int(r["id_filial"]) for r in aprazo_mes_rows}
        for fid in fids_aprazo:
            s = _secao(fid, "ativo_circulante", "aprazo")
            s["itens"] = []
            s["total"] = 0.0
            s["ordem"] = int(_SOLVENCIA_ASOF_ORDEM.get("aprazo", 30))
            s["editavel"] = False
            s["id_tipo"] = None
        for r in aprazo_mes_rows:
            fid = int(r["id_filial"])
            val = float(r["valor"] or 0)
            s = _secao(fid, "ativo_circulante", "aprazo")
            s["itens"] = [{
                "label": f"A Prazo (venc. {_month_label_ptbr(target)})",
                "valor": round(val, 2),
                "qtd": float(r["qtd"]) if r.get("qtd") is not None else None,
                "origem": "auto",
                "editavel": False,
                "ativos_do_mes": True,
            }]
            s["total"] = round(val, 2)

    def _set_havel(fid: int) -> None:
        hv = havel_by_fid.get(fid)
        if not hv or abs(float(hv.get("valor") or 0)) < 0.005:
            return
        # Linha própria (separada de Cartões), valor negativo = crédito antecipado.
        _set_asof_secao(
            fid, "ativo_circulante", "havel",
            "Havel Clientes",
            float(hv["valor"]),
            qtd=hv.get("qtd"),
        )
        # _set_asof força as_of=True; Havel é snapshot de CREDITO.
        s = _secao(fid, "ativo_circulante", "havel")
        if s["itens"]:
            s["itens"][0]["as_of"] = False

    # Overlay as-of (abertura dia 1 00:00 SP).
    asof_fids: set[int] = set()
    cartoes_hint: Dict[int, List[Dict[str, Any]]] = {}
    bancos_hint: Dict[int, List[Dict[str, Any]]] = {}
    for r in banco_conta_rows:
        fid = int(r["id_filial"])
        if r.get("ativo") is False:
            continue
        saldo = round(float(r.get("saldo") or 0), 2)
        if abs(saldo) < 0.005:
            continue
        banco = str(r.get("banco_nome") or "Banco").strip()
        desc = str(r.get("descricao") or "").strip()
        nro = str(r.get("nro_conta") or "").strip()
        label = desc or (f"{banco} · C/C {nro}" if nro else banco)
        bancos_hint.setdefault(fid, []).append(
            {"label": label, "valor": saldo, "qtd": None}
        )

    _asof_secoes_overlay = {"dinheiro", "banco", "cartoes", "estoque", "boleto"}
    for r in asof_rows:
        fid = int(r["id_filial"])
        asof_fids.add(fid)
        caixa = float(r.get("ativo_caixa") or 0)
        banco = float(r.get("ativo_banco") or 0)
        cartoes = float(r.get("ativo_cartoes") or 0)
        cart_cr = float(r.get("ativo_cartoes_credito") or 0)
        cart_db = float(r.get("ativo_cartoes_debito") or 0)
        est_loja = float(r.get("ativo_estoque_loja") or 0)
        est_comb = float(r.get("ativo_estoque_combustivel") or 0)
        passivo = float(r.get("passivo_contas_pagar") or 0)

        _set_asof_secao(
            fid, "ativo_circulante", "dinheiro",
            "Dinheiro em espécie (fechamento D−1)", caixa,
        )
        # Dinheiro: valor de sistema editável; override manual marca "editado".
        s_din = _secao(fid, "ativo_circulante", "dinheiro")
        s_din["valor_sistema"] = round(caixa, 2)
        s_din["editado_humano"] = False
        if "dinheiro" in manual_secao:
            s_din["editavel"] = True
            s_din["id_tipo"] = manual_secao["dinheiro"]["id_tipo"]

        _set_asof_secao(
            fid, "ativo_circulante", "banco",
            "Saldos bancários (abertura do mês)", banco,
        )
        s_banco = _secao(fid, "ativo_circulante", "banco")
        s_banco["valor_sistema"] = round(banco, 2)
        s_banco["editado_humano"] = False
        if bancos_hint.get(fid):
            s_banco["hint_itens"] = list(bancos_hint[fid])
            s_banco["itens"] = [
                {
                    "label": h["label"],
                    "valor": h["valor"],
                    "qtd": None,
                    "origem": "auto",
                    "editavel": False,
                    "as_of": True,
                }
                for h in bancos_hint[fid]
            ]
            s_banco["colapsado"] = True
        if "banco" in manual_secao:
            s_banco["editavel"] = True
            s_banco["id_tipo"] = manual_secao["banco"]["id_tipo"]

        _set_asof_secao(
            fid, "ativo_circulante", "cartoes",
            "Cartões a receber", cartoes,
        )
        hint_c: List[Dict[str, Any]] = []
        if cart_cr > 0.005:
            hint_c.append({"label": "Crédito", "valor": round(cart_cr, 2), "qtd": None})
        if cart_db > 0.005:
            hint_c.append({"label": "Débito", "valor": round(cart_db, 2), "qtd": None})
        residual = round(cartoes - cart_cr - cart_db, 2)
        if hint_c and abs(residual) > 0.05:
            hint_c.append({"label": "Convênio / outros", "valor": residual, "qtd": None})
        elif not hint_c and cartoes > 0.005:
            # Split ainda não materializado no mês — hint honesto sem inventar tipo.
            hint_c.append({"label": "Cartões a receber", "valor": round(cartoes, 2), "qtd": None})
        if hint_c:
            cartoes_hint[fid] = sorted(hint_c, key=lambda x: -x["valor"])
        _set_havel(fid)
        _set_asof_secao(
            fid, "ativo_circulante", "estoque",
            "Estoque loja (ESTOQUE − mov. após abertura)", est_loja,
        )
        if est_comb > 0:
            _asof_secoes_overlay.add("combustivel")
            _set_asof_secao(
                fid, "ativo_circulante", "combustivel",
                "Combustível (leitura tanque antes da abertura)", est_comb,
            )
        _set_asof_secao(
            fid, "passivo_circulante", "boleto",
            "Contas a pagar (e despesas vencendo no mês)", passivo,
        )

    for fid in havel_by_fid:
        if fid not in asof_fids:
            _set_havel(fid)

    for r in manual_rows:
        fid = int(r["id_filial"])
        ms = tipos.get(int(r["id_tipo"]))
        if not ms:
            continue
        s = _secao(fid, ms["grupo"], ms["secao"])
        # Dinheiro: manual sobrescreve o as-of e marca edição humana.
        if ms["secao"] == "dinheiro":
            val = float(r["valor"] or 0)
            sistema = float(s.get("valor_sistema") or s.get("total") or 0)
            s["itens"] = [{
                "id": int(r["id"]),
                "label": r["descricao"] or "Dinheiro em espécie (editado)",
                "valor": round(val, 2),
                "qtd": None,
                "origem": "manual",
                "editavel": True,
                "editado_humano": True,
                "valor_sistema": sistema,
            }]
            s["total"] = round(val, 2)
            s["editavel"] = True
            s["id_tipo"] = int(r["id_tipo"])
            s["editado_humano"] = True
            s["valor_sistema"] = sistema
            continue
        # Bancos: lista manual substitui o as-of (mantém valor_sistema para auditoria).
        if ms["secao"] == "banco":
            if not s.get("editado_humano"):
                s["valor_sistema"] = float(s.get("valor_sistema") or s.get("total") or 0)
                s["itens"] = []
                s["total"] = 0.0
                s["hint_itens"] = None
                s["colapsado"] = False
                s["editado_humano"] = True
            val = float(r["valor"] or 0)
            s["itens"].append({
                "id": int(r["id"]),
                "label": r["descricao"] or "Conta bancária",
                "valor": round(val, 2),
                "qtd": None,
                "origem": "manual",
                "editavel": True,
                "editado_humano": True,
            })
            s["total"] = round(float(s["total"]) + val, 2)
            s["editavel"] = True
            s["id_tipo"] = int(r["id_tipo"])
            continue
        if fid in asof_fids and ms["secao"] in _asof_secoes_overlay:
            continue
        s["ordem"] = manual_secao.get(ms["secao"], {}).get("ordem", 0)
        val = float(r["valor"] or 0)
        s["itens"].append({
            "id": int(r["id"]),
            "label": r["descricao"],
            "valor": round(val, 2),
            "qtd": None,
            "origem": "manual",
            "editavel": True,
        })
        s["total"] = round(s["total"] + val, 2)

    # garante os painéis manuais mesmo vazios (para a tela mostrar "clique para preencher")
    all_fids = set(filiais.keys()) | {int(r["id_filial"]) for r in auto_rows} | asof_fids | set(havel_by_fid)
    for fid in all_fids:
        for secao, ms in manual_secao.items():
            _secao(fid, ms["grupo"], secao)

    # Despesas: MESMA fonte do DRE Gerencial — dw.fact_despesa_operacional (PG),
    # classificada pelo plano Xpert 3.2*/3.3*. CH é só fallback legado.
    despesas_by_fid: Dict[int, Dict[str, Any]] = {}
    despesas_ano_mes: Optional[int] = None
    _desp_hint_empty = [
        {"label": "Despesas com Funcionários", "valor": 0.0},
        {"label": "Despesas Comerciais", "valor": 0.0},
        {"label": "Despesas Administrativas", "valor": 0.0},
        {"label": "Tributos Operacionais", "valor": 0.0},
        {"label": "Financeiras/Excepcionais", "valor": 0.0},
    ]
    try:
        scoped = _branch_ids(id_filial)
        branch_ids = scoped if scoped else sorted(int(x) for x in (all_fids or set()))

        def _pg_desp_branch(sql_params: List[Any]) -> str:
            if len(branch_ids) == 1:
                sql_params.append(int(branch_ids[0]))
                return "AND id_filial = %s"
            if branch_ids:
                sql_params.append([int(b) for b in branch_ids])
                return "AND id_filial = ANY(%s)"
            return ""

        # Sempre o mês do seletor (igual DRE) — sem fallback para outro mês.
        despesas_ano_mes = int(target)
        params = [id_empresa, despesas_ano_mes]
        clause = _pg_desp_branch(params)
        with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as c:
            rows = c.execute(
                f"""
                SELECT
                  id_filial,
                  COALESCE(SUM(CASE WHEN classificacao_gerencial = 'pessoal'
                                    THEN valor ELSE 0 END), 0)::float AS desp_pessoal,
                  COALESCE(SUM(CASE WHEN classificacao_gerencial = 'comercial'
                                    THEN valor ELSE 0 END), 0)::float AS desp_comercial,
                  COALESCE(SUM(CASE WHEN classificacao_gerencial = 'administrativo'
                                    THEN valor ELSE 0 END), 0)::float AS desp_administrativa,
                  COALESCE(SUM(CASE WHEN COALESCE(is_tributo_operacional, false)
                                      OR classificacao_gerencial = 'tributos'
                                    THEN valor ELSE 0 END), 0)::float AS desp_tributaria_operacional,
                  COALESCE(SUM(CASE WHEN classificacao_gerencial = 'financeiro'
                                    THEN valor ELSE 0 END), 0)::float AS desp_financeira,
                  COALESCE(SUM(CASE WHEN COALESCE(is_excepcional, false)
                                      OR classificacao_gerencial IN ('excepcional', 'perdas')
                                    THEN valor ELSE 0 END), 0)::float AS desp_excepcional,
                  COALESCE(SUM(valor), 0)::float AS desp_operacional_total
                FROM dw.fact_despesa_operacional
                WHERE id_empresa = %s
                  AND ano_mes_competencia = %s
                  AND COALESCE(entra_dre, true)
                  {clause}
                GROUP BY id_filial
                """,
                params,
            ).fetchall()
        for row in rows or []:
            fid = int(row["id_filial"])
            total = round(float(row.get("desp_operacional_total") or 0), 2)
            despesas_by_fid[fid] = {
                "total": total,
                "hint": [
                    {"label": "Despesas com Funcionários", "valor": round(float(row.get("desp_pessoal") or 0), 2)},
                    {"label": "Despesas Comerciais", "valor": round(float(row.get("desp_comercial") or 0), 2)},
                    {"label": "Despesas Administrativas", "valor": round(float(row.get("desp_administrativa") or 0), 2)},
                    {"label": "Tributos Operacionais", "valor": round(float(row.get("desp_tributaria_operacional") or 0), 2)},
                    {"label": "Financeiras/Excepcionais", "valor": round(
                        float(row.get("desp_financeira") or 0) + float(row.get("desp_excepcional") or 0), 2
                    )},
                ],
            }
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).exception(
            "solvencia_detalhada: falha ao buscar despesas PG (empresa=%s mes=%s): %s",
            id_empresa, target, exc,
        )
        despesas_by_fid = {}
        despesas_ano_mes = None

    desp_label = "Despesas"
    if despesas_ano_mes and int(despesas_ano_mes) != int(target):
        desp_label = f"Despesas ({_month_label_ptbr(int(despesas_ano_mes))})"

    for fid in all_fids:
        desp = despesas_by_fid.get(fid) or {"total": 0.0, "hint": list(_desp_hint_empty)}
        s = _secao(fid, "passivo_circulante", "despesas")
        s["ordem"] = _SOLVENCIA_ASOF_ORDEM["despesas"]
        s["editavel"] = False
        s["id_tipo"] = None
        s["label"] = desp_label
        s["total"] = float(desp["total"] or 0)
        s["itens"] = []
        s["hint_itens"] = list(desp["hint"])
        s["colapsado"] = True

    for fid in all_fids:
        g = filiais.get(fid, {}).get("ativo_circulante", {})
        if "cheques" in g:
            _collapse_secao(g["cheques"], label="Cheques")
        if "cartoes" in g:
            s = g["cartoes"]
            if fid in cartoes_hint:
                s["hint_itens"] = cartoes_hint[fid]
            elif s.get("total"):
                s["hint_itens"] = [{"label": "Cartões a receber", "valor": round(float(s["total"]), 2), "qtd": None}]
            s["itens"] = []
            s["colapsado"] = True
            s["label"] = "Cartões"
        if "combustivel" in g and len(g["combustivel"].get("itens") or []) > 1:
            _collapse_secao(g["combustivel"], label="Combustível")

    out_filiais = []
    for fid in sorted(all_fids):
        grupos_raw = filiais.get(fid, {})
        grupos_out = {}
        totais = {"ativo_circulante": 0.0, "ativo_nao_circulante": 0.0, "passivo_circulante": 0.0}
        for grupo in ("ativo_circulante", "ativo_nao_circulante", "passivo_circulante"):
            secoes = sorted(grupos_raw.get(grupo, {}).values(), key=lambda s: (s["ordem"], -s["total"]))
            g_total = round(sum(s["total"] for s in secoes), 2)
            totais[grupo] = g_total
            grupos_out[grupo] = {
                "label": _SOLVENCIA_GRUPO_LABEL[grupo],
                "total": g_total,
                "secoes": secoes,
            }
        ativo_total = round(totais["ativo_circulante"] + totais["ativo_nao_circulante"], 2)
        passivo = totais["passivo_circulante"]
        # Estoque = loja + combustível (seções as-of).
        g_ac = grupos_raw.get("ativo_circulante") or {}
        estoque_total = round(
            float((g_ac.get("estoque") or {}).get("total") or 0)
            + float((g_ac.get("combustivel") or {}).get("total") or 0),
            2,
        )
        ativo_sem_estoque = round(ativo_total - estoque_total, 2)
        ativo_com_estoque = ativo_total
        out_filiais.append({
            "id_filial": fid,
            "nome": filial_nome_map.get(fid) or f"Filial {fid}",
            "grupos": grupos_out,
            "totais": {
                "ativo_circulante": totais["ativo_circulante"],
                "ativo_nao_circulante": totais["ativo_nao_circulante"],
                "ativo_total": ativo_total,
                "ativo_com_estoque": ativo_com_estoque,
                "ativo_sem_estoque": ativo_sem_estoque,
                "estoque_total": estoque_total,
                "passivo": passivo,
                "capital_giro": round(ativo_total - passivo, 2),
                "capital_giro_com_estoque": round(ativo_com_estoque - passivo, 2),
                "capital_giro_sem_estoque": round(ativo_sem_estoque - passivo, 2),
                "liquidez_corrente": round(ativo_total / passivo, 4) if passivo > 0 else None,
                "liquidez_sem_estoque": round(ativo_sem_estoque / passivo, 4) if passivo > 0 else None,
                "cobre_passivo": ativo_total >= passivo,
            },
        })

    # Janela navegável: mês corrente + 11 anteriores + manuais + meses com as-of em liquidez.
    hoje = business_today(id_empresa)
    base = max(hoje.year * 100 + hoje.month, target)
    b_ano, b_mes = base // 100, base % 100
    janela = set()
    for _ in range(12):
        janela.add(b_ano * 100 + b_mes)
        b_mes -= 1
        if b_mes == 0:
            b_mes, b_ano = 12, b_ano - 1
    # Mais antigo → mais novo (seletor da Gestão de Lucro).
    meses = sorted(
        janela
        | {int(r["ano_mes"]) for r in meses_rows}
        | {int(r["ano_mes"]) for r in meses_asof_rows}
        | {target},
    )
    return {
        "ano_mes": target,
        "meses_disponiveis": meses,
        "posicao": "as_of_abertura_mes",
        "schema_version": "solvencia_despesas_v2",
        "ativos_do_mes": bool(ativos_do_mes),
        "despesas_ano_mes": despesas_ano_mes,
        "filiais": out_filiais,
        "source": source,
    }


def solvencia_manual_upsert(
    role: str,
    id_empresa: int,
    id_filial: int,
    ano_mes: int,
    id_tipo: int,
    itens: List[Dict[str, Any]],
    **kwargs: Any,
) -> Dict[str, Any]:
    """Substitui os itens manuais de (filial, mês, tipo) pela lista informada.

    Recebe a lista completa do painel (ex.: todos os bancos daquele mês) e faz
    replace atômico: remove os antigos do escopo e regrava os enviados.
    """
    if id_filial is None:
        raise ValueError("solvencia_manual_upsert requer id_filial")
    ano, mes = int(ano_mes) // 100, int(ano_mes) % 100
    if not (1 <= mes <= 12):
        raise ValueError("ano_mes inválido")
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        tipo_row = conn.execute(
            "SELECT id_tipo, chave, secao FROM app.solvencia_tipo_manual WHERE id_tipo = %s",
            [int(id_tipo)],
        ).fetchone()
        if not tipo_row:
            raise ValueError(f"id_tipo desconhecido: {id_tipo}")
        # Dinheiro: apenas 1 valor editável (sem múltiplas linhas).
        itens_in = list(itens or [])
        if str(tipo_row.get("chave") or "") == "dinheiro" or str(tipo_row.get("secao") or "") == "dinheiro":
            if itens_in:
                first = itens_in[0]
                itens_in = [{
                    "descricao": str(first.get("descricao") or first.get("label") or "Dinheiro em espécie").strip()
                    or "Dinheiro em espécie",
                    "valor": first.get("valor") or 0,
                }]
            else:
                itens_in = []
        conn.execute(
            """DELETE FROM app.solvencia_entrada_manual
               WHERE id_empresa=%s AND id_filial=%s AND ano=%s AND mes=%s AND id_tipo=%s""",
            [id_empresa, int(id_filial), ano, mes, int(id_tipo)],
        )
        n = 0
        for ordem, it in enumerate(itens_in):
            desc = str(it.get("descricao") or it.get("label") or "").strip()
            try:
                val = round(float(it.get("valor") or 0), 2)
            except (TypeError, ValueError):
                val = 0.0
            if not desc:
                continue
            conn.execute(
                """INSERT INTO app.solvencia_entrada_manual
                     (id_empresa, id_filial, ano, mes, id_tipo, descricao, valor, ordem)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                [id_empresa, int(id_filial), ano, mes, int(id_tipo), desc[:120], val, ordem],
            )
            n += 1
        conn.commit()
    return {"ok": True, "itens_gravados": n}


def _solvencia_overview_from_ch(
    id_empresa: int,
    id_filial: Optional[int],
    ano_mes: int,
) -> Optional[Dict[str, Any]]:
    """Lê torqmind_mart.solvencia_snapshot_mensal; None se vazio/indisponível."""
    from app.db_clickhouse import query_dict

    branch_ids = None
    if id_filial is not None and int(id_filial) != -1:
        if isinstance(id_filial, (list, tuple, set)):
            branch_ids = [int(v) for v in id_filial if v is not None and int(v) != -1]
        else:
            branch_ids = [int(id_filial)]

    params: Dict[str, Any] = {"id_empresa": int(id_empresa), "ano_mes": int(ano_mes)}
    filial_sql = ""
    if branch_ids:
        if len(branch_ids) == 1:
            filial_sql = "AND id_filial = %(id_filial)s"
            params["id_filial"] = branch_ids[0]
        else:
            filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)

    try:
        meses_params: Dict[str, Any] = {"id_empresa": int(id_empresa)}
        meses_filial_sql = ""
        if branch_ids and len(branch_ids) == 1:
            meses_filial_sql = "AND id_filial = %(id_filial)s"
            meses_params["id_filial"] = branch_ids[0]
        elif branch_ids:
            meses_filial_sql = "AND id_filial IN (%s)" % ", ".join(str(b) for b in branch_ids)

        meses_rows = query_dict(
            f"""
            SELECT DISTINCT ano_mes
            FROM torqmind_mart.solvencia_snapshot_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              {meses_filial_sql}
            ORDER BY ano_mes
            """,
            meses_params,
        )
        rows = query_dict(
            f"""
            SELECT
              id_filial, ano_mes,
              ativo_caixa, ativo_banco, ativo_cartoes, ativo_cheques,
              ativo_estoque, ativo_estoque_combustivel, ativo_estoque_loja,
              passivo_contas_pagar, published_at
            FROM torqmind_mart.solvencia_snapshot_mensal FINAL
            WHERE id_empresa = %(id_empresa)s
              AND ano_mes = %(ano_mes)s
              {filial_sql}
            ORDER BY id_filial
            """,
            params,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "solvencia CH snapshot unavailable: %s", str(exc)[:200]
        )
        return None

    if not rows:
        return None

    meses_disponiveis = sorted({int(r["ano_mes"]) for r in meses_rows}) if meses_rows else [int(ano_mes)]
    tot_caixa = tot_banco = tot_cartoes = tot_cheques = tot_estoque = 0.0
    tot_estoque_comb = tot_estoque_loja = 0.0
    tot_passivo = 0.0
    updated_at = None
    por_filial: List[Dict[str, Any]] = []

    for r in rows:
        fid = int(r["id_filial"])
        caixa = round(float(r.get("ativo_caixa") or 0), 2)
        banco = round(float(r.get("ativo_banco") or 0), 2)
        cartoes = round(float(r.get("ativo_cartoes") or 0), 2)
        cheques = round(float(r.get("ativo_cheques") or 0), 2)
        estoque = round(float(r.get("ativo_estoque") or 0), 2)
        estoque_comb = round(float(r.get("ativo_estoque_combustivel") or 0), 2)
        estoque_loja = round(float(r.get("ativo_estoque_loja") or 0), 2)
        passivo = round(float(r.get("passivo_contas_pagar") or 0), 2)
        circulante = round(caixa + banco + cartoes + cheques + estoque, 2)
        disponivel = round(caixa + banco, 2)
        liquidez = round(circulante / passivo, 4) if passivo > 0 else 0.0
        capital_giro = round(circulante - passivo, 2)
        f_tem_ativo = circulante > 0
        tot_caixa += caixa
        tot_banco += banco
        tot_cartoes += cartoes
        tot_cheques += cheques
        tot_estoque += estoque
        tot_estoque_comb += estoque_comb
        tot_estoque_loja += estoque_loja
        tot_passivo += passivo
        pub = r.get("published_at")
        if pub and (updated_at is None or str(pub) > str(updated_at)):
            updated_at = pub
        por_filial.append({
            "id_filial": fid,
            "filial_label": _filial_label(fid, None),
            "ativo_caixa": caixa,
            "ativo_banco": banco,
            "ativo_cartoes": cartoes,
            "ativo_cheques": cheques,
            "ativo_estoque": estoque,
            "ativo_estoque_combustivel": estoque_comb,
            "ativo_estoque_loja": estoque_loja,
            "estoque_combustivel_medido": estoque_comb > 0,
            "ativo_disponivel": disponivel,
            "ativo_circulante": circulante,
            "passivo_contas_pagar": passivo,
            "passivo_vencido": 0.0,
            "passivo_qtd_titulos": 0,
            "liquidez_corrente": liquidez,
            "capital_giro_liquido": capital_giro,
            "cobre_passivo": (f_tem_ativo and circulante >= passivo),
            "tem_ativo_dados": f_tem_ativo,
        })

    ativo_circulante = round(tot_caixa + tot_banco + tot_cartoes + tot_cheques + tot_estoque, 2)
    ativo_disponivel = round(tot_caixa + tot_banco, 2)
    tot_passivo = round(tot_passivo, 2)
    tem_ativo_dados = ativo_circulante > 0
    return {
        "ano_mes": int(ano_mes),
        "mes_label": _month_label_ptbr(int(ano_mes)),
        "consolidado": len(por_filial) > 1,
        "filiais_count": len(por_filial),
        "meses_disponiveis": [
            {"ano_mes": m, "label": _month_label_ptbr(m)} for m in meses_disponiveis
        ],
        "ativo": {
            "caixa": round(tot_caixa, 2),
            "banco": round(tot_banco, 2),
            "cartoes": round(tot_cartoes, 2),
            "cheques": round(tot_cheques, 2),
            "estoque": round(tot_estoque, 2),
            "estoque_combustivel": round(tot_estoque_comb, 2),
            "estoque_loja": round(tot_estoque_loja, 2),
            "disponivel": ativo_disponivel,
            "circulante": ativo_circulante,
        },
        "cobertura_estoque": {
            "postos_com_combustivel": sum(1 for p in por_filial if p["estoque_combustivel_medido"]),
            "postos_total": len(por_filial),
            "ultima_leitura": "",
        },
        "passivo": {
            "contas_pagar": tot_passivo,
            "qtd_titulos": 0,
            "vencido": 0.0,
        },
        "indices": {
            "liquidez_corrente": round(ativo_circulante / tot_passivo, 4) if tot_passivo > 0 else 0.0,
            "capital_giro_liquido": round(ativo_circulante - tot_passivo, 2),
            "cobre_passivo": bool(tem_ativo_dados and ativo_circulante >= tot_passivo),
        },
        "tem_ativo_dados": tem_ativo_dados,
        "por_filial": por_filial,
        "freshness": str(updated_at) if updated_at else "",
        "disclaimer": (
            "Solvência gerencial (ClickHouse snapshot mensal). "
            "Indicador gerencial, não é balanço contábil oficial."
        ),
        "source": "clickhouse",
    }


def solvencia_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    ano_mes: int,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Solvencia de curto prazo (aba Solvencia do DRE Gerencial).

    Leitura canônica: ClickHouse ``torqmind_mart.solvencia_snapshot_mensal``.
    Fallback: mart.liquidez_solvencia (PG) se snapshot CH vazio.
    """
    target = int(ano_mes)
    ch_payload = _solvencia_overview_from_ch(id_empresa, id_filial, target)
    if ch_payload is not None:
        return ch_payload

    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        meses_rows = conn.execute(
            f"""
            SELECT DISTINCT ano_mes
            FROM mart.liquidez_solvencia
            WHERE id_empresa = %s
              {where_filial}
            ORDER BY ano_mes
            """,
            [id_empresa] + branch_params,
        ).fetchall()
        meses_disponiveis = [int(r["ano_mes"]) for r in meses_rows]

        rows = conn.execute(
            f"""
            SELECT
              id_filial,
              passivo_contas_pagar, passivo_qtd_titulos, passivo_vencido,
              ativo_caixa, ativo_banco, ativo_cartoes, ativo_cheques, ativo_estoque,
              ativo_estoque_combustivel, ativo_estoque_loja,
              estoque_combustivel_medido, estoque_data_leitura,
              tem_ativo_dados, updated_at
            FROM mart.liquidez_solvencia
            WHERE id_empresa = %s
              {where_filial}
              AND ano_mes = %s
            ORDER BY id_filial
            """,
            [id_empresa] + branch_params + [target],
        ).fetchall()

        filial_nome_map = {
            int(r["id_filial"]): r.get("nome")
            for r in conn.execute(
                "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s",
                [id_empresa],
            ).fetchall()
            if r.get("id_filial") is not None
        }

    tot_caixa = tot_banco = tot_cartoes = tot_cheques = tot_estoque = 0.0
    tot_estoque_comb = tot_estoque_loja = 0.0
    tot_passivo = tot_vencido = 0.0
    tot_titulos = 0
    postos_comb_medido = 0
    ult_leitura = None
    tem_ativo_dados = False
    updated_at = None
    por_filial: List[Dict[str, Any]] = []

    for r in rows:
        fid = int(r["id_filial"])
        caixa = round(float(r.get("ativo_caixa") or 0), 2)
        banco = round(float(r.get("ativo_banco") or 0), 2)
        cartoes = round(float(r.get("ativo_cartoes") or 0), 2)
        cheques = round(float(r.get("ativo_cheques") or 0), 2)
        estoque = round(float(r.get("ativo_estoque") or 0), 2)
        estoque_comb = round(float(r.get("ativo_estoque_combustivel") or 0), 2)
        estoque_loja = round(float(r.get("ativo_estoque_loja") or 0), 2)
        comb_medido = bool(r.get("estoque_combustivel_medido"))
        data_leitura = r.get("estoque_data_leitura")
        passivo = round(float(r.get("passivo_contas_pagar") or 0), 2)
        vencido = round(float(r.get("passivo_vencido") or 0), 2)
        titulos = int(r.get("passivo_qtd_titulos") or 0)
        f_tem_ativo = bool(r.get("tem_ativo_dados"))

        circulante = round(caixa + banco + cartoes + cheques + estoque, 2)
        disponivel = round(caixa + banco, 2)
        liquidez = round(circulante / passivo, 4) if passivo > 0 else 0.0
        capital_giro = round(circulante - passivo, 2)

        tot_caixa += caixa
        tot_banco += banco
        tot_cartoes += cartoes
        tot_cheques += cheques
        tot_estoque += estoque
        tot_estoque_comb += estoque_comb
        tot_estoque_loja += estoque_loja
        if comb_medido:
            postos_comb_medido += 1
            if data_leitura and (ult_leitura is None or data_leitura > ult_leitura):
                ult_leitura = data_leitura
        tot_passivo += passivo
        tot_vencido += vencido
        tot_titulos += titulos
        tem_ativo_dados = tem_ativo_dados or f_tem_ativo
        if r.get("updated_at") and (updated_at is None or r["updated_at"] > updated_at):
            updated_at = r["updated_at"]

        por_filial.append({
            "id_filial": fid,
            "filial_label": _filial_label(fid, filial_nome_map.get(fid)),
            "ativo_caixa": caixa,
            "ativo_banco": banco,
            "ativo_cartoes": cartoes,
            "ativo_cheques": cheques,
            "ativo_estoque": estoque,
            "ativo_estoque_combustivel": estoque_comb,
            "ativo_estoque_loja": estoque_loja,
            "estoque_combustivel_medido": comb_medido,
            "ativo_disponivel": disponivel,
            "ativo_circulante": circulante,
            "passivo_contas_pagar": passivo,
            "passivo_vencido": vencido,
            "passivo_qtd_titulos": titulos,
            "liquidez_corrente": liquidez,
            "capital_giro_liquido": capital_giro,
            "cobre_passivo": (f_tem_ativo and circulante >= passivo),
            "tem_ativo_dados": f_tem_ativo,
        })

    ativo_circulante = round(tot_caixa + tot_banco + tot_cartoes + tot_cheques + tot_estoque, 2)
    ativo_disponivel = round(tot_caixa + tot_banco, 2)
    tot_passivo = round(tot_passivo, 2)
    liquidez_corrente = round(ativo_circulante / tot_passivo, 4) if tot_passivo > 0 else 0.0
    capital_giro_liquido = round(ativo_circulante - tot_passivo, 2)

    return {
        "ano_mes": target,
        "mes_label": _month_label_ptbr(target),
        "consolidado": len(por_filial) > 1,
        "filiais_count": len(por_filial),
        "meses_disponiveis": [
            {"ano_mes": m, "label": _month_label_ptbr(m)} for m in meses_disponiveis
        ],
        "ativo": {
            "caixa": round(tot_caixa, 2),
            "banco": round(tot_banco, 2),
            "cartoes": round(tot_cartoes, 2),
            "cheques": round(tot_cheques, 2),
            "estoque": round(tot_estoque, 2),
            "estoque_combustivel": round(tot_estoque_comb, 2),
            "estoque_loja": round(tot_estoque_loja, 2),
            "disponivel": ativo_disponivel,
            "circulante": ativo_circulante,
        },
        "cobertura_estoque": {
            "postos_com_combustivel": postos_comb_medido,
            "postos_total": len(por_filial),
            "ultima_leitura": str(ult_leitura) if ult_leitura else "",
        },
        "passivo": {
            "contas_pagar": tot_passivo,
            "qtd_titulos": tot_titulos,
            "vencido": round(tot_vencido, 2),
        },
        "indices": {
            "liquidez_corrente": liquidez_corrente,
            "capital_giro_liquido": capital_giro_liquido,
            "cobre_passivo": bool(tem_ativo_dados and ativo_circulante >= tot_passivo),
        },
        "tem_ativo_dados": tem_ativo_dados,
        "por_filial": por_filial,
        "freshness": str(updated_at) if updated_at else "",
        "disclaimer": (
            "Solvência gerencial de curto prazo: ativo circulante (disponível, "
            "recebíveis de curto prazo e estoque a custo) x contas a pagar do mês. "
            "Indicador gerencial, não é balanço contábil oficial."
        ),
        "source": "postgres",
    }


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
        summary = "Sem movimento de formas de pagamento no período selecionado."
    elif total_curr <= 0 and nonzero_rows == 0:
        source_status = "value_gap"
        summary = "Os registros de pagamento chegaram, mas os valores ainda precisam de validação da carga para leitura executiva."
    elif unknown_share > 0:
        source_status = "partial"
        summary = "A taxonomia oficial está aplicada, mas ainda existem pagamentos sem classificação no período."
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
    caixa_liquido = cash_net_value(total_vendas, total_cancelamentos)
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
    caixa_liquido = cash_net_value(total_vendas, total_cancelamentos)

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
          AND {_sales_status_expression('v')} IN ({SALE_STATUS}, {CANCELLATION_STATUS})
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
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {CANCELLATION_STATUS})::int AS qtd_cancelamentos
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
          COUNT(DISTINCT doc_key) FILTER (WHERE situacao = {CANCELLATION_STATUS})::int AS qtd_cancelamentos
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
      ORDER BY c.total_vendas DESC, c.total_cancelamentos DESC, c.last_event_at DESC
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
    caixa_liquido = cash_net_value(total_vendas, total_cancelamentos)
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
          "recebimentos_periodo": total_pagamentos,
            "total_cancelamentos": total_cancelamentos,
          "cancelamentos_periodo": total_cancelamentos,
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
          "recebimentos_periodo": total_pagamentos,
            "total_cancelamentos": total_cancelamentos,
          "cancelamentos_periodo": total_cancelamentos,
            "qtd_cancelamentos": qtd_cancelamentos,
            "caixas_com_cancelamento": len(cancelamentos),
            "total_devolucoes": 0.0,
            "qtd_devolucoes": 0,
            "caixas_com_devolucao": 0,
            "caixa_liquido": cash_net_value(total_vendas, total_cancelamentos),
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
        "inutilizacoes": {"qtd": 0, "valor_total": 0.0, "items": []},
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
                "cause": "A taxonomia oficial de pagamentos já foi aplicada, mas o período ainda mostra anomalia ou valores sem identificação comercial.",
                "action": "Abrir o bloco de pagamentos, validar o turno mais exposto e corrigir a origem dos meios sem classificação ainda neste ciclo.",
                "evidence": [
                    f"Sem classificação: {_format_brl(payments_kpis.get('unknown_valor'))}",
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
        confidence_reasons.append("vendas do dia ainda aguardam a atualização final")

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
        confidence_reason = "Base pronta e coerente para orientar a decisão deste período."
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
            "headline": "Operação estável no período atual, sem foco crítico acima da linha de corte.",
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
            "highlights": ["A operação seguiu estável no período.", "Nenhum risco material superou a linha de intervenção imediata."],
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


def customers_summary_paginated(
    role: str,
    id_empresa: int,
    id_filial: Any,
    *,
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "total_compras_30d",
    sort_order: str = "DESC",
    search: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Paginated customer summary from mart.customer_screen_summary (DB-level pagination)."""
    # Map frontend sort keys to actual columns
    sort_map = {
        "total_compras_30d": "compras_30d",
        "compras_30d": "compras_30d",
        "faturamento_30d": "faturamento_30d",
        "faturamento": "faturamento_30d",
        "ticket_medio": "ticket_medio_30d",
        "ticket_medio_30d": "ticket_medio_30d",
        "ultima_compra": "ultima_compra",
        "cliente_nome": "cliente_nome",
    }
    col = sort_map.get(sort_by, "faturamento_30d")
    direction = "DESC" if sort_order.upper() == "DESC" else "ASC"
    nulls = "NULLS LAST" if direction == "DESC" else "NULLS FIRST"
    offset = (max(1, page) - 1) * page_size

    where_filial, branch_params = _branch_scope_clause("m.id_filial", id_filial)
    where_search = ""
    search_params: list = []
    if search and search.strip():
        where_search = "AND m.cliente_nome ILIKE %s"
        search_params = [f"%{search.strip()}%"]

    count_sql = f"""
      SELECT COUNT(*)::int AS total
      FROM mart.customer_screen_summary m
      WHERE m.id_empresa = %s
        {where_filial}
        {where_search}
    """
    count_params = [id_empresa] + branch_params + search_params

    data_sql = f"""
      SELECT
        m.id_cliente,
        m.cliente_nome,
        m.faturamento_30d AS faturamento,
        m.compras_30d AS compras,
        m.ultima_compra,
        m.ticket_medio_30d AS ticket_medio,
        m.faturamento_90d,
        m.compras_90d,
        m.ticket_medio_90d
      FROM mart.customer_screen_summary m
      WHERE m.id_empresa = %s
        {where_filial}
        {where_search}
      ORDER BY m.{col} {direction} {nulls}, m.id_cliente
      LIMIT %s OFFSET %s
    """
    data_params = [id_empresa] + branch_params + search_params + [page_size, offset]

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        total_row = conn.execute(count_sql, count_params).fetchone()
        total = int(total_row["total"]) if total_row else 0
        if total == 0:
            # Fallback: mart may not be populated yet, use legacy approach
            from datetime import timedelta
            today = business_today(id_empresa)
            dt_ini = today - timedelta(days=30)
            items = customers_top(role, id_empresa, id_filial, dt_ini, today, limit=500)
            if search:
                s = search.lower()
                items = [i for i in items if s in str(i.get("cliente_nome", "")).lower()]
            total = len(items)
            return {
                "items": items[offset:offset + page_size],
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 0,
                "source": "postgres_legacy",
            }
        items = list(conn.execute(data_sql, data_params).fetchall())

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 0,
        "source": "mart",
    }


# ================================================================
# Filial Params (ABC thresholds, etc.)
# ================================================================

def get_filial_params(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    """Return ABC params for a filial, or defaults if not configured."""
    defaults = {"abc_threshold_a": 80, "abc_threshold_b": 95, "abc_exclude_fuel": True}
    if not id_filial:
        return defaults
    sql = "SELECT abc_threshold_a, abc_threshold_b, abc_exclude_fuel FROM app.filial_params WHERE id_empresa = %s AND id_filial = %s"
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, [id_empresa, id_filial]).fetchone()
    if not row:
        return defaults
    return {"abc_threshold_a": int(row[0]), "abc_threshold_b": int(row[1]), "abc_exclude_fuel": bool(row[2])}


def upsert_filial_params(role: str, id_empresa: int, id_filial: int, *, abc_threshold_a: int, abc_threshold_b: int, abc_exclude_fuel: bool) -> None:
    """Insert or update filial params (ABC thresholds)."""
    sql = """
        INSERT INTO app.filial_params (id_empresa, id_filial, abc_threshold_a, abc_threshold_b, abc_exclude_fuel, updated_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (id_empresa, id_filial)
        DO UPDATE SET abc_threshold_a = EXCLUDED.abc_threshold_a,
                      abc_threshold_b = EXCLUDED.abc_threshold_b,
                      abc_exclude_fuel = EXCLUDED.abc_exclude_fuel,
                      updated_at = now()
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        conn.execute(sql, [id_empresa, id_filial, abc_threshold_a, abc_threshold_b, abc_exclude_fuel])


def _ano_mes_from_date(d: date) -> int:
    return int(d.year) * 100 + int(d.month)


def refresh_fraud_credito_funcionario(
    role: str,
    id_empresa: int,
    ano_mes: Optional[int] = None,
) -> int:
    """Materializa mash PG e publica no ClickHouse (fonte de leitura da API)."""
    ym = int(ano_mes) if ano_mes else _ano_mes_from_date(business_today(id_empresa))
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        row = conn.execute(
            "SELECT etl.refresh_fraud_credito_funcionario(%s, %s) AS n",
            [id_empresa, ym],
        ).fetchone() or {}
        n = int(row.get("n") or 0)
    try:
        publish_fraud_credito_funcionario_to_ch(role, id_empresa, ym)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "publish_fraud_credito_funcionario_to_ch failed empresa=%s mes=%s err=%s",
            id_empresa, ym, str(exc)[:240],
        )
    return n


def publish_fraud_credito_funcionario_to_ch(
    role: str,
    id_empresa: int,
    ano_mes: int,
) -> Dict[str, int]:
    """Copia mart PG → torqmind_mart_rt (ReplacingMergeTree). Front lê só o CH."""
    import json
    from datetime import datetime, timezone

    from app.db_clickhouse import insert_batch

    ym = int(ano_mes)
    published_at = datetime.now(timezone.utc)
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        resumo_rows = conn.execute(
            """
            SELECT
              id_empresa, id_funcionario, ano_mes, id_filial_ref, id_entidade,
              nome_funcionario, cpf, ativo, limite_vale, vales_cadastro,
              usado_mes, saldo_restante, qtd_usos_mes, max_usos_mesmo_dia,
              status, motivos
            FROM mart.fraud_credito_funcionario_resumo
            WHERE id_empresa = %s AND ano_mes = %s
            """,
            [id_empresa, ym],
        ).fetchall()
        uso_rows = conn.execute(
            """
            SELECT
              id_empresa, id_funcionario, ano_mes, id_filial, id_entidade,
              id_contasreceber, id_comprovante, nro_cupom, nro_documento,
              dt_evento, valor, id_usuario_caixa, operador_caixa, historico, atipico
            FROM mart.fraud_credito_funcionario_uso
            WHERE id_empresa = %s AND ano_mes = %s
            """,
            [id_empresa, ym],
        ).fetchall()

    resumo_ch = []
    for r in resumo_rows:
        motivos = r.get("motivos") or []
        if not isinstance(motivos, list):
            motivos = list(motivos) if motivos else []
        resumo_ch.append({
            "id_empresa": int(r["id_empresa"]),
            "id_funcionario": int(r["id_funcionario"]),
            "ano_mes": ym,
            "id_filial_ref": int(r.get("id_filial_ref") or 0),
            "id_entidade": int(r.get("id_entidade") or 0),
            "nome_funcionario": str(r.get("nome_funcionario") or ""),
            "cpf": str(r.get("cpf") or ""),
            "ativo": 1 if r.get("ativo") else 0,
            "limite_vale": float(r.get("limite_vale") or 0),
            "vales_cadastro": float(r.get("vales_cadastro") or 0),
            "usado_mes": float(r.get("usado_mes") or 0),
            "saldo_restante": float(r.get("saldo_restante") or 0),
            "qtd_usos_mes": int(r.get("qtd_usos_mes") or 0),
            "max_usos_mesmo_dia": int(r.get("max_usos_mesmo_dia") or 0),
            "status": str(r.get("status") or "Normal"),
            "motivos": json.dumps(motivos, ensure_ascii=False),
            "published_at": published_at,
        })

    uso_ch = []
    for u in uso_rows:
        uso_ch.append({
            "id_empresa": int(u["id_empresa"]),
            "id_funcionario": int(u["id_funcionario"]),
            "ano_mes": ym,
            "id_filial": int(u.get("id_filial") or 0),
            "id_entidade": int(u.get("id_entidade") or 0),
            "id_contasreceber": int(u["id_contasreceber"]),
            "id_comprovante": int(u.get("id_comprovante") or 0),
            "nro_cupom": str(u.get("nro_cupom") or ""),
            "nro_documento": str(u.get("nro_documento") or ""),
            "dt_evento": u.get("dt_evento"),
            "valor": float(u.get("valor") or 0),
            "id_usuario_caixa": int(u.get("id_usuario_caixa") or 0),
            "operador_caixa": str(u.get("operador_caixa") or ""),
            "historico": str(u.get("historico") or ""),
            "atipico": 1 if u.get("atipico") else 0,
            "published_at": published_at,
        })

    n_r = insert_batch(
        "torqmind_mart_rt.mart_fraud_credito_funcionario_resumo",
        resumo_ch,
        order_by=["id_empresa", "ano_mes", "id_funcionario"],
    )
    n_u = insert_batch(
        "torqmind_mart_rt.mart_fraud_credito_funcionario_uso",
        uso_ch,
        order_by=["id_empresa", "ano_mes", "id_funcionario", "id_filial", "id_contasreceber"],
    )
    return {"resumo": n_r, "usos": n_u}


def fraud_credito_funcionario(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    ano_mes: Optional[int] = None,
    status: str = "todos",
    refresh: bool = False,
    limit: int = 500,
) -> Dict[str, Any]:
    """Grid principal + usos (drill-down) do crédito/vale de funcionário.

    Regras Suspeito (OR): limite extrapolado, >=2 usos no mesmo dia, valor atípico.
    Refresh só com refresh=true (ETL ~1min; não bloquear GET padrão).
    """
    ym = int(ano_mes) if ano_mes else _ano_mes_from_date(business_today(id_empresa))
    status_key = str(status or "todos").strip().lower()
    if refresh:
        try:
            refresh_fraud_credito_funcionario(role, id_empresa, ym)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "refresh_fraud_credito_funcionario failed empresa=%s mes=%s err=%s",
                id_empresa, ym, str(exc)[:240],
            )

    where_filial, branch_params = _branch_scope_clause("r.id_filial_ref", id_filial)
    status_sql = ""
    if status_key in ("suspeito", "suspeitos", "suspeitas"):
        status_sql = "AND r.status = 'Suspeito'"
    elif status_key in ("normal", "normais"):
        status_sql = "AND r.status = 'Normal'"

    list_sql = f"""
      SELECT
        r.id_funcionario,
        r.id_filial_ref,
        r.id_entidade,
        r.nome_funcionario,
        r.cpf,
        r.ativo,
        r.limite_vale,
        r.vales_cadastro,
        r.usado_mes,
        r.saldo_restante,
        r.qtd_usos_mes,
        r.max_usos_mesmo_dia,
        r.status,
        r.motivos,
        r.refreshed_at
      FROM mart.fraud_credito_funcionario_resumo r
      WHERE r.id_empresa = %s
        AND r.ano_mes = %s
        {where_filial}
        {status_sql}
      ORDER BY
        CASE WHEN r.status = 'Suspeito' THEN 0 ELSE 1 END,
        r.usado_mes DESC,
        r.nome_funcionario
      LIMIT %s
    """
    params = [id_empresa, ym] + branch_params + [int(limit)]

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(r) for r in conn.execute(list_sql, params).fetchall()]
        func_ids = [int(r["id_funcionario"]) for r in rows if r.get("id_funcionario") is not None]
        usos_by: Dict[int, List[Dict[str, Any]]] = {fid: [] for fid in func_ids}
        if func_ids:
            uso_sql = """
              SELECT
                u.id_funcionario,
                u.id_filial,
                u.id_entidade,
                u.id_contasreceber,
                u.id_comprovante,
                u.nro_cupom,
                u.nro_documento,
                u.dt_evento,
                u.valor,
                u.id_usuario_caixa,
                u.operador_caixa,
                u.historico,
                u.atipico
              FROM mart.fraud_credito_funcionario_uso u
              WHERE u.id_empresa = %s
                AND u.ano_mes = %s
                AND u.id_funcionario = ANY(%s)
              ORDER BY u.dt_evento DESC NULLS LAST, u.valor DESC
            """
            for u in conn.execute(uso_sql, [id_empresa, ym, func_ids]).fetchall():
                d = dict(u)
                fid = int(d["id_funcionario"])
                usos_by.setdefault(fid, []).append({
                    "id_filial": d.get("id_filial"),
                    "id_entidade": d.get("id_entidade"),
                    "id_contasreceber": d.get("id_contasreceber"),
                    "id_comprovante": d.get("id_comprovante"),
                    "nro_cupom": d.get("nro_cupom") or d.get("nro_documento") or "",
                    "dt_evento": d.get("dt_evento").isoformat() if d.get("dt_evento") else None,
                    "valor": float(d.get("valor") or 0),
                    "id_usuario_caixa": d.get("id_usuario_caixa"),
                    "operador_caixa": d.get("operador_caixa") or "—",
                    "historico": d.get("historico") or "",
                    "atipico": bool(d.get("atipico")),
                })

        summary = conn.execute(
            f"""
              SELECT
                count(*)::int AS total,
                count(*) FILTER (WHERE status = 'Suspeito')::int AS suspeitos,
                count(*) FILTER (WHERE status = 'Normal')::int AS normais,
                coalesce(sum(usado_mes),0)::numeric(18,2) AS usado_total,
                coalesce(sum(limite_vale),0)::numeric(18,2) AS limite_total
              FROM mart.fraud_credito_funcionario_resumo r
              WHERE r.id_empresa = %s AND r.ano_mes = %s
              {where_filial}
            """,
            [id_empresa, ym] + branch_params,
        ).fetchone() or {}

    funcionarios = []
    for r in rows:
        fid = int(r["id_funcionario"])
        funcionarios.append({
            "id_funcionario": fid,
            "id_filial": r.get("id_filial_ref"),
            "id_entidade": r.get("id_entidade"),
            "nome": r.get("nome_funcionario") or "",
            "cpf": r.get("cpf") or "",
            "ativo": bool(r.get("ativo")),
            "limite": float(r.get("limite_vale") or 0),
            "vales_cadastro": float(r.get("vales_cadastro") or 0),
            "usado_mes": float(r.get("usado_mes") or 0),
            "saldo_restante": float(r.get("saldo_restante") or 0),
            "qtd_usos_mes": int(r.get("qtd_usos_mes") or 0),
            "max_usos_mesmo_dia": int(r.get("max_usos_mesmo_dia") or 0),
            "status": r.get("status") or "Normal",
            "motivos": list(r.get("motivos") or []),
            "usos": usos_by.get(fid, []),
            "refreshed_at": r.get("refreshed_at").isoformat() if r.get("refreshed_at") else None,
        })

    return {
        "ano_mes": ym,
        "summary": {
            "total": int(summary.get("total") or 0),
            "suspeitos": int(summary.get("suspeitos") or 0),
            "normais": int(summary.get("normais") or 0),
            "usado_total": float(summary.get("usado_total") or 0),
            "limite_total": float(summary.get("limite_total") or 0),
        },
        "funcionarios": funcionarios,
        "disclaimer": (
            "Limite: FUNCIONARIOS.LIMITEVALE. Uso: CONTASRECEBER a prazo do cliente "
            "vinculado por CPF, com data/operador resolvidos via cupom no comprovante. "
            "Suspeito = limite extrapolado OR ≥2 usos no mesmo dia OR valor atípico vs histórico 90d."
        ),
    }
