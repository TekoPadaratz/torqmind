from __future__ import annotations

"""ClickHouse-backed analytical reads for TorqMind Smart Marts.

The public function signatures intentionally mirror ``repos_mart.py``. Route
selection is handled by ``repos_analytics.py``; this module never falls back to
PostgreSQL on ClickHouse errors.
"""

from datetime import date, datetime, timedelta, timezone
import json
import logging
import math
from typing import Any, Dict, List, Optional

from app.business_time import business_clock_payload, business_timezone, business_today
from app.db_clickhouse import query_dict

logger = logging.getLogger(__name__)


CASH_STALE_WINDOW_HOURS = 96

SNAPSHOT_TABLES = {
    "customer_churn_risk_daily": "torqmind_mart.customer_churn_risk_daily",
    "finance_aging_daily": "torqmind_mart.finance_aging_daily",
    "health_score_daily": "torqmind_mart.health_score_daily",
}

EVENT_TYPE_LABELS = {
    "CANCELAMENTO": "Cancelamento fora do padrão",
    "CANCELAMENTO_SEGUIDO_VENDA": "Cancelamento seguido de nova venda",
    "DESCONTO_ALTO": "Desconto acima do padrão",
    "FUNCIONARIO_OUTLIER": "Comportamento fora do padrão",
}


def cash_definitions() -> Dict[str, str]:
    return {
        "historical": "O histórico do caixa preserva a trilha de reconciliação por turno, enquanto a camada comercial principal usa comprovantes ativos e cancelados com CFOP comercial para fechar vendas e cancelamentos do período.",
        "live_now": (
            f"O monitor ao vivo mostra apenas turnos que seguem abertos e tiveram movimento recente nas últimas {CASH_STALE_WINDOW_HOURS} horas. "
            "Turnos antigos sem atividade ficam separados para investigação, sem poluir o agora."
        ),
        "operator": "O nome exibido é o operador logado responsável pelo turno. Caixa e Antifraude usam essa mesma referência para evitar divergência de responsável.",
        "closing_rule": "Um turno deixa de aparecer como aberto quando o fechamento foi confirmado e não houve nova movimentação depois disso.",
        "aggregates": "A visão principal do caixa parte de comprovantes com CFOP comercial e flag de cancelamento; a reconciliação detalhada continua exposta separadamente quando necessário.",
        "net_cash": "Saldo comercial do período = vendas ativas de saída - cancelamentos. Recebimentos e componentes financeiros seguem expostos separadamente.",
    }


def fraud_definitions() -> Dict[str, str]:
    return {
        "operational_cancelamentos": "Cancelamento operacional é a venda cancelada que ainda precisa de revisão, sempre reconciliada com o turno real do caixa para não gerar leitura duplicada ou fora de contexto.",
        "cashier_operator": "Sempre mostramos o operador logado responsável pela operação do caixa. O usuário gravado no documento só entra como apoio quando o turno não consegue resolver o responsável.",
        "high_risk_events": "Evento de alto risco é um comportamento que foge do padrão esperado e merece revisão prioritária, como sequência incomum de cancelamentos, desconto fora da curva ou operação em contexto atípico.",
        "estimated_impact": "Impacto estimado é o valor potencial exposto no evento, usado para priorizar auditoria. Em cancelamento modelado usamos 70% do valor da operação; em desconto alto usamos o maior entre o desconto total e 8% da venda. Não é perda confirmada.",
        "score_meaning": "O score médio resume o nível de alerta dos eventos do período numa escala de 0 a 100. Quanto maior o score, maior a concentração de sinais que pedem investigação.",
        "coverage": "Leitura operacional mostra o que realmente ocorreu no período. Leitura modelada depende da janela coberta pelo motor de risco; quando a cobertura é parcial, a tela avisa isso sem apagar os eventos operacionais.",
        "impact_formulas": "Cancelamento modelado: 70% do valor da venda cancelada. Desconto alto: maior entre o desconto concedido e 8% do valor da venda. Pagamentos fora do padrão usam a exposição monetária do próprio evento.",
    }


def finance_definitions() -> Dict[str, Dict[str, str]]:
    return {
        "receber_aberto": {
            "label": "Receber em aberto",
            "formula": "Soma dos títulos a receber ainda não quitados na data-base.",
            "source": "torqmind_mart.financeiro_vencimentos_diaria",
            "impact": "Mostra o caixa que ainda deve entrar.",
        },
        "receber_vencido": {
            "label": "Receber vencido",
            "formula": "Parcela do contas a receber cujo vencimento já passou e segue em aberto.",
            "source": "torqmind_mart.finance_aging_daily",
            "impact": "Mostra caixa atrasado e necessidade de cobrança.",
        },
        "pagar_aberto": {
            "label": "Pagar em aberto",
            "formula": "Soma dos compromissos a pagar ainda não liquidados na data-base.",
            "source": "torqmind_mart.financeiro_vencimentos_diaria",
            "impact": "Mostra obrigação futura que ainda pressiona o caixa.",
        },
        "pagar_vencido": {
            "label": "Pagar vencido",
            "formula": "Parcela do contas a pagar cujo vencimento já passou e segue em aberto.",
            "source": "torqmind_mart.finance_aging_daily",
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
            "source": "torqmind_mart.finance_aging_daily",
            "impact": "Ajuda a ver dependência excessiva de poucos títulos.",
        },
        "payments_total": {
            "label": "Leitura dos pagamentos",
            "formula": "Soma dos pagamentos conciliados no período.",
            "source": "torqmind_mart.agg_pagamentos_turno",
            "impact": "Mostra por onde o dinheiro entrou e sustenta conferência com caixa.",
        },
        "payments_unknown_share": {
            "label": "Pagamentos sem classificação",
            "formula": "Valor sem mapeamento oficial dividido pelo valor total conciliado de pagamentos.",
            "source": "torqmind_mart.agg_pagamentos_turno",
            "impact": "Indica perda de explicabilidade do recebimento.",
        },
    }


def _date_key(value: date) -> int:
    return value.year * 10000 + value.month * 100 + value.day


def _date_from_key(value: Any) -> Optional[date]:
    digits = str(value or "").strip()
    if len(digits) != 8 or not digits.isdigit():
        return None
    try:
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    except ValueError:
        return None


def _to_float(value: Any, decimals: int = 2) -> float:
    try:
        number = float(value or 0)
        if not math.isfinite(number):
            return 0.0
        return round(number, decimals)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _iso_or_none(value: Any) -> Optional[str]:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _iso_from_clickhouse_epoch(epoch: Any, id_empresa: int) -> Optional[str]:
    try:
        epoch_value = float(epoch or 0)
    except (TypeError, ValueError):
        return None
    if epoch_value <= 0:
        return None
    return (
        datetime.fromtimestamp(epoch_value, tz=timezone.utc)
        .astimezone(business_timezone(id_empresa))
        .replace(microsecond=0)
        .isoformat()
    )


def _json_obj(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _branch_ids(id_filial: Any) -> Optional[List[int]]:
    if id_filial is None or id_filial == -1:
        return None
    if isinstance(id_filial, (list, tuple, set)):
        values = sorted({int(value) for value in id_filial if value is not None and int(value) != -1})
        return values if values else []
    value = int(id_filial)
    return None if value == -1 else [value]


def _conn_branch_id(id_filial: Any) -> Optional[int]:
    branch_ids = _branch_ids(id_filial)
    if not branch_ids or len(branch_ids) != 1:
        return None
    return int(branch_ids[0])


def _branch_clause(column: str, id_filial: Any) -> str:
    branch_ids = _branch_ids(id_filial)
    if branch_ids is None:
        return ""
    if not branch_ids:
        return " AND 0"
    if len(branch_ids) == 1:
        return f" AND {column} = {int(branch_ids[0])}"
    values = ", ".join(str(int(value)) for value in branch_ids)
    return f" AND {column} IN ({values})"


def _filial_label(id_filial: Any, filial_nome: Any = None) -> str:
    nome = str(filial_nome or "").strip()
    if nome:
        return nome
    branch_ids = _branch_ids(id_filial)
    if branch_ids is None:
        return "Todas as filiais"
    if len(branch_ids) > 1:
        return f"{len(branch_ids)} filiais selecionadas"
    return "Filial sem cadastro"


def _turno_label(turno_value: Any, id_turno: Any = None) -> str:
    value = str(turno_value or "").strip()
    if value:
        return value
    if id_turno is not None and _to_int(id_turno) > 0:
        return str(_to_int(id_turno))
    return "Turno sem cadastro"


def _cash_operator_label(usuario_nome: Any, id_usuario: Any = None) -> str:
    nome = str(usuario_nome or "").strip()
    if nome:
        return nome
    return "Operador sem cadastro"


def _employee_label(funcionario_nome: Any, id_funcionario: Any = None) -> str:
    nome = str(funcionario_nome or "").strip()
    if nome and nome.lower() not in {"(sem funcionário)", "(sem funcionario)", "sem funcionário", "sem funcionario"}:
        return nome
    return "Equipe sem cadastro"


def _event_type_label(event_type: Any) -> str:
    key = str(event_type or "").strip().upper()
    return EVENT_TYPE_LABELS.get(key, key.replace("_", " ").title() or "Evento de risco")


def _payment_category_label(category: Any, label: Any = None) -> str:
    category_value = str(category or "").strip().upper()
    label_value = str(label or "").strip()
    if label_value and label_value.upper() != "NÃO IDENTIFICADO":
        return label_value
    if category_value and category_value != "NAO_IDENTIFICADO":
        return category_value.replace("_", " ").title()
    return "NÃO IDENTIFICADO"


def _format_brl(value: Any) -> str:
    return f"R$ {float(value or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _run(sql: str, parameters: Dict[str, Any], tenant_id: int) -> List[Dict[str, Any]]:
    return query_dict(sql, parameters=parameters, tenant_id=tenant_id)


def _first(sql: str, parameters: Dict[str, Any], tenant_id: int) -> Dict[str, Any]:
    rows = _run(sql, parameters, tenant_id)
    return dict(rows[0]) if rows else {}


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
    covered_days = max((overlap_end - overlap_start).days + 1, 0) if overlap_end >= overlap_start else 0

    if requested_dt_ini > latest_available_dt:
        effective_dt_ini = requested_dt_ini
        effective_dt_fim = requested_dt_fim
        mode = "requested_outside_coverage"
        message = (
            f"O período solicitado começa em {requested_dt_ini.isoformat()}, mas a última base comercial disponível "
            f"vai até {latest_available_dt.isoformat()}. Não há vendas publicadas para a data solicitada."
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


def _snapshot_meta(
    table_name: str,
    id_empresa: int,
    id_filial: Any,
    requested_dt_ref: Optional[date],
    precision_mode: str,
) -> Dict[str, Any]:
    table = SNAPSHOT_TABLES[table_name]
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          min(dt_ref) AS coverage_start_dt_ref,
          max(dt_ref) AS coverage_end_dt_ref,
          count() AS row_count,
          countIf(dt_ref = {{dt_ref:Date}}) > 0 AS has_exact,
          maxIf(dt_ref, dt_ref <= {{dt_ref:Date}}) AS effective_dt_ref,
          max(updated_at) AS latest_updated_at
        FROM {table}
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "dt_ref": requested_dt_ref or business_today(id_empresa)},
        id_empresa,
    )
    effective_dt_ref = row.get("effective_dt_ref")
    has_exact = bool(row.get("has_exact"))
    snapshot_status = "exact" if has_exact else ("best_effort" if effective_dt_ref else "missing")
    return {
        "requested_dt_ref": requested_dt_ref,
        "effective_dt_ref": effective_dt_ref,
        "coverage_start_dt_ref": row.get("coverage_start_dt_ref"),
        "coverage_end_dt_ref": row.get("coverage_end_dt_ref"),
        "precision_mode": "exact" if has_exact else precision_mode,
        "snapshot_status": snapshot_status,
        "source_table": table,
        "source_kind": "snapshot" if effective_dt_ref else "missing",
        "latest_updated_at": row.get("latest_updated_at"),
        "row_count": _to_int(row.get("row_count")),
    }


def commercial_window_coverage(
    role: str,
    id_empresa: int,
    id_filial: Any,
    requested_dt_ini: date,
    requested_dt_fim: date,
) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT min(data_key) AS min_data_key, max(data_key) AS max_data_key
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    return _window_coverage_payload(
        requested_dt_ini=requested_dt_ini,
        requested_dt_fim=requested_dt_fim,
        min_data_key=row.get("min_data_key"),
        max_data_key=row.get("max_data_key"),
        source_label="torqmind_mart.agg_vendas_diaria",
    )


def _sales_sync_meta(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          count() AS row_count,
          max(data_key) AS max_data_key,
          toUnixTimestamp(max(updated_at)) AS latest_updated_at_epoch
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    row_count = _to_int(row.get("row_count"))
    max_data_key = _to_int(row.get("max_data_key")) if row_count > 0 else 0
    max_dt = _date_from_key(max_data_key)
    last_sync_at = _iso_from_clickhouse_epoch(row.get("latest_updated_at_epoch"), id_empresa) if row_count > 0 else None
    return {
        "last_sync_at": last_sync_at,
        "snapshot_generated_at": last_sync_at,
        "source": "torqmind_mart.agg_vendas_diaria",
        "dt_ref": (max_dt or dt_fim).isoformat(),
        "max_data_key": max_data_key if max_data_key > 0 else None,
        "row_count": row_count,
    }


def risk_model_coverage(dt_ini: date, dt_fim: date, risk_window: Dict[str, Any]) -> Dict[str, Any]:
    requested_start_key = _date_key(dt_ini)
    requested_end_key = _date_key(dt_fim)
    requested_days = max((dt_fim - dt_ini).days + 1, 0)
    window_start_key = _to_int(risk_window.get("min_data_key"))
    window_end_key = _to_int(risk_window.get("max_data_key"))
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


def dashboard_kpis(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          sum(faturamento) AS faturamento,
          sum(margem) AS margem,
          avg(ticket_medio) AS ticket_medio,
          sum(quantidade_itens) AS itens
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return {
        "faturamento": _to_float(row.get("faturamento")),
        "margem": _to_float(row.get("margem")),
        "ticket_medio": _to_float(row.get("ticket_medio")),
        "itens": _to_float(row.get("itens")),
    }


def dashboard_series(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT data_key, id_filial, sum(faturamento) AS faturamento, sum(margem) AS margem
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY data_key, id_filial
        ORDER BY data_key, id_filial
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return [
        {
            "data_key": _to_int(row.get("data_key")),
            "id_filial": _to_int(row.get("id_filial")),
            "faturamento": _to_float(row.get("faturamento")),
            "margem": _to_float(row.get("margem")),
        }
        for row in rows
    ]


def insights_base(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          data_key,
          id_filial,
          sum(faturamento_dia) AS faturamento_dia,
          sum(faturamento_mes_acum) AS faturamento_mes_acum,
          sum(comparativo_mes_anterior) AS comparativo_mes_anterior
        FROM torqmind_mart.insights_base_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY data_key, id_filial
        ORDER BY data_key, id_filial
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return [
        {
            "data_key": _to_int(row.get("data_key")),
            "id_filial": _to_int(row.get("id_filial")),
            "faturamento_dia": _to_float(row.get("faturamento_dia")),
            "faturamento_mes_acum": _to_float(row.get("faturamento_mes_acum")),
            "comparativo_mes_anterior": _to_float(row.get("comparativo_mes_anterior")),
        }
        for row in rows
    ]


def sales_by_hour(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          data_key,
          id_filial,
          hora,
          sum(faturamento) AS faturamento,
          sum(margem) AS margem,
          sum(vendas) AS vendas
        FROM torqmind_mart.agg_vendas_hora
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY data_key, id_filial, hora
        ORDER BY data_key, hora, id_filial
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return [
        {
            "data_key": _to_int(row.get("data_key")),
            "id_filial": _to_int(row.get("id_filial")),
            "hora": _to_int(row.get("hora")),
            "faturamento": _to_float(row.get("faturamento")),
            "margem": _to_float(row.get("margem")),
            "vendas": _to_int(row.get("vendas")),
        }
        for row in rows
    ]


def sales_top_products(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_produto,
          produto_nome,
          faturamento,
          custo_total,
          margem,
          qtd,
          if(toFloat64(qtd) = 0, 0, toFloat64(faturamento) / toFloat64(qtd)) AS valor_unitario_medio
        FROM (
          SELECT
            id_produto,
            max(produto_nome) AS produto_nome,
            sum(faturamento) AS faturamento,
            sum(custo_total) AS custo_total,
            sum(margem) AS margem,
            sum(qtd) AS qtd
          FROM torqmind_mart.agg_produtos_diaria
          WHERE id_empresa = {{id_empresa:Int32}}
            AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
            {branch}
          GROUP BY id_produto
        )
        ORDER BY faturamento DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    return [
        {
            "id_produto": _to_int(row.get("id_produto")),
            "produto_nome": row.get("produto_nome") or "",
            "faturamento": _to_float(row.get("faturamento")),
            "custo_total": _to_float(row.get("custo_total")),
            "margem": _to_float(row.get("margem")),
            "qtd": _to_float(row.get("qtd"), 3),
            "valor_unitario_medio": _to_float(row.get("valor_unitario_medio"), 4),
        }
        for row in rows
    ]


def sales_top_groups(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_grupo_produto,
          max(grupo_nome) AS grupo_nome,
          sum(faturamento) AS faturamento,
          sum(margem) AS margem
        FROM torqmind_mart.agg_grupos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY id_grupo_produto
        ORDER BY faturamento DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    return [
        {
            "id_grupo_produto": _to_int(row.get("id_grupo_produto")),
            "grupo_nome": row.get("grupo_nome") or "",
            "faturamento": _to_float(row.get("faturamento")),
            "margem": _to_float(row.get("margem")),
        }
        for row in rows
    ]


def sales_top_employees(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_funcionario,
          max(funcionario_nome) AS funcionario_nome,
          sum(faturamento) AS faturamento,
          sum(margem) AS margem,
          sum(vendas) AS vendas
        FROM torqmind_mart.agg_funcionarios_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND id_funcionario != -1
          {branch}
        GROUP BY id_funcionario
        HAVING funcionario_nome != ''
        ORDER BY faturamento DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    return [
        {
            "id_funcionario": _to_int(row.get("id_funcionario")),
            "funcionario_nome": row.get("funcionario_nome") or "",
            "faturamento": _to_float(row.get("faturamento")),
            "margem": _to_float(row.get("margem")),
            "vendas": _to_int(row.get("vendas")),
        }
        for row in rows
    ]


def _sales_historical_bundle_from_marts(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    *,
    include_details: bool = True,
) -> Dict[str, Any]:
    kpis = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    by_day = dashboard_series(role, id_empresa, id_filial, dt_ini, dt_fim)
    by_hour = sales_by_hour(role, id_empresa, id_filial, dt_ini, dt_fim) if include_details else []
    top_products = sales_top_products(role, id_empresa, id_filial, dt_ini, dt_fim, limit=15) if include_details else []
    top_groups = sales_top_groups(role, id_empresa, id_filial, dt_ini, dt_fim, limit=10) if include_details else []
    top_employees = sales_top_employees(role, id_empresa, id_filial, dt_ini, dt_fim, limit=10) if include_details else []
    sync_meta = _sales_sync_meta(role, id_empresa, id_filial, dt_ini, dt_fim)
    return {
        "kpis": {
            "faturamento": _to_float(kpis.get("faturamento")),
            "margem": _to_float(kpis.get("margem")),
            "ticket_medio": _to_float(kpis.get("ticket_medio")),
            "devolucoes": 0.0,
        },
        "by_day": by_day,
        "by_hour": by_hour,
        "top_products": top_products,
        "top_groups": top_groups,
        "top_employees": top_employees,
        "stats": {"vendas": sum(_to_int(row.get("vendas")) for row in by_hour)},
        "operational_sync": sync_meta,
        "freshness": {
            "mode": "mart_snapshot",
            "operational_day": None,
            "live_through_at": sync_meta.get("last_sync_at"),
            "snapshot_generated_at": sync_meta.get("snapshot_generated_at"),
            "historical_through_dt": dt_fim.isoformat(),
            "source": "torqmind_mart.agg_vendas_diaria",
        },
    }


def _commercial_annual_comparison(monthly_rows: List[Dict[str, Any]], *, current_year: int) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    by_month = {(_to_int(row.get("ano")), _to_int(row.get("mes"))): row for row in monthly_rows}
    normalized_rows: List[Dict[str, Any]] = []
    comparison_months: List[Dict[str, Any]] = []
    previous_year = current_year - 1
    for year in (previous_year, current_year):
        for month in range(1, 13):
            source = by_month.get((year, month), {})
            normalized_rows.append(
                {
                    "month_ref": date(year, month, 1).isoformat(),
                    "ano": year,
                    "mes": month,
                    "saidas": _to_float(source.get("saidas")),
                    "entradas": 0.0,
                    "cancelamentos": _to_float(source.get("cancelamentos")),
                }
            )
    for month in range(1, 13):
        current = by_month.get((current_year, month), {})
        previous = by_month.get((previous_year, month), {})
        comparison_months.append(
            {
                "mes": month,
                "saidas_atual": _to_float(current.get("saidas")),
                "saidas_anterior": _to_float(previous.get("saidas")),
                "entradas_atual": 0.0,
                "entradas_anterior": 0.0,
                "cancelamentos_atual": _to_float(current.get("cancelamentos")),
                "cancelamentos_anterior": _to_float(previous.get("cancelamentos")),
                "month_ref_atual": date(current_year, month, 1).isoformat(),
                "month_ref_anterior": date(previous_year, month, 1).isoformat(),
            }
        )
    return normalized_rows, {"current_year": current_year, "previous_year": previous_year, "months": comparison_months}


def sales_commercial_overview(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch_sales = _branch_clause("id_filial", id_filial)
    branch_fraud = _branch_clause("id_filial", id_filial)
    comparison_year = dt_fim.year
    comparison_start = _date_key(date(comparison_year - 1, 1, 1))
    comparison_end = _date_key(date(comparison_year, 12, 31))
    kpis = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    fraud = fraud_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    monthly_rows = _run(
        f"""
        SELECT
          intDiv(data_key, 100) AS month_key,
          intDiv(intDiv(data_key, 100), 100) AS ano,
          modulo(intDiv(data_key, 100), 100) AS mes,
          sum(faturamento) AS saidas,
          0 AS entradas,
          0 AS cancelamentos
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch_sales}
        GROUP BY month_key, ano, mes
        UNION ALL
        SELECT
          intDiv(data_key, 100) AS month_key,
          intDiv(intDiv(data_key, 100), 100) AS ano,
          modulo(intDiv(data_key, 100), 100) AS mes,
          0 AS saidas,
          0 AS entradas,
          sum(valor_cancelado) AS cancelamentos
        FROM torqmind_mart.fraude_cancelamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch_fraud}
        GROUP BY month_key, ano, mes
        ORDER BY month_key
        """,
        {"id_empresa": int(id_empresa), "ini": comparison_start, "fim": comparison_end},
        id_empresa,
    )
    merged_months: Dict[int, Dict[str, Any]] = {}
    for row in monthly_rows:
        target = merged_months.setdefault(
            _to_int(row.get("month_key")),
            {"month_key": _to_int(row.get("month_key")), "ano": _to_int(row.get("ano")), "mes": _to_int(row.get("mes")), "saidas": 0.0, "entradas": 0.0, "cancelamentos": 0.0},
        )
        target["saidas"] += _to_float(row.get("saidas"))
        target["cancelamentos"] += _to_float(row.get("cancelamentos"))
    monthly_series, annual_comparison = _commercial_annual_comparison(list(merged_months.values()), current_year=comparison_year)
    by_hour = [
        {"hora": row["hora"], "saidas": row["faturamento"], "entradas": 0.0, "cancelamentos": 0.0}
        for row in sales_by_hour(role, id_empresa, id_filial, dt_ini, dt_fim)
    ]
    return {
        "kpis": {
            "saidas": _to_float(kpis.get("faturamento")),
            "qtd_saidas": _to_int(kpis.get("itens")),
            "entradas": 0.0,
            "qtd_entradas": 0,
            "cancelamentos": _to_float(fraud.get("valor_cancelado")),
            "qtd_cancelamentos": _to_int(fraud.get("cancelamentos")),
        },
        "cfop_breakdown": [
            {
                "cfop_class": "saida_normal",
                "label": "Vendas normais",
                "documentos": _to_int(kpis.get("itens")),
                "valor_ativo": _to_float(kpis.get("faturamento")),
                "valor_cancelado": 0.0,
                "valor_total": _to_float(kpis.get("faturamento")),
            },
            {
                "cfop_class": "cancelamento",
                "label": "Cancelamentos",
                "documentos": _to_int(fraud.get("cancelamentos")),
                "valor_ativo": 0.0,
                "valor_cancelado": _to_float(fraud.get("valor_cancelado")),
                "valor_total": _to_float(fraud.get("valor_cancelado")),
            },
        ],
        "by_hour": by_hour,
        "monthly_evolution": monthly_series,
        "annual_comparison": annual_comparison,
    }


def sales_overview_bundle(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
    *,
    include_details: bool = True,
) -> Dict[str, Any]:
    sales_coverage = commercial_window_coverage(role, id_empresa, id_filial, dt_ini, dt_fim)
    effective_dt_ini = sales_coverage.get("effective_dt_ini") or dt_ini
    effective_dt_fim = sales_coverage.get("effective_dt_fim") or dt_fim
    bundle = _sales_historical_bundle_from_marts(role, id_empresa, id_filial, effective_dt_ini, effective_dt_fim, include_details=include_details)
    commercial = sales_commercial_overview(role, id_empresa, id_filial, effective_dt_ini, effective_dt_fim)
    bundle["commercial_kpis"] = commercial.get("kpis") or {}
    bundle["cfop_breakdown"] = commercial.get("cfop_breakdown") or []
    bundle["commercial_by_hour"] = commercial.get("by_hour") or []
    bundle["monthly_evolution"] = commercial.get("monthly_evolution") or []
    bundle["annual_comparison"] = commercial.get("annual_comparison") or {}
    bundle["commercial_coverage"] = sales_coverage
    reading_status = "unavailable_for_requested_window" if sales_coverage.get("mode") == "requested_outside_coverage" else "mart_snapshot"
    bundle["reading_status"] = reading_status
    freshness = dict(bundle.get("freshness") or {})
    freshness["mode"] = reading_status
    freshness["source"] = "torqmind_mart.agg_vendas_diaria"
    freshness["historical_through_dt"] = _iso_or_none(dt_fim)
    freshness["live_through_at"] = (bundle.get("operational_sync") or {}).get("last_sync_at")
    freshness["snapshot_generated_at"] = (bundle.get("operational_sync") or {}).get("snapshot_generated_at")
    bundle["freshness"] = freshness
    return bundle


def sales_operational_day_bundle(
    role: str,
    id_empresa: int,
    id_filial: Any,
    day_ref: date,
    *,
    include_rankings: bool = True,
    canonicalize_groups: bool = True,
) -> Optional[Dict[str, Any]]:
    day_key = _date_key(day_ref)
    conn_branch_id = _conn_branch_id(id_filial)
    kpis = dashboard_kpis(role, id_empresa, id_filial, day_ref, day_ref)
    by_hour = sales_by_hour(role, id_empresa, id_filial, day_ref, day_ref) if include_rankings else []
    top_products = sales_top_products(role, id_empresa, id_filial, day_ref, day_ref, limit=15) if include_rankings else []
    top_groups = sales_top_groups(role, id_empresa, id_filial, day_ref, day_ref, limit=10) if include_rankings else []
    top_employees = sales_top_employees(role, id_empresa, id_filial, day_ref, day_ref, limit=10) if include_rankings else []
    sync_meta = _sales_sync_meta(role, id_empresa, id_filial, day_ref, day_ref)
    faturamento = _to_float(kpis.get("faturamento"))
    margem = _to_float(kpis.get("margem"))
    vendas = sum(_to_int(row.get("vendas")) for row in by_hour)
    if faturamento <= 0 and margem <= 0 and vendas <= 0 and not by_hour:
        return None
    return {
        "kpis": {
            "faturamento": faturamento,
            "margem": margem,
            "ticket_medio": _to_float(kpis.get("ticket_medio")),
            "devolucoes": 0.0,
        },
        "by_day": [
            {
                "data_key": day_key,
                "id_filial": conn_branch_id,
                "faturamento": faturamento,
                "margem": margem,
            }
        ],
        "by_hour": by_hour,
        "top_products": top_products,
        "top_groups": top_groups,
        "top_employees": top_employees,
        "stats": {"vendas": vendas, "data_key": day_key},
        "operational_sync": sync_meta,
        "freshness": {
            "mode": "live_day",
            "operational_day": day_ref.isoformat(),
            "live_through_at": sync_meta.get("last_sync_at"),
            "snapshot_generated_at": sync_meta.get("snapshot_generated_at"),
            "historical_through_dt": None,
            "source": "torqmind_mart.agg_vendas_diaria",
        },
    }


def sales_operational_range_bundle(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    *,
    include_rankings: bool = True,
    canonicalize_groups: bool = True,
) -> Optional[Dict[str, Any]]:
    if dt_fim < dt_ini:
        return None
    bundle = _sales_historical_bundle_from_marts(role, id_empresa, id_filial, dt_ini, dt_fim, include_details=include_rankings)
    faturamento = _to_float((bundle.get("kpis") or {}).get("faturamento"))
    margem = _to_float((bundle.get("kpis") or {}).get("margem"))
    vendas = _to_int((bundle.get("stats") or {}).get("vendas"))
    if faturamento <= 0 and margem <= 0 and vendas <= 0 and not bundle.get("by_day"):
        return None
    freshness = dict(bundle.get("freshness") or {})
    freshness.update(
        {
            "mode": "live_range",
            "operational_day": dt_fim.isoformat(),
            "live_through_at": (bundle.get("operational_sync") or {}).get("last_sync_at"),
            "snapshot_generated_at": (bundle.get("operational_sync") or {}).get("snapshot_generated_at"),
            "historical_through_dt": dt_fim.isoformat(),
            "source": "torqmind_mart.agg_vendas_diaria",
        }
    )
    sync = dict(bundle.get("operational_sync") or {})
    sync.update({"source": "torqmind_mart.agg_vendas_diaria", "dt_ref": dt_fim.isoformat()})
    bundle["freshness"] = freshness
    bundle["operational_sync"] = sync
    return bundle


def sales_operational_current(
    role: str,
    id_empresa: int,
    id_filial: Any,
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


def fraud_kpis(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT sum(cancelamentos) AS cancelamentos, sum(valor_cancelado) AS valor_cancelado
        FROM torqmind_mart.fraude_cancelamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return {"cancelamentos": _to_int(row.get("cancelamentos")), "valor_cancelado": _to_float(row.get("valor_cancelado"))}


def fraud_series(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT data_key, id_filial, sum(cancelamentos) AS cancelamentos, sum(valor_cancelado) AS valor_cancelado
        FROM torqmind_mart.fraude_cancelamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY data_key, id_filial
        ORDER BY data_key, id_filial
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return [
        {"data_key": _to_int(row.get("data_key")), "id_filial": _to_int(row.get("id_filial")), "cancelamentos": _to_int(row.get("cancelamentos")), "valor_cancelado": _to_float(row.get("valor_cancelado"))}
        for row in rows
    ]


def fraud_data_window(role: str, id_empresa: int, id_filial: Any) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT min(data_key) AS min_data_key, max(data_key) AS max_data_key, count() AS rows
        FROM torqmind_mart.fraude_cancelamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    return {"min_data_key": row.get("min_data_key"), "max_data_key": row.get("max_data_key"), "rows": _to_int(row.get("rows"))}


def fraud_last_events(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 30) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT id_filial, filial_nome, id_db, id_comprovante, data, data_key, id_usuario, id_usuario AS id_usuario_documento,
               usuario_source, usuario_nome, id_turno, turno_value, valor_total
        FROM torqmind_mart.fraude_cancelamentos_eventos
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        ORDER BY data DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["valor_total"] = _to_float(row.get("valor_total"))
    return rows


def fraud_top_users(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_usuario,
          max(usuario_nome) AS usuario_nome,
          count() AS cancelamentos,
          sum(valor_total) AS valor_cancelado,
          countIf(usuario_source = 'turno') AS resolvidos_por_turno,
          countIf(usuario_source = 'comprovante') AS fallback_comprovante
        FROM torqmind_mart.fraude_cancelamentos_eventos
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY id_usuario
        ORDER BY valor_cancelado DESC, cancelamentos DESC, id_usuario
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["cancelamentos"] = _to_int(row.get("cancelamentos"))
        row["valor_cancelado"] = _to_float(row.get("valor_cancelado"))
    return rows


def risk_kpis(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          sum(eventos_risco_total) AS total_eventos,
          sum(eventos_alto_risco) AS eventos_alto_risco,
          sum(impacto_estimado_total) AS impacto_total,
          avg(score_medio) AS score_medio
        FROM torqmind_mart.agg_risco_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return {
        "total_eventos": _to_int(row.get("total_eventos")),
        "eventos_alto_risco": _to_int(row.get("eventos_alto_risco")),
        "impacto_total": _to_float(row.get("impacto_total")),
        "score_medio": _to_float(row.get("score_medio")),
    }


def risk_series(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT data_key, id_filial, eventos_risco_total, eventos_alto_risco, impacto_estimado_total, score_medio, p95_score
        FROM torqmind_mart.agg_risco_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        ORDER BY data_key, id_filial
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return rows


def risk_data_window(role: str, id_empresa: int, id_filial: Any) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT min(data_key) AS min_data_key, max(data_key) AS max_data_key, count() AS rows
        FROM torqmind_mart.agg_risco_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    return {"min_data_key": row.get("min_data_key"), "max_data_key": row.get("max_data_key"), "rows": _to_int(row.get("rows"))}


def risk_top_employees(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_funcionario,
          max(funcionario_nome) AS funcionario_nome,
          sum(eventos) AS eventos,
          sum(alto_risco) AS alto_risco,
          sum(impacto_estimado) AS impacto_estimado,
          avg(score_medio) AS score_medio
        FROM torqmind_mart.risco_top_funcionarios_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND id_funcionario != -1
          {branch}
        GROUP BY id_funcionario
        HAVING funcionario_nome != ''
        ORDER BY impacto_estimado DESC, score_medio DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    return rows


def risk_last_events(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 30) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id,
          id_filial,
          filial_nome,
          data_key,
          data,
          event_type,
          id_db,
          id_comprovante,
          id_movprodutos,
          id_usuario,
          id_funcionario,
          funcionario_nome,
          id_turno,
          turno_value,
          operador_caixa_id,
          operador_caixa_nome,
          operador_caixa_source,
          id_cliente,
          valor_total,
          impacto_estimado,
          score_risco,
          score_level,
          reasons
        FROM torqmind_mart.risco_eventos_recentes
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        ORDER BY data DESC, id DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["event_label"] = _event_type_label(row.get("event_type"))
        row["funcionario_label"] = _employee_label(row.get("funcionario_nome"), row.get("id_funcionario"))
        row["operador_caixa_label"] = _cash_operator_label(row.get("operador_caixa_nome"), row.get("operador_caixa_id") or row.get("id_usuario"))
        reasons = _json_obj(row.get("reasons"))
        row["reasons"] = reasons
        row["reasons_humanized"] = [_event_type_label(row.get("event_type"))]
        row["reason_summary"] = row["reasons_humanized"][0]
        if row.get("operador_caixa_nome") or row.get("operador_caixa_id"):
            row["responsavel_label"] = row["operador_caixa_label"]
            row["responsavel_kind"] = "operador_caixa"
        else:
            row["responsavel_label"] = row["funcionario_label"]
            row["responsavel_kind"] = "colaborador_venda"
    return rows


def risk_by_turn_local(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_filial,
          max(filial_nome) AS filial_nome,
          id_turno,
          max(turno_value) AS turno_value,
          id_local_venda,
          max(local_nome) AS local_nome,
          sum(eventos) AS eventos,
          sum(alto_risco) AS alto_risco,
          sum(impacto_estimado) AS impacto_estimado,
          avg(score_medio) AS score_medio
        FROM torqmind_mart.risco_turno_local_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY id_filial, id_turno, id_local_venda
        ORDER BY impacto_estimado DESC, score_medio DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["local_label"] = "Canal não informado" if not row.get("local_nome") else row.get("local_nome")
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    return rows


def operational_score(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    sales = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    risk = risk_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    faturamento = _to_float(sales.get("faturamento"))
    margem = _to_float(sales.get("margem"))
    ticket = _to_float(sales.get("ticket_medio"))
    eventos_alto = _to_int(risk.get("eventos_alto_risco"))
    eventos_total = _to_int(risk.get("total_eventos"))
    impacto = _to_float(risk.get("impacto_total"))
    margem_ratio = margem / faturamento if faturamento > 0 else 0.0
    margem_score = min(100.0, max(0.0, (margem_ratio / 0.15) * 100))
    risk_density = eventos_alto / eventos_total if eventos_total > 0 else 0.0
    risk_score = max(0.0, 100.0 - min(100.0, risk_density * 120.0 + (impacto / max(faturamento, 1.0)) * 100.0))
    ticket_score = min(100.0, max(0.0, (ticket / 120.0) * 100.0))
    score = round((margem_score * 0.45) + (risk_score * 0.40) + (ticket_score * 0.15), 2)
    return {"score": max(0, min(100, score)), "components": {"margem_score": round(margem_score, 2), "risk_score": round(risk_score, 2), "ticket_score": round(ticket_score, 2)}}


def customers_top(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_cliente,
          max(cliente_nome) AS cliente_nome,
          sum(monetary_90) AS faturamento,
          sum(frequency_90) AS compras,
          max(last_purchase) AS ultima_compra,
          if(sum(frequency_90) = 0, 0, sum(monetary_90) / sum(frequency_90)) AS ticket_medio
        FROM torqmind_mart.customer_rfm_daily
        WHERE id_empresa = {{id_empresa:Int32}}
          AND dt_ref <= {{dt_fim:Date}}
          AND id_cliente != -1
          {branch}
        GROUP BY id_cliente
        ORDER BY faturamento DESC, compras DESC, id_cliente
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "dt_fim": dt_fim, "limit": int(limit)},
        id_empresa,
    )
    return rows


def customers_rfm_snapshot(role: str, id_empresa: int, id_filial: Any, as_of: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          countIf(id_cliente != -1) AS clientes_identificados,
          countIf(last_purchase >= {{as_of:Date}} - 7) AS ativos_7d,
          countIf(recency_days > 30) AS em_risco_30d,
          sum(monetary_90) AS faturamento_90d
        FROM torqmind_mart.customer_rfm_daily
        WHERE id_empresa = {{id_empresa:Int32}}
          AND dt_ref = (
            SELECT max(dt_ref)
            FROM torqmind_mart.customer_rfm_daily
            WHERE id_empresa = {{id_empresa:Int32}}
              AND dt_ref <= {{as_of:Date}}
              {branch}
          )
          {branch}
        """,
        {"id_empresa": int(id_empresa), "as_of": as_of},
        id_empresa,
    )
    return {
        "clientes_identificados": _to_int(row.get("clientes_identificados")),
        "ativos_7d": _to_int(row.get("ativos_7d")),
        "em_risco_30d": _to_int(row.get("em_risco_30d")),
        "faturamento_90d": _to_float(row.get("faturamento_90d")),
    }


def customers_churn_risk(role: str, id_empresa: int, id_filial: Any, min_score: int = 60, limit: int = 10) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT id_cliente, cliente_nome, churn_score, last_purchase, compras_30d, compras_60_30, faturamento_30d, faturamento_60_30, reasons
        FROM torqmind_mart.clientes_churn_risco
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_cliente != -1
          AND churn_score >= {{min_score:Int32}}
          {branch}
        ORDER BY churn_score DESC, faturamento_60_30 DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "min_score": int(min_score), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["reasons"] = _json_obj(row.get("reasons"))
    return rows


def _customers_churn_operational_current(id_empresa: int, id_filial: Any, as_of: Optional[date], min_score: int, limit: int, id_cliente: Optional[int] = None) -> List[Dict[str, Any]]:
    effective_as_of = as_of or business_today(id_empresa)
    branch = _branch_clause("id_filial", id_filial)
    customer_clause = "" if id_cliente is None else " AND id_cliente = {id_cliente:Int32}"
    rows = _run(
        f"""
        SELECT
          {{as_of:Date}} AS dt_ref,
          id_cliente,
          cliente_nome,
          last_purchase,
          dateDiff('day', last_purchase, {{as_of:Date}}) AS recency_days,
          30 AS expected_cycle_days,
          compras_30d AS frequency_30,
          compras_30d + compras_60_30 AS frequency_90,
          faturamento_30d AS monetary_30,
          faturamento_30d + faturamento_60_30 AS monetary_90,
          if(compras_30d > 0, faturamento_30d / compras_30d, 0) AS ticket_30,
          churn_score,
          greatest(faturamento_60_30, 0) AS revenue_at_risk_30d,
          'Leitura operacional corrente do churn; snapshot diário exato indisponível para a data solicitada.' AS recommendation,
          reasons,
          updated_at
        FROM torqmind_mart.clientes_churn_risco
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_cliente != -1
          AND churn_score >= {{min_score:Int32}}
          {branch}
          {customer_clause}
        ORDER BY churn_score DESC, faturamento_60_30 DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "as_of": effective_as_of, "min_score": int(min_score), "limit": int(limit), "id_cliente": int(id_cliente or 0)},
        id_empresa,
    )
    for row in rows:
        row["reasons"] = _json_obj(row.get("reasons"))
    return rows


def customers_churn_bundle(role: str, id_empresa: int, id_filial: Any, as_of: Optional[date] = None, min_score: int = 60, limit: int = 20) -> Dict[str, Any]:
    requested_as_of = as_of or business_today(id_empresa)
    snapshot_meta = _snapshot_meta("customer_churn_risk_daily", id_empresa, id_filial, requested_as_of, "latest_leq_ref")
    rows: List[Dict[str, Any]] = []
    effective_dt_ref = snapshot_meta.get("effective_dt_ref")
    if effective_dt_ref:
        branch = _branch_clause("id_filial", id_filial)
        rows = _run(
            f"""
            SELECT
              dt_ref, id_cliente, cliente_nome, last_purchase, recency_days, expected_cycle_days,
              frequency_30, frequency_90, monetary_30, monetary_90, ticket_30, churn_score,
              revenue_at_risk_30d, recommendation, reasons, updated_at
            FROM torqmind_mart.customer_churn_risk_daily
            WHERE id_empresa = {{id_empresa:Int32}}
              AND dt_ref = {{dt_ref:Date}}
              AND id_cliente != -1
              AND churn_score >= {{min_score:Int32}}
              {branch}
            ORDER BY churn_score DESC, revenue_at_risk_30d DESC
            LIMIT {{limit:UInt32}}
            """,
            {"id_empresa": int(id_empresa), "dt_ref": effective_dt_ref, "min_score": int(min_score), "limit": int(limit)},
            id_empresa,
        )
    if not rows:
        rows = _customers_churn_operational_current(id_empresa, id_filial, requested_as_of, min_score, limit)
        if rows:
            snapshot_meta = {
                **snapshot_meta,
                "snapshot_status": "operational_current",
                "precision_mode": "operational_current",
                "effective_dt_ref": rows[0].get("dt_ref"),
                "source_table": "torqmind_mart.clientes_churn_risco",
                "source_kind": "operational_current",
                "latest_updated_at": max((row.get("updated_at") for row in rows), default=None),
                "row_count": len(rows),
            }
    for row in rows:
        row["reasons"] = _json_obj(row.get("reasons"))
    total_revenue_at_risk = sum(_to_float(row.get("revenue_at_risk_30d")) for row in rows)
    avg_churn_score = round(sum(_to_float(row.get("churn_score")) for row in rows) / len(rows), 2) if rows else 0.0
    return {
        "top_risk": rows,
        "summary": {"total_top_risk": len(rows), "avg_churn_score": avg_churn_score, "revenue_at_risk_30d": round(total_revenue_at_risk, 2)},
        "snapshot_meta": snapshot_meta,
    }


def customers_churn_diamond(role: str, id_empresa: int, id_filial: Any, as_of: Optional[date] = None, min_score: int = 60, limit: int = 20) -> List[Dict[str, Any]]:
    return customers_churn_bundle(role, id_empresa, id_filial, as_of=as_of, min_score=min_score, limit=limit)["top_risk"]


def customers_churn_snapshot_meta(role: str, id_empresa: int, id_filial: Any, as_of: Optional[date]) -> Dict[str, Any]:
    requested_as_of = as_of or business_today(id_empresa)
    snapshot_meta = _snapshot_meta("customer_churn_risk_daily", id_empresa, id_filial, requested_as_of, "latest_leq_ref")
    if snapshot_meta.get("snapshot_status") != "missing":
        return snapshot_meta
    fallback_rows = _customers_churn_operational_current(id_empresa, id_filial, requested_as_of, min_score=0, limit=1)
    if not fallback_rows:
        return snapshot_meta
    return {
        **snapshot_meta,
        "snapshot_status": "operational_current",
        "precision_mode": "operational_current",
        "effective_dt_ref": fallback_rows[0].get("dt_ref"),
        "source_table": "torqmind_mart.clientes_churn_risco",
        "source_kind": "operational_current",
        "latest_updated_at": fallback_rows[0].get("updated_at"),
    }


def customer_churn_drilldown(role: str, id_empresa: int, id_filial: Any, id_cliente: int, dt_ini: date, dt_fim: date, as_of: Optional[date] = None) -> Dict[str, Any]:
    snapshot_meta = customers_churn_snapshot_meta(role, id_empresa, id_filial, as_of)
    branch = _branch_clause("id_filial", id_filial)
    series = _run(
        f"""
        SELECT toYYYYMMDD(dt_ref) AS data_key, monetary_30 AS faturamento, frequency_30 AS compras
        FROM torqmind_mart.customer_rfm_daily
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_cliente = {{id_cliente:Int32}}
          AND dt_ref BETWEEN {{dt_ini:Date}} AND {{dt_fim:Date}}
          {branch}
        ORDER BY dt_ref
        """,
        {"id_empresa": int(id_empresa), "id_cliente": int(id_cliente), "dt_ini": dt_ini, "dt_fim": dt_fim},
        id_empresa,
    )
    snapshot: Dict[str, Any] = {}
    effective_dt_ref = snapshot_meta.get("effective_dt_ref")
    if effective_dt_ref:
        rows = _run(
            f"""
            SELECT
              dt_ref,
              id_empresa,
              id_filial,
              id_cliente,
              cliente_nome,
              last_purchase,
              recency_days,
              frequency_30,
              frequency_90,
              monetary_30,
              monetary_90,
              ticket_30,
              expected_cycle_days,
              churn_score,
              revenue_at_risk_30d,
              recommendation,
              reasons,
              updated_at
            FROM torqmind_mart.customer_churn_risk_daily
            WHERE id_empresa = {{id_empresa:Int32}}
              AND id_cliente = {{id_cliente:Int32}}
              AND dt_ref = {{dt_ref:Date}}
              {branch}
            LIMIT 1
            """,
            {"id_empresa": int(id_empresa), "id_cliente": int(id_cliente), "dt_ref": effective_dt_ref},
            id_empresa,
        )
        snapshot = dict(rows[0]) if rows else {}
    if not snapshot and snapshot_meta.get("snapshot_status") == "operational_current":
        fallback = _customers_churn_operational_current(id_empresa, id_filial, as_of, min_score=0, limit=1, id_cliente=id_cliente)
        snapshot = fallback[0] if fallback else {}
    return {"snapshot": snapshot, "series": series, "snapshot_meta": snapshot_meta}


def anonymous_retention_overview(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    series = _run(
        f"""
        SELECT toYYYYMMDD(dt_ref) AS data_key, id_filial, anon_faturamento_7d, anon_faturamento_prev_28d,
               trend_pct, anon_share_pct_7d, repeat_proxy_idx, impact_estimated_7d
        FROM torqmind_mart.anonymous_retention_daily
        WHERE id_empresa = {{id_empresa:Int32}}
          AND dt_ref BETWEEN {{dt_ini:Date}} AND {{dt_fim:Date}}
          {branch}
        ORDER BY dt_ref, id_filial
        """,
        {"id_empresa": int(id_empresa), "dt_ini": dt_ini, "dt_fim": dt_fim},
        id_empresa,
    )
    latest_rows = _run(
        f"""
        SELECT
          dt_ref,
          id_empresa,
          id_filial,
          anon_faturamento_7d,
          anon_faturamento_prev_28d,
          trend_pct,
          anon_share_pct_7d,
          repeat_proxy_idx,
          impact_estimated_7d,
          details,
          updated_at
        FROM torqmind_mart.anonymous_retention_daily
        WHERE id_empresa = {{id_empresa:Int32}}
          AND dt_ref = (
            SELECT max(dt_ref)
            FROM torqmind_mart.anonymous_retention_daily
            WHERE id_empresa = {{id_empresa:Int32}}
              AND dt_ref <= {{dt_fim:Date}}
              {branch}
          )
          {branch}
        ORDER BY id_filial
        """,
        {"id_empresa": int(id_empresa), "dt_fim": dt_fim},
        id_empresa,
    )
    agg_impact = sum(_to_float(row.get("impact_estimated_7d")) for row in latest_rows)
    avg_trend = round(sum(_to_float(row.get("trend_pct")) for row in latest_rows) / len(latest_rows), 2) if latest_rows else 0.0
    avg_repeat = round(sum(_to_float(row.get("repeat_proxy_idx")) for row in latest_rows) / len(latest_rows), 2) if latest_rows else 0.0
    recommendation = (
        "Recorrência anônima caiu. Ajuste a operação por horário/dia, reveja o mix de produtos e acione promoções de retorno."
        if avg_trend < -8
        else "Recorrência anônima estável. Monitore horários de maior queda e mantenha ações de fidelização."
    )
    return {
        "kpis": {
            "impact_estimated_7d": round(agg_impact, 2),
            "trend_pct": avg_trend,
            "repeat_proxy_idx": avg_repeat,
            "severity": "CRITICAL" if avg_trend <= -15 else ("WARN" if avg_trend <= -8 else "OK"),
            "recommendation": recommendation,
        },
        "latest": latest_rows,
        "series": series,
        "breakdown_dow": [],
        "breakdown_hour": [],
        "mix": [],
    }


def finance_kpis(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          sumIf(valor_total, tipo_titulo = 1) AS receber_total,
          sumIf(valor_pago, tipo_titulo = 1) AS receber_pago,
          sumIf(valor_aberto, tipo_titulo = 1) AS receber_aberto,
          sumIf(valor_total, tipo_titulo = 0) AS pagar_total,
          sumIf(valor_pago, tipo_titulo = 0) AS pagar_pago,
          sumIf(valor_aberto, tipo_titulo = 0) AS pagar_aberto
        FROM torqmind_mart.financeiro_vencimentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    return {key: _to_float(row.get(key)) for key in ("receber_total", "receber_pago", "receber_aberto", "pagar_total", "pagar_pago", "pagar_aberto")}


def finance_series(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    return _run(
        f"""
        SELECT data_key, id_filial, tipo_titulo, valor_total, valor_pago, valor_aberto
        FROM torqmind_mart.financeiro_vencimentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        ORDER BY data_key, tipo_titulo, id_filial
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )


def finance_aging_overview(role: str, id_empresa: int, id_filial: Any, as_of: Optional[date] = None) -> Dict[str, Any]:
    requested_as_of = as_of or business_today(id_empresa)
    snapshot_meta = _snapshot_meta("finance_aging_daily", id_empresa, id_filial, requested_as_of, "latest_leq_ref")
    effective_dt_ref = snapshot_meta.get("effective_dt_ref")
    if effective_dt_ref:
        branch = _branch_clause("id_filial", id_filial)
        row = _first(
            f"""
            SELECT
              {{dt_ref:Date}} AS dt_ref,
              sum(receber_total_aberto) AS receber_total_aberto,
              sum(receber_total_vencido) AS receber_total_vencido,
              sum(pagar_total_aberto) AS pagar_total_aberto,
              sum(pagar_total_vencido) AS pagar_total_vencido,
              sum(bucket_0_7) AS bucket_0_7,
              sum(bucket_8_15) AS bucket_8_15,
              sum(bucket_16_30) AS bucket_16_30,
              sum(bucket_31_60) AS bucket_31_60,
              sum(bucket_60_plus) AS bucket_60_plus,
              avg(top5_concentration_pct) AS top5_concentration_pct,
              max(data_gaps) AS data_gaps,
              count() AS snapshot_rows
            FROM torqmind_mart.finance_aging_daily
            WHERE id_empresa = {{id_empresa:Int32}}
              AND dt_ref = {{dt_ref:Date}}
              {branch}
            """,
            {"id_empresa": int(id_empresa), "dt_ref": effective_dt_ref},
            id_empresa,
        )
        if _to_int(row.get("snapshot_rows")) > 0:
            payload = {
                "dt_ref": effective_dt_ref,
                "receber_total_aberto": _to_float(row.get("receber_total_aberto")),
                "receber_total_vencido": _to_float(row.get("receber_total_vencido")),
                "pagar_total_aberto": _to_float(row.get("pagar_total_aberto")),
                "pagar_total_vencido": _to_float(row.get("pagar_total_vencido")),
                "bucket_0_7": _to_float(row.get("bucket_0_7")),
                "bucket_8_15": _to_float(row.get("bucket_8_15")),
                "bucket_16_30": _to_float(row.get("bucket_16_30")),
                "bucket_31_60": _to_float(row.get("bucket_31_60")),
                "bucket_60_plus": _to_float(row.get("bucket_60_plus")),
                "top5_concentration_pct": _to_float(row.get("top5_concentration_pct")),
                "data_gaps": bool(row.get("data_gaps")),
                "snapshot_rows": _to_int(row.get("snapshot_rows")),
            }
            payload.update(snapshot_meta)
            payload["source_kind"] = "snapshot"
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


def cash_dre_summary(role: str, id_empresa: int, id_filial: Any, as_of: date) -> Dict[str, Any]:
    aging = finance_aging_overview(role, id_empresa, id_filial, as_of)
    has_snapshot = bool(_to_int(aging.get("snapshot_rows")))
    dt_ref = aging.get("dt_ref") if has_snapshot else None
    if not has_snapshot:
        return {
            "source_status": "unavailable",
            "summary": "Sem base financeira disponível para o período.",
            "cards": [
                {
                    "key": "contas_pagar_futuro_banco",
                    "label": "Contas a pagar futuras",
                    "status": "unavailable",
                    "amount": None,
                    "titles": None,
                    "detail": "Sem base financeira disponível para calcular títulos futuros.",
                },
                {
                    "key": "contas_receber",
                    "label": "Contas a receber",
                    "status": "unavailable",
                    "amount": None,
                    "titles": None,
                    "detail": "Sem base financeira disponível para calcular recebíveis em aberto.",
                },
                {
                    "key": "saldo_liquido_aberto",
                    "label": "Saldo líquido aberto",
                    "status": "unavailable",
                    "amount": None,
                    "titles": None,
                    "detail": "Sem base financeira disponível para calcular o saldo líquido.",
                },
            ],
            "pending": [],
            "stock": {"cards": []},
            "dt_ref": None,
        }

    pagar_total_aberto = _to_float(aging.get("pagar_total_aberto"))
    pagar_total_vencido = _to_float(aging.get("pagar_total_vencido"))
    receber_aberto = _to_float(aging.get("receber_total_aberto"))
    pagar_futuro = round(max(pagar_total_aberto - pagar_total_vencido, 0.0), 2)
    saldo_liquido = round(receber_aberto - pagar_futuro, 2)
    return {
        "source_status": "ok",
        "summary": "Resumo financeiro calculado a partir da mart financeira publicada.",
        "cards": [
            {
                "key": "contas_pagar_futuro_banco",
                "label": "Contas a pagar futuras",
                "status": "ready",
                "amount": pagar_futuro,
                "titles": None,
                "detail": "Títulos a pagar em aberto, descontando o que já está vencido.",
            },
            {
                "key": "contas_receber",
                "label": "Contas a receber",
                "status": "ready",
                "amount": round(receber_aberto, 2),
                "titles": None,
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
        ],
        "pending": [],
        "stock": {"cards": []},
        "dt_ref": dt_ref.isoformat() if isinstance(dt_ref, date) else dt_ref,
    }


def payments_overview_kpis(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    days = max((dt_fim - dt_ini).days + 1, 1)
    prev_fim = _date_key(dt_ini) - 1
    prev_ini = _date_key(dt_ini - timedelta(days=days))
    branch = _branch_clause("id_filial", id_filial)
    curr = _first(
        f"""
        SELECT
          sum(total_valor) AS valor_total,
          sumIf(total_valor, category = 'NAO_IDENTIFICADO') AS unknown_valor,
          sum(qtd_comprovantes) AS qtd_comprovantes,
          count() AS row_count,
          countIf(total_valor > 0) AS nonzero_rows
        FROM torqmind_mart.agg_pagamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    prev = _first(
        f"""
        SELECT sum(total_valor) AS total_valor
        FROM torqmind_mart.agg_pagamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa), "ini": prev_ini, "fim": prev_fim},
        id_empresa,
    )
    mix = _run(
        f"""
        SELECT category, label, sum(total_valor) AS total_valor
        FROM torqmind_mart.agg_pagamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY category, label
        ORDER BY total_valor DESC
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    total_curr = _to_float(curr.get("valor_total"))
    total_prev = _to_float(prev.get("total_valor"))
    unknown_val = _to_float(curr.get("unknown_valor"))
    row_count = _to_int(curr.get("row_count"))
    nonzero_rows = _to_int(curr.get("nonzero_rows"))
    unknown_share = (unknown_val / total_curr * 100.0) if total_curr > 0 else 0.0
    delta_pct = ((total_curr - total_prev) / total_prev * 100.0) if total_prev > 0 else (100.0 if total_curr > 0 else 0.0)
    for row in mix:
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
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
        "qtd_comprovantes": _to_int(curr.get("qtd_comprovantes")),
        "row_count": row_count,
        "nonzero_rows": nonzero_rows,
        "unknown_valor": round(unknown_val, 2),
        "unknown_share_pct": round(unknown_share, 2),
        "source_status": source_status,
        "summary": summary,
        "mix": mix,
    }


def payments_by_day(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT data_key, id_filial, category, label, total_valor, qtd_comprovantes, share_percent
        FROM torqmind_mart.agg_pagamentos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        ORDER BY data_key, total_valor DESC
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim)},
        id_empresa,
    )
    for row in rows:
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
    return rows


def payments_by_turno(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 18) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_filial,
          '' AS filial_nome,
          id_turno,
          toString(id_turno) AS turno_value,
          category,
          label,
          sum(total_valor) AS total_valor,
          sum(qtd_comprovantes) AS qtd_comprovantes,
          countDistinct(data_key) AS dias_com_movimento
        FROM torqmind_mart.agg_pagamentos_turno
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        GROUP BY id_filial, id_turno, category, label
        ORDER BY total_valor DESC, qtd_comprovantes DESC, id_filial, id_turno
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    return rows


def payments_anomalies(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          data_key,
          id_filial,
          '' AS filial_nome,
          id_turno,
          toString(id_turno) AS turno_value,
          event_type,
          severity,
          score,
          valor_total AS impacto_estimado,
          '{{}}' AS reasons,
          insight_id_hash AS insight_id,
          insight_id_hash
        FROM torqmind_mart.pagamentos_anomalias_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
        ORDER BY score DESC, valor_total DESC, data_key DESC
        LIMIT {{limit:UInt32}}
        """,
        {"id_empresa": int(id_empresa), "ini": _date_key(dt_ini), "fim": _date_key(dt_fim), "limit": int(limit)},
        id_empresa,
    )
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["event_label"] = _event_type_label(row.get("event_type"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["reasons"] = _json_obj(row.get("reasons"))
    return rows


def payments_overview(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, anomaly_limit: int = 20) -> Dict[str, Any]:
    return {
        "kpis": payments_overview_kpis(role, id_empresa, id_filial, dt_ini, dt_fim),
        "by_day": payments_by_day(role, id_empresa, id_filial, dt_ini, dt_fim),
        "by_turno": payments_by_turno(role, id_empresa, id_filial, dt_ini, dt_fim),
        "anomalies": payments_anomalies(role, id_empresa, id_filial, dt_ini, dt_fim, limit=anomaly_limit),
    }


def _cash_live_now(role: str, id_empresa: int, id_filial: Any) -> Dict[str, Any]:
    branch = _branch_clause("id_filial", id_filial)
    summary = _first(
        f"""
        SELECT
          count() AS caixas_abertos_fonte,
          countIf(severity != 'STALE') AS caixas_abertos,
          countIf(severity = 'STALE') AS caixas_stale,
          countIf(severity = 'CRITICAL') AS caixas_criticos,
          countIf(severity = 'HIGH') AS caixas_alto_risco,
          countIf(severity = 'WARN') AS caixas_em_monitoramento,
          sumIf(total_vendas, severity != 'STALE') AS total_vendas_abertas,
          sumIf(total_cancelamentos, severity != 'STALE') AS total_cancelamentos_abertas,
          toUnixTimestamp(max(updated_at)) AS snapshot_epoch,
          toUnixTimestamp(max(updated_at)) AS latest_activity_epoch
        FROM torqmind_mart.agg_caixa_turno_aberto
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch}
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    open_rows = _run(
        f"""
        SELECT
          id_filial, filial_nome, id_turno, toString(id_turno) AS turno_value, id_usuario, usuario_nome,
          'turno_id' AS usuario_source,
          toUnixTimestamp(abertura_ts) AS abertura_ts_epoch,
          toUnixTimestamp(updated_at) AS last_activity_ts_epoch,
          toUnixTimestamp(updated_at) AS snapshot_ts_epoch,
          horas_aberto, 0 AS horas_sem_movimento, severity, status_label, total_vendas, qtd_vendas,
          total_cancelamentos, qtd_cancelamentos, 0 AS total_devolucoes, 0 AS qtd_devolucoes, total_pagamentos
        FROM torqmind_mart.agg_caixa_turno_aberto
        WHERE id_empresa = {{id_empresa:Int32}}
          AND severity != 'STALE'
          {branch}
        ORDER BY multiIf(severity = 'CRITICAL', 0, severity = 'HIGH', 1, severity = 'WARN', 2, 3), horas_aberto DESC, updated_at DESC
        LIMIT 20
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    stale_rows = _run(
        f"""
        SELECT
          id_filial, filial_nome, id_turno, toString(id_turno) AS turno_value, id_usuario, usuario_nome,
          'turno_id' AS usuario_source,
          toUnixTimestamp(abertura_ts) AS abertura_ts_epoch,
          toUnixTimestamp(updated_at) AS last_activity_ts_epoch,
          toUnixTimestamp(updated_at) AS snapshot_ts_epoch,
          horas_aberto, 0 AS horas_sem_movimento, total_vendas, total_cancelamentos, 0 AS total_devolucoes
        FROM torqmind_mart.agg_caixa_turno_aberto
        WHERE id_empresa = {{id_empresa:Int32}}
          AND severity = 'STALE'
          {branch}
        ORDER BY updated_at DESC, horas_aberto DESC
        LIMIT 10
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    payment_mix_rows = _run(
        f"""
        SELECT forma_label AS label, forma_category AS category, sum(total_valor) AS total_valor,
               sum(qtd_comprovantes) AS qtd_comprovantes, countDistinct(id_turno) AS qtd_turnos
        FROM torqmind_mart.agg_caixa_forma_pagamento
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch}
        GROUP BY forma_label, forma_category
        ORDER BY total_valor DESC
        """,
        {"id_empresa": int(id_empresa)},
        id_empresa,
    )
    for row in open_rows:
        row["abertura_ts"] = _iso_from_clickhouse_epoch(row.pop("abertura_ts_epoch", None), id_empresa)
        row["last_activity_ts"] = _iso_from_clickhouse_epoch(row.pop("last_activity_ts_epoch", None), id_empresa)
        row["snapshot_ts"] = _iso_from_clickhouse_epoch(row.pop("snapshot_ts_epoch", None), id_empresa)
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
        row["total_devolucoes"] = _to_float(row.get("total_devolucoes"))
        row["caixa_liquido"] = _to_float(row.get("total_vendas")) - _to_float(row.get("total_cancelamentos"))
        row["alert_message"] = f"O turno {row['turno_label']} da {row['filial_label']} segue aberto há {row.get('horas_aberto') or 0} horas."
    for row in stale_rows:
        row["abertura_ts"] = _iso_from_clickhouse_epoch(row.pop("abertura_ts_epoch", None), id_empresa)
        row["last_activity_ts"] = _iso_from_clickhouse_epoch(row.pop("last_activity_ts_epoch", None), id_empresa)
        row["snapshot_ts"] = _iso_from_clickhouse_epoch(row.pop("snapshot_ts_epoch", None), id_empresa)
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = _cash_operator_label(row.get("usuario_nome"), row.get("id_usuario"))
        row["turno_label"] = _turno_label(row.get("turno_value"), row.get("id_turno"))
    total_vendas = _to_float(summary.get("total_vendas_abertas"))
    total_cancelamentos = _to_float(summary.get("total_cancelamentos_abertas"))
    caixa_liquido = round(total_vendas - total_cancelamentos, 2)
    critical_count = _to_int(summary.get("caixas_criticos"))
    high_count = _to_int(summary.get("caixas_alto_risco"))
    warn_count = _to_int(summary.get("caixas_em_monitoramento"))
    operational_open_total = _to_int(summary.get("caixas_abertos"))
    source_open_total = _to_int(summary.get("caixas_abertos_fonte"))
    stale_open_total = _to_int(summary.get("caixas_stale"))
    if source_open_total == 0:
        source_status = "ok"
        summary_text = "Nenhum caixa permanece aberto na fonte operacional atual."
    elif critical_count > 0:
        source_status = "ok"
        summary_text = f"{critical_count} caixa(s) aberto(s) há mais de 24 horas exigem ação imediata."
    elif high_count > 0:
        source_status = "ok"
        summary_text = f"{high_count} caixa(s) aberto(s) já ultrapassaram a janela segura de operação."
    elif warn_count > 0:
        source_status = "ok"
        summary_text = f"{warn_count} caixa(s) aberto(s) merecem monitoramento antes do fim do dia."
    else:
        source_status = "ok"
        summary_text = f"{operational_open_total} caixa(s) permanecem abertos na leitura operacional recente."
    snapshot_ts = _iso_from_clickhouse_epoch(summary.get("snapshot_epoch"), id_empresa) if source_open_total > 0 else None
    latest_activity_ts = _iso_from_clickhouse_epoch(summary.get("latest_activity_epoch"), id_empresa) if source_open_total > 0 else None
    alerts = [
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
    cancelamentos = [
        {
            "id_filial": row.get("id_filial"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
            "turno_label": row.get("turno_label"),
            "usuario_label": row.get("usuario_label"),
            "total_cancelamentos": _to_float(row.get("total_cancelamentos")),
            "qtd_cancelamentos": _to_int(row.get("qtd_cancelamentos")),
        }
        for row in open_rows
        if _to_float(row.get("total_cancelamentos")) > 0
    ]
    return {
        "source_status": source_status,
        "summary": summary_text,
        "kpis": {
            "total_turnos": source_open_total,
            "caixas_abertos_fonte": source_open_total,
            "caixas_abertos": operational_open_total,
            "caixas_stale": stale_open_total,
            "caixas_criticos": critical_count,
            "caixas_alto_risco": high_count,
            "caixas_em_monitoramento": warn_count,
            "total_vendas_abertas": total_vendas,
            "total_cancelamentos_abertos": total_cancelamentos,
            "total_devolucoes_abertas": 0.0,
            "caixa_liquido_aberto": caixa_liquido,
            "snapshot_ts": snapshot_ts,
            "latest_activity_ts": latest_activity_ts,
            "stale_window_hours": CASH_STALE_WINDOW_HOURS,
            "schema_mode": "clickhouse_mart",
        },
        "operational_sync": {"last_sync_at": latest_activity_ts or snapshot_ts, "snapshot_generated_at": snapshot_ts, "source": "torqmind_mart.agg_caixa_turno_aberto"},
        "freshness": {"mode": "live_monitor", "live_through_at": latest_activity_ts or snapshot_ts, "snapshot_generated_at": snapshot_ts, "source": "torqmind_mart.agg_caixa_turno_aberto"},
        "open_boxes": open_rows,
        "stale_boxes": stale_rows,
        "payment_mix": payment_mix_rows,
        "cancelamentos": cancelamentos[:10],
        "alerts": alerts,
    }


def open_cash_monitor(role: str, id_empresa: int, id_filial: Any) -> Dict[str, Any]:
    cash = _cash_live_now(role, id_empresa, id_filial)
    kpis = cash.get("kpis") or {}
    severity = "OK"
    if _to_int(kpis.get("caixas_criticos")) > 0:
        severity = "CRITICAL"
    elif _to_int(kpis.get("caixas_alto_risco")) > 0:
        severity = "HIGH"
    elif _to_int(kpis.get("caixas_em_monitoramento")) > 0:
        severity = "WARN"
    return {**cash, "severity": severity}


def cash_commercial_overview(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    sales = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    fraud = fraud_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    payments = payments_overview_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    total_vendas = _to_float(sales.get("faturamento"))
    qtd_vendas = _to_int(sales.get("itens"))
    total_cancelamentos = _to_float(fraud.get("valor_cancelado"))
    qtd_cancelamentos = _to_int(fraud.get("cancelamentos"))
    total_pagamentos = _to_float(payments.get("total_valor"))
    return {
        "source_status": "ok" if total_vendas or total_pagamentos or total_cancelamentos else "unavailable",
        "summary": f"Leitura ClickHouse consolidada de caixa entre {dt_ini.isoformat()} e {dt_fim.isoformat()}.",
        "requested_window": {"dt_ini": dt_ini, "dt_fim": dt_fim},
        "coverage": {"min_data_key": _date_key(dt_ini), "max_data_key": _date_key(dt_fim)},
        "kpis": {
            "caixas_periodo": 0,
            "dias_com_movimento": len(dashboard_series(role, id_empresa, id_filial, dt_ini, dt_fim)),
            "ticket_medio": round(total_vendas / qtd_vendas, 2) if qtd_vendas else 0.0,
            "total_vendas": total_vendas,
            "total_pagamentos": total_pagamentos,
            "total_cancelamentos": total_cancelamentos,
            "qtd_cancelamentos": qtd_cancelamentos,
            "caixas_com_cancelamento": 0,
            "total_devolucoes": 0.0,
            "qtd_devolucoes": 0,
            "caixas_com_devolucao": 0,
            "caixa_liquido": round(total_vendas - total_cancelamentos, 2),
            "saldo_comercial": round(total_vendas - total_cancelamentos, 2),
        },
        "by_day": dashboard_series(role, id_empresa, id_filial, dt_ini, dt_fim),
        "payment_mix": payments.get("mix") or [],
        "top_turnos": [],
        "cancelamentos": [],
    }


def cash_overview(role: str, id_empresa: int, id_filial: Any, dt_ini: Optional[date] = None, dt_fim: Optional[date] = None) -> Dict[str, Any]:
    effective_dt_fim = dt_fim or business_today(id_empresa)
    effective_dt_ini = dt_ini or (effective_dt_fim - timedelta(days=29))
    commercial_coverage = commercial_window_coverage(role, id_empresa, id_filial, effective_dt_ini, effective_dt_fim)
    historical_dt_ini = effective_dt_ini
    historical_dt_fim = effective_dt_fim
    historical = cash_commercial_overview(role, id_empresa, id_filial, historical_dt_ini, historical_dt_fim)
    commercial = dict(historical)
    commercial["commercial_coverage"] = commercial_coverage
    live_now = _cash_live_now(role, id_empresa, id_filial)
    dre_summary = cash_dre_summary(role, id_empresa, id_filial, historical_dt_fim)
    return {
        "source_status": historical.get("source_status"),
        "summary": historical.get("summary"),
        "kpis": historical.get("kpis"),
        "commercial": commercial,
        "dre_summary": dre_summary,
        "definitions": cash_definitions(),
        "operational_sync": live_now.get("operational_sync"),
        "freshness": {
            "mode": "requested_window",
            "historical_through_dt": historical_dt_fim.isoformat(),
            "live_through_at": (live_now.get("operational_sync") or {}).get("last_sync_at"),
            "source": "torqmind_mart.cash",
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


def health_score_latest(role: str, id_empresa: int, id_filial: Any, as_of: Optional[date] = None) -> Dict[str, Any]:
    requested_as_of = as_of or business_today(id_empresa)
    snapshot_meta = _snapshot_meta("health_score_daily", id_empresa, id_filial, requested_as_of, "latest_leq_ref")
    effective_as_of = snapshot_meta.get("effective_dt_ref") or requested_as_of
    branch = _branch_clause("id_filial", id_filial)
    row = _first(
        f"""
        SELECT
          dt_ref,
          avg(final_score) AS score_total,
          avg(health_pct) AS comp_operacao,
          avg(risk_pct) AS comp_fraude,
          avg(customer_pct) AS comp_churn,
          avg(health_pct) AS comp_margem,
          avg(health_pct) AS comp_finance,
          avg(health_pct) AS comp_dados
        FROM torqmind_mart.health_score_daily
        WHERE id_empresa = {{id_empresa:Int32}}
          AND dt_ref = {{dt_ref:Date}}
          {branch}
        GROUP BY dt_ref
        """,
        {"id_empresa": int(id_empresa), "dt_ref": effective_as_of},
        id_empresa,
    )
    payload = {
        "dt_ref": row.get("dt_ref") or requested_as_of,
        "score_total": _to_float(row.get("score_total")),
        "components": {
            "margem": _to_float(row.get("comp_margem")),
            "fraude": _to_float(row.get("comp_fraude")),
            "churn": _to_float(row.get("comp_churn")),
            "finance": _to_float(row.get("comp_finance")),
            "operacao": _to_float(row.get("comp_operacao")),
            "dados": _to_float(row.get("comp_dados")),
        },
        "reasons": {"source": "torqmind_mart.health_score_daily"},
    }
    payload.update(snapshot_meta)
    payload["snapshot_status"] = "exact" if payload.get("dt_ref") == requested_as_of else ("best_effort" if payload.get("dt_ref") else "missing")
    payload["source_kind"] = "snapshot" if payload.get("dt_ref") else "missing"
    return payload


def leaderboard_employees(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    if dt_fim < dt_ini:
        return []
    return sales_top_employees(role, id_empresa, id_filial, dt_ini, dt_fim, limit=limit)


def sales_peak_hours_signal(role: str, id_empresa: int, id_filial: Any, dt_ref: date) -> Dict[str, Any]:
    effective_ref = commercial_window_coverage(role, id_empresa, id_filial, dt_ref, dt_ref).get("effective_dt_fim") or dt_ref
    closed_end = effective_ref - timedelta(days=1)
    closed_start = closed_end - timedelta(days=29)
    if closed_end < closed_start:
        return {"source_status": "unavailable", "window_days": 0, "dt_ini": None, "dt_fim": None, "peak_hours": [], "off_peak_hours": [], "recommendations": {"peak": None, "off_peak": None}}
    rows = sales_by_hour(role, id_empresa, id_filial, closed_start, closed_end)
    by_hour: Dict[int, Dict[str, Any]] = {hour: {"hora": hour, "faturamento": 0.0, "vendas": 0} for hour in range(24)}
    for row in rows:
        item = by_hour[_to_int(row.get("hora"))]
        item["faturamento"] += _to_float(row.get("faturamento"))
        item["vendas"] += _to_int(row.get("vendas"))
    closed_days = max((closed_end - closed_start).days + 1, 1)
    normalized = [
        {
            "hora": hour,
            "label": f"{hour:02d}h",
            "avg_faturamento_dia": round(item["faturamento"] / closed_days, 2),
            "avg_vendas_dia": round(item["vendas"] / closed_days, 2),
        }
        for hour, item in by_hour.items()
    ]
    baseline = sum(item["avg_faturamento_dia"] for item in normalized) / len(normalized)
    for item in normalized:
        item["relative_index"] = round(item["avg_faturamento_dia"] / baseline, 2) if baseline > 0 else 0.0
    active = [item for item in normalized if item["avg_faturamento_dia"] > 0]
    peak = sorted(active, key=lambda item: (item["avg_faturamento_dia"], item["avg_vendas_dia"]), reverse=True)[:3]
    excluded = {item["hora"] for item in peak}
    off_peak = sorted([item for item in normalized if item["hora"] not in excluded], key=lambda item: (item["avg_faturamento_dia"], item["avg_vendas_dia"], item["hora"]))[:3]
    return {
        "source_status": "ok" if peak or off_peak else "unavailable",
        "window_days": closed_days,
        "dt_ini": closed_start.isoformat(),
        "dt_fim": closed_end.isoformat(),
        "peak_hours": peak,
        "off_peak_hours": off_peak,
        "recommendations": {"peak": "Ajuste escala, atenção de pista e conferência operacional nas horas de maior média recente.", "off_peak": "Use as horas de menor fluxo para reposição, rotina operacional e ofertas leves que não distorçam margem."},
    }


def sales_declining_products_signal(role: str, id_empresa: int, id_filial: Any, dt_ref: date, *, limit: int = 3) -> Dict[str, Any]:
    effective_ref = commercial_window_coverage(role, id_empresa, id_filial, dt_ref, dt_ref).get("effective_dt_fim") or dt_ref
    recent_end = effective_ref - timedelta(days=1)
    recent_start = recent_end - timedelta(days=29)
    prior_end = recent_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=29)
    if prior_end < prior_start:
        return {"source_status": "unavailable", "recent_window": {"dt_ini": None, "dt_fim": None}, "prior_window": {"dt_ini": None, "dt_fim": None}, "thresholds": {"min_prior_revenue": 1000.0, "min_absolute_drop": 300.0, "min_decline_pct": -15.0}, "items": []}
    branch = _branch_clause("id_filial", id_filial)
    rows = _run(
        f"""
        SELECT
          id_produto,
          max(produto_nome) AS produto_nome,
          '' AS grupo_nome,
          sumIf(faturamento, data_key BETWEEN {{recent_ini:Int32}} AND {{recent_fim:Int32}}) AS recent_faturamento,
          sumIf(qtd, data_key BETWEEN {{recent_ini:Int32}} AND {{recent_fim:Int32}}) AS recent_qtd,
          sumIf(faturamento, data_key BETWEEN {{prior_ini:Int32}} AND {{prior_fim:Int32}}) AS prior_faturamento,
          sumIf(qtd, data_key BETWEEN {{prior_ini:Int32}} AND {{prior_fim:Int32}}) AS prior_qtd
        FROM torqmind_mart.agg_produtos_diaria
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{prior_ini:Int32}} AND {{recent_fim:Int32}}
          {branch}
        GROUP BY id_produto
        HAVING prior_faturamento >= 1000 AND (prior_faturamento - recent_faturamento) >= 300 AND recent_faturamento <= (prior_faturamento * 0.85)
        ORDER BY (prior_faturamento - recent_faturamento) DESC, prior_faturamento DESC, produto_nome
        LIMIT {{limit:UInt32}}
        """,
        {
            "id_empresa": int(id_empresa),
            "recent_ini": _date_key(recent_start),
            "recent_fim": _date_key(recent_end),
            "prior_ini": _date_key(prior_start),
            "prior_fim": _date_key(prior_end),
            "limit": int(limit),
        },
        id_empresa,
    )
    items = []
    for row in rows:
        prior = _to_float(row.get("prior_faturamento"))
        recent = _to_float(row.get("recent_faturamento"))
        delta = round(prior - recent, 2)
        variation = round(((recent / prior) - 1) * 100, 2) if prior > 0 else 0.0
        items.append(
            {
                "id_produto": row.get("id_produto"),
                "produto_nome": row.get("produto_nome"),
                "grupo_nome": row.get("grupo_nome") or "(Sem grupo)",
                "recent_faturamento": recent,
                "prior_faturamento": prior,
                "recent_qtd": _to_float(row.get("recent_qtd"), 3),
                "prior_qtd": _to_float(row.get("prior_qtd"), 3),
                "delta_faturamento": delta,
                "variation_pct": variation,
                "recommendation": "Revise ruptura, exposição, mix e disciplina comercial do produto antes de perder recorrência.",
            }
        )
    return {
        "source_status": "ok" if items else "unavailable",
        "recent_window": {"dt_ini": recent_start.isoformat(), "dt_fim": recent_end.isoformat()},
        "prior_window": {"dt_ini": prior_start.isoformat(), "dt_fim": prior_end.isoformat()},
        "thresholds": {"min_prior_revenue": 1000.0, "min_absolute_drop": 300.0, "min_decline_pct": -15.0},
        "items": items,
    }


def jarvis_briefing(role: str, id_empresa: int, id_filial: Any, dt_ref: date, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    dt_ini = dt_ref - timedelta(days=6)
    risk = context.get("modeled_risk") if context else risk_kpis(role, id_empresa, id_filial, dt_ini, dt_ref)
    cash_live = context.get("cash_live") if context else _cash_live_now(role, id_empresa, id_filial)
    finance = context.get("finance_aging") if context else finance_aging_overview(role, id_empresa, id_filial, as_of=dt_ref)
    churn_bundle = context.get("churn") if context else customers_churn_bundle(role, id_empresa, id_filial, as_of=dt_ref, min_score=40, limit=5)
    payments = context.get("payments") if context else payments_overview(role, id_empresa, id_filial, dt_ini, dt_ref, anomaly_limit=5)
    sales = context.get("sales") if context else sales_overview_bundle(role, id_empresa, id_filial, dt_ini, dt_ref, as_of=dt_ref)
    signals = context.get("signals") if isinstance(context, dict) and isinstance(context.get("signals"), dict) else {
        "peak_hours": sales_peak_hours_signal(role, id_empresa, id_filial, dt_ref),
        "declining_products": sales_declining_products_signal(role, id_empresa, id_filial, dt_ref),
    }
    exposure = (
        _to_float((risk or {}).get("impacto_total"))
        + _to_float((finance or {}).get("receber_total_vencido"))
        + _to_float((finance or {}).get("pagar_total_vencido"))
        + sum(_to_float(item.get("revenue_at_risk_30d")) for item in ((churn_bundle or {}).get("top_risk") or [])[:5])
        + _to_float(((payments or {}).get("kpis") or {}).get("unknown_valor"))
        + _to_float(((cash_live or {}).get("kpis") or {}).get("total_vendas_abertas"))
    )
    status = "critical" if exposure > 5000 else ("warn" if exposure > 0 else "ok")
    return {
        "title": "Copiloto operacional",
        "data_ref": dt_ref.isoformat(),
        "status": status,
        "headline": "Priorize as frentes com maior valor em jogo no período atual." if exposure > 0 else "Operação estável no período atual, sem foco crítico acima da linha de corte.",
        "summary": "Leitura consolidada em ClickHouse a partir das Smart Marts analíticas.",
        "priority": "Hoje" if exposure > 0 else "Acompanhar",
        "impact_value": round(exposure, 2),
        "impact_label": f"{_format_brl(exposure)} em jogo" if exposure > 0 else "Sem exposição crítica material",
        "problem": "Exposição operacional consolidada acima de zero." if exposure > 0 else "Sem frente crítica acima da linha de corte.",
        "cause": "Fraude, caixa, clientes, financeiro e pagamentos foram consolidados nas marts ClickHouse.",
        "action": "Abrir o módulo com maior exposição e validar a ação com a operação local.",
        "confidence_label": "Alta",
        "confidence_level": "high",
        "confidence_reason": "Resposta calculada diretamente nas Smart Marts ClickHouse.",
        "data_freshness": {"sales": sales.get("freshness"), "cash": cash_live.get("freshness")},
        "primary_kind": None,
        "primary_shortcut": None,
        "evidence": ["Smart Marts ClickHouse ativas", f"Exposição consolidada: {_format_brl(exposure)}"],
        "secondary_focus": [],
        "signals": signals,
        "highlights": ["Acompanhe os módulos com maior valor em jogo.", "Mantenha a rotina diária de validação operacional."],
    }


def dashboard_home_bundle(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, dt_ref: date) -> Dict[str, Any]:
    sales_coverage = commercial_window_coverage(role, id_empresa, id_filial, dt_ini, dt_fim)
    sales_dt_ini = sales_coverage.get("effective_dt_ini") or dt_ini
    sales_dt_fim = sales_coverage.get("effective_dt_fim") or dt_fim
    sales = sales_overview_bundle(role, id_empresa, id_filial, sales_dt_ini, sales_dt_fim, as_of=dt_ref, include_details=False)
    sales["commercial_coverage"] = sales_coverage
    peak_hours_signal = sales_peak_hours_signal(role, id_empresa, id_filial, sales_dt_fim)
    declining_products_signal = sales_declining_products_signal(role, id_empresa, id_filial, sales_dt_fim)
    fraud_operational = {"kpis": fraud_kpis(role, id_empresa, id_filial, dt_ini, dt_fim), "window": fraud_data_window(role, id_empresa, id_filial)}
    modeled_risk = {"source_status": "ok", "message": None, "kpis": risk_kpis(role, id_empresa, id_filial, dt_ini, dt_fim), "window": risk_data_window(role, id_empresa, id_filial)}
    churn = customers_churn_bundle(role, id_empresa, id_filial, as_of=dt_ref, min_score=40, limit=10)
    finance_aging = finance_aging_overview(role, id_empresa, id_filial, as_of=dt_ref)
    cash_live = _cash_live_now(role, id_empresa, id_filial)
    payments = payments_overview(role, id_empresa, id_filial, sales_dt_ini, sales_dt_fim, anomaly_limit=5)
    from app import repos_mart as _postgres_repos

    notifications_unread = _postgres_repos.notifications_unread_count(role, id_empresa, id_filial)
    sales_sync = sales.get("operational_sync") or {}
    cash_sync = cash_live.get("operational_sync") or {}
    operational_sync = sales_sync if sales_sync.get("last_sync_at") else cash_sync or sales_sync
    live_through_at = sales_sync.get("last_sync_at") or cash_sync.get("last_sync_at")
    freshness = {
        "mode": "hybrid_operational_home",
        "sales": sales.get("freshness"),
        "cash": cash_live.get("freshness"),
        "live_through_at": live_through_at,
        "snapshot_generated_at": sales_sync.get("snapshot_generated_at") or cash_sync.get("snapshot_generated_at"),
        "source": "torqmind_mart",
    }
    branch_id = _conn_branch_id(id_filial)
    context = {
        "fraud_operational": fraud_operational.get("kpis"),
        "modeled_risk": modeled_risk.get("kpis"),
        "cash_live": cash_live,
        "finance_aging": finance_aging,
        "churn": churn,
        "payments": payments,
        "sales": sales,
        "signals": {"peak_hours": peak_hours_signal, "declining_products": declining_products_signal},
    }
    return {
        "scope": {"id_empresa": id_empresa, "id_filial": branch_id, "id_filiais": _branch_ids(id_filial) or [], "filial_label": _filial_label(id_filial), "dt_ini": dt_ini, "dt_fim": dt_fim, "requested_dt_ref": dt_ref},
        "overview": {
            "sales": sales,
            "insights_generated": [],
            "fraud": {"operational": fraud_operational, "modeled_risk": modeled_risk},
            "risk": modeled_risk,
            "cash": {"live_now": cash_live},
            "jarvis": jarvis_briefing(role, id_empresa, id_filial, dt_ref=dt_ref, context=context),
        },
        "churn": churn,
        "finance": {"aging": finance_aging},
        "cash": {"live_now": cash_live, "operational_sync": cash_live.get("operational_sync"), "freshness": cash_live.get("freshness")},
        "notifications_unread": notifications_unread,
        "operational_sync": operational_sync,
        "freshness": freshness,
        "commercial_coverage": sales_coverage,
        "health_score": health_score_latest(role, id_empresa, id_filial, as_of=dt_ref),
    }
