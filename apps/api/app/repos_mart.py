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

from app.db import get_conn


LOCAL_VENDA_LABELS = {
    -1: "Canal não identificado",
    1: "Pista",
    2: "Loja de conveniência",
    3: "Serviços",
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
    return f"Filial #{id_filial}"


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


def _fuel_filter_expression(group_alias: str, product_alias: str) -> str:
    product_name = _normalized_text_expression(f"{product_alias}.nome")
    group_name = _normalized_text_expression(f"{group_alias}.nome")
    unit_name = _normalized_text_expression(f"{product_alias}.unidade")
    return f"""
      (
        CASE
          WHEN (
            {product_name} LIKE '%%GASOL%%'
            OR {product_name} LIKE '%%ETANOL%%'
            OR {product_name} LIKE '%%ALCOOL%%'
            OR {product_name} LIKE '%%DIESEL S10%%'
            OR {product_name} LIKE '%%S-10%%'
            OR {product_name} LIKE '%%BS10%%'
            OR {product_name} LIKE '%%DIESEL S500%%'
            OR {product_name} LIKE '%%S-500%%'
            OR {product_name} LIKE '%%BS500%%'
            OR {product_name} LIKE '%%GNV%%'
          ) THEN true
          WHEN (
            {group_name} LIKE '%%COMBUST%%'
            OR {group_name} LIKE '%%GASOL%%'
            OR {group_name} LIKE '%%ETANOL%%'
            OR {group_name} LIKE '%%DIESEL%%'
          )
          AND ({unit_name} IN ('LT', 'L', 'LITRO', 'LITROS', 'M3', 'MTS3') OR {unit_name} = '')
          THEN true
          ELSE false
        END
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


def _employee_label(funcionario_nome: Any, id_funcionario: Any = None) -> str:
    nome = str(funcionario_nome or "").strip()
    if nome and nome.lower() not in {"(sem funcionário)", "sem funcionário", "sem funcionario"}:
        return nome
    if id_funcionario is None or int(id_funcionario or -1) < 0:
        return "Equipe não identificada"
    return f"Funcionário #{id_funcionario}"


def _payment_category_label(category: Any, label: Any = None) -> str:
    category_value = str(category or "").strip().upper()
    label_value = str(label or "").strip()
    if label_value and label_value.upper() != "NÃO IDENTIFICADO":
        return label_value
    if category_value and category_value != "NAO_IDENTIFICADO":
        return category_value.replace("_", " ").title()
    return "NÃO IDENTIFICADO"


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _branch_ids(id_filial: Any) -> Optional[List[int]]:
    if id_filial is None:
        return None
    if isinstance(id_filial, (list, tuple, set)):
        values = sorted({int(value) for value in id_filial if value is not None})
        return values or None
    return [int(id_filial)]


def _conn_branch_id(id_filial: Any) -> Optional[int]:
    branch_ids = _branch_ids(id_filial)
    if not branch_ids or len(branch_ids) != 1:
        return None
    return int(branch_ids[0])


def _branch_scope_clause(column: str, id_filial: Any) -> tuple[str, list[Any]]:
    branch_ids = _branch_ids(id_filial)
    if not branch_ids:
        return "", []
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
    insights_rows = risk_insights(role, id_empresa, id_filial, dt_ini, dt_fim, limit=20)
    sales = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    fraud_operational = {
        "kpis": fraud_kpis(role, id_empresa, id_filial, dt_ini, dt_fim),
        "window": fraud_data_window(role, id_empresa, id_filial),
    }
    modeled_risk = {
        "kpis": risk_kpis(role, id_empresa, id_filial, dt_ini, dt_fim),
        "window": risk_data_window(role, id_empresa, id_filial),
    }
    churn = customers_churn_bundle(role, id_empresa, id_filial, as_of=dt_ref, min_score=40, limit=10)
    finance_aging = finance_aging_overview(role, id_empresa, id_filial, as_of=dt_ref)
    cash = cash_overview(role, id_empresa, id_filial, dt_ini=dt_ini, dt_fim=dt_fim)
    payments = payments_overview(role, id_empresa, id_filial, dt_ini, dt_fim, anomaly_limit=5)
    notifications_unread = notifications_unread_count(role, id_empresa, id_filial)

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
                "historical": cash.get("historical"),
                "live_now": cash.get("live_now"),
            },
            "jarvis": jarvis_briefing(
                role,
                id_empresa,
                id_filial,
                dt_ref=dt_ref,
                context={
                    "fraud_operational": fraud_operational.get("kpis"),
                    "modeled_risk": modeled_risk.get("kpis"),
                    "cash_live": cash.get("live_now"),
                    "cash_historical": cash.get("historical"),
                    "finance_aging": finance_aging,
                    "churn": churn,
                    "payments": payments,
                },
            ),
        },
        "churn": churn,
        "finance": {
            "aging": finance_aging,
        },
        "cash": cash,
        "notifications_unread": notifications_unread,
    }

def dashboard_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params

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
        row = conn.execute(sql, params).fetchone()
        return row or {"faturamento": 0, "margem": 0, "ticket_medio": 0, "itens": 0}


def dashboard_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params
    sql = f"""
      SELECT data_key, id_filial, faturamento, margem
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


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
        return list(conn.execute(sql, params).fetchall())


def sales_top_products(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]
    sql = f"""
      SELECT
        id_produto,
        MAX(produto_nome) AS produto_nome,
        SUM(faturamento) AS faturamento,
        SUM(margem) AS margem,
        SUM(qtd) AS qtd
      FROM mart.agg_produtos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      GROUP BY id_produto
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


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
        return list(conn.execute(sql, params).fetchall())


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
        return list(conn.execute(sql, params).fetchall())


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
          CASE
            WHEN {_normalized_text_expression("p.nome")} LIKE '%%GASOL%%' THEN 'GASOLINA'
            WHEN {_normalized_text_expression("p.nome")} LIKE '%%ETANOL%%'
              OR {_normalized_text_expression("p.nome")} LIKE '%%ALCOOL%%' THEN 'ETANOL'
            WHEN {_normalized_text_expression("p.nome")} LIKE '%%S10%%'
              OR {_normalized_text_expression("p.nome")} LIKE '%%BS10%%' THEN 'DIESEL S10'
            WHEN {_normalized_text_expression("p.nome")} LIKE '%%S500%%'
              OR {_normalized_text_expression("p.nome")} LIKE '%%BS500%%'
              OR {_normalized_text_expression("p.nome")} LIKE '%%DIESEL%%' THEN 'DIESEL S500'
            WHEN {_normalized_text_expression("p.nome")} LIKE '%%GNV%%' THEN 'GNV'
            ELSE 'COMBUSTÍVEL'
          END AS familia_combustivel,
          COALESCE(p.custo_medio, 0)::numeric(18,4) AS custo_medio
        FROM dw.dim_produto p
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = p.id_empresa
         AND g.id_filial = p.id_filial
         AND g.id_grupo_produto = p.id_grupo_produto
        WHERE p.id_empresa = %s
          AND p.id_filial = %s
          AND {fuel_filter}
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
                CASE
                  WHEN {_normalized_text_expression("p.nome")} LIKE '%%GASOL%%' THEN 'GASOLINA'
                  WHEN {_normalized_text_expression("p.nome")} LIKE '%%ETANOL%%'
                    OR {_normalized_text_expression("p.nome")} LIKE '%%ALCOOL%%' THEN 'ETANOL'
                  WHEN {_normalized_text_expression("p.nome")} LIKE '%%S10%%'
                    OR {_normalized_text_expression("p.nome")} LIKE '%%BS10%%' THEN 'DIESEL S10'
                  WHEN {_normalized_text_expression("p.nome")} LIKE '%%S500%%'
                    OR {_normalized_text_expression("p.nome")} LIKE '%%BS500%%'
                    OR {_normalized_text_expression("p.nome")} LIKE '%%DIESEL%%' THEN 'DIESEL S500'
                  WHEN {_normalized_text_expression("p.nome")} LIKE '%%GNV%%' THEN 'GNV'
                  ELSE 'COMBUSTÍVEL'
                END AS familia_combustivel,
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


def fraud_last_events(role: str, id_empresa: int, id_filial: Optional[int], limit: int = 30) -> List[Dict[str, Any]]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa] + branch_params + [limit]

    sql = f"""
      SELECT id_filial, id_db, id_comprovante, data, id_usuario, id_turno, valor_total
      FROM mart.fraude_cancelamentos_eventos
      WHERE id_empresa = %s
      {where_filial}
      ORDER BY data DESC NULLS LAST
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


def fraud_top_users(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        id_usuario,
        COUNT(*)::int AS cancelamentos,
        COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_cancelado
      FROM dw.fact_comprovante
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        AND cancelado = true
        {where_filial}
      GROUP BY id_usuario
      ORDER BY cancelamentos DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, params).fetchall())


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


def risk_last_events(role: str, id_empresa: int, id_filial: Optional[int], limit: int = 30) -> List[Dict[str, Any]]:
    where_filial, branch_params = _branch_scope_clause("e.id_filial", id_filial)
    params = [id_empresa] + branch_params + [limit]

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
        e.id_turno,
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
      WHERE e.id_empresa = %s
      {where_filial}
      ORDER BY e.data DESC NULLS LAST, e.id DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, [id_empresa] + params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["event_label"] = _event_type_label(row.get("event_type"))
        row["funcionario_label"] = _employee_label(row.get("funcionario_nome"), row.get("id_funcionario"))
        row["reasons_humanized"] = _humanize_risk_reasons(row.get("reasons"), row.get("event_type"))
        row["reason_summary"] = " ".join(row["reasons_humanized"])
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
      WHERE rtl.id_empresa = %s
        AND rtl.data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY rtl.id_filial, f.nome, rtl.id_turno, rtl.id_local_venda
      ORDER BY impacto_estimado DESC, score_medio DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["local_label"] = _local_venda_label(row.get("id_local_venda"), row.get("local_nome"))
        row["turno_label"] = f"Turno {row['id_turno']}" if row.get("id_turno") is not None and int(row.get("id_turno")) >= 0 else "Turno não informado"
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
       AND i.id_movprodutos = v.id_movprodutos
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
    min_score: int,
    limit: int,
    id_cliente: Optional[int] = None,
) -> List[Dict[str, Any]]:
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    where_customer = "" if id_cliente is None else "AND id_cliente = %s"
    params = [id_empresa, min_score] + branch_params + ([] if id_cliente is None else [id_cliente]) + [limit]
    sql = f"""
      SELECT
        COALESCE((reasons->>'ref_date')::date, CURRENT_DATE) AS dt_ref,
        id_cliente,
        COALESCE(NULLIF(cliente_nome, ''), '#ID ' || id_cliente::text) AS cliente_nome,
        last_purchase,
        GREATEST(0, COALESCE((reasons->>'ref_date')::date, CURRENT_DATE) - last_purchase)::int AS recency_days,
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
        rows = _customers_churn_operational_current(role, id_empresa, id_filial, min_score=min_score, limit=limit)
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

    fallback_rows = _customers_churn_operational_current(
        role,
        id_empresa,
        id_filial,
        min_score=0,
        limit=1,
    )
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
        ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
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
    requested_as_of = as_of or date.today()
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
        COALESCE(SUM(total_valor),0)::numeric(18,2) AS total_valor
      FROM mart.agg_pagamentos_diaria
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      GROUP BY category
      ORDER BY total_valor DESC
      LIMIT 6
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


def payments_by_turno(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params
    sql = f"""
      SELECT
        data_key,
        id_filial,
        id_turno,
        category,
        label,
        total_valor,
        qtd_comprovantes
      FROM mart.agg_pagamentos_turno
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial}
      ORDER BY data_key DESC, total_valor DESC
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"))
        row["category_label"] = _payment_category_label(row.get("category"), row.get("label"))
        row["turno_label"] = f"Turno {row['id_turno']}" if row.get("id_turno") is not None and int(row.get("id_turno")) >= 0 else "Operação sem turno identificado"
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
        row["turno_label"] = f"Turno {row['id_turno']}" if row.get("id_turno") is not None and int(row.get("id_turno")) >= 0 else "Operação sem turno identificado"
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


def _cash_live_now(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial, branch_params = _branch_scope_clause("t.id_filial", id_filial)
    params = [id_empresa] + branch_params
    sql_total_turnos = f"""
      SELECT COUNT(*)::int AS total_turnos
      FROM dw.fact_caixa_turno t
      WHERE t.id_empresa = %s
      {where_filial}
    """
    sql_open = f"""
      WITH open_turnos AS (
        SELECT
          t.id_empresa,
          t.id_filial,
          t.id_turno,
          t.id_usuario,
          t.abertura_ts,
          ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2)::numeric(10,2) AS horas_aberto
        FROM dw.fact_caixa_turno t
        WHERE t.id_empresa = %s
          {where_filial}
          AND t.is_aberto = true
      ), comprovantes_caixa AS (
        SELECT
          c.id_empresa,
          c.id_filial,
          c.id_turno,
          COALESCE(SUM(c.valor_total) FILTER (WHERE c.cfop_num > 5000 AND NOT c.cancelado_bool), 0)::numeric(18,2) AS total_vendas,
          COUNT(*) FILTER (WHERE c.cfop_num > 5000 AND NOT c.cancelado_bool)::int AS qtd_vendas,
          COALESCE(SUM(c.valor_total) FILTER (WHERE c.cfop_num > 5000 AND c.cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(*) FILTER (WHERE c.cfop_num > 5000 AND c.cancelado_bool)::int AS qtd_cancelamentos
        FROM (
          SELECT
            fc.id_empresa,
            fc.id_filial,
            fc.id_turno,
            fc.valor_total,
            COALESCE(fc.cancelado, false) AS cancelado_bool,
            etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
          FROM dw.fact_comprovante fc
          JOIN open_turnos ot
            ON ot.id_empresa = fc.id_empresa
           AND ot.id_filial = fc.id_filial
           AND ot.id_turno = fc.id_turno
        ) c
        GROUP BY c.id_empresa, c.id_filial, c.id_turno
      ), pagamentos_turno AS (
        SELECT
          p.id_empresa,
          p.id_filial,
          p.id_turno,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        JOIN open_turnos ot
          ON ot.id_empresa = p.id_empresa
         AND ot.id_filial = p.id_filial
         AND ot.id_turno = p.id_turno
        GROUP BY p.id_empresa, p.id_filial, p.id_turno
      ), operador_turno AS (
        SELECT
          ranked.id_empresa,
          ranked.id_filial,
          ranked.id_turno,
          ranked.id_funcionario,
          COALESCE(NULLIF(df.nome, ''), '') AS funcionario_nome
        FROM (
          SELECT
            v.id_empresa,
            v.id_filial,
            v.id_turno,
            i.id_funcionario,
            COUNT(*)::int AS item_count,
            COALESCE(SUM(i.total), 0)::numeric(18,2) AS total_movimento,
            MAX(v.data) AS last_sale_at,
            ROW_NUMBER() OVER (
              PARTITION BY v.id_empresa, v.id_filial, v.id_turno
              ORDER BY
                COUNT(*) DESC,
                COALESCE(SUM(i.total), 0) DESC,
                MAX(v.data) DESC,
                MAX(i.id_funcionario) DESC
            ) AS rn
          FROM dw.fact_venda v
          JOIN open_turnos ot
            ON ot.id_empresa = v.id_empresa
           AND ot.id_filial = v.id_filial
           AND ot.id_turno = v.id_turno
          JOIN dw.fact_venda_item i
            ON i.id_empresa = v.id_empresa
           AND i.id_filial = v.id_filial
           AND i.id_db = v.id_db
           AND i.id_movprodutos = v.id_movprodutos
          WHERE v.id_turno IS NOT NULL
            AND i.id_funcionario IS NOT NULL
          GROUP BY v.id_empresa, v.id_filial, v.id_turno, i.id_funcionario
        ) ranked
        LEFT JOIN dw.dim_funcionario df
          ON df.id_empresa = ranked.id_empresa
         AND df.id_filial = ranked.id_filial
         AND df.id_funcionario = ranked.id_funcionario
        WHERE ranked.rn = 1
      )
      SELECT
        ot.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        ot.id_turno,
        ot.id_usuario,
        operador.id_funcionario,
        COALESCE(NULLIF(u.nome, ''), NULLIF(operador.funcionario_nome, ''), format('Usuário %%s', ot.id_usuario)) AS usuario_nome,
        ot.abertura_ts,
        ot.horas_aberto,
        CASE
          WHEN ot.horas_aberto >= 24 THEN 'CRITICAL'
          WHEN ot.horas_aberto >= 12 THEN 'HIGH'
          WHEN ot.horas_aberto >= 6 THEN 'WARN'
          ELSE 'OK'
        END AS severity,
        CASE
          WHEN ot.horas_aberto >= 24 THEN 'Crítico'
          WHEN ot.horas_aberto >= 12 THEN 'Atenção alta'
          WHEN ot.horas_aberto >= 6 THEN 'Monitorar'
          ELSE 'Dentro da janela'
        END AS status_label,
        COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
        COALESCE(c.qtd_vendas, 0)::int AS qtd_vendas,
        COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
        COALESCE(c.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
        COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos
      FROM open_turnos ot
      LEFT JOIN auth.filiais f
        ON f.id_empresa = ot.id_empresa
       AND f.id_filial = ot.id_filial
      LEFT JOIN dw.dim_usuario_caixa u
        ON u.id_empresa = ot.id_empresa
       AND u.id_filial = ot.id_filial
       AND u.id_usuario = ot.id_usuario
      LEFT JOIN operador_turno operador
        ON operador.id_empresa = ot.id_empresa
       AND operador.id_filial = ot.id_filial
       AND operador.id_turno = ot.id_turno
      LEFT JOIN comprovantes_caixa c
        ON c.id_empresa = ot.id_empresa
       AND c.id_filial = ot.id_filial
       AND c.id_turno = ot.id_turno
      LEFT JOIN pagamentos_turno p
        ON p.id_empresa = ot.id_empresa
       AND p.id_filial = ot.id_filial
       AND p.id_turno = ot.id_turno
      ORDER BY ot.horas_aberto DESC, COALESCE(c.total_vendas, 0) DESC, ot.id_turno DESC
      LIMIT 20
    """
    sql_payments = f"""
      WITH open_turnos AS (
        SELECT id_empresa, id_filial, id_turno
        FROM dw.fact_caixa_turno t
        WHERE t.id_empresa = %s
          {where_filial}
          AND t.is_aberto = true
      )
      SELECT
        COALESCE(m.label, 'NÃO IDENTIFICADO') AS forma_label,
        COALESCE(m.category, 'NAO_IDENTIFICADO') AS forma_category,
        COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
        COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
        COUNT(DISTINCT p.id_filial::text || ':' || p.id_turno::text)::int AS qtd_turnos
      FROM dw.fact_pagamento_comprovante p
      JOIN open_turnos ot
        ON ot.id_empresa = p.id_empresa
       AND ot.id_filial = p.id_filial
       AND ot.id_turno = p.id_turno
      LEFT JOIN LATERAL (
        SELECT label, category
        FROM app.payment_type_map m
        WHERE m.tipo_forma = p.tipo_forma
          AND m.active = true
          AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
        ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
        LIMIT 1
      ) m ON true
      GROUP BY 1, 2
      ORDER BY total_valor DESC
      LIMIT 12
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        total_turnos_row = conn.execute(sql_total_turnos, params).fetchone() or {"total_turnos": 0}
        open_rows = [dict(row) for row in conn.execute(sql_open, params).fetchall()]
        payment_rows = [dict(row) for row in conn.execute(sql_payments, params).fetchall()]

    total_turnos = int(total_turnos_row.get("total_turnos") or 0)
    critical_count = 0
    high_count = 0
    warn_count = 0
    total_vendas = 0.0
    total_cancelamentos = 0.0

    for row in open_rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = str(row.get("usuario_nome") or "").strip() or (
            f"Operador {int(row.get('id_usuario'))}" if row.get("id_usuario") is not None else "Operador não identificado"
        )
        row["alert_message"] = (
            f"O caixa {row.get('id_turno')} da {row['filial_label']} está aberto há {row.get('horas_aberto') or 0} horas."
        )
        severity = str(row.get("severity") or "OK").upper()
        if severity == "CRITICAL":
            critical_count += 1
        elif severity == "HIGH":
            high_count += 1
        elif severity == "WARN":
            warn_count += 1
        total_vendas += float(row.get("total_vendas") or 0)
        total_cancelamentos += float(row.get("total_cancelamentos") or 0)

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
            "id_usuario": row.get("id_usuario"),
            "usuario_nome": row.get("usuario_nome"),
            "usuario_label": row.get("usuario_label"),
            "abertura_ts": row.get("abertura_ts"),
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
    elif not open_rows:
        source_status = "ok"
        summary = "Nenhum caixa aberto no momento. A operação está sem pendências de fechamento."
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
        summary = f"{len(open_rows)} caixa(s) aberto(s) dentro da janela operacional esperada."

    return {
        "source_status": source_status,
        "summary": summary,
        "kpis": {
            "total_turnos": total_turnos,
            "caixas_abertos": len(open_rows),
            "caixas_criticos": critical_count,
            "caixas_alto_risco": high_count,
            "caixas_em_monitoramento": warn_count,
            "total_vendas_abertas": round(total_vendas, 2),
            "total_cancelamentos_abertos": round(total_cancelamentos, 2),
        },
        "open_boxes": open_rows,
        "payment_mix": payment_mix[:8],
        "cancelamentos": cancelamentos[:10],
        "alerts": alert_rows,
    }


def _cash_historical_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial_comp, comp_branch_params = _branch_scope_clause("fc.id_filial", id_filial)
    where_filial_pay, pay_branch_params = _branch_scope_clause("p.id_filial", id_filial)
    where_filial_agg, agg_branch_params = _branch_scope_clause("id_filial", id_filial)
    params_comp = [id_empresa, ini, fim] + comp_branch_params
    params_pay = [id_empresa, ini, fim] + pay_branch_params

    sql_summary = f"""
      WITH comprovantes AS (
        SELECT
          fc.id_filial,
          fc.id_turno,
          fc.data_key,
          fc.valor_total,
          COALESCE(fc.cancelado, false) AS cancelado_bool,
          etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
        FROM dw.fact_comprovante fc
        WHERE fc.id_empresa = %s
          AND fc.data_key BETWEEN %s AND %s
          {where_filial_comp}
          AND fc.id_turno IS NOT NULL
      ), vendas AS (
        SELECT
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text))::int AS caixas_periodo,
          COUNT(DISTINCT data_key)::int AS dias_com_movimento,
          COALESCE(SUM(valor_total) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool), 0)::numeric(18,2) AS total_vendas,
          COUNT(*) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool)::int AS qtd_vendas,
          COALESCE(SUM(valor_total) FILTER (WHERE cfop_num > 5000 AND cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(*) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS qtd_cancelamentos,
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text)) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS caixas_com_cancelamento,
          MIN(data_key)::int AS min_data_key,
          MAX(data_key)::int AS max_data_key
        FROM comprovantes
      ), pagamentos AS (
        SELECT
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND p.id_turno IS NOT NULL
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
    sql_by_day = f"""
      WITH comprovantes AS (
        SELECT
          fc.data_key,
          fc.id_filial,
          fc.id_turno,
          fc.valor_total,
          COALESCE(fc.cancelado, false) AS cancelado_bool,
          etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
        FROM dw.fact_comprovante fc
        WHERE fc.id_empresa = %s
          AND fc.data_key BETWEEN %s AND %s
          {where_filial_comp}
          AND fc.id_turno IS NOT NULL
      ), vendas AS (
        SELECT
          data_key,
          COUNT(DISTINCT (id_filial::text || ':' || id_turno::text))::int AS caixas,
          COALESCE(SUM(valor_total) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool), 0)::numeric(18,2) AS total_vendas,
          COALESCE(SUM(valor_total) FILTER (WHERE cfop_num > 5000 AND cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(*) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS qtd_cancelamentos
        FROM comprovantes
        GROUP BY data_key
      ), pagamentos AS (
        SELECT
          p.data_key,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND p.id_turno IS NOT NULL
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
        COALESCE(label, 'NÃO IDENTIFICADO') AS label,
        COALESCE(category, 'NAO_IDENTIFICADO') AS category,
        COALESCE(SUM(total_valor), 0)::numeric(18,2) AS total_valor,
        COALESCE(SUM(qtd_comprovantes), 0)::int AS qtd_comprovantes,
        COUNT(DISTINCT (id_filial::text || ':' || id_turno::text))::int AS qtd_turnos
      FROM mart.agg_pagamentos_turno
      WHERE id_empresa = %s
        AND data_key BETWEEN %s AND %s
        {where_filial_agg}
      GROUP BY 1, 2
      ORDER BY total_valor DESC
      LIMIT 12
    """
    sql_top_turnos = f"""
      WITH comprovantes AS (
        SELECT
          c.id_filial,
          c.id_turno,
          MIN(c.data) AS first_event_at,
          MAX(c.data) AS last_event_at,
          COALESCE(SUM(c.valor_total) FILTER (WHERE c.cfop_num > 5000 AND NOT c.cancelado_bool), 0)::numeric(18,2) AS total_vendas,
          COUNT(*) FILTER (WHERE c.cfop_num > 5000 AND NOT c.cancelado_bool)::int AS qtd_vendas,
          COALESCE(SUM(c.valor_total) FILTER (WHERE c.cfop_num > 5000 AND c.cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
          COUNT(*) FILTER (WHERE c.cfop_num > 5000 AND c.cancelado_bool)::int AS qtd_cancelamentos
        FROM (
          SELECT
            fc.id_filial,
            fc.id_turno,
            fc.data,
            fc.valor_total,
            COALESCE(fc.cancelado, false) AS cancelado_bool,
            etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
          FROM dw.fact_comprovante fc
          WHERE fc.id_empresa = %s
            AND fc.data_key BETWEEN %s AND %s
            {where_filial_comp}
            AND fc.id_turno IS NOT NULL
        ) c
        GROUP BY c.id_filial, c.id_turno
      ), pagamentos AS (
        SELECT
          p.id_filial,
          p.id_turno,
          COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
        FROM dw.fact_pagamento_comprovante p
        WHERE p.id_empresa = %s
          AND p.data_key BETWEEN %s AND %s
          {where_filial_pay}
          AND p.id_turno IS NOT NULL
        GROUP BY p.id_filial, p.id_turno
      ), operador_turno AS (
        SELECT
          ranked.id_empresa,
          ranked.id_filial,
          ranked.id_turno,
          ranked.id_funcionario,
          COALESCE(NULLIF(df.nome, ''), '') AS funcionario_nome
        FROM (
          SELECT
            v.id_empresa,
            v.id_filial,
            v.id_turno,
            i.id_funcionario,
            COUNT(*)::int AS item_count,
            COALESCE(SUM(i.total), 0)::numeric(18,2) AS total_movimento,
            MAX(v.data) AS last_sale_at,
            ROW_NUMBER() OVER (
              PARTITION BY v.id_empresa, v.id_filial, v.id_turno
              ORDER BY
                COUNT(*) DESC,
                COALESCE(SUM(i.total), 0) DESC,
                MAX(v.data) DESC,
                MAX(i.id_funcionario) DESC
            ) AS rn
          FROM dw.fact_venda v
          JOIN comprovantes c
            ON c.id_filial = v.id_filial
           AND c.id_turno = v.id_turno
          JOIN dw.fact_venda_item i
            ON i.id_empresa = v.id_empresa
           AND i.id_filial = v.id_filial
           AND i.id_db = v.id_db
           AND i.id_movprodutos = v.id_movprodutos
          WHERE v.id_empresa = %s
            AND v.data_key BETWEEN %s AND %s
            {where_filial_pay.replace('p.', 'v.')}
            AND v.id_turno IS NOT NULL
            AND i.id_funcionario IS NOT NULL
          GROUP BY v.id_empresa, v.id_filial, v.id_turno, i.id_funcionario
        ) ranked
        LEFT JOIN dw.dim_funcionario df
          ON df.id_empresa = ranked.id_empresa
         AND df.id_filial = ranked.id_filial
         AND df.id_funcionario = ranked.id_funcionario
        WHERE ranked.rn = 1
      )
      SELECT
        c.id_filial,
        COALESCE(f.nome, '') AS filial_nome,
        c.id_turno,
        t.id_usuario,
        operador.id_funcionario,
        COALESCE(NULLIF(u.nome, ''), NULLIF(operador.funcionario_nome, ''), format('Usuário %%s', t.id_usuario)) AS usuario_nome,
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
      FROM comprovantes c
      LEFT JOIN dw.fact_caixa_turno t
        ON t.id_empresa = %s
       AND t.id_filial = c.id_filial
       AND t.id_turno = c.id_turno
      LEFT JOIN dw.dim_usuario_caixa u
        ON u.id_empresa = %s
       AND u.id_filial = c.id_filial
       AND u.id_usuario = t.id_usuario
      LEFT JOIN operador_turno operador
        ON operador.id_empresa = %s
       AND operador.id_filial = c.id_filial
       AND operador.id_turno = c.id_turno
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
        summary_row = conn.execute(sql_summary, params_comp + params_pay).fetchone() or {}
        by_day_rows = [dict(row) for row in conn.execute(sql_by_day, params_comp + params_pay).fetchall()]
        payment_mix_rows = [dict(row) for row in conn.execute(sql_payment_mix, [id_empresa, ini, fim] + agg_branch_params).fetchall()]
        top_turnos_rows = [
            dict(row)
            for row in conn.execute(
                sql_top_turnos,
                params_comp + params_pay + [id_empresa, ini, fim] + pay_branch_params + [id_empresa, id_empresa, id_empresa, id_empresa],
            ).fetchall()
        ]

    total_vendas = round(float(summary_row.get("total_vendas") or 0), 2)
    qtd_vendas = int(summary_row.get("qtd_vendas") or 0)
    total_cancelamentos = round(float(summary_row.get("total_cancelamentos") or 0), 2)
    total_pagamentos = round(float(summary_row.get("total_pagamentos") or 0), 2)
    caixas_periodo = int(summary_row.get("caixas_periodo") or 0)
    qtd_cancelamentos = int(summary_row.get("qtd_cancelamentos") or 0)
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

    for row in top_turnos_rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["usuario_label"] = str(row.get("usuario_nome") or "").strip() or (
            f"Operador {int(row.get('id_usuario'))}" if row.get("id_usuario") is not None else "Operador não identificado"
        )

    cancelamentos = [
        {
            "id_filial": row.get("id_filial"),
            "filial_label": row.get("filial_label"),
            "id_turno": row.get("id_turno"),
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
            f"{caixas_periodo} caixa(s) movimentaram { _format_brl(total_vendas) } "
            f"entre {dt_ini.isoformat()} e {dt_fim.isoformat()}, com {qtd_cancelamentos} cancelamento(s) somando { _format_brl(total_cancelamentos) }."
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
        },
        "by_day": by_day_rows,
        "payment_mix": payment_mix[:8],
        "top_turnos": top_turnos_rows[:10],
        "cancelamentos": cancelamentos,
    }


def cash_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
) -> Dict[str, Any]:
    effective_dt_fim = dt_fim or date.today()
    effective_dt_ini = dt_ini or (effective_dt_fim - timedelta(days=29))
    historical = _cash_historical_overview(role, id_empresa, id_filial, dt_ini=effective_dt_ini, dt_fim=effective_dt_fim)
    live_now = _cash_live_now(role, id_empresa, id_filial)
    return {
        "source_status": historical.get("source_status"),
        "summary": historical.get("summary"),
        "kpis": historical.get("kpis"),
        "historical": historical,
        "live_now": live_now,
        "open_boxes": live_now.get("open_boxes") or [],
        "payment_mix": historical.get("payment_mix") or [],
        "cancelamentos": historical.get("cancelamentos") or [],
        "alerts": live_now.get("alerts") or [],
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
    elif cash.get("source_status") == "unavailable":
        severity = "UNAVAILABLE"

    return {
        "source_status": cash.get("source_status"),
        "severity": severity,
        "summary": cash.get("summary"),
        "total_turnos": int(kpis.get("total_turnos") or 0),
        "mapped_rows": int(kpis.get("total_turnos") or 0),
        "total_open": int(kpis.get("caixas_abertos") or 0),
        "warn_count": int(kpis.get("caixas_em_monitoramento") or 0),
        "high_count": int(kpis.get("caixas_alto_risco") or 0),
        "critical_count": int(kpis.get("caixas_criticos") or 0),
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
    """Goals configured for a given date within the selected scope."""

    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    sql = f"""
      SELECT
        goal_type,
        SUM(target_value)::numeric(18,2) AS target_value,
        COUNT(*)::int AS branch_goal_count
      FROM app.goals
      WHERE id_empresa = %s
        AND goal_date = %s
        {where_filial}
      GROUP BY goal_type
      ORDER BY goal_type
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return list(conn.execute(sql, [id_empresa, goal_date] + branch_params).fetchall())


def leaderboard_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    """Employee leaderboard for gamification."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial, branch_params = _branch_scope_clause("id_filial", id_filial)
    params = [id_empresa, ini, fim] + branch_params + [limit]

    sql = f"""
      SELECT
        id_funcionario,
        MAX(funcionario_nome) AS funcionario_nome,
        SUM(faturamento)::numeric(18,2) AS faturamento,
        SUM(margem)::numeric(18,2) AS margem,
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
        return list(conn.execute(sql, params).fetchall())


# ========================
# Jarvis (rule-based briefing)
# ========================

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
    if not isinstance(risk, dict):
        risk = risk_kpis(role, id_empresa, id_filial, dt_ini, dt_ref)

    risk_focus = (risk_by_turn_local(role, id_empresa, id_filial, dt_ini, dt_ref, limit=1) or [None])[0]
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
                    f"Turno {focus_box.get('id_turno')}" if focus_box and focus_box.get("id_turno") is not None else None,
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
            "cause": "Fraude, caixa, clientes e financeiro seguiram dentro da faixa esperada.",
            "action": "Sustentar o ritmo comercial, proteger margem e manter a rotina de acompanhamento diário.",
            "evidence": ["Sem alertas críticos acima do corte", "Ciclo operacional dentro da faixa esperada"],
            "secondary_focus": [],
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
        "cause": primary["cause"],
        "action": primary["action"],
        "evidence": [item for item in primary.get("evidence", []) if item],
        "secondary_focus": [
            {
                "label": item["headline"],
                "impact_label": _format_brl(item.get("impact_value")),
                "priority": item["priority"],
            }
            for item in secondary
        ],
        "highlights": [
            primary["action"],
            *[item["headline"] for item in secondary],
            *(
                [f"Financeiro em {finance.get('precision_mode')} com referência efetiva em {finance.get('effective_dt_ref')}."]
                if finance.get("snapshot_status") not in {"exact"}
                else []
            ),
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
