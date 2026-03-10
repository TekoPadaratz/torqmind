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


def _filial_label(id_filial: Any, filial_nome: Any = None) -> str:
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
        items.append(f"Desconto total de R$ {desconto_total:,.2f} na operacao.".replace(",", "X").replace(".", ",").replace("X", "."))
    if valor_total > 0 and not any("Valor acima" in item for item in items) and str(event_type or "").upper() == "CANCELAMENTO":
        items.append(f"Valor envolvido de R$ {valor_total:,.2f} no cancelamento.".replace(",", "X").replace(".", ",").replace("X", "."))

    if not items:
        items.append(f"{_event_type_label(event_type)} identificado pela leitura de risco.")

    return items[:3]


def _group_name_expression(group_alias: str, product_alias: str) -> str:
    normalized = f"UPPER(COALESCE(NULLIF({group_alias}.nome, ''), NULLIF({product_alias}.nome, ''), ''))"
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
    product_name = f"UPPER(COALESCE(NULLIF({product_alias}.nome, ''), ''))"
    group_name = f"UPPER(COALESCE(NULLIF({group_alias}.nome, ''), ''))"
    return f"""
      (
        (
          {product_name} LIKE 'GASOL%%'
          OR {product_name} LIKE 'ETANOL%%'
          OR {product_name} LIKE 'DIESEL%%'
          OR {product_name} LIKE 'GNV%%'
          OR (
            {group_name} LIKE '%%COMBUST%%'
            AND {product_name} NOT LIKE '%%BOMBA%%'
            AND {product_name} NOT LIKE '%%FILTRO%%'
            AND {product_name} NOT LIKE '%%KIT%%'
            AND {product_name} NOT LIKE '%%MANGUEIRA%%'
            AND {product_name} NOT LIKE '%%BICO%%'
            AND {product_name} NOT LIKE '%%MEDIDORA%%'
          )
        )
        AND {product_name} NOT LIKE 'ADITIVO%%'
        AND {product_name} NOT LIKE '%% ADITIVO%%'
        AND {product_name} NOT LIKE '%%BOMBA%%'
        AND {product_name} NOT LIKE '%%FILTRO%%'
        AND {product_name} NOT LIKE '%%KIT%%'
        AND {product_name} NOT LIKE '%%MANGUEIRA%%'
        AND {product_name} NOT LIKE '%%BICO%%'
        AND {product_name} NOT LIKE '%%MEDIDORA%%'
        AND {product_name} NOT LIKE '%%ARLA%%'
        AND {product_name} NOT LIKE '%%LUB%%'
        AND {product_name} NOT LIKE '%%ÓLEO%%'
        AND {product_name} NOT LIKE '%%OLEO%%'
        AND {product_name} NOT LIKE '%%FLUID%%'
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
    if category_value and category_value != "DESCONHECIDO":
        return label_value or category_value.replace("_", " ").title()
    if "TIPO_FORMA=" in label_value.upper():
        raw_code = label_value.upper().split("TIPO_FORMA=", 1)[1].split(")", 1)[0].strip()
        return f"Outras formas (tipo {raw_code})"
    return "Forma em validação"


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


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

def dashboard_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"faturamento": 0, "margem": 0, "ticket_medio": 0, "itens": 0}


def dashboard_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT data_key, id_filial, faturamento, margem
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def insights_base(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT data_key, id_filial, faturamento_dia, faturamento_mes_acum, comparativo_mes_anterior
      FROM mart.insights_base_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, id_filial
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Vendas & Stores
# ========================

def sales_by_hour(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT data_key, id_filial, hora, faturamento, margem, vendas
      FROM mart.agg_vendas_hora
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, hora
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def sales_top_products(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def sales_top_groups(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND v.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
    group_name_expr = _group_name_expression("g", "p")
    sql = f"""
      SELECT
        MIN(COALESCE(i.id_grupo_produto, p.id_grupo_produto, -1)) AS id_grupo_produto,
        {group_name_expr} AS grupo_nome,
        COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
        COALESCE(SUM(i.margem),0)::numeric(18,2) AS margem
      FROM dw.fact_venda v
      JOIN dw.fact_venda_item i
        ON i.id_empresa = v.id_empresa
       AND i.id_filial = v.id_filial
       AND i.id_db = v.id_db
       AND i.id_movprodutos = v.id_movprodutos
      LEFT JOIN dw.dim_produto p
        ON p.id_empresa = i.id_empresa
       AND p.id_filial = i.id_filial
       AND p.id_produto = i.id_produto
      LEFT JOIN dw.dim_grupo_produto g
        ON g.id_empresa = i.id_empresa
       AND g.id_filial = i.id_filial
       AND g.id_grupo_produto = COALESCE(i.id_grupo_produto, p.id_grupo_produto)
      WHERE v.id_empresa = %s
        AND v.data_key BETWEEN %s AND %s
        AND COALESCE(v.cancelado, false) = false
        AND COALESCE(i.cfop, 0) >= 5000
        {where_filial}
      GROUP BY {group_name_expr}
      ORDER BY faturamento DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def sales_top_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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
                    else "Manter preço atual e acompanhar o mercado"
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
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT
        COALESCE(SUM(cancelamentos),0)::int AS cancelamentos,
        COALESCE(SUM(valor_cancelado),0)::numeric(18,2) AS valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"cancelamentos": 0, "valor_cancelado": 0}


def fraud_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT data_key, id_filial, cancelamentos, valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def fraud_last_events(role: str, id_empresa: int, id_filial: Optional[int], limit: int = 30) -> List[Dict[str, Any]]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT id_filial, id_db, id_comprovante, data, id_usuario, id_turno, valor_total
      FROM mart.fraude_cancelamentos_eventos
      WHERE id_empresa = %s
      {where_filial}
      ORDER BY data DESC NULLS LAST
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def fraud_top_users(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Risk Scoring / Insights
# ========================

def risk_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"total_eventos": 0, "eventos_alto_risco": 0, "impacto_total": 0, "score_medio": 0}


def risk_series(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def risk_data_window(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT
        MIN(data_key)::int AS min_data_key,
        MAX(data_key)::int AS max_data_key,
        COUNT(*)::int AS rows
      FROM mart.agg_risco_diaria
      WHERE id_empresa = %s
      {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        return row or {"min_data_key": None, "max_data_key": None, "rows": 0}


def risk_top_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def risk_last_events(role: str, id_empresa: int, id_filial: Optional[int], limit: int = 30) -> List[Dict[str, Any]]:
    where_filial = "" if id_filial is None else "AND e.id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial]) + [limit]

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    where_status = "" if not status else "AND status = %s"
    params = [id_empresa, dt_ini, dt_fim] + ([] if id_filial is None else [id_filial]) + ([] if not status else [status]) + [limit]

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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
    where_filial = "" if id_filial is None else "AND rtl.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    for row in rows:
        row["filial_label"] = _filial_label(row.get("id_filial"), row.get("filial_nome"))
        row["local_label"] = _local_venda_label(row.get("id_local_venda"), row.get("local_nome"))
        row["turno_label"] = f"Turno {row['id_turno']}" if row.get("id_turno") is not None and int(row.get("id_turno")) >= 0 else "Turno não informado"
    return rows


def operational_score(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params_sales = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    params_risk = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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


def open_cash_monitor(role: str, id_empresa: int, id_filial: Optional[int]) -> Dict[str, Any]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    where_filial_turno = "" if id_filial is None else "AND t.id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial])

    sql_totals = f"""
      SELECT COUNT(*)::int AS total_turnos
      FROM stg.turnos
      WHERE id_empresa = %s
      {where_filial}
    """

    sql_parsed = f"""
      WITH turnos_raw AS (
        SELECT
          id_empresa,
          id_filial,
          id_turno,
          UPPER(COALESCE(
            payload->>'STATUS',
            payload->>'SITUACAO',
            payload->>'SITUACAO_TURNO',
            payload->>'ST',
            ''
          )) AS status_raw,
          etl.safe_timestamp(COALESCE(
            payload->>'DTABERTURA',
            payload->>'DATAABERTURA',
            payload->>'DTHRABERTURA',
            payload->>'DTHR_ABERTURA',
            payload->>'ABERTURA',
            payload->>'INICIO',
            payload->>'DTINICIO',
            payload->>'DATAINICIO'
          )) AS abertura_ts,
          etl.safe_timestamp(COALESCE(
            payload->>'DTFECHAMENTO',
            payload->>'DATAFECHAMENTO',
            payload->>'DTHRFECHAMENTO',
            payload->>'DTHR_FECHAMENTO',
            payload->>'FECHAMENTO',
            payload->>'FIM',
            payload->>'DTFIM',
            payload->>'DATAFIM'
          )) AS fechamento_ts
        FROM stg.turnos
        WHERE id_empresa = %s
        {where_filial}
      )
      SELECT
        COUNT(*) FILTER (WHERE abertura_ts IS NOT NULL)::int AS mapped_rows,
        COUNT(*) FILTER (
          WHERE abertura_ts IS NOT NULL
            AND fechamento_ts IS NULL
            AND status_raw NOT IN ('FECHADO', 'CLOSED')
        )::int AS total_open,
        COUNT(*) FILTER (
          WHERE abertura_ts IS NOT NULL
            AND fechamento_ts IS NULL
            AND EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 >= 24
        )::int AS critical_count,
        COUNT(*) FILTER (
          WHERE abertura_ts IS NOT NULL
            AND fechamento_ts IS NULL
            AND EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 >= 12
            AND EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 < 24
        )::int AS high_count,
        COUNT(*) FILTER (
          WHERE abertura_ts IS NOT NULL
            AND fechamento_ts IS NULL
            AND EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 >= 6
            AND EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 < 12
        )::int AS warn_count
      FROM turnos_raw
    """

    sql_items = f"""
      WITH turnos_raw AS (
        SELECT
          t.id_filial,
          COALESCE(f.nome, '') AS filial_nome,
          t.id_turno,
          UPPER(COALESCE(
            t.payload->>'STATUS',
            t.payload->>'SITUACAO',
            t.payload->>'SITUACAO_TURNO',
            t.payload->>'ST',
            ''
          )) AS status_raw,
          etl.safe_timestamp(COALESCE(
            t.payload->>'DTABERTURA',
            t.payload->>'DATAABERTURA',
            t.payload->>'DTHRABERTURA',
            t.payload->>'DTHR_ABERTURA',
            t.payload->>'ABERTURA',
            t.payload->>'INICIO',
            t.payload->>'DTINICIO',
            t.payload->>'DATAINICIO'
          )) AS abertura_ts,
          etl.safe_timestamp(COALESCE(
            t.payload->>'DTFECHAMENTO',
            t.payload->>'DATAFECHAMENTO',
            t.payload->>'DTHRFECHAMENTO',
            t.payload->>'DTHR_FECHAMENTO',
            t.payload->>'FECHAMENTO',
            t.payload->>'FIM',
            t.payload->>'DTFIM',
            t.payload->>'DATAFIM'
          )) AS fechamento_ts
        FROM stg.turnos t
        LEFT JOIN auth.filiais f
          ON f.id_empresa = t.id_empresa
         AND f.id_filial = t.id_filial
        WHERE t.id_empresa = %s
        {where_filial_turno}
      )
      SELECT
        id_filial,
        filial_nome,
        id_turno,
        abertura_ts,
        ROUND(EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0, 2) AS open_hours,
        CASE
          WHEN EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 >= 24 THEN 'CRITICAL'
          WHEN EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 >= 12 THEN 'HIGH'
          WHEN EXTRACT(EPOCH FROM (now() - abertura_ts)) / 3600.0 >= 6 THEN 'WARN'
          ELSE 'OK'
        END AS severity
      FROM turnos_raw
      WHERE abertura_ts IS NOT NULL
        AND fechamento_ts IS NULL
        AND status_raw NOT IN ('FECHADO', 'CLOSED')
      ORDER BY open_hours DESC NULLS LAST, id_turno DESC
      LIMIT 10
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        totals = conn.execute(sql_totals, params).fetchone() or {"total_turnos": 0}
        parsed = conn.execute(sql_parsed, params).fetchone() or {}
        items = list(conn.execute(sql_items, params).fetchall())

    total_turnos = int(totals.get("total_turnos", 0) or 0)
    mapped_rows = int(parsed.get("mapped_rows", 0) or 0)
    total_open = int(parsed.get("total_open", 0) or 0)
    critical_count = int(parsed.get("critical_count", 0) or 0)
    high_count = int(parsed.get("high_count", 0) or 0)
    warn_count = int(parsed.get("warn_count", 0) or 0)

    if total_turnos == 0:
        return {
            "source_status": "unavailable",
            "severity": "UNAVAILABLE",
            "summary": "Dados de turnos ainda não chegaram da operação para esta filial.",
            "total_turnos": 0,
            "mapped_rows": 0,
            "total_open": 0,
            "warn_count": 0,
            "high_count": 0,
            "critical_count": 0,
            "items": [],
        }

    if mapped_rows == 0:
        return {
            "source_status": "unmapped",
            "severity": "UNAVAILABLE",
            "summary": "A base de turnos já chegou, mas abertura e fechamento ainda precisam de mapeamento.",
            "total_turnos": total_turnos,
            "mapped_rows": 0,
            "total_open": 0,
            "warn_count": 0,
            "high_count": 0,
            "critical_count": 0,
            "items": [],
        }

    if total_open == 0:
        summary = "Nenhum turno em aberto acima do limite esperado."
        severity = "OK"
    elif critical_count > 0:
        summary = f"{critical_count} turno(s) aberto(s) em situação crítica."
        severity = "CRITICAL"
    elif high_count > 0:
        summary = f"{high_count} turno(s) aberto(s) em situação de alto risco."
        severity = "HIGH"
    elif warn_count > 0:
        summary = f"{warn_count} turno(s) aberto(s) acima do limite esperado."
        severity = "WARN"
    else:
        summary = f"{total_open} turno(s) em aberto dentro da janela monitorada."
        severity = "OK"

    return {
        "source_status": "ok",
        "severity": severity,
        "summary": summary,
        "total_turnos": total_turnos,
        "mapped_rows": mapped_rows,
        "total_open": total_open,
        "warn_count": warn_count,
        "high_count": high_count,
        "critical_count": critical_count,
        "items": items,
    }


# ========================
# Clientes
# ========================

def customers_top(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    """Top customers by revenue for the selected period."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)

    where_filial = "" if id_filial is None else "AND v.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT
        COALESCE(v.id_cliente, -1) AS id_cliente,
        CASE
          WHEN COALESCE(v.id_cliente, -1) = -1 THEN '(Sem cliente)'
          ELSE COALESCE(NULLIF(MAX(dc.nome), ''), '#ID ' || COALESCE(v.id_cliente, -1)::text)
        END AS cliente_nome,
        COALESCE(SUM(v.total_venda),0)::numeric(18,2) AS faturamento,
        COALESCE(COUNT(DISTINCT v.id_comprovante),0)::int AS compras,
        MAX(v.data) AS ultima_compra,
        CASE WHEN COUNT(DISTINCT v.id_comprovante)=0 THEN 0
             ELSE (SUM(v.total_venda)/COUNT(DISTINCT v.id_comprovante))::numeric(18,2)
        END AS ticket_medio
      FROM dw.fact_venda v
      LEFT JOIN dw.dim_cliente dc
        ON dc.id_empresa = v.id_empresa
       AND dc.id_filial = v.id_filial
       AND dc.id_cliente = v.id_cliente
      WHERE v.id_empresa = %s
        AND v.data_key BETWEEN %s AND %s
        AND COALESCE(v.cancelado,false) = false
        {where_filial}
      GROUP BY
        COALESCE(v.id_cliente,-1)
      ORDER BY faturamento DESC
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def customers_rfm_snapshot(role: str, id_empresa: int, id_filial: Optional[int], as_of: date) -> Dict[str, Any]:
    """Very lightweight RFM-like snapshot for *today* (rule-based, no ML yet)."""

    # Last 90 days window
    dt_ini = as_of - timedelta(days=90)
    ini = _date_key(dt_ini)
    fim = _date_key(as_of)

    where_filial = "" if id_filial is None else "AND v.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, min_score] + ([] if id_filial is None else [id_filial]) + [limit]

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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def customers_churn_diamond(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
    min_score: int = 60,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    where_as_of = "AND dt_ref <= %s" if as_of is not None else ""
    params = [id_empresa, min_score] + ([] if id_filial is None else [id_filial]) + ([] if as_of is None else [as_of]) + [limit]
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
        churn_score,
        revenue_at_risk_30d,
        recommendation,
        reasons
      FROM mart.customer_churn_risk_daily
      WHERE id_empresa = %s
        AND churn_score >= %s
        AND id_cliente <> -1
        {where_filial}
        {where_as_of}
      ORDER BY dt_ref DESC, churn_score DESC, revenue_at_risk_30d DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = list(conn.execute(sql, params).fetchall())
        if as_of is not None and not rows:
            fallback_params = [id_empresa, min_score] + ([] if id_filial is None else [id_filial]) + [limit]
            fallback_sql = f"""
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
                churn_score,
                revenue_at_risk_30d,
                recommendation,
                reasons
              FROM mart.customer_churn_risk_daily
              WHERE id_empresa = %s
                AND churn_score >= %s
                AND id_cliente <> -1
                {where_filial}
              ORDER BY dt_ref DESC, churn_score DESC, revenue_at_risk_30d DESC
              LIMIT %s
            """
            rows = list(conn.execute(fallback_sql, fallback_params).fetchall())
        return rows


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
    where_filial = "" if id_filial is None else "AND v.id_filial = %s"
    params = [id_empresa, id_cliente, ini, fim] + ([] if id_filial is None else [id_filial])

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
        {"" if id_filial is None else "AND id_filial = %s"}
        {"" if as_of is None else "AND dt_ref <= %s"}
      ORDER BY dt_ref DESC
      LIMIT 1
    """
    params_snapshot = [id_empresa, id_cliente] + ([] if id_filial is None else [id_filial]) + ([] if as_of is None else [as_of])

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        series = list(conn.execute(sql_series, params).fetchall())
        snap = conn.execute(sql_snapshot, params_snapshot).fetchone()
        if as_of is not None and not snap:
            fallback_sql = f"""
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
                reasons
              FROM mart.customer_churn_risk_daily
              WHERE id_empresa = %s
                AND id_cliente = %s
                {"" if id_filial is None else "AND id_filial = %s"}
              ORDER BY dt_ref DESC
              LIMIT 1
            """
            fallback_params = [id_empresa, id_cliente] + ([] if id_filial is None else [id_filial])
            snap = conn.execute(fallback_sql, fallback_params).fetchone()
    return {"snapshot": snap or {}, "series": series}


def anonymous_retention_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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
    params_latest = [id_empresa, id_empresa, dt_fim] + ([] if id_filial is None else [id_filial, id_filial])

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        latest_rows = list(conn.execute(sql_latest, params_latest).fetchall())
        series = list(conn.execute(sql_series, params).fetchall())

    agg_impact = sum(float(r.get("impact_estimated_7d") or 0) for r in latest_rows)
    avg_trend = (sum(float(r.get("trend_pct") or 0) for r in latest_rows) / len(latest_rows)) if latest_rows else 0.0
    avg_repeat = (sum(float(r.get("repeat_proxy_idx") or 0) for r in latest_rows) / len(latest_rows)) if latest_rows else 0.0

    recommendation = (
        "Recorrencia anonima caiu. Ajuste operacao por horario/dia, reveja mix de produtos e acione promocoes de retorno."
        if avg_trend < -8
        else "Recorrencia anonima estavel. Monitorar horarios de maior queda e manter acoes de fidelizacao."
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

    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

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

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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

    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    sql = f"""
      SELECT data_key, id_filial, tipo_titulo, valor_total, valor_pago, valor_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s AND data_key BETWEEN %s AND %s
      {where_filial}
      ORDER BY data_key, tipo_titulo
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def finance_aging_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    as_of_sql = "AND dt_ref <= %s" if as_of is not None else ""
    if id_filial is None:
        # Consolidated tenant view: aggregate latest day across branches.
        sql = f"""
          WITH latest AS (
            SELECT MAX(dt_ref) AS dt_ref
            FROM mart.finance_aging_daily
            WHERE id_empresa = %s
              {as_of_sql}
          )
          SELECT
            l.dt_ref,
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
            COALESCE(BOOL_OR(f.data_gaps), true) AS data_gaps
          FROM latest l
          LEFT JOIN mart.finance_aging_daily f
            ON f.id_empresa = %s
           AND f.dt_ref = l.dt_ref
          GROUP BY l.dt_ref
        """
        params = [id_empresa] + ([as_of] if as_of is not None else []) + [id_empresa]
        fallback_params = [id_empresa, id_empresa]
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
            data_gaps
          FROM mart.finance_aging_daily
          WHERE id_empresa = %s
          {where_filial}
          {as_of_sql}
          ORDER BY dt_ref DESC
          LIMIT 1
        """
        if as_of is not None:
            params = [id_empresa, id_filial, as_of]
            fallback_params = [id_empresa, id_filial]
        else:
            params = [id_empresa, id_filial]
            fallback_params = params
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        # Older reference dates may have no snapshot if marts were built only for CURRENT_DATE.
        # In this case, fallback to latest available snapshot to avoid returning all zeros.
        if as_of is not None and (not row or row.get("dt_ref") is None):
            fallback_sql = sql.replace(as_of_sql, "")
            row = conn.execute(fallback_sql, fallback_params).fetchone()
        return row or {
            "dt_ref": None,
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
        }


def payments_overview_kpis(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    days = max((dt_fim - dt_ini).days + 1, 1)
    prev_fim = ini - 1
    prev_ini = _date_key(dt_ini - timedelta(days=days))
    where_filial = "" if id_filial is None else "AND id_filial = %s"

    sql_curr = f"""
      SELECT
        COALESCE(SUM(total_valor),0)::numeric(18,2) AS total_valor,
        COALESCE(SUM(CASE WHEN category = 'DESCONHECIDO' THEN total_valor ELSE 0 END),0)::numeric(18,2) AS unknown_valor,
        COALESCE(SUM(qtd_comprovantes),0)::int AS qtd_comprovantes
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
    params_curr = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
    params_prev = [id_empresa, prev_ini, prev_fim] + ([] if id_filial is None else [id_filial])
    params_mix = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        curr = conn.execute(sql_curr, params_curr).fetchone() or {}
        prev = conn.execute(sql_prev, params_prev).fetchone() or {}
        mix = list(conn.execute(sql_mix, params_mix).fetchall())

    total_curr = float(curr.get("total_valor") or 0)
    total_prev = float(prev.get("total_valor") or 0)
    unknown_val = float(curr.get("unknown_valor") or 0)
    unknown_share = (unknown_val / total_curr * 100.0) if total_curr > 0 else 0.0
    delta_pct = ((total_curr - total_prev) / total_prev * 100.0) if total_prev > 0 else (100.0 if total_curr > 0 else 0.0)

    return {
        "total_valor": round(total_curr, 2),
        "total_valor_prev": round(total_prev, 2),
        "delta_pct": round(delta_pct, 2),
        "qtd_comprovantes": int(curr.get("qtd_comprovantes") or 0),
        "unknown_valor": round(unknown_val, 2),
        "unknown_share_pct": round(unknown_share, 2),
        "mix": mix,
    }


def payments_by_day(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def payments_by_turno(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial])
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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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
    where_filial = "" if id_filial is None else "AND p.id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]
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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
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


def health_score_latest(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    where_as_of = "AND dt_ref <= %s" if as_of is not None else ""
    params = [id_empresa] + ([] if id_filial is None else [id_filial]) + ([] if as_of is None else [as_of])
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
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        # Fallback to latest snapshot when requested reference date has no row.
        if as_of is not None and (not row or row.get("dt_ref") is None):
            fallback_params = [id_empresa] + ([] if id_filial is None else [id_filial])
            fallback_sql = f"""
              SELECT
                dt_ref,
                score_total,
                components,
                reasons
              FROM mart.health_score_daily
              WHERE id_empresa = %s
              {where_filial}
              ORDER BY dt_ref DESC
              LIMIT 1
            """
            row = conn.execute(fallback_sql, fallback_params).fetchone()
        return row or {"dt_ref": None, "score_total": 0, "components": {}, "reasons": {}}


# ========================
# Metas & Equipe
# ========================

def goals_today(role: str, id_empresa: int, id_filial: int, goal_date: date) -> List[Dict[str, Any]]:
    """Goals configured for a given date (branch)."""

    sql = """
      SELECT goal_type, target_value
      FROM app.goals
      WHERE id_empresa = %s AND id_filial = %s AND goal_date = %s
      ORDER BY goal_type
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, (id_empresa, id_filial, goal_date)).fetchall())


def leaderboard_employees(role: str, id_empresa: int, id_filial: Optional[int], dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    """Employee leaderboard for gamification."""

    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, ini, fim] + ([] if id_filial is None else [id_filial]) + [limit]

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

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


# ========================
# Jarvis (rule-based briefing)
# ========================

def jarvis_briefing(role: str, id_empresa: int, id_filial: Optional[int], dt_ref: date) -> Dict[str, Any]:
    """Return a short executive briefing.

    PT-BR: Ainda não é LLM/ML (isso vem depois). Aqui já entregamos inteligência operacional
    com regras simples e objetivas.
    """

    d0 = dt_ref
    d1 = dt_ref - timedelta(days=1)

    k0 = _date_key(d0)
    k1 = _date_key(d1)

    where_filial = "" if id_filial is None else "AND id_filial = %s"

    # Revenue day 0/1
    sql_rev = f"""
      SELECT data_key, COALESCE(SUM(faturamento),0)::numeric(18,2) AS faturamento,
             COALESCE(SUM(margem),0)::numeric(18,2) AS margem
      FROM mart.agg_vendas_diaria
      WHERE id_empresa = %s AND data_key IN (%s,%s)
      {where_filial}
      GROUP BY data_key
    """
    params_rev = [id_empresa, k0, k1] + ([] if id_filial is None else [id_filial])

    # Fraud cancellations day 0/1
    sql_can = f"""
      SELECT data_key, COALESCE(SUM(cancelamentos),0)::int AS cancelamentos,
             COALESCE(SUM(valor_cancelado),0)::numeric(18,2) AS valor_cancelado
      FROM mart.fraude_cancelamentos_diaria
      WHERE id_empresa = %s AND data_key IN (%s,%s)
      {where_filial}
      GROUP BY data_key
    """
    params_can = [id_empresa, k0, k1] + ([] if id_filial is None else [id_filial])

    # Open receivables overdue (any day < dt_ref)
    sql_overdue = f"""
      SELECT COALESCE(SUM(valor_aberto),0)::numeric(18,2) AS receber_vencido_aberto
      FROM mart.financeiro_vencimentos_diaria
      WHERE id_empresa = %s
        AND tipo_titulo = 1
        AND data_key < %s
        {where_filial}
    """
    params_overdue = [id_empresa, k0] + ([] if id_filial is None else [id_filial])

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rev_rows = conn.execute(sql_rev, params_rev).fetchall()
        can_rows = conn.execute(sql_can, params_can).fetchall()
        overdue = conn.execute(sql_overdue, params_overdue).fetchone() or {"receber_vencido_aberto": 0}

    rev_map = {r["data_key"]: r for r in rev_rows}
    can_map = {r["data_key"]: r for r in can_rows}

    f0 = float(rev_map.get(k0, {}).get("faturamento", 0) or 0)
    f1 = float(rev_map.get(k1, {}).get("faturamento", 0) or 0)
    m0 = float(rev_map.get(k0, {}).get("margem", 0) or 0)
    m1 = float(rev_map.get(k1, {}).get("margem", 0) or 0)

    c0 = int(can_map.get(k0, {}).get("cancelamentos", 0) or 0)
    c1 = int(can_map.get(k1, {}).get("cancelamentos", 0) or 0)
    cv0 = float(can_map.get(k0, {}).get("valor_cancelado", 0) or 0)
    cv1 = float(can_map.get(k1, {}).get("valor_cancelado", 0) or 0)

    receber_vencido_aberto = float(overdue.get("receber_vencido_aberto", 0) or 0)

    delta_f = f0 - f1
    delta_m = m0 - m1

    bullets: List[str] = []

    # Simple, high-impact heuristics
    if f1 > 0 and delta_f / f1 <= -0.08:
        bullets.append(f"📉 Faturamento caiu {abs(delta_f):,.2f} vs ontem. Ação: validar preço x concorrência e ruptura de bombas/loja.")
    elif delta_f > 0:
        bullets.append(f"📈 Faturamento subiu {delta_f:,.2f} vs ontem. Ação: replicar condições (preço/promo/escala) nas demais filiais.")

    if m0 < 0 and f0 > 0:
        bullets.append("⚠️ Margem negativa no dia. Ação: checar custo médio (cadastro de produtos) e descontos/erros de preço.")
    elif f0 > 0 and (m0 / f0) < 0.05:
        bullets.append("⚠️ Margem baixa (<5%). Ação: revisar mix (loja vs combustível), descontos e condições com fornecedores.")

    if c0 > max(3, int(c1 * 1.5)):
        bullets.append(f"🧨 Cancelamentos altos hoje ({c0}). Ação: auditar operador/turno e ativar alerta Telegram para o dono.")

    if receber_vencido_aberto > 0:
        bullets.append(f"💰 Recebíveis vencidos em aberto: {receber_vencido_aberto:,.2f}. Ação: cobrança ativa + renegociação (reduz churn e inadimplência).")

    if not bullets:
        bullets.append("✅ Operação dentro do esperado para o período selecionado. Ação: foque em aumentar ticket na loja e reduzir cancelamentos.")

    return {
        "data_ref": d0.isoformat(),
        "kpis": {
            "faturamento": f0,
            "margem": m0,
            "cancelamentos": c0,
            "valor_cancelado": cv0,
        },
        "comparativo": {
            "faturamento_vs_ontem": delta_f,
            "margem_vs_ontem": delta_m,
            "cancelamentos_vs_ontem": c0 - c1,
        },
        "bullets": bullets,
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
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    where_unread = "AND read_at IS NULL" if unread_only else ""
    params = [id_empresa] + ([] if id_filial is None else [id_filial]) + [limit]
    sql = f"""
      SELECT id, id_filial, severity, title, body, url, created_at, read_at
      FROM app.notifications
      WHERE id_empresa = %s
        {where_filial}
        {where_unread}
      ORDER BY created_at DESC
      LIMIT %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def notifications_unread_count(role: str, id_empresa: int, id_filial: Optional[int]) -> int:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial])
    sql = f"""
      SELECT COALESCE(COUNT(*),0)::int AS total
      FROM app.notifications
      WHERE id_empresa = %s
        {where_filial}
        AND read_at IS NULL
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone() or {"total": 0}
    return int(row["total"])


def notification_mark_read(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    notification_id: int,
) -> Dict[str, Any]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [id_empresa, notification_id] + ([] if id_filial is None else [id_filial])
    sql = f"""
      UPDATE app.notifications
      SET read_at = COALESCE(read_at, now())
      WHERE id_empresa = %s
        AND id = %s
        {where_filial}
      RETURNING id, read_at
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()
    return row or {"id": notification_id, "read_at": None}
