"""ClickHouse realtime mart reads for TorqMind BI.

Reads from torqmind_mart_rt (fed by CDC Mart Builder) instead of
torqmind_mart (fed by batch sync). Function signatures mirror
repos_mart_clickhouse.py EXACTLY for transparent switching via repos_analytics.py.

Feature flag: USE_REALTIME_MARTS=true activates this module.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from app.business_time import business_today
from app.config import settings
from app.db_clickhouse import query_dict, query_scalar
from app.repos_mart import _cash_operator_label, _filial_label, _turno_label

logger = logging.getLogger(__name__)

MART_RT_DB = "torqmind_mart_rt"
CURRENT_DB = "torqmind_current"


def _realtime_source() -> str:
    return str(getattr(settings, "realtime_marts_source", "stg") or "stg").lower()


def _branch_ids(id_filial: Any) -> Optional[List[int]]:
    """Parse id_filial into a list of branch IDs (mirrors repos_mart_clickhouse)."""
    if id_filial is None or id_filial == -1:
        return None
    if isinstance(id_filial, (list, tuple, set)):
        values = sorted({int(v) for v in id_filial if v is not None and int(v) != -1})
        return values if values else None
    value = int(id_filial)
    return None if value == -1 else [value]


def _branch_clause(column: str, id_filial: Any) -> str:
    """Build WHERE clause for filial filtering (mirrors repos_mart_clickhouse)."""
    branch_ids = _branch_ids(id_filial)
    if branch_ids is None:
        return ""
    if not branch_ids:
        return " AND 0"
    if len(branch_ids) == 1:
        return f" AND {column} = {int(branch_ids[0])}"
    values = ", ".join(str(int(v)) for v in branch_ids)
    return f" AND {column} IN ({values})"


def _date_range_filter(dt_ini: date, dt_fim: date, col: str = "data_key") -> str:
    from_key = int(dt_ini.strftime("%Y%m%d"))
    to_key = int(dt_fim.strftime("%Y%m%d"))
    return f" AND {col} >= {from_key} AND {col} <= {to_key}"


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _to_float(value: Any, decimals: int = 2) -> float:
    try:
        return round(float(value), decimals)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sales_product_meta_subquery() -> str:
    return f"""
        SELECT
            id_empresa,
            id_produto,
            argMax(nullIf(JSONExtractString(payload, 'UNIDADE'), ''), source_ts_ms) AS unidade
        FROM {CURRENT_DB}.stg_produtos FINAL
        WHERE is_deleted = 0
        GROUP BY id_empresa, id_produto
    """


def _sales_quantity_kind_sql(product_expr: str, group_expr: str) -> str:
    return (
        "multiIf("
        f"positionCaseInsensitiveUTF8(ifNull({group_expr}, ''), 'COMBUST') > 0 OR "
        f"positionCaseInsensitiveUTF8(ifNull({group_expr}, ''), 'GNV') > 0 OR "
        f"positionCaseInsensitiveUTF8(ifNull({product_expr}, ''), 'GASOL') > 0 OR "
        f"positionCaseInsensitiveUTF8(ifNull({product_expr}, ''), 'DIESEL') > 0 OR "
        f"positionCaseInsensitiveUTF8(ifNull({product_expr}, ''), 'ETANOL') > 0 OR "
        f"positionCaseInsensitiveUTF8(ifNull({product_expr}, ''), 'ALCOOL') > 0 OR "
        f"positionCaseInsensitiveUTF8(ifNull({product_expr}, ''), 'GNV') > 0, "
        "'fuel', 'unit')"
    )


def _load_current_filial_names(id_empresa: int, rows: List[Dict[str, Any]]) -> Dict[int, str]:
    branch_ids = sorted(
        {
            int(row["id_filial"])
            for row in rows
            if row.get("id_filial") is not None
        }
    )
    if not branch_ids:
        return {}

    values = ", ".join(str(branch_id) for branch_id in branch_ids)
    result = query_dict(
        f"""
        SELECT
            id_filial,
            argMax(
                coalesce(
                    nullIf(JSONExtractString(payload, 'NOMEFILIAL'), ''),
                    nullIf(JSONExtractString(payload, 'NOME'), ''),
                    nullIf(JSONExtractString(payload, 'RAZAOSOCIALFILIAL'), '')
                ),
                source_ts_ms
            ) AS filial_nome
        FROM {CURRENT_DB}.stg_filiais FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND is_deleted = 0
          AND id_filial IN ({values})
        GROUP BY id_filial
        """,
        parameters={"id_empresa": id_empresa},
    )
    return {
        int(row["id_filial"]): str(row.get("filial_nome") or "").strip()
        for row in result
    }


def _load_current_turno_values(id_empresa: int, rows: List[Dict[str, Any]]) -> Dict[tuple[int, int], str]:
    turno_pairs = sorted(
        {
            (int(row["id_filial"]), int(row["id_turno"]))
            for row in rows
            if row.get("id_filial") is not None and row.get("id_turno") is not None
        }
    )
    if not turno_pairs:
        return {}

    values = ", ".join(f"({id_filial}, {id_turno})" for id_filial, id_turno in turno_pairs)
    result = query_dict(
        f"""
        SELECT
            id_filial,
            id_turno,
            argMax(
                coalesce(
                    nullIf(JSONExtractString(payload, 'TURNO'), ''),
                    nullIf(JSONExtractString(payload, 'NO_TURNO'), ''),
                    nullIf(JSONExtractString(payload, 'NUMTURNO'), ''),
                    nullIf(JSONExtractString(payload, 'NR_TURNO'), ''),
                    nullIf(JSONExtractString(payload, 'NROTURNO'), ''),
                    nullIf(JSONExtractString(payload, 'TURNO_CAIXA'), ''),
                    nullIf(JSONExtractString(payload, 'TURNOCAIXA'), '')
                ),
                source_ts_ms
            ) AS turno_value
        FROM {CURRENT_DB}.stg_turnos FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND is_deleted = 0
          AND (id_filial, id_turno) IN ({values})
        GROUP BY id_filial, id_turno
        """,
        parameters={"id_empresa": id_empresa},
    )
    return {
        (int(row["id_filial"]), int(row["id_turno"])): str(row.get("turno_value") or "").strip()
        for row in result
    }


# ================================================================
# DASHBOARD HOME
# ================================================================

def dashboard_kpis(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> Dict[str, Any]:
    """KPIs for the main dashboard."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    rows = query_dict(f"""
        SELECT
            s_fat AS faturamento,
            s_vendas AS qtd_vendas,
            if(s_vendas > 0, s_fat / s_vendas, 0) AS ticket_medio,
            s_clientes AS qtd_clientes,
            s_cancel AS qtd_cancelamentos,
            s_val_cancel AS valor_cancelado
        FROM (
            SELECT
                sum(faturamento) AS s_fat,
                sum(qtd_vendas) AS s_vendas,
                sum(qtd_clientes) AS s_clientes,
                sum(qtd_cancelamentos) AS s_cancel,
                sum(valor_cancelado) AS s_val_cancel
            FROM {MART_RT_DB}.dashboard_home_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        )
    """, parameters={"id_empresa": id_empresa})

    if not rows:
        return {"faturamento": 0, "qtd_vendas": 0, "ticket_medio": 0, "qtd_clientes": 0, "qtd_cancelamentos": 0, "valor_cancelado": 0}
    return rows[0]


def dashboard_series(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Daily series for dashboard chart."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    return query_dict(f"""
        SELECT
            dt,
            s_fat AS faturamento,
            s_vendas AS qtd_vendas,
            if(s_vendas > 0, s_fat / s_vendas, 0) AS ticket_medio
        FROM (
            SELECT
                dt,
                sum(faturamento) AS s_fat,
                sum(qtd_vendas) AS s_vendas
            FROM {MART_RT_DB}.dashboard_home_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY dt
        )
        ORDER BY dt
    """, parameters={"id_empresa": id_empresa})


def dashboard_home_bundle(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    dt_ref: date = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Full dashboard home payload matching frontend contract."""
    from datetime import datetime, timezone

    source = _realtime_source()
    product_meta_sql = _sales_product_meta_subquery()
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    params = {"id_empresa": id_empresa}

    # --- Sales KPIs ---
    sales_kpi_rows = query_dict(f"""
        SELECT s_fat AS faturamento, s_margem AS margem, s_vendas AS qtd_vendas,
               if(s_vendas > 0, s_fat / s_vendas, 0) AS ticket_medio
        FROM (
            SELECT sum(faturamento) AS s_fat, sum(margem_total) AS s_margem, sum(qtd_vendas) AS s_vendas
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        )
    """, parameters=params)
    sales_kpis = sales_kpi_rows[0] if sales_kpi_rows else {"faturamento": 0, "margem": 0, "ticket_medio": 0, "qtd_vendas": 0}
    sales_kpis.setdefault("devolucoes", 0)

    # --- Sales by day ---
    by_day = query_dict(f"""
        SELECT dt, s_fat AS faturamento, s_vendas AS qtd_vendas
        FROM (
            SELECT dt, sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY dt
        ) ORDER BY dt
    """, parameters=params)

    # --- Sales by hour ---
    by_hour = query_dict(f"""
        SELECT hora, s_fat AS faturamento, s_vendas AS qtd_vendas
        FROM (
            SELECT hora, sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas
            FROM {MART_RT_DB}.sales_hourly_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY hora
        ) ORDER BY hora
    """, parameters=params)

    # --- Top products ---
    top_products = query_dict(f"""
        SELECT
            ranked.id_produto,
            ranked.nome_produto,
            ranked.nome_produto AS produto_nome,
            ranked.nome_grupo,
            ranked.nome_grupo AS grupo_nome,
            meta.unidade AS unidade,
            {_sales_quantity_kind_sql('ranked.nome_produto', 'ranked.nome_grupo')} AS quantity_kind,
            ranked.faturamento,
            ranked.qtd,
            ranked.margem
        FROM (
            SELECT id_produto, nome_produto, nome_grupo,
                   sum(faturamento) AS faturamento, sum(qtd) AS qtd, sum(margem) AS margem
            FROM {MART_RT_DB}.sales_products_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY id_produto, nome_produto, nome_grupo
        ) AS ranked
        LEFT JOIN ({product_meta_sql}) AS meta
            ON meta.id_empresa = {{id_empresa:Int32}} AND meta.id_produto = ranked.id_produto
        ORDER BY ranked.faturamento DESC
        LIMIT 10
    """, parameters=params)

    # --- Top groups ---
    top_groups = query_dict(f"""
        SELECT id_grupo_produto, nome_grupo AS grupo_nome, s_fat AS faturamento, s_margem AS margem
        FROM (
            SELECT id_grupo_produto, nome_grupo, sum(faturamento) AS s_fat, sum(margem) AS s_margem
            FROM {MART_RT_DB}.sales_groups_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY id_grupo_produto, nome_grupo
        ) ORDER BY faturamento DESC LIMIT 10
    """, parameters=params)

    # --- Fraud / Risk KPIs ---
    fraud_rows = query_dict(f"""
        SELECT s_ev AS qtd_eventos, s_imp AS impacto_total, s_score AS score_medio
        FROM (
            SELECT sum(qtd_eventos) AS s_ev, sum(impacto_total) AS s_imp, avg(score_medio) AS s_score
            FROM {MART_RT_DB}.fraud_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        )
    """, parameters=params)
    fraud_kpi = fraud_rows[0] if fraud_rows else {"qtd_eventos": 0, "impacto_total": 0, "score_medio": 0}

    # --- Cash live_now ---
    cash_rows = query_dict(f"""
        SELECT count() AS qtd_abertos, sum(faturamento_turno) AS fat_aberto
        FROM {MART_RT_DB}.cash_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} AND is_aberto = 1
    """, parameters=params)
    cash_live = cash_rows[0] if cash_rows else {"qtd_abertos": 0, "fat_aberto": 0}

    # --- Finance aging ---
    finance_rows = query_dict(f"""
        SELECT tipo_titulo, faixa, sum(qtd_titulos) AS qtd_titulos,
               sum(valor_total) AS valor_total, sum(valor_em_aberto) AS valor_em_aberto
        FROM {MART_RT_DB}.finance_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
        GROUP BY tipo_titulo, faixa
    """, parameters=params)

    # Build finance aging summary dict
    receber_vencido = sum(float(r.get("valor_em_aberto") or 0) for r in finance_rows if int(r.get("tipo_titulo") or 0) == 1 and str(r.get("faixa", "")).startswith("vencid"))
    pagar_vencido = sum(float(r.get("valor_em_aberto") or 0) for r in finance_rows if int(r.get("tipo_titulo") or 0) == 0 and str(r.get("faixa", "")).startswith("vencid"))
    total_em_aberto = sum(float(r.get("valor_em_aberto") or 0) for r in finance_rows)
    top5_pct = 0.0
    finance_aging = {
        "receber_total_vencido": receber_vencido,
        "pagar_total_vencido": pagar_vencido,
        "total_em_aberto": total_em_aberto,
        "top5_concentration_pct": top5_pct,
    }

    # --- Sales KPIs for fraud operational ---
    cancel_kpis_rows = query_dict(f"""
        SELECT s_cancel AS qtd_canceladas, s_val_cancel AS valor_cancelado
        FROM (
            SELECT sum(qtd_canceladas) AS s_cancel, sum(valor_cancelado) AS s_val_cancel
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        )
    """, parameters=params)
    cancel_kpis = cancel_kpis_rows[0] if cancel_kpis_rows else {"qtd_canceladas": 0, "valor_cancelado": 0}
    qtd_cancelamentos = int(cancel_kpis.get("qtd_canceladas") or 0)
    valor_cancelado_total = float(cancel_kpis.get("valor_cancelado") or 0)

    now_iso = datetime.now(timezone.utc).isoformat()
    freshness_meta = {"mode": "realtime", "source": source, "last_refresh": now_iso}

    return {
        "kpis": sales_kpis,
        "alerts": [],
        "series": {},
        "insights": None,
        "scope": {
            "id_empresa": id_empresa,
            "id_filial": id_filial,
            "dt_ini": dt_ini.isoformat(),
            "dt_fim": dt_fim.isoformat(),
        },
        "overview": {
            "sales": {
                "kpis": sales_kpis,
                "by_day": by_day,
                "by_hour": by_hour,
                "top_products": top_products,
                "top_groups": top_groups,
                "top_employees": [],
                "reading_status": "realtime_mart_snapshot",
                "freshness": freshness_meta,
                "operational_sync": {"source": "realtime", "last_publish": now_iso},
                "data_state": "available",
            },
            "insights_generated": [],
            "fraud": {
                "operational": {
                    "kpis": {
                        "cancelamentos": qtd_cancelamentos,
                        "valor_cancelado": valor_cancelado_total,
                    },
                    "window": {"rows": int(fraud_kpi.get("qtd_eventos", 0))},
                    "data_state": "available",
                },
                "modeled_risk": {
                    "kpis": {
                        "total_eventos": int(fraud_kpi.get("qtd_eventos", 0)),
                        "eventos_alto_risco": 0,
                        "impacto_total": float(fraud_kpi.get("impacto_total", 0)),
                        "score_medio": float(fraud_kpi.get("score_medio", 0)),
                    },
                    "window": {"rows": int(fraud_kpi.get("qtd_eventos", 0))},
                    "data_state": "available",
                },
            },
            "risk": {
                "kpis": {
                    "total_eventos": int(fraud_kpi.get("qtd_eventos", 0)),
                    "eventos_alto_risco": 0,
                    "impacto_total": float(fraud_kpi.get("impacto_total", 0)),
                    "score_medio": float(fraud_kpi.get("score_medio", 0)),
                },
                "window": {"rows": int(fraud_kpi.get("qtd_eventos", 0))},
                "data_state": "available",
            },
            "cash": {
                "historical": {"source_status": "available"},
                "live_now": {
                    "source_status": "available",
                    "kpis": {
                        "caixas_abertos": int(cash_live.get("qtd_abertos", 0)),
                        "caixas_criticos": 0,
                        "caixas_em_monitoramento": 0,
                        "caixas_alto_risco": 0,
                        "total_vendas_abertas": float(cash_live.get("fat_aberto", 0)),
                    },
                    "open_boxes": [],
                },
            },
            "jarvis": {
                "title": "Leitura consolidada",
                "headline": "Seus dados estão atualizados via streaming.",
                "summary": "A operação está funcionando normalmente com dados em tempo real.",
                "impact_label": "Normal",
                "action": "Nenhuma ação necessária.",
                "priority": "Normal",
                "status": "ok",
                "primary_kind": None,
                "primary_shortcut": None,
                "evidence": [],
                "highlights": [],
                "secondary_focus": [],
                "signals": {
                    "peak_hours": {"source_status": "available", "window_days": 0, "peak_hours": [], "off_peak_hours": [], "recommendations": {"peak": None, "off_peak": None}},
                    "declining_products": {"source_status": "available", "items": []},
                },
            },
        },
        "churn": {
            "top_risk": [],
            "summary": {"total_top_risk": 0, "avg_churn_score": 0, "revenue_at_risk_30d": 0},
        },
        "finance": {"aging": finance_aging, "aging_rows": finance_rows},
        "cash": {
            "source_status": "available",
            "summary": "Dados do caixa carregados via realtime mart.",
            "operational_sync": {"source": "realtime", "last_publish": now_iso},
            "freshness": freshness_meta,
            "historical": {"source_status": "available", "kpis": {}, "payment_mix": [], "top_turnos": [], "cancelamentos": [], "by_day": []},
            "live_now": {
                "source_status": "available",
                "kpis": {
                    "caixas_abertos": int(cash_live.get("qtd_abertos", 0)),
                    "caixas_criticos": 0,
                    "caixas_em_monitoramento": 0,
                    "caixas_alto_risco": 0,
                    "total_vendas_abertas": float(cash_live.get("fat_aberto", 0)),
                },
                "open_boxes": [],
                "stale_boxes": [],
                "payment_mix": [],
                "cancelamentos": [],
                "alerts": [],
            },
            "open_boxes": [],
            "stale_boxes": [],
            "payment_mix": [],
            "cancelamentos": [],
            "alerts": [],
        },
        "notifications_unread": 0,
        "operational_sync": {"source": "realtime", "last_publish": now_iso},
        "freshness": freshness_meta,
        "source": "realtime",
        "realtime_source": source,
    }


# ================================================================
# SALES DOMAIN
# ================================================================

def sales_overview_bundle(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
    *,
    include_details: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Sales overview with full contract matching frontend expectations."""
    from datetime import datetime, timezone

    source = _realtime_source()
    product_meta_sql = _sales_product_meta_subquery()
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    params = {"id_empresa": id_empresa}

    # --- Aggregated KPIs ---
    kpis_rows = query_dict(f"""
        SELECT s_fat AS faturamento, s_vendas AS qtd_vendas, s_itens AS qtd_itens,
               s_cancel AS qtd_canceladas, s_val_cancel AS valor_cancelado,
               s_desc AS desconto_total, s_margem AS margem,
               if(s_vendas > 0, s_fat / s_vendas, 0) AS ticket_medio
        FROM (
            SELECT sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas,
                   sum(qtd_itens) AS s_itens, sum(qtd_canceladas) AS s_cancel,
                   sum(valor_cancelado) AS s_val_cancel, sum(desconto_total) AS s_desc,
                   sum(margem_total) AS s_margem
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        )
    """, parameters=params)
    raw_kpis = kpis_rows[0] if kpis_rows else {}
    faturamento = float(raw_kpis.get("faturamento") or 0)
    margem = float(raw_kpis.get("margem") or 0)
    ticket_medio = float(raw_kpis.get("ticket_medio") or 0)
    qtd_vendas = int(raw_kpis.get("qtd_vendas") or 0)
    qtd_canceladas = int(raw_kpis.get("qtd_canceladas") or 0)
    valor_cancelado = float(raw_kpis.get("valor_cancelado") or 0)

    kpis = {"faturamento": faturamento, "margem": margem, "ticket_medio": ticket_medio, "devolucoes": 0}

    commercial_kpis = {
        "saidas": faturamento,
        "qtd_saidas": qtd_vendas,
        "entradas": 0,
        "qtd_entradas": 0,
        "cancelamentos": valor_cancelado,
        "qtd_cancelamentos": qtd_canceladas,
    }

    cfop_breakdown = [
        {"label": "Vendas normais", "valor_ativo": faturamento, "valor_cancelado": valor_cancelado},
        {"label": "Cancelamentos", "valor_ativo": 0, "valor_cancelado": valor_cancelado},
    ]

    # --- By day ---
    by_day = query_dict(f"""
        SELECT dt, s_fat AS faturamento, s_vendas AS qtd_vendas
        FROM (
            SELECT dt, sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY dt
        ) ORDER BY dt
    """, parameters=params)

    # --- By hour ---
    by_hour = query_dict(f"""
        SELECT hora, s_fat AS faturamento, s_vendas AS qtd_vendas
        FROM (
            SELECT hora, sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas
            FROM {MART_RT_DB}.sales_hourly_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY hora
        ) ORDER BY hora
    """, parameters=params)

    # --- Commercial by hour (with saidas) ---
    commercial_by_hour = [{"hora": r.get("hora"), "saidas": float(r.get("faturamento") or 0)} for r in by_hour]

    # --- Top products ---
    top_products = query_dict(f"""
        SELECT
            ranked.id_produto,
            ranked.nome_produto,
            ranked.nome_produto AS produto_nome,
            ranked.nome_grupo,
            ranked.nome_grupo AS grupo_nome,
            meta.unidade AS unidade,
            {_sales_quantity_kind_sql('ranked.nome_produto', 'ranked.nome_grupo')} AS quantity_kind,
            ranked.faturamento,
            ranked.qtd,
            ranked.margem,
            ranked.custo_total,
            if(ranked.qtd > 0, toFloat64(ranked.faturamento) / toFloat64(ranked.qtd), 0) AS valor_unitario_medio
        FROM (
            SELECT id_produto, nome_produto, nome_grupo,
                   sum(faturamento) AS faturamento, sum(qtd) AS qtd, sum(margem) AS margem,
                   sum(custo_total) AS custo_total
            FROM {MART_RT_DB}.sales_products_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY id_produto, nome_produto, nome_grupo
        ) AS ranked
        LEFT JOIN ({product_meta_sql}) AS meta
            ON meta.id_empresa = {{id_empresa:Int32}} AND meta.id_produto = ranked.id_produto
        ORDER BY ranked.faturamento DESC
        LIMIT 20
    """, parameters=params)

    # --- Top groups ---
    top_groups = query_dict(f"""
        SELECT id_grupo_produto, nome_grupo AS grupo_nome,
               s_fat AS faturamento, s_margem AS margem, s_itens AS qtd_itens
        FROM (
            SELECT id_grupo_produto, nome_grupo,
                   sum(faturamento) AS s_fat, sum(margem) AS s_margem, sum(qtd_itens) AS s_itens
            FROM {MART_RT_DB}.sales_groups_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY id_grupo_produto, nome_grupo
        ) ORDER BY faturamento DESC LIMIT 20
    """, parameters=params)

    # --- Monthly evolution ---
    monthly_rows = query_dict(f"""
        SELECT ano, mes, s_fat AS faturamento, s_vendas AS qtd_vendas,
               s_val_cancel AS valor_cancelado
        FROM (
            SELECT toYear(dt) AS ano, toMonth(dt) AS mes,
                   sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas,
                   sum(valor_cancelado) AS s_val_cancel
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {filial}
            GROUP BY ano, mes
        ) ORDER BY ano, mes
    """, parameters=params)
    monthly_evolution = [
        {
            "ano": int(r["ano"]),
            "mes": int(r["mes"]),
            "month_ref": f"{int(r['ano'])}-{int(r['mes']):02d}-01",
            "saidas": float(r.get("faturamento") or 0),
            "entradas": 0,
            "cancelamentos": float(r.get("valor_cancelado") or 0),
            "faturamento": float(r.get("faturamento") or 0),
            "qtd_vendas": int(r.get("qtd_vendas") or 0),
        }
        for r in monthly_rows
    ]

    # --- Annual comparison ---
    current_year = dt_fim.year
    prev_year = current_year - 1
    annual_current = {m["mes"]: m for m in monthly_evolution if m["ano"] == current_year}
    annual_prev = {m["mes"]: m for m in monthly_evolution if m["ano"] == prev_year}
    annual_comparison = {
        "current_year": current_year,
        "previous_year": prev_year,
        "months": [
            {
                "mes": mes,
                "saidas_atual": annual_current.get(mes, {}).get("saidas", 0),
                "saidas_anterior": annual_prev.get(mes, {}).get("saidas", 0),
                "entradas_atual": 0,
                "entradas_anterior": 0,
                "cancelamentos_atual": annual_current.get(mes, {}).get("cancelamentos", 0),
                "cancelamentos_anterior": annual_prev.get(mes, {}).get("cancelamentos", 0),
                "month_ref_atual": f"{current_year}-{mes:02d}-01",
                "month_ref_anterior": f"{prev_year}-{mes:02d}-01",
            }
            for mes in range(1, 13)
        ],
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    freshness_meta = {"mode": "realtime", "source": source, "last_refresh": now_iso}

    return {
        "kpis": kpis,
        "series": {},
        "ranking": top_products[:10],
        "filters": None,
        "commercial_kpis": commercial_kpis,
        "cfop_breakdown": cfop_breakdown,
        "commercial_by_hour": commercial_by_hour,
        "by_day": by_day,
        "by_hour": by_hour,
        "top_products": top_products,
        "top_groups": top_groups,
        "top_employees": [],
        "monthly_evolution": monthly_evolution,
        "annual_comparison": annual_comparison,
        "stats": {"vendas": qtd_vendas},
        "reading_status": "realtime_mart_snapshot",
        "operational_sync": {"source": "realtime", "last_publish": now_iso},
        "freshness": freshness_meta,
        "source": "realtime",
        "realtime_source": source,
    }


def sales_by_hour(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Hourly sales breakdown."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    return query_dict(f"""
        SELECT hora, sum(faturamento) AS faturamento, sum(qtd_vendas) AS qtd_vendas, sum(qtd_itens) AS qtd_itens
        FROM {MART_RT_DB}.sales_hourly_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY hora ORDER BY hora
    """, parameters={"id_empresa": id_empresa})


def sales_top_products(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 15,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Top products by revenue."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    product_meta_sql = _sales_product_meta_subquery()

    return query_dict(f"""
        SELECT
            ranked.id_produto,
            ranked.nome_produto,
            ranked.nome_produto AS produto_nome,
            ranked.nome_grupo,
            ranked.nome_grupo AS grupo_nome,
            meta.unidade AS unidade,
            {_sales_quantity_kind_sql('ranked.nome_produto', 'ranked.nome_grupo')} AS quantity_kind,
            ranked.faturamento,
            ranked.qtd,
            ranked.margem
        FROM (
            SELECT id_produto, nome_produto, nome_grupo,
                   sum(faturamento) AS faturamento, sum(qtd) AS qtd,
                   sum(margem) AS margem
            FROM {MART_RT_DB}.sales_products_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
            GROUP BY id_produto, nome_produto, nome_grupo
        ) AS ranked
        LEFT JOIN ({product_meta_sql}) AS meta
            ON meta.id_empresa = {{id_empresa:Int32}} AND meta.id_produto = ranked.id_produto
        ORDER BY ranked.faturamento DESC
        LIMIT {limit}
    """, parameters={"id_empresa": id_empresa})


def sales_top_groups(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 10,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Top product groups by revenue."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    return query_dict(f"""
        SELECT id_grupo_produto, nome_grupo,
               sum(faturamento) AS faturamento, sum(qtd_itens) AS qtd_itens,
               sum(margem) AS margem
        FROM {MART_RT_DB}.sales_groups_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY id_grupo_produto, nome_grupo
        ORDER BY faturamento DESC
        LIMIT {limit}
    """, parameters={"id_empresa": id_empresa})


# ================================================================
# CURVA ABC DE PRODUTOS
# ================================================================

def sales_abc_curve(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    *,
    sort_by: str = "faturamento",
    threshold_a: int = 80,
    threshold_b: int = 95,
    exclude_fuel: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """ABC curve analysis for products sold in period.

    Returns executive summary, chart data, full ranking and auto-insights.
    Classification is computed at query time using window functions so it
    adapts to whatever date/branch filter the user selects.
    """
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    product_meta_sql = _sales_product_meta_subquery()

    # Fuel exclusion filter
    fuel_filter = ""
    if exclude_fuel:
        fuel_filter = """
                AND NOT (lower(nome_grupo) LIKE '%combusti%' OR lower(nome_grupo) LIKE '%gasolina%' OR lower(nome_grupo) LIKE '%diesel%' OR lower(nome_grupo) LIKE '%etanol%' OR lower(nome_grupo) LIKE '%gnv%' OR lower(nome_grupo) LIKE '%gas natural%' OR lower(nome_grupo) LIKE '%alcool%')"""

    # Sort column for ORDER BY
    sort_col_map = {"faturamento": "fat", "quantidade": "qty", "lucro": "mrg"}
    sort_col = sort_col_map.get(sort_by, "fat")

    # Query with ranking, cumulative % and ABC classification
    rows = query_dict(f"""
        SELECT
            ranked.id_produto,
            ranked.nome_produto,
            ranked.nome_grupo,
            meta.unidade AS unidade,
            {_sales_quantity_kind_sql('ranked.nome_produto', 'ranked.nome_grupo')} AS quantity_kind,
            ranked.fat AS faturamento,
            ranked.qty AS qtd,
            ranked.cost AS custo_total,
            ranked.mrg AS margem,
            ranked.avg_price AS valor_unitario_medio,
            ranked.participacao_pct,
            ranked.acumulado_pct,
            multiIf(
                ranked.acumulado_pct <= {threshold_a}, 'A',
                ranked.acumulado_pct <= {threshold_b}, 'B',
                'C'
            ) AS classe_abc,
            ranked.posicao
        FROM (
            SELECT
                *,
                row_number() OVER (ORDER BY {sort_col} DESC, id_produto ASC) AS posicao,
                toFloat64({sort_col}) / nullIf(toFloat64(sum({sort_col}) OVER ()), 0) * 100 AS participacao_pct,
                toFloat64(sum({sort_col}) OVER (ORDER BY {sort_col} DESC, id_produto ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
                    / nullIf(toFloat64(sum({sort_col}) OVER ()), 0) * 100 AS acumulado_pct
            FROM (
                SELECT
                    id_produto,
                    nome_produto,
                    nome_grupo,
                    toFloat64(sum(faturamento)) AS fat,
                    toFloat64(sum(qtd)) AS qty,
                    toFloat64(sum(custo_total)) AS cost,
                    toFloat64(sum(faturamento)) - toFloat64(sum(custo_total)) AS mrg,
                    if(sum(qtd) > 0, toFloat64(sum(faturamento)) / toFloat64(sum(qtd)), 0) AS avg_price
                FROM {MART_RT_DB}.sales_products_rt FINAL
                WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}{fuel_filter}
                GROUP BY id_produto, nome_produto, nome_grupo
                HAVING sum(faturamento) > 0
            )
        ) AS ranked
        LEFT JOIN ({product_meta_sql}) AS meta
            ON meta.id_empresa = {{id_empresa:Int32}} AND meta.id_produto = ranked.id_produto
        ORDER BY ranked.posicao ASC
    """, parameters={"id_empresa": id_empresa})

    if not rows:
        return _abc_empty_response()

    # Build response sections
    total_faturamento = sum(float(r.get("faturamento") or 0) for r in rows)
    class_a = [r for r in rows if r.get("classe_abc") == "A"]
    class_b = [r for r in rows if r.get("classe_abc") == "B"]
    class_c = [r for r in rows if r.get("classe_abc") == "C"]

    fat_a = sum(float(r.get("faturamento") or 0) for r in class_a)
    fat_b = sum(float(r.get("faturamento") or 0) for r in class_b)
    fat_c = sum(float(r.get("faturamento") or 0) for r in class_c)

    pct_a = (fat_a / total_faturamento * 100) if total_faturamento > 0 else 0
    pct_b = (fat_b / total_faturamento * 100) if total_faturamento > 0 else 0
    pct_c = (fat_c / total_faturamento * 100) if total_faturamento > 0 else 0

    leader = rows[0] if rows else {}
    leader_pct = float(leader.get("participacao_pct") or 0)

    # Concentration insight
    top5_pct = sum(float(r.get("participacao_pct") or 0) for r in rows[:5])
    if top5_pct >= 70:
        concentration = "high"
        concentration_text = f"Alta concentração: 5 produtos representam {top5_pct:.1f}% do faturamento."
    elif len(class_c) > 50 and pct_c < 10:
        concentration = "dispersed"
        concentration_text = f"Mix pulverizado: Classe C tem {len(class_c)} produtos com apenas {pct_c:.1f}% do faturamento."
    else:
        concentration = "healthy"
        concentration_text = "Concentração saudável do portfólio de produtos."

    # Executive summary
    summary = {
        "total_produtos": len(rows),
        "total_faturamento": total_faturamento,
        "classe_a_count": len(class_a),
        "classe_a_pct": round(pct_a, 1),
        "classe_b_count": len(class_b),
        "classe_b_pct": round(pct_b, 1),
        "classe_c_count": len(class_c),
        "classe_c_pct": round(pct_c, 1),
        "produto_lider": leader.get("nome_produto") or "",
        "produto_lider_pct": round(leader_pct, 1),
        "produto_lider_faturamento": float(leader.get("faturamento") or 0),
        "concentration": concentration,
        "concentration_text": concentration_text,
    }

    # Chart data (top 40 for desktop, frontend truncates for mobile)
    chart_data = [
        {
            "posicao": int(r.get("posicao") or 0),
            "nome_produto": r.get("nome_produto") or "",
            "nome_grupo": r.get("nome_grupo") or "",
            "faturamento": float(r.get("faturamento") or 0),
            "qtd": float(r.get("qtd") or 0),
            "valor_unitario_medio": float(r.get("valor_unitario_medio") or 0),
            "participacao_pct": round(float(r.get("participacao_pct") or 0), 2),
            "acumulado_pct": round(float(r.get("acumulado_pct") or 0), 2),
            "classe_abc": r.get("classe_abc") or "C",
        }
        for r in rows[:40]
    ]

    # Full ranking for table
    ranking = [
        {
            "posicao": int(r.get("posicao") or 0),
            "id_produto": r.get("id_produto"),
            "nome_produto": r.get("nome_produto") or "",
            "nome_grupo": r.get("nome_grupo") or "",
            "unidade": r.get("unidade") or "",
            "quantity_kind": r.get("quantity_kind") or "unit",
            "qtd": float(r.get("qtd") or 0),
            "faturamento": float(r.get("faturamento") or 0),
            "custo_total": float(r.get("custo_total") or 0),
            "margem": float(r.get("margem") or 0),
            "valor_unitario_medio": float(r.get("valor_unitario_medio") or 0),
            "participacao_pct": round(float(r.get("participacao_pct") or 0), 2),
            "acumulado_pct": round(float(r.get("acumulado_pct") or 0), 2),
            "classe_abc": r.get("classe_abc") or "C",
        }
        for r in rows
    ]

    # Auto-generated insights
    insights = _abc_build_insights(
        rows=rows,
        class_a=class_a,
        class_b=class_b,
        class_c=class_c,
        pct_a=pct_a,
        pct_b=pct_b,
        pct_c=pct_c,
        leader=leader,
        leader_pct=leader_pct,
        top5_pct=top5_pct,
        total_faturamento=total_faturamento,
    )

    return {
        "summary": summary,
        "chart_data": chart_data,
        "ranking": ranking,
        "insights": insights,
        "thresholds": {"a": threshold_a, "b": threshold_b, "c": 100},
        "sort_by": sort_by,
        "source": "realtime",
    }


def _abc_empty_response() -> Dict[str, Any]:
    return {
        "summary": {
            "total_produtos": 0,
            "total_faturamento": 0,
            "classe_a_count": 0,
            "classe_a_pct": 0,
            "classe_b_count": 0,
            "classe_b_pct": 0,
            "classe_c_count": 0,
            "classe_c_pct": 0,
            "produto_lider": "",
            "produto_lider_pct": 0,
            "produto_lider_faturamento": 0,
            "concentration": "empty",
            "concentration_text": "",
        },
        "chart_data": [],
        "ranking": [],
        "insights": [],
        "thresholds": {"a": 80, "b": 95, "c": 100},
        "source": "realtime",
        "empty": True,
    }


def _abc_build_insights(
    *,
    rows: List[Dict[str, Any]],
    class_a: List[Dict[str, Any]],
    class_b: List[Dict[str, Any]],
    class_c: List[Dict[str, Any]],
    pct_a: float,
    pct_b: float,
    pct_c: float,
    leader: Dict[str, Any],
    leader_pct: float,
    top5_pct: float,
    total_faturamento: float,
) -> List[Dict[str, str]]:
    """Generate deterministic insights from ABC data."""
    insights: List[Dict[str, str]] = []

    # Leader insight
    leader_name = leader.get("nome_produto") or "Produto"
    if leader_pct >= 20:
        insights.append({
            "type": "leader",
            "text": f"O produto líder é {leader_name}, com {leader_pct:.1f}% do faturamento do período.",
        })

    # Class A concentration
    if class_a:
        insights.append({
            "type": "class_a",
            "text": f"Classe A concentra {pct_a:.1f}% do faturamento com apenas {len(class_a)} produto{'s' if len(class_a) != 1 else ''}.",
        })

    # Top 5 dependency
    if top5_pct >= 70:
        insights.append({
            "type": "dependency",
            "text": f"Alta dependência: os 5 principais produtos representam {top5_pct:.1f}% do faturamento.",
        })

    # Class C review
    if len(class_c) > 20 and pct_c < 10:
        insights.append({
            "type": "class_c",
            "text": f"Classe C possui {len(class_c)} produtos, mas soma apenas {pct_c:.1f}% do faturamento.",
        })

    # Class B opportunity
    if class_b and pct_b >= 10:
        insights.append({
            "type": "opportunity",
            "text": "A Classe B pode indicar oportunidades comerciais para ampliar participação.",
        })

    # Review suggestion
    if len(class_c) > 50:
        insights.append({
            "type": "review",
            "text": "Revise produtos Classe C com baixo impacto, principalmente se ocupam espaço operacional.",
        })

    return insights


# ================================================================
# PAYMENTS
# ================================================================

def payments_overview(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    anomaly_limit: int = 20,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Payments overview with breakdown by type."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    by_type_raw = query_dict(f"""
        SELECT tipo_forma, label, category,
               sum(valor_total) AS valor_total, sum(qtd_transacoes) AS qtd_transacoes
        FROM {MART_RT_DB}.payments_by_type_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY tipo_forma, label, category
        ORDER BY valor_total DESC
    """, parameters={"id_empresa": id_empresa})

    mix = [
        {
            "tipo_forma": r.get("tipo_forma"),
            "label": r.get("label"),
            "category": r.get("category"),
            "category_label": r.get("label") or r.get("category"),
            "total_valor": round(float(r.get("valor_total") or 0), 2),
            "qtd_comprovantes": int(r.get("qtd_transacoes") or 0),
        }
        for r in by_type_raw
    ]
    total_val = round(sum(r["total_valor"] for r in mix), 2)
    source_status = "ok" if mix else "unavailable"
    return {
        "kpis": {
            "total_valor": total_val,
            "source_status": source_status,
            "mix": mix,
            "source": "realtime",
        },
        "by_day": [],
        "by_turno": [],
        "anomalies": [],
        "source": "realtime",
        "realtime_source": _realtime_source(),
    }


# ================================================================
# CASH / CAIXA
# ================================================================

def _enrich_open_turno(
    t: Dict[str, Any],
    filial_names: Dict[int, str],
    turno_values: Dict[tuple[int, int], str],
) -> Dict[str, Any]:
    """Add frontend-expected fields to turno data."""
    from datetime import datetime, timezone

    fat = float(t.get("total_vendas") if t.get("total_vendas") is not None else (t.get("faturamento_turno") or 0))
    total_cancelamentos = round(float(t.get("total_cancelamentos") or 0), 2)
    total_pagamentos = round(float(t.get("total_pagamentos") if t.get("total_pagamentos") is not None else fat), 2)
    saldo_comercial = round(float(t.get("saldo_comercial") if t.get("saldo_comercial") is not None else (fat - total_cancelamentos)), 2)
    abertura = t.get("abertura_ts")
    id_filial = int(t["id_filial"]) if t.get("id_filial") is not None else None
    id_turno = int(t["id_turno"]) if t.get("id_turno") is not None else None
    filial_nome = filial_names.get(id_filial or -1)
    turno_value = (
        turno_values.get((id_filial, id_turno))
        if id_filial is not None and id_turno is not None
        else None
    )
    usuario_nome = str(t.get("nome_operador") or "").strip()
    horas_aberto = None
    if abertura:
        try:
            ts = abertura if isinstance(abertura, datetime) else datetime.fromisoformat(str(abertura))
            horas_aberto = round((datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600, 1)
        except Exception:
            pass
    return {
        **t,
        "filial_nome": filial_nome,
        "filial_label": _filial_label(id_filial, filial_nome),
        "turno_value": turno_value,
        "turno_label": _turno_label(turno_value, id_turno),
        "usuario_nome": usuario_nome,
        "usuario_label": _cash_operator_label(usuario_nome, t.get("id_usuario")),
        "total_vendas": fat,
        "total_cancelamentos": total_cancelamentos,
        "total_pagamentos": total_pagamentos,
        "saldo_comercial": saldo_comercial,
        "horas_aberto": horas_aberto,
    }


def cash_overview(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Cash/shift overview with commercial KPIs."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim) if dt_ini and dt_fim else ""
    sales_filial = _branch_clause("c.id_filial", id_filial)
    sales_date_range = _date_range_filter(dt_ini, dt_fim, "c.data_key") if dt_ini and dt_fim else ""
    params = {"id_empresa": id_empresa}

    # Open shifts
    turnos_raw = query_dict(f"""
        SELECT id_filial, id_turno, id_usuario, nome_operador,
               abertura_ts, fechamento_ts, is_aberto,
               faturamento_turno, qtd_vendas_turno
        FROM {MART_RT_DB}.cash_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
          AND is_aberto = 1
                    AND id_turno > 0
        ORDER BY abertura_ts DESC
        LIMIT 50
    """, parameters=params)

    # Top commercial turnos in the requested period.
    all_turnos_raw = query_dict(f"""
        WITH turn_sales AS (
            SELECT
                c.id_filial AS id_filial,
                c.id_turno AS id_turno,
                min(c.dt_evento_local) AS first_event_at,
                max(c.dt_evento_local) AS last_event_at,
                round(sumIf(c.valor_total, c.cancelado = 0), 2) AS total_vendas,
                toUInt32(uniqExactIf(tuple(c.id_empresa, c.id_filial, c.id_db, c.id_comprovante), c.cancelado = 0)) AS qtd_vendas,
                round(sumIf(c.valor_total, c.cancelado = 1), 2) AS total_cancelamentos,
                toUInt32(uniqExactIf(tuple(c.id_empresa, c.id_filial, c.id_db, c.id_comprovante), c.cancelado = 1)) AS qtd_cancelamentos
            FROM {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
            WHERE c.id_empresa = {{id_empresa:Int32}} {sales_date_range} {sales_filial}
              AND c.is_deleted = 0
              AND c.id_turno > 0
            GROUP BY c.id_filial, c.id_turno
        ), turn_meta AS (
            SELECT
                id_filial,
                id_turno,
                id_usuario,
                nome_operador,
                abertura_ts,
                fechamento_ts,
                is_aberto
            FROM {MART_RT_DB}.cash_overview_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {filial}
              AND id_turno > 0
        )
        SELECT
            s.id_filial,
            s.id_turno,
            m.id_usuario,
            m.nome_operador,
            coalesce(m.abertura_ts, s.first_event_at) AS abertura_ts,
            coalesce(m.fechamento_ts, s.last_event_at) AS fechamento_ts,
            coalesce(m.is_aberto, toUInt8(0)) AS is_aberto,
            s.first_event_at,
            s.last_event_at,
            s.total_vendas,
            s.qtd_vendas,
            s.total_cancelamentos,
            s.qtd_cancelamentos,
            coalesce(pay.total_pagamentos, 0) AS total_pagamentos,
            round(s.total_vendas - s.total_cancelamentos, 2) AS saldo_comercial
        FROM turn_sales AS s
                LEFT JOIN (
                        SELECT
                        c.id_filial AS id_filial,
                        c.id_turno AS id_turno,
                                round(sum(fp.valor), 2) AS total_pagamentos
                        FROM {CURRENT_DB}.stg_formas_pgto_slim AS fp FINAL
                        INNER JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
                            ON c.id_empresa = fp.id_empresa
                         AND c.id_filial = fp.id_filial
                         AND c.referencia = fp.id_referencia
                        WHERE c.id_empresa = {{id_empresa:Int32}} {sales_date_range} {sales_filial}
                            AND c.is_deleted = 0
                            AND fp.is_deleted = 0
                            AND c.id_turno > 0
                        GROUP BY c.id_filial, c.id_turno
                ) AS pay
          ON pay.id_filial = s.id_filial
         AND pay.id_turno = s.id_turno
        LEFT JOIN turn_meta AS m
          ON m.id_filial = s.id_filial
         AND m.id_turno = s.id_turno
        ORDER BY s.total_vendas DESC, s.qtd_vendas DESC, s.last_event_at DESC
        LIMIT 15
    """, parameters=params)

    label_source_rows = turnos_raw + all_turnos_raw
    filial_names = _load_current_filial_names(id_empresa, label_source_rows)
    turno_values = _load_current_turno_values(id_empresa, label_source_rows)

    turnos = [_enrich_open_turno(t, filial_names, turno_values) for t in turnos_raw]
    all_turnos = [_enrich_open_turno(t, filial_names, turno_values) for t in all_turnos_raw]

    # Commercial KPIs from sales_daily_rt
    sales_rows = query_dict(f"""
        SELECT s_fat AS total_vendas, s_cancel AS total_cancelamentos,
               s_vendas AS qtd_vendas, s_cancel_qtd AS qtd_cancelamentos
        FROM (
            SELECT sum(faturamento) AS s_fat, sum(valor_cancelado) AS s_cancel,
                   sum(qtd_vendas) AS s_vendas, sum(qtd_canceladas) AS s_cancel_qtd
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        )
    """, parameters=params)
    sales_kpi = sales_rows[0] if sales_rows else {}

    # Payment breakdown from payments_by_type_rt
    payments_raw = query_dict(f"""
        SELECT label, category, sum(valor_total) AS valor_total, sum(qtd_transacoes) AS qtd_transacoes
        FROM {MART_RT_DB}.payments_by_type_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY label, category
        ORDER BY valor_total DESC
    """, parameters=params)

    payments = [
        {
            "label": p.get("label"),
            "category": p.get("category"),
            "total_valor": round(float(p.get("valor_total") or 0), 2),
            "qtd_comprovantes": int(p.get("qtd_transacoes") or 0),
        }
        for p in payments_raw
    ]

    total_vendas = float(sales_kpi.get("total_vendas") or 0)
    total_cancelamentos = float(sales_kpi.get("total_cancelamentos") or 0)
    total_pagamentos = sum(p["total_valor"] for p in payments)
    saldo_comercial = total_vendas - total_cancelamentos

    commercial_kpis = {
        "total_vendas": total_vendas,
        "total_cancelamentos": total_cancelamentos,
        "cancelamentos_periodo": total_cancelamentos,
        "total_pagamentos": total_pagamentos,
        "recebimentos_periodo": total_pagamentos,
        "saldo_comercial": saldo_comercial,
        "qtd_vendas": int(sales_kpi.get("qtd_vendas") or 0),
        "qtd_cancelamentos": int(sales_kpi.get("qtd_cancelamentos") or 0),
    }

    # Sales by day for the period
    by_day = query_dict(f"""
        SELECT dt, sum(faturamento) AS faturamento, sum(qtd_vendas) AS qtd_vendas
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY dt ORDER BY dt
    """, parameters=params)

    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "kpis": commercial_kpis,
        "series": {},
        "turnos": all_turnos,
        "turnos_abertos": turnos,
        "qtd_abertos": len(turnos),
        "commercial": {
            "kpis": commercial_kpis,
            "summary": f"Período com {int(sales_kpi.get('qtd_vendas') or 0)} vendas registradas.",
            "by_day": by_day,
            "top_turnos": all_turnos,
        },
        "historical": {
            "source_status": "available",
            "payment_mix": payments,
            "kpis": commercial_kpis,
            "top_turnos": all_turnos,
            "cancelamentos": [],
            "by_day": by_day,
        },
        "live_now": {
            "source_status": "available",
            "summary": f"{len(turnos)} caixa(s) aberto(s) no momento.",
            "kpis": {
                "caixas_abertos": len(turnos),
                "caixas_criticos": 0,
                "total_vendas_abertas": sum(float(t.get("faturamento_turno") or 0) for t in turnos),
            },
            "open_boxes": turnos[:10],
            "stale_boxes": [],
            "payment_mix": [],
            "cancelamentos": [],
            "alerts": [],
        },
        "dre_summary": {
            "cards": [
                {"key": "receita", "label": "Receita bruta", "amount": total_vendas, "detail": "Total faturado no período"},
                {"key": "cancelamentos", "label": "Cancelamentos", "amount": total_cancelamentos, "detail": "Devoluções e cancelamentos"},
                {"key": "recebimentos", "label": "Recebimentos", "amount": total_pagamentos, "detail": "Pagamentos recebidos"},
            ],
            "pending": [],
        },
        "payment_mix": payments,
        "payment_breakdown": payments,
        "inutilizacoes": _cash_nfe_inutilizations(id_empresa, id_filial, dt_ini, dt_fim),
        "source": "realtime",
        "realtime_source": _realtime_source(),
        "freshness": {"mode": "realtime", "source": _realtime_source(), "last_refresh": now_iso},
        "operational_sync": {"source": "realtime", "last_publish": now_iso},
    }


def _cash_nfe_inutilizations(
    id_empresa: int,
    id_filial: Any,
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
) -> Dict[str, Any]:
    """Query NFE inutilizations (status=5) for the cash page."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim) if dt_ini and dt_fim else ""

    try:
        exists = query_scalar(
            f"SELECT count() FROM system.tables WHERE database = '{MART_RT_DB}' AND name = 'nfe_inutilizations_rt'",
            parameters={},
        )
        if not exists:
            return {"qtd": 0, "valor_total": 0.0, "items": []}
    except Exception:
        return {"qtd": 0, "valor_total": 0.0, "items": []}

    summary_rows = query_dict(f"""
        SELECT
            count() AS qtd,
            sum(valor_comprovante) AS valor_total
        FROM {MART_RT_DB}.nfe_inutilizations_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} {date_range}
    """, parameters={"id_empresa": id_empresa})
    summary = summary_rows[0] if summary_rows else {"qtd": 0, "valor_total": 0}
    total_items = int(summary.get("qtd") or 0)
    total_value = round(float(summary.get("valor_total") or 0), 2)
    if total_items <= 0:
        return {"qtd": 0, "valor_total": 0.0, "items": []}

    rows = query_dict(f"""
        SELECT
            id_filial, filial_nome, id_turno,
            turno_abertura_ts, turno_fechamento_ts,
            id_usuario, nome_operador,
            id_comprovante, id_nfe, numero_nfe, serie_nfe,
            chave_nfe, protocolo, modelo_nfe, data_emissao_nfe,
            valor_comprovante, referencia, dt, hora
        FROM {MART_RT_DB}.nfe_inutilizations_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} {date_range}
        ORDER BY dt DESC, hora DESC
        LIMIT 100
    """, parameters={"id_empresa": id_empresa})

    filial_names = _load_current_filial_names(id_empresa, rows) if rows else {}
    turno_values = _load_current_turno_values(id_empresa, rows) if rows else {}

    items = []
    for row in rows:
        fid = int(row.get("id_filial") or 0)
        tid = int(row.get("id_turno") or 0)
        filial_nome = filial_names.get(fid) or str(row.get("filial_nome") or "").strip()
        turno_value = turno_values.get((fid, tid))
        usuario_nome = str(row.get("nome_operador") or "").strip()
        items.append({
            "id_filial": fid,
            "filial_label": _filial_label(fid, filial_nome),
            "id_turno": tid,
            "turno_label": _turno_label(turno_value, tid),
            "turno_abertura_ts": str(row.get("turno_abertura_ts") or ""),
            "turno_fechamento_ts": str(row.get("turno_fechamento_ts") or ""),
            "usuario_label": _cash_operator_label(usuario_nome, row.get("id_usuario")),
            "id_comprovante": row.get("id_comprovante"),
            "id_nfe": row.get("id_nfe"),
            "numero_nfe": str(row.get("numero_nfe") or ""),
            "serie_nfe": str(row.get("serie_nfe") or ""),
            "chave_nfe": str(row.get("chave_nfe") or ""),
            "protocolo": str(row.get("protocolo") or ""),
            "modelo_nfe": str(row.get("modelo_nfe") or ""),
            "data_emissao_nfe": str(row.get("data_emissao_nfe") or ""),
            "valor_comprovante": round(float(row.get("valor_comprovante") or 0), 2),
            "referencia": str(row.get("referencia") or ""),
            "dt": str(row.get("dt") or ""),
            "hora": str(row.get("hora") or ""),
        })

    return {"qtd": total_items, "valor_total": total_value, "items": items}


def open_cash_monitor(
    role: str,
    id_empresa: int,
    id_filial: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Open cash shifts monitor."""
    filial = _branch_clause("id_filial", id_filial)

    turnos = query_dict(f"""
        SELECT id_filial, id_turno, id_usuario, nome_operador,
               abertura_ts, faturamento_turno, qtd_vendas_turno
        FROM {MART_RT_DB}.cash_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
          AND is_aberto = 1
                    AND id_turno > 0
        ORDER BY abertura_ts DESC
    """, parameters={"id_empresa": id_empresa})

    return {
        "turnos_abertos": turnos,
        "qtd_abertos": len(turnos),
        "source": "realtime",
        "realtime_source": _realtime_source(),
    }


# ================================================================
# FRAUD / RISK
# ================================================================

def fraud_kpis(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Operational fraud KPIs for the antifraud screen.

    The frontend contract expects cancellation totals under
    ``cancelamentos`` and ``valor_cancelado``. Keep the modeled-risk keys as
    compatibility metadata because some higher-level bundles still inspect them.
    """
    import math
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    rows = query_dict(f"""
        SELECT
            sum(qtd_eventos) AS qtd_eventos,
            sum(impacto_total) AS impacto_total,
            avg(score_medio) AS score_medio
        FROM {MART_RT_DB}.fraud_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
    """, parameters={"id_empresa": id_empresa})

    result = rows[0] if rows else {"qtd_eventos": 0, "impacto_total": 0, "score_medio": 0}
    # Sanitize NaN from avg() on empty sets
    for key in ("score_medio", "impacto_total", "qtd_eventos"):
        val = result.get(key)
        if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
            result[key] = 0
    qtd_eventos = int(result.get("qtd_eventos") or 0)
    impacto_total = float(result.get("impacto_total") or 0)
    result["qtd_eventos"] = qtd_eventos
    result["impacto_total"] = impacto_total
    result["cancelamentos"] = qtd_eventos
    result["valor_cancelado"] = impacto_total
    return result


def fraud_last_events(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 30,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Recent risk events with operator/employee names, shift and register."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    # Try enriched mart first (has id_turno, id_caixa, hora, filial_nome)
    try:
        rows = query_dict(f"""
            SELECT event_id AS id, id_filial, filial_nome, data_key, event_type, source,
                   id_turno, id_caixa, id_usuario, nome_operador,
                   id_funcionario, nome_funcionario, valor_total,
                   impacto_estimado, score_risco, score_level, reasons, hora,
                   dt
            FROM {MART_RT_DB}.mart_antifraude_eventos FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {filial} {date_range}
            ORDER BY event_id DESC
            LIMIT {limit}
        """, parameters={"id_empresa": id_empresa})
        if rows:
            for row in rows:
                # Build real datetime from dt (Date) + hora (UInt8)
                dt_val = row.get("dt")
                hora_val = row.get("hora", 0)
                if dt_val:
                    from datetime import datetime as _dt
                    if isinstance(dt_val, str):
                        row["data"] = f"{dt_val} {int(hora_val):02d}:00:00"
                    else:
                        row["data"] = _dt.combine(dt_val, _dt.min.time().replace(hour=int(hora_val))).isoformat()
                else:
                    row["data"] = None
                # Derive usuario_label / usuario_source
                nome_op = (row.get("nome_operador") or "").strip()
                id_usr = row.get("id_usuario", 0)
                if nome_op:
                    row["usuario_label"] = nome_op
                    row["usuario_source"] = "comprovante"
                elif id_usr:
                    row["usuario_label"] = f"Operador #{id_usr}"
                    row["usuario_source"] = "id_only"
                else:
                    row["usuario_label"] = "Operador não resolvido"
                    row["usuario_source"] = "unresolved"
                # Derive filial_label
                fn = (row.get("filial_nome") or "").strip()
                row["filial_label"] = fn if fn else f"Filial {row.get('id_filial', '?')}"
                # Derive turno_label
                id_turno = row.get("id_turno", 0)
                row["turno_label"] = f"Turno {id_turno}" if id_turno else ""
            return rows
    except Exception:
        pass

    # Fallback to legacy mart — enrich with filial name from dim_filial
    filial_r = _branch_clause("r.id_filial", id_filial)
    return query_dict(f"""
        SELECT r.id, r.id_filial,
               COALESCE(f.nome, concat('Filial ', toString(r.id_filial))) AS filial_nome,
               r.data_key, r.event_type, r.source,
               r.nome_operador, r.nome_funcionario, r.valor_total,
               r.impacto_estimado, r.score_risco, r.score_level, r.reasons
        FROM {MART_RT_DB}.risk_recent_events_rt AS r FINAL
        LEFT JOIN (
            SELECT id_filial, nome
            FROM {CURRENT_DB}.dim_filial FINAL
        ) AS f ON r.id_filial = f.id_filial
        WHERE r.id_empresa = {{id_empresa:Int32}} {filial_r} {date_range}
        ORDER BY r.id DESC
        LIMIT {limit}
    """, parameters={"id_empresa": id_empresa})


def fraud_series(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Fraud/cancellation daily series from fraud_daily_rt."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    return query_dict(f"""
        SELECT data_key, id_filial,
               sum(qtd_eventos) AS cancelamentos,
               sum(impacto_total) AS valor_cancelado
        FROM {MART_RT_DB}.fraud_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY data_key, id_filial
        ORDER BY data_key, id_filial
    """, parameters={"id_empresa": id_empresa})


def fraud_top_users(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 10,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Top users by cancellation volume from enriched mart_antifraude_eventos."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    rows = query_dict(f"""
        SELECT nome_operador AS usuario_nome,
               count() AS cancelamentos,
               sum(valor_total) AS valor_cancelado
        FROM {MART_RT_DB}.mart_antifraude_eventos FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
          AND event_type = 'cancelamento'
        GROUP BY nome_operador
        ORDER BY valor_cancelado DESC
        LIMIT {limit}
    """, parameters={"id_empresa": id_empresa})
    for row in rows:
        nome = (row.get("usuario_nome") or "").strip()
        row["usuario_label"] = nome if nome else "Operador não resolvido"
    return rows


# ================================================================
# FINANCE
# ================================================================

def finance_kpis(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Finance aging KPIs."""
    filial = _branch_clause("id_filial", id_filial)

    rows = query_dict(f"""
        SELECT
            tipo_titulo,
            faixa,
            sum(qtd_titulos) AS qtd_titulos,
            sum(valor_total) AS valor_total,
            sum(valor_em_aberto) AS valor_em_aberto
        FROM {MART_RT_DB}.finance_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
        GROUP BY tipo_titulo, faixa
    """, parameters={"id_empresa": id_empresa})

    return {"aging": rows, "source": "realtime", "realtime_source": _realtime_source()}


# ================================================================
# PLATFORM HEALTH (realtime streaming status)
# ================================================================

def streaming_health(id_empresa: int = 0, **kwargs: Any) -> Dict[str, Any]:
    """Platform health for streaming infrastructure."""
    # Source freshness
    freshness = query_dict(f"""
        SELECT domain, last_event_ts, lag_seconds, status
        FROM {MART_RT_DB}.source_freshness FINAL
        WHERE id_empresa = {{id_empresa:Int32}} OR id_empresa = 0
        ORDER BY domain
    """, parameters={"id_empresa": id_empresa})

    # CDC table state
    cdc_state = query_dict("""
        SELECT table_schema, table_name, id_empresa, events_total, last_event_at, last_op
        FROM torqmind_ops.cdc_table_state FINAL
        ORDER BY table_name
    """)

    # Recent errors
    errors = query_dict("""
        SELECT table_name, error_type, error_message, created_at
        FROM torqmind_ops.cdc_errors
        ORDER BY created_at DESC
        LIMIT 10
    """)

    # Lag
    lag = query_dict("""
        SELECT topic, kafka_partition, lag, measured_at
        FROM torqmind_ops.cdc_lag
        ORDER BY measured_at DESC
        LIMIT 20
    """)

    # Mart publication
    publications = query_dict(f"""
        SELECT mart_name, max(published_at) AS last_published, sum(rows_written) AS total_rows
        FROM {MART_RT_DB}.mart_publication_log
        GROUP BY mart_name
        ORDER BY last_published DESC
    """)

    return {
        "source_freshness": freshness,
        "cdc_state": cdc_state,
        "recent_errors": errors,
        "lag": lag,
        "mart_publications": publications,
        "source": "realtime",
        "realtime_source": _realtime_source(),
    }


# ================================================================
# CUSTOMERS (paginated from mart_clientes_resumo)
# ================================================================

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
    """Paginated customer summary from mart_clientes_resumo."""
    filial = _branch_clause("id_filial", id_filial)
    params = {"id_empresa": id_empresa}

    allowed_sorts = {"total_compras_30d", "qtd_compras_30d", "ticket_medio_30d", "total_compras_all", "recencia_dias", "nome_cliente"}
    if sort_by not in allowed_sorts:
        sort_by = "total_compras_30d"
    if sort_order.upper() not in ("ASC", "DESC"):
        sort_order = "DESC"

    search_clause = ""
    if search:
        safe_search = search.replace("'", "\\'").replace("\\", "\\\\")
        search_clause = f" AND (positionCaseInsensitive(nome_cliente, '{safe_search}') > 0 OR positionCaseInsensitive(documento, '{safe_search}') > 0)"

    offset = (max(1, page) - 1) * page_size

    count_rows = query_dict(f"""
        SELECT count() AS total
        FROM {MART_RT_DB}.mart_clientes_resumo FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} {search_clause}
    """, parameters=params)
    total = int(count_rows[0]["total"]) if count_rows else 0

    rows = query_dict(f"""
        SELECT id_cliente, nome_cliente, documento, telefone, email,
               segmento, risk_level,
               total_compras_30d, qtd_compras_30d, ticket_medio_30d,
               total_compras_all, qtd_compras_all,
               ultima_compra_key, recencia_dias
        FROM {MART_RT_DB}.mart_clientes_resumo FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} {search_clause}
        ORDER BY {sort_by} {sort_order}
        LIMIT {int(page_size)} OFFSET {int(offset)}
    """, parameters=params)

    return {
        "items": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 0,
        "source": "realtime",
    }


# ================================================================
# Leaderboard / Goals (read from ClickHouse dim or PG mart via slim)
# ================================================================

def sales_top_employees(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    """Top employees by sales revenue from realtime mart (comprovantes_slim + dim_funcionario)."""
    branch = _branch_clause("s.id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            s.id_usuario,
            coalesce(nullIf(u.nome, ''), concat('Usuário #', toString(s.id_usuario))) AS funcionario_nome,
            sum(s.valor_total) AS faturamento,
            toDecimal64(0, 2) AS margem,
            toUInt32(count()) AS vendas
        FROM {CURRENT_DB}.stg_comprovantes_slim AS s
        LEFT JOIN {CURRENT_DB}.dim_usuario_caixa AS u FINAL
            ON s.id_empresa = u.id_empresa AND s.id_filial = u.id_filial AND s.id_usuario = u.id_usuario
        WHERE s.id_empresa = {{id_empresa:Int32}}
          AND s.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND s.cancelado = 0 AND s.is_deleted = 0
          AND s.commercial_eligible = 1
          AND s.id_usuario > 0
          {branch}
        GROUP BY s.id_usuario, u.nome
        ORDER BY faturamento DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "limit": int(limit),
    })
    return [
        {
            "id_funcionario": _to_int(row.get("id_usuario")),
            "funcionario_nome": row.get("funcionario_nome") or "",
            "faturamento": _to_float(row.get("faturamento")),
            "margem": _to_float(row.get("margem")),
            "vendas": _to_int(row.get("vendas")),
        }
        for row in rows
    ]


def leaderboard_employees(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 20) -> List[Dict[str, Any]]:
    if dt_fim < dt_ini:
        return []
    return sales_top_employees(role, id_empresa, id_filial, dt_ini, dt_fim, limit=limit)


def risk_top_employees(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 10) -> List[Dict[str, Any]]:
    """Top employees by fraud/risk events."""
    branch = _branch_clause("r.id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            r.nome_operador AS funcionario_nome,
            count() AS eventos,
            sum(r.impacto_estimado) AS impacto
        FROM {MART_RT_DB}.risk_recent_events_rt AS r FINAL
        WHERE r.id_empresa = {{id_empresa:Int32}}
          AND r.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND r.nome_operador != ''
          {branch}
        GROUP BY r.nome_operador
        ORDER BY impacto DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "limit": int(limit),
    })
    return [
        {
            "funcionario_nome": row.get("funcionario_nome") or "",
            "eventos": _to_int(row.get("eventos")),
            "impacto": _to_float(row.get("impacto")),
        }
        for row in rows
    ]


def goals_today(role: str, id_empresa: int, id_filial: Any, goal_date: date) -> List[Dict[str, Any]]:
    """Read current goals from ClickHouse goals table."""
    branch = _branch_clause("g.id_filial", id_filial)
    month_start = goal_date.replace(day=1)
    if goal_date.month == 12:
        month_end = goal_date.replace(year=goal_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        month_end = goal_date.replace(month=goal_date.month + 1, day=1) - timedelta(days=1)
    rows = query_dict(f"""
        SELECT
            g.goal_type,
            sum(g.target_value) AS target_value,
            count() AS goal_rows,
            min(g.goal_date) AS goal_month
        FROM {CURRENT_DB}.goals AS g FINAL
        WHERE g.id_empresa = {{id_empresa:Int32}}
          AND g.is_deleted = 0
          AND g.goal_date BETWEEN {{month_ini:Date}} AND {{month_end:Date}}
          {branch}
        GROUP BY g.goal_type
    """, parameters={
        "id_empresa": int(id_empresa),
        "month_ini": month_start.isoformat(),
        "month_end": month_end.isoformat(),
    })
    return [dict(row) for row in rows]


def monthly_goal_projection(role: str, id_empresa: int, id_filial: Any, as_of: Optional[date] = None) -> Dict[str, Any]:
    """Monthly goal projection using realtime data."""
    effective_as_of = as_of or business_today(id_empresa)
    month_start = effective_as_of.replace(day=1)
    if effective_as_of.month == 12:
        month_end = effective_as_of.replace(year=effective_as_of.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        month_end = effective_as_of.replace(month=effective_as_of.month + 1, day=1) - timedelta(days=1)

    goals = goals_today(role, id_empresa, id_filial, effective_as_of)
    faturamento_goal = next((g for g in goals if g.get("goal_type") == "faturamento"), None)
    target = _to_float(faturamento_goal.get("target_value")) if faturamento_goal else 0.0

    branch = _branch_clause("id_filial", id_filial)
    mtd = query_dict(f"""
        SELECT sum(faturamento) AS faturamento
        FROM {MART_RT_DB}.dashboard_home_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {branch}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(month_start),
        "fim": _date_key(effective_as_of),
    })
    realizado = _to_float(mtd[0].get("faturamento")) if mtd else 0.0

    elapsed_days = max((effective_as_of - month_start).days + 1, 1)
    total_days = max((month_end - month_start).days + 1, 1)
    daily_avg = realizado / elapsed_days if elapsed_days > 0 else 0.0
    projected = daily_avg * total_days
    pct = round(realizado / target * 100, 1) if target > 0 else 0.0

    status = "ahead" if target > 0 and projected >= target else "behind" if target > 0 else "no_goal"
    return {
        "month_ref": month_start.isoformat(),
        "month_label": month_start.strftime("%B %Y"),
        "requested_as_of": effective_as_of.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "requested_month_ref": month_start.isoformat(),
        "commercial_coverage": {},
        "business_clock": {},
        "goal": {"goal_type": "faturamento", "target_value": target},
        "status": status,
        "summary": {
            "realizado": round(realizado, 2),
            "target": round(target, 2),
            "pct_realizado": pct,
            "projected": round(projected, 2),
            "remaining": round(max(target - realizado, 0), 2),
            "elapsed_days": elapsed_days,
            "total_days": total_days,
            "daily_avg": round(daily_avg, 2),
        },
        "headline": f"{'%.1f' % pct}% da meta" if target > 0 else "Sem meta definida",
        "forecast": {"projected_eom": round(projected, 2), "confidence": "medium"},
        "drivers": [],
        "history": [],
        "series_mtd": [],
    }


# ================================================================
# INVENTORY (for analytics facade routing)
# ================================================================

REALTIME_FUNCTIONS = {
    "dashboard_kpis",
    "dashboard_series",
    "dashboard_home_bundle",
    "sales_overview_bundle",
    "sales_abc_curve",
    "sales_by_hour",
    "sales_top_products",
    "sales_top_groups",
    "sales_top_employees",
    "leaderboard_employees",
    "payments_overview",
    "cash_overview",
    "open_cash_monitor",
    "fraud_kpis",
    "fraud_series",
    "fraud_top_users",
    "fraud_last_events",
    "risk_top_employees",
    "finance_kpis",
    "streaming_health",
    "goals_today",
    "monthly_goal_projection",
}
