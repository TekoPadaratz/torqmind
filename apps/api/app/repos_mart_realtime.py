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
from app.repos_mart import (
    customers_delinquency_overview as _pg_customers_delinquency_overview,
)

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


def _key_to_iso(value: Any) -> Optional[str]:
    """Convert a YYYYMMDD integer data_key into an ISO date string."""
    key = _to_int(value)
    if key < 10000101:
        return None
    s = str(key)
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


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

    # --- Monthly evolution (histórico completo desde Jan/2025) ---
    monthly_rows = query_dict(f"""
        SELECT ano, mes, s_fat AS faturamento, s_vendas AS qtd_vendas,
               s_val_cancel AS valor_cancelado
        FROM (
            SELECT toYear(dt) AS ano, toMonth(dt) AS mes,
                   sum(faturamento) AS s_fat, sum(qtd_vendas) AS s_vendas,
                   sum(valor_cancelado) AS s_val_cancel
            FROM {MART_RT_DB}.sales_daily_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {filial}
              AND dt >= toDate('2025-01-01')
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

    # Comparativo anual: sempre Jan–Dez dos 2 anos (atual e anterior),
    # preenchendo zeros para meses sem movimento (ex.: Jan–Abr/2025).
    current_year = max(date.today().year, 2026)
    prev_year = current_year - 1
    if prev_year < 2025:
        prev_year = 2025
        current_year = 2026
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

    # Group multi-select filter (id_grupos): restringe a curva a grupos escolhidos.
    id_grupos = kwargs.get("id_grupos") or None
    group_filter = ""
    if id_grupos:
        _gvals = ", ".join(str(int(g)) for g in id_grupos if str(g).strip() not in ("", "None"))
        if _gvals:
            group_filter = f" AND id_grupo_produto IN ({_gvals})"

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
                WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}{fuel_filter}{group_filter}
                GROUP BY id_produto, nome_produto, nome_grupo
                HAVING sum(faturamento) > 0
            )
        ) AS ranked
        LEFT JOIN ({product_meta_sql}) AS meta
            ON meta.id_empresa = {{id_empresa:Int32}} AND meta.id_produto = ranked.id_produto
        ORDER BY ranked.posicao ASC
    """, parameters={"id_empresa": id_empresa})

    # Grupos disponiveis no periodo/escopo (respeita exclude_fuel) para o seletor.
    group_rows = query_dict(f"""
        SELECT id_grupo_produto,
               any(nome_grupo) AS grupo_nome,
               sum(faturamento) AS fat_total
        FROM {MART_RT_DB}.sales_products_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}{fuel_filter}
        GROUP BY id_grupo_produto
        HAVING fat_total > 0
        ORDER BY fat_total DESC
    """, parameters={"id_empresa": id_empresa})
    available_groups = [
        {
            "id_grupo_produto": int(g.get("id_grupo_produto") or 0),
            "grupo_nome": g.get("grupo_nome") or "(Sem grupo)",
            "faturamento": round(float(g.get("fat_total") or 0), 2),
        }
        for g in group_rows
        if g.get("id_grupo_produto") is not None
    ]
    selected_groups = [int(g) for g in id_grupos] if id_grupos else []

    if not rows:
        return _abc_empty_response(available_groups, selected_groups)

    # Determine the metric field for sort_by
    metric_field_map = {"faturamento": "faturamento", "quantidade": "qtd", "lucro": "margem"}
    metric_field = metric_field_map.get(sort_by, "faturamento")

    # Build response sections
    total_faturamento = sum(float(r.get("faturamento") or 0) for r in rows)
    total_metric = sum(float(r.get(metric_field) or 0) for r in rows)
    class_a = [r for r in rows if r.get("classe_abc") == "A"]
    class_b = [r for r in rows if r.get("classe_abc") == "B"]
    class_c = [r for r in rows if r.get("classe_abc") == "C"]

    metric_a = sum(float(r.get(metric_field) or 0) for r in class_a)
    metric_b = sum(float(r.get(metric_field) or 0) for r in class_b)
    metric_c = sum(float(r.get(metric_field) or 0) for r in class_c)

    pct_a = (metric_a / total_metric * 100) if total_metric > 0 else 0
    pct_b = (metric_b / total_metric * 100) if total_metric > 0 else 0
    pct_c = (metric_c / total_metric * 100) if total_metric > 0 else 0

    leader = rows[0] if rows else {}
    leader_pct = float(leader.get("participacao_pct") or 0)

    # Concentration insight
    metric_label = {"faturamento": "faturamento", "quantidade": "quantidade", "lucro": "lucro"}.get(sort_by, "faturamento")
    top5_pct = sum(float(r.get("participacao_pct") or 0) for r in rows[:5])
    if top5_pct >= 70:
        concentration = "high"
        concentration_text = f"Alta concentração: 5 produtos representam {top5_pct:.1f}% do {metric_label}."
    elif len(class_c) > 50 and pct_c < 10:
        concentration = "dispersed"
        concentration_text = f"Mix pulverizado: Classe C tem {len(class_c)} produtos com apenas {pct_c:.1f}% do {metric_label}."
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
            "margem": float(r.get("margem") or 0),
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
        "groups": available_groups,
        "selected_groups": selected_groups,
        "source": "realtime",
    }


def _abc_empty_response(
    groups: Optional[List[Dict[str, Any]]] = None,
    selected_groups: Optional[List[int]] = None,
) -> Dict[str, Any]:
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
        "groups": groups or [],
        "selected_groups": selected_groups or [],
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
    # Commercial sales total for the same scope, to reconcile the payment mix.
    sales_rows = query_dict(f"""
        SELECT round(sum(faturamento), 2) AS total_vendas
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
    """, parameters={"id_empresa": id_empresa})
    total_vendas = float((sales_rows[0] if sales_rows else {}).get("total_vendas") or 0)
    mix, total_pagamentos_conciliado, diferenca_conciliacao = _reconcile_payment_mix(mix, total_vendas)
    total_val = round(sum(r["total_valor"] for r in mix), 2)
    source_status = "ok" if mix else "unavailable"
    return {
        "kpis": {
            "total_valor": total_val,
            "total_vendas": total_vendas,
            "total_pagamentos_conciliado": total_pagamentos_conciliado,
            "diferenca_conciliacao": diferenca_conciliacao,
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


def _reconcile_payment_mix(
    payments: List[Dict[str, Any]],
    total_vendas: float,
) -> tuple[List[Dict[str, Any]], float, float]:
    """Reconcile recorded payment forms against commercial sales.

    The payment forms come from the deduplicated ``payments_by_type_rt`` mart
    (which already strips troca-de-forma ghosts on overpaid references). When
    their sum still diverges from commercial sales — unrecorded fiado/prazo,
    residual correction ghosts, or rounding — the gap is surfaced as an explicit,
    non-hidden ``reconciliation`` line so the mix total matches ``total_vendas``
    instead of silently mismatching. Nothing is bucketed into a real payment form.

    Returns ``(mix_with_reconciliation, total_pagamentos_conciliado, diferenca)``
    where ``total_pagamentos_conciliado`` is the sum of the *real* forms and
    ``diferenca = total_vendas - total_pagamentos_conciliado``.
    """
    total_pagamentos = round(sum(float(p.get("total_valor") or 0) for p in payments), 2)
    diferenca = round(float(total_vendas or 0) - total_pagamentos, 2)
    mix = list(payments)
    if abs(diferenca) > 0.01:
        mix.append({
            "tipo_forma": None,
            "label": "Não conciliado (operacional)",
            "category": "reconciliation",
            "category_label": "Não conciliado (operacional)",
            "total_valor": diferenca,
            "qtd_comprovantes": 0,
            "qtd_transacoes": 0,
            "is_reconciliation": True,
        })
    return mix, total_pagamentos, diferenca


def _split_avista_recebido(payments: List[Dict[str, Any]]) -> tuple[float, float, float]:
    """Separa o mix de pagamentos em (à vista, a prazo, cheque).

    À vista = dinheiro, cartão, pix e afins efetivamente recebidos no caixa.
    Exclui PRAZO (fiado), qualquer forma de CHEQUE e o resíduo não conciliado
    (fiado/ajustes não registrados). É o "faturado à vista = faturado - a prazo -
    cheque" pedido para a tela de Caixa (recebimento de venda do dia).
    """
    avista = 0.0
    a_prazo = 0.0
    cheque = 0.0
    for p in payments:
        valor = float(p.get("total_valor") or 0)
        category = str(p.get("category") or "").strip().upper()
        if category == "PRAZO":
            a_prazo += valor
        elif "CHEQUE" in category:
            cheque += valor
        elif category == "RECONCILIATION":
            # resíduo não conciliado (fiado/ajustes): não é recebimento à vista
            continue
        else:
            avista += valor
    return round(avista, 2), round(a_prazo, 2), round(cheque, 2)


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
            toFloat64(0) AS total_pagamentos,
            round(s.total_vendas - s.total_cancelamentos, 2) AS saldo_comercial
        FROM turn_sales AS s
        LEFT JOIN turn_meta AS m
          ON m.id_filial = s.id_filial
         AND m.id_turno = s.id_turno
        ORDER BY s.total_vendas DESC, s.qtd_vendas DESC, s.last_event_at DESC
        LIMIT 15
    """, parameters=params)

    # Pagamentos por turno deduplicando correcoes de forma no ERP origem. Quando a forma
    # de pagamento e alterada (ex DINHEIRO->PRAZO) o ERP insere uma nova linha sem marcar
    # a antiga is_deleted; como a chave do ReplacingMergeTree inclui tipo_forma, o FINAL
    # mantem as duas e o valor duplica. Removemos a duplicata SO quando a referencia esta
    # superpaga (sum > venda), preservando splits legitimos de mesmo valor.
    #
    # Executado como query top-level (nao como subquery aninhada num LEFT JOIN): o
    # ClickHouse apresenta resultado nao-deterministico (as vezes vazio) ao aninhar
    # window functions sobre CTEs com JOIN dentro de um LEFT JOIN externo. Isolada, a
    # query e deterministica. O merge com os turnos e feito em Python.
    pay_by_turno_raw = query_dict(f"""
        WITH docs AS (
            SELECT
                c.id_empresa AS id_empresa,
                c.id_filial AS id_filial,
                c.id_turno AS id_turno,
                c.referencia AS referencia,
                argMax(c.valor_total, c.source_ts_ms) AS venda,
                argMax(c.commercial_eligible, c.source_ts_ms) AS commercial_eligible
            FROM {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
            WHERE c.id_empresa = {{id_empresa:Int32}} {sales_date_range} {sales_filial}
              AND c.is_deleted = 0
              AND c.id_turno > 0
              AND c.referencia > 0
            GROUP BY c.id_empresa, c.id_filial, c.id_turno, c.referencia
        ),
        pay AS (
            SELECT
                fp.id_empresa AS id_empresa,
                fp.id_filial AS id_filial,
                docs.id_turno AS id_turno,
                fp.id_referencia AS id_referencia,
                fp.tipo_forma AS tipo_forma,
                fp.valor AS valor,
                fp.source_ts_ms AS source_ts_ms,
                docs.venda AS venda
            FROM {CURRENT_DB}.stg_formas_pgto_slim AS fp FINAL
            INNER JOIN docs
                ON docs.id_empresa = fp.id_empresa
               AND docs.id_filial = fp.id_filial
               AND docs.referencia = fp.id_referencia
            WHERE fp.is_deleted = 0
              AND docs.commercial_eligible = 1
        ),
        ranked AS (
            SELECT
                id_filial,
                id_turno,
                valor,
                venda,
                sum(valor) OVER (PARTITION BY id_empresa, id_filial, id_referencia) AS ref_pago,
                row_number() OVER (
                    PARTITION BY id_empresa, id_filial, id_referencia, valor
                    ORDER BY source_ts_ms DESC, tipo_forma DESC
                ) AS dup_rank
            FROM pay
        )
        SELECT
            id_filial,
            id_turno,
            round(sum(valor), 2) AS total_pagamentos
        FROM ranked
        WHERE NOT (ref_pago > venda + 0.01 AND dup_rank > 1)
        GROUP BY id_filial, id_turno
    """, parameters=params)
    pay_by_turno = {
        (int(r["id_filial"]), int(r["id_turno"])): float(r.get("total_pagamentos") or 0)
        for r in pay_by_turno_raw
        if r.get("id_filial") is not None and r.get("id_turno") is not None
    }
    for r in all_turnos_raw:
        if r.get("id_filial") is not None and r.get("id_turno") is not None:
            r["total_pagamentos"] = pay_by_turno.get(
                (int(r["id_filial"]), int(r["id_turno"])),
                r.get("total_pagamentos") or 0.0,
            )

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

    # Safety fallback: if mart payments exceed commercial sales, rebuild mix from slim with
    # per-reference deduplication against document total (or item total when available).
    fallback_payments_raw = []
    payments_total_mart = sum(float(p.get("valor_total") or 0) for p in payments_raw)
    sales_total = float(sales_kpi.get("total_vendas") or 0)
    if sales_total > 0 and payments_total_mart > sales_total + 0.01:
        payments_date_range = _date_range_filter(dt_ini, dt_fim, "p.data_key") if dt_ini and dt_fim else ""
        payments_filial = _branch_clause("p.id_filial", id_filial)
        docs_date_range = _date_range_filter(dt_ini, dt_fim, "c.data_key") if dt_ini and dt_fim else ""
        docs_filial = _branch_clause("c.id_filial", id_filial)
        items_date_range = _date_range_filter(dt_ini, dt_fim, "i.data_key") if dt_ini and dt_fim else ""
        items_filial = _branch_clause("i.id_filial", id_filial)

        fallback_payments_raw = query_dict(f"""
            WITH docs AS (
                SELECT
                    c.id_empresa,
                    c.id_filial,
                    c.data_key,
                    c.referencia,
                    concat(
                        toString(c.id_empresa), '|', toString(c.id_filial), '|',
                        toString(c.data_key), '|', toString(c.referencia)
                    ) AS ref_key,
                    argMax(c.commercial_eligible, c.source_ts_ms) AS commercial_eligible,
                    argMax(c.valor_total, c.source_ts_ms) AS venda_doc
                FROM {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
                WHERE c.id_empresa = {{id_empresa:Int32}} {docs_date_range} {docs_filial}
                  AND c.is_deleted = 0
                  AND c.referencia > 0
                GROUP BY c.id_empresa, c.id_filial, c.data_key, c.referencia
            ),
            overpaid_refs AS (
                SELECT
                    p.ref_key AS ref_key
                FROM (
                    SELECT
                        p.id_empresa,
                        p.id_filial,
                        p.data_key,
                        p.id_referencia,
                        concat(
                            toString(p.id_empresa), '|', toString(p.id_filial), '|',
                            toString(p.data_key), '|', toString(p.id_referencia)
                        ) AS ref_key,
                        sum(p.valor) AS total_pag_ref
                    FROM {CURRENT_DB}.stg_formas_pgto_slim AS p FINAL
                    INNER JOIN docs
                        ON docs.id_empresa = p.id_empresa
                       AND docs.id_filial = p.id_filial
                       AND docs.data_key = p.data_key
                       AND docs.referencia = p.id_referencia
                    WHERE p.id_empresa = {{id_empresa:Int32}} {payments_date_range} {payments_filial}
                      AND p.is_deleted = 0
                      AND docs.commercial_eligible = 1
                    GROUP BY p.id_empresa, p.id_filial, p.data_key, p.id_referencia
                ) AS p
                INNER JOIN docs AS d
                    ON d.ref_key = p.ref_key
                LEFT JOIN (
                    SELECT
                        concat(
                            toString(c.id_empresa), '|', toString(c.id_filial), '|',
                            toString(i.data_key), '|', toString(c.referencia)
                        ) AS ref_key,
                        sum(i.total) AS venda_itens
                    FROM {CURRENT_DB}.stg_itenscomprovantes_slim AS i FINAL
                    INNER JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
                        ON c.id_empresa = i.id_empresa
                       AND c.id_filial = i.id_filial
                       AND c.id_db = i.id_db
                       AND c.id_comprovante = i.id_comprovante
                    WHERE i.id_empresa = {{id_empresa:Int32}} {items_date_range} {items_filial}
                      AND i.is_deleted = 0
                      AND c.is_deleted = 0
                      AND c.commercial_eligible = 1
                      AND c.referencia > 0
                      AND i.cfop > 5000
                                        GROUP BY ref_key
                ) AS s
                                    ON s.ref_key = p.ref_key
                WHERE if(coalesce(s.venda_itens, 0) > 0, s.venda_itens, d.venda_doc) > 0
                  AND p.total_pag_ref > if(coalesce(s.venda_itens, 0) > 0, s.venda_itens, d.venda_doc) + 0.01
            ),
            pay_rows AS (
                SELECT
                    p.id_empresa,
                    p.id_filial,
                    p.data_key,
                    p.id_referencia,
                    p.tipo_forma,
                    p.valor,
                    p.source_ts_ms,
                    row_number() OVER (
                        PARTITION BY p.id_empresa, p.id_filial, p.data_key, p.id_referencia, p.valor
                        ORDER BY p.source_ts_ms DESC, p.tipo_forma DESC
                    ) AS dup_rank
                FROM {CURRENT_DB}.stg_formas_pgto_slim AS p FINAL
                INNER JOIN docs
                    ON docs.id_empresa = p.id_empresa
                   AND docs.id_filial = p.id_filial
                   AND docs.data_key = p.data_key
                   AND docs.referencia = p.id_referencia
                WHERE p.id_empresa = {{id_empresa:Int32}} {payments_date_range} {payments_filial}
                  AND p.is_deleted = 0
                  AND docs.commercial_eligible = 1
            )
            SELECT
                coalesce(m.label, concat('Forma ', toString(p.tipo_forma))) AS label,
                coalesce(m.category, 'Outros') AS category,
                sum(p.valor) AS valor_total,
                toUInt32(count()) AS qtd_transacoes
            FROM pay_rows AS p
            LEFT JOIN {CURRENT_DB}.payment_type_map AS m FINAL
                ON p.tipo_forma = m.tipo_forma
            WHERE NOT (
                (
                    concat(
                        toString(p.id_empresa), '|', toString(p.id_filial), '|',
                        toString(p.data_key), '|', toString(p.id_referencia)
                    ) IN (SELECT ref_key FROM overpaid_refs)
                )
                AND p.dup_rank > 1
            )
            GROUP BY label, category
            ORDER BY valor_total DESC
        """, parameters=params)

    if fallback_payments_raw:
        payments_raw = fallback_payments_raw

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
    saldo_comercial = total_vendas - total_cancelamentos

    # Reconcile recorded payment forms against commercial sales. Residual gaps
    # (unrecorded fiado/prazo, leftover troca-de-forma ghosts, rounding) become an
    # explicit non-hidden line so payment_mix totals match total_vendas.
    payments, total_pagamentos, diferenca_conciliacao = _reconcile_payment_mix(payments, total_vendas)
    recebimentos_avista, total_a_prazo, total_cheque = _split_avista_recebido(payments)

    commercial_kpis = {
        "total_vendas": total_vendas,
        "total_cancelamentos": total_cancelamentos,
        "cancelamentos_periodo": total_cancelamentos,
        "total_pagamentos": total_pagamentos,
        "total_pagamentos_conciliado": total_pagamentos,
        "diferenca_conciliacao": diferenca_conciliacao,
        "recebimentos_periodo": recebimentos_avista,
        "recebimentos_avista": recebimentos_avista,
        "total_a_prazo": total_a_prazo,
        "total_cheque": total_cheque,
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
                {"key": "recebimentos", "label": "Recebimentos à vista", "amount": recebimentos_avista, "detail": "Dinheiro, cartão e pix recebidos no caixa (exclui prazo e cheque)"},
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
    """Recent cancellation events with operator/employee names, shift and register.

    Excludes caixa geral (turno 0), unresolved shifts and non-cancellation events.
    """
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    try:
        lim = max(1, min(int(limit), 200))
    except (TypeError, ValueError):
        lim = 30
    # Over-fetch then filter unresolved/zero shifts so the visible grid still fills.
    fetch_lim = min(lim * 4, 400)

    try:
        rows = query_dict(f"""
            SELECT event_id, id_filial, filial_nome, data_key, dt, hora,
                   event_type, source, id_turno, turno_numero, id_caixa, id_usuario, nome_operador,
                   id_funcionario, nome_funcionario, valor_total, impacto_estimado,
                   score_risco, score_level, reasons, id_comprovante, nro_comprovante
            FROM {MART_RT_DB}.mart_antifraude_eventos FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {filial} {date_range}
              AND lower(event_type) IN ('cancelamento', 'cancelamento_seguido_venda')
              AND turno_numero >= 1
            ORDER BY data_key DESC, score_risco DESC, event_id DESC
            LIMIT {fetch_lim}
        """, parameters={"id_empresa": id_empresa})
        if rows:
            try:
                nfe_map = _load_nfe_numbers(id_empresa, rows)
            except Exception:
                nfe_map = {}
            out: List[Dict[str, Any]] = []
            for r in rows:
                r["numero_nfe"] = nfe_map.get(
                    (_to_int(r.get("id_filial")), _to_int(r.get("id_comprovante"))), ""
                )
                ev = _build_antifraude_event(r)
                # Defesa: só turnos operacionais 1..N, com documento e data
                if _to_int(ev.get("turno_numero")) < 1:
                    continue
                label_l = str(ev.get("turno_label") or "").lower()
                if (
                    not ev.get("turno_label")
                    or "não resolvido" in label_l
                    or "nao resolvido" in label_l
                    or "sem cadastro" in label_l
                    or label_l == "caixa geral"
                ):
                    continue
                if not ev.get("data"):
                    continue
                if not ev.get("documento_label") or str(ev.get("documento_label") or "").strip() in ("", "—", "-"):
                    if not ev.get("id_comprovante"):
                        continue
                out.append(ev)
                if len(out) >= lim:
                    break
            return out
    except Exception:
        pass

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
        LIMIT {lim}
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

    # Série do gráfico "Cancelamentos por dia": 1 ponto/dia (empresa),
    # independente de filial. Detalhe por filial fica nos painéis abaixo.
    return query_dict(f"""
        SELECT data_key,
               sum(qtd_eventos) AS cancelamentos,
               sum(impacto_total) AS valor_cancelado
        FROM {MART_RT_DB}.fraud_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY data_key
        ORDER BY data_key
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

    # Mesmo universo de "Últimos cancelamentos": só turnos operacionais 1..N
    # e tipos de cancelamento. Evita top_users contar caixa geral (turno 0)
    # que o grid de últimos exclui — divergência VR01 15/07/2026.
    rows = query_dict(f"""
        SELECT id_filial,
               any(filial_nome) AS filial_nome,
               nome_operador AS usuario_nome,
               count() AS cancelamentos,
               sum(valor_total) AS valor_cancelado
        FROM {MART_RT_DB}.mart_antifraude_eventos FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
          AND lower(event_type) IN ('cancelamento', 'cancelamento_seguido_venda')
          AND turno_numero >= 1
          AND (id_comprovante > 0 OR nro_comprovante > 0)
        GROUP BY id_filial, nome_operador
        HAVING length(trim(nome_operador)) > 0
        ORDER BY valor_cancelado DESC
        LIMIT {limit}
    """, parameters={"id_empresa": id_empresa})
    for row in rows:
        nome = (row.get("usuario_nome") or "").strip()
        row["usuario_label"] = nome if nome else "Operador não resolvido"
        fid = _to_int(row.get("id_filial"))
        row["filial_label"] = _filial_label(fid, (row.get("filial_nome") or "").strip()) if fid else "Filial sem cadastro"
    return rows


def _troca_documento_numero(documento: Any, referencia: Any = None, troca_id: Any = None) -> str:
    """Extrai só o número de NF/NFC-e do texto de documento da troca."""
    import re

    text = str(documento or "").strip()
    if text:
        m = re.search(r"(?i)(?:NFC-?e|NF-?e)\s*[#:]?\s*(\d+)", text)
        if m:
            return m.group(1)
        digits = re.findall(r"\d{4,}", text)
        if digits:
            return digits[-1]
    ref = _to_int(referencia)
    if ref > 0:
        return str(ref)
    return "—"


def _enrich_troca_venda_status(id_empresa: int, rows: List[Dict[str, Any]]) -> None:
    """Resolve Ativa/Cancelada via NFE (canônico) + comprovante.cancelado.

    ``MOVLCTOSCANCELADOS.REFERENCIA`` frequentemente NÃO é ``id_comprovante``
    vivo no slim (ex.: NFC-e 89477 → referencia 2754653 inexistente; comprovante
    real 3549682 com NFE status=4). Por isso o join só por referencia marca
    tudo como Ativa. Caminho correto: número NFC-e no documento + data da troca.
    """
    if not rows:
        return
    need: List[tuple[int, str, str]] = []  # filial, numero_nfe, dt ISO
    for r in rows:
        fil = int(r.get("id_filial") or 0)
        nf = _extract_nfce_number(str(r.get("documento") or ""), str(r.get("documento_raw") or ""))
        if not nf:
            nf = _extract_nfce_number(str(r.get("documento_numero") or ""))
        dt = str(r.get("dt") or "")[:10]
        if fil > 0 and nf:
            need.append((fil, nf, dt))
            r["_nfce_lookup"] = nf
        else:
            r["_nfce_lookup"] = ""

    status_by_key: Dict[tuple[int, str, str], Dict[str, Any]] = {}
    status_by_nf: Dict[tuple[int, str], Dict[str, Any]] = {}
    pairs = sorted({(f, n) for f, n, _ in need})
    if pairs:
        values = ", ".join(f"({f}, '{n}')" for f, n in pairs)
        try:
            found = query_dict(
                f"""
                SELECT
                    n.id_filial AS id_filial,
                    n.numero_nfe AS numero_nfe,
                    toString(toDate(n.data_emissao)) AS dt_emissao,
                    argMax(n.status, n.source_ts_ms) AS nfe_status,
                    argMax(n.id_comprovante, n.source_ts_ms) AS id_comprovante,
                    argMax(coalesce(c.cancelado, toUInt8(0)), n.source_ts_ms) AS cancelado,
                    argMax(coalesce(c.situacao, 0), n.source_ts_ms) AS situacao
                FROM {CURRENT_DB}.stg_nfe_slim AS n FINAL
                LEFT JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
                  ON c.id_empresa = n.id_empresa
                 AND c.id_filial = n.id_filial
                 AND c.id_comprovante = n.id_comprovante
                 AND c.is_deleted = 0
                WHERE n.id_empresa = {{id_empresa:Int32}}
                  AND n.is_deleted = 0
                  AND n.numero_nfe != ''
                  AND (n.id_filial, n.numero_nfe) IN ({values})
                GROUP BY n.id_filial, n.numero_nfe, dt_emissao
                """,
                parameters={"id_empresa": id_empresa},
            )
        except Exception:
            found = []
        for row in found:
            fil = int(row.get("id_filial") or 0)
            nf = str(row.get("numero_nfe") or "").strip()
            dt = str(row.get("dt_emissao") or "")[:10]
            info = {
                "nfe_status": int(row.get("nfe_status") or 0),
                "cancelado": int(row.get("cancelado") or 0),
                "situacao": int(row.get("situacao") or 0),
                "id_comprovante": int(row.get("id_comprovante") or 0),
            }
            if fil and nf and dt:
                status_by_key[(fil, nf, dt)] = info
            if fil and nf:
                # Prefer cancelada (status 4) when multiple emission dates exist.
                prev = status_by_nf.get((fil, nf))
                if prev is None or int(info["nfe_status"]) == 4 or (
                    int(prev.get("nfe_status") or 0) != 4 and int(info["cancelado"]) > int(prev.get("cancelado") or 0)
                ):
                    status_by_nf[(fil, nf)] = info

    # Fallback: comprovante direto por referencia (quando existir no slim).
    refs = sorted({
        (int(r.get("id_filial") or 0), int(r.get("referencia") or 0))
        for r in rows
        if int(r.get("id_filial") or 0) > 0 and int(r.get("referencia") or 0) > 0
    })
    comp_by_ref: Dict[tuple[int, int], Dict[str, Any]] = {}
    if refs:
        values = ", ".join(f"({f}, {c})" for f, c in refs)
        try:
            comps = query_dict(
                f"""
                SELECT id_filial, id_comprovante,
                       argMax(cancelado, source_ts_ms) AS cancelado,
                       argMax(situacao, source_ts_ms) AS situacao
                FROM {CURRENT_DB}.stg_comprovantes_slim FINAL
                WHERE id_empresa = {{id_empresa:Int32}}
                  AND is_deleted = 0
                  AND (id_filial, id_comprovante) IN ({values})
                GROUP BY id_filial, id_comprovante
                """,
                parameters={"id_empresa": id_empresa},
            )
        except Exception:
            comps = []
        for row in comps:
            comp_by_ref[(int(row["id_filial"]), int(row["id_comprovante"]))] = {
                "cancelado": int(row.get("cancelado") or 0),
                "situacao": int(row.get("situacao") or 0),
            }

    for r in rows:
        fil = int(r.get("id_filial") or 0)
        nf = str(r.get("_nfce_lookup") or "")
        dt = str(r.get("dt") or "")[:10]
        info = status_by_key.get((fil, nf, dt)) or status_by_nf.get((fil, nf))
        if not info:
            ref = int(r.get("referencia") or 0)
            info = comp_by_ref.get((fil, ref)) or {}
        nfe_status = int(info.get("nfe_status") or 0)
        cancelado = int(info.get("cancelado") or 0)
        situacao = int(info.get("situacao") or int(r.get("comprovante_situacao") or 0))
        # NFE status=4 cancelamento real; status=5 inutilização ≠ cancelamento comercial.
        # Comprovante: cancelado=1 (POS) ou situacao=3 (legado).
        if nfe_status == 4 or (cancelado == 1 and nfe_status != 5) or situacao == 3:
            status = "Cancelada"
        else:
            status = "Ativa"
        r["venda_status"] = status
        r["venda_cancelada"] = status == "Cancelada"
        r["nfe_status"] = nfe_status or None
        r["comprovante_cancelado"] = bool(cancelado)
        r.pop("_nfce_lookup", None)


def fraud_troca_forma_pgto(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    only_suspeita: bool = True,
    limit: int = 200,
    forma_nova: Optional[str] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Payment-form-change events (antifraud).

    Incomplete rows (mart lag without movlcto join) are omitted. Cancelled
    sales remain visible with ``venda_status`` = Cancelada (NFE status=4 ou
    comprovante.cancelado) — troca seguida de cancelamento é sinal crítico
    (ex.: NFC-e 89477 / VR05). Optional ``forma_nova``: ``cheque_pre`` | ``prazo`` | ``todos``.
    """
    filial = _branch_clause("t.id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim, col="t.data_key")
    suspeita = "AND t.is_suspeita = 1" if only_suspeita else ""
    forma = str(forma_nova or "todos").strip().lower()
    if forma in ("cheque_pre", "cheque-pre", "cheque"):
        forma_filter = "AND positionCaseInsensitive(t.forma_para, 'CHEQUE PRE') > 0"
    elif forma == "prazo":
        forma_filter = "AND positionCaseInsensitive(t.forma_para, 'PRAZO') > 0"
    else:
        forma_filter = ""
    try:
        lim = max(1, min(int(limit), 1000))
    except (TypeError, ValueError):
        lim = 200
    fetch_lim = min(lim * 3, 1500)

    rows = query_dict(f"""
        SELECT
            t.troca_id,
            t.id_filial,
            t.filial_nome,
            t.data_key,
            t.dt,
            t.documento,
            t.id_turno,
            t.id_usuario,
            t.nome_operador,
            t.forma_de,
            t.categoria_de,
            t.forma_para,
            t.categoria_para,
            t.valor,
            t.data_troca_ts,
            t.hora,
            t.is_suspeita,
            t.score_risco,
            t.reasons,
            t.referencia,
            coalesce(c.situacao, 0) AS comprovante_situacao,
            coalesce(c.cancelado, toUInt8(0)) AS comprovante_cancelado_slim
        FROM {MART_RT_DB}.mart_troca_forma_pgto_rt AS t FINAL
        LEFT JOIN {CURRENT_DB}.stg_comprovantes_slim AS c
          ON c.id_empresa = {{id_empresa:Int32}}
         AND c.id_filial = t.id_filial
         AND c.id_comprovante = t.referencia
         AND c.is_deleted = 0
        WHERE t.id_empresa = {{id_empresa:Int32}} {filial} {date_range} {suspeita} {forma_filter}
          AND t.valor > 0
          AND t.forma_de != ''
          AND t.forma_para != ''
        ORDER BY t.data_key DESC, t.troca_id DESC
        LIMIT {fetch_lim}
    """, parameters={"id_empresa": id_empresa})

    out: List[Dict[str, Any]] = []
    for r in rows:
        raw_doc = r.get("documento")
        doc_num = _troca_documento_numero(raw_doc, r.get("referencia"), r.get("troca_id"))
        r["documento_raw"] = raw_doc
        r["documento"] = doc_num
        r["documento_numero"] = doc_num
        out.append(r)
        if len(out) >= lim:
            break
    _enrich_troca_venda_status(id_empresa, out)
    return out


def fraud_troca_forma_pgto_kpis(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    forma_nova: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Period-wide totals for payment-form-change events (antifraud)."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    forma = str(forma_nova or "todos").strip().lower()
    if forma in ("cheque_pre", "cheque-pre", "cheque"):
        forma_filter = "AND positionCaseInsensitive(forma_para, 'CHEQUE PRE') > 0"
    elif forma == "prazo":
        forma_filter = "AND positionCaseInsensitive(forma_para, 'PRAZO') > 0"
    else:
        forma_filter = ""

    quality = f"""
          AND valor > 0
          AND forma_de != ''
          AND forma_para != ''
          {forma_filter}
    """

    rows = query_dict(f"""
        SELECT
            countIf(is_suspeita = 1) AS suspeitas_qtd,
            sumIf(valor, is_suspeita = 1) AS suspeitas_valor,
            count() AS todas_qtd,
            sum(valor) AS todas_valor
        FROM {MART_RT_DB}.mart_troca_forma_pgto_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} {date_range}
        {quality}
    """, parameters={"id_empresa": id_empresa})

    row = rows[0] if rows else {}
    return {
        "suspeitas_qtd": int(row.get("suspeitas_qtd") or 0),
        "suspeitas_valor": float(row.get("suspeitas_valor") or 0.0),
        "todas_qtd": int(row.get("todas_qtd") or 0),
        "todas_valor": float(row.get("todas_valor") or 0.0),
    }


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


def finance_receipts_by_day(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Recebimentos de CONTAS A RECEBER por dia (baixas totais e parciais).

    Diferente do mix de formas de pagamento das VENDAS: aqui é o dinheiro que
    efetivamente entrou de títulos a receber (baixas do Xpert), pela DATA DA
    BAIXA (DATABAIXA). É o "recebimentos" da tela Financeiro.
    """
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            toYYYYMMDD(dt_baixa) AS data_key,
            round(sum(valor_baixa), 2) AS valor,
            toUInt32(count()) AS qtd
        FROM (
            SELECT
                toDateOrNull(substring(JSONExtractString(payload, 'DATABAIXA'), 1, 10)) AS dt_baixa,
                JSONExtractFloat(payload, 'VALORBAIXA') AS valor_baixa
            FROM {CURRENT_DB}.stg_contasreceberbaixa FINAL
            WHERE id_empresa = {{id_empresa:Int32}} {filial}
              AND is_deleted = 0
        )
        WHERE dt_baixa IS NOT NULL
          AND toYYYYMMDD(dt_baixa) BETWEEN {{ini:Int32}} AND {{fim:Int32}}
        GROUP BY data_key
        HAVING valor > 0
        ORDER BY data_key
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })
    by_day = [
        {
            "data_key": int(r.get("data_key") or 0),
            "valor": round(float(r.get("valor") or 0), 2),
            "qtd": int(r.get("qtd") or 0),
        }
        for r in rows
        if int(r.get("data_key") or 0) > 0
    ]
    total = round(sum(d["valor"] for d in by_day), 2)
    return {
        "by_day": by_day,
        "total_recebido": total,
        "qtd_baixas": sum(d["qtd"] for d in by_day),
        "source": "realtime",
        "realtime_source": _realtime_source(),
    }


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
# CUSTOMERS — top / rfm / delinquency / churn / retention
# ================================================================

def customers_top(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date, limit: int = 15) -> List[Dict[str, Any]]:
    """Top customers by revenue in the period."""
    filial = _branch_clause("s.id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            s.id_cliente,
            coalesce(nullIf(c.nome, ''), concat('#ID ', toString(s.id_cliente))) AS cliente_nome,
            sum(s.valor_total) AS faturamento,
            toUInt32(count()) AS compras,
            max(s.data_key) AS ultima_compra,
            if(count() = 0, toDecimal64(0, 2), toDecimal64(sum(s.valor_total) / count(), 2)) AS ticket_medio
        FROM {CURRENT_DB}.stg_comprovantes_slim AS s
        LEFT JOIN (
            SELECT id_empresa, id_cliente, argMax(nome, source_ts_ms) AS nome
            FROM {CURRENT_DB}.dim_cliente FINAL
            WHERE id_empresa = {{id_empresa:Int32}} AND is_deleted = 0
            GROUP BY id_empresa, id_cliente
        ) AS c ON s.id_empresa = c.id_empresa AND s.id_cliente = c.id_cliente
        WHERE s.id_empresa = {{id_empresa:Int32}}
          AND s.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND s.cancelado = 0 AND s.is_deleted = 0
          AND s.situacao != 3
          AND s.id_cliente > 0
          {filial}
        GROUP BY s.id_cliente, c.nome
        ORDER BY faturamento DESC, compras DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "limit": int(limit),
    })
    return [
        {
            "id_cliente": _to_int(r.get("id_cliente")),
            "cliente_nome": r.get("cliente_nome") or "",
            "faturamento": _to_float(r.get("faturamento")),
            "compras": _to_int(r.get("compras")),
            "ultima_compra": r.get("ultima_compra"),
            "ticket_medio": _to_float(r.get("ticket_medio")),
        }
        for r in rows
    ]


def customers_rfm_snapshot(role: str, id_empresa: int, id_filial: Any, as_of: date) -> Dict[str, Any]:
    """Lightweight RFM-like snapshot."""
    filial = _branch_clause("id_filial", id_filial)
    dt_90 = as_of - timedelta(days=90)
    dt_7 = as_of - timedelta(days=7)
    dt_30 = as_of - timedelta(days=30)
    row = query_dict(f"""
        SELECT
            uniqExact(id_cliente) AS clientes_identificados,
            uniqExactIf(id_cliente, max_dk >= {{key_7d:Int32}}) AS ativos_7d,
            uniqExactIf(id_cliente, max_dk < {{key_30d:Int32}}) AS em_risco_30d_raw,
            sum(valor_total) AS faturamento_90d
        FROM (
            SELECT
                id_cliente,
                sum(valor_total) AS valor_total,
                max(data_key) AS max_dk,
                min(data_key) AS min_dk
            FROM {CURRENT_DB}.stg_comprovantes_slim
            WHERE id_empresa = {{id_empresa:Int32}}
              AND data_key BETWEEN {{key_90d:Int32}} AND {{key_as_of:Int32}}
              AND cancelado = 0 AND is_deleted = 0 AND situacao != 3
              AND id_cliente > 0
              {filial}
            GROUP BY id_cliente
        )
    """, parameters={
        "id_empresa": int(id_empresa),
        "key_90d": _date_key(dt_90),
        "key_as_of": _date_key(as_of),
        "key_7d": _date_key(dt_7),
        "key_30d": _date_key(dt_30),
    })
    if row:
        r = row[0]
        return {
            "clientes_identificados": _to_int(r.get("clientes_identificados")),
            "ativos_7d": _to_int(r.get("ativos_7d")),
            "em_risco_30d": _to_int(r.get("em_risco_30d_raw")),
            "faturamento_90d": _to_float(r.get("faturamento_90d")),
        }
    return {"clientes_identificados": 0, "ativos_7d": 0, "em_risco_30d": 0, "faturamento_90d": 0.0}


def customers_delinquency_overview(role: str, id_empresa: int, id_filial: Any, as_of: date, *, limit: int = 0, sort_by: str = "gravity") -> Dict[str, Any]:
    """Delinquency overview served from the reconciled PostgreSQL mart.

    The previous ClickHouse-direct implementation produced two production bugs:
      1. Duplication: it LEFT JOINed ``torqmind_mart_rt.mart_clientes_resumo``
         (grain empresa/filial/cliente) by ``id_cliente`` only, with no GROUP BY
         and no branch filter, multiplying every customer by the number of
         branches (observed 22x for empresa 1) — "mil vezes o mesmo cliente".
      2. Wrong balances: it computed ``VALOR - VLRPAGO`` and ignored partial
         baixas in ``stg_contasreceberbaixa``.

    ``mart.customer_delinquency_summary`` (migrations 081/084) has the correct
    grain (empresa, filial, cliente), subtracts real baixas
    ``GREATEST(valor_pago, total_baixa)``, includes a-vencer titles for already
    overdue customers, and is refreshed every operational ETL cycle
    (CDC -> STG -> DW -> mart). It reconciles to the cent with the Xpert source
    of truth (validated client 7383: venc 953.772,35 / aberto 989.371,65). It
    also emits exactly the payload contract the Customers screen consumes.
    Deduplication is therefore enforced at the mart grain, not in the frontend.
    """
    return _pg_customers_delinquency_overview(
        role, id_empresa, id_filial, as_of, limit=limit, sort_by=sort_by
    )



def customers_churn_bundle(
    role: str,
    id_empresa: int,
    id_filial: Any,
    as_of: Optional[date] = None,
    min_score: int = 60,
    limit: int = 20,
) -> Dict[str, Any]:
    """Churn bundle using recency/frequency from stg_comprovantes_slim."""
    effective_as_of = as_of or business_today(id_empresa)
    filial = _branch_clause("id_filial", id_filial)

    rows = query_dict(f"""
        SELECT
            id_cliente,
            any(cliente_nome) AS cliente_nome,
            max(data_key) AS last_purchase_key,
            dateDiff('day', toDate(toString(max(data_key)), 'yyyyMMdd'), toDate({{as_of:String}})) AS recency_days,
            toUInt32(countIf(data_key >= {{key_30d:Int32}})) AS frequency_30,
            toUInt32(countIf(data_key >= {{key_90d:Int32}})) AS frequency_90,
            sumIf(valor_total, data_key >= {{key_30d:Int32}}) AS monetary_30,
            sumIf(valor_total, data_key >= {{key_90d:Int32}}) AS monetary_90,
            if(frequency_30 > 0, monetary_30 / frequency_30, toDecimal64(0, 2)) AS ticket_30,
            -- Simple churn score: higher recency + lower frequency = higher risk
            toInt32(least(100, greatest(0,
                toInt32(recency_days) * 2
                - toInt32(frequency_30) * 10
                - toInt32(frequency_90) * 3
                + 40
            ))) AS churn_score
        FROM (
            SELECT
                s.id_cliente,
                coalesce(nullIf(c.nome, ''), concat('#ID ', toString(s.id_cliente))) AS cliente_nome,
                s.data_key,
                s.valor_total
            FROM {CURRENT_DB}.stg_comprovantes_slim AS s
            LEFT JOIN (
                SELECT id_empresa, id_cliente, argMax(nome, source_ts_ms) AS nome
                FROM {CURRENT_DB}.dim_cliente FINAL
                WHERE id_empresa = {{id_empresa:Int32}} AND is_deleted = 0
                GROUP BY id_empresa, id_cliente
            ) AS c ON s.id_empresa = c.id_empresa AND s.id_cliente = c.id_cliente
            WHERE s.id_empresa = {{id_empresa:Int32}}
              AND s.data_key BETWEEN {{key_180d:Int32}} AND {{key_as_of:Int32}}
              AND s.cancelado = 0 AND s.is_deleted = 0 AND s.situacao != 3
              AND s.id_cliente > 0
              {filial}
        )
        GROUP BY id_cliente
        HAVING churn_score >= {{min_score:Int32}}
        ORDER BY churn_score DESC, monetary_30 DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "as_of": effective_as_of.isoformat(),
        "key_30d": _date_key(effective_as_of - timedelta(days=30)),
        "key_90d": _date_key(effective_as_of - timedelta(days=90)),
        "key_180d": _date_key(effective_as_of - timedelta(days=180)),
        "key_as_of": _date_key(effective_as_of),
        "min_score": int(min_score),
        "limit": int(limit),
    })

    formatted = [
        {
            "id_cliente": _to_int(r.get("id_cliente")),
            "cliente_nome": r.get("cliente_nome") or "",
            "last_purchase": r.get("last_purchase_key"),
            "recency_days": _to_int(r.get("recency_days")),
            "frequency_30": _to_int(r.get("frequency_30")),
            "frequency_90": _to_int(r.get("frequency_90")),
            "monetary_30": _to_float(r.get("monetary_30")),
            "monetary_90": _to_float(r.get("monetary_90")),
            "ticket_30": _to_float(r.get("ticket_30")),
            "churn_score": _to_int(r.get("churn_score")),
            "revenue_at_risk_30d": _to_float(r.get("monetary_30")),
            "recommendation": "Ativar contato comercial",
            "reasons": "Recência elevada e frequência em queda",
            "dt_ref": effective_as_of.isoformat(),
        }
        for r in rows
    ]

    avg_score = round(sum(r.get("churn_score", 0) for r in formatted) / len(formatted), 2) if formatted else 0.0
    total_risk = sum(_to_float(r.get("revenue_at_risk_30d")) for r in formatted)

    return {
        "top_risk": formatted,
        "summary": {
            "total_top_risk": len(formatted),
            "avg_churn_score": avg_score,
            "revenue_at_risk_30d": round(total_risk, 2),
        },
        "snapshot_meta": {
            "snapshot_status": "operational_current",
            "precision_mode": "operational_current",
            "effective_dt_ref": effective_as_of.isoformat(),
            "source_table": "stg_comprovantes_slim",
            "source_kind": "realtime",
        },
    }


def customers_churn_snapshot_meta(
    role: str,
    id_empresa: int,
    id_filial: Any,
    as_of: Optional[date],
) -> Dict[str, Any]:
    """Churn snapshot meta summary."""
    effective_as_of = as_of or business_today(id_empresa)
    filial = _branch_clause("id_filial", id_filial)

    row = query_dict(f"""
        SELECT
            uniqExact(id_cliente) AS total_risk,
            avg(churn_score) AS average_score
        FROM (
            SELECT
                id_cliente,
                toInt32(least(100, greatest(0,
                    toInt32(dateDiff('day', toDate(toString(max(data_key)), 'yyyyMMdd'), toDate({{as_of:String}}))) * 2
                    - toInt32(countIf(data_key >= {{key_30d:Int32}})) * 10
                    - toInt32(countIf(data_key >= {{key_90d:Int32}})) * 3
                    + 40
                ))) AS churn_score
            FROM {CURRENT_DB}.stg_comprovantes_slim
            WHERE id_empresa = {{id_empresa:Int32}}
              AND data_key BETWEEN {{key_180d:Int32}} AND {{key_as_of:Int32}}
              AND cancelado = 0 AND is_deleted = 0 AND situacao != 3
              AND id_cliente > 0
              {filial}
            GROUP BY id_cliente
            HAVING churn_score >= 40
        )
    """, parameters={
        "id_empresa": int(id_empresa),
        "as_of": effective_as_of.isoformat(),
        "key_30d": _date_key(effective_as_of - timedelta(days=30)),
        "key_90d": _date_key(effective_as_of - timedelta(days=90)),
        "key_180d": _date_key(effective_as_of - timedelta(days=180)),
        "key_as_of": _date_key(effective_as_of),
    })

    r = row[0] if row else {}
    return {
        "total_risk": _to_int(r.get("total_risk")),
        "average_score": _to_float(r.get("average_score")),
        "computed_at": effective_as_of.isoformat(),
        "snapshot_status": "operational_current",
        "precision_mode": "operational_current",
        "effective_dt_ref": effective_as_of.isoformat(),
        "source_table": "stg_comprovantes_slim",
        "source_kind": "realtime",
    }


def customer_churn_drilldown(
    role: str,
    id_empresa: int,
    id_filial: Any,
    id_cliente: int,
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    """Churn drilldown for a specific customer."""
    filial = _branch_clause("id_filial", id_filial)
    series = query_dict(f"""
        SELECT
            data_key,
            sum(valor_total) AS faturamento,
            toUInt32(count()) AS compras
        FROM {CURRENT_DB}.stg_comprovantes_slim
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_cliente = {{id_cliente:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND cancelado = 0 AND is_deleted = 0 AND situacao != 3
          {filial}
        GROUP BY data_key
        ORDER BY data_key
    """, parameters={
        "id_empresa": int(id_empresa),
        "id_cliente": int(id_cliente),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })

    snapshot_meta = customers_churn_snapshot_meta(role, id_empresa, id_filial, as_of)
    return {
        "snapshot": {},
        "series": [{"data_key": r.get("data_key"), "faturamento": _to_float(r.get("faturamento")), "compras": _to_int(r.get("compras"))} for r in series],
        "snapshot_meta": snapshot_meta,
    }


def anonymous_retention_overview(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    """Anonymous retention overview from comprovantes."""
    filial = _branch_clause("id_filial", id_filial)
    # Compare period with prior 28 days
    period_days = max((dt_fim - dt_ini).days + 1, 1)
    prior_start = dt_ini - timedelta(days=28)

    row = query_dict(f"""
        SELECT
            sumIf(valor_total, id_cliente <= 0 AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}) AS anon_faturamento_7d,
            sumIf(valor_total, id_cliente <= 0 AND data_key BETWEEN {{prior_ini:Int32}} AND {{prior_fim:Int32}}) AS anon_faturamento_prev_28d,
            sumIf(valor_total, data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}) AS total_faturamento_period,
            uniqExactIf(id_cliente, id_cliente > 0 AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}) AS identified_period,
            uniqExactIf(id_cliente, id_cliente > 0 AND data_key BETWEEN {{prior_ini:Int32}} AND {{prior_fim:Int32}}) AS identified_prior
        FROM {CURRENT_DB}.stg_comprovantes_slim
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{prior_ini:Int32}} AND {{fim:Int32}}
          AND cancelado = 0 AND is_deleted = 0 AND situacao != 3
          {filial}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "prior_ini": _date_key(prior_start),
        "prior_fim": _date_key(dt_ini - timedelta(days=1)),
    })

    r = row[0] if row else {}
    anon_current = _to_float(r.get("anon_faturamento_7d"))
    anon_prev = _to_float(r.get("anon_faturamento_prev_28d"))
    total_period = _to_float(r.get("total_faturamento_period"))
    trend_pct = round(((anon_current - anon_prev) / anon_prev * 100) if anon_prev > 0 else 0.0, 2)
    anon_share = round((anon_current / total_period * 100) if total_period > 0 else 0.0, 2)
    repeat_idx = round((_to_float(r.get("identified_period")) / max(_to_float(r.get("identified_prior")), 1)) * 100, 2)
    impact = round(anon_current * max(0, -trend_pct) / 100, 2) if trend_pct < 0 else 0.0

    recommendation = (
        "Recorrência anônima caiu. Ajuste a operação por horário/dia, reveja o mix de produtos e acione promoções de retorno."
        if trend_pct < -8
        else "Recorrência anônima estável. Monitore horários de maior queda e mantenha ações de fidelização."
    )

    return {
        "kpis": {
            "impact_estimated_7d": impact,
            "trend_pct": trend_pct,
            "repeat_proxy_idx": repeat_idx,
            "severity": "CRITICAL" if trend_pct <= -15 else ("WARN" if trend_pct <= -8 else "OK"),
            "recommendation": recommendation,
        },
        "latest": [],
        "series": [],
        "breakdown_dow": [],
        "breakdown_hour": [],
        "mix": [],
    }


# ================================================================
# FINANCE — series / aging
# ================================================================

def finance_series(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    """Daily finance series from finance_overview_rt."""
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            tipo_titulo,
            faixa,
            id_filial,
            qtd_titulos,
            valor_total,
            valor_pago_total,
            valor_em_aberto
        FROM {MART_RT_DB}.finance_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
    """, parameters={"id_empresa": int(id_empresa)})

    # Map to daily-like format for compatibility
    return [
        {
            "data_key": _date_key(dt_ini),
            "id_filial": _to_int(r.get("id_filial")),
            "tipo_titulo": _to_int(r.get("tipo_titulo")),
            "valor_total": _to_float(r.get("valor_total")),
            "valor_pago": _to_float(r.get("valor_pago_total")),
            "valor_aberto": _to_float(r.get("valor_em_aberto")),
        }
        for r in rows
    ]


def finance_aging_overview(
    role: str,
    id_empresa: int,
    id_filial: Any,
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    """Finance aging overview from finance_overview_rt."""
    requested_as_of = as_of or business_today(id_empresa)
    filial = _branch_clause("id_filial", id_filial)

    rows = query_dict(f"""
        SELECT
            tipo_titulo,
            faixa,
            sum(qtd_titulos) AS qtd_titulos,
            sum(valor_total) AS valor_total,
            sum(valor_pago_total) AS valor_pago_total,
            sum(valor_em_aberto) AS valor_em_aberto
        FROM {MART_RT_DB}.finance_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
        GROUP BY tipo_titulo, faixa
    """, parameters={"id_empresa": int(id_empresa)})

    receber_total = sum(_to_float(r.get("valor_em_aberto")) for r in rows if _to_int(r.get("tipo_titulo")) == 1)
    receber_vencido = sum(_to_float(r.get("valor_em_aberto")) for r in rows if _to_int(r.get("tipo_titulo")) == 1 and str(r.get("faixa", "")).startswith("venc"))
    pagar_total = sum(_to_float(r.get("valor_em_aberto")) for r in rows if _to_int(r.get("tipo_titulo")) == 0)
    pagar_vencido = sum(_to_float(r.get("valor_em_aberto")) for r in rows if _to_int(r.get("tipo_titulo")) == 0 and str(r.get("faixa", "")).startswith("venc"))

    return {
        "dt_ref": requested_as_of.isoformat(),
        "receber_total_aberto": round(receber_total, 2),
        "receber_total_vencido": round(receber_vencido, 2),
        "pagar_total_aberto": round(pagar_total, 2),
        "pagar_total_vencido": round(pagar_vencido, 2),
        "bucket_0_7": 0.0,
        "bucket_8_15": 0.0,
        "bucket_16_30": 0.0,
        "bucket_31_60": 0.0,
        "bucket_60_plus": 0.0,
        "top5_concentration_pct": 0.0,
        "data_gaps": receber_total == 0 and pagar_total == 0,
        "snapshot_rows": len(rows),
        "snapshot_status": "operational_current",
        "precision_mode": "operational_current",
        "effective_dt_ref": requested_as_of.isoformat(),
        "source": "realtime",
    }


# ================================================================
# RISK — kpis / series / last_events / by_turn_local / window / coverage
# ================================================================

def risk_kpis(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    """Risk KPIs from risk_recent_events_rt."""
    filial = _branch_clause("id_filial", id_filial)
    row = query_dict(f"""
        SELECT
            toUInt32(count()) AS total_eventos,
            toUInt32(countIf(score_level = 'HIGH' OR score_level = 'CRITICAL')) AS eventos_alto_risco,
            sum(impacto_estimado) AS impacto_total,
            avg(score_risco) AS score_medio
        FROM {MART_RT_DB}.risk_recent_events_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {filial}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })
    r = row[0] if row else {}
    return {
        "total_eventos": _to_int(r.get("total_eventos")),
        "eventos_alto_risco": _to_int(r.get("eventos_alto_risco")),
        "impacto_total": _to_float(r.get("impacto_total")),
        "score_medio": _to_float(r.get("score_medio")),
    }


def risk_series(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    """Daily risk series."""
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            data_key,
            id_filial,
            toUInt32(count()) AS eventos_risco_total,
            toUInt32(countIf(score_level = 'HIGH' OR score_level = 'CRITICAL')) AS eventos_alto_risco,
            sum(impacto_estimado) AS impacto_estimado_total,
            avg(score_risco) AS score_medio,
            quantile(0.95)(score_risco) AS p95_score
        FROM {MART_RT_DB}.risk_recent_events_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {filial}
        GROUP BY data_key, id_filial
        ORDER BY data_key, id_filial
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })
    return [
        {
            "data_key": _to_int(r.get("data_key")),
            "id_filial": _to_int(r.get("id_filial")),
            "eventos_risco_total": _to_int(r.get("eventos_risco_total")),
            "eventos_alto_risco": _to_int(r.get("eventos_alto_risco")),
            "impacto_estimado_total": _to_float(r.get("impacto_estimado_total")),
            "score_medio": _to_float(r.get("score_medio")),
            "p95_score": _to_float(r.get("p95_score")),
        }
        for r in rows
    ]


def _antifraude_event_labels(event_type: str, reasons: str) -> tuple[str, str]:
    """Derive (categoria, motivo) from event_type + reasons JSON. Never fake."""
    et = str(event_type or "").lower()
    categoria_map = {
        "cancelamento": "Cancelamento da venda",
        "cancelamento_seguido_venda": "Cancelou e refez logo depois",
        "desconto_alto": "Desconto fora do padrão",
        "horario_risco": "Operação em horário incomum",
        "funcionario_outlier": "Colaborador fora da curva",
    }
    categoria = categoria_map.get(et, "Alerta relevante")
    rule = ""
    try:
        import json as _json

        parsed = _json.loads(reasons or "{}")
        rule = str(parsed.get("rule") or "")
    except Exception:
        rule = ""
    motivo_map = {
        "cancelled_receipt": "Comprovante cancelado na origem (cancelamento de venda).",
        "cancel_then_resell": "Venda cancelada e refeita em seguida pelo mesmo operador.",
        "high_discount": "Desconto aplicado acima do limiar do padrão da loja.",
        "odd_hour": "Operação registrada em horário fora do expediente típico.",
    }
    motivo = motivo_map.get(rule, "") or f"Evento de risco do tipo {categoria.lower()}."
    return categoria, motivo


def _antifraude_turno_label(turno_numero: int, id_turno: int) -> tuple[str, bool]:
    """Operational shift label. ``turno_numero`` is the REAL operational shift
    (1..N; 0 = caixa geral). ``id_turno`` (=ID_TURNOS) is the technical id and is
    NEVER shown as the shift number. Returns (label, resolved).
    """
    if turno_numero >= 1:
        return f"Turno {turno_numero}", True
    if id_turno > 1:
        # bound to a real technical shift, but the operational number is 0 ->
        # caixa geral / turno geral (not an operational shift 1..N).
        return "Caixa geral", True
    return "Turno não resolvido", False


def _load_nfe_numbers(id_empresa: int, rows: List[Dict[str, Any]]) -> Dict[tuple[int, int], str]:
    """Busca em lote o número da Nota Fiscal (NF/NFC-e) por comprovante.

    Retorna {(id_filial, id_comprovante): numero_nfe}. Vendas que nunca emitiram
    documento fiscal (comum em cancelamentos) simplesmente não aparecem no mapa,
    e o chamador cai honestamente no número do comprovante.
    """
    pairs = sorted({
        (int(r["id_filial"]), int(r["id_comprovante"]))
        for r in rows
        if r.get("id_filial") is not None and r.get("id_comprovante") not in (None, 0)
    })
    if not pairs:
        return {}
    values = ", ".join(f"({f}, {c})" for f, c in pairs)
    # ALIAS != coluna base: usar ``numero_nfe`` como alias do argMax colide com a
    # coluna ``numero_nfe`` referenciada no WHERE e o ClickHouse levanta
    # ILLEGAL_AGGREGATION (code 184). Alias distinto resolve o WHERE para a coluna.
    result = query_dict(
        f"""
        SELECT id_filial,
               id_comprovante,
               argMax(numero_nfe, source_ts_ms) AS numero_nfe_res
        FROM {CURRENT_DB}.stg_nfe_slim
        WHERE id_empresa = {{id_empresa:Int32}}
          AND is_deleted = 0
          AND (id_filial, id_comprovante) IN ({values})
          AND numero_nfe != ''
          AND numero_nfe != '0'
        GROUP BY id_filial, id_comprovante
        """,
        parameters={"id_empresa": id_empresa},
    )
    return {
        (int(r["id_filial"]), int(r["id_comprovante"])): str(r.get("numero_nfe_res") or "").strip()
        for r in result
        if r.get("id_filial") is not None
        and r.get("id_comprovante") is not None
        and str(r.get("numero_nfe_res") or "").strip()
    }


def _extract_nfce_number(*texts: str) -> str:
    """Extrai número de NF-e/NFC-e de textos (HISTORICO, nro_documento mash)."""
    import re

    for text in texts:
        raw = str(text or "").strip()
        if not raw:
            continue
        m = re.search(r"(?i)(?:NFC-?e|NF-?e)\s*[#:]?\s*(\d+)", raw)
        if m:
            return m.group(1)
        # Mash já pode trazer só o número (ex.: nro_documento='325152').
        if raw.isdigit() and len(raw) >= 4:
            return raw
    return ""


def _enrich_credito_usos_operador_via_nfe(
    id_empresa: int,
    uso_rows: List[Dict[str, Any]],
) -> None:
    """Preenche id_comprovante + operador_caixa via NFC-e → stg_nfe_slim → comprovante.

    Documento na tela continua sendo a NF. Operador exige o comprovante que liberou
    a venda; HISTORICO atual costuma trazer só "NFC-e N" (sem Cupom:), então o mash
    PG não casa direto em NROCOMPROVANTE. ClickHouse tem a ponte canônica.
    Mutates ``uso_rows`` in place.
    """
    need: List[tuple[int, str]] = []
    for u in uso_rows:
        op = str(u.get("operador_caixa") or "").strip()
        if op and op not in ("—", "-", "None"):
            continue
        if int(u.get("id_comprovante") or 0) > 0 and int(u.get("id_usuario_caixa") or 0) > 0 and op:
            continue
        fil = int(u.get("id_filial") or 0)
        nf = _extract_nfce_number(
            str(u.get("nro_documento") or ""),
            str(u.get("historico") or ""),
        )
        if fil > 0 and nf:
            need.append((fil, nf))
    if not need:
        return

    pairs = sorted(set(need))
    values = ", ".join(f"({f}, '{nf}')" for f, nf in pairs)
    try:
        found = query_dict(
            f"""
            SELECT
                n.id_filial AS id_filial,
                n.numero_nfe AS numero_nfe,
                argMax(n.id_comprovante, n.source_ts_ms) AS id_comprovante,
                argMax(c.id_usuario_shadow, n.source_ts_ms) AS id_usuario_caixa,
                argMax(
                    coalesce(
                        nullIf(trim(du.nome), ''),
                        nullIf(JSONExtractString(us.payload, 'NOMEUSUARIOS'), ''),
                        nullIf(JSONExtractString(us.payload, 'NOME'), ''),
                        ''
                    ),
                    n.source_ts_ms
                ) AS operador_caixa
            FROM {CURRENT_DB}.stg_nfe_slim AS n FINAL
            INNER JOIN {CURRENT_DB}.stg_comprovantes AS c FINAL
                ON c.id_empresa = n.id_empresa
               AND c.id_filial = n.id_filial
               AND c.id_comprovante = n.id_comprovante
               AND c.is_deleted = 0
            LEFT JOIN {CURRENT_DB}.dim_usuario_caixa AS du FINAL
                ON du.id_empresa = c.id_empresa
               AND du.id_usuario = c.id_usuario_shadow
               AND du.is_deleted = 0
            LEFT JOIN {CURRENT_DB}.stg_usuarios AS us FINAL
                ON us.id_empresa = c.id_empresa
               AND us.id_usuario = c.id_usuario_shadow
               AND us.is_deleted = 0
            WHERE n.id_empresa = {{id_empresa:Int32}}
              AND n.is_deleted = 0
              AND n.status != 5
              AND (n.id_filial, n.numero_nfe) IN ({values})
              AND n.numero_nfe != ''
              AND n.numero_nfe != '0'
            GROUP BY n.id_filial, n.numero_nfe
            """,
            parameters={"id_empresa": int(id_empresa)},
        )
    except Exception as exc:
        logger.warning(
            "enrich credito operador via nfe failed empresa=%s: %s",
            id_empresa,
            str(exc)[:200],
        )
        return

    by_key = {
        (int(r["id_filial"]), str(r.get("numero_nfe") or "").strip()): r
        for r in found
        if r.get("id_filial") is not None and str(r.get("numero_nfe") or "").strip()
    }
    for u in uso_rows:
        fil = int(u.get("id_filial") or 0)
        nf = _extract_nfce_number(
            str(u.get("nro_documento") or ""),
            str(u.get("historico") or ""),
        )
        hit = by_key.get((fil, nf)) if fil and nf else None
        if not hit:
            continue
        if not int(u.get("id_comprovante") or 0):
            u["id_comprovante"] = int(hit.get("id_comprovante") or 0) or None
        if not int(u.get("id_usuario_caixa") or 0):
            u["id_usuario_caixa"] = int(hit.get("id_usuario_caixa") or 0) or None
        op = str(hit.get("operador_caixa") or "").strip()
        if op and (not str(u.get("operador_caixa") or "").strip()
                   or str(u.get("operador_caixa") or "").strip() in ("—", "-")):
            u["operador_caixa"] = op


def _antifraude_documento(
    numero_nfe: Any, nro_comprovante: int, id_comprovante: int
) -> tuple[Any, str, str, Optional[str]]:
    """Documento operacional da venda = número da NF-e/NFC-e.

    Regra absoluta (AGENTS.md / 07-documento-nota-fiscal):
    DOCUMENTO = nota fiscal. Sem NF → "—". Nunca NROCOMPROVANTE nem id_comprovante.
    Label = somente o número (sem prefixo).
    Retorna (documento_venda, label, source, documento_fiscal).
    """
    nfe = str(numero_nfe or "").strip()
    if nfe and nfe != "0":
        return nfe, nfe, "nota_fiscal", nfe
    return None, "—", "fallback", None


def _build_antifraude_event(r: Dict[str, Any]) -> Dict[str, Any]:
    """Map an enriched mart_antifraude_eventos row to the fraud-screen contract.

    Single, consistent contract shared by risk_last_events / fraud_last_events so
    the screen never mixes a rich operational read with a poor modeled read.
    """
    from datetime import datetime as _dt

    id_filial = _to_int(r.get("id_filial"))
    filial_nome = (r.get("filial_nome") or "").strip()
    # Apelido (nome reduzido) tem prioridade em todo o sistema; cai no nome
    # completo do mart e, por ultimo, num rotulo honesto por id.
    filial_label = _filial_label(id_filial, filial_nome) if id_filial else "Filial sem cadastro"

    dt_val = r.get("dt")
    hora_val = _to_int(r.get("hora"))
    data_iso = None
    if dt_val:
        if isinstance(dt_val, str):
            data_iso = f"{dt_val[:10]}T{hora_val:02d}:00:00"
        else:
            try:
                data_iso = _dt.combine(dt_val, _dt.min.time().replace(hour=min(hora_val, 23))).isoformat()
            except Exception:
                data_iso = None

    id_turno = _to_int(r.get("id_turno"))  # technical ID_TURNOS, traceability only
    turno_numero = _to_int(r.get("turno_numero"))  # operational shift (1..N; 0=caixa geral)
    turno_label, turno_resolved = _antifraude_turno_label(turno_numero, id_turno)

    id_caixa = _to_int(r.get("id_caixa"))
    id_usuario = _to_int(r.get("id_usuario"))
    nome_op = (r.get("nome_operador") or "").strip()
    if nome_op:
        operador_label = nome_op
        operador_source = "comprovante"
    elif id_usuario:
        operador_label = f"Operador #{id_usuario}"
        operador_source = "id_only"
    else:
        operador_label = "Operador sem cadastro"
        operador_source = "unresolved"

    id_funcionario = r.get("id_funcionario")
    nome_func = (r.get("nome_funcionario") or "").strip()
    frentista_label = nome_func or "Sem frentista associado"

    event_type = r.get("event_type") or ""
    reasons = r.get("reasons") or "{}"
    categoria, motivo = _antifraude_event_labels(event_type, reasons)

    event_id = r.get("event_id") if r.get("event_id") is not None else r.get("id")
    id_comprovante = _to_int(r.get("id_comprovante"))
    nro_comprovante = _to_int(r.get("nro_comprovante"))
    numero_nfe = str(r.get("numero_nfe") or "").strip()
    documento_venda, documento_label, documento_source, documento_fiscal = _antifraude_documento(
        numero_nfe, nro_comprovante, id_comprovante
    )

    return {
        "id_evento": str(event_id) if event_id is not None else None,
        "id": str(event_id) if event_id is not None else None,
        "event_id": str(event_id) if event_id is not None else None,
        "id_comprovante": id_comprovante or None,
        "documento_venda": documento_venda,
        "documento_label": documento_label,
        "documento_source": documento_source,
        "documento_fiscal": documento_fiscal,
        "referencia": documento_label,
        "id_filial": id_filial,
        "filial_nome": filial_nome,
        "filial_label": filial_label,
        "data": data_iso,
        "data_key": _to_int(r.get("data_key")),
        "hora": hora_val,
        "id_turno": id_turno,
        "turno_numero": turno_numero,
        "turno_label": turno_label,
        "id_caixa": id_caixa,
        "id_usuario": id_usuario,
        "operador_label": operador_label,
        "operador_caixa_label": operador_label,
        "responsavel_label": operador_label,
        "usuario_label": operador_label,
        "usuario_source": operador_source,
        "id_funcionario": id_funcionario,
        "frentista_label": frentista_label,
        "funcionario_label": frentista_label,
        "valor": _to_float(r.get("valor_total")),
        "valor_total": _to_float(r.get("valor_total")),
        "impacto_estimado": _to_float(r.get("impacto_estimado")),
        "score": _to_int(r.get("score_risco")),
        "score_risco": _to_int(r.get("score_risco")),
        "score_level": r.get("score_level") or "",
        "categoria": categoria,
        "event_type": event_type,
        "event_label": categoria,
        "motivo": motivo,
        "reason_summary": motivo,
        "reasons": reasons,
        "source": r.get("source") or "",
    }


def risk_last_events(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Enriched antifraud events for the screen (filial/turno/operator/date).

    Reads ``mart_antifraude_eventos`` (rich: real id_turno, operator name, date,
    filial name) instead of the poor ``risk_recent_events_rt``. Emits a single
    consistent contract so the review queue never shows "Operador sem cadastro",
    "Turno sem cadastro" or "quando -" when the data exists.
    """
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            event_id, id_filial, filial_nome, data_key, dt, hora,
            event_type, source, id_turno, turno_numero, id_caixa, id_usuario, nome_operador,
            id_funcionario, nome_funcionario, valor_total, impacto_estimado,
            score_risco, score_level, reasons, id_comprovante, nro_comprovante
        FROM {MART_RT_DB}.mart_antifraude_eventos FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {filial}
        ORDER BY data_key DESC, score_risco DESC, event_id DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "limit": int(limit),
    })
    # NFE enrichment is best-effort: a failure here must never drop the screen to
    # the poor legacy fallback (which depends on a non-populated PostgreSQL MV).
    try:
        nfe_map = _load_nfe_numbers(id_empresa, rows)
    except Exception:
        nfe_map = {}
    for r in rows:
        r["numero_nfe"] = nfe_map.get(
            (_to_int(r.get("id_filial")), _to_int(r.get("id_comprovante"))), ""
        )
    return [_build_antifraude_event(r) for r in rows]


def risk_by_turn_local(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Risk concentration by REAL operational shift (turno_numero) + operator.

    Fixes two semantic bugs: (1) the old `id_usuario AS id_turno` (a user is not
    a shift) and (2) showing the technical ``id_turno`` (ID_TURNOS, e.g. 34292)
    as the shift number. Groups by the REAL operational shift ``turno_numero``
    (1..N) per filial and surfaces the operator most associated with it. Shift 0
    (caixa geral) and unresolved shifts are excluded from this per-shift
    concentration (they still appear in the review queue); there is no reliable
    "canal"/local in source, so we show the operational responsible instead.
    """
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            id_filial,
            any(filial_nome) AS filial_nome,
            turno_numero,
            toUInt32(count()) AS eventos,
            toUInt32(countIf(score_level = 'HIGH' OR score_level = 'CRITICAL')) AS alto_risco,
            sum(impacto_estimado) AS impacto_estimado,
            avg(score_risco) AS score_medio,
            argMax(nome_operador, 1) AS operador_top
        FROM {MART_RT_DB}.mart_antifraude_eventos FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND turno_numero >= 1
          {filial}
        GROUP BY id_filial, turno_numero
        ORDER BY impacto_estimado DESC, score_medio DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "limit": int(limit),
    })
    result = []
    for r in rows:
        id_filial_r = _to_int(r.get("id_filial"))
        filial_nome = (r.get("filial_nome") or "").strip()
        turno_numero = _to_int(r.get("turno_numero"))
        operador_top = (r.get("operador_top") or "").strip()
        result.append({
            "id_filial": id_filial_r,
            "filial_nome": filial_nome,
            "filial_label": _filial_label(id_filial_r, filial_nome),
            "turno_numero": turno_numero,
            "turno_label": f"Turno {turno_numero}" if turno_numero >= 1 else "Caixa geral",
            "operador_label": operador_top or "Operador sem cadastro",
            "eventos": _to_int(r.get("eventos")),
            "alto_risco": _to_int(r.get("alto_risco")),
            "impacto_estimado": _to_float(r.get("impacto_estimado")),
            "score_medio": _to_float(r.get("score_medio")),
        })
    return result



def risk_data_window(role: str, id_empresa: int, id_filial: Any) -> Dict[str, Any]:
    """Data window available for risk."""
    filial = _branch_clause("id_filial", id_filial)
    row = query_dict(f"""
        SELECT
            min(data_key) AS min_data_key,
            max(data_key) AS max_data_key,
            toUInt32(count()) AS rows
        FROM {MART_RT_DB}.risk_recent_events_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
    """, parameters={"id_empresa": int(id_empresa)})
    r = row[0] if row else {}
    return {
        "min_data_key": r.get("min_data_key") if _to_int(r.get("rows")) > 0 else None,
        "max_data_key": r.get("max_data_key") if _to_int(r.get("rows")) > 0 else None,
        "rows": _to_int(r.get("rows")),
    }


def risk_model_coverage(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    """Model coverage based on risk data window."""
    risk_window = risk_data_window(role, id_empresa, id_filial)
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

    # Parse date keys
    def _date_from_key(k: int) -> date:
        s = str(k)
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

    covered_start = _date_from_key(covered_start_key)
    covered_end = _date_from_key(covered_end_key)
    covered_days = max((covered_end - covered_start).days + 1, 0) if covered_end >= covered_start else 0
    covered_fully = window_start_key <= requested_start_key and window_end_key >= requested_end_key

    if covered_fully:
        status = "covered"
        message = "A leitura modelada cobre todo o período selecionado."
    elif covered_days > 0:
        status = "partial"
        message = f"A leitura modelada cobre de {covered_start.strftime('%d/%m/%Y')} a {covered_end.strftime('%d/%m/%Y')}."
    else:
        status = "not_covered"
        message = "A leitura modelada não cobre este período."

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


# ================================================================
# PAYMENTS — anomalies
# ================================================================

def payments_anomalies(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Payment anomalies from payments_by_type_rt (threshold-based)."""
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            data_key,
            id_filial,
            tipo_forma,
            label,
            category,
            valor_total,
            qtd_transacoes
        FROM {MART_RT_DB}.payments_by_type_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND (category = '' OR category = 'DESCONHECIDO')
          {filial}
        ORDER BY valor_total DESC
        LIMIT {{limit:UInt32}}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
        "limit": int(limit),
    })
    return [
        {
            "data_key": _to_int(r.get("data_key")),
            "id_filial": _to_int(r.get("id_filial")),
            "filial_nome": "",
            "event_type": "UNKNOWN_PAYMENT",
            "severity": "WARN",
            "score": 50,
            "impacto_estimado": _to_float(r.get("valor_total")),
            "reasons": f'{{"label": "{r.get("label") or ""}", "qtd": {_to_int(r.get("qtd_transacoes"))}}}',
            "filial_label": _filial_label(_to_int(r.get("id_filial")), ""),
            "event_label": f"Pagamento sem classificação: {r.get('label') or 'N/A'}",
        }
        for r in rows
    ]


# ================================================================
# SALES — operational current
# ================================================================

def sales_operational_current(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    as_of: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    """Sales operational current from stg_comprovantes_slim (today only)."""
    if as_of is None or dt_ini != dt_fim or dt_fim != as_of:
        return None
    filial = _branch_clause("id_filial", id_filial)
    today_key = _date_key(as_of)

    row = query_dict(f"""
        SELECT
            sum(valor_total) AS faturamento,
            toUInt32(count()) AS vendas,
            avg(valor_total) AS ticket_medio,
            sumIf(valor_total, cancelado = 1) AS valor_cancelado,
            toUInt32(countIf(cancelado = 1)) AS cancelamentos
        FROM {CURRENT_DB}.stg_comprovantes_slim
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key = {{today:Int32}}
          AND is_deleted = 0 AND situacao != 3
          {filial}
    """, parameters={"id_empresa": int(id_empresa), "today": today_key})

    r = row[0] if row else {}
    if _to_float(r.get("faturamento")) == 0 and _to_int(r.get("vendas")) == 0:
        return None

    return {
        "kpis": {
            "faturamento": _to_float(r.get("faturamento")),
            "margem": 0.0,
            "ticket_medio": _to_float(r.get("ticket_medio")),
            "devolucoes": 0.0,
        },
        "commercial_kpis": {
            "saidas": _to_float(r.get("faturamento")),
            "qtd_saidas": _to_int(r.get("vendas")),
            "cancelamentos": _to_float(r.get("valor_cancelado")),
            "qtd_cancelamentos": _to_int(r.get("cancelamentos")),
        },
        "stats": {"vendas": _to_int(r.get("vendas"))},
        "reading_status": "operational_current",
        "source": "realtime",
    }


# ================================================================
# MISC — commercial_window / health_score / insights / operational_score / jarvis
# ================================================================

def commercial_window_coverage(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    """Commercial window coverage from sales_daily_rt."""
    filial = _branch_clause("id_filial", id_filial)
    row = query_dict(f"""
        SELECT
            min(data_key) AS min_data_key,
            max(data_key) AS max_data_key
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
    """, parameters={"id_empresa": int(id_empresa)})

    r = row[0] if row else {}
    min_dk = r.get("min_data_key")
    max_dk = r.get("max_data_key")
    requested_start_key = _date_key(dt_ini)
    requested_end_key = _date_key(dt_fim)

    if not min_dk or not max_dk or _to_int(min_dk) == 0:
        return {
            "status": "no_data",
            "covered_fully": False,
            "requested_start_key": requested_start_key,
            "requested_end_key": requested_end_key,
            "available_start_key": None,
            "available_end_key": None,
            "source_label": "sales_daily_rt",
        }

    min_key = _to_int(min_dk)
    max_key = _to_int(max_dk)
    covered_fully = min_key <= requested_start_key and max_key >= requested_end_key

    return {
        "status": "covered" if covered_fully else "partial",
        "covered_fully": covered_fully,
        "requested_start_key": requested_start_key,
        "requested_end_key": requested_end_key,
        "available_start_key": min_key,
        "available_end_key": max_key,
        "source_label": "sales_daily_rt",
    }


def health_score_latest(
    role: str,
    id_empresa: int,
    id_filial: Any,
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    """Health score based on source_freshness."""
    row = query_dict(f"""
        SELECT
            count() AS total_domains,
            countIf(status = 'ok') AS ok_domains,
            max(lag_seconds) AS max_lag
        FROM {MART_RT_DB}.source_freshness FINAL
        WHERE id_empresa = {{id_empresa:Int32}} OR id_empresa = 0
    """, parameters={"id_empresa": int(id_empresa)})

    r = row[0] if row else {}
    total = max(_to_int(r.get("total_domains")), 1)
    ok = _to_int(r.get("ok_domains"))
    max_lag = _to_float(r.get("max_lag"))

    # Score: % of OK domains, penalized by lag
    freshness_score = round((ok / total) * 100, 2)
    lag_penalty = min(30, max_lag / 3600 * 10) if max_lag > 300 else 0
    score_total = round(max(0, min(100, freshness_score - lag_penalty)), 2)

    return {
        "dt_ref": (as_of or business_today(id_empresa)).isoformat(),
        "score_total": score_total,
        "components": {
            "freshness": freshness_score,
            "lag_penalty": round(lag_penalty, 2),
        },
        "reasons": {
            "total_domains": total,
            "ok_domains": ok,
            "max_lag_seconds": round(max_lag, 0),
        },
        "snapshot_status": "operational_current",
        "precision_mode": "realtime",
        "source": "realtime",
    }


def insights_base(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> List[Dict[str, Any]]:
    """Insights base data from sales_daily_rt."""
    filial = _branch_clause("id_filial", id_filial)
    rows = query_dict(f"""
        SELECT
            data_key,
            id_filial,
            faturamento AS faturamento_dia,
            toDecimal64(0, 2) AS faturamento_mes_acum,
            toDecimal64(0, 2) AS comparativo_mes_anterior
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {filial}
        ORDER BY data_key, id_filial
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })
    return [
        {
            "data_key": _to_int(r.get("data_key")),
            "id_filial": _to_int(r.get("id_filial")),
            "faturamento_dia": _to_float(r.get("faturamento_dia")),
            "faturamento_mes_acum": _to_float(r.get("faturamento_mes_acum")),
            "comparativo_mes_anterior": _to_float(r.get("comparativo_mes_anterior")),
        }
        for r in rows
    ]


def operational_score(role: str, id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date) -> Dict[str, Any]:
    """Operational score from sales_daily_rt + risk_recent_events_rt."""
    filial_sales = _branch_clause("id_filial", id_filial)
    filial_risk = _branch_clause("id_filial", id_filial)

    sales_row = query_dict(f"""
        SELECT
            sum(faturamento) AS faturamento,
            sum(margem_total) AS margem,
            avg(ticket_medio) AS ticket_medio
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {filial_sales}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })

    risk_row = query_dict(f"""
        SELECT
            toUInt32(countIf(score_level = 'HIGH' OR score_level = 'CRITICAL')) AS eventos_alto_risco,
            toUInt32(count()) AS eventos_risco_total,
            sum(impacto_estimado) AS impacto_estimado_total
        FROM {MART_RT_DB}.risk_recent_events_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          {filial_risk}
    """, parameters={
        "id_empresa": int(id_empresa),
        "ini": _date_key(dt_ini),
        "fim": _date_key(dt_fim),
    })

    s = sales_row[0] if sales_row else {}
    rk = risk_row[0] if risk_row else {}

    faturamento = _to_float(s.get("faturamento"))
    margem = _to_float(s.get("margem"))
    ticket = _to_float(s.get("ticket_medio"))
    eventos_alto = _to_int(rk.get("eventos_alto_risco"))
    eventos_total = _to_int(rk.get("eventos_risco_total"))
    impacto = _to_float(rk.get("impacto_estimado_total"))

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


def jarvis_briefing(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ref: date,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Simplified Jarvis briefing using realtime data."""
    dt_ini = dt_ref - timedelta(days=6)

    # Gather context
    try:
        risk = risk_kpis(role, id_empresa, id_filial, dt_ini, dt_ref)
    except Exception:
        risk = {"total_eventos": 0, "eventos_alto_risco": 0, "impacto_total": 0, "score_medio": 0}

    try:
        fin = finance_aging_overview(role, id_empresa, id_filial, as_of=dt_ref)
    except Exception:
        fin = {}

    # Build briefing
    receiving_overdue = _to_float(fin.get("receber_total_vencido"))
    risk_total = _to_int(risk.get("total_eventos"))
    risk_alto = _to_int(risk.get("eventos_alto_risco"))
    impacto = _to_float(risk.get("impacto_total"))

    candidates: List[Dict[str, Any]] = []

    if risk_alto > 0:
        candidates.append({
            "kind": "fraud",
            "weight": impacto + risk_alto * 500,
            "impact_value": impacto,
            "priority": "Imediatamente" if risk_alto >= 5 else "Hoje",
            "headline": "Auditar cancelamentos e descontos relevantes antes do próximo fechamento.",
            "cause": "A modelagem de risco encontrou concentração relevante em cancelamentos.",
            "action": "Abrir o antifraude, revisar o turno mais sensível e validar o colaborador mais exposto.",
            "evidence": [f"{risk_alto} evento(s) de alto risco", f"Impacto: R$ {impacto:,.2f}"],
        })

    if receiving_overdue > 0:
        candidates.append({
            "kind": "finance",
            "weight": receiving_overdue,
            "impact_value": receiving_overdue,
            "priority": "Hoje",
            "headline": "Cobrar hoje os vencidos mais concentrados para aliviar a pressão de caixa.",
            "cause": "A carteira vencida concentra recursos que já deveriam estar no caixa.",
            "action": "Ativar régua de cobrança nos maiores títulos vencidos.",
            "evidence": [f"Receber vencido: R$ {receiving_overdue:,.2f}"],
        })

    if not candidates:
        return {
            "title": "Copiloto operacional",
            "data_ref": dt_ref.isoformat(),
            "status": "ok",
            "headline": "Operação estável no período atual, sem foco crítico acima da linha de corte.",
            "summary": "O momento pede disciplina de execução e acompanhamento.",
            "priority": "Acompanhar",
            "impact_value": 0.0,
            "impact_label": "Sem exposição crítica material",
            "problem": "Sem frente crítica acima da linha de corte.",
            "cause": "Fraude, caixa, clientes e financeiro seguiram dentro da faixa esperada.",
            "action": "Sustentar o ritmo comercial e manter a rotina de acompanhamento diário.",
            "confidence_label": "Moderada",
            "confidence_level": "medium",
            "confidence_reason": "Leitura realtime com base operacional.",
            "data_freshness": {},
            "primary_kind": None,
            "primary_shortcut": None,
            "evidence": ["Sem alertas críticos acima do corte"],
            "secondary_focus": [],
            "signals": {},
            "highlights": ["A operação seguiu estável no período."],
        }

    candidates.sort(key=lambda c: float(c.get("weight") or 0), reverse=True)
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
        "impact_label": f"R$ {float(primary.get('impact_value') or 0):,.2f} em jogo",
        "problem": primary["headline"],
        "cause": primary["cause"],
        "action": primary["action"],
        "confidence_label": "Moderada",
        "confidence_level": "medium",
        "confidence_reason": "Leitura realtime com base operacional.",
        "data_freshness": {},
        "primary_kind": primary.get("kind"),
        "primary_shortcut": None,
        "evidence": [item for item in primary.get("evidence", []) if item],
        "secondary_focus": [
            {
                "kind": item.get("kind"),
                "label": item["headline"],
                "impact_label": f"R$ {float(item.get('impact_value') or 0):,.2f}",
                "priority": item["priority"],
            }
            for item in secondary
        ],
        "signals": {},
        "highlights": [primary["action"]] + [item["headline"] for item in secondary][:2],
    }


def _credito_func_entidades_ativas_na_filial(
    id_empresa: int, branch_ids: List[int]
) -> List[int]:
    """IDs de entidade (grupo 12, com limite, ATIVO) presentes na(s) filial(is).

    Cadastro é replicado entre postos; a filial “de casa” operacional é a cópia
    ATIVA na filial selecionada. Gastos continuam empresa-wide no mash de usos.
    """
    if not branch_ids:
        return []
    fil_sql = (
        f"id_filial = {int(branch_ids[0])}"
        if len(branch_ids) == 1
        else "id_filial IN (" + ", ".join(str(int(v)) for v in branch_ids) + ")"
    )
    rows = query_dict(
        f"""
        SELECT DISTINCT id_entidade
        FROM {CURRENT_DB}.stg_entidades FINAL
        WHERE id_empresa = %(id_empresa)s
          AND {fil_sql}
          AND is_deleted = 0
          AND id_entidade > 0
          AND JSONExtractString(payload, 'ID_GRUPOENTIDADES') = '12'
          AND (
            toFloat64OrZero(JSONExtractString(payload, 'LIMITE')) > 0
            OR toFloat64OrZero(JSONExtractString(payload, 'LIMITE_VALE')) > 0
          )
          AND lowerUTF8(JSONExtractString(payload, 'ATIVO')) IN ('true', '1', 't')
        """,
        {"id_empresa": int(id_empresa)},
    )
    return sorted({int(r["id_entidade"]) for r in rows if r.get("id_entidade") is not None})


def fraud_credito_funcionario(
    role: str,
    id_empresa: int,
    id_filial: Any,
    ano_mes: Optional[int] = None,
    status: str = "todos",
    refresh: bool = False,
    limit: int = 500,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Antifraude crédito funcionário — lê ClickHouse mart_rt (não PG/STG).

    Mash/refresh continua no PG via ``repos_mart.refresh_fraud_credito_funcionario``
    (que também publica no CH). GET padrão só consulta CH.
    """
    import json
    from datetime import datetime

    from app.repos_mart import (
        _ano_mes_from_date,
        refresh_fraud_credito_funcionario as _pg_refresh_publish,
    )

    ym = int(ano_mes) if ano_mes else _ano_mes_from_date(business_today(id_empresa))
    if refresh:
        try:
            _pg_refresh_publish(role, id_empresa, ym)
        except Exception as exc:
            logger.warning(
                "fraud_credito_funcionario refresh/publish failed empresa=%s mes=%s: %s",
                id_empresa, ym, str(exc)[:240],
            )

    status_key = str(status or "todos").strip().lower()
    status_sql = ""
    if status_key in ("suspeito", "suspeitos", "suspeitas"):
        status_sql = "AND status = 'Suspeito'"
    elif status_key in ("normal", "normais"):
        status_sql = "AND status = 'Normal'"

    # Lista: só funcionários com cadastro ATIVO na filial selecionada (grupo 12).
    # Usos: empresa-wide (não filtrar pela filial do gasto).
    branch_ids = _branch_ids(id_filial)
    entidade_sql = ""
    if branch_ids:
        ents = _credito_func_entidades_ativas_na_filial(int(id_empresa), branch_ids)
        if not ents:
            return {
                "ano_mes": ym,
                "meses_disponiveis": [ym],
                "source": "clickhouse",
                "summary": {
                    "total": 0, "suspeitos": 0, "normais": 0,
                    "usado_total": 0.0, "usado_prazo_total": 0.0, "usado_vale_total": 0.0,
                    "limite_total": 0.0, "limite_prazo_total": 0.0, "limite_vale_total": 0.0,
                },
                "funcionarios": [],
                "disclaimer": (
                    "Funcionários: ENTIDADES grupo 12 ativas na filial selecionada. "
                    "Usos: CONTASRECEBER da entidade em toda a empresa."
                ),
            }
        entidade_sql = "AND id_entidade IN (" + ", ".join(str(i) for i in ents) + ")"

    try:
        lim = max(1, min(int(limit), 2000))
    except (TypeError, ValueError):
        lim = 500

    rows = query_dict(
        f"""
        SELECT
            id_funcionario,
            id_filial_ref,
            id_entidade,
            nome_funcionario,
            cpf,
            ativo,
            limite_prazo,
            limite_vale,
            limite_total,
            vales_cadastro,
            usado_prazo,
            usado_vale,
            usado_mes,
            saldo_prazo,
            saldo_vale,
            saldo_restante,
            qtd_usos_mes,
            max_usos_mesmo_dia,
            status,
            motivos,
            published_at
        FROM {MART_RT_DB}.mart_fraud_credito_funcionario_resumo FINAL
        WHERE id_empresa = %(id_empresa)s
          AND ano_mes = %(ano_mes)s
          {entidade_sql}
          {status_sql}
        ORDER BY nome_funcionario ASC, id_funcionario ASC
        LIMIT %(lim)s
        """,
        {"id_empresa": int(id_empresa), "ano_mes": ym, "lim": lim},
    )

    func_ids = [int(r["id_funcionario"]) for r in rows if r.get("id_funcionario") is not None]
    usos_by: Dict[int, List[Dict[str, Any]]] = {fid: [] for fid in func_ids}
    if func_ids:
        ids_csv = ", ".join(str(i) for i in func_ids)
        uso_rows = query_dict(
            f"""
            SELECT
                id_funcionario,
                id_filial,
                id_entidade,
                id_contasreceber,
                id_comprovante,
                nro_cupom,
                nro_documento,
                tipo_uso,
                dt_evento,
                valor,
                id_usuario_caixa,
                operador_caixa,
                historico,
                atipico
            FROM {MART_RT_DB}.mart_fraud_credito_funcionario_uso FINAL
            WHERE id_empresa = %(id_empresa)s
              AND ano_mes = %(ano_mes)s
              AND id_funcionario IN ({ids_csv})
            ORDER BY id_filial ASC, dt_evento DESC, valor DESC
            """,
            {"id_empresa": int(id_empresa), "ano_mes": ym},
        )
        # Operador: NFC-e → comprovante (HISTORICO sem Cupom: não casa direto).
        _enrich_credito_usos_operador_via_nfe(int(id_empresa), uso_rows)
        # NF/NFC-e canônico via stg_nfe_slim (DOCUMENTO = nota fiscal).
        nfe_map = _load_nfe_numbers(int(id_empresa), uso_rows)
        for u in uso_rows:
            fid = int(u["id_funcionario"])
            dt = u.get("dt_evento")
            if isinstance(dt, datetime):
                dt_s = dt.isoformat()
            else:
                dt_s = str(dt) if dt else None
            id_filial = int(u.get("id_filial") or 0)
            id_comp = int(u.get("id_comprovante") or 0)
            nfe_join = nfe_map.get((id_filial, id_comp), "")
            nfe_hist = _extract_nfce_number(
                str(u.get("nro_documento") or ""),
                str(u.get("historico") or ""),
            )
            documento_venda, documento_label, documento_source, documento_fiscal = _antifraude_documento(
                nfe_join or nfe_hist,
                0,
                0,
            )
            usos_by.setdefault(fid, []).append({
                "id_filial": id_filial or None,
                "filial_label": _filial_label(id_filial, "") if id_filial else "—",
                "id_entidade": u.get("id_entidade"),
                "id_contasreceber": u.get("id_contasreceber"),
                "id_comprovante": id_comp or None,
                "tipo_uso": str(u.get("tipo_uso") or "prazo"),
                "documento": documento_label,
                "documento_label": documento_label,
                "documento_venda": documento_venda,
                "documento_source": documento_source,
                "documento_fiscal": documento_fiscal,
                "dt_evento": dt_s,
                "valor": float(u.get("valor") or 0),
                "id_usuario_caixa": u.get("id_usuario_caixa") or None,
                "operador_caixa": u.get("operador_caixa") or "—",
                "historico": u.get("historico") or "",
                "atipico": bool(int(u.get("atipico") or 0)),
            })
        for fid in usos_by:
            def _uso_sort_key(x: Dict[str, Any]) -> tuple:
                dt = str(x.get("dt_evento") or "")
                # YYYY-MM-DD… → chave numérica YYYYMMDD (data mais recente primeiro)
                digits = "".join(c for c in dt if c.isdigit())
                day_key = int(digits[:8]) if len(digits) >= 8 else 0
                return (
                    int(x.get("id_filial") or 0),
                    -day_key,
                    -float(x.get("valor") or 0),
                )

            usos_by[fid].sort(key=_uso_sort_key)

    summary_rows = query_dict(
        f"""
        SELECT
            count() AS total,
            countIf(status = 'Suspeito') AS suspeitos,
            countIf(status = 'Normal') AS normais,
            sum(usado_mes) AS usado_total,
            sum(usado_prazo) AS usado_prazo_total,
            sum(usado_vale) AS usado_vale_total,
            sum(limite_total) AS limite_total,
            sum(limite_prazo) AS limite_prazo_total,
            sum(limite_vale) AS limite_vale_total
        FROM {MART_RT_DB}.mart_fraud_credito_funcionario_resumo FINAL
        WHERE id_empresa = %(id_empresa)s
          AND ano_mes = %(ano_mes)s
          {entidade_sql}
        """,
        {"id_empresa": int(id_empresa), "ano_mes": ym},
    )
    summary = summary_rows[0] if summary_rows else {}

    meses_rows = query_dict(
        f"""
        SELECT DISTINCT ano_mes
        FROM {MART_RT_DB}.mart_fraud_credito_funcionario_resumo FINAL
        WHERE id_empresa = %(id_empresa)s
        ORDER BY ano_mes DESC
        """,
        {"id_empresa": int(id_empresa)},
    )
    meses_disponiveis = [int(r["ano_mes"]) for r in meses_rows if r.get("ano_mes") is not None]
    if ym not in meses_disponiveis:
        meses_disponiveis = sorted(set(meses_disponiveis + [ym]), reverse=True)

    funcionarios = []
    for r in rows:
        fid = int(r["id_funcionario"])
        motivos_raw = r.get("motivos") or "[]"
        try:
            motivos = json.loads(motivos_raw) if isinstance(motivos_raw, str) else list(motivos_raw or [])
        except Exception:
            motivos = []
        pub = r.get("published_at")
        limite_prazo = float(r.get("limite_prazo") or 0)
        limite_vale = float(r.get("limite_vale") or 0)
        limite_total = float(r.get("limite_total") or (limite_prazo + limite_vale))
        usado_prazo = float(r.get("usado_prazo") or 0)
        usado_vale = float(r.get("usado_vale") or 0)
        usado_mes = float(r.get("usado_mes") or (usado_prazo + usado_vale))
        funcionarios.append({
            "id_funcionario": fid,
            "id_filial": r.get("id_filial_ref"),
            "id_entidade": r.get("id_entidade") or None,
            "nome": r.get("nome_funcionario") or "",
            "cpf": r.get("cpf") or "",
            "ativo": bool(int(r.get("ativo") or 0)),
            "limite_prazo": limite_prazo,
            "limite_vale": limite_vale,
            "limite_total": limite_total,
            "limite": limite_total,
            "vales_cadastro": float(r.get("vales_cadastro") or 0),
            "usado_prazo": usado_prazo,
            "usado_vale": usado_vale,
            "usado_mes": usado_mes,
            "saldo_prazo": float(r.get("saldo_prazo") or max(limite_prazo - usado_prazo, 0)),
            "saldo_vale": float(r.get("saldo_vale") or max(limite_vale - usado_vale, 0)),
            "saldo_restante": float(r.get("saldo_restante") or max(limite_total - usado_mes, 0)),
            "qtd_usos_mes": int(r.get("qtd_usos_mes") or 0),
            "max_usos_mesmo_dia": int(r.get("max_usos_mesmo_dia") or 0),
            "status": r.get("status") or "Normal",
            "motivos": motivos,
            "usos": usos_by.get(fid, []),
            "refreshed_at": pub.isoformat() if isinstance(pub, datetime) else (str(pub) if pub else None),
        })

    return {
        "ano_mes": ym,
        "meses_disponiveis": meses_disponiveis,
        "source": "clickhouse",
        "summary": {
            "total": int(summary.get("total") or 0),
            "suspeitos": int(summary.get("suspeitos") or 0),
            "normais": int(summary.get("normais") or 0),
            "usado_total": float(summary.get("usado_total") or 0),
            "usado_prazo_total": float(summary.get("usado_prazo_total") or 0),
            "usado_vale_total": float(summary.get("usado_vale_total") or 0),
            "limite_total": float(summary.get("limite_total") or 0),
            "limite_prazo_total": float(summary.get("limite_prazo_total") or 0),
            "limite_vale_total": float(summary.get("limite_vale_total") or 0),
        },
        "funcionarios": funcionarios,
        "disclaimer": (
            "Funcionários: ENTIDADES grupo 12 ativas na filial selecionada. "
            "Limites LIMITE + LIMITE_VALE. Usos: CONTASRECEBER da entidade em toda a empresa "
            "(pode gastar em qualquer posto). Documento = NF-e/NFC-e."
        ),
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
    "fraud_troca_forma_pgto",
    "fraud_troca_forma_pgto_kpis",
    "risk_top_employees",
    "finance_kpis",
    "finance_receipts_by_day",
    "streaming_health",
    "goals_today",
    "monthly_goal_projection",
    "customers_summary_paginated",
    "customers_top",
    "customers_rfm_snapshot",
    "customers_delinquency_overview",
    "customers_churn_bundle",
    "customers_churn_snapshot_meta",
    "customer_churn_drilldown",
    "anonymous_retention_overview",
    "finance_series",
    "finance_aging_overview",
    "risk_kpis",
    "risk_series",
    "risk_last_events",
    "risk_by_turn_local",
    "risk_data_window",
    "risk_model_coverage",
    "payments_anomalies",
    "sales_operational_current",
    "commercial_window_coverage",
    "health_score_latest",
    "insights_base",
    "operational_score",
    "jarvis_briefing",
    "fraud_credito_funcionario",
}
