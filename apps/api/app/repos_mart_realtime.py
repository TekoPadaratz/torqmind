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

    by_type = query_dict(f"""
        SELECT tipo_forma, label, category,
               sum(valor_total) AS valor_total, sum(qtd_transacoes) AS qtd_transacoes
        FROM {MART_RT_DB}.payments_by_type_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY tipo_forma, label, category
        ORDER BY valor_total DESC
    """, parameters={"id_empresa": id_empresa})

    total = sum(float(r.get("valor_total", 0) or 0) for r in by_type)
    return {
        "total": total,
        "by_type": by_type,
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

    fat = float(t.get("faturamento_turno") or 0)
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
        "total_cancelamentos": 0,
        "total_pagamentos": fat,
        "saldo_comercial": fat,
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
    params = {"id_empresa": id_empresa}

    # Open shifts
    turnos_raw = query_dict(f"""
        SELECT id_filial, id_turno, id_usuario, nome_operador,
               abertura_ts, fechamento_ts, is_aberto,
               faturamento_turno, qtd_vendas_turno
        FROM {MART_RT_DB}.cash_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
          AND is_aberto = 1
        ORDER BY abertura_ts DESC
        LIMIT 50
    """, parameters=params)

    # Top commercial turnos (all shifts in period)
    all_turnos_raw = query_dict(f"""
        SELECT id_filial, id_turno, id_usuario, nome_operador,
               abertura_ts, fechamento_ts, is_aberto,
               faturamento_turno, qtd_vendas_turno
        FROM {MART_RT_DB}.cash_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
        ORDER BY faturamento_turno DESC
        LIMIT 50
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
    payments = query_dict(f"""
        SELECT label, category, sum(valor_total) AS valor_total, sum(qtd_transacoes) AS qtd_transacoes
        FROM {MART_RT_DB}.payments_by_type_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY label, category
        ORDER BY valor_total DESC
    """, parameters=params)

    total_vendas = float(sales_kpi.get("total_vendas") or 0)
    total_cancelamentos = float(sales_kpi.get("total_cancelamentos") or 0)
    total_pagamentos = sum(float(p.get("valor_total") or 0) for p in payments)
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
        "source": "realtime",
        "realtime_source": _realtime_source(),
        "freshness": {"mode": "realtime", "source": _realtime_source(), "last_refresh": now_iso},
        "operational_sync": {"source": "realtime", "last_publish": now_iso},
    }


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
    """Recent risk events with operator/employee names."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    return query_dict(f"""
        SELECT id, id_filial, data_key, event_type, source,
               nome_operador, nome_funcionario, valor_total,
               impacto_estimado, score_risco, score_level, reasons
        FROM {MART_RT_DB}.risk_recent_events_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial} {date_range}
        ORDER BY id DESC
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
    """Top users by cancellation volume from risk_recent_events_rt."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    return query_dict(f"""
        SELECT nome_operador AS usuario_nome,
               count() AS cancelamentos,
               sum(valor_total) AS valor_cancelado
        FROM {MART_RT_DB}.risk_recent_events_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
          AND event_type = 'cancelamento'
        GROUP BY nome_operador
        ORDER BY valor_cancelado DESC
        LIMIT {limit}
    """, parameters={"id_empresa": id_empresa})


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
# INVENTORY (for analytics facade routing)
# ================================================================

REALTIME_FUNCTIONS = {
    "dashboard_kpis",
    "dashboard_series",
    "dashboard_home_bundle",
    "sales_overview_bundle",
    "sales_by_hour",
    "sales_top_products",
    "sales_top_groups",
    "payments_overview",
    "cash_overview",
    "open_cash_monitor",
    "fraud_kpis",
    "fraud_series",
    "fraud_top_users",
    "fraud_last_events",
    "finance_kpis",
    "streaming_health",
}
