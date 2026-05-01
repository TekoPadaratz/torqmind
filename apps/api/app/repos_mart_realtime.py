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
from app.db_clickhouse import query_dict, query_scalar

logger = logging.getLogger(__name__)

MART_RT_DB = "torqmind_mart_rt"
CURRENT_DB = "torqmind_current"


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
            sum(faturamento) AS faturamento,
            sum(qtd_vendas) AS qtd_vendas,
            if(sum(qtd_vendas) > 0, sum(faturamento) / sum(qtd_vendas), 0) AS ticket_medio,
            sum(qtd_clientes) AS qtd_clientes,
            sum(qtd_cancelamentos) AS qtd_cancelamentos,
            sum(valor_cancelado) AS valor_cancelado
        FROM {MART_RT_DB}.dashboard_home_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
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
            sum(faturamento) AS faturamento,
            sum(qtd_vendas) AS qtd_vendas,
            if(sum(qtd_vendas) > 0, sum(faturamento) / sum(qtd_vendas), 0) AS ticket_medio
        FROM {MART_RT_DB}.dashboard_home_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY dt
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
    """Full dashboard home payload."""
    kpis = dashboard_kpis(role, id_empresa, id_filial, dt_ini, dt_fim)
    series = dashboard_series(role, id_empresa, id_filial, dt_ini, dt_fim)
    return {"kpis": kpis, "series": series, "source": "realtime"}


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
    """Sales overview with KPIs, series, and top rankings."""
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)

    kpis_rows = query_dict(f"""
        SELECT
            sum(faturamento) AS faturamento,
            sum(qtd_vendas) AS qtd_vendas,
            if(sum(qtd_vendas) > 0, sum(faturamento) / sum(qtd_vendas), 0) AS ticket_medio,
            sum(qtd_itens) AS qtd_itens,
            sum(qtd_canceladas) AS qtd_canceladas,
            sum(valor_cancelado) AS valor_cancelado,
            sum(desconto_total) AS desconto_total,
            sum(margem_total) AS margem_total
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
    """, parameters={"id_empresa": id_empresa})

    series = query_dict(f"""
        SELECT dt, sum(faturamento) AS faturamento, sum(qtd_vendas) AS qtd_vendas
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY dt ORDER BY dt
    """, parameters={"id_empresa": id_empresa})

    return {
        "kpis": kpis_rows[0] if kpis_rows else {},
        "series": series,
        "source": "realtime",
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

    return query_dict(f"""
        SELECT id_produto, nome_produto, nome_grupo,
               sum(faturamento) AS faturamento, sum(qtd) AS qtd,
               sum(margem) AS margem
        FROM {MART_RT_DB}.sales_products_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {date_range} {filial}
        GROUP BY id_produto, nome_produto, nome_grupo
        ORDER BY faturamento DESC
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
    }


# ================================================================
# CASH / CAIXA
# ================================================================

def cash_overview(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Cash/shift overview."""
    filial = _branch_clause("id_filial", id_filial)

    turnos = query_dict(f"""
        SELECT id_filial, id_turno, id_usuario, nome_operador,
               abertura_ts, fechamento_ts, is_aberto,
               faturamento_turno, qtd_vendas_turno
        FROM {MART_RT_DB}.cash_overview_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}} {filial}
          AND is_aberto = 1
        ORDER BY abertura_ts DESC
        LIMIT 50
    """, parameters={"id_empresa": id_empresa})

    return {
        "turnos_abertos": turnos,
        "qtd_abertos": len(turnos),
        "source": "realtime",
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
    """Fraud/risk KPIs."""
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

    return rows[0] if rows else {"qtd_eventos": 0, "impacto_total": 0, "score_medio": 0}


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

    return {"aging": rows, "source": "realtime"}


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
    "fraud_last_events",
    "finance_kpis",
    "streaming_health",
}
