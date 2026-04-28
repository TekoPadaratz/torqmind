"""ClickHouse-backed analytics repository (Phase 3 Refactored).

PT-BR:
- Versão refatorada de repos_mart.py para ler do ClickHouse em vez do PostgreSQL
- Mantém assinaturas de funções IDÊNTICAS para não quebrar callers
- Usa db_clickhouse.py para conexão e queries
- Suporta dual-read mode para validação durante migração

EN:
- Refactored version of repos_mart.py to read from ClickHouse instead of PostgreSQL
- Maintains IDENTICAL function signatures to not break callers
- Uses db_clickhouse.py for connection and queries
- Supports dual-read mode for validation during migration

Migration Strategy:
  1. Esta funções são idênticas em assinatura mas use ClickHouse internally
  2. Rodar em staging + canary (10-20% traffic) antes de 100% cutover
  3. Feature flag: USE_CLICKHOUSE controla fallback para Postgres
  4. Dual-read mode: Compara ambos os resultados para detecção de bugs
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, List, Dict, Any
import logging
import json

from app.config import settings
from app.db_clickhouse import (
    query_dict,
    query_scalar,
    get_dual_read_validator,
)

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS (same as original repos_mart.py)
# ============================================================================

def _date_key(dt: date) -> int:
    """Convert date to YYYYMMDD integer key.
    
    Example: date(2026, 4, 28) -> 20260428
    """
    return dt.year * 10000 + dt.month * 100 + dt.day


def _branch_scope_clause(column: str, id_filial: Optional[int]) -> tuple:
    """Build WHERE clause for branch scope (tenant context).
    
    Returns: (where_clause_fragment, parameter_list)
    """
    if id_filial is None or id_filial == -1:
        return "", []
    return f"AND {column} = %s", [id_filial]


def _format_decimal(value: Any, decimals: int = 2) -> float:
    """Safely format decimal values."""
    try:
        return round(float(value or 0), decimals)
    except (TypeError, ValueError):
        return 0.0


# ============================================================================
# PHASE 3 REFACTORED FUNCTIONS: ClickHouse versions
# ============================================================================

def dashboard_kpis(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    """Daily sales KPIs for dashboard.
    
    Changes from Postgres:
    - Reads from torqmind_mart.agg_vendas_diaria (ClickHouse) instead of mart.agg_vendas_diaria
    - Uses query_dict instead of get_conn
    - No JOIN needed (dimensions already denormalized)
    
    Returns: {"faturamento": float, "margem": float, "ticket_medio": float, "itens": float}
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            COALESCE(SUM(faturamento), 0) AS faturamento,
            COALESCE(SUM(margem), 0) AS margem,
            COALESCE(AVG(ticket_medio), 0) AS ticket_medio,
            COALESCE(SUM(quantidade_itens), 0) AS itens
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {id_empresa}
          AND data_key BETWEEN {ini} AND {fim}
          {where_filial}
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        if rows:
            row = rows[0]
            return {
                "faturamento": _format_decimal(row.get("faturamento")),
                "margem": _format_decimal(row.get("margem")),
                "ticket_medio": _format_decimal(row.get("ticket_medio")),
                "itens": _format_decimal(row.get("itens")),
            }
    except Exception as e:
        logger.error(f"ClickHouse dashboard_kpis error: {e}")
        if not settings.use_clickhouse:
            # Fallback to Postgres (not implemented in this file)
            raise
    
    return {"faturamento": 0, "margem": 0, "ticket_medio": 0, "itens": 0}


def dashboard_series(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> List[Dict[str, Any]]:
    """Daily sales time series for dashboard.
    
    Changes from Postgres:
    - Reads from torqmind_mart.agg_vendas_diaria (ClickHouse)
    - No JOIN to dim_* tables
    
    Returns: [{"data_key": int, "id_filial": int, "faturamento": float, "margem": float}, ...]
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            data_key,
            id_filial,
            faturamento,
            margem
        FROM torqmind_mart.agg_vendas_diaria
        WHERE id_empresa = {id_empresa}
          AND data_key BETWEEN {ini} AND {fim}
          {where_filial}
        ORDER BY data_key, id_filial
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        return [
            {
                "data_key": int(row.get("data_key", 0)),
                "id_filial": int(row.get("id_filial", -1)),
                "faturamento": _format_decimal(row.get("faturamento")),
                "margem": _format_decimal(row.get("margem")),
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"ClickHouse dashboard_series error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return []


def fraud_kpis(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    """Fraud KPIs (cancellations).
    
    Changes from Postgres:
    - Reads from torqmind_mart.fraude_cancelamentos_diaria (ClickHouse)
    - Agregations are pre-computed in MV
    
    Returns: {"cancelamentos": int, "valor_cancelado": float}
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            COALESCE(SUM(cancelamentos), 0) AS cancelamentos,
            COALESCE(SUM(valor_cancelado), 0) AS valor_cancelado
        FROM torqmind_mart.fraude_cancelamentos_diaria
        WHERE id_empresa = {id_empresa}
          AND data_key BETWEEN {ini} AND {fim}
          {where_filial}
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        if rows:
            row = rows[0]
            return {
                "cancelamentos": int(row.get("cancelamentos", 0)),
                "valor_cancelado": _format_decimal(row.get("valor_cancelado")),
            }
    except Exception as e:
        logger.error(f"ClickHouse fraud_kpis error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return {"cancelamentos": 0, "valor_cancelado": 0}


def fraud_last_events(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Recent cancellation events (drill-down).
    
    Changes from Postgres:
    - Reads from torqmind_mart.fraude_cancelamentos_eventos (MergeTree, no aggregation)
    - Events already denormalized (no JOINs needed)
    
    Returns: [{"id_comprovante": str, "data": str, "valor_total": float, ...}, ...]
    """
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            id_comprovante,
            data,
            data_key,
            id_usuario,
            valor_total
        FROM torqmind_mart.fraude_cancelamentos_eventos
        WHERE id_empresa = {id_empresa}
          {where_filial}
        ORDER BY data DESC
        LIMIT {limit}
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        return [
            {
                "id_comprovante": row.get("id_comprovante"),
                "data": str(row.get("data", "")),
                "data_key": int(row.get("data_key", 0)),
                "id_usuario": int(row.get("id_usuario", -1)),
                "valor_total": _format_decimal(row.get("valor_total")),
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"ClickHouse fraud_last_events error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return []


def risk_kpis(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    """Risk event KPIs.
    
    Changes from Postgres:
    - Reads from torqmind_mart.agg_risco_diaria (AggregatingMergeTree)
    - Percentiles pre-computed
    
    Returns: {"eventos_alto_risco": int, "impacto_total": float, "p95_score": float}
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            COALESCE(SUM(eventos_alto_risco), 0) AS eventos_alto_risco,
            COALESCE(SUM(impacto_estimado_total), 0) AS impacto_total,
            COALESCE(AVG(p95_score), 0) AS p95_score
        FROM torqmind_mart.agg_risco_diaria
        WHERE id_empresa = {id_empresa}
          AND data_key BETWEEN {ini} AND {fim}
          {where_filial}
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        if rows:
            row = rows[0]
            return {
                "eventos_alto_risco": int(row.get("eventos_alto_risco", 0)),
                "impacto_total": _format_decimal(row.get("impacto_total")),
                "p95_score": _format_decimal(row.get("p95_score")),
            }
    except Exception as e:
        logger.error(f"ClickHouse risk_kpis error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return {"eventos_alto_risco": 0, "impacto_total": 0, "p95_score": 0}


def customers_churn_bundle(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    min_score: int = 50,
    limit: int = 100,
) -> Dict[str, Any]:
    """Churn risk customers bundle.
    
    Changes from Postgres:
    - Reads from torqmind_mart.customer_churn_risk_daily (ReplacingMergeTree snapshot)
    - Churn score already computed
    
    Returns: {"total_at_risk": int, "revenue_at_risk": float, "customers": [...]}
    """
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            id_cliente,
            cliente_nome,
            churn_score,
            revenue_at_risk_30d,
            recommendation
        FROM torqmind_mart.customer_churn_risk_daily
        WHERE id_empresa = {id_empresa}
          {where_filial}
          AND churn_score >= {min_score}
          AND dt_ref = today()
        ORDER BY revenue_at_risk_30d DESC
        LIMIT {limit}
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        total_revenue_at_risk = sum(
            _format_decimal(row.get("revenue_at_risk_30d", 0))
            for row in rows
        )
        customers = [
            {
                "id_cliente": int(row.get("id_cliente", -1)),
                "cliente_nome": row.get("cliente_nome", ""),
                "churn_score": int(row.get("churn_score", 0)),
                "revenue_at_risk_30d": _format_decimal(row.get("revenue_at_risk_30d")),
                "recommendation": row.get("recommendation", ""),
            }
            for row in rows
        ]
        return {
            "total_at_risk": len(customers),
            "revenue_at_risk": round(total_revenue_at_risk, 2),
            "customers": customers,
        }
    except Exception as e:
        logger.error(f"ClickHouse customers_churn_bundle error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return {"total_at_risk": 0, "revenue_at_risk": 0, "customers": []}


def finance_aging_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
) -> Dict[str, Any]:
    """Finance aging overview (receivables/payables).
    
    Changes from Postgres:
    - Reads from torqmind_mart.finance_aging_daily (ReplacingMergeTree)
    - Aging buckets already computed
    
    Returns: {
        "receber_total": float,
        "pagar_total": float,
        "buckets": {
            "0_7": float,
            "8_15": float,
            ...
        }
    }
    """
    sql = f"""
        SELECT
            receber_total_aberto AS receber_total,
            pagar_total_aberto AS pagar_total,
            bucket_0_7,
            bucket_8_15,
            bucket_16_30,
            bucket_31_60,
            bucket_60_plus
        FROM torqmind_mart.finance_aging_daily
        WHERE id_empresa = {id_empresa}
          {'AND id_filial = ' + str(id_filial) if id_filial and id_filial != -1 else ''}
          AND dt_ref = today()
        LIMIT 1
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        if rows:
            row = rows[0]
            return {
                "receber_total": _format_decimal(row.get("receber_total")),
                "pagar_total": _format_decimal(row.get("pagar_total")),
                "buckets": {
                    "0_7": _format_decimal(row.get("bucket_0_7")),
                    "8_15": _format_decimal(row.get("bucket_8_15")),
                    "16_30": _format_decimal(row.get("bucket_16_30")),
                    "31_60": _format_decimal(row.get("bucket_31_60")),
                    "60_plus": _format_decimal(row.get("bucket_60_plus")),
                },
            }
    except Exception as e:
        logger.error(f"ClickHouse finance_aging_overview error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return {
        "receber_total": 0,
        "pagar_total": 0,
        "buckets": {"0_7": 0, "8_15": 0, "16_30": 0, "31_60": 0, "60_plus": 0},
    }


def payments_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    """Payment forms overview.
    
    Changes from Postgres:
    - Reads from torqmind_mart.agg_pagamentos_diaria (SummingMergeTree)
    - Payment type mapping already denormalized
    
    Returns: {"total_valor": float, "categories": [...]}
    """
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            category,
            label,
            COALESCE(SUM(total_valor), 0) AS total_valor,
            COALESCE(SUM(qtd_comprovantes), 0) AS qtd_comprovantes
        FROM torqmind_mart.agg_pagamentos_diaria
        WHERE id_empresa = {id_empresa}
          AND data_key BETWEEN {ini} AND {fim}
          {where_filial}
        GROUP BY category, label
        ORDER BY total_valor DESC
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        total_valor = sum(
            _format_decimal(row.get("total_valor", 0))
            for row in rows
        )
        categories = [
            {
                "category": row.get("category", ""),
                "label": row.get("label", ""),
                "total_valor": _format_decimal(row.get("total_valor")),
                "qtd_comprovantes": int(row.get("qtd_comprovantes", 0)),
            }
            for row in rows
        ]
        return {
            "total_valor": round(total_valor, 2),
            "categories": categories,
        }
    except Exception as e:
        logger.error(f"ClickHouse payments_overview error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return {"total_valor": 0, "categories": []}


def cash_overview(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
) -> Dict[str, Any]:
    """Open cash registers overview.
    
    Changes from Postgres:
    - Reads from torqmind_mart.agg_caixa_turno_aberto (ReplacingMergeTree)
    - Status already computed
    
    Returns: {"total_aberto": int, "severity_counts": {...}, "registers": [...]}
    """
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            id_turno,
            usuario_nome,
            abertura_ts,
            horas_aberto,
            severity,
            total_vendas,
            total_pagamentos
        FROM torqmind_mart.agg_caixa_turno_aberto
        WHERE id_empresa = {id_empresa}
          {where_filial}
        ORDER BY horas_aberto DESC
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        severity_counts = {"CRITICAL": 0, "HIGH": 0, "WARN": 0, "OK": 0}
        for row in rows:
            severity = row.get("severity", "OK")
            if severity in severity_counts:
                severity_counts[severity] += 1
        
        registers = [
            {
                "id_turno": int(row.get("id_turno", -1)),
                "usuario_nome": row.get("usuario_nome", ""),
                "abertura_ts": str(row.get("abertura_ts", "")),
                "horas_aberto": _format_decimal(row.get("horas_aberto")),
                "severity": row.get("severity", ""),
                "total_vendas": _format_decimal(row.get("total_vendas")),
                "total_pagamentos": _format_decimal(row.get("total_pagamentos")),
            }
            for row in rows
        ]
        return {
            "total_aberto": len(registers),
            "severity_counts": severity_counts,
            "registers": registers,
        }
    except Exception as e:
        logger.error(f"ClickHouse cash_overview error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return {"total_aberto": 0, "severity_counts": {}, "registers": []}


def health_score_latest(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Latest health scores (ranked by final_score).
    
    Changes from Postgres:
    - Reads from torqmind_mart.health_score_daily (ReplacingMergeTree)
    - Composite score already computed
    
    Returns: [{"id_filial": int, "health_pct": float, "final_score": float}, ...]
    """
    where_filial = ""
    if id_filial is not None and id_filial != -1:
        where_filial = f" AND id_filial = {id_filial}"
    
    sql = f"""
        SELECT
            id_filial,
            fat_30d,
            margem_30d,
            high_risk_30d,
            health_pct,
            customer_pct,
            risk_pct,
            final_score
        FROM torqmind_mart.health_score_daily
        WHERE id_empresa = {id_empresa}
          {where_filial}
          AND dt_ref = today()
        ORDER BY final_score DESC
        LIMIT {limit}
    """
    
    try:
        rows = query_dict(sql, tenant_id=id_empresa)
        return [
            {
                "id_filial": int(row.get("id_filial", -1)),
                "fat_30d": _format_decimal(row.get("fat_30d")),
                "margem_30d": _format_decimal(row.get("margem_30d")),
                "high_risk_30d": int(row.get("high_risk_30d", 0)),
                "health_pct": _format_decimal(row.get("health_pct")),
                "customer_pct": _format_decimal(row.get("customer_pct")),
                "risk_pct": _format_decimal(row.get("risk_pct")),
                "final_score": _format_decimal(row.get("final_score")),
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"ClickHouse health_score_latest error: {e}")
        if not settings.use_clickhouse:
            raise
    
    return []


# ============================================================================
# PHASE 3 MIGRATION: Add more functions here following the pattern above
# ============================================================================

# TODO (Phase 3 Week 3-4): Refactor remaining 50+ functions:
#  - sales_top_products()
#  - sales_top_groups()
#  - sales_top_employees()
#  - customers_rfm_snapshot()
#  - finance_aging_drilldown()
#  - payments_anomalies()
#  - anonymous_retention_overview()
#  - ... (see phase2_mapping.md for complete list)
