from __future__ import annotations

from datetime import date, datetime, timezone
import copy
import logging
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi import HTTPException
from pydantic import BaseModel, Field

from app.business_time import business_clock_payload, resolve_business_date
from app.db_compat import SNAPSHOT_FALLBACK_ERRORS
from app.deps import get_current_claims
from app.scope import resolve_scope, resolve_scope_filters, accessible_branch_ids, primary_branch_id
from app import repos_mart
from app import repos_auth
from app.services import snapshot_cache
from app.services.jarvis_ai import ai_usage_summary, generate_jarvis_ai_plans
from app.services.telegram import send_telegram_alert
from app.schemas_bi import GoalTargetRequest

router = APIRouter(prefix="/bi", tags=["bi"])
logger = logging.getLogger(__name__)
ROUTE_SNAPSHOT_FALLBACK_ERRORS = SNAPSHOT_FALLBACK_ERRORS + (TimeoutError,)


def _normalize_branch_scope(branch_scope: Optional[int | List[int]]) -> List[int]:
    if isinstance(branch_scope, list):
        return [int(value) for value in branch_scope if value is not None]
    if branch_scope is None:
        return []
    return [int(branch_scope)]


def _build_snapshot_context(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date],
    branch_scope: Optional[int | List[int]],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "dt_ref": dt_ref.isoformat() if dt_ref else None,
        "branch_ids": _normalize_branch_scope(branch_scope),
    }
    if extra:
        context.update(extra)
    return context


def _effective_commercial_window(
    role: str,
    tenant_id: int,
    filial_scope: Optional[int | List[int]],
    dt_ini: date,
    dt_fim: date,
) -> tuple[Dict[str, Any], date, date]:
    coverage = repos_mart.commercial_window_coverage(role, tenant_id, filial_scope, dt_ini, dt_fim)
    return (
        coverage,
        coverage.get("effective_dt_ini") or dt_ini,
        coverage.get("effective_dt_fim") or dt_fim,
    )


def _with_fallback_state(
    payload: Dict[str, Any],
    *,
    fallback_state: str,
    message: str,
    data_state: str = "transient_unavailable",
) -> Dict[str, Any]:
    annotated = copy.deepcopy(payload)
    annotated["data_state"] = annotated.get("data_state") or data_state
    fallback_meta = annotated.get("_fallback_meta") if isinstance(annotated.get("_fallback_meta"), dict) else {}
    annotated["_fallback_meta"] = {
        "fallback_state": fallback_state,
        "data_state": data_state,
        "message": message,
        **fallback_meta,
    }
    return annotated


def _with_cached_response(
    scope_key: str,
    role: str,
    tenant_id: int,
    branch_scope: Optional[int | List[int]],
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date],
    compute: Callable[[], Dict[str, Any]],
    extra_context: Optional[Dict[str, Any]] = None,
    safe_fallback: Optional[Callable[[], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    context = _build_snapshot_context(dt_ini, dt_fim, dt_ref, branch_scope, extra_context)
    scope_signature = snapshot_cache.build_scope_signature(context)
    branch_for_cache = primary_branch_id(branch_scope)

    def safe_read_snapshot_record() -> Optional[Dict[str, Any]]:
        try:
            return snapshot_cache.read_snapshot_record(role, tenant_id, branch_for_cache, scope_key, scope_signature)
        except ROUTE_SNAPSHOT_FALLBACK_ERRORS as exc:
            logger.warning(
                "Snapshot cache unavailable for %s tenant=%s while reading: %s",
                scope_key,
                tenant_id,
                exc.__class__.__name__,
                exc_info=exc,
            )
            return None

    def safe_read_latest_compatible_snapshot_record() -> Optional[Dict[str, Any]]:
        try:
            return snapshot_cache.read_latest_compatible_snapshot_record(role, tenant_id, branch_for_cache, scope_key)
        except ROUTE_SNAPSHOT_FALLBACK_ERRORS as exc:
            logger.warning(
                "Compatible snapshot cache unavailable for %s tenant=%s while reading: %s",
                scope_key,
                tenant_id,
                exc.__class__.__name__,
                exc_info=exc,
            )
            return None

    def safe_hot_route_guard() -> Dict[str, Any]:
        try:
            return snapshot_cache.get_hot_route_guard(tenant_id)
        except ROUTE_SNAPSHOT_FALLBACK_ERRORS as exc:
            logger.warning(
                "Snapshot guard unavailable for %s tenant=%s: %s",
                scope_key,
                tenant_id,
                exc.__class__.__name__,
                exc_info=exc,
            )
            return {
                "protect_reads": True,
                "etl_running": False,
                "tenant_lock_available": None,
                "lock_waiters": 0,
                "long_running_queries": 0,
                "reasons": ["guard_unavailable"],
            }

    def safe_write_snapshot(payload: Dict[str, Any]) -> Optional[Any]:
        try:
            return snapshot_cache.write_snapshot(
                role,
                tenant_id,
                branch_for_cache,
                scope_key,
                scope_signature,
                context,
                payload,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Snapshot cache unavailable for %s tenant=%s while writing: %s",
                scope_key,
                tenant_id,
                exc.__class__.__name__,
                exc_info=exc,
            )
            return None

    def attach_scope_meta(
        payload: Dict[str, Any],
        *,
        matched_signature: Optional[str],
        exact_scope_match: bool,
    ) -> Dict[str, Any]:
        annotated = copy.deepcopy(payload)
        annotated["_scope"] = {
            "route_key": scope_key,
            "signature": scope_signature,
            "matched_signature": matched_signature,
            "exact_scope_match": exact_scope_match,
            "tenant_id": tenant_id,
            "branch_scope": _normalize_branch_scope(branch_scope),
            "context": context,
        }
        return annotated

    bypass_snapshot = snapshot_cache.route_snapshot_is_bypassed(scope_key)
    if bypass_snapshot:
        guard_state = safe_hot_route_guard()
        try:
            payload = compute()
        except ROUTE_SNAPSHOT_FALLBACK_ERRORS as exc:
            logger.warning(
                "Live BI read unavailable for %s tenant=%s: %s",
                scope_key,
                tenant_id,
                exc.__class__.__name__,
                exc_info=exc,
            )
            if safe_fallback is not None:
                payload = safe_fallback()
                fallback_overrides = payload.pop("_fallback_meta", {}) if isinstance(payload.get("_fallback_meta"), dict) else {}
                payload = attach_scope_meta(payload, matched_signature=None, exact_scope_match=True)
                payload["_snapshot_cache"] = {
                    "source": "live",
                    "scope_key": scope_key,
                    "mode": "live_unavailable",
                    "reason": exc.__class__.__name__,
                    "signature": scope_signature,
                    "matched_signature": None,
                    "exact_scope_match": True,
                    "updated_at": None,
                    "age_seconds": None,
                    "busy_reasons": list(guard_state.get("reasons") or []),
                    "message": "A leitura ao vivo desta tela ficou indisponível agora. A resposta mantém indisponibilidade explícita em vez de reaproveitar snapshot stale.",
                } | fallback_overrides
                return payload
            raise

        payload = attach_scope_meta(payload, matched_signature=scope_signature, exact_scope_match=True)
        generated_at = datetime.now(timezone.utc).isoformat()
        payload["_snapshot_cache"] = {
            "source": "live",
            "scope_key": scope_key,
            "mode": "cache_bypassed",
            "reason": "truth_over_performance",
            "signature": scope_signature,
            "matched_signature": None,
            "exact_scope_match": True,
            "updated_at": generated_at,
            "age_seconds": 0.0,
            "busy_reasons": list(guard_state.get("reasons") or []),
            "message": None,
        }
        return payload

    cached_record = safe_read_snapshot_record()
    guard_state = safe_hot_route_guard()
    etl_running = bool(guard_state.get("etl_running"))
    protect_reads = bool(guard_state.get("protect_reads"))
    guard_reasons = list(guard_state.get("reasons") or [])
    if etl_running and "etl_running" not in guard_reasons:
        guard_reasons.append("etl_running")
    snapshot_updated_at = cached_record.get("updated_at") if cached_record is not None else None
    snapshot_fresh = cached_record is not None and snapshot_cache.snapshot_is_fresh(snapshot_updated_at, scope_key)

    def fallback_meta(
        *,
        mode: str,
        reason: str,
        message: str,
        refresh_scheduled: bool = False,
        exact_scope_match: bool = False,
    ) -> Dict[str, Any]:
        return {
            "source": "fallback",
            "scope_key": scope_key,
            "mode": mode,
            "reason": reason,
            "signature": scope_signature,
            "matched_signature": None,
            "exact_scope_match": exact_scope_match,
            "refresh_scheduled": refresh_scheduled,
            "busy_reasons": guard_reasons,
            "message": message,
        }

    def build_cached_payload(
        record: Dict[str, Any],
        mode: str,
        reason: str,
        *,
        message: Optional[str],
        refresh_scheduled: bool = False,
        exact_scope_match: bool = True,
    ) -> Dict[str, Any]:
        payload = attach_scope_meta(
            record["snapshot_data"],
            matched_signature=record.get("scope_signature"),
            exact_scope_match=exact_scope_match,
        )
        updated_at = record.get("updated_at")
        payload["_snapshot_cache"] = {
            "source": "snapshot",
            "scope_key": scope_key,
            "mode": mode,
            "reason": reason,
            "signature": scope_signature,
            "matched_signature": record.get("scope_signature"),
            "exact_scope_match": exact_scope_match,
            "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else None,
            "age_seconds": snapshot_cache.snapshot_age_seconds(updated_at),
            "refresh_after_seconds": snapshot_cache.snapshot_refresh_after_seconds(scope_key),
            "refresh_scheduled": refresh_scheduled,
            "busy_reasons": guard_reasons,
            "message": message,
        }
        return payload

    def refresh_snapshot() -> None:
        payload = compute()
        safe_write_snapshot(payload)

    def safe_refresh_snapshot_async() -> bool:
        try:
            return snapshot_cache.refresh_snapshot_async(scope_key, tenant_id, scope_signature, refresh_snapshot)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Snapshot refresh scheduling failed for %s tenant=%s: %s",
                scope_key,
                tenant_id,
                exc.__class__.__name__,
                exc_info=exc,
            )
            return False

    if cached_record is not None:
        if protect_reads:
            return build_cached_payload(
                cached_record,
                mode="protected_snapshot",
                reason=guard_reasons[0] if guard_reasons else "busy",
                message="Mostrando a última consolidação enquanto a base termina de processar novas leituras.",
            )
        if snapshot_fresh:
            return build_cached_payload(
                cached_record,
                mode="fresh_snapshot",
                reason="snapshot_fresh",
                message=None,
            )
        refresh_scheduled = safe_refresh_snapshot_async()
        return build_cached_payload(
            cached_record,
            mode="refreshing",
            reason="background_refresh",
            message="Mostrando a última consolidação enquanto a atualização segue em segundo plano. Você pode continuar usando a tela e tentar atualizar em instantes.",
            refresh_scheduled=refresh_scheduled,
        )

    compatible_record = safe_read_latest_compatible_snapshot_record() if protect_reads else None
    if compatible_record is not None:
        return build_cached_payload(
            compatible_record,
            mode="protected_stale_snapshot",
            reason="stale_snapshot_fallback",
            message="Mostrando a última consolidação compatível enquanto a base termina de processar o recorte atual.",
            exact_scope_match=False,
        )

    if safe_fallback is not None and protect_reads:
        payload = safe_fallback()
        fallback_overrides = payload.pop("_fallback_meta", {}) if isinstance(payload.get("_fallback_meta"), dict) else {}
        payload = attach_scope_meta(payload, matched_signature=None, exact_scope_match=False)
        payload["_snapshot_cache"] = {
            **fallback_meta(
                mode="protected_unavailable",
                reason="transient_snapshot_unavailable",
                message="A leitura consolidada deste recorte ainda não ficou pronta e a base está protegida agora. A tela mostra indisponibilidade transitória em vez de assumir zero real.",
            ),
            **fallback_overrides,
        }
        return payload

    if protect_reads:
        return {
            "data_state": "transient_unavailable",
            "_scope": {
                "route_key": scope_key,
                "signature": scope_signature,
                "matched_signature": None,
                "exact_scope_match": False,
                "tenant_id": tenant_id,
                "branch_scope": _normalize_branch_scope(branch_scope),
                "context": context,
            },
            "_snapshot_cache": fallback_meta(
                mode="protected_unavailable",
                reason="transient_snapshot_unavailable",
                message="A leitura consolidada deste recorte ainda não ficou pronta e a base está protegida agora.",
            ),
        }

    try:
        payload = compute()
    except ROUTE_SNAPSHOT_FALLBACK_ERRORS as exc:
        logger.warning(
            "Returning cached snapshot for %s tenant=%s due to %s",
            scope_key,
            tenant_id,
            exc.__class__.__name__,
            exc_info=exc,
        )
        if cached_record is not None:
            return build_cached_payload(
                cached_record,
                mode="recoverable_error",
                reason=str(exc),
                message="Mostrando a última consolidação disponível para evitar interrupção enquanto a leitura ao vivo se recupera.",
            )
        if safe_fallback is not None:
            payload = safe_fallback()
            fallback_overrides = payload.pop("_fallback_meta", {}) if isinstance(payload.get("_fallback_meta"), dict) else {}
            payload = attach_scope_meta(payload, matched_signature=None, exact_scope_match=True)
            payload["_snapshot_cache"] = fallback_meta(
                mode="recoverable_error",
                reason=str(exc),
                message="A leitura ao vivo ficou indisponível por um erro recuperável. Mantivemos um contrato seguro para não interromper a operação.",
            ) | fallback_overrides
            return payload
        raise
    updated_at = safe_write_snapshot(payload)
    payload = attach_scope_meta(payload, matched_signature=scope_signature, exact_scope_match=True)
    payload["_snapshot_cache"] = {
        "source": "live",
        "scope_key": scope_key,
        "mode": "cold_miss_sync" if cached_record is None else "live_computed",
        "reason": "snapshot_cold_miss" if cached_record is None else "snapshot_refreshed",
        "signature": scope_signature,
        "matched_signature": scope_signature,
        "exact_scope_match": True,
        "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else None,
        "age_seconds": 0.0 if updated_at is not None else None,
        "busy_reasons": [],
        "message": None,
    }
    return payload


def _raise_auth_error(exc: repos_auth.AuthError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


def _missing_finance_aging(dt_ref: Optional[date]) -> Dict[str, Any]:
    return {
        "requested_dt_ref": dt_ref.isoformat() if dt_ref else None,
        "effective_dt_ref": None,
        "coverage_start_dt_ref": None,
        "coverage_end_dt_ref": None,
        "precision_mode": "missing",
        "snapshot_status": "missing",
        "source_table": "mart.finance_aging_daily",
        "source_kind": "missing",
        "latest_updated_at": None,
        "row_count": 0,
        "dt_ref": dt_ref.isoformat() if dt_ref else None,
        "receber_total_aberto": None,
        "receber_total_vencido": None,
        "pagar_total_aberto": None,
        "pagar_total_vencido": None,
        "bucket_0_7": None,
        "bucket_8_15": None,
        "bucket_16_30": None,
        "bucket_31_60": None,
        "bucket_60_plus": None,
        "top5_concentration_pct": None,
        "data_gaps": True,
    }


def _missing_churn_snapshot(dt_ref: Optional[date]) -> Dict[str, Any]:
    requested = dt_ref.isoformat() if dt_ref else None
    return {
        "requested_dt_ref": requested,
        "effective_dt_ref": None,
        "coverage_start_dt_ref": None,
        "coverage_end_dt_ref": None,
        "precision_mode": "missing",
        "snapshot_status": "missing",
        "source_table": "mart.customer_churn_risk_daily",
        "source_kind": "missing",
        "latest_updated_at": None,
        "row_count": 0,
    }


def _safe_dashboard_home_payload(
    tenant_id: int,
    branch_scope: Optional[int | List[int]],
    dt_ini: date,
    dt_fim: date,
    dt_ref: date,
) -> Dict[str, Any]:
    return _with_fallback_state(
        {
        "scope": {
            "id_empresa": tenant_id,
            "id_filial": primary_branch_id(branch_scope),
            "id_filiais": _normalize_branch_scope(branch_scope),
            "filial_label": "Leitura consolidada em preparação",
            "dt_ini": dt_ini.isoformat(),
            "dt_fim": dt_fim.isoformat(),
            "requested_dt_ref": dt_ref.isoformat(),
        },
            "overview": {
                "sales": {
                "kpis": {"faturamento": None, "margem": None, "ticket_medio": None, "devolucoes": None},
                "by_day": [],
                "by_hour": [],
                "top_products": [],
                "top_groups": [],
                "top_employees": [],
                "reading_status": "preparing",
                "freshness": {"mode": "preparing"},
                "operational_sync": None,
                "data_state": "transient_unavailable",
            },
            "insights_generated": [],
            "fraud": {
                "operational": {
                    "kpis": {"cancelamentos": None, "valor_cancelado": None},
                    "window": {"rows": None},
                    "data_state": "transient_unavailable",
                },
                "modeled_risk": {
                    "kpis": {"total_eventos": None, "eventos_alto_risco": None, "impacto_total": None, "score_medio": None},
                    "window": {"rows": None},
                    "data_state": "transient_unavailable",
                },
            },
            "risk": {
                "kpis": {"total_eventos": None, "eventos_alto_risco": None, "impacto_total": None, "score_medio": None},
                "window": {"rows": None},
                "data_state": "transient_unavailable",
            },
            "cash": {"historical": {"source_status": "unavailable"}, "live_now": {"source_status": "unavailable"}},
            "jarvis": {
                "title": "Leitura consolidada em atualização",
                "headline": "Os dados estão sendo atualizados.",
                "summary": "A operação continua acessível, mas esta tela vai exibir a leitura consolidada assim que a atualização terminar.",
                "impact_label": "Aguardando consolidação",
                "action": "Acompanhar a atualização e retomar a análise em seguida.",
                "priority": "Aguardar",
                "status": "warn",
                "primary_kind": None,
                "primary_shortcut": None,
                "evidence": [],
                "highlights": [],
                "secondary_focus": [],
                "signals": {
                    "peak_hours": {
                        "source_status": "unavailable",
                        "window_days": 0,
                        "dt_ini": None,
                        "dt_fim": None,
                        "peak_hours": [],
                        "off_peak_hours": [],
                        "recommendations": {"peak": None, "off_peak": None},
                    },
                    "declining_products": {
                        "source_status": "unavailable",
                        "recent_window": {"dt_ini": None, "dt_fim": None},
                        "prior_window": {"dt_ini": None, "dt_fim": None},
                        "thresholds": {"min_prior_revenue": 1000.0, "min_absolute_drop": 300.0, "min_decline_pct": -15.0},
                        "items": [],
                    },
                },
            },
        },
        "churn": {
            "top_risk": [],
            "summary": {"total_top_risk": None, "avg_churn_score": None, "revenue_at_risk_30d": None},
            "snapshot_meta": _missing_churn_snapshot(dt_ref),
        },
        "finance": {"aging": _missing_finance_aging(dt_ref)},
        "cash": {
            "source_status": "unavailable",
            "summary": "A leitura consolidada do caixa ainda não ficou pronta.",
            "operational_sync": None,
            "freshness": {"mode": "preparing"},
            "definitions": repos_mart.cash_definitions(),
            "historical": {"source_status": "unavailable", "summary": "Histórico do caixa em preparação.", "kpis": {}, "payment_mix": [], "top_turnos": [], "cancelamentos": [], "by_day": []},
            "live_now": {"source_status": "unavailable", "summary": "Monitor operacional em preparação.", "kpis": {}, "open_boxes": [], "stale_boxes": [], "payment_mix": [], "cancelamentos": [], "alerts": []},
            "open_boxes": [],
            "stale_boxes": [],
            "payment_mix": [],
            "cancelamentos": [],
            "alerts": [],
        },
        "notifications_unread": 0,
        "operational_sync": None,
        "freshness": {"mode": "preparing"},
        },
        fallback_state="preparing",
        message="O dashboard geral ainda está preparando a primeira leitura consolidada confiável para este recorte.",
    )


def _safe_fraud_overview_payload(tenant_id: int, dt_ini: date, dt_fim: date, dt_ref: date) -> Dict[str, Any]:
    requested_window = {
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "dt_ref": dt_ref.isoformat(),
    }
    operational = {
        "kind": "operational",
        "kpis": {"cancelamentos": None, "valor_cancelado": None},
        "window": {"rows": 0, "dt_ini": dt_ini.isoformat(), "dt_fim": dt_fim.isoformat()},
    }
    modeled = {
        "kind": "modeled",
        "kpis": {"total_eventos": None, "eventos_alto_risco": None, "impacto_total": None, "score_medio": None},
        "window": {"min_data_key": None, "max_data_key": None, "rows": None},
        "coverage": {
            "status": "unavailable",
            "covered_fully": False,
            "covered_days": 0,
            "requested_days": max((dt_fim - dt_ini).days + 1, 0),
            "message": "A janela modelada ainda está sendo preparada.",
        },
    }
    return _with_fallback_state(
        {
            "requested_window": requested_window,
            "business_clock": business_clock_payload(tenant_id),
            "kpis": operational["kpis"],
            "by_day": [],
            "top_users": [],
            "last_events": [],
            "definitions": repos_mart.fraud_definitions(),
            "operational": operational,
            "risk_kpis": modeled["kpis"],
            "risk_by_day": [],
            "risk_window": modeled["window"],
            "model_coverage": modeled["coverage"],
            "modeled_risk": modeled,
            "risk_top_employees": [],
            "risk_by_turn_local": [],
            "risk_last_events": [],
            "insights": [],
            "payments_risk": [],
            "open_cash": {"source_status": "unavailable", "severity": "UNAVAILABLE", "summary": "Monitor operacional indisponível.", "items": []},
        },
        fallback_state="preparing",
        message="O antifraude ainda não tem um snapshot confiável para este recorte. A tela evita mostrar zero como se fosse dado real.",
    )


def _empty_sales_overview_payload() -> Dict[str, Any]:
    return {
        "kpis": {"faturamento": None, "margem": None, "ticket_medio": None, "devolucoes": None},
        "commercial_kpis": {
            "saidas": None,
            "qtd_saidas": None,
            "entradas": None,
            "qtd_entradas": None,
            "cancelamentos": None,
            "qtd_cancelamentos": None,
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
        "reading_status": "preparing",
        "operational_sync": None,
        "freshness": {"mode": "preparing"},
    }


def _safe_sales_overview_payload(
    role: str,
    tenant_id: int,
    filial: Optional[int],
    dt_ini: date,
    dt_fim: date,
    as_of: date,
) -> Dict[str, Any]:
    try:
        operational = repos_mart.sales_operational_current(role, tenant_id, filial, dt_ini, dt_fim, as_of)
    except ROUTE_SNAPSHOT_FALLBACK_ERRORS as exc:
        logger.warning(
            "Sales operational fallback unavailable for tenant=%s: %s",
            tenant_id,
            exc.__class__.__name__,
            exc_info=exc,
        )
        operational = None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Sales operational fallback failed for tenant=%s: %s",
            tenant_id,
            exc.__class__.__name__,
            exc_info=exc,
        )
        operational = None

    if operational:
        operational = copy.deepcopy(operational)
        operational.setdefault("monthly_evolution", [])
        operational.setdefault(
            "annual_comparison",
            {"current_year": None, "previous_year": None, "months": []},
        )
        operational["_fallback_meta"] = {
            "fallback_state": "operational_current",
            "message": "Mostrando a leitura operacional válida de hoje enquanto a consolidação completa desta tela termina.",
        }
        return operational

    return _with_fallback_state(
        _empty_sales_overview_payload(),
        fallback_state="preparing",
        message="Os dados consolidados de vendas deste recorte ainda estão em preparação. Para evitar zero falso, a tela volta a mostrar totais quando houver leitura confiável.",
    )


def _safe_customers_overview_payload(as_of: date) -> Dict[str, Any]:
    return _with_fallback_state(
        {
        "top_customers": [],
        "rfm": {"clientes_identificados": None, "ativos_7d": None, "em_risco_30d": None, "faturamento_90d": None},
        "delinquency": {
            "summary": {
                "clientes_em_aberto": None,
                "titulos_em_aberto": None,
                "valor_total": None,
                "max_dias_atraso": None,
            },
            "buckets": [],
            "customers": [],
            "dt_ref": as_of.isoformat(),
        },
        "churn_top": [],
        "churn_snapshot": _missing_churn_snapshot(as_of),
        "anonymous_retention": {
            "kpis": {
                "impact_estimated_7d": None,
                "trend_pct": None,
                "repeat_proxy_idx": None,
                "severity": "UNAVAILABLE",
                "recommendation": "A leitura de recorrência anônima ainda está sendo consolidada.",
            },
            "latest": [],
            "series": [],
            "breakdown_dow": [],
        },
        },
        fallback_state="preparing",
        message="Clientes ainda está preparando a leitura consolidada deste recorte. A resposta mantém o contrato sem assumir churn ou recorrência zerados.",
    )


def _safe_finance_overview_payload(
    tenant_id: int,
    as_of: date,
    include_series: bool,
    include_payments: bool,
    include_operational: bool,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "kpis": {"receber_aberto": None, "pagar_aberto": None, "saldo_liquido": None},
        "aging": _missing_finance_aging(as_of),
        "definitions": repos_mart.finance_definitions(),
        "business_clock": business_clock_payload(tenant_id),
    }
    if include_series:
        payload["by_day"] = []
    if include_payments:
        payload["payments"] = {
            "kpis": {
                "total_valor": None,
                "total_valor_prev": None,
                "delta_pct": None,
                "qtd_comprovantes": None,
                "row_count": None,
                "nonzero_rows": None,
                "unknown_valor": None,
                "unknown_share_pct": None,
                "source_status": "unavailable",
                "summary": "Os pagamentos ainda não estão consolidados para este recorte.",
                "mix": [],
            },
            "by_day": [],
            "by_turno": [],
            "anomalies": [],
        }
    if include_operational:
        payload["open_cash"] = {"source_status": "unavailable", "severity": "UNAVAILABLE", "summary": "Monitor operacional indisponível.", "items": []}
    return _with_fallback_state(
        payload,
        fallback_state="preparing",
        message="O financeiro ainda não tem uma consolidação confiável para este recorte. Os indicadores ficam explícitos como indisponíveis em vez de zerados.",
    )


def _safe_cash_overview_payload() -> Dict[str, Any]:
    return _with_fallback_state(
        {
        "source_status": "unavailable",
        "summary": "A leitura consolidada do caixa ainda não está pronta.",
        "operational_sync": None,
        "freshness": {"mode": "preparing"},
        "kpis": {
            "caixas_periodo": None,
            "dias_com_movimento": None,
            "ticket_medio": None,
            "total_vendas": None,
            "total_pagamentos": None,
            "total_cancelamentos": None,
            "qtd_cancelamentos": None,
            "caixas_com_cancelamento": None,
            "total_devolucoes": None,
            "qtd_devolucoes": None,
            "caixas_com_devolucao": None,
            "caixa_liquido": None,
        },
        "commercial": {
            "summary": "Leitura comercial em preparação.",
            "kpis": {
                "total_vendas": None,
                "qtd_vendas": None,
                "total_cancelamentos": None,
                "qtd_cancelamentos": None,
                "total_entradas": None,
                "total_pagamentos": None,
                "saldo_comercial": None,
                "caixas_periodo": None,
            },
            "by_day": [],
            "top_turnos": [],
        },
        "dre_summary": {
            "cards": [],
            "pending": [],
            "dt_ref": None,
        },
        "definitions": repos_mart.cash_definitions(),
        "historical": {
            "source_status": "unavailable",
            "summary": "Histórico do caixa em preparação.",
            "kpis": {
                "caixas_periodo": None,
                "dias_com_movimento": None,
                "ticket_medio": None,
                "total_vendas": None,
                "total_pagamentos": None,
                "total_cancelamentos": None,
                "qtd_cancelamentos": None,
                "caixas_com_cancelamento": None,
                "total_devolucoes": None,
                "qtd_devolucoes": None,
                "caixas_com_devolucao": None,
                "caixa_liquido": None,
            },
            "payment_mix": [],
            "top_turnos": [],
            "cancelamentos": [],
            "by_day": [],
        },
        "live_now": {
            "source_status": "unavailable",
            "summary": "Monitor operacional em preparação.",
            "kpis": {
                "total_turnos": None,
                "caixas_abertos_fonte": None,
                "caixas_abertos": None,
                "caixas_stale": None,
                "caixas_criticos": None,
                "caixas_alto_risco": None,
                "caixas_em_monitoramento": None,
                "total_vendas_abertas": None,
                "total_cancelamentos_abertos": None,
                "total_devolucoes_abertas": None,
                "caixa_liquido_aberto": None,
                "snapshot_ts": None,
                "latest_activity_ts": None,
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
        fallback_state="preparing",
        message="O caixa ainda está preparando a leitura consolidada do período. O payload sinaliza indisponibilidade transitória sem simular fechamento zerado.",
    )


def _safe_pricing_overview_payload(dt_ini: date, dt_fim: date, days_simulation: int) -> Dict[str, Any]:
    return _with_fallback_state(
        {
        "meta": {
            "dt_ini": dt_ini.isoformat(),
            "dt_fim": dt_fim.isoformat(),
            "days_window": max((dt_fim - dt_ini).days + 1, 1),
            "days_simulation": days_simulation,
        },
        "summary": {
            "fuel_types": None,
            "total_current_revenue_10d": None,
            "total_no_change_revenue_10d": None,
            "total_match_revenue_10d": None,
            "total_lost_if_no_change_10d": None,
            "total_match_vs_current_10d": None,
            "total_match_vs_no_change_10d": None,
        },
        "items": [],
        },
        fallback_state="preparing",
        message="A simulação de concorrência ainda não tem um snapshot consolidado compatível para este recorte.",
    )


def _safe_goals_overview_payload(tenant_id: int) -> Dict[str, Any]:
    return _with_fallback_state(
        {
            "business_clock": business_clock_payload(tenant_id),
            "leaderboard": [],
            "goals_today": [],
            "risk_top_employees": [],
            "monthly_projection": {
                "status": "preparing",
                "summary": {
                    "mtd_actual": None,
                    "avg_daily_mtd": None,
                    "projection_base": None,
                    "projection_adjusted": None,
                    "remaining_days": None,
                    "days_elapsed": None,
                    "total_days": None,
                },
                "goal": {"configured": False, "target_value": None, "gap_to_goal": None, "variation_pct": None, "required_daily_to_goal": None},
                "history": {"last_3_months": [], "average_last_3_months": None, "variation_vs_last_3m_pct": None},
                "forecast": {"confidence_level": "low", "confidence_label": "Baixa", "confidence_reason": "A leitura ainda está sendo preparada."},
                "drivers": [],
                "series_mtd": [],
            },
            "reading_status": "preparing",
        },
        fallback_state="preparing",
        message="Metas e equipe ainda está preparando a leitura consolidada deste recorte.",
    )


class CompetitorPriceItem(BaseModel):
    id_produto: int
    competitor_price: float = Field(..., gt=0)


class CompetitorPriceUpsertRequest(BaseModel):
    items: List[CompetitorPriceItem] = Field(default_factory=list)


@router.get("/filiais")
def get_filiais(
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, _ = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=None)
    can_list_all, allowed_branch_ids = accessible_branch_ids(claims, tenant)
    items = repos_mart.list_filiais(role, tenant)
    if not can_list_all:
        items = [item for item in items if int(item.get("id_filial") or 0) in allowed_branch_ids]
    return {"items": items}


# ------------------------
# Dashboard Geral
# ------------------------

@router.get("/dashboard/overview")
def dashboard_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    compact: bool = Query(False, description="Return only signals used by the executive home"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)

    if compact:
        return {
            "insights_generated": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=20),
            "risk": {
                "kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
                "window": repos_mart.risk_data_window(role, tenant, filial),
            },
            "jarvis": repos_mart.jarvis_briefing(role, tenant, filial, dt_ref=as_of),
        }

    return {
        "kpis": repos_mart.dashboard_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.dashboard_series(role, tenant, filial, dt_ini, dt_fim),
        "insights": repos_mart.insights_base(role, tenant, filial, dt_ini, dt_fim),
        "insights_generated": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=20),
        "payments": repos_mart.payments_overview(role, tenant, filial, dt_ini, dt_fim, anomaly_limit=8),
        "open_cash": repos_mart.open_cash_monitor(role, tenant, filial),
        "risk": {
            "kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
            "by_day": repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim),
            "window": repos_mart.risk_data_window(role, tenant, filial),
        },
        "operational_score": repos_mart.operational_score(role, tenant, filial, dt_ini, dt_fim),
        "health_score": repos_mart.health_score_latest(role, tenant, filial, as_of=as_of),
        "jarvis": repos_mart.jarvis_briefing(role, tenant, filial, dt_ref=as_of),
        "notifications_unread": repos_mart.notifications_unread_count(role, tenant, filial),
    }


@router.get("/dashboard/home")
def dashboard_home(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)
    return _with_cached_response(
        scope_key="dashboard_home",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=as_of,
        compute=lambda: repos_mart.dashboard_home_bundle(role, tenant, filial, dt_ini=dt_ini, dt_fim=dt_fim, dt_ref=as_of),
        safe_fallback=lambda: _safe_dashboard_home_payload(tenant, branch_scope, dt_ini, dt_fim, as_of),
    )


# ------------------------
# Vendas & Stores
# ------------------------

@router.get("/sales/overview")
def sales_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)

    def build_response() -> Dict[str, Any]:
        return repos_mart.sales_overview_bundle(role, tenant, filial, dt_ini, dt_fim, as_of=as_of)

    return _with_cached_response(
        scope_key="sales_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=as_of,
        compute=build_response,
        extra_context={"module": "sales"},
        safe_fallback=lambda: _safe_sales_overview_payload(role, tenant, filial, dt_ini, dt_fim, as_of),
    )


# ------------------------
# Anti-fraude
# ------------------------

@router.get("/fraud/overview")
def fraud_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)

    def build_response() -> Dict[str, Any]:
        operational_kpis = repos_mart.fraud_kpis(role, tenant, filial, dt_ini, dt_fim)
        operational_series = repos_mart.fraud_series(role, tenant, filial, dt_ini, dt_fim)
        top_users = repos_mart.fraud_top_users(role, tenant, filial, dt_ini, dt_fim, limit=10)
        last_events = repos_mart.fraud_last_events(role, tenant, filial, dt_ini, dt_fim, limit=30)
        risk_kpis = repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim)
        risk_series = repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim)
        risk_window = repos_mart.risk_data_window(role, tenant, filial)
        model_coverage = repos_mart.risk_model_coverage(dt_ini, dt_fim, risk_window)
        risk_top_employees = repos_mart.risk_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=10)
        risk_by_turn_local = repos_mart.risk_by_turn_local(role, tenant, filial, dt_ini, dt_fim, limit=10)
        risk_last_events = repos_mart.risk_last_events(role, tenant, filial, dt_ini, dt_fim, limit=30)
        payments_risk = repos_mart.payments_anomalies(role, tenant, filial, dt_ini, dt_fim, limit=20)
        open_cash = repos_mart.open_cash_monitor(role, tenant, filial)
        operational_summary = {
            "kind": "operational",
            "kpis": operational_kpis,
            "window": {
                "rows": len(last_events),
                "dt_ini": dt_ini.isoformat(),
                "dt_fim": dt_fim.isoformat(),
            },
        }
        modeled_summary = {
            "kind": "modeled",
            "kpis": risk_kpis,
            "window": risk_window,
            "coverage": model_coverage,
        }
        return {
            "requested_window": {
                "dt_ini": dt_ini.isoformat(),
                "dt_fim": dt_fim.isoformat(),
                "dt_ref": as_of.isoformat(),
            },
            "business_clock": business_clock_payload(tenant),
            "kpis": operational_kpis,
            "by_day": operational_series,
            "top_users": top_users,
            "last_events": last_events,
            "definitions": repos_mart.fraud_definitions(),
            "operational": operational_summary,
            "risk_kpis": risk_kpis,
            "risk_by_day": risk_series,
            "risk_window": risk_window,
            "model_coverage": model_coverage,
            "modeled_risk": modeled_summary,
            "risk_top_employees": risk_top_employees,
            "risk_by_turn_local": risk_by_turn_local,
            "risk_last_events": risk_last_events,
            "insights": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, limit=15),
            "payments_risk": payments_risk,
            "open_cash": open_cash,
        }

    return _with_cached_response(
        scope_key="fraud_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=as_of,
        compute=build_response,
        extra_context={"module": "fraud"},
        safe_fallback=lambda: _safe_fraud_overview_payload(tenant, dt_ini, dt_fim, as_of),
    )


@router.get("/risk/overview")
def risk_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    status: Optional[str] = Query(None, description="NOVO/LIDO/RESOLVIDO"),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)

    return {
        "kpis": repos_mart.risk_kpis(role, tenant, filial, dt_ini, dt_fim),
        "by_day": repos_mart.risk_series(role, tenant, filial, dt_ini, dt_fim),
        "window": repos_mart.risk_data_window(role, tenant, filial),
        "top_employees": repos_mart.risk_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=10),
        "by_turn_local": repos_mart.risk_by_turn_local(role, tenant, filial, dt_ini, dt_fim, limit=15),
        "last_events": repos_mart.risk_last_events(role, tenant, filial, dt_ini, dt_fim, limit=30),
        "insights": repos_mart.risk_insights(role, tenant, filial, dt_ini, dt_fim, status=status, limit=30),
        "operational_score": repos_mart.operational_score(role, tenant, filial, dt_ini, dt_fim),
    }


# ------------------------
# Clientes
# ------------------------

@router.get("/customers/overview")
def customers_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)

    def build_response() -> Dict[str, Any]:
        commercial_coverage, effective_dt_ini, effective_dt_fim = _effective_commercial_window(
            role,
            tenant,
            filial,
            dt_ini,
            dt_fim,
        )
        churn_bundle = repos_mart.customers_churn_bundle(role, tenant, filial, as_of=as_of, min_score=40, limit=10)
        churn_top = []
        for c in churn_bundle.get("top_risk") or []:
            freq_30 = int(c.get("frequency_30") or 0)
            freq_90 = int(c.get("frequency_90") or 0)
            mon_30 = float(c.get("monetary_30") or 0)
            mon_90 = float(c.get("monetary_90") or 0)
            churn_top.append(
                {
                    "id_cliente": c.get("id_cliente"),
                    "cliente_nome": c.get("cliente_nome"),
                    "churn_score": c.get("churn_score"),
                    "last_purchase": c.get("last_purchase"),
                    "compras_30d": freq_30,
                    "compras_60_30": max(0, freq_90 - freq_30),
                    "faturamento_30d": mon_30,
                    "faturamento_60_30": max(0.0, mon_90 - mon_30),
                    "revenue_at_risk_30d": c.get("revenue_at_risk_30d"),
                    "reasons": c.get("reasons"),
                    "recommendation": c.get("recommendation"),
                }
            )

        return {
            "top_customers": repos_mart.customers_top(role, tenant, filial, effective_dt_ini, effective_dt_fim, limit=15),
            "rfm": repos_mart.customers_rfm_snapshot(role, tenant, filial, as_of=as_of),
            "delinquency": repos_mart.customers_delinquency_overview(role, tenant, filial, as_of=as_of, limit=15),
            "churn_top": churn_top,
            "churn_snapshot": churn_bundle.get("snapshot_meta") or repos_mart.customers_churn_snapshot_meta(role, tenant, filial, as_of),
            "anonymous_retention": repos_mart.anonymous_retention_overview(role, tenant, filial, effective_dt_ini, effective_dt_fim),
            "commercial_coverage": commercial_coverage,
        }

    return _with_cached_response(
        scope_key="customers_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=as_of,
        compute=build_response,
        extra_context={"feature": "churn"},
        safe_fallback=lambda: _safe_customers_overview_payload(as_of),
    )


@router.get("/clients/churn")
def clients_churn(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_cliente: Optional[int] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    min_score: int = Query(60, ge=0, le=100),
    limit: int = Query(20, ge=1, le=100),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)
    churn_bundle = repos_mart.customers_churn_bundle(role, tenant, filial, as_of=as_of, min_score=min_score, limit=limit)
    top = churn_bundle.get("top_risk") or []

    drilldown = {"snapshot": {}, "series": []}
    if id_cliente is not None:
        drilldown = repos_mart.customer_churn_drilldown(
            role,
            tenant,
            filial,
            id_cliente=id_cliente,
            dt_ini=dt_ini,
            dt_fim=dt_fim,
            as_of=as_of,
        )

    return {
        "top_risk": top,
        "snapshot_meta": churn_bundle.get("snapshot_meta") or repos_mart.customers_churn_snapshot_meta(role, tenant, filial, as_of),
        "summary": churn_bundle.get("summary") or {
            "total_top_risk": len(top),
            "avg_churn_score": 0.0,
            "revenue_at_risk_30d": 0.0,
        },
        "drilldown": drilldown,
    }


@router.get("/clients/retention-anonymous")
def clients_retention_anonymous(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    commercial_coverage, effective_dt_ini, effective_dt_fim = _effective_commercial_window(
        role,
        tenant,
        filial,
        dt_ini,
        dt_fim,
    )
    payload = repos_mart.anonymous_retention_overview(role, tenant, filial, effective_dt_ini, effective_dt_fim)
    payload["commercial_coverage"] = commercial_coverage
    return payload


# ------------------------
# Financeiro
# ------------------------

@router.get("/finance/overview")
def finance_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    include_series: bool = Query(True),
    include_payments: bool = Query(True),
    include_operational: bool = Query(True),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)

    def build_response() -> Dict[str, Any]:
        finance_payload: Dict[str, Any] = {
            "kpis": repos_mart.finance_kpis(role, tenant, filial, dt_ini, dt_fim),
            "aging": repos_mart.finance_aging_overview(role, tenant, filial, as_of=as_of),
            "definitions": repos_mart.finance_definitions(),
            "business_clock": business_clock_payload(tenant),
        }
        response: Dict[str, Any] = {
            **finance_payload,
        }
        if include_series:
            response["by_day"] = repos_mart.finance_series(role, tenant, filial, dt_ini, dt_fim)
        if include_payments:
            response["payments"] = repos_mart.payments_overview(role, tenant, filial, dt_ini, dt_fim, anomaly_limit=10)
        if include_operational:
            response["open_cash"] = repos_mart.open_cash_monitor(role, tenant, filial)
        return response

    return _with_cached_response(
        scope_key="finance_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=as_of,
        compute=build_response,
        extra_context={
            "include_series": include_series,
            "include_payments": include_payments,
            "include_operational": include_operational,
        },
        safe_fallback=lambda: _safe_finance_overview_payload(tenant, as_of, include_series, include_payments, include_operational),
    )


@router.get("/payments/overview")
def payments_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return repos_mart.payments_overview(role, tenant, filial, dt_ini, dt_fim, anomaly_limit=30)


# ------------------------
# Caixa
# ------------------------

@router.get("/cash/overview")
def cash_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Legacy reference date; current server date is used when omitted"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return _with_cached_response(
        scope_key="cash_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=None,
        compute=lambda: repos_mart.cash_overview(role, tenant, filial, dt_ini=dt_ini, dt_fim=dt_fim),
        safe_fallback=_safe_cash_overview_payload,
    )


# ------------------------
# Precificacao Concorrencia
# ------------------------

@router.get("/pricing/competitor/overview")
def pricing_competitor_overview(
    dt_ini: date,
    dt_fim: date,
    days_simulation: int = Query(10, ge=1, le=60),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial_scope, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    filial = primary_branch_id(filial_scope)
    if filial is None:
        raise HTTPException(status_code=400, detail="id_filial is required for competitor pricing simulation")

    return _with_cached_response(
        scope_key="pricing_competitor_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=None,
        compute=lambda: repos_mart.competitor_pricing_overview(
            role,
            tenant,
            filial,
            dt_ini=dt_ini,
            dt_fim=dt_fim,
            days_simulation=days_simulation,
        ),
        extra_context={"days_simulation": days_simulation},
        safe_fallback=lambda: _safe_pricing_overview_payload(dt_ini, dt_fim, days_simulation),
    )


@router.get("/sync/status")
def sync_status(
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    tenant, _, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return snapshot_cache.last_consolidated_sync(tenant_id=tenant, branch_id=primary_branch_id(branch_scope))


@router.post("/pricing/competitor/prices")
def pricing_competitor_prices_upsert(
    payload: CompetitorPriceUpsertRequest,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER", "MANAGER"}:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        _raise_auth_error(exc)

    tenant, filial_scope, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    filial = primary_branch_id(filial_scope)
    if filial is None:
        raise HTTPException(status_code=400, detail="id_filial is required to save competitor prices")

    if not payload.items:
        return {"ok": True, "saved": 0}

    requested_ids = [item.id_produto for item in payload.items]
    allowed_ids = repos_mart.competitor_fuel_product_ids(role, tenant, filial, requested_ids)
    invalid_ids = [pid for pid in requested_ids if pid not in allowed_ids]
    if invalid_ids:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "competitor_invalid_product",
                "message": "Só é permitido registrar preços para combustíveis ativos.",
                "ids": invalid_ids,
            },
        )

    items = [{"id_produto": it.id_produto, "competitor_price": it.competitor_price} for it in payload.items]
    result = repos_mart.competitor_pricing_upsert(
        role,
        tenant,
        filial,
        items=items,
        updated_by=str(claims.get("sub") or ""),
    )
    return {"ok": True, **result}


# ------------------------
# Metas & Equipe
# ------------------------

@router.get("/goals/overview")
def goals_overview(
    dt_ini: date,
    dt_fim: date,
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, branch_scope = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    as_of = resolve_business_date(dt_ref, tenant)

    def build_response() -> Dict[str, Any]:
        commercial_coverage, effective_dt_ini, effective_dt_fim = _effective_commercial_window(
            role,
            tenant,
            filial,
            dt_ini,
            dt_fim,
        )
        return {
            "business_clock": business_clock_payload(tenant),
            "leaderboard": repos_mart.leaderboard_employees(role, tenant, filial, effective_dt_ini, effective_dt_fim, limit=15),
            "goals_today": repos_mart.goals_today(role, tenant, filial, goal_date=as_of),
            "risk_top_employees": repos_mart.risk_top_employees(role, tenant, filial, dt_ini, dt_fim, limit=15),
            "monthly_projection": repos_mart.monthly_goal_projection(role, tenant, filial, as_of=as_of),
            "commercial_coverage": commercial_coverage,
        }

    return _with_cached_response(
        scope_key="goals_overview",
        role=role,
        tenant_id=tenant,
        branch_scope=branch_scope,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        dt_ref=as_of,
        compute=build_response,
        extra_context={"goal_date": as_of.isoformat()},
        safe_fallback=lambda: _safe_goals_overview_payload(tenant),
    )


@router.post("/goals/target")
def goals_target(
    body: GoalTargetRequest,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER", "MANAGER"}:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        _raise_auth_error(exc)
    tenant, filial, branch_scope = resolve_scope_filters(
        claims,
        id_empresa_q=id_empresa,
        id_filial_q=id_filial,
        id_filiais_q=id_filiais,
    )
    target_filial = primary_branch_id(branch_scope)
    if target_filial is None:
        raise HTTPException(status_code=400, detail="Uma filial precisa estar selecionada para ajustar a meta.")

    goal_date = body.goal_month or resolve_business_date(None, tenant)

    result = repos_mart.upsert_goal(
        role,
        tenant,
        target_filial,
        goal_date,
        body.goal_type,
        body.target_value,
    )
    return {"goal": result}


# ------------------------
# Jarvis briefing
# ------------------------

@router.get("/jarvis/briefing")
def jarvis_briefing(
    dt_ref: date,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial_scope, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return repos_mart.jarvis_briefing(role, tenant, primary_branch_id(filial_scope), dt_ref=dt_ref)


@router.post("/jarvis/generate")
def jarvis_generate(
    dt_ref: date,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    force: bool = Query(False),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        _raise_auth_error(exc)
    tenant, filial_scope, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    filial = primary_branch_id(filial_scope)
    stats = generate_jarvis_ai_plans(role, tenant, filial, dt_ref=dt_ref, limit=limit, force=force)
    return {
        "ok": True,
        "id_empresa": tenant,
        "id_filial": filial,
        "dt_ref": dt_ref.isoformat(),
        "stats": stats,
    }


@router.get("/admin/ai-usage")
def admin_ai_usage(
    days: int = Query(30, ge=1, le=365),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER"}:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        _raise_auth_error(exc)

    tenant, filial_scope, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return ai_usage_summary(role, tenant, primary_branch_id(filial_scope), days=days)


@router.post("/admin/telegram/test")
def admin_telegram_test(
    dt_ref: Optional[date] = Query(None, description="Reference date used as simulated 'today'"),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    if role not in {"MASTER", "OWNER"}:
        raise HTTPException(status_code=403, detail="forbidden")

    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    payload = {
        "severity": "CRITICAL",
        "id_filial": filial,
        "event_type": "TELEGRAM_TEST",
        "event_time": resolve_business_date(dt_ref, tenant).isoformat(),
        "impacto_estimado": 0,
        "title": "Teste de alerta Telegram",
        "body": "Mensagem de teste enviada pelo endpoint /bi/admin/telegram/test",
        "url": "/dashboard",
        "business_clock": business_clock_payload(tenant),
    }
    result = send_telegram_alert(id_empresa=tenant, payload=payload, force=True)
    return {"ok": True, "id_empresa": tenant, "id_filial": filial, "result": result}


@router.get("/notifications")
def notifications_list(
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    unread_only: bool = Query(False),
    limit: int = Query(30, ge=1, le=200),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return {
        "items": repos_mart.notifications_list(role, tenant, filial, limit=limit, unread_only=unread_only),
        "unread": repos_mart.notifications_unread_count(role, tenant, filial),
    }


@router.post("/notifications/{notification_id}/read")
def notifications_mark_read(
    notification_id: int,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    row = repos_mart.notification_mark_read(role, tenant, filial, notification_id)
    return {"ok": True, "item": row}


@router.get("/notifications/unread-count")
def notifications_unread_count(
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    role = claims["role"]
    tenant, filial, _ = resolve_scope_filters(claims, id_empresa_q=id_empresa, id_filial_q=id_filial, id_filiais_q=id_filiais)
    return {"unread": repos_mart.notifications_unread_count(role, tenant, filial)}
