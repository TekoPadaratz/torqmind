from __future__ import annotations

import json
import logging
import os
import time
from contextlib import suppress
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from app.db import get_conn
from app.services.telegram import send_telegram_alert

TRACK_OPERATIONAL = "operational"
TRACK_RISK = "risk"
TRACK_FULL = "full"
SUPPORTED_TRACKS = frozenset({TRACK_OPERATIONAL, TRACK_RISK, TRACK_FULL})
PUBLICATION_MODE_NONE = "none"
PUBLICATION_MODE_FAST_PATH = "fast_path"
PUBLICATION_MODE_GLOBAL_REFRESH = "global_refresh"

TRACK_CYCLE_LOCKS: dict[str, tuple[tuple[int, int], ...]] = {
    TRACK_OPERATIONAL: ((62041, 230319),),
    TRACK_RISK: ((62041, 230320),),
    # Full keeps the legacy end-to-end cycle and blocks both dedicated lanes.
    TRACK_FULL: ((62041, 230319), (62041, 230320)),
}
TENANT_TRACK_LOCK_NAMESPACE = 62042

PHASE_META_KEYS = (
    "dim_filial",
    "dim_grupos",
    "dim_localvendas",
    "dim_produtos",
    "dim_funcionarios",
    "dim_usuario_caixa",
    "dim_clientes",
    "fact_comprovante",
    "fact_caixa_turno",
    "fact_pagamento_comprovante",
    "fact_venda",
    "fact_venda_item",
    "fact_financeiro",
    "fact_estoque_atual",
    "risk_events",
)

CLOCK_REFRESH_META_KEYS = (
    "clock_daily_rollover",
    "clock_open_cash_turns",
    "clock_churn_mart_refresh",
    "clock_cash_open_refresh",
)

CLOCK_POST_REFRESH_RANGE_KEYS = (
    "clock_customer_rfm_start_dt_ref",
    "clock_customer_churn_risk_start_dt_ref",
    "clock_finance_aging_start_dt_ref",
    "clock_health_score_start_dt_ref",
)

RISK_SOURCE_WATERMARK_DATASETS = (
    "comprovantes",
    "itenscomprovantes",
    "formas_pgto_comprovantes",
    "turnos",
)
RISK_EVENT_PROPAGATION_DAYS = 30
RISK_EVENT_BACKSHIFT_DAYS = 1

GLOBAL_REFRESH_LOG_TENANT_ID = 0
STALE_RUN_LOG_ERROR = "stale_runtime_state"

PHASE_SQL_STEPS: tuple[tuple[str, str], ...] = (
    ("dim_filial", "SELECT etl.load_dim_filial(%s) AS rows"),
    ("dim_grupos", "SELECT etl.load_dim_grupos(%s) AS rows"),
    ("dim_localvendas", "SELECT etl.load_dim_localvendas(%s) AS rows"),
    ("dim_produtos", "SELECT etl.load_dim_produtos(%s) AS rows"),
    ("dim_funcionarios", "SELECT etl.load_dim_funcionarios(%s) AS rows"),
    ("dim_usuario_caixa", "SELECT etl.load_dim_usuario_caixa(%s) AS rows"),
    ("dim_clientes", "SELECT etl.load_dim_clientes(%s) AS rows"),
    ("fact_comprovante", "SELECT etl.load_fact_comprovante(%s) AS rows"),
    ("fact_caixa_turno", "SELECT etl.load_fact_caixa_turno(%s) AS rows"),
    ("fact_pagamento_comprovante", "SELECT etl.load_fact_pagamento_comprovante(%s) AS rows"),
    ("fact_venda", "SELECT etl.load_fact_venda(%s) AS rows"),
    ("fact_venda_item", "SELECT etl.load_fact_venda_item(%s) AS rows"),
    ("fact_financeiro", "SELECT etl.load_fact_financeiro(%s) AS rows"),
    ("fact_estoque_atual", "SELECT etl.load_fact_estoque_atual(%s) AS rows"),
)

ProgressCallback = Callable[[dict[str, Any]], None]


logger = logging.getLogger(__name__)


def _env_positive_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    with suppress(ValueError, TypeError):
        return max(minimum, int(raw))
    return default


SALES_BULK_CHUNK_THRESHOLD_ROWS = _env_positive_int(
    "ETL_SALES_BULK_CHUNK_THRESHOLD_ROWS",
    250_000,
    minimum=10_000,
)
PAYMENT_REFERENCE_CHUNK_ROWS = _env_positive_int(
    "ETL_PAYMENT_REFERENCE_CHUNK_ROWS",
    100_000,
    minimum=10_000,
)
VENDA_ITEM_COMPROVANTE_CHUNK_ROWS = _env_positive_int(
    "ETL_VENDA_ITEM_COMPROVANTE_CHUNK_ROWS",
    250_000,
    minimum=10_000,
)


class EtlCycleBusyError(RuntimeError):
    pass


def normalize_track(track: str | None) -> str:
    value = str(track or TRACK_FULL).strip().lower()
    if value not in SUPPORTED_TRACKS:
        raise ValueError(f"Unsupported ETL track: {track}")
    return value


def _track_runs_operational(track: str) -> bool:
    return track in {TRACK_OPERATIONAL, TRACK_FULL}


def _track_runs_risk(track: str) -> bool:
    return track in {TRACK_RISK, TRACK_FULL}


def _track_runs_publication(track: str) -> bool:
    return track in {TRACK_RISK, TRACK_FULL}


def list_target_tenants(tenant_id: int | None = None) -> list[dict[str, Any]]:
    where = "WHERE id_empresa = %s" if tenant_id is not None else "WHERE is_active = true"
    params: list[Any] = [tenant_id] if tenant_id is not None else []
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            f"""
            SELECT id_empresa, nome, status, is_active
            FROM app.tenants
            {where}
            ORDER BY id_empresa
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def run_incremental_cycle(
    tenant_ids: list[int],
    *,
    ref_date: date,
    refresh_mart: bool = True,
    force_full: bool = False,
    fail_fast: bool = False,
    track: str = TRACK_FULL,
    skip_busy_tenants: bool = False,
    db_role: str = "MASTER",
    db_tenant_scope: int | None = None,
    tenant_rows: list[dict[str, Any]] | None = None,
    acquire_lock: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    track = normalize_track(track)
    started_at = datetime.now(timezone.utc)
    tenant_rows = tenant_rows or [{"id_empresa": tenant_id} for tenant_id in tenant_ids]
    tenant_by_id = {int(row["id_empresa"]): row for row in tenant_rows}
    if not tenant_ids:
        return {
            "ok": True,
            "track": track,
            "reference_date": ref_date.isoformat(),
            "processed": 0,
            "failed": 0,
            "skipped": 0,
            "duration_ms": 0.0,
            "global_refresh": _empty_refresh_meta(ref_date),
            "items": [],
        }

    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    skipped_items: list[dict[str, Any]] = []
    successful_items: list[dict[str, Any]] = []
    fail_fast_abort_reason: str | None = None
    cycle_started = time.perf_counter()

    with get_conn(role=db_role, tenant_id=db_tenant_scope, branch_id=None) as conn:
        acquired_cycle_locks: list[tuple[int, int]] = []
        held_tenant_locks: set[int] = set()
        inspect_running_etl_state(conn, tenant_id=db_tenant_scope)
        if acquire_lock:
            acquired_cycle_locks = _try_cycle_locks(conn, track)
            if not acquired_cycle_locks:
                raise EtlCycleBusyError(f"ETL {track} cycle is already running.")

        try:
            for tenant_id in tenant_ids:
                tenant_id = int(tenant_id)
                tenant_ctx = tenant_by_id.get(tenant_id, {})
                item_started = time.perf_counter()
                if acquire_lock and not _try_tenant_track_lock(conn, tenant_id):
                    busy_item = {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_ctx.get("nome"),
                        "tenant_status": tenant_ctx.get("status"),
                        "is_active": bool(tenant_ctx.get("is_active", True)),
                        "track": track,
                        "ok": True,
                        "skipped": True,
                        "busy": True,
                        "reason": "tenant_busy",
                        "message": "Tenant is already being processed by another ETL track.",
                    }
                    if skip_busy_tenants:
                        items.append(busy_item)
                        skipped_items.append(busy_item)
                        _emit_progress(
                            progress_callback,
                            event="tenant_skipped",
                            tenant_id=tenant_id,
                            tenant_name=tenant_ctx.get("nome"),
                            stage="phase",
                            track=track,
                            reason="tenant_busy",
                        )
                        continue

                    failure = {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_ctx.get("nome"),
                        "tenant_status": tenant_ctx.get("status"),
                        "is_active": bool(tenant_ctx.get("is_active", True)),
                        "track": track,
                        "error_code": "tenant_busy",
                        "error": "Tenant is already being processed by another ETL track.",
                        "ok": False,
                    }
                    failures.append(failure)
                    items.append(failure)
                    _emit_progress(
                        progress_callback,
                        event="tenant_finished",
                        tenant_id=tenant_id,
                        tenant_name=tenant_ctx.get("nome"),
                        track=track,
                        ok=False,
                        error_code="tenant_busy",
                        error=failure["error"],
                    )
                    if fail_fast:
                        break
                    continue

                held_tenant_locks.add(tenant_id)
                _emit_progress(
                    progress_callback,
                    event="tenant_started",
                    tenant_id=tenant_id,
                    tenant_name=tenant_ctx.get("nome"),
                    stage="phase",
                    track=track,
                    ref_date=ref_date.isoformat(),
                )
                try:
                    phase_result = _run_tenant_phase(
                        conn,
                        tenant_id,
                        force_full,
                        ref_date,
                        track=track,
                        progress_callback=progress_callback,
                    )
                except Exception as exc:  # noqa: BLE001
                    conn.rollback()
                    with suppress(Exception):
                        _unlock_tenant_track_lock(conn, tenant_id)
                    held_tenant_locks.discard(tenant_id)
                    failure = {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_ctx.get("nome"),
                        "tenant_status": tenant_ctx.get("status"),
                        "is_active": bool(tenant_ctx.get("is_active", True)),
                        "track": track,
                        "error": str(exc),
                        "ok": False,
                    }
                    failures.append(failure)
                    items.append(failure)
                    _emit_progress(
                        progress_callback,
                        event="tenant_finished",
                        tenant_id=tenant_id,
                        tenant_name=tenant_ctx.get("nome"),
                        track=track,
                        ok=False,
                        error=str(exc),
                    )
                    if fail_fast:
                        break
                    continue

                phase_meta = _extract_meta(phase_result)
                clock_meta = (
                    _run_tenant_clock_meta_sql(conn, tenant_id, ref_date)
                    if _track_runs_operational(track)
                    else {}
                )
                item = {
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_ctx.get("nome"),
                    "tenant_status": tenant_ctx.get("status"),
                    "is_active": bool(tenant_ctx.get("is_active", True)),
                    "track": track,
                    "phase_result": phase_result,
                    "phase_meta": phase_meta,
                    "clock_meta": clock_meta,
                    "phase_domains": _phase_domains(phase_meta, force_full=force_full, track=track),
                    "_perf_started": item_started,
                    "elapsed_ms": 0.0,
                    "ok": True,
                }
                items.append(item)
                successful_items.append(item)

            aggregated_meta = _aggregate_refresh_meta(successful_items, force_full=force_full, track=track)
            publication_requested = refresh_mart and bool(successful_items) and _refresh_meta_has_requested_work(aggregated_meta)
            publication_enabled = publication_requested and _track_runs_publication(track)
            publication_deferred = publication_requested and track == TRACK_OPERATIONAL
            refresh_meta = _empty_refresh_meta(ref_date)
            if publication_enabled:
                refresh_meta = _empty_refresh_meta(ref_date) | {"requested": True} | _run_global_refresh(
                    conn,
                    aggregated_meta,
                    ref_date,
                    tenant_ids=[int(item["tenant_id"]) for item in successful_items],
                    progress_callback=progress_callback,
                )
            elif publication_deferred:
                refresh_meta = _empty_refresh_meta(ref_date) | {
                    "requested": True,
                    "deferred": True,
                    "deferred_reason": "operational_track_keeps_global_refresh_off_hot_path",
                    "recommended_track": TRACK_RISK,
                    "fast_path_available": True,
                    "publication_policy": "operational_fast_path_plus_risk_heavy_refresh",
                }

            refreshed_any = bool(refresh_meta.get("refreshed_any"))
            fast_path_items = 0

            for item in successful_items:
                payment_details = _empty_notification_details()
                cash_details = _empty_notification_details()
                post_meta = _empty_post_meta()
                publication_mode = PUBLICATION_MODE_NONE
                needs_post_refresh = False
                if refresh_mart and _item_needs_post_refresh(item):
                    if track == TRACK_OPERATIONAL:
                        needs_post_refresh = True
                        publication_mode = PUBLICATION_MODE_FAST_PATH
                    elif publication_enabled and refreshed_any:
                        needs_post_refresh = True
                        publication_mode = PUBLICATION_MODE_GLOBAL_REFRESH

                if needs_post_refresh:
                    try:
                        post_meta = _run_tenant_post_refresh(
                            conn,
                            item["tenant_id"],
                            _item_post_refresh_meta(item),
                            ref_date,
                            force_full,
                            item["phase_result"].get("hot_window_days"),
                            track=track,
                            publication_mode=publication_mode,
                            progress_callback=progress_callback,
                        )
                        if publication_mode == PUBLICATION_MODE_FAST_PATH:
                            fast_path_items += 1
                            refresh_meta["fast_path_executed"] = True
                            refresh_meta["fast_path_items"] = fast_path_items
                    except Exception as exc:  # noqa: BLE001
                        conn.rollback()
                        item.update(
                            {
                                "ok": False,
                                "error": str(exc),
                                "elapsed_ms": round((time.perf_counter() - item["_perf_started"]) * 1000, 2),
                            }
                        )
                        failures.append(
                            {
                                "tenant_id": item["tenant_id"],
                                "tenant_name": item.get("tenant_name"),
                                "tenant_status": item.get("tenant_status"),
                                "error": str(exc),
                            }
                        )
                        _emit_progress(
                            progress_callback,
                            event="tenant_finished",
                            tenant_id=item["tenant_id"],
                            tenant_name=item.get("tenant_name"),
                            track=track,
                            ok=False,
                            error=str(exc),
                        )
                        with suppress(Exception):
                            _unlock_tenant_track_lock(conn, int(item["tenant_id"]))
                        held_tenant_locks.discard(int(item["tenant_id"]))
                        if fail_fast:
                            fail_fast_abort_reason = (
                                f"post_refresh_aborted_due_to_fail_fast_after_tenant_{item['tenant_id']}"
                            )
                            break
                        continue

                    if int(post_meta.get("payment_notifications", 0) or 0) > 0:
                        payment_details = _dispatch_payment_telegram_alerts(conn, item["tenant_id"], ref_date)
                    if int(post_meta.get("cash_notifications", 0) or 0) > 0:
                        cash_details = _dispatch_cash_telegram_alerts(conn, item["tenant_id"])
                    post_meta = post_meta | {
                        "payment_telegram_sent": payment_details["telegram_sent"],
                        "payment_telegram_suppressed": payment_details["telegram_suppressed"],
                        "cash_telegram_sent": cash_details["telegram_sent"],
                        "cash_telegram_suppressed": cash_details["telegram_suppressed"],
                    }

                combined_meta = _combine_item_meta(
                    phase_meta=item["phase_meta"],
                    clock_meta=item["clock_meta"],
                    refresh_meta=refresh_meta,
                    post_meta=post_meta,
                    refresh_requested=refresh_mart,
                    refreshed_any=refreshed_any,
                    post_refresh_executed=needs_post_refresh,
                    publication_deferred=publication_deferred,
                    publication_mode=publication_mode,
                )
                item["result"] = {
                    "ok": item.get("ok", True),
                    "id_empresa": item["tenant_id"],
                    "track": track,
                    "force_full": force_full,
                    "ref_date": ref_date,
                    "hot_window_days": item["phase_result"].get("hot_window_days"),
                    "started_at": item["phase_result"].get("started_at", started_at),
                    "finished_at": datetime.now(timezone.utc),
                    "meta": combined_meta,
                    "payments_notifications": payment_details,
                    "cash_notifications": cash_details,
                }
                item["payment_notifications"] = combined_meta.get("payment_notifications")
                item["cash_notifications"] = combined_meta.get("cash_notifications")
                item["mart_refreshed"] = combined_meta.get("mart_refreshed")
                item["elapsed_ms"] = round((time.perf_counter() - item["_perf_started"]) * 1000, 2)
                _emit_progress(
                    progress_callback,
                    event="tenant_finished",
                    tenant_id=item["tenant_id"],
                    tenant_name=item.get("tenant_name"),
                    track=track,
                    ok=item.get("ok", True),
                    elapsed_ms=item["elapsed_ms"],
                    mart_refreshed=item.get("mart_refreshed"),
                )
                with suppress(Exception):
                    _unlock_tenant_track_lock(conn, int(item["tenant_id"]))
                held_tenant_locks.discard(int(item["tenant_id"]))

            if fail_fast_abort_reason:
                for item in successful_items:
                    if item.get("ok") is False or "result" in item:
                        continue
                    item.update(
                        {
                            "ok": False,
                            "error": fail_fast_abort_reason,
                            "elapsed_ms": round((time.perf_counter() - item["_perf_started"]) * 1000, 2),
                        }
                    )
                    failures.append(
                        {
                            "tenant_id": item["tenant_id"],
                            "tenant_name": item.get("tenant_name"),
                            "tenant_status": item.get("tenant_status"),
                            "error": fail_fast_abort_reason,
                        }
                    )
                    _emit_progress(
                        progress_callback,
                        event="tenant_finished",
                        tenant_id=item["tenant_id"],
                        tenant_name=item.get("tenant_name"),
                        track=track,
                        ok=False,
                        error=fail_fast_abort_reason,
                    )
                    with suppress(Exception):
                        _unlock_tenant_track_lock(conn, int(item["tenant_id"]))
                    held_tenant_locks.discard(int(item["tenant_id"]))

            processed_items = []
            for item in items:
                if item.get("skipped"):
                    processed_items.append(
                        {
                            "tenant_id": item["tenant_id"],
                            "tenant_name": item.get("tenant_name"),
                            "tenant_status": item.get("tenant_status"),
                            "is_active": item.get("is_active"),
                            "track": track,
                            "ok": True,
                            "skipped": True,
                            "busy": item.get("busy", False),
                            "reason": item.get("reason"),
                            "message": item.get("message"),
                        }
                    )
                    continue
                if item.get("ok") is False and "result" not in item:
                    processed_items.append(
                        {
                            "tenant_id": item["tenant_id"],
                            "tenant_name": item.get("tenant_name"),
                            "tenant_status": item.get("tenant_status"),
                            "is_active": item.get("is_active"),
                            "track": track,
                            "ok": False,
                            "error": item.get("error"),
                            **({"error_code": item["error_code"]} if item.get("error_code") else {}),
                        }
                    )
                    continue
                processed_items.append(
                    {
                        "tenant_id": item["tenant_id"],
                        "tenant_name": item.get("tenant_name"),
                        "tenant_status": item.get("tenant_status"),
                        "is_active": item.get("is_active"),
                        "track": track,
                        "elapsed_ms": item.get("elapsed_ms"),
                        "result": item.get("result"),
                        "payment_notifications": item.get("payment_notifications"),
                        "cash_notifications": item.get("cash_notifications"),
                        "mart_refreshed": item.get("mart_refreshed"),
                        "ok": item.get("ok", True),
                        **({"error": item["error"]} if item.get("error") else {}),
                    }
                )

            return {
                "ok": not failures,
                "track": track,
                "reference_date": ref_date.isoformat(),
                "processed": len(processed_items),
                "failed": len(failures),
                "skipped": len(skipped_items),
                "duration_ms": round((time.perf_counter() - cycle_started) * 1000, 2),
                "global_refresh": refresh_meta,
                "items": processed_items,
            }
        finally:
            for tenant_id in list(held_tenant_locks):
                with suppress(Exception):
                    _unlock_tenant_track_lock(conn, tenant_id)
            if acquire_lock:
                with suppress(Exception):
                    _unlock_cycle_locks(conn, acquired_cycle_locks)


def _empty_refresh_meta(ref_date: date) -> dict[str, Any]:
    return {
        "ref_date": ref_date.isoformat(),
        "requested": False,
        "deferred": False,
        "deferred_reason": None,
        "recommended_track": None,
        "publication_policy": None,
        "fast_path_available": False,
        "fast_path_executed": False,
        "fast_path_items": 0,
        "refreshed_any": False,
        "sales_marts_refreshed": False,
        "finance_mart_refreshed": False,
        "risk_marts_refreshed": False,
        "payments_marts_refreshed": False,
        "cash_marts_refreshed": False,
        "anonymous_retention_refreshed": False,
        "churn_clock_mart_refreshed": False,
        "cash_open_alert_marts_refreshed": False,
    }


def _empty_post_meta() -> dict[str, Any]:
    return {
        "customer_sales_daily_refreshed": False,
        "customer_rfm_refreshed": False,
        "customer_churn_risk_refreshed": False,
        "finance_aging_refreshed": False,
        "health_score_refreshed": False,
        "payment_notifications": 0,
        "notification_rows": 0,
        "notification_ms": 0,
        "cash_notifications": 0,
        "insights_generated": 0,
        "customer_rfm_clock_driven": False,
        "customer_churn_risk_clock_driven": False,
        "finance_aging_clock_driven": False,
        "health_score_clock_driven": False,
        "cash_notifications_clock_driven": False,
        "publication_mode": PUBLICATION_MODE_NONE,
    }


def _empty_notification_details() -> dict[str, Any]:
    return {
        "critical_events": 0,
        "telegram_sent": 0,
        "telegram_suppressed": 0,
        "items": [],
    }


def _extract_meta(result: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    meta = result.get("meta")
    return meta if isinstance(meta, dict) else {}


def _aggregate_phase_meta(items: list[dict[str, Any]], *, force_full: bool, track: str) -> dict[str, Any]:
    aggregated: dict[str, Any] = {"force_full": force_full, "track": track}
    for item in items:
        meta = item.get("phase_meta") or {}
        for key in PHASE_META_KEYS:
            aggregated[key] = int(aggregated.get(key, 0) or 0) + int(meta.get(key, 0) or 0)
        aggregated["risk_events_total_mutations"] = int(aggregated.get("risk_events_total_mutations", 0) or 0) + int(
            meta.get("risk_events_total_mutations", 0) or 0
        )
        aggregated["risk_events_has_changes"] = bool(aggregated.get("risk_events_has_changes")) or bool(
            meta.get("risk_events_has_changes")
        )
    return aggregated


def _aggregate_refresh_meta(items: list[dict[str, Any]], *, force_full: bool, track: str) -> dict[str, Any]:
    aggregated = _aggregate_phase_meta(items, force_full=force_full, track=track)
    for key in CLOCK_REFRESH_META_KEYS:
        aggregated[key] = any(bool(item.get("clock_meta", {}).get(key)) for item in items)
    return aggregated


def _refresh_meta_has_requested_work(meta: dict[str, Any]) -> bool:
    if any(int(meta.get(key, 0) or 0) > 0 for key in PHASE_META_KEYS):
        return True
    if bool(meta.get("risk_events_has_changes")) or int(meta.get("risk_events_total_mutations", 0) or 0) > 0:
        return True
    return any(bool(meta.get(key)) for key in CLOCK_REFRESH_META_KEYS)


def _phase_domains(meta: dict[str, Any], *, force_full: bool, track: str) -> dict[str, bool]:
    track = normalize_track(track)
    risk_changed = bool(force_full) or bool(meta.get("risk_events_has_changes")) or int(
        meta.get("risk_events_total_mutations", 0) or 0
    ) > 0 or int(meta.get("risk_events", 0) or 0) > 0
    if track == TRACK_OPERATIONAL:
        return {
            "sales": any(
                int(meta.get(key, 0) or 0) > 0
                for key in (
                    "dim_clientes",
                    "fact_comprovante",
                    "fact_venda",
                    "fact_venda_item",
                )
            ),
            "finance": int(meta.get("fact_financeiro", 0) or 0) > 0,
            "risk": False,
            "payments": any(
                int(meta.get(key, 0) or 0) > 0 for key in ("fact_pagamento_comprovante", "fact_comprovante")
            ),
            "cash": any(
                int(meta.get(key, 0) or 0) > 0
                for key in (
                    "fact_caixa_turno",
                    "fact_pagamento_comprovante",
                    "fact_comprovante",
                    "dim_usuario_caixa",
                    "fact_estoque_atual",
                )
            ),
        }
    if track == TRACK_RISK:
        return {
            "sales": False,
            "finance": False,
            "risk": risk_changed,
            "payments": False,
            "cash": False,
        }
    if force_full:
        return {
            "sales": True,
            "finance": True,
            "risk": True,
            "payments": True,
            "cash": True,
        }
    return {
        "sales": any(
            int(meta.get(key, 0) or 0) > 0
            for key in ("dim_clientes", "fact_comprovante", "fact_venda", "fact_venda_item")
        ),
        "finance": int(meta.get("fact_financeiro", 0) or 0) > 0,
        "risk": risk_changed or int(meta.get("dim_funcionarios", 0) or 0) > 0,
        "payments": any(int(meta.get(key, 0) or 0) > 0 for key in ("fact_pagamento_comprovante", "fact_comprovante")),
        "cash": any(
            int(meta.get(key, 0) or 0) > 0
            for key in (
                "fact_caixa_turno",
                "fact_pagamento_comprovante",
                "fact_comprovante",
                "dim_usuario_caixa",
                "fact_estoque_atual",
            )
        ),
    }


def _combine_item_meta(
    *,
    phase_meta: dict[str, Any],
    clock_meta: dict[str, Any],
    refresh_meta: dict[str, Any],
    post_meta: dict[str, Any],
    refresh_requested: bool,
    refreshed_any: bool,
    post_refresh_executed: bool,
    publication_deferred: bool,
    publication_mode: str,
) -> dict[str, Any]:
    combined = dict(phase_meta)
    combined["clock_meta"] = clock_meta
    combined["mart_refresh"] = dict(refresh_meta)
    combined["mart_refreshed"] = refreshed_any
    combined["refresh_requested"] = refresh_requested
    combined["publication_deferred"] = publication_deferred
    combined["post_refresh_executed"] = post_refresh_executed
    combined.update(post_meta)
    combined["publication_mode"] = publication_mode
    combined["publication_executed"] = bool(post_refresh_executed or refreshed_any)
    return combined


def _emit_progress(progress_callback: ProgressCallback | None, /, **event: Any) -> None:
    if progress_callback is None:
        return
    payload = {"ts": datetime.now(timezone.utc).isoformat(), **event}
    with suppress(Exception):
        progress_callback(payload)


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _log_meta_dict(row: dict[str, Any]) -> dict[str, Any]:
    meta = row.get("meta")
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        with suppress(Exception):
            loaded = json.loads(meta)
            if isinstance(loaded, dict):
                return loaded
    return {}


def _running_log_track(row: dict[str, Any]) -> str:
    meta = _log_meta_dict(row)
    return normalize_track(meta.get("track") or TRACK_FULL)


def _running_log_tenant_ids(row: dict[str, Any]) -> list[int]:
    tenant_ids: set[int] = set()
    raw_tenant_id = row.get("id_empresa")
    with suppress(Exception):
        tenant_id = int(raw_tenant_id)
        if tenant_id > 0:
            tenant_ids.add(tenant_id)

    meta = _log_meta_dict(row)
    for value in meta.get("tenant_ids") or []:
        with suppress(Exception):
            tenant_id = int(value)
            if tenant_id > 0:
                tenant_ids.add(tenant_id)
    return sorted(tenant_ids)


def _running_log_has_live_locks(conn, row: dict[str, Any]) -> bool:
    track = _running_log_track(row)
    cycle_busy = any(
        not advisory_lock_is_available(conn, left, right)
        for left, right in TRACK_CYCLE_LOCKS[track]
    )
    if cycle_busy:
        return True

    for tenant_id in _running_log_tenant_ids(row):
        if not advisory_lock_is_available(conn, TENANT_TRACK_LOCK_NAMESPACE, tenant_id):
            return True
    return False


def inspect_running_etl_state(conn, tenant_id: int | None = None) -> dict[str, list[dict[str, Any]]]:
    where = "WHERE status = 'running'"
    params: list[Any] = []
    if tenant_id is not None:
        where += " AND id_empresa IN (%s, 0)"
        params.append(int(tenant_id))

    rows = conn.execute(
        f"""
        SELECT id, id_empresa, started_at, step_name, meta
        FROM etl.run_log
        {where}
        ORDER BY started_at ASC, id ASC
        """,
        params,
    ).fetchall()
    row_maps = [dict(row) for row in rows]

    live_rows: list[dict[str, Any]] = []
    stale_rows: list[dict[str, Any]] = []
    for row in row_maps:
        if _running_log_has_live_locks(conn, row):
            live_rows.append(row)
        else:
            stale_rows.append(row)

    if stale_rows:
        finished_at = datetime.now(timezone.utc)
        for row in stale_rows:
            meta = _log_meta_dict(row) | {
                "stale_cleanup": True,
                "stale_cleanup_at": finished_at.isoformat(),
                "stale_cleanup_reason": "running_log_without_matching_advisory_lock",
                "track": _running_log_track(row),
            }
            conn.execute(
                """
                UPDATE etl.run_log
                SET
                  finished_at = %s,
                  status = 'failed',
                  error = COALESCE(error, %s),
                  duration_ms = GREATEST(0, FLOOR(EXTRACT(epoch FROM (%s::timestamptz - started_at)) * 1000)::int),
                  meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                WHERE id = %s
                  AND status = 'running'
                """,
                (
                    finished_at,
                    STALE_RUN_LOG_ERROR,
                    finished_at,
                    _json_dumps(meta),
                    int(row["id"]),
                ),
            )
        conn.commit()
        logger.warning(
            "Marked stale ETL run_log rows as failed: %s",
            [
                {
                    "id": int(row["id"]),
                    "tenant_id": row.get("id_empresa"),
                    "step_name": row.get("step_name"),
                    "track": _running_log_track(row),
                }
                for row in stale_rows
            ],
        )

    return {
        "live_rows": live_rows,
        "stale_rows": stale_rows,
    }


def _parse_optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _as_utc_datetime(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _date_to_key(value: date | None) -> int | None:
    if value is None:
        return None
    return int(value.strftime("%Y%m%d"))


def _hot_window_days(conn) -> int:
    row = conn.execute("SELECT etl.hot_window_days() AS value").fetchone()
    return max(1, int((row or {}).get("value") or 1))


def _risk_source_watermarks_ahead(conn, tenant_id: int) -> bool:
    row = conn.execute(
        """
        SELECT
          MAX(last_ts) FILTER (WHERE dataset = ANY(%s)) AS latest_source_ts,
          MAX(last_ts) FILTER (WHERE dataset = 'risk_events') AS risk_ts
        FROM etl.watermark
        WHERE id_empresa = %s
        """,
        (list(RISK_SOURCE_WATERMARK_DATASETS), tenant_id),
    ).fetchone() or {}
    latest_source_ts = row.get("latest_source_ts")
    risk_ts = row.get("risk_ts")
    if latest_source_ts is None:
        return False
    if risk_ts is None:
        return True
    return latest_source_ts > risk_ts


def _run_sql_count(conn, query: str, params: tuple[Any, ...]) -> int:
    row = conn.execute(query, params).fetchone()
    return int((row or {}).get("rows") or 0)


def _run_payment_loader_detail(conn, tenant_id: int) -> tuple[int, dict[str, Any]]:
    row = conn.execute(
        "SELECT etl.load_fact_pagamento_comprovante_detail(%s) AS result",
        (tenant_id,),
    ).fetchone() or {}
    payload = dict(row.get("result") or {})
    rows = int(payload.pop("rows", 0) or 0)
    return rows, payload


def _run_venda_item_loader_detail(conn, tenant_id: int) -> tuple[int, dict[str, Any]]:
    row = conn.execute(
        "SELECT etl.load_fact_venda_item_detail(%s) AS result",
        (tenant_id,),
    ).fetchone() or {}
    payload = dict(row.get("result") or {})
    rows = int(payload.pop("rows", 0) or 0)
    return rows, payload


def _json_int(payload: dict[str, Any], key: str) -> int:
    return int(payload.get(key, 0) or 0)


def _json_optional_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    return int(value)


def _find_running_step_log_id(conn, tenant_id: int, step_name: str) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM etl.run_log
        WHERE id_empresa = %s
          AND step_name = %s
          AND status = 'running'
        ORDER BY id DESC
        LIMIT 1
        """,
        (tenant_id, step_name),
    ).fetchone() or {}
    value = row.get("id")
    return int(value) if value is not None else None


def _update_running_step_log(
    conn,
    log_id: int | None,
    *,
    rows_processed: int,
    meta: dict[str, Any],
) -> None:
    if not log_id:
        return
    now_ts = datetime.now(timezone.utc)
    conn.execute(
        """
        UPDATE etl.run_log
        SET
          rows_processed = %s,
          duration_ms = GREATEST(0, FLOOR(EXTRACT(epoch FROM (%s::timestamptz - started_at)) * 1000)::int),
          meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
        WHERE id = %s
          AND status = 'running'
        """,
        (
            rows_processed,
            now_ts,
            _json_dumps(meta),
            log_id,
        ),
    )


def _chunk_plan(min_key: int | None, max_key: int | None, candidate_count: int, target_rows: int) -> tuple[int, int]:
    if min_key is None or max_key is None or candidate_count <= 0:
        return 0, 0
    bucket_count = max(1, (candidate_count + max(1, target_rows) - 1) // max(1, target_rows))
    span = max(1, ((max_key - min_key) + bucket_count) // bucket_count)
    return span, bucket_count


def _finalize_payment_loader_watermarks(conn, tenant_id: int, bounds: dict[str, Any]) -> dict[str, Any]:
    watermark_before = bounds.get("watermark_before")
    bridge_watermark_before = bounds.get("bridge_watermark_before")
    max_forms = None
    max_bridge = None

    if watermark_before is not None:
        row = conn.execute(
            """
            SELECT MAX(received_at) AS max_ts
            FROM stg.formas_pgto_comprovantes
            WHERE id_empresa = %s
              AND received_at > %s
            """,
            (tenant_id, watermark_before),
        ).fetchone() or {}
        max_forms = row.get("max_ts")
        conn.execute(
            "SELECT etl.set_watermark(%s, 'formas_pgto_comprovantes', %s, NULL::bigint)",
            (tenant_id, max_forms or watermark_before),
        )

    if bridge_watermark_before is not None:
        row = conn.execute(
            """
            SELECT MAX(updated_at) AS max_ts
            FROM etl.pagamento_comprovante_bridge
            WHERE id_empresa = %s
              AND updated_at > %s
            """,
            (tenant_id, bridge_watermark_before),
        ).fetchone() or {}
        max_bridge = row.get("max_ts")
        conn.execute(
            "SELECT etl.set_watermark(%s, 'pagamento_comprovante_bridge', %s, NULL::bigint)",
            (tenant_id, max_bridge or bridge_watermark_before),
        )

    return {
        "watermark_updated": True,
        "watermark_after": max_forms or watermark_before,
        "bridge_watermark_after": max_bridge or bridge_watermark_before,
    }


def _finalize_venda_item_watermark(conn, tenant_id: int, bounds: dict[str, Any]) -> dict[str, Any]:
    watermark_before = bounds.get("watermark_before")
    max_items = None

    if watermark_before is not None:
        row = conn.execute(
            """
            SELECT MAX(received_at) AS max_ts
            FROM stg.itenscomprovantes
            WHERE id_empresa = %s
              AND received_at > %s
            """,
            (tenant_id, watermark_before),
        ).fetchone() or {}
        max_items = row.get("max_ts")
        conn.execute(
            "SELECT etl.set_watermark(%s, 'itenscomprovantes_sales_fact', %s, NULL::bigint)",
            (tenant_id, max_items or watermark_before),
        )

    return {
        "watermark_updated": True,
        "watermark_after": max_items or watermark_before,
    }


def _run_payment_loader(
    conn,
    tenant_id: int,
    *,
    force_full: bool,
    progress_callback: ProgressCallback | None = None,
) -> tuple[int, dict[str, Any]]:
    bounds_row = conn.execute(
        "SELECT etl.fact_pagamento_comprovante_pending_bounds(%s) AS result",
        (tenant_id,),
    ).fetchone() or {}
    bounds = dict(bounds_row.get("result") or {})
    candidate_refs = _json_int(bounds, "candidate_refs")
    if candidate_refs <= 0:
        return 0, {**bounds, "chunked": False}

    should_chunk = force_full or candidate_refs > SALES_BULK_CHUNK_THRESHOLD_ROWS
    if not should_chunk:
        rows, payload = _run_payment_loader_detail(conn, tenant_id)
        return rows, {**bounds, **payload, "chunked": False}

    min_ref = _json_optional_int(bounds, "min_referencia")
    max_ref = _json_optional_int(bounds, "max_referencia")
    span, chunk_count = _chunk_plan(min_ref, max_ref, candidate_refs, PAYMENT_REFERENCE_CHUNK_ROWS)
    if span <= 0 or chunk_count <= 0:
        return 0, {**bounds, "chunked": False}

    log_id = _find_running_step_log_id(conn, tenant_id, "fact_pagamento_comprovante")
    total_rows = 0
    aggregate: dict[str, Any] = {
        **bounds,
        "chunked": True,
        "chunk_count": chunk_count,
        "range_span": span,
        "upsert_inserts": 0,
        "upsert_updates": 0,
        "conflict_count": 0,
        "bridge_miss_count": 0,
        "bridge_rows": 0,
        "candidate_count": 0,
    }

    current_ref = min_ref
    chunk_index = 0
    while current_ref is not None and max_ref is not None and current_ref <= max_ref:
        chunk_index += 1
        chunk_from = current_ref
        chunk_to = min(max_ref, chunk_from + span - 1)
        row = conn.execute(
            "SELECT etl.load_fact_pagamento_comprovante_range_detail(%s, %s, %s, false) AS result",
            (tenant_id, chunk_from, chunk_to),
        ).fetchone() or {}
        payload = dict(row.get("result") or {})
        chunk_rows = int(payload.pop("rows", 0) or 0)
        total_rows += chunk_rows
        for key in ("upsert_inserts", "upsert_updates", "conflict_count", "bridge_miss_count", "bridge_rows", "candidate_count"):
            aggregate[key] = int(aggregate.get(key, 0) or 0) + int(payload.get(key, 0) or 0)
        chunk_meta = {
            **aggregate,
            "chunk_index": chunk_index,
            "chunk_from": chunk_from,
            "chunk_to": chunk_to,
            "chunk_rows": chunk_rows,
            "rows_processed": total_rows,
        }
        _update_running_step_log(conn, log_id, rows_processed=total_rows, meta=chunk_meta)
        conn.commit()
        _emit_progress(
            progress_callback,
            event="step_chunk",
            tenant_id=tenant_id,
            step_name="fact_pagamento_comprovante",
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            rows_processed=chunk_rows,
            total_rows_processed=total_rows,
            range_from=chunk_from,
            range_to=chunk_to,
            meta=chunk_meta,
        )
        current_ref = chunk_to + 1

    aggregate |= _finalize_payment_loader_watermarks(conn, tenant_id, bounds)
    conn.commit()
    return total_rows, aggregate


def _run_venda_item_loader(
    conn,
    tenant_id: int,
    *,
    force_full: bool,
    progress_callback: ProgressCallback | None = None,
) -> tuple[int, dict[str, Any]]:
    bounds_row = conn.execute(
        "SELECT etl.fact_venda_item_pending_bounds(%s) AS result",
        (tenant_id,),
    ).fetchone() or {}
    bounds = dict(bounds_row.get("result") or {})
    candidate_rows = _json_int(bounds, "candidate_rows")
    if candidate_rows <= 0:
        return 0, {**bounds, "chunked": False}

    should_chunk = force_full or candidate_rows > SALES_BULK_CHUNK_THRESHOLD_ROWS
    if not should_chunk:
        rows, payload = _run_venda_item_loader_detail(conn, tenant_id)
        return rows, {**bounds, **payload, "chunked": False}

    min_doc = _json_optional_int(bounds, "min_id_comprovante")
    max_doc = _json_optional_int(bounds, "max_id_comprovante")
    span, chunk_count = _chunk_plan(min_doc, max_doc, candidate_rows, VENDA_ITEM_COMPROVANTE_CHUNK_ROWS)
    if span <= 0 or chunk_count <= 0:
        return 0, {**bounds, "chunked": False}

    log_id = _find_running_step_log_id(conn, tenant_id, "fact_venda_item")
    total_rows = 0
    aggregate: dict[str, Any] = {
        **bounds,
        "chunked": True,
        "chunk_count": chunk_count,
        "range_span": span,
        "upsert_inserts": 0,
        "upsert_updates": 0,
        "conflict_count": 0,
        "candidate_count": 0,
    }

    current_doc = min_doc
    chunk_index = 0
    while current_doc is not None and max_doc is not None and current_doc <= max_doc:
        chunk_index += 1
        chunk_from = current_doc
        chunk_to = min(max_doc, chunk_from + span - 1)
        row = conn.execute(
            "SELECT etl.load_fact_venda_item_range_detail(%s, %s, %s, false) AS result",
            (tenant_id, chunk_from, chunk_to),
        ).fetchone() or {}
        payload = dict(row.get("result") or {})
        chunk_rows = int(payload.pop("rows", 0) or 0)
        total_rows += chunk_rows
        for key in ("upsert_inserts", "upsert_updates", "conflict_count", "candidate_count"):
            aggregate[key] = int(aggregate.get(key, 0) or 0) + int(payload.get(key, 0) or 0)
        chunk_meta = {
            **aggregate,
            "chunk_index": chunk_index,
            "chunk_from": chunk_from,
            "chunk_to": chunk_to,
            "chunk_rows": chunk_rows,
            "rows_processed": total_rows,
        }
        _update_running_step_log(conn, log_id, rows_processed=total_rows, meta=chunk_meta)
        conn.commit()
        _emit_progress(
            progress_callback,
            event="step_chunk",
            tenant_id=tenant_id,
            step_name="fact_venda_item",
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            rows_processed=chunk_rows,
            total_rows_processed=total_rows,
            range_from=chunk_from,
            range_to=chunk_to,
            meta=chunk_meta,
        )
        current_doc = chunk_to + 1

    aggregate |= _finalize_venda_item_watermark(conn, tenant_id, bounds)
    conn.commit()
    return total_rows, aggregate


def _risk_window_detail(
    conn,
    tenant_id: int,
    *,
    force_full: bool,
    lookback_days: int,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    row = conn.execute(
        """
        WITH src AS (
          SELECT
            MIN(raw.min_ts) AS min_source_ts,
            MAX(raw.max_ts) AS max_source_ts
          FROM (
            SELECT
              MIN(c.data) AS min_ts,
              MAX(c.data) AS max_ts
            FROM dw.fact_comprovante c
            WHERE c.id_empresa = %s
              AND c.data IS NOT NULL
              AND (%s::timestamptz IS NULL OR c.data < (%s::timestamptz + interval '1 day'))

            UNION ALL

            SELECT
              MIN(v.data) AS min_ts,
              MAX(v.data) AS max_ts
            FROM dw.fact_venda v
            WHERE v.id_empresa = %s
              AND v.data IS NOT NULL
              AND (%s::timestamptz IS NULL OR v.data < (%s::timestamptz + interval '1 day'))
          ) raw
        ), changed AS (
          SELECT
            MIN(raw.min_ts) AS min_changed_ts,
            MAX(raw.max_ts) AS max_changed_ts
          FROM (
            SELECT
              MIN(c.data) AS min_ts,
              MAX(c.data) AS max_ts
            FROM dw.fact_comprovante c
            WHERE c.id_empresa = %s
              AND c.data IS NOT NULL
              AND c.updated_at > COALESCE(
                (
                  SELECT last_ts
                  FROM etl.watermark
                  WHERE id_empresa = %s
                    AND dataset = 'risk_events'
                ),
                '-infinity'::timestamptz
              )
              AND (%s::timestamptz IS NULL OR c.data < (%s::timestamptz + interval '1 day'))

            UNION ALL

            SELECT
              MIN(v.data) AS min_ts,
              MAX(v.data) AS max_ts
            FROM dw.fact_venda v
            WHERE v.id_empresa = %s
              AND v.data IS NOT NULL
              AND v.updated_at > COALESCE(
                (
                  SELECT last_ts
                  FROM etl.watermark
                  WHERE id_empresa = %s
                    AND dataset = 'risk_events'
                ),
                '-infinity'::timestamptz
              )
              AND (%s::timestamptz IS NULL OR v.data < (%s::timestamptz + interval '1 day'))

            UNION ALL

            SELECT
              MIN(v.data) AS min_ts,
              MAX(v.data) AS max_ts
            FROM dw.fact_venda_item i
            JOIN dw.fact_venda v
              ON v.id_empresa = i.id_empresa
             AND v.id_filial = i.id_filial
             AND v.id_db = i.id_db
             AND v.id_comprovante = i.id_comprovante
            WHERE i.id_empresa = %s
              AND v.data IS NOT NULL
              AND i.updated_at > COALESCE(
                (
                  SELECT last_ts
                  FROM etl.watermark
                  WHERE id_empresa = %s
                    AND dataset = 'risk_events'
                ),
                '-infinity'::timestamptz
              )
              AND (%s::timestamptz IS NULL OR v.data < (%s::timestamptz + interval '1 day'))
          ) raw
        )
        SELECT
          src.min_source_ts,
          src.max_source_ts,
          changed.min_changed_ts,
          changed.max_changed_ts,
          (
            SELECT last_ts
            FROM etl.watermark
            WHERE id_empresa = %s
              AND dataset = 'risk_events'
          ) AS risk_watermark
        FROM src
        CROSS JOIN changed
        """,
        (
            tenant_id,
            end_ts,
            end_ts,
            tenant_id,
            end_ts,
            end_ts,
            tenant_id,
            tenant_id,
            end_ts,
            end_ts,
            tenant_id,
            tenant_id,
            end_ts,
            end_ts,
            tenant_id,
            tenant_id,
            end_ts,
            end_ts,
            tenant_id,
        ),
    ).fetchone() or {}

    min_source_ts = _as_utc_datetime(row.get("min_source_ts"))
    max_source_ts = _as_utc_datetime(row.get("max_source_ts"))
    min_changed_ts = _as_utc_datetime(row.get("min_changed_ts"))
    max_changed_ts = _as_utc_datetime(row.get("max_changed_ts"))
    risk_watermark = _as_utc_datetime(row.get("risk_watermark"))
    effective_now = _as_utc_datetime(end_ts) or datetime.now(timezone.utc)

    if max_source_ts is None:
        return {
            "has_source_data": False,
            "window_start_dt_ref": None,
            "window_end_dt_ref": None,
            "window_days": 0,
            "watermark_before": risk_watermark.isoformat() if risk_watermark else None,
            "source_max_ts": None,
        }

    normalized_lookback = max(1, int(lookback_days or 1))
    if force_full:
        start_ts = min_source_ts or (max_source_ts - timedelta(days=90))
        effective_end_ts = max_source_ts if end_ts is None else min(max_source_ts, end_ts)
    elif risk_watermark is None:
        effective_end_ts = max_source_ts if end_ts is None else min(max_source_ts, end_ts)
        if effective_end_ts is None:
            effective_end_ts = max_source_ts or effective_now
        start_ts = effective_end_ts - timedelta(days=normalized_lookback)
    else:
        if min_changed_ts is None or max_changed_ts is None:
            return {
                "has_source_data": False,
                "window_start_dt_ref": None,
                "window_end_dt_ref": None,
                "window_days": 0,
                "watermark_before": risk_watermark.isoformat() if risk_watermark else None,
                "source_max_ts": max_source_ts.isoformat() if max_source_ts else None,
            }
        start_ts = min_changed_ts - timedelta(days=RISK_EVENT_BACKSHIFT_DAYS)
        propagation_end_ts = max_changed_ts + timedelta(days=RISK_EVENT_PROPAGATION_DAYS)
        capped_end_ts = max_source_ts if end_ts is None else min(max_source_ts, end_ts)
        effective_end_ts = min(propagation_end_ts, capped_end_ts)

    if effective_end_ts < start_ts:
        effective_end_ts = start_ts

    window_start_dt_ref = start_ts.astimezone(timezone.utc).date()
    window_end_dt_ref = effective_end_ts.astimezone(timezone.utc).date()
    window_days = max(1, (window_end_dt_ref - window_start_dt_ref).days + 1)
    return {
        "has_source_data": True,
        "window_start_dt_ref": window_start_dt_ref,
        "window_end_dt_ref": window_end_dt_ref,
        "window_days": window_days,
        "watermark_before": risk_watermark.isoformat() if risk_watermark else None,
        "source_max_ts": max_source_ts.isoformat() if max_source_ts else None,
    }


def _count_risk_rows_in_window(
    conn,
    tenant_id: int,
    window_start_dt_ref: date | None,
    window_end_dt_ref: date | None,
) -> int:
    start_key = _date_to_key(window_start_dt_ref)
    end_key = _date_to_key(window_end_dt_ref)
    if start_key is None or end_key is None or start_key > end_key:
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*)::int AS total
        FROM dw.fact_risco_evento
        WHERE id_empresa = %s
          AND data_key BETWEEN %s AND %s
        """,
        (tenant_id, start_key, end_key),
    ).fetchone() or {}
    return int(row.get("total") or 0)


def _run_risk_loader_detail(
    conn,
    tenant_id: int,
    *,
    force_full: bool,
    lookback_days: int = 14,
) -> tuple[int, dict[str, Any]]:
    window = _risk_window_detail(
        conn,
        tenant_id,
        force_full=force_full,
        lookback_days=lookback_days,
        end_ts=None,
    )
    window_start_dt_ref = window.get("window_start_dt_ref")
    window_end_dt_ref = window.get("window_end_dt_ref")
    before_count = _count_risk_rows_in_window(conn, tenant_id, window_start_dt_ref, window_end_dt_ref)
    row = conn.execute(
        "SELECT etl.compute_risk_events_v2(%s, %s, %s, %s) AS rows",
        (tenant_id, force_full, lookback_days, None),
    ).fetchone() or {}
    rows = int(row.get("rows") or 0)
    after_count = _count_risk_rows_in_window(conn, tenant_id, window_start_dt_ref, window_end_dt_ref)
    deleted_count = max(before_count + rows - after_count, 0)
    total_mutations = rows + deleted_count
    return rows, {
        "risk_events_window_start_dt_ref": (
            window_start_dt_ref.isoformat() if isinstance(window_start_dt_ref, date) else None
        ),
        "risk_events_window_end_dt_ref": (
            window_end_dt_ref.isoformat() if isinstance(window_end_dt_ref, date) else None
        ),
        "risk_events_window_days": int(window.get("window_days") or 0),
        "risk_events_source_max_ts": window.get("source_max_ts"),
        "risk_events_watermark_before": window.get("watermark_before"),
        "risk_events_source_scan_empty": not bool(window.get("has_source_data")),
        "risk_events_rowcount_before": before_count,
        "risk_events_rowcount_after": after_count,
        "risk_events_rowcount_delta": after_count - before_count,
        "risk_events_deleted": deleted_count,
        "risk_events_total_mutations": total_mutations,
        "risk_events_has_changes": bool(total_mutations > 0 or before_count != after_count),
    }


def _start_step_log(conn, tenant_id: int, step_name: str, meta: dict[str, Any]) -> int:
    row = conn.execute(
        """
        INSERT INTO etl.run_log (
          id_empresa,
          started_at,
          status,
          step_name,
          rows_processed,
          meta
        )
        VALUES (%s, %s, 'running', %s, 0, %s::jsonb)
        RETURNING id
        """,
        (tenant_id, datetime.now(timezone.utc), step_name, _json_dumps(meta)),
    ).fetchone()
    return int((row or {}).get("id") or 0)


def _finish_step_log(
    conn,
    log_id: int,
    *,
    status: str,
    rows_processed: int,
    meta: dict[str, Any],
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE etl.run_log
        SET
          finished_at = %s,
          status = %s,
          rows_processed = %s,
          error = %s,
          duration_ms = GREATEST(0, FLOOR(EXTRACT(epoch FROM (%s::timestamptz - started_at)) * 1000)::int),
          meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
        WHERE id = %s
        """,
        (
            datetime.now(timezone.utc),
            status,
            rows_processed,
            error,
            datetime.now(timezone.utc),
            _json_dumps(meta),
            log_id,
        ),
    )


def _log_instant_step(
    conn,
    tenant_id: int,
    step_name: str,
    *,
    status: str,
    rows_processed: int,
    meta: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
    error: str | None = None,
) -> None:
    started_at = datetime.now(timezone.utc)
    conn.execute(
        """
        INSERT INTO etl.run_log (
          id_empresa,
          started_at,
          finished_at,
          status,
          step_name,
          rows_processed,
          error,
          duration_ms,
          meta
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s::jsonb)
        """,
        (tenant_id, started_at, started_at, status, step_name, rows_processed, error, _json_dumps(meta)),
    )
    conn.commit()
    _emit_progress(
        progress_callback,
        event="step_finished" if status == "ok" else "step_failed",
        tenant_id=tenant_id,
        step_name=step_name,
        status=status,
        rows_processed=rows_processed,
        duration_ms=0,
        meta=meta,
        error=error,
    )


def _run_logged_count_step(
    conn,
    tenant_id: int,
    step_name: str,
    *,
    stage: str,
    ref_date: date,
    operation: Callable[[], int | tuple[int, dict[str, Any]]],
    meta: dict[str, Any] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> tuple[int, int]:
    base_meta = {
        "stage": stage,
        "ref_date": ref_date.isoformat(),
        **(meta or {}),
    }
    log_id = _start_step_log(conn, tenant_id, step_name, base_meta)
    conn.commit()
    started = time.perf_counter()
    _emit_progress(
        progress_callback,
        event="step_started",
        tenant_id=tenant_id,
        step_name=step_name,
        stage=stage,
        meta=base_meta,
    )
    try:
        operation_result = operation()
        extra_meta: dict[str, Any] = {}
        if isinstance(operation_result, tuple):
            raw_rows, raw_meta = operation_result
            rows = int(raw_rows or 0)
            extra_meta = dict(raw_meta or {})
        else:
            rows = int(operation_result or 0)
        duration_ms = round((time.perf_counter() - started) * 1000)
        final_meta = {**base_meta, **extra_meta, "ms": duration_ms}
        _finish_step_log(conn, log_id, status="ok", rows_processed=rows, meta=final_meta)
        conn.commit()
        _emit_progress(
            progress_callback,
            event="step_finished",
            tenant_id=tenant_id,
            step_name=step_name,
            stage=stage,
            rows_processed=rows,
            duration_ms=duration_ms,
            meta=final_meta,
        )
        return rows, duration_ms
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        duration_ms = round((time.perf_counter() - started) * 1000)
        final_meta = {**base_meta, "ms": duration_ms}
        _finish_step_log(conn, log_id, status="failed", rows_processed=0, meta=final_meta, error=str(exc))
        conn.commit()
        _emit_progress(
            progress_callback,
            event="step_failed",
            tenant_id=tenant_id,
            step_name=step_name,
            stage=stage,
            rows_processed=0,
            duration_ms=duration_ms,
            meta=final_meta,
            error=str(exc),
        )
        raise


def _log_stage_summary(
    conn,
    tenant_id: int,
    step_name: str,
    *,
    stage: str,
    started_at: datetime,
    status: str,
    rows_processed: int,
    meta: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
    error: str | None = None,
) -> None:
    finished_at = datetime.now(timezone.utc)
    conn.execute(
        """
        INSERT INTO etl.run_log (
          id_empresa,
          started_at,
          finished_at,
          status,
          step_name,
          rows_processed,
          error,
          duration_ms,
          meta
        )
        VALUES (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          GREATEST(0, FLOOR(EXTRACT(epoch FROM (%s::timestamptz - %s::timestamptz)) * 1000)::int),
          %s::jsonb
        )
        """,
        (
            tenant_id,
            started_at,
            finished_at,
            status,
            step_name,
            rows_processed,
            error,
            finished_at,
            started_at,
            _json_dumps({"stage": stage, **meta}),
        ),
    )
    conn.commit()
    _emit_progress(
        progress_callback,
        event="stage_finished" if status == "ok" else "stage_failed",
        tenant_id=tenant_id,
        step_name=step_name,
        stage=stage,
        status=status,
        rows_processed=rows_processed,
        meta=meta,
        error=error,
    )


def _run_tenant_phase(
    conn,
    tenant_id: int,
    force_full: bool,
    ref_date: date,
    *,
    track: str = TRACK_FULL,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    track = normalize_track(track)
    started_at = datetime.now(timezone.utc)
    hot_window_days = _hot_window_days(conn)
    meta: dict[str, Any] = {"force_full": force_full, "track": track}
    try:
        if force_full and _track_runs_operational(track):
            _run_logged_count_step(
                conn,
                tenant_id,
                "watermark_reset",
                stage="phase",
                ref_date=ref_date,
                operation=lambda: conn.execute(
                    "DELETE FROM etl.watermark WHERE id_empresa = %s",
                    (tenant_id,),
                ).rowcount
                or 0,
                meta={"force_full": True},
                progress_callback=progress_callback,
            )
            meta["watermark_reset"] = True

        step_count = (len(PHASE_SQL_STEPS) if _track_runs_operational(track) else 0) + int(_track_runs_risk(track))
        step_index = 0

        if _track_runs_operational(track):
            for step_name, query in PHASE_SQL_STEPS:
                step_index += 1
                if step_name == "fact_pagamento_comprovante":
                    operation = lambda: _run_payment_loader(
                        conn,
                        tenant_id,
                        force_full=force_full,
                        progress_callback=progress_callback,
                    )
                elif step_name == "fact_venda_item":
                    operation = lambda: _run_venda_item_loader(
                        conn,
                        tenant_id,
                        force_full=force_full,
                        progress_callback=progress_callback,
                    )
                else:
                    operation = lambda q=query: _run_sql_count(conn, q, (tenant_id,))
                rows, step_ms = _run_logged_count_step(
                    conn,
                    tenant_id,
                    step_name,
                    stage="phase",
                    ref_date=ref_date,
                    operation=operation,
                    meta={
                        "force_full": force_full,
                        "track": track,
                        "step_index": step_index,
                        "step_count": step_count,
                    },
                    progress_callback=progress_callback,
                )
                meta[step_name] = rows
                meta[f"{step_name}_ms"] = step_ms

        risk_inputs_changed = any(
            int(meta.get(key, 0) or 0) > 0
            for key in ("fact_comprovante", "fact_venda", "fact_venda_item", "fact_pagamento_comprovante")
        )
        should_compute_risk = _track_runs_risk(track) and (
            force_full
            or risk_inputs_changed
            or (track == TRACK_RISK and _risk_source_watermarks_ahead(conn, tenant_id))
        )
        if should_compute_risk:
            step_index += 1
            risk_detail: dict[str, Any] = {}

            def _risk_operation() -> tuple[int, dict[str, Any]]:
                nonlocal risk_detail
                rows, risk_detail = _run_risk_loader_detail(
                    conn,
                    tenant_id,
                    force_full=force_full,
                    lookback_days=14,
                )
                return rows, risk_detail

            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "risk_events",
                stage="phase",
                ref_date=ref_date,
                operation=_risk_operation,
                meta={
                    "force_full": force_full,
                    "track": track,
                    "step_index": step_index,
                    "step_count": step_count,
                },
                progress_callback=progress_callback,
            )
            meta["risk_events"] = rows
            meta["risk_events_ms"] = step_ms
            meta.update(risk_detail)
        elif _track_runs_risk(track):
            meta["risk_events"] = 0
            meta["risk_events_skipped"] = True
            meta["risk_events_skip_reason"] = (
                "source_watermarks_not_ahead_of_risk"
                if track == TRACK_RISK
                else "no_fact_changes"
            )
            meta["risk_events_has_changes"] = False
            meta["risk_events_total_mutations"] = 0
            _log_instant_step(
                conn,
                tenant_id,
                "risk_events",
                status="ok",
                rows_processed=0,
                meta={
                    "stage": "phase",
                    "track": track,
                    "ref_date": ref_date.isoformat(),
                    "skipped": True,
                    "reason": meta["risk_events_skip_reason"],
                },
                progress_callback=progress_callback,
            )
        else:
            meta["risk_events"] = 0
            meta["risk_events_skipped"] = True
            meta["risk_events_skip_reason"] = "track_excludes_risk"
            meta["risk_events_has_changes"] = False
            meta["risk_events_total_mutations"] = 0

        meta["refresh_domains"] = _phase_domains(meta, force_full=force_full, track=track)
        result = {
            "ok": True,
            "id_empresa": tenant_id,
            "track": track,
            "force_full": force_full,
            "ref_date": ref_date,
            "hot_window_days": hot_window_days,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "meta": meta,
        }
        _log_stage_summary(
            conn,
            tenant_id,
            "run_tenant_phase",
            stage="phase",
            started_at=started_at,
            status="ok",
            rows_processed=1,
            meta={"force_full": force_full, "track": track, "ref_date": ref_date.isoformat(), "meta": meta},
            progress_callback=progress_callback,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        _log_stage_summary(
            conn,
            tenant_id,
            "run_tenant_phase",
            stage="phase",
            started_at=started_at,
            status="failed",
            rows_processed=0,
            error=str(exc),
            meta={"track": track, "ref_date": ref_date.isoformat(), "meta_partial": meta},
            progress_callback=progress_callback,
        )
        raise


def _run_global_refresh(
    conn,
    meta: dict[str, Any],
    ref_date: date,
    *,
    tenant_ids: list[int],
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    log_tenant_id = tenant_ids[0] if len(tenant_ids) == 1 else GLOBAL_REFRESH_LOG_TENANT_ID
    step_meta = {
        "stage": "refresh",
        "scope": "global",
        "tenant_ids": tenant_ids,
        "ref_date": ref_date.isoformat(),
    }
    log_id = _start_step_log(conn, log_tenant_id, "refresh_marts", step_meta)
    conn.commit()
    started = time.perf_counter()
    _emit_progress(
        progress_callback,
        event="step_started",
        tenant_id=log_tenant_id,
        step_name="refresh_marts",
        stage="refresh",
        meta=step_meta,
    )
    try:
        refresh_meta = _run_global_refresh_sql(conn, meta, ref_date)
        rows_processed = 1 if bool(refresh_meta.get("refreshed_any")) else 0
        duration_ms = round((time.perf_counter() - started) * 1000)
        final_meta = {**step_meta, "ms": duration_ms, "refresh": refresh_meta}
        _finish_step_log(conn, log_id, status="ok", rows_processed=rows_processed, meta=final_meta)
        conn.commit()
        _emit_progress(
            progress_callback,
            event="step_finished",
            tenant_id=log_tenant_id,
            step_name="refresh_marts",
            stage="refresh",
            rows_processed=rows_processed,
            duration_ms=duration_ms,
            meta=final_meta,
        )
        return refresh_meta
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        duration_ms = round((time.perf_counter() - started) * 1000)
        final_meta = {**step_meta, "ms": duration_ms}
        _finish_step_log(
            conn,
            log_id,
            status="failed",
            rows_processed=0,
            meta=final_meta,
            error=str(exc),
        )
        conn.commit()
        _emit_progress(
            progress_callback,
            event="step_failed",
            tenant_id=log_tenant_id,
            step_name="refresh_marts",
            stage="refresh",
            rows_processed=0,
            duration_ms=duration_ms,
            meta=final_meta,
            error=str(exc),
        )
        raise


def _run_tenant_post_refresh(
    conn,
    tenant_id: int,
    meta: dict[str, Any],
    ref_date: date,
    force_full: bool,
    hot_window_days: Any,
    *,
    track: str = TRACK_FULL,
    publication_mode: str = PUBLICATION_MODE_GLOBAL_REFRESH,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    track = normalize_track(track)
    started_at = datetime.now(timezone.utc)
    post_meta = _empty_post_meta()
    post_meta["publication_mode"] = publication_mode
    phase_domains = _phase_domains(meta, force_full=force_full, track=track)
    sales_changed = bool(phase_domains["sales"])
    finance_changed = bool(phase_domains["finance"])
    risk_changed = bool(phase_domains["risk"])
    payment_changed = bool(phase_domains["payments"])
    cash_changed = bool(phase_domains["cash"])
    runs_operational = _track_runs_operational(track)
    runs_risk = _track_runs_risk(track)
    effective_hot_window = max(1, int(hot_window_days or _hot_window_days(conn)))
    window_start = ref_date - timedelta(days=effective_hot_window)

    clock_customer_rfm_start = _parse_optional_date(meta.get("clock_customer_rfm_start_dt_ref"))
    clock_customer_rfm_end = _parse_optional_date(meta.get("clock_customer_rfm_end_dt_ref"))
    clock_customer_churn_start = _parse_optional_date(meta.get("clock_customer_churn_risk_start_dt_ref"))
    clock_customer_churn_end = _parse_optional_date(meta.get("clock_customer_churn_risk_end_dt_ref"))
    clock_finance_start = _parse_optional_date(meta.get("clock_finance_aging_start_dt_ref"))
    clock_finance_end = _parse_optional_date(meta.get("clock_finance_aging_end_dt_ref"))
    clock_health_start = _parse_optional_date(meta.get("clock_health_score_start_dt_ref"))
    clock_health_end = _parse_optional_date(meta.get("clock_health_score_end_dt_ref"))
    clock_cash_notifications = bool(meta.get("clock_cash_notifications"))
    risk_window_start = _parse_optional_date(meta.get("risk_events_window_start_dt_ref"))
    risk_window_end = _parse_optional_date(meta.get("risk_events_window_end_dt_ref"))
    risk_window_requested = bool(force_full) or bool(meta.get("risk_events_has_changes")) or int(
        meta.get("risk_events_total_mutations", 0) or 0
    ) > 0 or int(meta.get("risk_events", 0) or 0) > 0

    customer_sales_start = window_start if sales_changed else None
    customer_sales_end = ref_date if sales_changed else None
    customer_rfm_start = window_start if sales_changed else clock_customer_rfm_start
    customer_rfm_end = ref_date if sales_changed else clock_customer_rfm_end
    customer_churn_start = window_start if sales_changed else clock_customer_churn_start
    customer_churn_end = ref_date if sales_changed else clock_customer_churn_end
    finance_start = window_start if finance_changed else clock_finance_start
    finance_end = ref_date if finance_changed else clock_finance_end
    health_window_source = "clock"
    if sales_changed or finance_changed:
        health_start = window_start
        health_end = ref_date
        health_window_source = "hot_window"
    elif (
        runs_risk
        and risk_window_requested
        and risk_window_start is not None
        and risk_window_end is not None
        and risk_window_start <= risk_window_end
    ):
        health_start = risk_window_start
        health_end = risk_window_end
        health_window_source = "risk_delta"
    else:
        health_start = clock_health_start
        health_end = clock_health_end

    risk_insights_window_relevant = False
    if runs_risk and risk_window_requested and risk_window_end is not None:
        risk_insights_window_relevant = risk_window_end >= (ref_date - timedelta(days=6))

    def _skip_step(step_name: str, reason: str) -> None:
        _log_instant_step(
            conn,
            tenant_id,
            step_name,
            status="ok",
            rows_processed=0,
            meta={
                "stage": "post_refresh",
                "track": track,
                "publication_mode": publication_mode,
                "ref_date": ref_date.isoformat(),
                "skipped": True,
                "reason": reason,
            },
            progress_callback=progress_callback,
        )

    try:
        if (
            runs_operational
            and customer_sales_start is not None
            and customer_sales_end is not None
            and customer_sales_start <= customer_sales_end
        ):
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "customer_sales_daily_snapshot",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.backfill_customer_sales_daily_range(%s, %s, %s) AS rows",
                    (tenant_id, customer_sales_start, customer_sales_end),
                ),
                meta={"start_dt_ref": customer_sales_start.isoformat(), "end_dt_ref": customer_sales_end.isoformat()},
                progress_callback=progress_callback,
            )
            post_meta["customer_sales_daily_refreshed"] = True
            post_meta["customer_sales_daily_rows"] = rows
            post_meta["customer_sales_daily_ms"] = step_ms
        else:
            post_meta["customer_sales_daily_skipped"] = True
            _skip_step(
                "customer_sales_daily_snapshot",
                "no_window" if runs_operational else "track_excludes_step",
            )

        if runs_operational and customer_rfm_start is not None and customer_rfm_end is not None and customer_rfm_start <= customer_rfm_end:
            clock_driven = not sales_changed
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "customer_rfm_snapshot",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.backfill_customer_rfm_range(%s, %s, %s) AS rows",
                    (tenant_id, customer_rfm_start, customer_rfm_end),
                ),
                meta={
                    "start_dt_ref": customer_rfm_start.isoformat(),
                    "end_dt_ref": customer_rfm_end.isoformat(),
                    "clock_driven": clock_driven,
                },
                progress_callback=progress_callback,
            )
            post_meta["customer_rfm_refreshed"] = True
            post_meta["customer_rfm_rows"] = rows
            post_meta["customer_rfm_ms"] = step_ms
            post_meta["customer_rfm_clock_driven"] = clock_driven
        else:
            post_meta["customer_rfm_skipped"] = True
            _skip_step("customer_rfm_snapshot", "no_window" if runs_operational else "track_excludes_step")

        if (
            runs_operational
            and customer_churn_start is not None
            and customer_churn_end is not None
            and customer_churn_start <= customer_churn_end
        ):
            clock_driven = not sales_changed
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "customer_churn_risk_snapshot",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.backfill_customer_churn_risk_range(%s, %s, %s) AS rows",
                    (tenant_id, customer_churn_start, customer_churn_end),
                ),
                meta={
                    "start_dt_ref": customer_churn_start.isoformat(),
                    "end_dt_ref": customer_churn_end.isoformat(),
                    "clock_driven": clock_driven,
                },
                progress_callback=progress_callback,
            )
            post_meta["customer_churn_risk_refreshed"] = True
            post_meta["customer_churn_risk_rows"] = rows
            post_meta["customer_churn_risk_ms"] = step_ms
            post_meta["customer_churn_risk_clock_driven"] = clock_driven
        else:
            post_meta["customer_churn_risk_skipped"] = True
            _skip_step(
                "customer_churn_risk_snapshot",
                "no_window" if runs_operational else "track_excludes_step",
            )

        if runs_operational and finance_start is not None and finance_end is not None and finance_start <= finance_end:
            clock_driven = not finance_changed
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "finance_aging_snapshot",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.backfill_finance_aging_range(%s, %s, %s) AS rows",
                    (tenant_id, finance_start, finance_end),
                ),
                meta={
                    "start_dt_ref": finance_start.isoformat(),
                    "end_dt_ref": finance_end.isoformat(),
                    "clock_driven": clock_driven,
                },
                progress_callback=progress_callback,
            )
            post_meta["finance_aging_refreshed"] = True
            post_meta["finance_aging_rows"] = rows
            post_meta["finance_aging_ms"] = step_ms
            post_meta["finance_aging_clock_driven"] = clock_driven
        else:
            post_meta["finance_aging_skipped"] = True
            _skip_step("finance_aging_snapshot", "no_window" if runs_operational else "track_excludes_step")

        if runs_risk and health_start is not None and health_end is not None and health_start <= health_end:
            clock_driven = not (sales_changed or finance_changed or risk_changed)
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "health_score_snapshot",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.backfill_health_score_range(%s, %s, %s) AS rows",
                    (tenant_id, health_start, health_end),
                ),
                meta={
                    "start_dt_ref": health_start.isoformat(),
                    "end_dt_ref": health_end.isoformat(),
                    "clock_driven": clock_driven,
                },
                progress_callback=progress_callback,
            )
            post_meta["health_score_refreshed"] = True
            post_meta["health_score_rows"] = rows
            post_meta["health_score_ms"] = step_ms
            post_meta["health_score_clock_driven"] = clock_driven
            post_meta["health_score_window_source"] = health_window_source
        else:
            post_meta["health_score_skipped"] = True
            _skip_step("health_score_snapshot", "no_window" if runs_risk else "track_excludes_step")

        if runs_operational and payment_changed:
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "payment_notifications",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.sync_payment_anomaly_notifications(%s, %s) AS rows",
                    (tenant_id, ref_date),
                ),
                progress_callback=progress_callback,
            )
            post_meta["payment_notifications"] = rows
            post_meta["payment_notifications_ms"] = step_ms
            post_meta["notification_rows"] = rows
            post_meta["notification_ms"] = step_ms
        else:
            post_meta["payment_notifications_skipped"] = True
            _skip_step("payment_notifications", "no_payment_changes" if runs_operational else "track_excludes_step")

        if runs_operational and (cash_changed or clock_cash_notifications):
            cash_clock_driven = not cash_changed and clock_cash_notifications
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "cash_notifications",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.sync_cash_open_notifications(%s) AS rows",
                    (tenant_id,),
                ),
                meta={"clock_driven": cash_clock_driven},
                progress_callback=progress_callback,
            )
            post_meta["cash_notifications"] = rows
            post_meta["cash_notifications_ms"] = step_ms
            post_meta["cash_notifications_clock_driven"] = cash_clock_driven
        else:
            post_meta["cash_notifications_skipped"] = True
            _skip_step("cash_notifications", "no_cash_changes" if runs_operational else "track_excludes_step")

        if runs_risk and (sales_changed or finance_changed or risk_insights_window_relevant):
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "insights_generated",
                stage="post_refresh",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.generate_insights(%s, %s, %s) AS rows",
                    (tenant_id, ref_date, 7),
                ),
                progress_callback=progress_callback,
            )
            post_meta["insights_generated"] = rows
            post_meta["insights_generated_ms"] = step_ms
        else:
            post_meta["insights_generated_skipped"] = True
            _skip_step("insights_generated", "no_domain_changes" if runs_risk else "track_excludes_step")

        snapshot_bounds = [
            customer_sales_start,
            customer_sales_end,
            customer_rfm_start,
            customer_rfm_end,
            customer_churn_start,
            customer_churn_end,
            finance_start,
            finance_end,
            health_start,
            health_end,
        ]
        valid_bounds = [bound for bound in snapshot_bounds if bound is not None]
        snapshot_window_start = min(valid_bounds) if valid_bounds else None
        snapshot_window_end = max(valid_bounds) if valid_bounds else None
        snapshot_window_days = (
            max(1, (snapshot_window_end - snapshot_window_start).days + 1)
            if snapshot_window_start is not None and snapshot_window_end is not None
            else 0
        )

        result = post_meta | {
            "snapshot_window_days": snapshot_window_days,
            "snapshot_window_start_dt_ref": snapshot_window_start,
            "snapshot_window_end_dt_ref": snapshot_window_end,
            "clock_daily_rollover": bool(meta.get("clock_daily_rollover")),
            "clock_open_cash_turns": bool(meta.get("clock_open_cash_turns")),
            "publication_mode": publication_mode,
            "risk_events_window_start_dt_ref": risk_window_start,
            "risk_events_window_end_dt_ref": risk_window_end,
            "risk_events_has_changes": risk_window_requested,
        }
        _log_stage_summary(
            conn,
            tenant_id,
            "run_tenant_post_refresh",
            stage="post_refresh",
            started_at=started_at,
            status="ok",
            rows_processed=1,
            meta={
                "ref_date": ref_date.isoformat(),
                "track": track,
                "publication_mode": publication_mode,
                "window_days": snapshot_window_days,
                "window_start": snapshot_window_start.isoformat() if snapshot_window_start else None,
                "meta": post_meta,
            },
            progress_callback=progress_callback,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        _log_stage_summary(
            conn,
            tenant_id,
            "run_tenant_post_refresh",
            stage="post_refresh",
            started_at=started_at,
            status="failed",
            rows_processed=0,
            error=str(exc),
            meta={
                "track": track,
                "ref_date": ref_date.isoformat(),
                "publication_mode": publication_mode,
                "meta_partial": post_meta,
            },
            progress_callback=progress_callback,
        )
        raise


def _run_tenant_phase_sql(conn, tenant_id: int, force_full: bool, ref_date: date) -> dict[str, Any]:
    row = conn.execute(
        "SELECT etl.run_tenant_phase(%s, %s, %s) AS result",
        (tenant_id, force_full, ref_date),
    ).fetchone()
    return row["result"] if row else {}


def _run_global_refresh_sql(conn, meta: dict[str, Any], ref_date: date) -> dict[str, Any]:
    row = conn.execute(
        "SELECT etl.refresh_marts(%s::jsonb, %s::date) AS result",
        (_json_dumps(meta), ref_date),
    ).fetchone()
    return row["result"] if row else {}


def _run_tenant_clock_meta_sql(conn, tenant_id: int, ref_date: date) -> dict[str, Any]:
    row = conn.execute(
        "SELECT etl.collect_tenant_clock_meta(%s, %s::date) AS result",
        (tenant_id, ref_date),
    ).fetchone()
    return row["result"] if row else {}


def _run_tenant_post_refresh_sql(
    conn,
    tenant_id: int,
    meta: dict[str, Any],
    ref_date: date,
    force_full: bool,
) -> dict[str, Any]:
    payload = dict(meta)
    payload["force_full"] = force_full
    row = conn.execute(
        "SELECT etl.run_tenant_post_refresh(%s, %s::jsonb, %s::date) AS result",
        (tenant_id, _json_dumps(payload), ref_date),
    ).fetchone()
    return row["result"] if row else {}


def _item_post_refresh_meta(item: dict[str, Any]) -> dict[str, Any]:
    payload = dict(item.get("phase_meta") or {})
    payload.update(item.get("clock_meta") or {})
    return payload


def _item_needs_post_refresh(item: dict[str, Any]) -> bool:
    if any(item.get("phase_domains", {}).values()):
        return True
    clock_meta = item.get("clock_meta") or {}
    if bool(clock_meta.get("clock_cash_notifications")):
        return True
    return any(clock_meta.get(key) for key in CLOCK_POST_REFRESH_RANGE_KEYS)


def _dispatch_payment_telegram_alerts(conn, tenant_id: int, ref_date: date) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT
          id_filial,
          id_turno,
          event_type,
          score,
          impacto_estimado,
          data_key,
          reasons,
          insight_id_hash
        FROM mart.pagamentos_anomalias_diaria
        WHERE id_empresa = %s
          AND severity = 'CRITICAL'
          AND data_key >= to_char((%s::date - interval '2 day')::date, 'YYYYMMDD')::int
        ORDER BY data_key DESC, score DESC, impacto_estimado DESC
        LIMIT 5
        """,
        (tenant_id, ref_date),
    ).fetchall()
    telegram_sent = 0
    telegram_suppressed = 0
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = {
            "severity": "CRITICAL",
            "insight_id": int(row["insight_id_hash"]) if row.get("insight_id_hash") is not None else None,
            "insight_type": f"PAYMENT_{row['event_type']}",
            "id_filial": int(row["id_filial"]) if row.get("id_filial") is not None else None,
            "event_time": str(row.get("data_key") or ""),
            "impacto_estimado": float(row.get("impacto_estimado") or 0),
            "title": f"Anomalia de pagamento ({row['event_type']})",
            "body": (
                f"Score {int(row.get('score') or 0)}"
                + (f" | Turno {int(row['id_turno'])}" if row.get("id_turno") is not None and int(row["id_turno"]) >= 0 else "")
            ),
            "url": "/fraud",
            "event_type": str(row["event_type"]),
        }
        telegram = _safe_send_telegram_alert(tenant_id, payload)
        if telegram.get("sent"):
            telegram_sent += 1
        else:
            telegram_suppressed += 1
        items.append(
            {
                "id_filial": row.get("id_filial"),
                "id_turno": row.get("id_turno"),
                "event_type": row.get("event_type"),
                "score": int(row.get("score") or 0),
                "impacto_estimado": float(row.get("impacto_estimado") or 0),
                "data_key": row.get("data_key"),
            }
        )
    return {
        "critical_events": len(items),
        "telegram_sent": telegram_sent,
        "telegram_suppressed": telegram_suppressed,
        "items": items,
    }


def _dispatch_cash_telegram_alerts(conn, tenant_id: int) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT
          id_filial,
          filial_nome,
          id_turno,
          id_usuario,
          usuario_nome,
          horas_aberto,
          title,
          body,
          url,
          insight_id_hash
        FROM mart.alerta_caixa_aberto
        WHERE id_empresa = %s
        ORDER BY horas_aberto DESC, id_turno DESC
        LIMIT 5
        """,
        (tenant_id,),
    ).fetchall()
    telegram_sent = 0
    telegram_suppressed = 0
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = {
            "severity": "CRITICAL",
            "insight_id": int(row["insight_id_hash"]) if row.get("insight_id_hash") is not None else None,
            "insight_type": "CASH_OPEN_OVER_24H",
            "id_filial": int(row["id_filial"]) if row.get("id_filial") is not None else None,
            "filial_nome": row.get("filial_nome"),
            "event_time": datetime.now(tz=timezone.utc).isoformat(),
            "impacto_estimado": 0,
            "title": row.get("title") or "Caixa aberto acima do limite",
            "body": row.get("body") or "",
            "url": row.get("url") or "/cash",
            "event_type": "CASH_OPEN_OVER_24H",
        }
        telegram = _safe_send_telegram_alert(tenant_id, payload)
        if telegram.get("sent"):
            telegram_sent += 1
        else:
            telegram_suppressed += 1
        items.append(
            {
                "id_filial": row.get("id_filial"),
                "filial_nome": row.get("filial_nome"),
                "id_turno": row.get("id_turno"),
                "usuario_nome": row.get("usuario_nome"),
                "horas_aberto": float(row.get("horas_aberto") or 0),
            }
        )
    return {
        "critical_events": len(items),
        "telegram_sent": telegram_sent,
        "telegram_suppressed": telegram_suppressed,
        "items": items,
    }


def _safe_send_telegram_alert(tenant_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return send_telegram_alert(id_empresa=tenant_id, payload=payload)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "sent": False, "reason": "dispatch_error", "error": str(exc)}


def _try_advisory_lock(conn, left: int, right: int) -> bool:
    row = conn.execute("SELECT pg_try_advisory_lock(%s, %s) AS locked", (left, right)).fetchone()
    return bool(row and row["locked"])


def advisory_lock_is_available(conn, left: int, right: int) -> bool:
    if not _try_advisory_lock(conn, left, right):
        return False
    _unlock_advisory_lock(conn, left, right)
    return True


def _unlock_advisory_lock(conn, left: int, right: int) -> None:
    conn.execute("SELECT pg_advisory_unlock(%s, %s)", (left, right))


def _try_cycle_locks(conn, track: str) -> list[tuple[int, int]]:
    acquired: list[tuple[int, int]] = []
    for left, right in TRACK_CYCLE_LOCKS[track]:
        if _try_advisory_lock(conn, left, right):
            acquired.append((left, right))
            continue
        _unlock_cycle_locks(conn, acquired)
        return []
    return acquired


def _unlock_cycle_locks(conn, locks: list[tuple[int, int]]) -> None:
    for left, right in reversed(locks):
        with suppress(Exception):
            _unlock_advisory_lock(conn, left, right)


def _try_tenant_track_lock(conn, tenant_id: int) -> bool:
    return _try_advisory_lock(conn, TENANT_TRACK_LOCK_NAMESPACE, int(tenant_id))


def _unlock_tenant_track_lock(conn, tenant_id: int) -> None:
    _unlock_advisory_lock(conn, TENANT_TRACK_LOCK_NAMESPACE, int(tenant_id))


def inspect_track_locks(conn, track: str, tenant_id: int | None = None) -> dict[str, Any]:
    normalized_track = normalize_track(track)
    cycle_locks = [
        {
            "left": left,
            "right": right,
            "available": advisory_lock_is_available(conn, left, right),
        }
        for left, right in TRACK_CYCLE_LOCKS[normalized_track]
    ]
    tenant_available = None
    if tenant_id is not None:
        tenant_available = advisory_lock_is_available(conn, TENANT_TRACK_LOCK_NAMESPACE, int(tenant_id))
    return {
        "track": normalized_track,
        "cycle_available": all(bool(item["available"]) for item in cycle_locks),
        "tenant_available": tenant_available,
        "cycle_locks": cycle_locks,
    }
