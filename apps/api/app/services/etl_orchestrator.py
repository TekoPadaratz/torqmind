from __future__ import annotations

import json
import time
from contextlib import suppress
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from app.db import get_conn
from app.services.telegram import send_telegram_alert

LOCK_KEY_LEFT = 62041
LOCK_KEY_RIGHT = 230319

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

GLOBAL_REFRESH_LOG_TENANT_ID = 0

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
)

ProgressCallback = Callable[[dict[str, Any]], None]


class EtlCycleBusyError(RuntimeError):
    pass


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
    db_role: str = "MASTER",
    db_tenant_scope: int | None = None,
    tenant_rows: list[dict[str, Any]] | None = None,
    acquire_lock: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    tenant_rows = tenant_rows or [{"id_empresa": tenant_id} for tenant_id in tenant_ids]
    tenant_by_id = {int(row["id_empresa"]): row for row in tenant_rows}
    if not tenant_ids:
        return {
            "ok": True,
            "reference_date": ref_date.isoformat(),
            "processed": 0,
            "failed": 0,
            "duration_ms": 0.0,
            "global_refresh": _empty_refresh_meta(ref_date),
            "items": [],
        }

    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    successful_items: list[dict[str, Any]] = []
    fail_fast_abort_reason: str | None = None
    cycle_started = time.perf_counter()

    with get_conn(role=db_role, tenant_id=db_tenant_scope, branch_id=None) as conn:
        if acquire_lock and not _try_cycle_lock(conn):
            raise EtlCycleBusyError("Incremental ETL cycle is already running.")

        try:
            for tenant_id in tenant_ids:
                tenant_ctx = tenant_by_id.get(int(tenant_id), {})
                item_started = time.perf_counter()
                _emit_progress(
                    progress_callback,
                    event="tenant_started",
                    tenant_id=int(tenant_id),
                    tenant_name=tenant_ctx.get("nome"),
                    stage="phase",
                    ref_date=ref_date.isoformat(),
                )
                try:
                    phase_result = _run_tenant_phase(
                        conn,
                        int(tenant_id),
                        force_full,
                        ref_date,
                        progress_callback=progress_callback,
                    )
                except Exception as exc:  # noqa: BLE001
                    conn.rollback()
                    failure = {
                        "tenant_id": int(tenant_id),
                        "tenant_name": tenant_ctx.get("nome"),
                        "tenant_status": tenant_ctx.get("status"),
                        "error": str(exc),
                        "ok": False,
                    }
                    failures.append(failure)
                    items.append(failure)
                    _emit_progress(
                        progress_callback,
                        event="tenant_finished",
                        tenant_id=int(tenant_id),
                        tenant_name=tenant_ctx.get("nome"),
                        ok=False,
                        error=str(exc),
                    )
                    if fail_fast:
                        break
                    continue

                phase_meta = _extract_meta(phase_result)
                clock_meta = _run_tenant_clock_meta_sql(conn, int(tenant_id), ref_date)
                item = {
                    "tenant_id": int(tenant_id),
                    "tenant_name": tenant_ctx.get("nome"),
                    "tenant_status": tenant_ctx.get("status"),
                    "is_active": bool(tenant_ctx.get("is_active", True)),
                    "phase_result": phase_result,
                    "phase_meta": phase_meta,
                    "clock_meta": clock_meta,
                    "phase_domains": _phase_domains(phase_meta, force_full=force_full),
                    "_perf_started": item_started,
                    "elapsed_ms": 0.0,
                    "ok": True,
                }
                items.append(item)
                successful_items.append(item)

            aggregated_meta = _aggregate_refresh_meta(successful_items, force_full=force_full)
            refresh_meta = _empty_refresh_meta(ref_date)
            if refresh_mart and successful_items:
                refresh_meta = _run_global_refresh(
                    conn,
                    aggregated_meta,
                    ref_date,
                    tenant_ids=[int(item["tenant_id"]) for item in successful_items],
                    progress_callback=progress_callback,
                )

            refreshed_any = bool(refresh_meta.get("refreshed_any"))

            for item in successful_items:
                payment_details = _empty_notification_details()
                cash_details = _empty_notification_details()
                post_meta = _empty_post_meta()
                needs_post_refresh = refresh_mart and refreshed_any and _item_needs_post_refresh(item)

                if needs_post_refresh:
                    try:
                        post_meta = _run_tenant_post_refresh(
                            conn,
                            item["tenant_id"],
                            _item_post_refresh_meta(item),
                            ref_date,
                            force_full,
                            item["phase_result"].get("hot_window_days"),
                            progress_callback=progress_callback,
                        )
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
                            ok=False,
                            error=str(exc),
                        )
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
                )
                item["result"] = {
                    "ok": item.get("ok", True),
                    "id_empresa": item["tenant_id"],
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
                    ok=item.get("ok", True),
                    elapsed_ms=item["elapsed_ms"],
                    mart_refreshed=item.get("mart_refreshed"),
                )

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
                        ok=False,
                        error=fail_fast_abort_reason,
                    )

            processed_items = []
            for item in items:
                if item.get("ok") is False and "result" not in item:
                    processed_items.append(
                        {
                            "tenant_id": item["tenant_id"],
                            "tenant_name": item.get("tenant_name"),
                            "tenant_status": item.get("tenant_status"),
                            "is_active": item.get("is_active"),
                            "ok": False,
                            "error": item.get("error"),
                        }
                    )
                    continue
                processed_items.append(
                    {
                        "tenant_id": item["tenant_id"],
                        "tenant_name": item.get("tenant_name"),
                        "tenant_status": item.get("tenant_status"),
                        "is_active": item.get("is_active"),
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
                "reference_date": ref_date.isoformat(),
                "processed": len(processed_items),
                "failed": len(failures),
                "duration_ms": round((time.perf_counter() - cycle_started) * 1000, 2),
                "global_refresh": refresh_meta,
                "items": processed_items,
            }
        finally:
            if acquire_lock:
                with suppress(Exception):
                    _unlock_cycle(conn)


def _empty_refresh_meta(ref_date: date) -> dict[str, Any]:
    return {
        "ref_date": ref_date.isoformat(),
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
        "cash_notifications": 0,
        "insights_generated": 0,
        "customer_rfm_clock_driven": False,
        "customer_churn_risk_clock_driven": False,
        "finance_aging_clock_driven": False,
        "health_score_clock_driven": False,
        "cash_notifications_clock_driven": False,
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


def _aggregate_phase_meta(items: list[dict[str, Any]], *, force_full: bool) -> dict[str, Any]:
    aggregated: dict[str, Any] = {"force_full": force_full}
    for item in items:
        meta = item.get("phase_meta") or {}
        for key in PHASE_META_KEYS:
            aggregated[key] = int(aggregated.get(key, 0) or 0) + int(meta.get(key, 0) or 0)
    return aggregated


def _aggregate_refresh_meta(items: list[dict[str, Any]], *, force_full: bool) -> dict[str, Any]:
    aggregated = _aggregate_phase_meta(items, force_full=force_full)
    for key in CLOCK_REFRESH_META_KEYS:
        aggregated[key] = any(bool(item.get("clock_meta", {}).get(key)) for item in items)
    return aggregated


def _phase_domains(meta: dict[str, Any], *, force_full: bool) -> dict[str, bool]:
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
            for key in ("dim_grupos", "dim_produtos", "dim_funcionarios", "dim_clientes", "fact_comprovante", "fact_venda", "fact_venda_item")
        ),
        "finance": int(meta.get("fact_financeiro", 0) or 0) > 0,
        "risk": any(int(meta.get(key, 0) or 0) > 0 for key in ("risk_events", "dim_funcionarios")),
        "payments": any(int(meta.get(key, 0) or 0) > 0 for key in ("fact_pagamento_comprovante", "fact_comprovante")),
        "cash": any(int(meta.get(key, 0) or 0) > 0 for key in ("fact_caixa_turno", "fact_pagamento_comprovante", "fact_comprovante", "dim_usuario_caixa")),
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
) -> dict[str, Any]:
    combined = dict(phase_meta)
    combined["clock_meta"] = clock_meta
    combined["mart_refresh"] = refresh_meta
    combined["mart_refreshed"] = refreshed_any
    combined["refresh_requested"] = refresh_requested
    combined["post_refresh_executed"] = post_refresh_executed
    combined.update(post_meta)
    return combined


def _emit_progress(progress_callback: ProgressCallback | None, /, **event: Any) -> None:
    if progress_callback is None:
        return
    payload = {"ts": datetime.now(timezone.utc).isoformat(), **event}
    with suppress(Exception):
        progress_callback(payload)


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _parse_optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _hot_window_days(conn) -> int:
    row = conn.execute("SELECT etl.hot_window_days() AS value").fetchone()
    return max(1, int((row or {}).get("value") or 1))


def _run_sql_count(conn, query: str, params: tuple[Any, ...]) -> int:
    row = conn.execute(query, params).fetchone()
    return int((row or {}).get("rows") or 0)


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
    operation: Callable[[], int],
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
        rows = int(operation() or 0)
        duration_ms = round((time.perf_counter() - started) * 1000)
        final_meta = {**base_meta, "ms": duration_ms}
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
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    hot_window_days = _hot_window_days(conn)
    meta: dict[str, Any] = {"force_full": force_full}
    try:
        if force_full:
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

        for step_index, (step_name, query) in enumerate(PHASE_SQL_STEPS, start=1):
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                step_name,
                stage="phase",
                ref_date=ref_date,
                operation=lambda q=query: _run_sql_count(conn, q, (tenant_id,)),
                meta={
                    "force_full": force_full,
                    "step_index": step_index,
                    "step_count": len(PHASE_SQL_STEPS) + 1,
                },
                progress_callback=progress_callback,
            )
            meta[step_name] = rows
            meta[f"{step_name}_ms"] = step_ms

        should_compute_risk = force_full or any(
            int(meta.get(key, 0) or 0) > 0
            for key in ("fact_comprovante", "fact_venda", "fact_venda_item", "fact_pagamento_comprovante")
        )
        if should_compute_risk:
            rows, step_ms = _run_logged_count_step(
                conn,
                tenant_id,
                "risk_events",
                stage="phase",
                ref_date=ref_date,
                operation=lambda: _run_sql_count(
                    conn,
                    "SELECT etl.compute_risk_events(%s, %s, %s, %s) AS rows",
                    (tenant_id, force_full, 14, None),
                ),
                meta={"force_full": force_full, "step_index": len(PHASE_SQL_STEPS) + 1, "step_count": len(PHASE_SQL_STEPS) + 1},
                progress_callback=progress_callback,
            )
            meta["risk_events"] = rows
            meta["risk_events_ms"] = step_ms
        else:
            meta["risk_events"] = 0
            meta["risk_events_skipped"] = True
            meta["risk_events_skip_reason"] = "no_fact_changes"
            _log_instant_step(
                conn,
                tenant_id,
                "risk_events",
                status="ok",
                rows_processed=0,
                meta={
                    "stage": "phase",
                    "ref_date": ref_date.isoformat(),
                    "skipped": True,
                    "reason": "no_fact_changes",
                },
                progress_callback=progress_callback,
            )

        meta["refresh_domains"] = _phase_domains(meta, force_full=force_full)
        result = {
            "ok": True,
            "id_empresa": tenant_id,
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
            meta={"force_full": force_full, "ref_date": ref_date.isoformat(), "meta": meta},
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
            meta={"ref_date": ref_date.isoformat(), "meta_partial": meta},
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
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    post_meta = _empty_post_meta()
    phase_domains = _phase_domains(meta, force_full=force_full)
    sales_changed = bool(phase_domains["sales"])
    finance_changed = bool(phase_domains["finance"])
    risk_changed = bool(phase_domains["risk"])
    payment_changed = bool(phase_domains["payments"])
    cash_changed = bool(phase_domains["cash"])
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

    customer_sales_start = window_start if sales_changed else None
    customer_sales_end = ref_date if sales_changed else None
    customer_rfm_start = window_start if sales_changed else clock_customer_rfm_start
    customer_rfm_end = ref_date if sales_changed else clock_customer_rfm_end
    customer_churn_start = window_start if sales_changed else clock_customer_churn_start
    customer_churn_end = ref_date if sales_changed else clock_customer_churn_end
    finance_start = window_start if finance_changed else clock_finance_start
    finance_end = ref_date if finance_changed else clock_finance_end
    health_start = window_start if (sales_changed or finance_changed or risk_changed) else clock_health_start
    health_end = ref_date if (sales_changed or finance_changed or risk_changed) else clock_health_end

    try:
        if customer_sales_start is not None and customer_sales_end is not None and customer_sales_start <= customer_sales_end:
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
            _log_instant_step(
                conn,
                tenant_id,
                "customer_sales_daily_snapshot",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_window"},
                progress_callback=progress_callback,
            )

        if customer_rfm_start is not None and customer_rfm_end is not None and customer_rfm_start <= customer_rfm_end:
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
            _log_instant_step(
                conn,
                tenant_id,
                "customer_rfm_snapshot",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_window"},
                progress_callback=progress_callback,
            )

        if customer_churn_start is not None and customer_churn_end is not None and customer_churn_start <= customer_churn_end:
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
            _log_instant_step(
                conn,
                tenant_id,
                "customer_churn_risk_snapshot",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_window"},
                progress_callback=progress_callback,
            )

        if finance_start is not None and finance_end is not None and finance_start <= finance_end:
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
            _log_instant_step(
                conn,
                tenant_id,
                "finance_aging_snapshot",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_window"},
                progress_callback=progress_callback,
            )

        if health_start is not None and health_end is not None and health_start <= health_end:
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
        else:
            post_meta["health_score_skipped"] = True
            _log_instant_step(
                conn,
                tenant_id,
                "health_score_snapshot",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_window"},
                progress_callback=progress_callback,
            )

        if payment_changed:
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
        else:
            post_meta["payment_notifications_skipped"] = True
            _log_instant_step(
                conn,
                tenant_id,
                "payment_notifications",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_payment_changes"},
                progress_callback=progress_callback,
            )

        if cash_changed or clock_cash_notifications:
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
            _log_instant_step(
                conn,
                tenant_id,
                "cash_notifications",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_cash_changes"},
                progress_callback=progress_callback,
            )

        if sales_changed or finance_changed or risk_changed:
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
            _log_instant_step(
                conn,
                tenant_id,
                "insights_generated",
                status="ok",
                rows_processed=0,
                meta={"stage": "post_refresh", "ref_date": ref_date.isoformat(), "skipped": True, "reason": "no_domain_changes"},
                progress_callback=progress_callback,
            )

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
            meta={"ref_date": ref_date.isoformat(), "meta_partial": post_meta},
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


def _try_cycle_lock(conn) -> bool:
    row = conn.execute(
        "SELECT pg_try_advisory_lock(%s, %s) AS locked",
        (LOCK_KEY_LEFT, LOCK_KEY_RIGHT),
    ).fetchone()
    return bool(row and row["locked"])


def _unlock_cycle(conn) -> None:
    conn.execute(
        "SELECT pg_advisory_unlock(%s, %s)",
        (LOCK_KEY_LEFT, LOCK_KEY_RIGHT),
    )
