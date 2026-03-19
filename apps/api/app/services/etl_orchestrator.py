from __future__ import annotations

import json
import time
from contextlib import suppress
from datetime import date, datetime, timezone
from typing import Any

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
                try:
                    phase_result = _run_tenant_phase_sql(conn, int(tenant_id), force_full, ref_date)
                    conn.commit()
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
                refresh_meta = _run_global_refresh_sql(conn, aggregated_meta, ref_date)
                conn.commit()

            refreshed_any = bool(refresh_meta.get("refreshed_any"))

            for item in successful_items:
                payment_details = _empty_notification_details()
                cash_details = _empty_notification_details()
                post_meta = _empty_post_meta()
                needs_post_refresh = refresh_mart and refreshed_any and _item_needs_post_refresh(item)

                if needs_post_refresh:
                    try:
                        post_meta = _run_tenant_post_refresh_sql(conn, item["tenant_id"], _item_post_refresh_meta(item), ref_date, force_full)
                        conn.commit()
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


def _run_tenant_phase_sql(conn, tenant_id: int, force_full: bool, ref_date: date) -> dict[str, Any]:
    row = conn.execute(
        "SELECT etl.run_tenant_phase(%s, %s, %s) AS result",
        (tenant_id, force_full, ref_date),
    ).fetchone()
    return row["result"] if row else {}


def _run_global_refresh_sql(conn, meta: dict[str, Any], ref_date: date) -> dict[str, Any]:
    row = conn.execute(
        "SELECT etl.refresh_marts(%s::jsonb, %s::date) AS result",
        (json.dumps(meta, ensure_ascii=False, default=str), ref_date),
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
        (tenant_id, json.dumps(payload, ensure_ascii=False, default=str), ref_date),
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
