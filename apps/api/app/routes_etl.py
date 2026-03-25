from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException, Header

from app.config import settings
from app.db import get_conn
from app.deps import get_current_claims
from app import repos_auth
from app.security import decode_token
from app.scope import resolve_scope
from app.services.etl_orchestrator import EtlCycleBusyError, TRACK_OPERATIONAL, normalize_track, run_incremental_cycle
from app.services.telegram import send_telegram_alert

router = APIRouter(prefix="/etl", tags=["etl"])
logger = logging.getLogger(__name__)


@router.post("/run")
def run_etl(
    refresh_mart: bool = Query(True),
    force_full: bool = Query(False),
    track: str = Query(TRACK_OPERATIONAL, pattern="^(operational|risk|full)$", description="ETL lane: operational, risk, or full."),
    ref_date: Optional[date] = Query(None, description="Reference date used as simulated 'today' (YYYY-MM-DD)"),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    """Run the STG→DW→MART pipeline for a tenant.

    PT-BR: Esse endpoint é o botão "atualizar dados" do seu BI.
    EN   : This is the "refresh" button for your BI.

    Security:
    - MASTER can run for any tenant (id_empresa query param).
    - OWNER/MANAGER can run only for their tenant.
    """

    role = claims["role"]
    track = normalize_track(track)
    effective_ref_date = ref_date or date.today()
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    tenant, _ = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=None)

    # Managers typically should not run ETL in production, but for dev we allow.
    try:
        summary = run_incremental_cycle(
            [tenant],
            ref_date=effective_ref_date,
            refresh_mart=refresh_mart,
            force_full=force_full,
            fail_fast=True,
            track=track,
            skip_busy_tenants=False,
            db_role=role,
            db_tenant_scope=tenant,
            tenant_rows=[{"id_empresa": tenant}],
            acquire_lock=True,
        )
        item = (summary.get("items") or [None])[0] or {}
        if item.get("error_code") == "tenant_busy":
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "etl_busy",
                    "message": str(item.get("error") or "Tenant is busy with another ETL lane."),
                },
            )
        if item.get("ok") is False:
            raise RuntimeError(str(item.get("error") or "tenant_failed"))
        if item.get("skipped"):
            return {
                "ok": True,
                "track": track,
                "skipped": True,
                "reason": item.get("reason"),
                "message": item.get("message"),
            }
        return item.get("result") or {}
    except EtlCycleBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "etl_busy",
                "message": str(exc),
            },
        )
    except Exception as e:
        logger.exception("ETL failed for tenant=%s refresh_mart=%s force_full=%s", tenant, refresh_mart, force_full, exc_info=e)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "etl_failed",
                "message": "Falha ao atualizar dados. Tente novamente em instantes.",
            },
        )


def _resolve_id_empresa_from_ingest(x_ingest_key: Optional[str]) -> Optional[int]:
    if not x_ingest_key:
        return None
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        try:
            row = conn.execute(
                "SELECT id_empresa FROM app.tenants WHERE ingest_key = %s AND is_active = true",
                (x_ingest_key,),
            ).fetchone()
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
        if not row:
            raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
        return int(row["id_empresa"])


def _resolve_micro_scope(
    id_empresa_q: Optional[int],
    id_filial_q: Optional[int],
    authorization: Optional[str],
    x_ingest_key: Optional[str],
    x_internal_key: Optional[str],
) -> tuple[int, Optional[int], str]:
    if x_internal_key:
        if not settings.etl_internal_key or x_internal_key != settings.etl_internal_key:
            raise HTTPException(status_code=401, detail="Invalid X-Internal-Key")
        return int(id_empresa_q or 1), id_filial_q, "INTERNAL"

    ingest_empresa = _resolve_id_empresa_from_ingest(x_ingest_key)
    if ingest_empresa is not None:
        filial = int(id_filial_q) if id_filial_q is not None else None
        return ingest_empresa, filial, "INGEST_KEY"

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing auth (Bearer or X-Ingest-Key/X-Internal-Key)")

    token = authorization.split(" ", 1)[1].strip()
    payload = decode_token(token)
    try:
        claims = repos_auth.get_session_context(
            user_id=str(payload.get("sub") or ""),
            id_empresa=payload.get("id_empresa"),
            id_filial=payload.get("id_filial"),
            channel_id=payload.get("channel_id"),
        )
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    tenant, filial = resolve_scope(claims, id_empresa_q=id_empresa_q, id_filial_q=id_filial_q)
    return tenant, filial, "BEARER"


def _upsert_notification_critical(
    id_empresa: int,
    id_filial: Optional[int],
    insight_id: int,
    title: str,
    body: str,
    url: str,
) -> None:
    sql_update = """
      UPDATE app.notifications
      SET
        severity = 'CRITICAL',
        title = %s,
        body = %s,
        url = %s,
        created_at = now(),
        read_at = NULL
      WHERE id_empresa = %s
        AND insight_id = %s
        AND (
          (id_filial IS NULL AND %s IS NULL)
          OR id_filial = %s
        )
    """
    sql_insert = """
      INSERT INTO app.notifications (id_empresa, id_filial, insight_id, severity, title, body, url)
      VALUES (%s,%s,%s,'CRITICAL',%s,%s,%s)
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(sql_update, (title, body, url, id_empresa, insight_id, id_filial, id_filial))
        if (cur.rowcount or 0) == 0:
            conn.execute(sql_insert, (id_empresa, id_filial, insight_id, title, body, url))
        conn.commit()


@router.post("/micro_risk")
def run_micro_risk(
    minutes: int = Query(5, ge=1, le=120),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER or internal key"),
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_internal_key: Optional[str] = Header(None, alias="X-Internal-Key"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    id_emp, filial_scope, auth_mode = _resolve_micro_scope(
        id_empresa_q=id_empresa,
        id_filial_q=id_filial,
        authorization=authorization,
        x_ingest_key=x_ingest_key,
        x_internal_key=x_internal_key,
    )

    critical_min_score = int(settings.micro_risk_critical_min_score)
    critical_min_impact = float(settings.micro_risk_critical_min_impact)
    hot_days = 1 if minutes <= 1440 else max(1, int(minutes / 1440))

    with get_conn(role="MASTER", tenant_id=id_emp, branch_id=filial_scope) as conn:
        where_filial_source = "" if filial_scope is None else "AND id_filial = %s"
        source_params = [id_emp] + ([] if filial_scope is None else [filial_scope])
        source_ref = conn.execute(
            f"""
            SELECT MAX(src.data_key) AS latest_data_key
            FROM (
              SELECT MAX(data_key) AS data_key
              FROM dw.fact_comprovante
              WHERE id_empresa = %s
                {where_filial_source}
                AND data_key IS NOT NULL
              UNION ALL
              SELECT MAX(data_key) AS data_key
              FROM dw.fact_venda
              WHERE id_empresa = %s
                {where_filial_source}
                AND data_key IS NOT NULL
            ) src
            """,
            source_params + source_params,
        ).fetchone()
        latest_data_key = int(source_ref["latest_data_key"]) if source_ref and source_ref["latest_data_key"] is not None else None
        latest_data_at = (
            datetime.strptime(str(latest_data_key), "%Y%m%d").replace(tzinfo=timezone.utc)
            if latest_data_key is not None
            else None
        )

        if latest_data_key is None:
            return {
                "ok": True,
                "auth_mode": auth_mode,
                "id_empresa": id_emp,
                "id_filial": filial_scope,
                "minutes": minutes,
                "critical_thresholds": {"min_score": critical_min_score, "min_impact": critical_min_impact},
                "risk_events_computed": 0,
                "critical_groups": 0,
                "insights_created": 0,
                "insights_updated": 0,
                "notifications_upserted": 0,
                "telegram_sent": 0,
                "telegram_suppressed": 0,
                "reference_time": None,
                "message": "Sem dados recentes para processar micro-risco nesta filial.",
                "items": [],
            }

        freshness_limit_key = int((datetime.now(timezone.utc) - timedelta(days=hot_days)).strftime("%Y%m%d"))
        if latest_data_key < freshness_limit_key:
            return {
                "ok": True,
                "auth_mode": auth_mode,
                "id_empresa": id_emp,
                "id_filial": filial_scope,
                "minutes": minutes,
                "critical_thresholds": {"min_score": critical_min_score, "min_impact": critical_min_impact},
                "risk_events_computed": 0,
                "critical_groups": 0,
                "insights_created": 0,
                "insights_updated": 0,
                "notifications_upserted": 0,
                "telegram_sent": 0,
                "telegram_suppressed": 0,
                "reference_time": latest_data_at,
                "message": "Sem movimento recente na janela de micro-risco para esta filial.",
                "items": [],
            }

        risk_rows = conn.execute(
            """
            SELECT etl.compute_risk_events(
              %s::int,
              %s::boolean,
              %s::int,
              %s::timestamptz
            ) AS rows
            """,
            (int(id_emp), False, int(hot_days), latest_data_at),
        ).fetchone()
        computed_rows = int((risk_rows or {}).get("rows") or 0)

        where_filial = "" if filial_scope is None else "AND id_filial = %s"
        params = [id_emp, latest_data_at, minutes, critical_min_score, critical_min_impact] + (
            [] if filial_scope is None else [filial_scope]
        )
        recent = conn.execute(
            f"""
            SELECT
              id_filial,
              event_type,
              COUNT(*)::int AS eventos,
              COALESCE(SUM(impacto_estimado),0)::numeric(18,2) AS impacto_total,
              MAX(score_risco)::int AS max_score,
              MAX(data) AS last_event_at
            FROM dw.fact_risco_evento
            WHERE id_empresa = %s
              AND data >= (%s::timestamptz - make_interval(mins => %s))
              AND data <= %s::timestamptz
              AND (score_risco >= %s OR impacto_estimado >= %s)
              {where_filial}
            GROUP BY id_filial, event_type
            ORDER BY impacto_total DESC, max_score DESC
            """,
            [params[0], params[1], params[2], params[1], params[3], params[4]] + ([] if filial_scope is None else [filial_scope]),
        ).fetchall()

        created = 0
        updated = 0
        notif_upserts = 0
        telegram_sent = 0
        telegram_suppressed = 0
        insight_items = []

        for r in recent:
            id_fil = int(r["id_filial"])
            event_type = str(r["event_type"])
            eventos = int(r["eventos"] or 0)
            impacto = float(r["impacto_total"] or 0)
            max_score = int(r["max_score"] or 0)
            last_event_at = r["last_event_at"]
            dt_ref = (last_event_at.date() if last_event_at else datetime.now(timezone.utc).date())

            insight_type = f"MICRO_CRITICAL_{event_type}"
            title = f"Risco crítico ({event_type}) na filial {id_fil}"
            body = (
                f"{eventos} evento(s) nos últimos {minutes} min, score máx. {max_score}, "
                f"impacto estimado R$ {impacto:,.2f}"
            )
            rec = "Investigar imediatamente funcionário/turno/hora e bloquear reincidência hoje."

            upsert = conn.execute(
                """
                INSERT INTO app.insights_gerados (
                  id_empresa, id_filial, insight_type, severity, dt_ref,
                  impacto_estimado, title, message, recommendation, status, meta
                )
                VALUES (%s,%s,%s,'CRITICAL',%s,%s,%s,%s,%s,'NOVO',%s::jsonb)
                ON CONFLICT ON CONSTRAINT uq_insights_gerados_nk
                DO UPDATE SET
                  impacto_estimado = EXCLUDED.impacto_estimado,
                  title = EXCLUDED.title,
                  message = EXCLUDED.message,
                  recommendation = EXCLUDED.recommendation,
                  status = 'NOVO',
                  meta = EXCLUDED.meta
                RETURNING id, (xmax = 0) AS inserted_flag
                """,
                (
                    id_emp,
                    id_fil,
                    insight_type,
                    dt_ref,
                    impacto,
                    title,
                    body,
                    rec,
                    json.dumps(
                        {
                            "minutes": minutes,
                            "event_type": event_type,
                            "eventos": eventos,
                            "max_score": max_score,
                            "last_event_at": str(last_event_at or ""),
                        },
                        ensure_ascii=False,
                    ),
                ),
            ).fetchone()
            insight_id = int(upsert["id"])
            if upsert["inserted_flag"]:
                created += 1
            else:
                updated += 1

            _upsert_notification_critical(
                id_empresa=id_emp,
                id_filial=id_fil,
                insight_id=insight_id,
                title=title,
                body=body,
                url="/fraud",
            )
            notif_upserts += 1

            tg = send_telegram_alert(
                id_empresa=id_emp,
                payload={
                    "severity": "CRITICAL",
                    "insight_id": insight_id,
                    "insight_type": insight_type,
                    "id_filial": id_fil,
                    "event_time": str(last_event_at or dt_ref),
                    "impacto_estimado": impacto,
                    "title": title,
                    "body": body,
                    "url": "/fraud",
                    "event_type": event_type,
                },
            )
            if tg.get("sent"):
                telegram_sent += 1
            else:
                telegram_suppressed += 1

            insight_items.append(
                {
                    "insight_id": insight_id,
                    "id_filial": id_fil,
                    "event_type": event_type,
                    "eventos": eventos,
                    "impacto_estimado": impacto,
                    "max_score": max_score,
                    "last_event_at": last_event_at,
                    "telegram": tg,
                }
            )

        conn.commit()

    return {
        "ok": True,
        "auth_mode": auth_mode,
        "id_empresa": id_emp,
        "id_filial": filial_scope,
        "minutes": minutes,
        "critical_thresholds": {"min_score": critical_min_score, "min_impact": critical_min_impact},
        "reference_time": latest_data_at,
        "risk_events_computed": computed_rows,
        "critical_groups": len(recent),
        "insights_created": created,
        "insights_updated": updated,
        "notifications_upserted": notif_upserts,
        "telegram_sent": telegram_sent,
        "telegram_suppressed": telegram_suppressed,
        "items": insight_items[:20],
    }
