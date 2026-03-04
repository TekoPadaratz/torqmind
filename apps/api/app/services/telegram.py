from __future__ import annotations

"""Telegram notification helper.

PT-BR: O Anti-fraude do TorqMind envia alerta de cancelamento em tempo real.
EN   : TorqMind anti-fraud sends real-time cancellation alerts.

This module is intentionally defensive:
- If Telegram is not configured, ingestion still works.
- If Telegram API errors, ingestion still works.
"""

import asyncio
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

import httpx

from app.config import settings
from app.db import get_conn

logger = logging.getLogger(__name__)


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(str(x).strip())
        except Exception:
            return None


def _to_bool(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def _get_any(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


async def _send_telegram(chat_id: str, text: str) -> None:
    token = settings.telegram_bot_token
    if not token:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    async with httpx.AsyncClient(timeout=10.0) as client:
        await client.post(url, json={"chat_id": chat_id, "text": text})


def _send_telegram_sync(chat_id: str, text: str, retries: int = 3) -> bool:
    token = settings.telegram_bot_token
    if not token:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    sleep_s = 1.0
    for _ in range(max(1, retries)):
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, json={"chat_id": chat_id, "text": text})
                if resp.status_code < 300:
                    return True
        except Exception:
            pass
        time.sleep(sleep_s)
        sleep_s = min(8.0, sleep_s * 2.0)
    return False


def _get_recipients(id_empresa: int) -> List[str]:
    """Return telegram chat_ids for owners/master that opted in."""

    sql = """
      SELECT DISTINCT s.telegram_chat_id
      FROM auth.user_tenants ut
      JOIN app.user_notification_settings s
        ON s.user_id = ut.user_id
      WHERE s.telegram_enabled = true
        AND s.telegram_chat_id IS NOT NULL
        AND (
          (ut.role = 'OWNER' AND ut.id_empresa = %s)
          OR (ut.role = 'MASTER')
        )
    """

    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(sql, (id_empresa,)).fetchall()
        return [r["telegram_chat_id"] for r in rows if r.get("telegram_chat_id")]


def _get_telegram_setting(id_empresa: int) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT id_empresa, chat_id, is_enabled
      FROM app.telegram_settings
      WHERE id_empresa = %s
      LIMIT 1
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        row = conn.execute(sql, (id_empresa,)).fetchone()
        return row


def _register_dispatch_once(
    id_empresa: int,
    id_filial: Optional[int],
    event_type: str,
    event_date: str,
    insight_id: Optional[int],
    dedupe_hash: str,
    payload: Dict[str, Any],
) -> bool:
    sql = """
      INSERT INTO app.telegram_dispatch_log (
        id_empresa, id_filial, event_type, event_date, insight_id, dedupe_hash, payload
      )
      VALUES (%s,%s,%s,%s::date,%s,%s,%s::jsonb)
      ON CONFLICT (id_empresa, dedupe_hash)
      DO NOTHING
      RETURNING id
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        row = conn.execute(
            sql,
            (
                id_empresa,
                id_filial,
                event_type,
                event_date,
                insight_id,
                dedupe_hash,
                json_dumps(payload),
            ),
        ).fetchone()
        conn.commit()
        return bool(row)


def send_telegram_alert(id_empresa: int, payload: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    """Send CRITICAL alerts to company owner chat_id with dedupe by day.

    Dedupe key: empresa + filial + (insight_id|event_type) + event_date
    """

    severity = str(payload.get("severity") or "").upper()
    if severity != "CRITICAL" and not force:
        logger.info("telegram_suppressed reason=not_critical id_empresa=%s severity=%s", id_empresa, severity)
        return {"ok": True, "sent": False, "reason": "not_critical"}

    cfg = _get_telegram_setting(id_empresa)
    if not cfg or not _to_bool(cfg.get("is_enabled")):
        logger.info("telegram_suppressed reason=telegram_disabled id_empresa=%s", id_empresa)
        return {"ok": True, "sent": False, "reason": "telegram_disabled"}

    chat_id = str(cfg.get("chat_id") or "").strip()
    if not chat_id:
        logger.info("telegram_suppressed reason=missing_chat_id id_empresa=%s", id_empresa)
        return {"ok": True, "sent": False, "reason": "missing_chat_id"}

    id_filial = _to_int(payload.get("id_filial"))
    filial_nome = str(payload.get("filial_nome") or "")
    event_type = str(payload.get("event_type") or payload.get("insight_type") or "ALERTA_CRITICO").upper()
    event_time = str(payload.get("event_time") or payload.get("dt_ref") or "")
    event_date = event_time[:10] if len(event_time) >= 10 else time.strftime("%Y-%m-%d")
    insight_id = _to_int(payload.get("insight_id"))
    impacto = float(payload.get("impacto_estimado") or 0.0)
    title = str(payload.get("title") or "Alerta critico")
    body = str(payload.get("body") or payload.get("message") or "Risco critico detectado.")
    url = str(payload.get("url") or "/dashboard")

    dedupe_raw = f"{id_empresa}|{id_filial}|{insight_id or event_type}|{event_date}"
    dedupe_hash = hashlib.sha256(dedupe_raw.encode("utf-8")).hexdigest()
    if not force:
        inserted = _register_dispatch_once(
            id_empresa=id_empresa,
            id_filial=id_filial,
            event_type=event_type,
            event_date=event_date,
            insight_id=insight_id,
            dedupe_hash=dedupe_hash,
            payload=payload,
        )
        if not inserted:
            logger.info(
                "telegram_suppressed reason=duplicate_daily id_empresa=%s id_filial=%s insight_id=%s event_type=%s event_date=%s",
                id_empresa,
                id_filial,
                insight_id,
                event_type,
                event_date,
            )
            return {"ok": True, "sent": False, "reason": "duplicate_daily"}

    filial_label = f"{id_filial} {filial_nome}".strip() if id_filial is not None else "Todas"
    text = (
        "🚨 TORQMIND CRITICAL\n"
        f"Empresa: {id_empresa}\n"
        f"Filial: {filial_label}\n"
        f"Horario: {event_time or '-'}\n"
        f"Tipo: {event_type}\n"
        f"Impacto estimado: R$ {impacto:,.2f}\n"
        f"Titulo: {title}\n"
        f"Detalhe: {body}\n"
        f"Drill-down: {url}"
    )

    sent = _send_telegram_sync(chat_id=chat_id, text=text, retries=3)
    logger.info(
        "telegram_dispatch id_empresa=%s id_filial=%s chat_id=%s sent=%s event_type=%s insight_id=%s dedupe_hash=%s",
        id_empresa,
        id_filial,
        chat_id,
        sent,
        event_type,
        insight_id,
        dedupe_hash,
    )
    return {"ok": True, "sent": sent, "chat_id": chat_id, "dedupe_hash": dedupe_hash}


def _insert_alert_if_new(
    id_empresa: int,
    id_filial: int,
    id_db: int,
    id_comprovante: int,
    payload: Dict[str, Any],
) -> bool:
    """Insert into app.alert_comprovante_cancelado once (idempotent). Returns True if inserted."""

    data = _get_any(payload, ["DATA", "data"])
    valor_total = _get_any(payload, ["VLRTOTAL", "valor_total"])
    id_usuario = _get_any(payload, ["ID_USUARIOS", "id_usuario"])
    id_turno = _get_any(payload, ["ID_TURNOS", "id_turno"])

    sql = """
      INSERT INTO app.alert_comprovante_cancelado (
        id_empresa, id_filial, id_db, id_comprovante,
        comprovante_data, valor_total, id_usuario, id_turno, payload
      )
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
      ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
      DO NOTHING
      RETURNING id
    """

    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        row = conn.execute(
            sql,
            (
                id_empresa,
                id_filial,
                id_db,
                id_comprovante,
                data,
                valor_total,
                id_usuario,
                id_turno,
                json_dumps(payload),
            ),
        ).fetchone()
        return bool(row)


def json_dumps(obj: Any) -> str:
    import json

    return json.dumps(obj, ensure_ascii=False)


async def notify_cancelled_comprovantes(id_empresa: int, raw_rows: List[Dict[str, Any]]) -> None:
    """Scan ingested comprovantes rows; if CANCELADO=true, persist alert and notify via Telegram."""

    if not settings.telegram_bot_token:
        # Not configured; still record alerts (optional) but no external call.
        pass

    recipients = _get_recipients(id_empresa)
    if not recipients:
        # No one opted-in.
        return

    tasks: List[asyncio.Task] = []

    for row in raw_rows:
        if not isinstance(row, dict):
            continue

        cancelado = _to_bool(_get_any(row, ["CANCELADO", "cancelado"]))
        if not cancelado:
            continue

        id_filial = _to_int(_get_any(row, ["ID_FILIAL", "id_filial"]))
        id_db = _to_int(_get_any(row, ["ID_DB", "id_db"]))
        id_comprovante = _to_int(_get_any(row, ["ID_COMPROVANTE", "id_comprovante"]))

        if id_filial is None or id_db is None or id_comprovante is None:
            continue

        inserted = _insert_alert_if_new(
            id_empresa=id_empresa,
            id_filial=id_filial,
            id_db=id_db,
            id_comprovante=id_comprovante,
            payload=row,
        )
        if not inserted:
            continue

        data = _get_any(row, ["DATA", "data"]) or "(sem data)"
        valor_total = _get_any(row, ["VLRTOTAL", "valor_total"]) or 0
        id_usuario = _get_any(row, ["ID_USUARIOS", "id_usuario"]) or "?"
        id_turno = _get_any(row, ["ID_TURNOS", "id_turno"]) or "?"

        text = (
            "🚨 CANCELAMENTO DETECTADO\n"
            f"Empresa: {id_empresa}\n"
            f"Filial: {id_filial} | DB: {id_db}\n"
            f"Comprovante: {id_comprovante}\n"
            f"Data: {data}\n"
            f"Valor: R$ {valor_total}\n"
            f"Usuário: {id_usuario} | Turno: {id_turno}"
        )

        for chat_id in recipients:
            tasks.append(asyncio.create_task(_send_telegram(chat_id, text)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
