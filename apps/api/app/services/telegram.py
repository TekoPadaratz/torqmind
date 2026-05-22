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


def raw_comprovante_is_cancelled(row: Dict[str, Any]) -> bool:
    """Resolve raw Xpert comprovante cancellation semantics.

    Only CANCELADO=true is a real cancellation.
    situacao=2 is NOT treated as cancellation (was previously assumed to be devolução).
    """

    cancelado_raw = _get_any(row, ["CANCELADO", "cancelado"])
    if cancelado_raw is not None:
        return _to_bool(cancelado_raw)

    return False


def raw_nfe_is_voided(row: Dict[str, Any]) -> bool:
    """Detect NFE with status=5 (inutilização fiscal)."""
    status = _to_int(_get_any(row, ["STATUS", "status", "STATUSNFE", "STATUS_NFE"]))
    return status == 5


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
          (ut.role IN ('OWNER', 'owner') AND ut.id_empresa = %s)
          OR (ut.role IN ('MASTER', 'platform_master'))
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
    min_severity = str(settings.notify_min_severity or "CRITICAL").upper()
    sev_rank = {"INFO": 1, "WARN": 2, "CRITICAL": 3}
    if not force and sev_rank.get(severity, 0) < sev_rank.get(min_severity, 3):
        logger.info(
            "telegram_suppressed reason=below_min_severity id_empresa=%s severity=%s min=%s",
            id_empresa,
            severity,
            min_severity,
        )
        return {"ok": True, "sent": False, "reason": "below_min_severity", "min_severity": min_severity}

    id_filial = _to_int(payload.get("id_filial"))
    filial_nome = str(payload.get("filial_nome") or "")
    event_type = str(payload.get("event_type") or payload.get("insight_type") or "ALERTA_CRITICO").upper()
    event_time = str(payload.get("event_time") or payload.get("dt_ref") or "")
    event_date = event_time[:10] if len(event_time) >= 10 else time.strftime("%Y-%m-%d")
    insight_id = _to_int(payload.get("insight_id"))
    impacto = float(payload.get("impacto_estimado") or 0.0)
    title = str(payload.get("title") or "Alerta crítico")
    body = str(payload.get("body") or payload.get("message") or "Risco crítico detectado.")
    url = str(payload.get("url") or "/dashboard")

    # Resolve chat_ids: prefer per-company telegram_settings, fall back to per-user recipients
    chat_ids: List[str] = []
    cfg = _get_telegram_setting(id_empresa)
    if cfg and _to_bool(cfg.get("is_enabled")) and str(cfg.get("chat_id") or "").strip():
        chat_ids = [str(cfg["chat_id"]).strip()]
    else:
        # Fall back to users who opted-in via user_notification_settings
        chat_ids = _get_recipients(id_empresa)

    if not chat_ids:
        logger.info("telegram_suppressed reason=no_recipients id_empresa=%s", id_empresa)
        return {"ok": True, "sent": False, "reason": "no_recipients"}

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

    sent_count = 0
    for cid in chat_ids:
        if _send_telegram_sync(chat_id=cid, text=text, retries=3):
            sent_count += 1
    sent = sent_count > 0
    logger.info(
        "telegram_dispatch id_empresa=%s id_filial=%s recipients=%s sent=%s event_type=%s insight_id=%s dedupe_hash=%s",
        id_empresa,
        id_filial,
        len(chat_ids),
        sent,
        event_type,
        insight_id,
        dedupe_hash,
    )
    return {"ok": True, "sent": sent, "recipients": len(chat_ids), "sent_count": sent_count, "dedupe_hash": dedupe_hash}


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


def _resolve_filial_nome(id_empresa: int, id_filial: int) -> str:
    """Resolve branch name from auth.filiais. Returns short name or id_filial."""
    sql = """
      SELECT nome FROM auth.filiais
      WHERE id_empresa = %s AND id_filial = %s
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_empresa, id_filial)).fetchone()
            if row and row.get("nome"):
                return str(row["nome"]).strip()
    except Exception:
        pass
    return str(id_filial)


def _format_datetime(raw: Any) -> str:
    """Format ISO datetime string to 'dd/mm/aaaa HH:MM'."""
    from datetime import datetime

    if not raw:
        return "(sem data)"
    s = str(raw).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d/%m/%Y %H:%M")
        except (ValueError, TypeError):
            continue
    # Fallback: try parsing just the date part
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return s


def _resolve_usuario_nome(id_empresa: int, id_filial: int, id_usuario: int) -> Optional[str]:
    """Resolve operator name from dw.dim_usuario_caixa."""
    sql = """
      SELECT nome FROM dw.dim_usuario_caixa
      WHERE id_empresa = %s AND id_filial = %s AND id_usuario = %s
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_empresa, id_filial, id_usuario)).fetchone()
            if row and row.get("nome"):
                return str(row["nome"]).strip()
    except Exception:
        pass
    return None


def json_dumps(obj: Any) -> str:
    import json

    return json.dumps(obj, ensure_ascii=False)


async def notify_cancelled_comprovantes(id_empresa: int, raw_rows: List[Dict[str, Any]]) -> None:
    """Scan ingested comprovantes; send Telegram alert for each real cancellation (CANCELADO=true)."""

    if not settings.telegram_bot_token:
        return

    recipients = _get_recipients(id_empresa)
    if not recipients:
        return

    tasks: List[asyncio.Task] = []

    for row in raw_rows:
        if not isinstance(row, dict):
            continue

        if not raw_comprovante_is_cancelled(row):
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

        filial_nome = _resolve_filial_nome(id_empresa, id_filial)
        data_raw = _get_any(row, ["DATA", "data"]) or ""
        data_fmt = _format_datetime(data_raw)
        valor_total = _get_any(row, ["VLRTOTAL", "valor_total"]) or 0
        id_usuario = _to_int(_get_any(row, ["ID_USUARIOS", "id_usuario"]))
        id_turno = _to_int(_get_any(row, ["ID_TURNOS", "id_turno"]))
        referencia = _get_any(row, ["REFERENCIA", "referencia"]) or ""

        nome_usuario = _resolve_usuario_nome(id_empresa, id_filial, id_usuario) if id_usuario else None

        try:
            valor_fmt = f"R$ {float(valor_total):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (ValueError, TypeError):
            valor_fmt = f"R$ {valor_total}"

        text = (
            f"🚨 VENDA CANCELADA na filial {filial_nome}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 Comprovante: {id_comprovante}"
            + (f" (Ref: {referencia})" if referencia else "")
            + f"\n"
            f"💰 Valor: {valor_fmt}\n"
            f"📅 Data: {data_fmt}\n"
            f"👤 Operador: {nome_usuario or id_usuario or '?'}\n"
            f"🔄 Turno: {id_turno or '?'}"
        )

        for chat_id in recipients:
            tasks.append(asyncio.create_task(_send_telegram(chat_id, text)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def notify_voided_nfes(id_empresa: int, raw_rows: List[Dict[str, Any]]) -> None:
    """Scan ingested NFEs; send Telegram alert for each voided note (STATUS=5, inutilização)."""

    if not settings.telegram_bot_token:
        return

    recipients = _get_recipients(id_empresa)
    if not recipients:
        return

    tasks: List[asyncio.Task] = []

    for row in raw_rows:
        if not isinstance(row, dict):
            continue

        if not raw_nfe_is_voided(row):
            continue

        id_filial = _to_int(_get_any(row, ["ID_FILIAL", "id_filial"]))
        id_db = _to_int(_get_any(row, ["ID_DB", "id_db"]))
        id_nfe = _to_int(_get_any(row, ["ID_NFE", "id_nfe", "ID_NOTASFISCAIS", "id_notasfiscais"]))

        if id_filial is None or id_nfe is None:
            continue

        # Dedupe: use dispatch log to avoid sending same NFE twice
        dedupe_raw = f"{id_empresa}|{id_filial}|NFE_INUTILIZADA|{id_nfe}"
        dedupe_hash = hashlib.sha256(dedupe_raw.encode("utf-8")).hexdigest()
        inserted = _register_dispatch_once(
            id_empresa=id_empresa,
            id_filial=id_filial,
            event_type="NFE_INUTILIZADA",
            event_date=time.strftime("%Y-%m-%d"),
            insight_id=None,
            dedupe_hash=dedupe_hash,
            payload={"id_nfe": id_nfe, "id_filial": id_filial, "id_db": id_db},
        )
        if not inserted:
            continue

        filial_nome = _resolve_filial_nome(id_empresa, id_filial)
        numero_nfe = _get_any(row, ["NRONF", "NUMERO", "NUMERONFE", "NUMERO_NFE", "numero_nfe", "numero"]) or "?"
        serie = _get_any(row, ["SERIE", "serie"]) or "?"
        valor = _get_any(row, ["VALOR", "VALORNFE", "VALOR_NFE", "VLRTOTAL", "valor_nfe"]) or 0
        data_inut = _format_datetime(_get_any(row, ["DATAINUTILIZACAO", "DATA_INUTILIZACAO", "data_inutilizacao"]) or "")
        data_emissao = _format_datetime(_get_any(row, ["DATA", "DATAEMISSAO", "DATA_EMISSAO", "data_emissao"]) or "")
        id_usuario = _to_int(_get_any(row, ["ID_USUARIOS", "id_usuario", "ID_USUARIO"]))

        nome_usuario = _resolve_usuario_nome(id_empresa, id_filial, id_usuario) if id_usuario else None

        try:
            valor_fmt = f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (ValueError, TypeError):
            valor_fmt = f"R$ {valor}"

        text = (
            f"📋 NOTA INUTILIZADA na filial {filial_nome}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 NFe Nº: {numero_nfe} | Série: {serie}\n"
            f"💰 Valor: {valor_fmt}\n"
            f"📅 Emissão: {data_emissao}\n"
            f"📅 Inutilização: {data_inut}\n"
            f"👤 Operador: {nome_usuario or id_usuario or '?'}"
        )

        for chat_id in recipients:
            tasks.append(asyncio.create_task(_send_telegram(chat_id, text)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# User-facing config helpers
# ---------------------------------------------------------------------------

def get_telegram_config(user_id: str) -> Dict[str, Any]:
    """Return the current user's Telegram notification settings."""
    sql = """
      SELECT telegram_chat_id, telegram_username, telegram_enabled
      FROM app.user_notification_settings
      WHERE user_id = %s::uuid
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (str(user_id),)).fetchone()
        if row:
            return {
                "telegram_chat_id": row["telegram_chat_id"],
                "telegram_username": row["telegram_username"],
                "telegram_enabled": bool(row["telegram_enabled"]),
                "configured": bool(row["telegram_chat_id"] and row["telegram_enabled"]),
                "bot_token_set": bool(settings.telegram_bot_token),
            }
        return {
            "telegram_chat_id": None,
            "telegram_username": None,
            "telegram_enabled": False,
            "configured": False,
            "bot_token_set": bool(settings.telegram_bot_token),
        }


def save_telegram_config(
    user_id: str,
    *,
    telegram_chat_id: Optional[str],
    telegram_username: Optional[str],
    telegram_enabled: bool,
) -> Dict[str, Any]:
    """Upsert user Telegram notification settings."""
    chat_id = str(telegram_chat_id or "").strip() or None
    username = str(telegram_username or "").strip() or None
    sql = """
      INSERT INTO app.user_notification_settings
        (user_id, telegram_chat_id, telegram_username, telegram_enabled)
      VALUES (%s::uuid, %s, %s, %s)
      ON CONFLICT (user_id)
      DO UPDATE SET
        telegram_chat_id = EXCLUDED.telegram_chat_id,
        telegram_username = EXCLUDED.telegram_username,
        telegram_enabled = EXCLUDED.telegram_enabled
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(sql, (str(user_id), chat_id, username, telegram_enabled))
        conn.commit()
    return {
        "ok": True,
        "telegram_chat_id": chat_id,
        "telegram_username": username,
        "telegram_enabled": telegram_enabled,
        "configured": bool(chat_id and telegram_enabled),
    }


# ---------------------------------------------------------------------------
# Dispatch queued notifications
# ---------------------------------------------------------------------------

def dispatch_pending_notifications(
    id_empresa: int,
    *,
    limit: int = 20,
    severity: str = "CRITICAL",
    force: bool = False,
) -> Dict[str, Any]:
    """Send CRITICAL app.notifications not yet dispatched via Telegram.

    Tracks dispatches via telegram_dispatch_log using notification_id as key.
    """
    if not settings.telegram_bot_token:
        return {"ok": False, "reason": "bot_token_not_set", "sent": 0, "skipped": 0, "total": 0}

    sql = """
      SELECT n.id, n.id_filial, n.severity, n.title, n.body, n.url, n.created_at
      FROM app.notifications n
      WHERE n.id_empresa = %s
        AND n.severity = %s
        AND NOT EXISTS (
          SELECT 1 FROM app.telegram_dispatch_log dl
          WHERE dl.id_empresa = %s
            AND dl.event_type = 'NOTIFICATION_DISPATCH'
            AND dl.insight_id = n.id
        )
      ORDER BY n.created_at DESC
      LIMIT %s
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        rows = [dict(r) for r in conn.execute(sql, (id_empresa, severity, id_empresa, limit)).fetchall()]

    if not rows:
        return {"ok": True, "reason": "no_pending", "sent": 0, "skipped": 0, "total": 0}

    sent = 0
    skipped = 0
    for row in rows:
        created = row.get("created_at")
        event_time = created.isoformat() if hasattr(created, "isoformat") else str(created or "")
        payload = {
            "severity": row.get("severity", "CRITICAL"),
            "insight_id": int(row["id"]),
            "event_type": "NOTIFICATION_DISPATCH",
            "id_filial": row.get("id_filial"),
            "event_time": event_time,
            "impacto_estimado": 0.0,
            "title": row.get("title") or "Alerta crítico",
            "body": row.get("body") or "",
            "url": row.get("url") or "/dashboard",
        }
        result = send_telegram_alert(id_empresa=id_empresa, payload=payload, force=force)
        if result.get("sent"):
            sent += 1
        else:
            skipped += 1

    return {
        "ok": True,
        "sent": sent,
        "skipped": skipped,
        "total": len(rows),
    }
