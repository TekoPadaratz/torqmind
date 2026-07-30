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

# Catálogo tipado dos alertas Telegram (settings + dispatcher).
EVENT_VENDA_CANCELADA = "VENDA_CANCELADA"
EVENT_NFE_INUTILIZADA = "NFE_INUTILIZADA"
EVENT_CASH_OPEN_OVER_24H = "CASH_OPEN_OVER_24H"
EVENT_PRECO_FIXO_BOMBA = "PRECO_FIXO_BOMBA_DESATUALIZADO"
TELEGRAM_ALERT_CATALOG: List[Dict[str, str]] = [
    {"key": EVENT_VENDA_CANCELADA, "label": "Venda cancelada"},
    {"key": EVENT_NFE_INUTILIZADA, "label": "NFe inutilizada"},
    {"key": EVENT_CASH_OPEN_OVER_24H, "label": "Caixa aberto > 24h"},
    {"key": EVENT_PRECO_FIXO_BOMBA, "label": "Preço bomba × preço fixo"},
]
TELEGRAM_ALERT_KEYS = frozenset(item["key"] for item in TELEGRAM_ALERT_CATALOG)
DELTA_PRECO_FIXO = 0.005
_COMPANY_ALERT_FLAG = {
    EVENT_VENDA_CANCELADA: "alert_venda_cancelada",
    EVENT_NFE_INUTILIZADA: "alert_nfe_inutilizada",
    EVENT_CASH_OPEN_OVER_24H: "alert_cash_open_over_24h",
    EVENT_PRECO_FIXO_BOMBA: "alert_preco_fixo_bomba",
}


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

    Only CANCELADO=true is a real cancellation of a SALE.
    situacao=2 is NOT treated as cancellation (was previously assumed to be devolução).
    CFOP < 5000 means entry/purchase note — never alert as cancelled sale.
    """

    cancelado_raw = _get_any(row, ["CANCELADO", "cancelado"])
    if cancelado_raw is None or not _to_bool(cancelado_raw):
        return False

    # Filter out entry notes (CFOP < 5000 = compra/entrada/transferência interna)
    cfop_raw = _get_any(row, ["CFOP", "cfop"])
    if cfop_raw is not None:
        try:
            cfop_num = int(str(cfop_raw).replace(".", "").replace(",", "").strip()[:4])
            if cfop_num < 5000:
                return False
        except (ValueError, TypeError):
            pass

    return True


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


def _get_recipients(id_empresa: int, event_type: Optional[str] = None) -> List[str]:
    """Return telegram chat_ids for owners/master that opted in.

    Se existirem assinaturas tipadas (notification_subscriptions) para o catálogo,
    filtra por event_type + is_enabled. Sem assinaturas tipadas → comportamento legado.
    """
    event_key = str(event_type or "").upper().strip() or None
    sql = """
      SELECT DISTINCT s.telegram_chat_id, s.user_id
      FROM auth.user_tenants ut
      JOIN app.user_notification_settings s
        ON s.user_id = ut.user_id
      WHERE s.telegram_enabled = true
        AND s.telegram_chat_id IS NOT NULL
        AND btrim(s.telegram_chat_id) <> ''
        AND (
          (ut.role IN ('OWNER', 'owner', 'tenant_admin') AND ut.id_empresa = %s)
          OR (ut.role IN ('MASTER', 'platform_master'))
        )
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        rows = [dict(r) for r in conn.execute(sql, (id_empresa,)).fetchall()]
        if not event_key or event_key not in TELEGRAM_ALERT_KEYS:
            return [r["telegram_chat_id"] for r in rows if r.get("telegram_chat_id")]

        typed = conn.execute(
            """
            SELECT 1
            FROM app.notification_subscriptions ns
            INNER JOIN auth.user_tenants ut ON ut.user_id = ns.user_id
            WHERE ns.channel = 'telegram'
              AND ns.event_type = ANY(%s)
              AND (ns.tenant_id IS NULL OR ns.tenant_id = %s OR ut.id_empresa = %s)
            LIMIT 1
            """,
            (list(TELEGRAM_ALERT_KEYS), id_empresa, id_empresa),
        ).fetchone()
        if not typed:
            return [r["telegram_chat_id"] for r in rows if r.get("telegram_chat_id")]

        out: List[str] = []
        for r in rows:
            uid = r.get("user_id")
            chat = r.get("telegram_chat_id")
            if not uid or not chat:
                continue
            sub = conn.execute(
                """
                SELECT is_enabled
                FROM app.notification_subscriptions
                WHERE user_id = %s::uuid
                  AND channel = 'telegram'
                  AND event_type = %s
                  AND (tenant_id IS NULL OR tenant_id = %s)
                ORDER BY CASE WHEN tenant_id = %s THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (str(uid), event_key, id_empresa, id_empresa),
            ).fetchone()
            # Sem assinatura tipada para o user → default ON; is_enabled=false → fora
            if sub is None or bool(sub.get("is_enabled")):
                out.append(chat)
        return out


def _get_telegram_setting(id_empresa: int) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT
        id_empresa, chat_id, is_enabled,
        COALESCE(preco_fixo_alerta_base, 'venda') AS preco_fixo_alerta_base,
        COALESCE(alert_venda_cancelada, true) AS alert_venda_cancelada,
        COALESCE(alert_nfe_inutilizada, true) AS alert_nfe_inutilizada,
        COALESCE(alert_cash_open_over_24h, true) AS alert_cash_open_over_24h,
        COALESCE(alert_preco_fixo_bomba, true) AS alert_preco_fixo_bomba
      FROM app.telegram_settings
      WHERE id_empresa = %s
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_empresa,)).fetchone()
            return dict(row) if row else None
    except Exception:
        # Migration ainda não aplicada: fallback mínimo
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(
                "SELECT id_empresa, chat_id, is_enabled FROM app.telegram_settings WHERE id_empresa = %s LIMIT 1",
                (id_empresa,),
            ).fetchone()
            return dict(row) if row else None


def get_company_alert_prefs(id_empresa: int) -> Dict[str, Any]:
    cfg = _get_telegram_setting(id_empresa) or {}
    return {
        "preco_fixo_alerta_base": str(cfg.get("preco_fixo_alerta_base") or "venda"),
        "alert_venda_cancelada": bool(cfg.get("alert_venda_cancelada", True)),
        "alert_nfe_inutilizada": bool(cfg.get("alert_nfe_inutilizada", True)),
        "alert_cash_open_over_24h": bool(cfg.get("alert_cash_open_over_24h", True)),
        "alert_preco_fixo_bomba": bool(cfg.get("alert_preco_fixo_bomba", True)),
    }


def company_event_enabled(id_empresa: int, event_type: str) -> bool:
    key = str(event_type or "").upper()
    flag = _COMPANY_ALERT_FLAG.get(key)
    if not flag:
        return True
    prefs = get_company_alert_prefs(id_empresa)
    return bool(prefs.get(flag, True))


def resolve_telegram_recipients(id_empresa: int, event_type: str) -> List[str]:
    """Destinatários finais: canal empresa (se ligado) ou usuários filtrados por assinatura."""
    if not company_event_enabled(id_empresa, event_type):
        return []
    chat_ids: List[str] = []
    cfg = _get_telegram_setting(id_empresa)
    if cfg and _to_bool(cfg.get("is_enabled")) and str(cfg.get("chat_id") or "").strip():
        chat_ids = [str(cfg["chat_id"]).strip()]
    else:
        chat_ids = _get_recipients(id_empresa, event_type)
    return chat_ids


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
    chat_ids = resolve_telegram_recipients(id_empresa, event_type)
    if not chat_ids:
        logger.info("telegram_suppressed reason=no_recipients id_empresa=%s event_type=%s", id_empresa, event_type)
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

    _insert_in_app_notification(
        id_empresa=id_empresa,
        id_filial=id_filial,
        severity=severity or "CRITICAL",
        title=title,
        body=body,
        url=url,
        dedupe_key=f"tg|{id_empresa}|{id_filial}|{insight_id or event_type}|{event_date}",
    )

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


def _insert_in_app_notification(
    *,
    id_empresa: int,
    id_filial: Optional[int],
    severity: str,
    title: str,
    body: str,
    url: str,
    dedupe_key: Optional[str] = None,
) -> bool:
    """Espelha alerta Telegram na inbox in-app (app.notifications) até o usuário marcar lido.

    Sem migration: dedupe por título+corpo recentes (7d) ou insight_id sintético via hash.
    """
    sev = str(severity or "CRITICAL").upper()
    if sev not in {"INFO", "WARN", "CRITICAL"}:
        sev = "CRITICAL"
    title_s = str(title or "Alerta").strip()[:240]
    body_s = str(body or "").strip()[:4000]
    url_s = str(url or "/dashboard").strip()[:500]
    insight_id = None
    if dedupe_key:
        # insight_id positivo grande e estável a partir do dedupe (evita colisão com insights reais pequenos)
        digest = hashlib.sha1(dedupe_key.encode("utf-8")).hexdigest()[:15]
        insight_id = int(digest, 16) % 9000000000000000 + 1000000000000000

    sql = """
      INSERT INTO app.notifications (
        id_empresa, id_filial, insight_id, severity, title, body, url
      )
      VALUES (%s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (id_empresa, id_filial, insight_id) WHERE insight_id IS NOT NULL
      DO UPDATE SET
        severity = EXCLUDED.severity,
        title = EXCLUDED.title,
        body = EXCLUDED.body,
        url = EXCLUDED.url,
        created_at = now(),
        read_at = NULL
      RETURNING id
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            if insight_id is None:
                # Sem chave: evita flood de duplicatas idênticas na mesma hora
                exists = conn.execute(
                    """
                    SELECT id FROM app.notifications
                    WHERE id_empresa = %s
                      AND COALESCE(id_filial, -1) = COALESCE(%s::int, -1)
                      AND title = %s
                      AND body = %s
                      AND created_at >= now() - interval '1 day'
                      AND read_at IS NULL
                    LIMIT 1
                    """,
                    (id_empresa, id_filial, title_s, body_s),
                ).fetchone()
                if exists:
                    return False
                row = conn.execute(
                    """
                    INSERT INTO app.notifications (
                      id_empresa, id_filial, insight_id, severity, title, body, url
                    )
                    VALUES (%s, %s, NULL, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (id_empresa, id_filial, sev, title_s, body_s, url_s),
                ).fetchone()
            else:
                row = conn.execute(
                    sql,
                    (id_empresa, id_filial, insight_id, sev, title_s, body_s, url_s),
                ).fetchone()
            conn.commit()
            return bool(row)
    except Exception:
        logger.exception(
            "in_app_notification_failed id_empresa=%s id_filial=%s title=%s",
            id_empresa,
            id_filial,
            title_s,
        )
        return False


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
    """Resolve branch label from auth.filiais.

    Prefere o apelido curto (ex.: "VR 01") quando definido na Plataforma;
    senão usa o nome completo; por último cai no id_filial.
    """
    sql = """
      SELECT COALESCE(NULLIF(btrim(apelido), ''), nome) AS nome
      FROM auth.filiais
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


def _resolve_numero_nfe(
    id_empresa: int, id_filial: int, id_db: int, id_comprovante: int
) -> Optional[str]:
    """Número da Nota Fiscal (NF/NFC-e) do comprovante, se emitida.

    Cancelamentos normalmente não têm nota emitida -> retorna None e o alerta cai
    honestamente no número do comprovante (nunca inventamos uma NF).
    """
    sql = """
      SELECT numero_nfe_shadow
      FROM stg.nfe
      WHERE id_empresa = %s AND id_filial = %s AND id_db = %s AND id_comprovante = %s
        AND numero_nfe_shadow IS NOT NULL
        AND btrim(numero_nfe_shadow) <> ''
        AND numero_nfe_shadow <> '0'
      ORDER BY status_shadow DESC, id_nfe DESC
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_empresa, id_filial, id_db, id_comprovante)).fetchone()
            if row and row.get("numero_nfe_shadow"):
                return str(row["numero_nfe_shadow"]).strip()
    except Exception:
        pass
    return None


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
    """Scan ingested comprovantes; alert Telegram + inbox in-app for each real cancellation."""

    recipients = (
        resolve_telegram_recipients(id_empresa, EVENT_VENDA_CANCELADA)
        if settings.telegram_bot_token
        else []
    )

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
        referencia = _get_any(row, ["REFERENCIA", "referencia"]) or ""

        nome_usuario = _resolve_usuario_nome(id_empresa, id_filial, id_usuario) if id_usuario else None

        try:
            valor_fmt = f"R$ {float(valor_total):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (ValueError, TypeError):
            valor_fmt = f"R$ {valor_total}"

        numero_nfe = _resolve_numero_nfe(id_empresa, id_filial, id_db, id_comprovante)
        if numero_nfe:
            documento_line = f"📄 Nota fiscal: {numero_nfe}"
        else:
            documento_line = f"📄 Comprovante: {id_comprovante}"

        text = (
            f"🚨 VENDA CANCELADA na filial {filial_nome}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            + documento_line
            + (f" (Ref: {referencia})" if referencia else "")
            + f"\n"
            f"💰 Valor: {valor_fmt}\n"
            f"📅 Data: {data_fmt}\n"
            f"👤 Operador: {nome_usuario or id_usuario or '?'}"
        )

        _insert_in_app_notification(
            id_empresa=id_empresa,
            id_filial=id_filial,
            severity="CRITICAL",
            title=f"Venda cancelada — {filial_nome}",
            body=text,
            url="/fraud",
            dedupe_key=f"venda_cancelada|{id_empresa}|{id_filial}|{id_db}|{id_comprovante}",
        )

        for chat_id in recipients:
            tasks.append(asyncio.create_task(_send_telegram(chat_id, text)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def notify_voided_nfes(id_empresa: int, raw_rows: List[Dict[str, Any]]) -> None:
    """Scan ingested NFEs; alert Telegram + inbox in-app for each voided note (STATUS=5)."""

    recipients = (
        resolve_telegram_recipients(id_empresa, EVENT_NFE_INUTILIZADA)
        if settings.telegram_bot_token
        else []
    )

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

        # Extract SERIE from CHAVEACESSO (positions 22-24) when not available directly
        serie = _get_any(row, ["SERIE", "serie"])
        if not serie:
            chave = str(_get_any(row, ["CHAVEACESSO", "chaveacesso", "CHAVE_ACESSO"]) or "")
            if len(chave) >= 25:
                try:
                    serie = str(int(chave[22:25]))
                except (ValueError, TypeError):
                    serie = None
        serie = serie or "?"

        # Lookup VALOR from comprovante when not available directly
        valor = _get_any(row, ["VALOR", "VALORNFE", "VALOR_NFE", "VLRTOTAL", "valor_nfe"])
        id_comprovante = _to_int(_get_any(row, ["ID_COMPROVANTE", "id_comprovante"]))
        if not valor and id_comprovante and id_filial:
            try:
                with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
                    comp_row = conn.execute(
                        "SELECT (payload->>'VLRTOTAL')::numeric AS valor FROM stg.comprovantes WHERE id_empresa=%s AND id_filial=%s AND id_comprovante=%s LIMIT 1",
                        (id_empresa, id_filial, id_comprovante),
                    ).fetchone()
                    if comp_row and comp_row.get("valor"):
                        valor = comp_row["valor"]
            except Exception:
                pass
        valor = valor or 0

        # Use DATA as inutilização date (Xpert doesn't have separate DATAINUTILIZACAO)
        data_inut = _format_datetime(
            _get_any(row, ["DATAINUTILIZACAO", "DATA_INUTILIZACAO", "data_inutilizacao", "DATA", "data"]) or ""
        )
        data_emissao = _format_datetime(_get_any(row, ["DATAEMISSAO", "DATA_EMISSAO", "data_emissao", "DATA", "data"]) or "")
        id_usuario = _to_int(_get_any(row, ["ID_USUARIOS", "id_usuario", "ID_USUARIO"]))

        # NFE rows often lack ID_USUARIOS; resolve from parent comprovante
        if not id_usuario and id_comprovante and id_filial:
            try:
                with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
                    user_row = conn.execute(
                        "SELECT (payload->>'ID_USUARIOS')::int AS id_usuario "
                        "FROM stg.comprovantes "
                        "WHERE id_empresa=%s AND id_filial=%s AND id_comprovante=%s LIMIT 1",
                        (id_empresa, id_filial, id_comprovante),
                    ).fetchone()
                    if user_row and user_row.get("id_usuario"):
                        id_usuario = int(user_row["id_usuario"])
            except Exception:
                pass

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

        _insert_in_app_notification(
            id_empresa=id_empresa,
            id_filial=id_filial,
            severity="CRITICAL",
            title=f"Nota inutilizada — {filial_nome}",
            body=text,
            url="/cash",
            dedupe_key=f"nfe_inutilizada|{id_empresa}|{id_filial}|{id_db or 0}|{id_nfe}",
        )

        for chat_id in recipients:
            tasks.append(asyncio.create_task(_send_telegram(chat_id, text)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# User-facing config helpers
# ---------------------------------------------------------------------------

def _list_user_alert_subscriptions(user_id: str, id_empresa: Optional[int] = None) -> Dict[str, bool]:
    defaults = {k: True for k in TELEGRAM_ALERT_KEYS}
    sql = """
      SELECT event_type, is_enabled, tenant_id
      FROM app.notification_subscriptions
      WHERE user_id = %s::uuid
        AND channel = 'telegram'
        AND event_type = ANY(%s)
    """
    params: list[Any] = [str(user_id), list(TELEGRAM_ALERT_KEYS)]
    if id_empresa is not None:
        sql += " AND (tenant_id IS NULL OR tenant_id = %s)"
        params.append(int(id_empresa))
    try:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
    except Exception:
        return defaults
    # Prefer tenant-scoped row over NULL tenant
    ranked: Dict[str, tuple[int, bool]] = {}
    for row in rows:
        key = str(row.get("event_type") or "").upper()
        if key not in TELEGRAM_ALERT_KEYS:
            continue
        rank = 0 if (id_empresa is not None and row.get("tenant_id") == id_empresa) else 1
        prev = ranked.get(key)
        if prev is None or rank < prev[0]:
            ranked[key] = (rank, bool(row.get("is_enabled")))
    for key, (_, enabled) in ranked.items():
        defaults[key] = enabled
    return defaults


def _upsert_user_alert_subscriptions(
    user_id: str,
    subscriptions: Dict[str, bool],
    *,
    id_empresa: Optional[int] = None,
) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        for key, enabled in subscriptions.items():
            event_key = str(key or "").upper()
            if event_key not in TELEGRAM_ALERT_KEYS:
                continue
            existing = conn.execute(
                """
                SELECT id FROM app.notification_subscriptions
                WHERE user_id = %s::uuid
                  AND channel = 'telegram'
                  AND event_type = %s
                  AND COALESCE(tenant_id, -1) = COALESCE(%s::int, -1)
                  AND branch_id IS NULL
                LIMIT 1
                """,
                (str(user_id), event_key, id_empresa),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE app.notification_subscriptions
                    SET is_enabled = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (bool(enabled), int(existing["id"])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO app.notification_subscriptions (
                      user_id, tenant_id, branch_id, event_type, channel, severity_min, is_enabled
                    )
                    VALUES (%s::uuid, %s, NULL, %s, 'telegram', 'CRITICAL', %s)
                    """,
                    (str(user_id), id_empresa, event_key, bool(enabled)),
                )
        conn.commit()


def save_company_alert_prefs(
    id_empresa: int,
    *,
    preco_fixo_alerta_base: Optional[str] = None,
    alert_flags: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    base = str(preco_fixo_alerta_base or "venda").strip().lower()
    if base not in {"venda", "custo"}:
        base = "venda"
    flags = alert_flags or {}
    sql = """
      INSERT INTO app.telegram_settings (
        id_empresa, chat_id, is_enabled,
        preco_fixo_alerta_base,
        alert_venda_cancelada, alert_nfe_inutilizada,
        alert_cash_open_over_24h, alert_preco_fixo_bomba
      )
      VALUES (
        %s, NULL, false, %s,
        COALESCE(%s, true), COALESCE(%s, true),
        COALESCE(%s, true), COALESCE(%s, true)
      )
      ON CONFLICT (id_empresa) DO UPDATE SET
        preco_fixo_alerta_base = EXCLUDED.preco_fixo_alerta_base,
        alert_venda_cancelada = COALESCE(%s, app.telegram_settings.alert_venda_cancelada),
        alert_nfe_inutilizada = COALESCE(%s, app.telegram_settings.alert_nfe_inutilizada),
        alert_cash_open_over_24h = COALESCE(%s, app.telegram_settings.alert_cash_open_over_24h),
        alert_preco_fixo_bomba = COALESCE(%s, app.telegram_settings.alert_preco_fixo_bomba),
        updated_at = now()
    """
    v_cancel = flags.get(EVENT_VENDA_CANCELADA)
    v_nfe = flags.get(EVENT_NFE_INUTILIZADA)
    v_cash = flags.get(EVENT_CASH_OPEN_OVER_24H)
    v_preco = flags.get(EVENT_PRECO_FIXO_BOMBA)
    # Also accept snake keys from UI
    if "alert_venda_cancelada" in flags:
        v_cancel = flags["alert_venda_cancelada"]
    if "alert_nfe_inutilizada" in flags:
        v_nfe = flags["alert_nfe_inutilizada"]
    if "alert_cash_open_over_24h" in flags:
        v_cash = flags["alert_cash_open_over_24h"]
    if "alert_preco_fixo_bomba" in flags:
        v_preco = flags["alert_preco_fixo_bomba"]
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            conn.execute(
                sql,
                (
                    id_empresa,
                    base,
                    v_cancel,
                    v_nfe,
                    v_cash,
                    v_preco,
                    v_cancel,
                    v_nfe,
                    v_cash,
                    v_preco,
                ),
            )
            conn.commit()
    except Exception as exc:
        logger.warning("save_company_alert_prefs failed: %s", exc)
    return get_company_alert_prefs(id_empresa)


def get_telegram_config(user_id: str, *, id_empresa: Optional[int] = None) -> Dict[str, Any]:
    """Return the current user's Telegram notification settings + alert toggles."""
    sql = """
      SELECT telegram_chat_id, telegram_username, telegram_enabled
      FROM app.user_notification_settings
      WHERE user_id = %s::uuid
    """
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(sql, (str(user_id),)).fetchone()
        base = {
            "telegram_chat_id": row["telegram_chat_id"] if row else None,
            "telegram_username": row["telegram_username"] if row else None,
            "telegram_enabled": bool(row["telegram_enabled"]) if row else False,
            "configured": bool(row and row["telegram_chat_id"] and row["telegram_enabled"]),
            "bot_token_set": bool(settings.telegram_bot_token),
            "alert_catalog": TELEGRAM_ALERT_CATALOG,
            "alert_subscriptions": _list_user_alert_subscriptions(user_id, id_empresa),
        }
        if id_empresa is not None:
            base["company_prefs"] = get_company_alert_prefs(int(id_empresa))
        return base


def save_telegram_config(
    user_id: str,
    *,
    telegram_chat_id: Optional[str],
    telegram_username: Optional[str],
    telegram_enabled: bool,
    alert_subscriptions: Optional[Dict[str, bool]] = None,
    id_empresa: Optional[int] = None,
    preco_fixo_alerta_base: Optional[str] = None,
) -> Dict[str, Any]:
    """Upsert user Telegram notification settings + optional alert toggles."""
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
    if alert_subscriptions:
        _upsert_user_alert_subscriptions(
            user_id,
            alert_subscriptions,
            id_empresa=id_empresa,
        )
    if id_empresa is not None and preco_fixo_alerta_base is not None:
        save_company_alert_prefs(int(id_empresa), preco_fixo_alerta_base=preco_fixo_alerta_base)
    return get_telegram_config(user_id, id_empresa=id_empresa)


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
