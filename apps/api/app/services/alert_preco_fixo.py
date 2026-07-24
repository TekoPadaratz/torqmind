"""Alerta Telegram: bomba subiu e preço fixo do cliente não acompanhou.

Evento canônico: PRECO_FIXO_BOMBA_DESATUALIZADO

Base de comparação por empresa (app.telegram_settings.preco_fixo_alerta_base):
  - venda → preco_bomba_dia vs valor_fixo
  - custo → custo de reposição (último custo_unitario_shadow) vs valor_fixo

Dedupe: app.alert_preco_fixo_bomba PK (empresa, filial, entidade, produto).
Re-dispara só se a referência subir de novo sem reajuste do fixo.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from app.db import get_conn
from app.filial_apelido import apelido_for
from app.services.telegram import (
    DELTA_PRECO_FIXO,
    EVENT_PRECO_FIXO_BOMBA,
    _send_telegram_sync,
    _to_int,
    get_company_alert_prefs,
    resolve_telegram_recipients,
)

logger = logging.getLogger(__name__)


def _fmt_money(value: float) -> str:
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(value)


def _load_active_cadastros(id_empresa: int) -> List[Dict[str, Any]]:
    sql = """
      SELECT DISTINCT ON (
        d.id_empresa, d.id_filial,
        COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0),
        COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0)
      )
        d.id_empresa,
        d.id_filial,
        COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0) AS id_entidade,
        COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto,
        COALESCE((d.payload->>'VALOR')::numeric, 0) AS valor_fixo
      FROM stg.descontos_entidades_itens d
      WHERE d.id_empresa = %s
        AND COALESCE(d.payload->>'VALORFIXO','0') IN ('1','true','True','t')
        AND COALESCE(d.payload->>'ATIVO','1') IN ('1','true','True','t')
        AND COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0) > 0
        AND COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0) > 0
      ORDER BY
        d.id_empresa, d.id_filial,
        COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0),
        COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0),
        COALESCE((d.payload->>'VALOR')::numeric, 0) ASC
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        return [dict(r) for r in conn.execute(sql, (id_empresa,)).fetchall()]


def _preco_bomba_hoje(id_empresa: int, id_filial: int, id_produto: int) -> Optional[float]:
    sql = """
      SELECT preco_venda_shadow AS preco
      FROM stg.preco_bomba_hist
      WHERE id_empresa = %s
        AND id_filial = %s
        AND id_produto = %s
        AND preco_venda_shadow IS NOT NULL
        AND preco_venda_shadow > 0
        AND (dt_alteracao_shadow AT TIME ZONE 'America/Sao_Paulo')::date <= CURRENT_DATE
      ORDER BY dt_alteracao_shadow DESC
      LIMIT 1
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        row = conn.execute(sql, (id_empresa, id_filial, id_produto)).fetchone()
        if row and row.get("preco") is not None:
            return float(row["preco"])
    return None


def _custo_reposicao(id_empresa: int, id_filial: int, id_produto: int) -> Optional[float]:
    sql = """
      SELECT custo_unitario_shadow AS custo
      FROM stg.itenscomprovantes
      WHERE id_empresa = %s
        AND id_filial = %s
        AND COALESCE(NULLIF(payload->>'ID_PRODUTOS','')::int, id_produto_shadow, 0) = %s
        AND custo_unitario_shadow IS NOT NULL
        AND custo_unitario_shadow > 0
      ORDER BY received_at DESC NULLS LAST, id_itemcomprovante DESC
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_empresa, id_filial, id_produto)).fetchone()
            if row and row.get("custo") is not None:
                return float(row["custo"])
    except Exception:
        logger.exception("custo_reposicao lookup failed empresa=%s filial=%s produto=%s", id_empresa, id_filial, id_produto)
    return None


def _cliente_nome(id_empresa: int, id_filial: int, id_entidade: int) -> str:
    sql = """
      SELECT COALESCE(
        NULLIF(payload->>'NOMEENTIDADE', ''),
        NULLIF(payload->>'NOME', ''),
        format('Cliente %s', %s)
      ) AS nome
      FROM stg.entidades
      WHERE id_empresa = %s AND id_filial = %s AND id_entidade = %s
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_entidade, id_empresa, id_filial, id_entidade)).fetchone()
            if row and row.get("nome"):
                return str(row["nome"]).strip()
    except Exception:
        pass
    return f"Cliente {id_entidade}"


def _produto_nome(id_empresa: int, id_filial: int, id_produto: int) -> str:
    sql = """
      SELECT COALESCE(NULLIF(payload->>'NOMEPRODUTO', ''), format('Produto %s', %s)) AS nome
      FROM stg.produtos
      WHERE id_empresa = %s AND id_filial = %s AND id_produto = %s
      LIMIT 1
    """
    try:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            row = conn.execute(sql, (id_produto, id_empresa, id_filial, id_produto)).fetchone()
            if row and row.get("nome"):
                return str(row["nome"]).strip()
    except Exception:
        pass
    return f"Produto {id_produto}"


def _should_notify(
    id_empresa: int,
    id_filial: int,
    id_entidade: int,
    id_produto: int,
    *,
    valor_fixo: float,
    preco_ref: float,
    base_ref: str,
) -> bool:
    if preco_ref <= valor_fixo + DELTA_PRECO_FIXO:
        return False
    sql = """
      SELECT preco_fixo, preco_ref_na_notificacao, base_ref
      FROM app.alert_preco_fixo_bomba
      WHERE id_empresa = %s AND id_filial = %s AND id_entidade = %s AND id_produto = %s
    """
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        row = conn.execute(sql, (id_empresa, id_filial, id_entidade, id_produto)).fetchone()
    if not row:
        return True
    last_ref = float(row.get("preco_ref_na_notificacao") or 0)
    last_fixo = float(row.get("preco_fixo") or 0)
    # Reajuste do fixo ≥ referência notificada → ciclo novo só se bomba subir de novo
    if valor_fixo >= last_ref - DELTA_PRECO_FIXO:
        return preco_ref > last_ref + DELTA_PRECO_FIXO
    # Fixo ainda abaixo; só re-dispara se a referência subiu
    return preco_ref > last_ref + DELTA_PRECO_FIXO or abs(valor_fixo - last_fixo) > DELTA_PRECO_FIXO


def _mark_notified(
    id_empresa: int,
    id_filial: int,
    id_entidade: int,
    id_produto: int,
    *,
    base_ref: str,
    valor_fixo: float,
    preco_ref: float,
    payload: Dict[str, Any],
) -> None:
    sql = """
      INSERT INTO app.alert_preco_fixo_bomba (
        id_empresa, id_filial, id_entidade, id_produto,
        base_ref, preco_fixo, preco_ref_na_notificacao, notificado_em, payload
      )
      VALUES (%s,%s,%s,%s,%s,%s,%s,now(),%s::jsonb)
      ON CONFLICT (id_empresa, id_filial, id_entidade, id_produto)
      DO UPDATE SET
        base_ref = EXCLUDED.base_ref,
        preco_fixo = EXCLUDED.preco_fixo,
        preco_ref_na_notificacao = EXCLUDED.preco_ref_na_notificacao,
        notificado_em = now(),
        payload = EXCLUDED.payload
    """
    import json

    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        conn.execute(
            sql,
            (
                id_empresa,
                id_filial,
                id_entidade,
                id_produto,
                base_ref,
                valor_fixo,
                preco_ref,
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()


def _dispatch_one(
    id_empresa: int,
    cad: Dict[str, Any],
    *,
    base_ref: str,
    dry_run: bool = False,
) -> Optional[Dict[str, Any]]:
    id_filial = int(cad["id_filial"])
    id_entidade = int(cad["id_entidade"])
    id_produto = int(cad["id_produto"])
    valor_fixo = float(cad.get("valor_fixo") or 0)
    if valor_fixo <= 0:
        return None

    if base_ref == "custo":
        preco_ref = _custo_reposicao(id_empresa, id_filial, id_produto)
        base_label = "custo de reposição"
    else:
        preco_ref = _preco_bomba_hoje(id_empresa, id_filial, id_produto)
        base_label = "preço da bomba"

    if preco_ref is None:
        return None
    if not _should_notify(
        id_empresa,
        id_filial,
        id_entidade,
        id_produto,
        valor_fixo=valor_fixo,
        preco_ref=preco_ref,
        base_ref=base_ref,
    ):
        return None

    prefs = get_company_alert_prefs(id_empresa)
    if not prefs.get("alert_preco_fixo_bomba", True):
        return {"skipped": True, "reason": "company_alert_disabled"}

    recipients = resolve_telegram_recipients(id_empresa, EVENT_PRECO_FIXO_BOMBA)
    if not recipients:
        return {"skipped": True, "reason": "no_recipients"}

    filial_label = apelido_for(id_filial) or str(id_filial)
    cliente = _cliente_nome(id_empresa, id_filial, id_entidade)
    produto = _produto_nome(id_empresa, id_filial, id_produto)
    delta = preco_ref - valor_fixo
    text = (
        f"⛽ PREÇO FIXO DESATUALIZADO — {filial_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Cliente: {cliente}\n"
        f"🛢️ Produto: {produto}\n"
        f"📌 Preço fixo: {_fmt_money(valor_fixo)}\n"
        f"📈 {base_label.capitalize()}: {_fmt_money(preco_ref)}\n"
        f"📉 Diferença: {_fmt_money(delta)}\n"
        f"A bomba/custo subiu e o valor fixo do cliente não acompanhou."
    )
    payload = {
        "event_type": EVENT_PRECO_FIXO_BOMBA,
        "id_filial": id_filial,
        "id_entidade": id_entidade,
        "id_produto": id_produto,
        "cliente_nome": cliente,
        "produto_nome": produto,
        "valor_fixo": valor_fixo,
        "preco_ref": preco_ref,
        "base_ref": base_ref,
        "delta": delta,
    }
    if dry_run:
        return {"dry_run": True, "recipients": len(recipients), **payload}

    sent = 0
    for chat_id in recipients:
        if _send_telegram_sync(chat_id, text):
            sent += 1
    if sent > 0:
        _mark_notified(
            id_empresa,
            id_filial,
            id_entidade,
            id_produto,
            base_ref=base_ref,
            valor_fixo=valor_fixo,
            preco_ref=preco_ref,
            payload=payload,
        )
    return {"sent": sent, "recipients": len(recipients), **payload}


def scan_and_notify_preco_fixo(
    id_empresa: int,
    *,
    candidates: Optional[Sequence[Dict[str, Any]]] = None,
    dry_run: bool = False,
    limit: int = 200,
) -> Dict[str, Any]:
    """Varre cadastros elegíveis (lote EOD ou lista filtrada da venda)."""
    prefs = get_company_alert_prefs(id_empresa)
    base_ref = str(prefs.get("preco_fixo_alerta_base") or "venda")
    if base_ref not in {"venda", "custo"}:
        base_ref = "venda"

    cadastros = list(candidates) if candidates is not None else _load_active_cadastros(id_empresa)
    notified: List[Dict[str, Any]] = []
    skipped = 0
    for cad in cadastros[: max(1, int(limit))]:
        try:
            result = _dispatch_one(id_empresa, cad, base_ref=base_ref, dry_run=dry_run)
        except Exception as exc:  # noqa: BLE001
            logger.warning("preco_fixo alert failed: %s", exc)
            skipped += 1
            continue
        if not result:
            skipped += 1
            continue
        if result.get("skipped"):
            skipped += 1
            continue
        notified.append(result)
    return {
        "ok": True,
        "event_type": EVENT_PRECO_FIXO_BOMBA,
        "base_ref": base_ref,
        "scanned": len(cadastros),
        "notified": len(notified),
        "skipped": skipped,
        "items": notified[:20],
    }


def notify_preco_fixo_from_itens(
    id_empresa: int,
    raw_itens: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Hot path na ingestão de itenscomprovantes: filtra candidatos e dispara."""
    if not raw_itens:
        return {"ok": True, "notified": 0, "reason": "empty"}

    filiais: set[int] = set()
    produtos: set[int] = set()
    entidades: set[int] = set()
    for row in raw_itens:
        if not isinstance(row, dict):
            continue
        id_filial = _to_int(row.get("ID_FILIAL") or row.get("id_filial"))
        id_entidade = _to_int(
            row.get("ID_ENTIDADE")
            or row.get("ID_CLIENTES")
            or row.get("id_entidade")
            or row.get("id_cliente")
        )
        id_produto = _to_int(row.get("ID_PRODUTOS") or row.get("ID_PRODUTO") or row.get("id_produto"))
        if id_filial:
            filiais.add(id_filial)
        if id_produto:
            produtos.add(id_produto)
        if id_entidade:
            entidades.add(id_entidade)

    if not filiais or not produtos:
        return {"ok": True, "notified": 0, "reason": "no_keys"}

    cadastros = _load_active_cadastros(id_empresa)
    candidates = [
        c
        for c in cadastros
        if int(c["id_filial"]) in filiais
        and int(c["id_produto"]) in produtos
        and (not entidades or int(c["id_entidade"]) in entidades)
    ]
    return scan_and_notify_preco_fixo(id_empresa, candidates=candidates, limit=100)
