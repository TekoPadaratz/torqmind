"""Repository: Competitor Pricing — simplified workflow.

Single-transaction capture of competitor fuel prices by field managers.
No prior station registration required. Audit trail via revisions table.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from app.db import get_conn

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PRICE_MIN = Decimal("1.00")
PRICE_MAX = Decimal("20.00")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_name(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    collapsed = re.sub(r"\s+", " ", ascii_text.strip().lower())
    return collapsed


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _validate_station_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name.strip())
    if len(cleaned) < 3:
        raise ValueError("Nome do posto deve ter ao menos 3 caracteres.")
    if cleaned.isdigit():
        raise ValueError("Nome do posto não pode ser apenas números.")
    if re.fullmatch(r"[^a-zA-Z0-9\u00C0-\u024F]+", cleaned):
        raise ValueError("Nome do posto não pode ser apenas caracteres especiais.")
    return cleaned


def _validate_price(price: Decimal) -> None:
    if price <= 0:
        raise ValueError("Preço deve ser maior que zero.")
    if price < PRICE_MIN or price > PRICE_MAX:
        raise ValueError(f"Preço deve estar entre R$ {PRICE_MIN} e R$ {PRICE_MAX}.")


# ---------------------------------------------------------------------------
# Fuel products for a branch
# ---------------------------------------------------------------------------
def list_fuel_products(
    role: str,
    id_empresa: int,
    id_filial: int,
) -> List[Dict[str, Any]]:
    """Return fuel products for a given branch with own price if available."""
    sql = """
        SELECT
            p.id_produto    AS product_id,
            p.nome          AS product_name,
            COALESCE(g.nome, '')  AS grupo_nome,
            p.unidade,
            p.custo_medio,
            CASE
                WHEN UPPER(p.nome) LIKE '%%GASOL%%' THEN 'GASOLINA'
                WHEN UPPER(p.nome) LIKE '%%ETANOL%%' OR UPPER(p.nome) LIKE '%%ALCOOL%%' THEN 'ETANOL'
                WHEN UPPER(p.nome) LIKE '%%DIESEL S10%%' OR UPPER(p.nome) LIKE '%%S10%%' THEN 'DIESEL S10'
                WHEN UPPER(p.nome) LIKE '%%DIESEL%%' THEN 'DIESEL S500'
                WHEN UPPER(p.nome) LIKE '%%GNV%%' THEN 'GNV'
                ELSE NULL
            END AS fuel_type
        FROM dw.dim_produto p
        LEFT JOIN dw.dim_grupo_produto g
            ON g.id_empresa = p.id_empresa
           AND g.id_filial  = p.id_filial
           AND g.id_grupo_produto = p.id_grupo_produto
        WHERE p.id_empresa = %(id_empresa)s
          AND p.id_filial  = %(id_filial)s
          AND COALESCE(p.situacao, 1) = 1
          AND UPPER(COALESCE(p.unidade, '')) IN ('LT','L','LITRO','LITROS','M3','MTS3')
          AND (
              UPPER(COALESCE(g.nome, '')) LIKE '%%COMBUST%%'
              OR UPPER(COALESCE(g.nome, '')) LIKE '%%GNV%%'
          )
          AND UPPER(COALESCE(g.nome, '')) NOT LIKE '%%FILTRO%%'
          AND UPPER(COALESCE(g.nome, '')) NOT LIKE '%%OLEO%%'
          AND UPPER(COALESCE(g.nome, '')) NOT LIKE '%%LUBR%%'
          AND UPPER(COALESCE(g.nome, '')) NOT LIKE '%%ADITIV%%'
          AND UPPER(COALESCE(p.nome, '')) NOT LIKE '%%ADITIV%%'
          AND UPPER(COALESCE(p.nome, '')) NOT LIKE '%%ARLA%%'
          AND UPPER(COALESCE(p.nome, '')) NOT LIKE '%%LUBR%%'
          AND UPPER(COALESCE(p.nome, '')) NOT LIKE '%%FILTRO%%'
        ORDER BY
            CASE
                WHEN UPPER(p.nome) LIKE '%%GASOL%%' THEN 1
                WHEN UPPER(p.nome) LIKE '%%ETANOL%%' OR UPPER(p.nome) LIKE '%%ALCOOL%%' THEN 2
                WHEN UPPER(p.nome) LIKE '%%DIESEL%%' THEN 3
                WHEN UPPER(p.nome) LIKE '%%GNV%%' THEN 4
                ELSE 5
            END,
            p.nome
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(sql, {"id_empresa": id_empresa, "id_filial": id_filial})
        rows = cur.fetchall()

    own_prices = _get_own_prices(role, id_empresa, id_filial)

    result = []
    for r in rows:
        pid = r["product_id"]
        own = own_prices.get(pid)
        result.append({
            "product_id": pid,
            "product_name": r["product_name"],
            "fuel_type": r["fuel_type"],
            "grupo_nome": r["grupo_nome"],
            "own_current_price": str(own["price"]) if own else None,
            "own_price_source": own["source"] if own else None,
        })
    return result


def _get_own_prices(
    role: str,
    id_empresa: int,
    id_filial: int,
) -> Dict[int, Dict[str, Any]]:
    """Get own fuel prices from recent avg sale price (7-day window)."""
    sql = """
        SELECT
            a.id_produto,
            CASE WHEN SUM(a.qtd) > 0
                 THEN ROUND(SUM(a.faturamento) / SUM(a.qtd), 4)
                 ELSE NULL
            END AS avg_price
        FROM mart.agg_produtos_diaria a
        WHERE a.id_empresa = %(id_empresa)s
          AND a.id_filial  = %(id_filial)s
          AND a.data >= CURRENT_DATE - INTERVAL '7 days'
          AND a.data <= CURRENT_DATE
        GROUP BY a.id_produto
        HAVING SUM(a.qtd) > 0
    """
    result: Dict[int, Dict[str, Any]] = {}
    try:
        with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
            cur = conn.execute(sql, {"id_empresa": id_empresa, "id_filial": id_filial})
            for r in cur.fetchall():
                if r["avg_price"] and r["avg_price"] > 0:
                    result[r["id_produto"]] = {
                        "price": r["avg_price"],
                        "source": "LAST_SALE",
                    }
    except Exception:
        logger.warning("Failed to get own prices from mart.agg_produtos_diaria", exc_info=True)
    return result


# ---------------------------------------------------------------------------
# Create capture (register prices)
# ---------------------------------------------------------------------------
def create_capture(
    *,
    role: str,
    id_empresa: int,
    id_filial: int,
    station_name: str,
    capture_date: date,
    observation: Optional[str],
    items: List[Dict[str, Any]],
    user_id: str,
    user_name: str,
    client_ip: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> Dict[str, Any]:
    """Register a full price capture in a single transaction."""
    station_name = _validate_station_name(station_name)
    normalized = _normalize_name(station_name)
    now = _now_utc()

    valid_items = []
    for item in items:
        price = Decimal(str(item["price"]))
        if price <= 0:
            continue
        _validate_price(price)
        valid_items.append({**item, "price": price})

    if not valid_items:
        raise ValueError("Informe ao menos um combustível com preço maior que zero.")

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        station_id = _find_or_create_station(
            conn, id_empresa, id_filial, station_name, normalized,
            user_id, user_name, now,
        )

        cap_sql = """
            INSERT INTO app.competitor_price_captures
                (id_empresa, id_filial, station_id, station_name_snapshot,
                 station_name_normalized, capture_date, captured_at, status,
                 registered_by_user_id, registered_by_user_name, registered_at,
                 observation, client_ip, user_agent)
            VALUES
                (%(id_empresa)s, %(id_filial)s, %(station_id)s, %(station_name)s,
                 %(normalized)s, %(capture_date)s, %(now)s, 'CONFIRMED',
                 %(user_id)s, %(user_name)s, %(now)s,
                 %(observation)s, %(client_ip)s, %(user_agent)s)
            RETURNING id
        """
        cur = conn.execute(cap_sql, {
            "id_empresa": id_empresa, "id_filial": id_filial,
            "station_id": str(station_id), "station_name": station_name,
            "normalized": normalized, "capture_date": capture_date,
            "now": now, "user_id": user_id, "user_name": user_name,
            "observation": observation or None,
            "client_ip": client_ip, "user_agent": user_agent,
        })
        capture_id = cur.fetchone()["id"]

        product_map = _get_product_names(conn, id_empresa, id_filial,
                                         [it["product_id"] for it in valid_items])

        saved_items = []
        for it in valid_items:
            pid = it["product_id"]
            pinfo = product_map.get(pid, {})
            pname = pinfo.get("nome", f"Produto {pid}")
            ftype = pinfo.get("fuel_type")

            item_sql = """
                INSERT INTO app.competitor_price_items
                    (id_empresa, id_filial, capture_id, station_id,
                     station_name_snapshot, capture_date, product_id,
                     product_name_snapshot, fuel_type_snapshot, price,
                     is_valid, created_by_user_id, created_by_user_name)
                VALUES
                    (%(id_empresa)s, %(id_filial)s, %(capture_id)s, %(station_id)s,
                     %(station_name)s, %(capture_date)s, %(product_id)s,
                     %(product_name)s, %(fuel_type)s, %(price)s,
                     true, %(user_id)s, %(user_name)s)
                RETURNING id
            """
            cur2 = conn.execute(item_sql, {
                "id_empresa": id_empresa, "id_filial": id_filial,
                "capture_id": str(capture_id), "station_id": str(station_id),
                "station_name": station_name, "capture_date": capture_date,
                "product_id": pid, "product_name": pname, "fuel_type": ftype,
                "price": it["price"], "user_id": user_id, "user_name": user_name,
            })
            item_id = cur2.fetchone()["id"]

            conn.execute(
                """INSERT INTO app.competitor_price_item_revisions
                       (id_empresa, id_filial, capture_id, item_id, station_id,
                        station_name_snapshot, capture_date, product_id,
                        product_name_snapshot, revision_number, action_type,
                        old_price, new_price, changed_by_user_id,
                        changed_by_user_name, changed_at)
                   VALUES
                       (%(id_empresa)s, %(id_filial)s, %(capture_id)s, %(item_id)s,
                        %(station_id)s, %(station_name)s, %(capture_date)s,
                        %(product_id)s, %(product_name)s, 1, 'CREATE',
                        NULL, %(price)s, %(user_id)s, %(user_name)s, %(now)s)""",
                {
                    "id_empresa": id_empresa, "id_filial": id_filial,
                    "capture_id": str(capture_id), "item_id": str(item_id),
                    "station_id": str(station_id), "station_name": station_name,
                    "capture_date": capture_date, "product_id": pid,
                    "product_name": pname, "price": it["price"],
                    "user_id": user_id, "user_name": user_name, "now": now,
                },
            )

            saved_items.append({
                "item_id": str(item_id),
                "product_id": pid,
                "product_name": pname,
                "fuel_type": ftype,
                "price": str(it["price"]),
            })

        conn.commit()

    return {
        "capture_id": str(capture_id),
        "station_id": str(station_id),
        "station_name": station_name,
        "capture_date": str(capture_date),
        "items_saved": len(saved_items),
        "items": saved_items,
    }


def _find_or_create_station(
    conn, id_empresa: int, id_filial: int,
    station_name: str, normalized: str,
    user_id: str, user_name: str, now: datetime,
) -> UUID:
    cur = conn.execute(
        """SELECT id FROM app.competitor_stations
           WHERE id_empresa = %(e)s AND id_filial = %(f)s
             AND station_name_normalized = %(n)s AND deleted_at IS NULL
           LIMIT 1""",
        {"e": id_empresa, "f": id_filial, "n": normalized},
    )
    row = cur.fetchone()
    if row:
        return row["id"]

    cur = conn.execute(
        """INSERT INTO app.competitor_stations
               (id_empresa, id_filial, station_name, station_name_normalized,
                active, created_by_user_id, created_by_user_name, created_at)
           VALUES (%(e)s, %(f)s, %(name)s, %(norm)s, true, %(uid)s, %(uname)s, %(now)s)
           RETURNING id""",
        {"e": id_empresa, "f": id_filial, "name": station_name,
         "norm": normalized, "uid": user_id, "uname": user_name, "now": now},
    )
    return cur.fetchone()["id"]


def _get_product_names(
    conn, id_empresa: int, id_filial: int, product_ids: List[int],
) -> Dict[int, Dict[str, Any]]:
    if not product_ids:
        return {}
    placeholders = ",".join(["%s"] * len(product_ids))
    sql = f"""
        SELECT id_produto, nome,
            CASE
                WHEN UPPER(nome) LIKE '%%GASOL%%' THEN 'GASOLINA'
                WHEN UPPER(nome) LIKE '%%ETANOL%%' OR UPPER(nome) LIKE '%%ALCOOL%%' THEN 'ETANOL'
                WHEN UPPER(nome) LIKE '%%DIESEL S10%%' OR UPPER(nome) LIKE '%%S10%%' THEN 'DIESEL S10'
                WHEN UPPER(nome) LIKE '%%DIESEL%%' THEN 'DIESEL S500'
                WHEN UPPER(nome) LIKE '%%GNV%%' THEN 'GNV'
                ELSE NULL
            END AS fuel_type
        FROM dw.dim_produto
        WHERE id_empresa = %s AND id_filial = %s
          AND id_produto IN ({placeholders})
    """
    cur = conn.execute(sql, [id_empresa, id_filial] + product_ids)
    return {r["id_produto"]: r for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# History (captures of the day)
# ---------------------------------------------------------------------------
def list_history(
    role: str,
    id_empresa: int,
    id_filial: int,
    capture_date: date,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            c.id AS capture_id,
            c.station_name_snapshot AS station_name,
            c.capture_date,
            c.captured_at,
            c.registered_by_user_name,
            c.observation,
            c.station_id
        FROM app.competitor_price_captures c
        WHERE c.id_empresa = %(id_empresa)s
          AND c.id_filial  = %(id_filial)s
          AND c.capture_date = %(capture_date)s
        ORDER BY c.captured_at DESC
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(sql, {
            "id_empresa": id_empresa, "id_filial": id_filial,
            "capture_date": capture_date,
        })
        captures = cur.fetchall()

        if not captures:
            return []

        capture_ids = [str(c["capture_id"]) for c in captures]
        placeholders = ",".join(["%s"] * len(capture_ids))

        items_sql = f"""
            SELECT
                i.id AS item_id,
                i.capture_id,
                i.product_id,
                i.product_name_snapshot AS product_name,
                i.fuel_type_snapshot AS fuel_type,
                i.price,
                i.is_valid,
                i.created_at,
                i.created_by_user_name,
                COALESCE(
                    (SELECT MAX(r.revision_number)
                     FROM app.competitor_price_item_revisions r
                     WHERE r.item_id = i.id), 1
                ) AS latest_revision_number,
                (SELECT r.old_price
                 FROM app.competitor_price_item_revisions r
                 WHERE r.item_id = i.id AND r.action_type = 'UPDATE_PRICE'
                 ORDER BY r.revision_number DESC LIMIT 1
                ) AS previous_price
            FROM app.competitor_price_items i
            WHERE i.capture_id IN ({placeholders})
            ORDER BY i.product_id
        """
        cur2 = conn.execute(items_sql, capture_ids)
        items_by_capture: Dict[str, list] = {}
        for it in cur2.fetchall():
            cid = str(it["capture_id"])
            items_by_capture.setdefault(cid, []).append({
                "item_id": str(it["item_id"]),
                "product_id": it["product_id"],
                "product_name": it["product_name"],
                "fuel_type": it["fuel_type"],
                "price": str(it["price"]),
                "is_valid": it["is_valid"],
                "created_at": it["created_at"].isoformat() if it["created_at"] else None,
                "created_by_user_name": it["created_by_user_name"],
                "latest_revision_number": it["latest_revision_number"],
                "previous_price": str(it["previous_price"]) if it["previous_price"] else None,
            })

    result = []
    for c in captures:
        cid = str(c["capture_id"])
        result.append({
            "capture_id": cid,
            "station_name": c["station_name"],
            "capture_date": str(c["capture_date"]),
            "captured_at": c["captured_at"].isoformat() if c["captured_at"] else None,
            "registered_by_user_name": c["registered_by_user_name"],
            "observation": c["observation"],
            "items": items_by_capture.get(cid, []),
        })
    return result


# ---------------------------------------------------------------------------
# Update price (with audit revision)
# ---------------------------------------------------------------------------
def update_item_price(
    *,
    role: str,
    id_empresa: int,
    item_id: str,
    new_price: Decimal,
    change_reason: Optional[str],
    user_id: str,
    user_name: str,
    client_ip: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> Dict[str, Any]:
    _validate_price(new_price)
    now = _now_utc()

    with get_conn(role=role, tenant_id=id_empresa) as conn:
        cur = conn.execute(
            """SELECT i.*, c.station_id, c.station_name_snapshot
               FROM app.competitor_price_items i
               JOIN app.competitor_price_captures c ON c.id = i.capture_id
               WHERE i.id = %(item_id)s AND i.id_empresa = %(id_empresa)s""",
            {"item_id": item_id, "id_empresa": id_empresa},
        )
        item = cur.fetchone()
        if not item:
            raise ValueError("Item não encontrado.")

        old_price = item["price"]
        if old_price == new_price:
            return {
                "item_id": item_id,
                "price": str(old_price),
                "message": "Preço não alterado (mesmo valor).",
            }

        cur2 = conn.execute(
            """SELECT COALESCE(MAX(revision_number), 0) + 1 AS next_rev
               FROM app.competitor_price_item_revisions
               WHERE item_id = %(item_id)s""",
            {"item_id": item_id},
        )
        next_rev = cur2.fetchone()["next_rev"]

        conn.execute(
            """UPDATE app.competitor_price_items
               SET price = %(new_price)s, created_at = %(now)s
               WHERE id = %(item_id)s""",
            {"new_price": new_price, "now": now, "item_id": item_id},
        )

        conn.execute(
            """INSERT INTO app.competitor_price_item_revisions
                   (id_empresa, id_filial, capture_id, item_id, station_id,
                    station_name_snapshot, capture_date, product_id,
                    product_name_snapshot, revision_number, action_type,
                    old_price, new_price, changed_by_user_id,
                    changed_by_user_name, changed_at, change_reason,
                    client_ip, user_agent)
               VALUES
                   (%(id_empresa)s, %(id_filial)s, %(capture_id)s, %(item_id)s,
                    %(station_id)s, %(station_name)s, %(capture_date)s,
                    %(product_id)s, %(product_name)s, %(next_rev)s, 'UPDATE_PRICE',
                    %(old_price)s, %(new_price)s, %(user_id)s,
                    %(user_name)s, %(now)s, %(change_reason)s,
                    %(client_ip)s, %(user_agent)s)""",
            {
                "id_empresa": item["id_empresa"],
                "id_filial": item["id_filial"],
                "capture_id": str(item["capture_id"]),
                "item_id": item_id,
                "station_id": str(item["station_id"]),
                "station_name": item["station_name_snapshot"],
                "capture_date": item["capture_date"],
                "product_id": item["product_id"],
                "product_name": item["product_name_snapshot"],
                "next_rev": next_rev,
                "old_price": old_price,
                "new_price": new_price,
                "user_id": user_id,
                "user_name": user_name,
                "now": now,
                "change_reason": change_reason,
                "client_ip": client_ip,
                "user_agent": user_agent,
            },
        )

        conn.commit()

    return {
        "item_id": item_id,
        "old_price": str(old_price),
        "new_price": str(new_price),
        "revision_number": next_rev,
        "message": "Preço atualizado com sucesso.",
    }


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------
def get_comparison(
    role: str,
    id_empresa: int,
    id_filial: int,
    capture_date: date,
) -> List[Dict[str, Any]]:
    """Build comparison: own price vs latest competitor price per product/date."""
    competitor_sql = """
        WITH ranked AS (
            SELECT
                i.product_id,
                i.product_name_snapshot AS product_name,
                i.fuel_type_snapshot AS fuel_type,
                i.price,
                i.station_id,
                c.station_name_snapshot AS station_name,
                i.created_at,
                i.created_by_user_name,
                ROW_NUMBER() OVER (
                    PARTITION BY i.station_id, i.product_id
                    ORDER BY i.created_at DESC
                ) AS rn
            FROM app.competitor_price_items i
            JOIN app.competitor_price_captures c ON c.id = i.capture_id
            WHERE i.id_empresa = %(id_empresa)s
              AND i.id_filial  = %(id_filial)s
              AND i.capture_date = %(capture_date)s
              AND i.is_valid = true
              AND i.price > 0
        )
        SELECT product_id, product_name, fuel_type, price,
               station_id, station_name, created_at, created_by_user_name
        FROM ranked
        WHERE rn = 1
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(competitor_sql, {
            "id_empresa": id_empresa, "id_filial": id_filial,
            "capture_date": capture_date,
        })
        competitor_rows = cur.fetchall()

    own_prices = _get_own_prices(role, id_empresa, id_filial)
    fuels = list_fuel_products(role, id_empresa, id_filial)

    by_product: Dict[int, list] = {}
    for r in competitor_rows:
        by_product.setdefault(r["product_id"], []).append(r)

    result = []
    for fuel in fuels:
        pid = fuel["product_id"]
        own = own_prices.get(pid)
        entries = by_product.get(pid, [])

        if not entries and not own:
            continue

        own_price = Decimal(own["price"]) if own else None
        own_source = own["source"] if own else None

        if entries:
            prices = [Decimal(str(e["price"])) for e in entries]
            avg_price = sum(prices) / len(prices)
            min_price = min(prices)
            min_entry = min(entries, key=lambda e: Decimal(str(e["price"])))
            last_entry = max(entries, key=lambda e: e["created_at"])

            diff_value = (own_price - min_price) if own_price else None
            diff_percent = (
                (diff_value / min_price * 100) if diff_value is not None and min_price > 0 else None
            )

            if own_price is None:
                status = "SEM_PRECO_PROPRIO"
            elif own_price < min_price:
                status = "MEU_POSTO_MAIS_BARATO"
            elif own_price > min_price:
                status = "MEU_POSTO_MAIS_CARO"
            else:
                status = "IGUAL_AO_MENOR"

            result.append({
                "product_id": pid,
                "product_name": fuel["product_name"],
                "fuel_type": fuel["fuel_type"],
                "own_current_price": str(own_price) if own_price else None,
                "own_price_source": own_source,
                "competitor_avg_price": str(round(avg_price, 4)),
                "competitor_min_price": str(min_price),
                "competitor_min_station_name": min_entry["station_name"],
                "competitor_count": len(entries),
                "diff_value": str(round(diff_value, 4)) if diff_value is not None else None,
                "diff_percent": str(round(diff_percent, 2)) if diff_percent is not None else None,
                "status": status,
                "last_competitor_record_at": last_entry["created_at"].isoformat() if last_entry["created_at"] else None,
                "last_competitor_user_name": last_entry["created_by_user_name"],
            })
        else:
            result.append({
                "product_id": pid,
                "product_name": fuel["product_name"],
                "fuel_type": fuel["fuel_type"],
                "own_current_price": str(own_price) if own_price else None,
                "own_price_source": own_source,
                "competitor_avg_price": None,
                "competitor_min_price": None,
                "competitor_min_station_name": None,
                "competitor_count": 0,
                "diff_value": None,
                "diff_percent": None,
                "status": "SEM_CONCORRENTE",
                "last_competitor_record_at": None,
                "last_competitor_user_name": None,
            })

    return result
