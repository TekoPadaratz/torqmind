"""Repository for competitor pricing CRUD operations (PostgreSQL).

All operations are transactional — the web layer does NOT do real-time
joins; everything is materialized in Marts by the CDC consumer.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from app.db import get_conn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Lowercase + strip accents + collapse whitespace for unique-index matching."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_text.strip().lower())


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Stations
# ---------------------------------------------------------------------------

def list_stations(
    role: str,
    id_empresa: int,
    id_filial: int,
    *,
    include_deleted: bool = False,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, station_name, station_name_normalized, document_number,
               address_text, city, state, latitude, longitude, is_active,
               created_by_user_name_snapshot AS created_by,
               created_at, updated_at, deleted_at
        FROM app.competitor_stations
        WHERE id_empresa = %s AND id_filial = %s
    """
    params: list = [id_empresa, id_filial]
    if not include_deleted:
        sql += " AND deleted_at IS NULL"
    sql += " ORDER BY station_name"
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_station(
    role: str,
    id_empresa: int,
    id_filial: int,
    station_id: UUID,
) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT id, station_name, station_name_normalized, document_number,
               address_text, city, state, latitude, longitude, is_active,
               created_by_user_name_snapshot AS created_by,
               created_at, updated_at, deleted_at
        FROM app.competitor_stations
        WHERE id_empresa = %s AND id_filial = %s AND id = %s
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, [id_empresa, id_filial, str(station_id)]).fetchone()
        return dict(row) if row else None


def create_station(
    role: str,
    id_empresa: int,
    id_filial: int,
    *,
    station_name: str,
    document_number: Optional[str] = None,
    address_text: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    user_id: str,
    user_name: str,
) -> Dict[str, Any]:
    normalized = _normalize_name(station_name)
    sql = """
        INSERT INTO app.competitor_stations
            (id_empresa, id_filial, station_name, station_name_normalized,
             document_number, address_text, city, state, latitude, longitude,
             created_by_user_id, created_by_user_name_snapshot, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        RETURNING id, station_name, station_name_normalized, created_at
    """
    params = [
        id_empresa, id_filial, station_name.strip(), normalized,
        document_number, address_text, city, state, latitude, longitude,
        user_id, user_name,
    ]
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()
        return dict(row)


def update_station(
    role: str,
    id_empresa: int,
    id_filial: int,
    station_id: UUID,
    *,
    station_name: Optional[str] = None,
    document_number: Optional[str] = None,
    address_text: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    is_active: Optional[bool] = None,
    user_id: str,
    user_name: str,
) -> Optional[Dict[str, Any]]:
    sets: list[str] = []
    params: list = []

    if station_name is not None:
        sets.append("station_name = %s")
        params.append(station_name.strip())
        sets.append("station_name_normalized = %s")
        params.append(_normalize_name(station_name))
    if document_number is not None:
        sets.append("document_number = %s")
        params.append(document_number)
    if address_text is not None:
        sets.append("address_text = %s")
        params.append(address_text)
    if city is not None:
        sets.append("city = %s")
        params.append(city)
    if state is not None:
        sets.append("state = %s")
        params.append(state)
    if latitude is not None:
        sets.append("latitude = %s")
        params.append(latitude)
    if longitude is not None:
        sets.append("longitude = %s")
        params.append(longitude)
    if is_active is not None:
        sets.append("is_active = %s")
        params.append(is_active)

    if not sets:
        return get_station(role, id_empresa, id_filial, station_id)

    sets.append("updated_by_user_id = %s")
    params.append(user_id)
    sets.append("updated_by_user_name_snapshot = %s")
    params.append(user_name)
    sets.append("updated_at = now()")

    params.extend([id_empresa, id_filial, str(station_id)])

    sql = f"""
        UPDATE app.competitor_stations
        SET {', '.join(sets)}
        WHERE id_empresa = %s AND id_filial = %s AND id = %s AND deleted_at IS NULL
        RETURNING id, station_name, station_name_normalized, is_active, updated_at
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()
        return dict(row) if row else None


def soft_delete_station(
    role: str,
    id_empresa: int,
    id_filial: int,
    station_id: UUID,
    *,
    user_id: str,
    user_name: str,
) -> bool:
    sql = """
        UPDATE app.competitor_stations
        SET deleted_at = now(), is_active = false,
            updated_by_user_id = %s, updated_by_user_name_snapshot = %s, updated_at = now()
        WHERE id_empresa = %s AND id_filial = %s AND id = %s AND deleted_at IS NULL
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(sql, [user_id, user_name, id_empresa, id_filial, str(station_id)])
        conn.commit()
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Captures + Items (transactional)
# ---------------------------------------------------------------------------

def upsert_capture(
    role: str,
    id_empresa: int,
    id_filial: int,
    *,
    station_id: str,
    capture_date: date,
    items: List[Dict[str, Any]],
    observation: Optional[str] = None,
    source: str = "WEB",
    client_ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    geo_latitude: Optional[float] = None,
    geo_longitude: Optional[float] = None,
    geo_accuracy_meters: Optional[float] = None,
    user_id: str,
    user_name: str,
) -> Dict[str, Any]:
    """Create or update a price capture for a station+date.

    If a capture already exists for this station+date (status <> 'DELETED'),
    update it: soft-delete old items and insert new ones, tracking revisions.
    """
    now = _now_utc()

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        # Check station exists
        station = conn.execute(
            "SELECT id, station_name FROM app.competitor_stations WHERE id = %s AND id_empresa = %s AND id_filial = %s AND deleted_at IS NULL",
            [station_id, id_empresa, id_filial],
        ).fetchone()
        if not station:
            raise ValueError("station_not_found")

        # Find existing capture for this station+date
        existing = conn.execute(
            """SELECT id FROM app.competitor_price_captures
               WHERE id_empresa = %s AND id_filial = %s AND station_id = %s AND capture_date = %s AND status <> 'DELETED'""",
            [id_empresa, id_filial, station_id, capture_date],
        ).fetchone()

        if existing:
            capture_id = existing["id"]
            # Update capture header
            conn.execute(
                """UPDATE app.competitor_price_captures
                   SET last_updated_by_user_id = %s, last_updated_by_user_name_snapshot = %s,
                       last_updated_at = %s, observation = COALESCE(%s, observation),
                       updated_at = %s
                   WHERE id = %s""",
                [user_id, user_name, now, observation, now, capture_id],
            )
        else:
            # Create new capture
            row = conn.execute(
                """INSERT INTO app.competitor_price_captures
                       (id_empresa, id_filial, station_id, capture_date, captured_at, status,
                        registered_by_user_id, registered_by_user_name_snapshot, registered_at,
                        observation, source, client_ip, user_agent,
                        geo_latitude, geo_longitude, geo_accuracy_meters)
                   VALUES (%s, %s, %s, %s, %s, 'CONFIRMED', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                [
                    id_empresa, id_filial, station_id, capture_date, now,
                    user_id, user_name, now, observation, source,
                    client_ip, user_agent,
                    geo_latitude, geo_longitude, geo_accuracy_meters,
                ],
            ).fetchone()
            capture_id = row["id"]

        # Upsert items
        created_count = 0
        updated_count = 0
        for item in items:
            product_id = item["id_produto"]
            new_price = item["price"]
            product_name = item.get("product_name", "")
            fuel_type = item.get("fuel_type", "")

            # Check existing item
            existing_item = conn.execute(
                """SELECT id, current_price, revision_number FROM app.competitor_price_capture_items
                   WHERE capture_id = %s AND id_produto = %s""",
                [capture_id, product_id],
            ).fetchone()

            if existing_item:
                old_price = float(existing_item["current_price"])
                if abs(old_price - new_price) < 0.0001:
                    continue  # no change
                new_rev = existing_item["revision_number"] + 1
                # Update item
                conn.execute(
                    """UPDATE app.competitor_price_capture_items
                       SET current_price = %s, revision_number = %s,
                           last_updated_by_user_id = %s, last_updated_by_user_name_snapshot = %s,
                           last_updated_at = %s, updated_at = %s
                       WHERE id = %s""",
                    [new_price, new_rev, user_id, user_name, now, now, existing_item["id"]],
                )
                # Record revision
                conn.execute(
                    """INSERT INTO app.competitor_price_capture_item_revisions
                           (id_empresa, id_filial, capture_id, capture_item_id, station_id, capture_date,
                            id_produto, product_name_snapshot, fuel_type_snapshot,
                            revision_number, action_type, old_price, new_price,
                            changed_by_user_id, changed_by_user_name_snapshot, changed_at,
                            client_ip, user_agent)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'UPDATE_PRICE', %s, %s, %s, %s, %s, %s, %s)""",
                    [
                        id_empresa, id_filial, capture_id, existing_item["id"], station_id, capture_date,
                        product_id, product_name, fuel_type,
                        new_rev, old_price, new_price,
                        user_id, user_name, now,
                        client_ip, user_agent,
                    ],
                )
                updated_count += 1
            else:
                # Create new item
                item_row = conn.execute(
                    """INSERT INTO app.competitor_price_capture_items
                           (id_empresa, id_filial, capture_id, station_id, capture_date,
                            id_produto, product_name_snapshot, fuel_type_snapshot,
                            original_price, current_price,
                            original_registered_by_user_id, original_registered_by_user_name_snapshot,
                            original_registered_at, revision_number)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                       RETURNING id""",
                    [
                        id_empresa, id_filial, capture_id, station_id, capture_date,
                        product_id, product_name, fuel_type,
                        new_price, new_price,
                        user_id, user_name, now,
                    ],
                ).fetchone()
                # Record initial revision
                conn.execute(
                    """INSERT INTO app.competitor_price_capture_item_revisions
                           (id_empresa, id_filial, capture_id, capture_item_id, station_id, capture_date,
                            id_produto, product_name_snapshot, fuel_type_snapshot,
                            revision_number, action_type, old_price, new_price,
                            changed_by_user_id, changed_by_user_name_snapshot, changed_at,
                            client_ip, user_agent)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 'CREATE', NULL, %s, %s, %s, %s, %s, %s)""",
                    [
                        id_empresa, id_filial, capture_id, item_row["id"], station_id, capture_date,
                        product_id, product_name, fuel_type,
                        new_price,
                        user_id, user_name, now,
                        client_ip, user_agent,
                    ],
                )
                created_count += 1

        conn.commit()

    return {
        "capture_id": str(capture_id),
        "station_id": station_id,
        "capture_date": capture_date.isoformat(),
        "created": created_count,
        "updated": updated_count,
    }


def list_captures(
    role: str,
    id_empresa: int,
    id_filial: int,
    *,
    station_id: Optional[str] = None,
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT c.id, c.station_id, s.station_name,
               c.capture_date, c.captured_at, c.status,
               c.registered_by_user_name_snapshot AS registered_by,
               c.registered_at,
               c.last_updated_by_user_name_snapshot AS last_updated_by,
               c.last_updated_at,
               c.observation,
               (SELECT count(*) FROM app.competitor_price_capture_items i WHERE i.capture_id = c.id AND i.is_active) AS item_count
        FROM app.competitor_price_captures c
        JOIN app.competitor_stations s ON s.id = c.station_id
        WHERE c.id_empresa = %s AND c.id_filial = %s AND c.status <> 'DELETED'
    """
    params: list = [id_empresa, id_filial]
    if station_id:
        sql += " AND c.station_id = %s"
        params.append(station_id)
    if dt_ini:
        sql += " AND c.capture_date >= %s"
        params.append(dt_ini)
    if dt_fim:
        sql += " AND c.capture_date <= %s"
        params.append(dt_fim)
    sql += " ORDER BY c.capture_date DESC, s.station_name"

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_capture_detail(
    role: str,
    id_empresa: int,
    id_filial: int,
    capture_id: str,
) -> Optional[Dict[str, Any]]:
    """Get capture header + items + revision history."""
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cap = conn.execute(
            """SELECT c.id, c.station_id, s.station_name,
                      c.capture_date, c.captured_at, c.status,
                      c.registered_by_user_name_snapshot AS registered_by,
                      c.registered_at,
                      c.last_updated_by_user_name_snapshot AS last_updated_by,
                      c.last_updated_at,
                      c.observation
               FROM app.competitor_price_captures c
               JOIN app.competitor_stations s ON s.id = c.station_id
               WHERE c.id = %s AND c.id_empresa = %s AND c.id_filial = %s""",
            [capture_id, id_empresa, id_filial],
        ).fetchone()
        if not cap:
            return None

        items = conn.execute(
            """SELECT id, id_produto, product_name_snapshot AS product_name,
                      fuel_type_snapshot AS fuel_type,
                      original_price, current_price, revision_number,
                      original_registered_by_user_name_snapshot AS original_registered_by,
                      original_registered_at,
                      last_updated_by_user_name_snapshot AS last_updated_by,
                      last_updated_at,
                      is_active
               FROM app.competitor_price_capture_items
               WHERE capture_id = %s
               ORDER BY product_name_snapshot""",
            [capture_id],
        ).fetchall()

        revisions = conn.execute(
            """SELECT r.id, r.capture_item_id, r.id_produto,
                      r.product_name_snapshot AS product_name,
                      r.revision_number, r.action_type,
                      r.old_price, r.new_price,
                      r.changed_by_user_name_snapshot AS changed_by,
                      r.changed_at, r.change_reason
               FROM app.competitor_price_capture_item_revisions r
               WHERE r.capture_id = %s
               ORDER BY r.changed_at DESC""",
            [capture_id],
        ).fetchall()

        result = dict(cap)
        result["items"] = [dict(i) for i in items]
        result["revisions"] = [dict(r) for r in revisions]
        return result


def delete_capture(
    role: str,
    id_empresa: int,
    id_filial: int,
    capture_id: str,
    *,
    user_id: str,
    user_name: str,
) -> bool:
    sql = """
        UPDATE app.competitor_price_captures
        SET status = 'DELETED',
            last_updated_by_user_id = %s, last_updated_by_user_name_snapshot = %s,
            last_updated_at = now(), updated_at = now()
        WHERE id = %s AND id_empresa = %s AND id_filial = %s AND status <> 'DELETED'
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(sql, [user_id, user_name, capture_id, id_empresa, id_filial])
        if cur.rowcount > 0:
            # Deactivate items
            conn.execute(
                "UPDATE app.competitor_price_capture_items SET is_active = false, updated_at = now() WHERE capture_id = %s",
                [capture_id],
            )
        conn.commit()
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Fuel products listing (for the capture form)
# ---------------------------------------------------------------------------

def list_fuel_products(
    role: str,
    id_empresa: int,
    id_filial: int,
) -> List[Dict[str, Any]]:
    """Return fuel products available for competitor pricing.

    Uses the same fuel detection logic as repos_mart (CANONICAL_GROUP_COMBUSTIVEIS).
    """
    from app.repos_mart import _fuel_family_case_expression, CANONICAL_GROUP_COMBUSTIVEIS

    fuel_filter = _build_fuel_filter()
    sql = f"""
        SELECT p.id_produto,
               COALESCE(NULLIF(p.nome, ''), '#ID ' || p.id_produto::text) AS nome,
               COALESCE(g.nome, '') AS grupo_nome,
               {_fuel_family_case_expression("g", "p")} AS fuel_type,
               p.unidade,
               COALESCE(p.custo_medio, 0)::numeric(18,4) AS custo_medio
        FROM dw.dim_produto p
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = p.id_empresa
         AND g.id_filial = p.id_filial
         AND g.id_grupo_produto = p.id_grupo_produto
        WHERE p.id_empresa = %s
          AND p.id_filial = %s
          AND {fuel_filter}
          AND COALESCE(p.is_active, true)
        ORDER BY p.nome
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        rows = conn.execute(sql, [id_empresa, id_filial]).fetchall()
        return [dict(r) for r in rows]


def _build_fuel_filter() -> str:
    """Build SQL WHERE clause for fuel products (same logic as repos_mart)."""
    from app.repos_mart import CANONICAL_GROUP_COMBUSTIVEIS
    group_patterns = " OR ".join(
        f"UPPER(COALESCE(g.nome,'')) LIKE '%{g}%'" for g in CANONICAL_GROUP_COMBUSTIVEIS
    )
    return f"""(
        ({group_patterns})
        AND UPPER(COALESCE(p.unidade,'')) IN ('LT','L','LITRO','LITROS','M3')
        AND UPPER(COALESCE(g.nome,'')) NOT LIKE '%FILTRO%'
        AND UPPER(COALESCE(g.nome,'')) NOT LIKE '%OLEO%'
        AND UPPER(COALESCE(g.nome,'')) NOT LIKE '%LUBR%'
    )"""
