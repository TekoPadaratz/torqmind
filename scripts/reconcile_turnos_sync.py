#!/usr/bin/env python3
"""Catch-up missing TURNOS from Xpert into TorqMind STG (+ optional ClickHouse).

Root cause
----------
The agent watermark for turnos is **temporal** (MAX of DATA/DATATURNO/
DATAFECHAMENTO). After a successful batch the local cursor advances; any
``ID_TURNOS`` that failed the first delivery (or never entered pending) and is
already closed + outside the revisit window disappears from the incremental
forever. Comprovantes/aferições then reference a turno that never landed in
``stg.turnos`` → mart joins yield ``turno_numero=0`` / "Turno não resolvido".

This script is the **server-side safety net** (same role as
``reconcile_contasreceber_sync``). Cron: ``deploy/scripts/prod-reconcile-turnos.sh``.

What it heals
-------------
1. Orphan FK refs: ``(id_filial, id_turno)`` on recent comprovantes + aferições
   missing from ``stg.turnos`` (default: all comprovantes, not only cancelled).
2. Xpert window gap: every ``dbo.TURNOS`` row in the last ``--since-days`` whose
   PK is absent from STG (covers IDs never referenced yet).
3. Upsert PG ``stg.turnos``; optional direct CH ``stg_turnos`` (don't wait CDC).

Usage
-----
    set -a; source /etc/torqmind/prod.app.env; set +a
    python scripts/reconcile_turnos_sync.py \\
        --sqlserver-env-file config/source-explorer.env --id-empresa 1 \\
        --since-days 60 --also-clickhouse
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("reconcile_turnos")


def _load_env_file(path: str) -> None:
    if not path or not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def _mssql_config() -> dict[str, Any]:
    host = os.getenv("SQLSERVER_HOST", "")
    user = os.getenv("SQLSERVER_USER", "")
    password = os.getenv("SQLSERVER_PASSWORD", "")
    database = os.getenv("SQLSERVER_DATABASE", "")
    if not (host and user and database):
        log.error("Missing SQLSERVER_* env. Pass --sqlserver-env-file.")
        sys.exit(2)
    return {
        "server": host,
        "port": int(os.getenv("SQLSERVER_PORT", "1433")),
        "user": user,
        "password": password,
        "database": database,
        "timeout": int(os.getenv("SQLSERVER_TIMEOUT_SECONDS", "120")),
        "login_timeout": int(os.getenv("SQLSERVER_TIMEOUT_SECONDS", "30")),
    }


def _pg_dsn() -> dict[str, Any]:
    host = os.getenv("PG_HOST") or os.getenv("POSTGRES_HOST", "")
    if not host:
        log.error("Missing PG_HOST/POSTGRES_HOST.")
        sys.exit(2)
    return {
        "host": host,
        "port": int(os.getenv("PG_PORT") or os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("PG_DATABASE") or os.getenv("POSTGRES_DB", "torqmind"),
        "user": os.getenv("PG_USER") or os.getenv("POSTGRES_USER", "torqmind"),
        "password": os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD", ""),
    }


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    return str(obj)


def _missing_pairs(
    pg, id_empresa: int, since_days: int, *, cancelled_only: bool = False
) -> list[tuple[int, int]]:
    """Turnos referenciados (comprovantes + aferições) ausentes em stg.turnos."""
    cancel_filter = ""
    if cancelled_only:
        cancel_filter = """
        AND (
          COALESCE(c.cancelado_shadow, false)
          OR lower(COALESCE(c.payload->>'CANCELADO', '')) IN ('true', 't', '1')
        )
        """
    sql = f"""
      WITH refs AS (
        SELECT DISTINCT
          c.id_filial,
          COALESCE(
            NULLIF(c.id_turno_shadow, 0),
            NULLIF((c.payload->>'ID_TURNOS')::int, 0),
            NULLIF((c.payload->>'ID_TURNO')::int, 0)
          ) AS id_turno
        FROM stg.comprovantes c
        WHERE c.id_empresa = %s
          AND COALESCE(
                NULLIF(c.id_turno_shadow, 0),
                NULLIF((c.payload->>'ID_TURNOS')::int, 0),
                NULLIF((c.payload->>'ID_TURNO')::int, 0)
              ) > 0
          AND COALESCE(c.dt_evento, c.received_at, c.ingested_at)
                >= now() - (%s || ' days')::interval
          {cancel_filter}
        UNION
        SELECT DISTINCT
          a.id_filial,
          NULLIF((a.payload->>'ID_TURNOS')::int, 0) AS id_turno
        FROM stg.afericoes a
        WHERE a.id_empresa = %s
          AND NULLIF((a.payload->>'ID_TURNOS')::int, 0) > 0
          AND COALESCE(
                a.dt_evento,
                etl.safe_timestamp(a.payload->>'DATA'),
                a.received_at,
                a.ingested_at
              ) >= now() - (%s || ' days')::interval
      )
      SELECT r.id_filial, r.id_turno
      FROM refs r
      LEFT JOIN stg.turnos t
        ON t.id_empresa = %s
       AND t.id_filial = r.id_filial
       AND t.id_turno = r.id_turno
      WHERE r.id_turno IS NOT NULL
        AND t.id_turno IS NULL
      ORDER BY 1, 2
      LIMIT 8000
    """
    with pg.cursor() as cur:
        cur.execute(sql, (id_empresa, since_days, id_empresa, since_days, id_empresa))
        rows = cur.fetchall()
    return [(int(r[0]), int(r[1])) for r in rows if r[1]]


def _stg_turno_keys(pg, id_empresa: int, pairs: list[tuple[int, int]]) -> set[tuple[int, int]]:
    """Subset of ``pairs`` that already exist in stg.turnos."""
    if not pairs:
        return set()
    found: set[tuple[int, int]] = set()
    chunk = 500
    with pg.cursor() as cur:
        for i in range(0, len(pairs), chunk):
            part = pairs[i : i + chunk]
            values = ",".join(f"({f},{t})" for f, t in part)
            cur.execute(
                f"""
                SELECT id_filial, id_turno
                FROM stg.turnos
                WHERE id_empresa = %s
                  AND (id_filial, id_turno) IN ({values})
                """,
                (id_empresa,),
            )
            found.update((int(r[0]), int(r[1])) for r in cur.fetchall())
    return found


def _xpert_recent_keys(mssql, since_days: int, *, limit: int = 20000) -> list[tuple[int, int]]:
    """All (ID_FILIAL, ID_TURNOS) in Xpert inside the operational window."""
    import pymssql

    days = max(1, min(int(since_days), 180))
    lim = max(100, min(int(limit), 50000))
    conn = pymssql.connect(**mssql, as_dict=True)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT TOP {lim}
              t.ID_FILIAL AS id_filial,
              t.ID_TURNOS AS id_turno
            FROM dbo.TURNOS t
            WHERE COALESCE(t.DATA, t.DATATURNO, t.DATAFECHAMENTO)
                    >= DATEADD(day, -%s, GETDATE())
              AND t.ID_TURNOS IS NOT NULL
              AND t.ID_FILIAL IS NOT NULL
            ORDER BY t.ID_TURNOS DESC
            """,
            (days,),
        )
        rows = cur.fetchall() or []
    finally:
        conn.close()
    out: list[tuple[int, int]] = []
    for r in rows:
        try:
            out.append((int(r["id_filial"]), int(r["id_turno"])))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _missing_from_xpert_window(
    pg, mssql, id_empresa: int, since_days: int
) -> list[tuple[int, int]]:
    """Xpert turnos in window whose PK is absent from STG (independent of refs)."""
    xpert_keys = _xpert_recent_keys(mssql, since_days)
    if not xpert_keys:
        return []
    existing = _stg_turno_keys(pg, id_empresa, xpert_keys)
    missing = [k for k in xpert_keys if k not in existing]
    log.info(
        "xpert_window_keys=%s stg_hit=%s xpert_gap=%s since_days=%s",
        len(xpert_keys),
        len(existing),
        len(missing),
        since_days,
    )
    return missing[:8000]


def _fetch_xpert_turnos(mssql, pairs: list[tuple[int, int]]) -> list[dict[str, Any]]:
    if not pairs:
        return []
    import pymssql

    conn = pymssql.connect(**mssql, as_dict=True)
    cur = conn.cursor()
    out: list[dict[str, Any]] = []
    chunk = 200
    for i in range(0, len(pairs), chunk):
        part = pairs[i : i + chunk]
        values = ",".join(f"({f},{t})" for f, t in part)
        cur.execute(
            f"""
            SELECT t.*
            FROM dbo.TURNOS t
            INNER JOIN (VALUES {values}) AS v(ID_FILIAL, ID_TURNOS)
              ON t.ID_FILIAL = v.ID_FILIAL AND t.ID_TURNOS = v.ID_TURNOS
            """
        )
        out.extend(cur.fetchall() or [])
    conn.close()
    return out


def _upsert_pg(pg, id_empresa: int, rows: list[dict[str, Any]], dry_run: bool) -> int:
    if dry_run or not rows:
        return 0
    sql = """
      INSERT INTO stg.turnos (
        id_empresa, id_filial, id_turno, payload, ingested_at, dt_evento, received_at
      ) VALUES (
        %s, %s, %s, %s::jsonb, now(), %s, now()
      )
      ON CONFLICT (id_empresa, id_filial, id_turno) DO UPDATE SET
        payload = EXCLUDED.payload,
        ingested_at = now(),
        dt_evento = COALESCE(EXCLUDED.dt_evento, stg.turnos.dt_evento),
        received_at = now()
    """
    n = 0
    with pg.cursor() as cur:
        for r in rows:
            id_filial = int(r.get("ID_FILIAL") or 0)
            id_turno = int(r.get("ID_TURNOS") or r.get("ID_TURNO") or 0)
            if id_filial <= 0 or id_turno <= 0:
                continue
            dt_evento = r.get("DATA") or r.get("DATATURNO") or r.get("DATAFECHAMENTO")
            payload = json.dumps(r, default=_json_default, ensure_ascii=False)
            cur.execute(sql, (id_empresa, id_filial, id_turno, payload, dt_evento))
            n += 1
    pg.commit()
    return n


def _upsert_ch(rows: list[dict[str, Any]], id_empresa: int) -> int:
    host = os.getenv("CLICKHOUSE_HOST", "")
    port = os.getenv("CLICKHOUSE_PORT", "8123")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    if not host or not rows:
        return 0
    import urllib.parse
    import urllib.request

    # ClickHouse JSONEachRow insert
    lines = []
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    for r in rows:
        id_filial = int(r.get("ID_FILIAL") or 0)
        id_turno = int(r.get("ID_TURNOS") or r.get("ID_TURNO") or 0)
        if id_filial <= 0 or id_turno <= 0:
            continue
        payload = json.dumps(r, default=_json_default, ensure_ascii=False)
        lines.append(
            json.dumps(
                {
                    "id_empresa": id_empresa,
                    "id_filial": id_filial,
                    "id_turno": id_turno,
                    "payload": payload,
                    "is_deleted": 0,
                    "source_ts_ms": now_ms,
                },
                ensure_ascii=False,
            )
        )
    if not lines:
        return 0
    body = ("\n".join(lines) + "\n").encode("utf-8")
    q = urllib.parse.urlencode(
        {
            "query": "INSERT INTO torqmind_current.stg_turnos FORMAT JSONEachRow",
            "user": user,
            "password": password,
        }
    )
    url = f"http://{host}:{port}/?{q}"
    req = urllib.request.Request(url, data=body, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        resp.read()
    return len(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sqlserver-env-file", default="")
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--since-days", type=int, default=60)
    ap.add_argument("--also-clickhouse", action="store_true")
    ap.add_argument(
        "--all-comprovantes",
        action="store_true",
        default=True,
        help="Heal turnos referenced by any recent comprovante (default: on)",
    )
    ap.add_argument(
        "--cancelled-only",
        action="store_true",
        help="Restrict orphan-FK scan to cancelled comprovantes only",
    )
    ap.add_argument(
        "--skip-xpert-window",
        action="store_true",
        help="Skip Xpert→STG window gap scan (refs-only)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    _load_env_file(args.sqlserver_env_file)
    import psycopg

    cancelled_only = bool(args.cancelled_only) or not bool(args.all_comprovantes)
    mssql = _mssql_config()
    with psycopg.connect(**_pg_dsn()) as pg:
        missing_refs = _missing_pairs(
            pg,
            args.id_empresa,
            args.since_days,
            cancelled_only=cancelled_only,
        )
        missing_xpert: list[tuple[int, int]] = []
        if not args.skip_xpert_window:
            missing_xpert = _missing_from_xpert_window(
                pg, mssql, args.id_empresa, args.since_days
            )
        missing = sorted(set(missing_refs) | set(missing_xpert))
        log.info(
            "missing_turnos=%s (refs=%s xpert_gap=%s) since_days=%s cancelled_only=%s",
            len(missing),
            len(missing_refs),
            len(missing_xpert),
            args.since_days,
            cancelled_only,
        )
        if not missing:
            return 0
        log.info("sample_missing=%s", missing[:15])
        xpert = _fetch_xpert_turnos(mssql, missing)
        log.info("xpert_rows=%s (of %s missing keys)", len(xpert), len(missing))
        if args.dry_run:
            log.info("dry_run — not writing")
            return 0
        n_pg = _upsert_pg(pg, args.id_empresa, xpert, dry_run=False)
        log.info("upserted_pg=%s", n_pg)
        if args.also_clickhouse:
            n_ch = _upsert_ch(xpert, args.id_empresa)
            log.info("upserted_ch=%s", n_ch)
        still = _missing_pairs(
            pg,
            args.id_empresa,
            args.since_days,
            cancelled_only=cancelled_only,
        )
        log.info("still_missing_refs_after=%s", len(still))
        if still:
            log.warning("still_missing_sample=%s", still[:20])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
