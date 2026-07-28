#!/usr/bin/env python3
"""Catch-up missing TURNOS from Xpert into TorqMind STG (+ optional ClickHouse).

Root cause
----------
Cancelled receipts in ``stg.comprovantes`` often reference ``ID_TURNOS`` that
never landed in ``stg.turnos`` (agent watermark gap / open-shift revisit). The
antifraud mart then joins to an empty turno → ``turno_numero=0`` and the UI used
to hide those rows while ``fraud_daily`` still counted them.

This script:
1. Finds (id_filial, id_turno) present on recent cancelled comprovantes but
   missing from ``stg.turnos``.
2. Pulls those rows from Xpert ``dbo.TURNOS``.
3. Upserts ``stg.turnos`` (payload jsonb).
4. Optionally upserts ``torqmind_current.stg_turnos`` so mart refresh can run
   without waiting for Debezium.

Usage
-----
    set -a; source /etc/torqmind/prod.app.env; set +a
    python scripts/fix_turnos_sync.py \\
        --sqlserver-env-file config/source-explorer.env --id-empresa 1 \\
        --since-days 45 --also-clickhouse
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
    pg, id_empresa: int, since_days: int, *, cancelled_only: bool = True
) -> list[tuple[int, int]]:
    """Turnos referenced by recent comprovantes but absent from stg.turnos."""
    cancel_filter = ""
    if cancelled_only:
        cancel_filter = """
        AND (
          COALESCE(c.cancelado_shadow, false)
          OR lower(COALESCE(c.payload->>'CANCELADO', '')) IN ('true', 't', '1')
        )
        """
    sql = f"""
      SELECT DISTINCT
        c.id_filial,
        COALESCE(
          NULLIF(c.id_turno_shadow, 0),
          NULLIF((c.payload->>'ID_TURNOS')::int, 0),
          NULLIF((c.payload->>'ID_TURNO')::int, 0)
        ) AS id_turno
      FROM stg.comprovantes c
      LEFT JOIN stg.turnos t
        ON t.id_empresa = c.id_empresa
       AND t.id_filial = c.id_filial
       AND t.id_turno = COALESCE(
             NULLIF(c.id_turno_shadow, 0),
             NULLIF((c.payload->>'ID_TURNOS')::int, 0),
             NULLIF((c.payload->>'ID_TURNO')::int, 0)
           )
      WHERE c.id_empresa = %s
        AND COALESCE(
              NULLIF(c.id_turno_shadow, 0),
              NULLIF((c.payload->>'ID_TURNOS')::int, 0),
              NULLIF((c.payload->>'ID_TURNO')::int, 0)
            ) > 0
        AND t.id_turno IS NULL
        AND COALESCE(c.dt_evento, c.received_at, c.ingested_at)
              >= now() - (%s || ' days')::interval
        {cancel_filter}
      ORDER BY 1, 2
      LIMIT 8000
    """
    with pg.cursor() as cur:
        cur.execute(sql, (id_empresa, since_days))
        rows = cur.fetchall()
    return [(int(r[0]), int(r[1])) for r in rows if r[1]]


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
    ap.add_argument("--since-days", type=int, default=45)
    ap.add_argument("--also-clickhouse", action="store_true")
    ap.add_argument(
        "--all-comprovantes",
        action="store_true",
        help="Also heal turnos referenced by non-cancelled recent comprovantes",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    _load_env_file(args.sqlserver_env_file)
    import psycopg

    mssql = _mssql_config()
    with psycopg.connect(**_pg_dsn()) as pg:
        missing = _missing_pairs(
            pg,
            args.id_empresa,
            args.since_days,
            cancelled_only=not args.all_comprovantes,
        )
        log.info("missing_turnos=%s since_days=%s", len(missing), args.since_days)
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
            cancelled_only=not args.all_comprovantes,
        )
        log.info("still_missing_after=%s", len(still))
        if still:
            log.warning("still_missing_sample=%s", still[:20])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
