#!/usr/bin/env python3
"""Bootstrap MOVLCTOS de despesas (3.2*/3.3*) Xpert → stg.movlctos.

Fonte canônica do DRE Xpert (docs/product/dre_referencia_xpert.md + XPERT_DRE_DESPESAS_MAP.md).

Uso (homolog):
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/homolog.app.env; set +a
  .venv/bin/python tools/bootstrap_movlctos_despesas_from_xpert.py \\
    --id-empresa 1 --pg-database torqmind_homolog --from-date 2026-01-01

Não deleta. Idempotente (UPSERT PK).
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import pymssql
import psycopg
from psycopg.types.json import Jsonb


def _env(name: str, default: Optional[str] = None) -> str:
    val = os.environ.get(name, default)
    if not val:
        raise SystemExit(f"missing env {name}")
    return val


def _mssql():
    return pymssql.connect(
        server=_env("SQLSERVER_HOST"),
        port=int(os.environ.get("SQLSERVER_PORT") or 1433),
        user=_env("SQLSERVER_USER"),
        password=_env("SQLSERVER_PASSWORD"),
        database=_env("SQLSERVER_DATABASE"),
        login_timeout=30,
        timeout=600,
        as_dict=True,
    )


def _pg(database: str):
    host = os.environ.get("PG_HOST") or os.environ.get("POSTGRES_HOST")
    port = os.environ.get("PG_PORT") or os.environ.get("POSTGRES_PORT") or "5432"
    user = os.environ.get("PG_USER") or os.environ.get("POSTGRES_USER")
    pwd = os.environ.get("PG_PASSWORD") or os.environ.get("POSTGRES_PASSWORD")
    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={user} password={pwd}",
        autocommit=False,
    )


def _jsonable(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat(sep=" ", timespec="seconds")
        elif isinstance(v, bool):
            out[k] = v
        elif isinstance(v, int):
            out[k] = v
        elif hasattr(v, "as_integer_ratio") and not isinstance(v, bool):
            try:
                if v == v.to_integral_value():
                    out[k] = int(v)
                else:
                    out[k] = float(v)
            except Exception:
                out[k] = float(v)
        else:
            out[k] = v
    return out


def upsert_rows(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    n = 0
    sql = """
      INSERT INTO stg.movlctos AS t
        (id_empresa, id_filial, id_db, id_movlctos, payload, dt_evento,
         id_db_shadow, id_chave_natural, ingested_at, received_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (id_empresa, id_filial, id_db, id_movlctos) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            ingested_at = now(),
            received_at = now()
    """
    batch: list[tuple] = []
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        mid = int(r["ID_MOVLCTOS"])
        dt = r.get("DTACONTA") or r.get("DATA")
        batch.append(
            (
                id_empresa,
                fid,
                id_db,
                mid,
                Jsonb(_jsonable(r)),
                dt,
                id_db,
                f"{fid}:{id_db}:{mid}",
            )
        )
        n += 1
        if len(batch) >= 2000:
            cur.executemany(sql, batch)
            print(f"  upserted {n}...", flush=True)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", default="torqmind_homolog")
    ap.add_argument("--from-date", default="2026-01-01")
    ap.add_argument("--id-filial", type=int, default=None)
    args = ap.parse_args()

    print(f"extract MOVLCTOS despesas 3.2*/3.3* from={args.from_date} → {args.pg_database}")
    mssql = _mssql()
    cur_m = mssql.cursor()
    sql = """
      SELECT m.*
      FROM dbo.MOVLCTOS m
      JOIN dbo.PLANODECONTAS p
        ON p.ID_PLANODECONTAS = m.ID_PLANODECONTAS
       AND p.ID_FILIAL = m.ID_FILIAL
      WHERE m.DTACONTA >= %s
        AND (p.CODIGOPLANODECONTAS LIKE '3.2.%%' OR p.CODIGOPLANODECONTAS LIKE '3.3.%%')
    """
    params: list[Any] = [args.from_date]
    if args.id_filial is not None:
        sql += " AND m.ID_FILIAL = %s"
        params.append(args.id_filial)
    sql += " ORDER BY m.ID_FILIAL, m.ID_DB, m.ID_MOVLCTOS"
    cur_m.execute(sql, tuple(params))
    rows = cur_m.fetchall()
    mssql.close()
    print(f"xpert rows={len(rows)}")

    pg = _pg(args.pg_database)
    try:
        with pg.cursor() as cur:
            n = upsert_rows(cur, args.id_empresa, rows)
        pg.commit()
        print(f"OK upserted={n}")
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()


if __name__ == "__main__":
    main()
