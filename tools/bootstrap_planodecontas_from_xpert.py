#!/usr/bin/env python3
"""Bootstrap one-shot: dbo.PLANODECONTAS (Xpert → stg.planodecontas).

Uso (homolog):
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/homolog.app.env; set +a
  .venv/bin/python tools/bootstrap_planodecontas_from_xpert.py --id-empresa 1 --pg-database torqmind_homolog

Não imprime senhas. Idempotente (UPSERT PK id_empresa+id_filial+id_planodecontas).
Não deleta linhas.
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


def upsert_planos(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    n = 0
    sql = """
      INSERT INTO stg.planodecontas AS t
        (id_empresa, id_filial, id_planodecontas, payload, dt_evento,
         id_db_shadow, id_chave_natural, ingested_at, received_at, updated_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s, now(), now(), now())
      ON CONFLICT (id_empresa, id_filial, id_planodecontas) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            id_db_shadow = EXCLUDED.id_db_shadow,
            id_chave_natural = EXCLUDED.id_chave_natural,
            ingested_at = now(),
            updated_at = now()
    """
    for r in rows:
        fid = int(r["ID_FILIAL"])
        pid = int(r["ID_PLANODECONTAS"])
        payload = _jsonable(r)
        dt = r.get("DATAREPL")
        cur.execute(
            sql,
            (
                id_empresa,
                fid,
                pid,
                Jsonb(payload),
                dt,
                fid,  # id_db_shadow: plano é por filial
                f"{fid}:{pid}",
            ),
        )
        n += 1
        if n % 2000 == 0:
            print(f"  upserted {n}...", flush=True)
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", default="torqmind_homolog")
    ap.add_argument("--id-filial", type=int, default=None, help="Opcional: só uma filial")
    args = ap.parse_args()

    print(f"extract PLANODECONTAS → {args.pg_database} empresa={args.id_empresa}")
    mssql = _mssql()
    cur_m = mssql.cursor()
    if args.id_filial:
        cur_m.execute(
            "SELECT * FROM dbo.PLANODECONTAS WHERE ID_FILIAL=%s ORDER BY ID_FILIAL, ID_PLANODECONTAS",
            (args.id_filial,),
        )
    else:
        cur_m.execute("SELECT * FROM dbo.PLANODECONTAS ORDER BY ID_FILIAL, ID_PLANODECONTAS")
    rows = cur_m.fetchall()
    mssql.close()
    print(f"xpert rows={len(rows)}")

    pg = _pg(args.pg_database)
    try:
        with pg.cursor() as cur:
            n = upsert_planos(cur, args.id_empresa, rows)
        pg.commit()
        print(f"OK upserted={n}")
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()


if __name__ == "__main__":
    main()
