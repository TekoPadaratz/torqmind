#!/usr/bin/env python3
"""Bootstrap one-shot: DESCONTOSENTIDADESITENS (Xpert → STG PG).

Uso (homolog):
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/homolog.app.env; set +a
  .venv/bin/python tools/bootstrap_descontos_entidades_itens.py --id-empresa 1 --pg-database torqmind_homolog

Idempotente (UPSERT pela PK STG).
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
      INSERT INTO stg.descontos_entidades_itens AS t
        (id_empresa, id_filial, id_descontoentidadesitens, payload,
         id_db_shadow, id_chave_natural, dt_evento, ingested_at, received_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (id_empresa, id_filial, id_descontoentidadesitens) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            ingested_at = now()
    """
    for r in rows:
        fid = int(r["ID_FILIAL"])
        did = int(r["ID_DESCONTOENTIDADESITENS"])
        dt = r.get("DATAREPL")
        cur.execute(
            sql,
            (
                id_empresa,
                fid,
                did,
                Jsonb(_jsonable(r)),
                fid,
                f"{fid}:{did}",
                dt,
            ),
        )
        n += 1
        if n % 2000 == 0:
            print(f"  upserted {n}…", flush=True)
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", required=True)
    ap.add_argument(
        "--only-fixo",
        action="store_true",
        help="Importa só VALORFIXO=1 (recomendado para esta feature)",
    )
    args = ap.parse_args()

    print("connecting Xpert…", flush=True)
    mss = _mssql()
    mcur = mss.cursor(as_dict=True)

    if args.only_fixo:
        print("loading DESCONTOSENTIDADESITENS WHERE VALORFIXO=1…", flush=True)
        mcur.execute(
            """
            SELECT *
            FROM dbo.DESCONTOSENTIDADESITENS
            WHERE ISNULL(VALORFIXO, 0) = 1
            """
        )
    else:
        print("loading DESCONTOSENTIDADESITENS (full)…", flush=True)
        mcur.execute("SELECT * FROM dbo.DESCONTOSENTIDADESITENS")
    rows = list(mcur.fetchall())
    print(f"  {len(rows)} rows", flush=True)
    mss.close()

    print(f"upserting into {args.pg_database}.stg.descontos_entidades_itens…", flush=True)
    pg = _pg(args.pg_database)
    with pg.cursor() as cur:
        n = upsert_rows(cur, args.id_empresa, rows)
    pg.commit()
    pg.close()
    print(f"OK upserted={n}", flush=True)


if __name__ == "__main__":
    main()
