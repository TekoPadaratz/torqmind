#!/usr/bin/env python3
"""Cura STG contaspagar a partir do Xpert (Não Pagas + pagos recentes).

Problema: DTAPGTO/VLRPAGO mudam no Xpert sem bump de DATAREPL. Se o agent
atrasa/para, a STG fica com fantasma "em aberto" e a tela TorqMind infla
contas a pagar vs o Xpert (filtro Não Pagas = DTAPGTO IS NULL).

Uso (prod):
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/prod.app.env; set +a
  .venv/bin/python tools/heal_contaspagar_from_xpert.py \\
    --id-empresa 1 --pg-database torqmind --id-filial 14458 --paid-days 180

Não deleta. UPSERT idempotente pela PK.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pymssql
import psycopg
from psycopg.types.json import Jsonb


def _env(name: str, default: Optional[str] = None) -> str:
    val = os.environ.get(name, default)
    if not val:
        raise SystemExit(f"missing env {name}")
    return val


def _load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if not key or not key.replace("_", "").isalnum() or key[0].isdigit():
                continue
            if key and key not in os.environ:
                os.environ[key] = value


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
    if not host or not user or not pwd:
        raise SystemExit("missing PG_HOST/PG_USER/PG_PASSWORD")
    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={user} password={pwd}",
        autocommit=False,
    )


def _jsonable(row: Dict[str, Any]) -> Dict[str, Any]:
    """Serializa row Xpert para JSONB sem IDs floatish (ex.: 282384.0).

    pymssql devolve float para várias colunas numéricas. Se gravarmos 282384.0,
    ``etl.safe_int`` antigo e ``toInt32OrZero`` no CH quebram o join das baixas.
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat(sep=" ", timespec="seconds")
        elif isinstance(v, bool):
            out[k] = v
        elif isinstance(v, int) and not isinstance(v, bool):
            out[k] = v
        elif isinstance(v, float):
            out[k] = int(v) if v.is_integer() else v
        elif hasattr(v, "as_integer_ratio") and not isinstance(v, bool):
            # Decimal
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


def upsert_contaspagar(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    sql = """
      INSERT INTO stg.contaspagar AS t
        (id_empresa, id_filial, id_db, id_contaspagar, payload, dt_evento,
         id_db_shadow, id_chave_natural, ingested_at, received_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (id_empresa, id_filial, id_db, id_contaspagar) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            ingested_at = now(),
            received_at = now()
    """
    n = 0
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        cid = int(r["ID_CONTASPAGAR"])
        dt = r.get("DTACONTA") or r.get("DTAVCTO") or r.get("DTAPGTO")
        cur.execute(
            sql,
            (
                id_empresa,
                fid,
                id_db,
                cid,
                Jsonb(_jsonable(r)),
                dt,
                id_db,
                f"{fid}:{id_db}:{cid}",
            ),
        )
        n += 1
        if n % 2000 == 0:
            print(f"  contaspagar upserted {n}...", flush=True)
    return n


def upsert_baixas(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    sql = """
      INSERT INTO stg.contaspagarbaixa AS t
        (id_empresa, id_filial, id_db, id_contaspagarbaixa, payload, dt_evento,
         id_db_shadow, id_chave_natural, ingested_at, received_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (id_empresa, id_filial, id_db, id_contaspagarbaixa) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            ingested_at = now(),
            received_at = now()
    """
    n = 0
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        bid = int(r["ID_CONTASPAGARBAIXA"])
        dt = r.get("DATABAIXA") or r.get("DATAREPL")
        cur.execute(
            sql,
            (
                id_empresa,
                fid,
                id_db,
                bid,
                Jsonb(_jsonable(r)),
                dt,
                id_db,
                f"{fid}:{id_db}:{bid}",
            ),
        )
        n += 1
        if n % 2000 == 0:
            print(f"  contaspagarbaixa upserted {n}...", flush=True)
    return n


def fetch_xpert_titles(id_filial: Optional[int], paid_days: int) -> List[Dict[str, Any]]:
    """Não Pagas (DTAPGTO IS NULL) + pagos recentes (janela paid_days)."""
    mssql = _mssql()
    try:
        cur = mssql.cursor()
        sql = f"""
          SELECT *
          FROM dbo.CONTASPAGAR
          WHERE (
              DTAPGTO IS NULL
              OR CAST(DTAPGTO AS date) >= CAST(DATEADD(day, -{int(paid_days)}, GETDATE()) AS date)
          )
        """
        params: list[Any] = []
        if id_filial is not None:
            sql += " AND ID_FILIAL = %s"
            params.append(int(id_filial))
        sql += " ORDER BY ID_FILIAL, ID_DB, ID_CONTASPAGAR"
        cur.execute(sql, tuple(params) if params else None)
        return list(cur.fetchall() or [])
    finally:
        mssql.close()


def fetch_xpert_baixas(id_filial: Optional[int], paid_days: int) -> List[Dict[str, Any]]:
    """Baixas dos títulos no escopo — join por ID_DB+ID_CONTASPAGAR (não só ID_FILIAL da baixa)."""
    mssql = _mssql()
    try:
        cur = mssql.cursor()
        sql = f"""
          SELECT b.*
          FROM dbo.CONTASPAGARBAIXA b
          WHERE CAST(b.DATABAIXA AS date) >= CAST(DATEADD(day, -{int(paid_days)}, GETDATE()) AS date)
             OR EXISTS (
                  SELECT 1 FROM dbo.CONTASPAGAR c
                  WHERE c.ID_DB = b.ID_DB
                    AND c.ID_CONTASPAGAR = b.ID_CONTASPAGAR
                    AND c.DTAPGTO IS NULL
        """
        params: list[Any] = []
        if id_filial is not None:
            sql += " AND c.ID_FILIAL = %s"
            params.append(int(id_filial))
        sql += " )"
        cur.execute(sql, tuple(params) if params else None)
        return list(cur.fetchall() or [])
    finally:
        mssql.close()

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", default="torqmind")
    ap.add_argument("--id-filial", type=int, default=None)
    ap.add_argument("--paid-days", type=int, default=180)
    ap.add_argument("--skip-baixas", action="store_true")
    ap.add_argument(
        "--source-env",
        default="/home/tm/torqmind/config/source-explorer.env",
    )
    args = ap.parse_args()

    _load_dotenv(args.source_env)

    print(
        f"heal CONTASPAGAR empresa={args.id_empresa} filial={args.id_filial} "
        f"paid_days={args.paid_days} → {args.pg_database}",
        flush=True,
    )
    titles = fetch_xpert_titles(args.id_filial, args.paid_days)
    print(f"xpert titles={len(titles)}", flush=True)

    baixas: List[Dict[str, Any]] = []
    if not args.skip_baixas:
        baixas = fetch_xpert_baixas(args.id_filial, args.paid_days)
        print(f"xpert baixas={len(baixas)}", flush=True)

    pg = _pg(args.pg_database)
    try:
        with pg.cursor() as cur:
            n_cp = upsert_contaspagar(cur, args.id_empresa, titles)
            n_bx = 0
            if baixas:
                n_bx = upsert_baixas(cur, args.id_empresa, baixas)
        pg.commit()
        print(f"OK contaspagar={n_cp} contaspagarbaixa={n_bx}", flush=True)
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()


if __name__ == "__main__":
    main()
