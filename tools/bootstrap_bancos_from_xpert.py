#!/usr/bin/env python3
"""Bootstrap one-shot: CONTASBANCARIA + BANCOSPADRAO + MOVBANCOS + TRANSF AJUSTE* (Xpert → STG PG).

Uso (homolog):
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/homolog.app.env; set +a
  .venv/bin/python tools/bootstrap_bancos_from_xpert.py --id-empresa 1 --pg-database torqmind_homolog

Não imprime senhas. Idempotente (UPSERT pelas PKs STG).

Inclui stg.movbancos_ajuste_plano (TRANSF AJUSTE / AJUSTE-SALDO / AJUSTE EMPRESTIMO…).
Prova Banrisul VR01: movbancos 152.833,35 − TRANSF AJUSTE PIX 24.869,45 = 127.963,90.
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
            # Decimal: preserva inteiros (IDs) sem virar 10.0 no JSON.
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


def upsert_movbancos(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    n = 0
    sql = """
      INSERT INTO stg.movbancos AS t
        (id_empresa, id_filial, id_db, id_movbancos, payload, dt_evento,
         id_db_shadow, id_chave_natural, ingested_at, received_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (id_empresa, id_filial, id_db, id_movbancos) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            ingested_at = now()
    """
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        mid = int(r["ID_MOVBANCOS"])
        dt = r.get("DTACONTA")
        cur.execute(
            sql,
            (
                id_empresa,
                fid,
                id_db,
                mid,
                Jsonb(_jsonable(r)),
                dt,
                id_db,
                f"{fid}:{id_db}:{mid}",
            ),
        )
        n += 1
        if n % 5000 == 0:
            print(f"  movbancos upserted {n}…", flush=True)
    return n


def upsert_ajuste_plano(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    n = 0
    sql = """
      INSERT INTO stg.movbancos_ajuste_plano AS t
        (id_empresa, id_filial, id_db, id_movlctos, payload, dt_evento,
         id_db_shadow, id_chave_natural, ingested_at, received_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (id_empresa, id_filial, id_db, id_movlctos) DO UPDATE
        SET payload = EXCLUDED.payload,
            dt_evento = EXCLUDED.dt_evento,
            ingested_at = now()
    """
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        mid = int(r["ID_MOVLCTOS"])
        dt = r.get("DTACONTA")
        cur.execute(
            sql,
            (
                id_empresa,
                fid,
                id_db,
                mid,
                Jsonb(_jsonable(r)),
                dt,
                id_db,
                f"{fid}:{id_db}:{mid}",
            ),
        )
        n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", required=True)
    ap.add_argument(
        "--from-date",
        default="2019-01-01",
        help="DTACONTA >= from-date for MOVBANCOS (histórico completo necessário para saldo as-of)",
    )
    ap.add_argument("--skip-mov", action="store_true")
    args = ap.parse_args()

    print("connecting Xpert…", flush=True)
    mss = _mssql()
    mcur = mss.cursor(as_dict=True)

    print("loading CONTASBANCARIA…", flush=True)
    mcur.execute("SELECT * FROM dbo.CONTASBANCARIA")
    contas = list(mcur.fetchall())
    print(f"  {len(contas)} rows", flush=True)

    print("loading BANCOSPADRAO…", flush=True)
    mcur.execute("SELECT * FROM dbo.BANCOSPADRAO")
    bancos = list(mcur.fetchall())
    print(f"  {len(bancos)} rows", flush=True)

    movs: List[Dict[str, Any]] = []
    ajustes: List[Dict[str, Any]] = []
    if not args.skip_mov:
        print(f"loading MOVBANCOS since {args.from_date} (DELETAR=0)…", flush=True)
        mcur.execute(
            """
            SELECT *
            FROM dbo.MOVBANCOS
            WHERE ISNULL(DELETAR,0)=0
              AND DTACONTA >= %s
            """,
            (args.from_date,),
        )
        movs = list(mcur.fetchall())
        print(f"  {len(movs)} rows", flush=True)

    print("loading MOVLCTOS ajustes de plano bancário…", flush=True)
    mcur.execute(
        """
        SELECT *
        FROM dbo.MOVLCTOS
        WHERE ISNULL(ESTORNO, 0) = 0
          AND (
            UPPER(LTRIM(RTRIM(DOCUMENTO))) LIKE 'TRANSF AJUSTE%'
            OR UPPER(LTRIM(RTRIM(DOCUMENTO))) LIKE 'AJUSTE-SALDO%'
            OR UPPER(LTRIM(RTRIM(DOCUMENTO))) LIKE 'AJUSTE SALDO%'
            OR UPPER(LTRIM(RTRIM(DOCUMENTO))) LIKE 'AJUSTE DE SALDOS%'
            OR UPPER(LTRIM(RTRIM(DOCUMENTO))) LIKE 'AJUSTE EMPRESTIMO%'
          )
        """
    )
    ajustes = list(mcur.fetchall())
    print(f"  {len(ajustes)} rows", flush=True)
    mss.close()

    print(f"writing PG {args.pg_database}…", flush=True)
    with _pg(args.pg_database) as conn:
        with conn.cursor() as cur:
            n_cb = 0
            for r in contas:
                fid = int(r["ID_FILIAL"])
                iid = int(r["ID_CONTASBANCARIAS"])
                cur.execute(
                    """
                    INSERT INTO stg.contasbancaria AS t
                      (id_empresa, id_filial, id_contasbancarias, payload,
                       id_db_shadow, id_chave_natural, dt_evento, ingested_at, received_at)
                    VALUES (%s,%s,%s,%s,NULL,%s,NULL,now(),now())
                    ON CONFLICT (id_empresa, id_filial, id_contasbancarias) DO UPDATE
                      SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    (args.id_empresa, fid, iid, Jsonb(_jsonable(r)), f"{fid}:{iid}"),
                )
                n_cb += 1
            print(f"  contasbancaria={n_cb}", flush=True)

            n_bp = 0
            for r in bancos:
                fid = int(r["ID_FILIAL"])
                iid = int(r["ID_BANCOSPADRAO"])
                cur.execute(
                    """
                    INSERT INTO stg.bancospadrao AS t
                      (id_empresa, id_filial, id_bancospadrao, payload,
                       id_db_shadow, id_chave_natural, dt_evento, ingested_at, received_at)
                    VALUES (%s,%s,%s,%s,NULL,%s,NULL,now(),now())
                    ON CONFLICT (id_empresa, id_filial, id_bancospadrao) DO UPDATE
                      SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    (args.id_empresa, fid, iid, Jsonb(_jsonable(r)), f"{fid}:{iid}"),
                )
                n_bp += 1
            print(f"  bancospadrao={n_bp}", flush=True)

            if movs:
                n_mv = upsert_movbancos(cur, args.id_empresa, movs)
                print(f"  movbancos={n_mv}", flush=True)

            if ajustes:
                n_aj = upsert_ajuste_plano(cur, args.id_empresa, ajustes)
                print(f"  movbancos_ajuste_plano={n_aj}", flush=True)

            conn.commit()
            print("STG committed. refresh_liquidez_banco…", flush=True)
            cur.execute("SELECT etl.refresh_liquidez_banco(%s)", (args.id_empresa,))
            refreshed = cur.fetchone()[0]
            print(f"  liquidez rows touched={refreshed}", flush=True)
            conn.commit()

            cur.execute(
                """
                SELECT id_filial, ano_mes, ativo_banco
                FROM mart.liquidez_solvencia
                WHERE id_empresa=%s AND COALESCE(ativo_banco,0) <> 0
                ORDER BY ano_mes DESC, ABS(ativo_banco) DESC
                LIMIT 15
                """,
                (args.id_empresa,),
            )
            print("sample ativo_banco:", cur.fetchall())
            cur.execute(
                """
                SELECT id_filial, COUNT(*) n, ROUND(SUM(saldo)::numeric,2) total
                FROM mart.solvencia_banco_conta
                WHERE id_empresa=%s AND ano_mes = (
                  SELECT MAX(ano_mes) FROM mart.solvencia_banco_conta WHERE id_empresa=%s
                )
                GROUP BY id_filial ORDER BY total DESC NULLS LAST LIMIT 10
                """,
                (args.id_empresa, args.id_empresa),
            )
            print("sample banco_conta:", cur.fetchall())
        conn.commit()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
