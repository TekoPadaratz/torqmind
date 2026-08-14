#!/usr/bin/env python3
"""Reconcilia órfãos de stg.movlctos vs Xpert (tombstone seguro).

Remove da STG apenas chaves (id_empresa,id_filial,id_db,id_movlctos) que:
  - estão na janela DTACONTA pedida;
  - NÃO existem mais no Xpert (MOVLCTOS);
  - opcionalmente filtradas a planos 3.2*/3.3* (despesas DRE).

Não apaga registros ainda presentes no Xpert (protege caso-ouro VR01).
Não faz TRUNCATE/DROP. Idempotente.

Uso:
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/prod.app.env; set +a
  .venv/bin/python tools/reconcile_movlctos_tombstones_from_xpert.py \\
    --id-empresa 1 --pg-database torqmind --from-date 2026-01-01 --apply
"""
from __future__ import annotations

import argparse
import os
from datetime import date
from typing import Any, Optional, Set, Tuple

import pymssql
import psycopg


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
        tds_version="7.0",
    )


def _pg(database: str):
    host = os.environ.get("PG_HOST") or os.environ.get("POSTGRES_HOST") or os.environ.get("STG_PG_HOST")
    port = os.environ.get("PG_PORT") or os.environ.get("POSTGRES_PORT") or os.environ.get("STG_PG_PORT") or "5432"
    user = os.environ.get("PG_USER") or os.environ.get("POSTGRES_USER") or os.environ.get("STG_PG_USER")
    pwd = os.environ.get("PG_PASSWORD") or os.environ.get("POSTGRES_PASSWORD") or os.environ.get("STG_PG_PASSWORD")
    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={user} password={pwd}",
        autocommit=False,
    )


Key = Tuple[int, int, int]  # id_filial, id_db, id_movlctos


def xpert_keys(from_date: str, despesas_only: bool) -> Set[Key]:
    mssql = _mssql()
    cur = mssql.cursor(as_dict=True)
    if despesas_only:
        sql = """
          SELECT m.ID_FILIAL, m.ID_DB, m.ID_MOVLCTOS
          FROM dbo.MOVLCTOS m WITH (NOLOCK)
          JOIN dbo.PLANODECONTAS p WITH (NOLOCK)
            ON p.ID_PLANODECONTAS = m.ID_PLANODECONTAS
           AND p.ID_FILIAL = m.ID_FILIAL
          WHERE m.DTACONTA >= %s
            AND (p.CODIGOPLANODECONTAS LIKE '3.2.%%' OR p.CODIGOPLANODECONTAS LIKE '3.3.%%')
        """
    else:
        sql = """
          SELECT m.ID_FILIAL, m.ID_DB, m.ID_MOVLCTOS
          FROM dbo.MOVLCTOS m WITH (NOLOCK)
          WHERE m.DTACONTA >= %s
        """
    cur.execute(sql, (from_date,))
    keys = {(int(r["ID_FILIAL"]), int(r["ID_DB"]), int(r["ID_MOVLCTOS"])) for r in cur.fetchall()}
    mssql.close()
    return keys


def stg_keys(pg, id_empresa: int, from_date: str, despesas_only: bool) -> Set[Key]:
    with pg.cursor() as cur:
        cur.execute("SELECT set_config('app.current_empresa', %s, true)", [str(id_empresa)])
        if despesas_only:
            cur.execute(
                """
                SELECT m.id_filial, m.id_db, m.id_movlctos
                FROM stg.movlctos m
                JOIN dw.dim_plano_contas_gerencial d
                  ON d.id_empresa = m.id_empresa
                 AND d.id_filial = m.id_filial
                 AND d.id_planodecontas = etl.safe_int(m.payload->>'ID_PLANODECONTAS')
                WHERE m.id_empresa = %s
                  AND coalesce(d.entra_dre, false) IS TRUE
                  AND (etl.safe_timestamp(m.payload->>'DTACONTA'))::date >= %s::date
                """,
                [id_empresa, from_date],
            )
        else:
            cur.execute(
                """
                SELECT id_filial, id_db, id_movlctos
                FROM stg.movlctos
                WHERE id_empresa = %s
                  AND (etl.safe_timestamp(payload->>'DTACONTA'))::date >= %s::date
                """,
                [id_empresa, from_date],
            )
        return {(int(a), int(b), int(c)) for a, b, c in cur.fetchall()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", default="torqmind_homolog")
    ap.add_argument("--from-date", default="2026-01-01")
    ap.add_argument("--despesas-only", action="store_true", default=True)
    ap.add_argument("--all-movlctos", action="store_true", help="não restringe a DRE 3.2/3.3")
    ap.add_argument("--apply", action="store_true", help="executa DELETE dos órfãos; default=dry-run")
    ap.add_argument("--limit", type=int, default=0, help="limite de deletes (0=sem limite)")
    args = ap.parse_args()

    despesas_only = not args.all_movlctos
    print(
        f"reconcile movlctos orphans empresa={args.id_empresa} db={args.pg_database} "
        f"from={args.from_date} despesas_only={despesas_only} apply={args.apply}"
    )
    xkeys = xpert_keys(args.from_date, despesas_only)
    print(f"xpert keys={len(xkeys)}")
    pg = _pg(args.pg_database)
    try:
        skeys = stg_keys(pg, args.id_empresa, args.from_date, despesas_only)
        print(f"stg keys={len(skeys)}")
        orphans = sorted(skeys - xkeys)
        print(f"orphans={len(orphans)}")
        if orphans[:20]:
            print("sample orphans (filial,id_db,id_movlctos):", orphans[:20])
        if not args.apply:
            print("DRY-RUN: passe --apply para deletar órfãos")
            return
        if not orphans:
            print("OK nothing to delete")
            return
        to_del = orphans if not args.limit else orphans[: args.limit]
        with pg.cursor() as cur:
            cur.execute("SELECT set_config('app.current_empresa', %s, true)", [str(args.id_empresa)])
            cur.executemany(
                """
                DELETE FROM stg.movlctos
                WHERE id_empresa = %s AND id_filial = %s AND id_db = %s AND id_movlctos = %s
                """,
                [(args.id_empresa, f, d, m) for f, d, m in to_del],
            )
        pg.commit()
        print(f"OK deleted={len(to_del)}")
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()


if __name__ == "__main__":
    main()
