#!/usr/bin/env python3
"""Copia tabelas STG entre Postgres (ex.: torqmind → torqmind_homolog).

Uso:
  set -a; source /etc/torqmind/prod.app.env; set +a
  .venv/bin/python tools/sync_stg_pg_to_homolog.py \\
    --source-db torqmind --target-db torqmind_homolog \\
    --tables funcionarios,entidades --id-empresa 1

Idempotente (UPSERT pela PK). Não imprime senhas.
"""
from __future__ import annotations

import argparse
import os
from typing import Iterable, List, Sequence, Tuple

import psycopg
from psycopg.types.json import Jsonb

TABLE_PKS = {
    "funcionarios": ("id_empresa", "id_filial", "id_funcionario"),
    "entidades": ("id_empresa", "id_filial", "id_entidade"),
    "usuarios": ("id_empresa", "id_usuario"),
    "contasbancaria": ("id_empresa", "id_filial", "id_contasbancarias"),
    "bancospadrao": ("id_empresa", "id_filial", "id_bancospadrao"),
}

COLUMNS = (
    "id_empresa",
    "id_filial",
    "id_funcionario",
    "id_entidade",
    "id_usuario",
    "id_contasbancarias",
    "id_bancospadrao",
    "payload",
    "ingested_at",
    "dt_evento",
    "id_db_shadow",
    "id_chave_natural",
    "received_at",
)


def _env(*names: str, default: str | None = None) -> str:
    for name in names:
        val = os.environ.get(name)
        if val:
            return val
    if default is not None:
        return default
    raise SystemExit(f"missing env one of {names}")


def _connect(database: str):
    host = _env("PG_HOST", "POSTGRES_HOST", default="172.30.0.8")
    port = _env("PG_PORT", "POSTGRES_PORT", default="5432")
    user = _env("PG_USER", "POSTGRES_USER")
    pwd = _env("PG_PASSWORD", "POSTGRES_PASSWORD")
    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={user} password={pwd}",
        autocommit=False,
    )


def _table_columns(conn, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'stg' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


def sync_table(
    src,
    dst,
    table: str,
    id_empresa: int,
    batch_size: int = 2000,
) -> int:
    pk = TABLE_PKS.get(table)
    if not pk:
        raise SystemExit(f"unsupported table {table}; known={sorted(TABLE_PKS)}")

    src_cols = _table_columns(src, table)
    dst_cols = _table_columns(dst, table)
    cols = [c for c in src_cols if c in dst_cols]
    if not cols:
        raise SystemExit(f"no shared columns for stg.{table}")
    for key in pk:
        if key not in cols:
            raise SystemExit(f"PK column {key} missing on stg.{table}")

    col_sql = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    conflict = ", ".join(pk)
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in pk
    )
    insert_sql = f"""
      INSERT INTO stg.{table} ({col_sql})
      VALUES ({placeholders})
      ON CONFLICT ({conflict}) DO UPDATE SET {updates}
    """

    select_sql = f"""
      SELECT {col_sql}
      FROM stg.{table}
      WHERE id_empresa = %s
    """

    total = 0
    with src.cursor(name=f"sync_{table}") as scur, dst.cursor() as dcur:
        scur.itersize = batch_size
        scur.execute(select_sql, (id_empresa,))
        batch: List[Tuple] = []
        for row in scur:
            values = []
            for i, col in enumerate(cols):
                val = row[i]
                if col == "payload" and isinstance(val, dict):
                    val = Jsonb(val)
                values.append(val)
            batch.append(tuple(values))
            if len(batch) >= batch_size:
                dcur.executemany(insert_sql, batch)
                total += len(batch)
                print(f"  stg.{table} upserted {total}…", flush=True)
                batch.clear()
        if batch:
            dcur.executemany(insert_sql, batch)
            total += len(batch)
    dst.commit()
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-db", required=True)
    ap.add_argument("--target-db", required=True)
    ap.add_argument("--tables", default="funcionarios,entidades")
    ap.add_argument("--id-empresa", type=int, default=1)
    args = ap.parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    print(f"sync {args.source_db} → {args.target_db} tables={tables}", flush=True)
    with _connect(args.source_db) as src, _connect(args.target_db) as dst:
        for table in tables:
            print(f"copying stg.{table}…", flush=True)
            n = sync_table(src, dst, table, args.id_empresa)
            print(f"  stg.{table}={n}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
