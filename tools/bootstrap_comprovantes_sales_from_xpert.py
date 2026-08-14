#!/usr/bin/env python3
"""Bootstrap histórico de vendas Xpert → stg.comprovantes + stg.itenscomprovantes.

Corrige janela ausente (ex.: Jan–Abr/2025) sem reset, sem apagar CDC corrente.
Idempotente (UPSERT por PK completa com id_db). Fonte canônica: COMPROVANTES +
ITENSCOMPROVANTE (não MOVPRODUTOS / pagamentos).

Uso (Hom):
  set -a; source /home/tm/torqmind/config/source-explorer.env
  set -a; source /etc/torqmind/homolog.app.env; set +a
  .venv/bin/python tools/bootstrap_comprovantes_sales_from_xpert.py \\
    --id-empresa 1 --pg-database torqmind_homolog \\
    --from-date 2025-01-01 --to-date 2025-05-01

Uso (Prod):
  set -a; source /etc/torqmind/prod.app.env; set +a
  .venv/bin/python tools/bootstrap_comprovantes_sales_from_xpert.py \\
    --id-empresa 1 --pg-database torqmind \\
    --from-date 2025-01-01 --to-date 2025-05-01
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

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
        timeout=900,
        as_dict=True,
        tds_version="7.0",
    )


def _pg(database: str):
    host = os.environ.get("PG_HOST") or os.environ.get("POSTGRES_HOST") or os.environ.get("STG_PG_HOST")
    port = os.environ.get("PG_PORT") or os.environ.get("POSTGRES_PORT") or os.environ.get("STG_PG_PORT") or "5432"
    user = os.environ.get("PG_USER") or os.environ.get("POSTGRES_USER") or os.environ.get("STG_PG_USER")
    pwd = os.environ.get("PG_PASSWORD") or os.environ.get("POSTGRES_PASSWORD") or os.environ.get("STG_PG_PASSWORD")
    if not all([host, user, pwd]):
        raise SystemExit("missing PG_* / STG_PG_* credentials")
    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={user} password={pwd}",
        autocommit=False,
    )


def _jsonable(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat(sep=" ", timespec="seconds")
        elif isinstance(v, date) and not isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, bool):
            out[k] = v
        elif isinstance(v, Decimal):
            if v == v.to_integral_value():
                out[k] = int(v)
            else:
                out[k] = float(v)
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


def _month_windows(from_d: date, to_d: date) -> Iterator[Tuple[date, date]]:
    """Half-open [start, end) monthly windows covering [from_d, to_d)."""
    cur = date(from_d.year, from_d.month, 1)
    if cur < from_d:
        cur = from_d
    while cur < to_d:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        end = min(nxt, to_d)
        yield cur, end
        cur = end


def _to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        return bool(int(v))
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "sim", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "nao", "não", "no", "n", ""}:
        return False
    return None


def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _cfop_to_int(v: Any) -> Optional[int]:
    """Xpert grava CFOP como '5.656' (=5656). Nunca usar float→int (vira 5)."""
    if v is None or v == "":
        return None
    s = str(v).strip().replace(".", "").replace(",", "")
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _to_num(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def upsert_comprovantes(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    sql = """
      INSERT INTO stg.comprovantes AS t (
        id_empresa, id_filial, id_db, id_comprovante, payload, dt_evento,
        id_db_shadow, id_chave_natural, ingested_at, received_at,
        referencia_shadow, id_usuario_shadow, id_turno_shadow, id_cliente_shadow,
        valor_total_shadow, cancelado_shadow, situacao_shadow
      ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s, now(), now(),
        %s,%s,%s,%s,%s,%s,%s
      )
      ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO UPDATE SET
        payload = EXCLUDED.payload,
        dt_evento = EXCLUDED.dt_evento,
        id_db_shadow = EXCLUDED.id_db_shadow,
        id_chave_natural = EXCLUDED.id_chave_natural,
        ingested_at = now(),
        received_at = now(),
        referencia_shadow = EXCLUDED.referencia_shadow,
        id_usuario_shadow = EXCLUDED.id_usuario_shadow,
        id_turno_shadow = EXCLUDED.id_turno_shadow,
        id_cliente_shadow = EXCLUDED.id_cliente_shadow,
        valor_total_shadow = EXCLUDED.valor_total_shadow,
        cancelado_shadow = EXCLUDED.cancelado_shadow,
        situacao_shadow = EXCLUDED.situacao_shadow
    """
    batch: List[tuple] = []
    n = 0
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        cid = int(r["ID_COMPROVANTE"])
        dt = r.get("DATA") or r.get("DTACONTA")
        batch.append(
            (
                id_empresa,
                fid,
                id_db,
                cid,
                Jsonb(_jsonable(r)),
                dt,
                id_db,
                f"{fid}:{id_db}:{cid}",
                _to_int(r.get("REFERENCIA")),
                _to_int(r.get("ID_USUARIOS") or r.get("ID_USUARIO")),
                _to_int(r.get("ID_TURNOS") or r.get("ID_TURNO")),
                _to_int(r.get("ID_ENTIDADE") or r.get("ID_CLIENTE")),
                _to_num(r.get("VLRTOTAL")),
                _to_bool(r.get("CANCELADO")),
                _to_int(r.get("SITUACAO")),
            )
        )
        n += 1
        if len(batch) >= 1500:
            cur.executemany(sql, batch)
            print(f"    comprovantes upserted {n}...", flush=True)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
    return n


def upsert_itens(cur, id_empresa: int, rows: Iterable[Dict[str, Any]]) -> int:
    sql = """
      INSERT INTO stg.itenscomprovantes AS t (
        id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante,
        payload, dt_evento, id_db_shadow, id_chave_natural, ingested_at, received_at,
        id_produto_shadow, id_funcionario_shadow, cfop_shadow, qtd_shadow,
        valor_unitario_shadow, total_shadow, desconto_shadow, custo_unitario_shadow
      ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now(),
        %s,%s,%s,%s,%s,%s,%s,%s
      )
      ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante) DO UPDATE SET
        payload = EXCLUDED.payload,
        dt_evento = EXCLUDED.dt_evento,
        id_db_shadow = EXCLUDED.id_db_shadow,
        id_chave_natural = EXCLUDED.id_chave_natural,
        ingested_at = now(),
        received_at = now(),
        id_produto_shadow = EXCLUDED.id_produto_shadow,
        id_funcionario_shadow = EXCLUDED.id_funcionario_shadow,
        cfop_shadow = EXCLUDED.cfop_shadow,
        qtd_shadow = EXCLUDED.qtd_shadow,
        valor_unitario_shadow = EXCLUDED.valor_unitario_shadow,
        total_shadow = EXCLUDED.total_shadow,
        desconto_shadow = EXCLUDED.desconto_shadow,
        custo_unitario_shadow = EXCLUDED.custo_unitario_shadow
    """
    batch: List[tuple] = []
    n = 0
    for r in rows:
        fid = int(r["ID_FILIAL"])
        id_db = int(r["ID_DB"])
        cid = int(r["ID_COMPROVANTE"])
        iid = int(r["ID_ITENSCOMPROVANTE"])
        # Alias canônico esperado pelo TorqMind
        payload = _jsonable(r)
        payload["ID_ITEMCOMPROVANTE"] = iid
        dt = r.get("TORQMIND_DT_EVENTO") or r.get("DATA")
        cfop = _cfop_to_int(r.get("CFOP"))
        batch.append(
            (
                id_empresa,
                fid,
                id_db,
                cid,
                iid,
                Jsonb(payload),
                dt,
                id_db,
                f"{fid}:{id_db}:{cid}:{iid}",
                _to_int(r.get("ID_PRODUTOS") or r.get("ID_PRODUTO")),
                _to_int(r.get("ID_FUNCIONARIOS") or r.get("ID_FUNCIONARIO")),
                cfop,
                _to_num(r.get("QTDE")),
                _to_num(r.get("VLRUNITARIO")),
                _to_num(r.get("VLRTOTALITEM") or r.get("TOTAL")),
                _to_num(r.get("VLRDESCONTO")),
                _to_num(r.get("VLRCUSTO")),
            )
        )
        n += 1
        if len(batch) >= 1500:
            cur.executemany(sql, batch)
            print(f"    itens upserted {n}...", flush=True)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
    return n


def fetch_comprovantes(mssql, start: date, end: date, id_filial: Optional[int]) -> List[Dict[str, Any]]:
    cur = mssql.cursor(as_dict=True)
    sql = """
      SELECT c.*
      FROM dbo.COMPROVANTES c WITH (NOLOCK)
      WHERE c.DATA >= %s AND c.DATA < %s
    """
    params: List[Any] = [start.isoformat(), end.isoformat()]
    if id_filial is not None:
        sql += " AND c.ID_FILIAL = %s"
        params.append(id_filial)
    sql += " ORDER BY c.ID_FILIAL, c.ID_DB, c.ID_COMPROVANTE"
    cur.execute(sql, tuple(params))
    return list(cur.fetchall())


def fetch_itens(mssql, start: date, end: date, id_filial: Optional[int]) -> List[Dict[str, Any]]:
    cur = mssql.cursor(as_dict=True)
    sql = """
      SELECT i.*, c.DATA AS TORQMIND_DT_EVENTO
      FROM dbo.ITENSCOMPROVANTE i WITH (NOLOCK)
      INNER JOIN dbo.COMPROVANTES c WITH (NOLOCK)
        ON c.ID_COMPROVANTE = i.ID_COMPROVANTE
       AND c.ID_FILIAL = i.ID_FILIAL
       AND c.ID_DB = i.ID_DB
      WHERE c.DATA >= %s AND c.DATA < %s
    """
    params: List[Any] = [start.isoformat(), end.isoformat()]
    if id_filial is not None:
        sql += " AND i.ID_FILIAL = %s"
        params.append(id_filial)
    sql += " ORDER BY i.ID_FILIAL, i.ID_DB, i.ID_COMPROVANTE, i.ID_ITENSCOMPROVANTE"
    cur.execute(sql, tuple(params))
    return list(cur.fetchall())


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--pg-database", default="torqmind_homolog")
    ap.add_argument("--from-date", default="2025-01-01")
    ap.add_argument("--to-date", default="2025-05-01", help="exclusive end date")
    ap.add_argument("--id-filial", type=int, default=None)
    ap.add_argument("--skip-itens", action="store_true")
    ap.add_argument("--skip-comprovantes", action="store_true")
    args = ap.parse_args()

    from_d = parse_date(args.from_date)
    to_d = parse_date(args.to_date)
    if to_d <= from_d:
        raise SystemExit("--to-date must be > --from-date")

    print(
        f"bootstrap sales Xpert→STG empresa={args.id_empresa} db={args.pg_database} "
        f"[{from_d}, {to_d})"
    )
    mssql = _mssql()
    pg = _pg(args.pg_database)
    # Bypass RLS for controlled bootstrap (same role used by other heal tools).
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT set_config('app.current_empresa', %s, true)", [str(args.id_empresa)])
            cur.execute("SET LOCAL statement_timeout = '0'")
        total_c = total_i = 0
        for start, end in _month_windows(from_d, to_d):
            print(f"  month window [{start}, {end})", flush=True)
            with pg.cursor() as cur:
                cur.execute("SELECT set_config('app.current_empresa', %s, true)", [str(args.id_empresa)])
                cur.execute("SET LOCAL statement_timeout = '0'")
                if not args.skip_comprovantes:
                    comps = fetch_comprovantes(mssql, start, end, args.id_filial)
                    print(f"    xpert comprovantes={len(comps)}", flush=True)
                    n = upsert_comprovantes(cur, args.id_empresa, comps)
                    total_c += n
                    print(f"    upserted comprovantes={n}", flush=True)
                if not args.skip_itens:
                    itens = fetch_itens(mssql, start, end, args.id_filial)
                    print(f"    xpert itens={len(itens)}", flush=True)
                    n = upsert_itens(cur, args.id_empresa, itens)
                    total_i += n
                    print(f"    upserted itens={n}", flush=True)
            pg.commit()
            print(f"  committed [{start}, {end})", flush=True)
        print(f"OK total comprovantes={total_c} itens={total_i}")
    except Exception:
        pg.rollback()
        raise
    finally:
        mssql.close()
        pg.close()


if __name__ == "__main__":
    main()
