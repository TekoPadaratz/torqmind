#!/usr/bin/env python3
"""Backfill de snapshots mensais de solvência no ClickHouse (PRODUÇÃO).

Calcula a posição do dia 1 00:00 America/Sao_Paulo de cada mês desde
2025-01 e grava em torqmind_mart.solvencia_snapshot_mensal.

Fonte prioritária: PostgreSQL mart.liquidez_solvencia (já as-of).
Fallback: zeros + log — não inventa saldo.

Uso (no host analytics / API com CH prod)::

    PATH="$PWD/.venv/bin:$PATH" python tools/backfill_solvencia_snapshot_ch.py \\
      --id-empresa 1 --from-ym 202501 --to-ym 202607

Requer CLICKHOUSE_* no ambiente (ou --dsn). NÃO apaga STG/DW.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

# Allow running from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apps", "api"))


def _ym_range(from_ym: int, to_ym: int) -> List[int]:
    out: List[int] = []
    y, m = divmod(from_ym, 100)
    m = from_ym % 100
    y = from_ym // 100
    cur = from_ym
    while cur <= to_ym:
        out.append(cur)
        m += 1
        if m > 12:
            m, y = 1, y + 1
        cur = y * 100 + m
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-empresa", type=int, required=True)
    ap.add_argument("--from-ym", type=int, default=202501)
    ap.add_argument("--to-ym", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    to_ym = args.to_ym
    if to_ym is None:
        now = datetime.now()
        to_ym = now.year * 100 + now.month

    from app.db import get_conn

    months = _ym_range(args.from_ym, to_ym)
    rows: List[Dict[str, Any]] = []
    with get_conn(role="MASTER", tenant_id=args.id_empresa, branch_id=None) as conn:
        for ym in months:
            pg_rows = conn.execute(
                """
                SELECT id_empresa, id_filial, ano_mes,
                       COALESCE(ativo_caixa,0) AS ativo_caixa,
                       COALESCE(ativo_banco,0) AS ativo_banco,
                       COALESCE(ativo_cartoes,0) AS ativo_cartoes,
                       COALESCE(ativo_cartoes_credito,0) AS ativo_cartoes_credito,
                       COALESCE(ativo_cartoes_debito,0) AS ativo_cartoes_debito,
                       COALESCE(ativo_cheques,0) AS ativo_cheques,
                       COALESCE(ativo_estoque,0) AS ativo_estoque,
                       COALESCE(ativo_estoque_loja,0) AS ativo_estoque_loja,
                       COALESCE(ativo_estoque_combustivel,0) AS ativo_estoque_combustivel,
                       COALESCE(passivo_contas_pagar,0) AS passivo_contas_pagar
                FROM mart.liquidez_solvencia
                WHERE id_empresa = %s AND ano_mes = %s
                """,
                [args.id_empresa, ym],
            ).fetchall()
            y, m = ym // 100, ym % 100
            as_of = f"{y:04d}-{m:02d}-01 00:00:00"
            for r in pg_rows:
                rows.append({
                    "id_empresa": int(r["id_empresa"]),
                    "id_filial": int(r["id_filial"]),
                    "ano_mes": int(r["ano_mes"]),
                    "as_of_ts": as_of,
                    "ativo_caixa": float(r["ativo_caixa"]),
                    "ativo_banco": float(r["ativo_banco"]),
                    "ativo_cartoes": float(r["ativo_cartoes"]),
                    "ativo_cartoes_credito": float(r["ativo_cartoes_credito"]),
                    "ativo_cartoes_debito": float(r["ativo_cartoes_debito"]),
                    "ativo_cheques": float(r["ativo_cheques"]),
                    "ativo_aprazo": 0.0,
                    "ativo_estoque_loja": float(r["ativo_estoque_loja"]),
                    "ativo_estoque_combustivel": float(r["ativo_estoque_combustivel"]),
                    "ativo_estoque": float(r["ativo_estoque"]),
                    "qtd_estoque_loja": 0.0,
                    "qtd_estoque_combustivel": 0.0,
                    "passivo_contas_pagar": float(r["passivo_contas_pagar"]),
                    "saldo_bancos_json": "[]",
                    "cheques_abertos_json": "[]",
                    "source": "pg_liquidez_solvencia",
                })

    print(json.dumps({"empresa": args.id_empresa, "months": months, "rows": len(rows)}, ensure_ascii=False))
    if args.dry_run or not rows:
        return 0

    cols = list(rows[0].keys())
    data = [[r[c] for c in cols] for r in rows]

    # Prefer clickhouse_connect helper; fallback HTTP insert (native protocol not required).
    inserted = False
    try:
        from app.db_clickhouse import get_clickhouse_client, insert_batch
        client = get_clickhouse_client()
        if client is not None:
            n = insert_batch("torqmind_mart.solvencia_snapshot_mensal", data, column_names=cols)
            print(f"OK inserted {n} into torqmind_mart.solvencia_snapshot_mensal via clickhouse_connect")
            inserted = True
    except Exception as exc:
        print(f"clickhouse_connect insert failed: {exc!r}; falling back to HTTP")

    if not inserted:
        import urllib.parse
        import urllib.request

        host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
        port = os.environ.get("CLICKHOUSE_PORT", "8123")
        user = os.environ.get("CLICKHOUSE_USER", "default")
        password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        database = os.environ.get("CLICKHOUSE_DATABASE", "default")

        def _esc(v: Any) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "1" if v else "0"
            if isinstance(v, (int, float)):
                return str(v)
            s = str(v).replace("\\", "\\\\").replace("'", "\\'")
            return f"'{s}'"

        values_sql = ",".join(
            "(" + ",".join(_esc(r[c]) for c in cols) + ")" for r in rows
        )
        col_list = ", ".join(cols)
        query = (
            f"INSERT INTO torqmind_mart.solvencia_snapshot_mensal ({col_list}) VALUES {values_sql}"
        )
        qs = urllib.parse.urlencode({"database": database, "user": user, "password": password})
        url = f"http://{host}:{port}/?{qs}"
        req = urllib.request.Request(url, data=query.encode("utf-8"), method="POST")
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            print(f"OK inserted {len(rows)} into torqmind_mart.solvencia_snapshot_mensal via HTTP status={resp.status} body={body!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
