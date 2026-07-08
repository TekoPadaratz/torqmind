#!/usr/bin/env python3
"""TorqMind — Backfill/refresh da slim de NFE no ClickHouse a partir da STG.

Contexto: ``torqmind_current.stg_nfe_slim`` é a fonte que a tela AntiFraude usa
para amarrar o cancelamento ao NÚMERO DA NOTA FISCAL (o documento que o cliente
busca no Xpert). Ela é populada pelo CDC a partir da raw ``stg_nfe``. Enquanto o
mapeamento CDC de ``stg.nfe`` não estiver ativo (ver mappings.py), esta slim não
se atualiza sozinha; este utilitário preenche o intervalo lendo direto da STG
PostgreSQL (``stg.nfe``, sempre atualizada pelo Agent) e escrevendo na slim.

Seguro por construção:
- Só faz SELECT na STG PostgreSQL (read-only) e INSERT na slim do ClickHouse.
- A slim é ``ReplacingMergeTree(source_ts_ms)``: re-execuções são idempotentes
  (dedupe por chave, mantém a versão de maior source_ts_ms).
- Não altera o consumer, não mexe em marts de venda, não derruba API/Web.

Credenciais vêm SEMPRE de variáveis de ambiente (nunca hardcoded):
    PG_HOST PG_PORT PG_DB PG_USER PG_PASSWORD
    CH_HOST CH_PORT CH_USER CH_PASSWORD CH_DB

Uso::

    # dry-run (só conta o intervalo, não escreve)
    PG_HOST=172.30.0.8 PG_USER=torqmind PG_PASSWORD=*** \
    CH_HOST=172.30.0.9 CH_USER=torqmind CH_PASSWORD=*** \
    python tools/backfill_nfe_slim.py --since 2026-05-11 --dry-run

    # backfill de verdade
    python tools/backfill_nfe_slim.py --since 2026-05-11
"""
from __future__ import annotations

import argparse
import os
import sys
import time

# Colunas da slim (ordem do INSERT), espelhando torqmind_current.stg_nfe_slim.
_SLIM_COLUMNS = (
    "id_empresa", "id_filial", "id_db", "id_comprovante", "id_nfe",
    "status", "numero_nfe", "serie", "chave_nfe", "protocolo", "modelo",
    "data_emissao", "valor_nfe", "is_deleted", "source_ts_ms",
)

# Leitura da STG: colunas *_shadow (já tipadas) mapeadas para a slim. O
# source_ts_ms deriva do dt_evento (mesmo instante que o CDC usaria).
_READ_SQL = """
    SELECT id_empresa, id_filial, id_db, id_comprovante, id_nfe,
           COALESCE(status_shadow, 0)::int,
           COALESCE(numero_nfe_shadow, ''),
           COALESCE(serie_shadow, ''),
           COALESCE(chave_nfe_shadow, ''),
           COALESCE(protocolo_shadow, ''),
           COALESCE(modelo_shadow, ''),
           data_emissao_shadow,
           COALESCE(valor_nfe_shadow, 0)::numeric(18, 2),
           0,
           COALESCE((extract(epoch FROM dt_evento) * 1000)::bigint, 0)
    FROM stg.nfe
    WHERE data_emissao_shadow >= %(since)s
    {empresa_filter}
"""


def _env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None or val == "":
        print(f"ERRO: variável de ambiente obrigatória ausente: {name}", file=sys.stderr)
        sys.exit(2)
    return val


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill idempotente da slim de NFE (STG -> ClickHouse).")
    parser.add_argument("--since", default="2026-05-11", help="Data mínima de emissão (YYYY-MM-DD). Default 2026-05-11.")
    parser.add_argument("--id-empresa", type=int, default=None, help="Filtrar por empresa (opcional).")
    parser.add_argument("--batch", type=int, default=50000, help="Tamanho do lote (default 50000).")
    parser.add_argument("--dry-run", action="store_true", help="Apenas conta o intervalo; não escreve.")
    args = parser.parse_args()

    import psycopg
    import clickhouse_connect

    empresa_filter = ""
    params: dict[str, object] = {"since": args.since}
    if args.id_empresa is not None:
        empresa_filter = "AND id_empresa = %(id_empresa)s"
        params["id_empresa"] = args.id_empresa
    read_sql = _READ_SQL.format(empresa_filter=empresa_filter)

    pg = psycopg.connect(
        host=_env("PG_HOST"), port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "torqmind"), user=_env("PG_USER"),
        password=_env("PG_PASSWORD"),
    )
    ch = clickhouse_connect.get_client(
        host=_env("CH_HOST"), port=int(os.environ.get("CH_PORT", "8123")),
        username=_env("CH_USER"), password=_env("CH_PASSWORD"),
    )
    ch_db = os.environ.get("CH_DB", "torqmind_current")
    slim = f"{ch_db}.stg_nfe_slim"

    count_sql = "SELECT count(*) FROM stg.nfe WHERE data_emissao_shadow >= %(since)s " + empresa_filter
    with pg.cursor() as c:
        c.execute(count_sql, params)
        gap = c.fetchone()[0]
    print(f"Intervalo a processar (since={args.since}): {gap} notas")

    if args.dry_run:
        print("dry-run: nada escrito.")
        pg.close(); ch.close()
        return 0

    t0 = time.time()
    total = 0
    with pg.cursor(name="nfe_backfill") as c:
        c.itersize = args.batch
        c.execute(read_sql, params)
        while True:
            rows = c.fetchmany(args.batch)
            if not rows:
                break
            ch.insert(slim, [list(r) for r in rows], column_names=list(_SLIM_COLUMNS))
            total += len(rows)
            print(f"  inseridos {total}/{gap}...", flush=True)
    pg.close()

    after = ch.query(f"SELECT count() FROM {slim}").result_rows[0][0]
    ch.close()
    print(f"OK: {total} linhas em {time.time() - t0:.1f}s | slim total agora: {after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
