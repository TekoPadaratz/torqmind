#!/usr/bin/env python3
"""Seed controlado Solvencia → PostgreSQL HOMOLOG only (torqmind_homolog).

Lê SQL Server Xpert (RO) e grava STG em homologação para a filial VR01 (14458).
Abortar se o destino não for torqmind_homolog.

Uso:
  ENV_FILE=/etc/torqmind/homolog.app.env \\
  python tools/seed_solvencia_homolog_vr01.py \\
    --xpert-env config/source-explorer.env \\
    --id-empresa 1 --id-filial 14458
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from xpert_source_explorer import Config, get_connection  # noqa: E402


def _load_env_file(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip("'").strip('"')
    return out


def _jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep="T", timespec="seconds")
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, memoryview):
        return bytes(v).decode("utf-8", errors="replace")
    return v


def _row_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _jsonable(v) for k, v in row.items()}


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


def _pg_connect(env: Dict[str, str]):
    import psycopg2
    from psycopg2.extras import execute_batch, Json

    host = env.get("PG_HOST") or env.get("POSTGRES_HOST")
    port = int(env.get("PG_PORT") or env.get("POSTGRES_PORT") or 5432)
    db = env.get("PG_DATABASE") or env.get("POSTGRES_DB")
    user = env.get("PG_USER") or env.get("POSTGRES_USER")
    password = env.get("PG_PASSWORD") or env.get("POSTGRES_PASSWORD")
    app_env = (env.get("APP_ENV") or "").lower()

    if db != "torqmind_homolog":
        raise SystemExit(f"ABORT: destino deve ser torqmind_homolog, veio {db!r}")
    if app_env not in {"homolog", "homologation"}:
        raise SystemExit(f"ABORT: APP_ENV deve ser homolog, veio {app_env!r}")

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    conn.autocommit = False
    return conn, execute_batch, Json


def _fetch(xpert, sql: str, batch: int = 5000) -> Iterable[List[Dict[str, Any]]]:
    cur = xpert.cursor(as_dict=True)
    cur.execute(sql)
    while True:
        rows = cur.fetchmany(batch)
        if not rows:
            break
        # pymssql as_dict may return list of dict-like
        yield [dict(r) for r in rows]
    cur.close()


def seed_convenios(xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int) -> int:
    sql = f"""
    SELECT * FROM dbo.CONVENIOS WHERE ID_FILIAL = {int(id_filial)}
    """
    total = 0
    upsert = """
    INSERT INTO stg.convenios (id_empresa, id_filial, id_convenios, id_db, payload, dt_evento, ingested_at, received_at)
    VALUES (%s,%s,%s,0,%s, now(), now(), now())
    ON CONFLICT (id_empresa, id_filial, id_convenios) DO UPDATE SET
      payload = EXCLUDED.payload, ingested_at = now(), received_at = now()
    """
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql):
            params = []
            for r in batch:
                cid = _to_int(r.get("ID_CONVENIOS"))
                if cid is None:
                    continue
                params.append((id_empresa, id_filial, cid, Json(_row_payload(r))))
            if params:
                execute_batch(cur, upsert, params, page_size=500)
                total += len(params)
        pg.commit()
    return total


def seed_saldoclientes(xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int) -> int:
    sql = f"""
    SELECT * FROM dbo.SALDOCLIENTES WHERE ID_FILIAL = {int(id_filial)}
    """
    total = 0
    upsert = """
    INSERT INTO stg.saldoclientes
      (id_empresa, id_filial, id_db, id_saldoclientes, payload, dt_evento, ingested_at, received_at)
    VALUES (%s,%s,%s,%s,%s, now(), now(), now())
    ON CONFLICT (id_empresa, id_filial, id_db, id_saldoclientes) DO UPDATE SET
      payload = EXCLUDED.payload, ingested_at = now(), received_at = now()
    """
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql):
            params = []
            for r in batch:
                sid = _to_int(r.get("ID_SALDOCLIENTES"))
                id_db = _to_int(r.get("ID_DB")) or 0
                if sid is None:
                    continue
                params.append((id_empresa, id_filial, id_db, sid, Json(_row_payload(r))))
            if params:
                execute_batch(cur, upsert, params, page_size=500)
                total += len(params)
        pg.commit()
    return total


def seed_contasreceber(xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int) -> int:
    """Títulos a receber (A Prazo). Inclui abertos + pagos recentes (120d) para não
    congelar baixos no STG — espelha a regra do Agent de CONTASRECEBER."""
    sql = f"""
    SELECT *
    FROM dbo.CONTASRECEBER
    WHERE ID_FILIAL = {int(id_filial)}
      AND (
        DTAPGTO IS NULL
        OR CAST(DTAPGTO AS date) >= CAST(DATEADD(day, -120, GETDATE()) AS date)
      )
    """
    total = 0
    upsert = """
    INSERT INTO stg.contasreceber
      (id_empresa, id_filial, id_db, id_contasreceber, payload, dt_evento, ingested_at, received_at)
    VALUES (%s,%s,%s,%s,%s,
      COALESCE((%s::timestamp AT TIME ZONE 'America/Sao_Paulo'), now()),
      now(), now())
    ON CONFLICT (id_empresa, id_filial, id_db, id_contasreceber) DO UPDATE SET
      payload = EXCLUDED.payload,
      dt_evento = EXCLUDED.dt_evento,
      ingested_at = now(),
      received_at = now()
    """
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql):
            params = []
            for r in batch:
                tid = _to_int(r.get("ID_CONTASRECEBER"))
                id_db = _to_int(r.get("ID_DB"))
                if tid is None or id_db is None:
                    continue
                dt = r.get("DTACONTA") or r.get("DATAREPL") or r.get("DTAPGTO")
                params.append(
                    (
                        id_empresa,
                        id_filial,
                        id_db,
                        tid,
                        Json(_row_payload(r)),
                        _jsonable(dt),
                    )
                )
            if params:
                execute_batch(cur, upsert, params, page_size=400)
                total += len(params)
                if total % 20000 < 400:
                    print(f"  contasreceber +{len(params)} (total {total})", flush=True)
                    pg.commit()
        pg.commit()
    return total


def seed_credito(xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int) -> int:
    """Crédito antecipado de pista (Havel) — dbo.CREDITO.SALDO > 0."""
    sql = f"""
    SELECT ID_CREDITO, ID_FILIAL, ID_ENTIDADE, ID_PRODUTOS, SALDO
    FROM dbo.CREDITO
    WHERE ID_FILIAL = {int(id_filial)}
    """
    total = 0
    upsert = """
    INSERT INTO stg.credito
      (id_empresa, id_filial, id_db, id_credito, payload, ingested_at, received_at)
    VALUES (%s,%s,0,%s,%s, now(), now())
    ON CONFLICT (id_empresa, id_filial, id_credito) DO UPDATE SET
      payload = EXCLUDED.payload, ingested_at = now(), received_at = now()
    """
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql):
            params = []
            for r in batch:
                cid = _to_int(r.get("ID_CREDITO"))
                if cid is None:
                    continue
                params.append((id_empresa, id_filial, cid, Json(_row_payload(r))))
            if params:
                execute_batch(cur, upsert, params, page_size=500)
                total += len(params)
        pg.commit()
    return total


def seed_comprovantes(
    xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int, dt_from: str, dt_to: str
) -> int:
    sql = f"""
    SELECT ID_COMPROVANTE, ID_FILIAL, ID_DB, DATA, DATAREPL, SITUACAO, CANCELADO,
           REFERENCIA, VLRTOTAL, ID_USUARIOS, ID_TURNOS, ID_ENTIDADE
    FROM dbo.COMPROVANTES
    WHERE ID_FILIAL = {int(id_filial)}
      AND DATA >= '{dt_from}' AND DATA < '{dt_to}'
    """
    total = 0
    upsert = """
    INSERT INTO stg.comprovantes (
      id_empresa, id_filial, id_db, id_comprovante, payload, dt_evento,
      referencia_shadow, situacao_shadow, cancelado_shadow, valor_total_shadow,
      id_usuario_shadow, id_turno_shadow, id_cliente_shadow,
      ingested_at, received_at
    ) VALUES (
      %s,%s,%s,%s,%s,
      (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'),
      %s,%s,%s,%s,%s,%s,%s, now(), now()
    )
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO UPDATE SET
      payload = EXCLUDED.payload,
      dt_evento = EXCLUDED.dt_evento,
      referencia_shadow = EXCLUDED.referencia_shadow,
      situacao_shadow = EXCLUDED.situacao_shadow,
      cancelado_shadow = EXCLUDED.cancelado_shadow,
      valor_total_shadow = EXCLUDED.valor_total_shadow,
      ingested_at = now(), received_at = now()
    """
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql):
            params = []
            for r in batch:
                idc = _to_int(r.get("ID_COMPROVANTE"))
                id_db = _to_int(r.get("ID_DB"))
                if idc is None or id_db is None:
                    continue
                data = r.get("DATA")
                data_s = _jsonable(data)
                cancelado = bool(_to_int(r.get("CANCELADO")) or 0)
                valor = r.get("VLRTOTAL")
                params.append(
                    (
                        id_empresa,
                        id_filial,
                        id_db,
                        idc,
                        Json(_row_payload(r)),
                        data_s,
                        _to_int(r.get("REFERENCIA")),
                        _to_int(r.get("SITUACAO")),
                        cancelado,
                        float(valor) if valor is not None else None,
                        _to_int(r.get("ID_USUARIOS")),
                        _to_int(r.get("ID_TURNOS")),
                        _to_int(r.get("ID_ENTIDADE")),
                    )
                )
            if params:
                execute_batch(cur, upsert, params, page_size=500)
                total += len(params)
                print(f"  comprovantes +{len(params)} (total {total})", flush=True)
        pg.commit()
    return total


def seed_formas(
    xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int, dt_from: str, dt_to: str
) -> int:
    sql = f"""
    SELECT f.ID_FORMAS_PGTO_COMPROVANTES, f.ID_FILIAL, f.ID_DB, f.ID_REFERENCIA, f.TIPO_FORMA,
           f.VALOR_PAGO, f.ID_CARTAO, f.BANDEIRA, f.COD_AUTORIZACAO, f.DATAREPL,
           c.DATA AS COMPROVANTE_DATA, c.ID_COMPROVANTE
    FROM dbo.FORMAS_PGTO_COMPROVANTES f
    JOIN dbo.COMPROVANTES c
      ON c.ID_FILIAL = f.ID_FILIAL AND c.ID_DB = f.ID_DB
     AND c.REFERENCIA = TRY_CAST(f.ID_REFERENCIA AS int)
    WHERE f.ID_FILIAL = {int(id_filial)}
      AND c.DATA >= '{dt_from}' AND c.DATA < '{dt_to}'
    """
    total = 0
    upsert = """
    INSERT INTO stg.formas_pgto_comprovantes (
      id_empresa, id_filial, id_referencia, tipo_forma, payload, dt_evento,
      valor_shadow, bandeira_shadow, autorizacao_shadow, id_db_shadow,
      ingested_at, received_at
    ) VALUES (
      %s,%s,%s,%s,%s,
      (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'),
      %s,%s,%s,%s, now(), now()
    )
    ON CONFLICT (id_empresa, id_filial, id_referencia, tipo_forma) DO UPDATE SET
      payload = EXCLUDED.payload,
      dt_evento = EXCLUDED.dt_evento,
      valor_shadow = EXCLUDED.valor_shadow,
      bandeira_shadow = EXCLUDED.bandeira_shadow,
      autorizacao_shadow = EXCLUDED.autorizacao_shadow,
      id_db_shadow = EXCLUDED.id_db_shadow,
      ingested_at = now(), received_at = now()
    """
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql):
            params = []
            for r in batch:
                ref = _to_int(r.get("ID_REFERENCIA"))
                tipo = _to_int(r.get("TIPO_FORMA"))
                if ref is None or tipo is None:
                    continue
                valor = r.get("VALOR_PAGO")
                data_s = _jsonable(r.get("COMPROVANTE_DATA"))
                payload = _row_payload(r)
                # Campos canônicos esperados pelos ETLs
                payload["ID_REFERENCIA"] = ref
                payload["TIPO_FORMA"] = tipo
                payload["VALOR_PAGO"] = _jsonable(valor)
                payload["ID_CARTAO"] = _to_int(r.get("ID_CARTAO"))
                payload["ID_DB"] = _to_int(r.get("ID_DB"))
                params.append(
                    (
                        id_empresa,
                        id_filial,
                        ref,
                        tipo,
                        Json(payload),
                        data_s,
                        float(valor) if valor is not None else None,
                        str(r.get("BANDEIRA")) if r.get("BANDEIRA") is not None else None,
                        r.get("COD_AUTORIZACAO"),
                        _to_int(r.get("ID_DB")),
                    )
                )
            if params:
                execute_batch(cur, upsert, params, page_size=500)
                total += len(params)
                print(f"  formas +{len(params)} (total {total})", flush=True)
        pg.commit()
    return total


def seed_movimentos(
    xpert, pg, execute_batch, Json, id_empresa: int, id_filial: int, dt_from: str, dt_to: str
) -> Tuple[int, int]:
    sql_mov = f"""
    SELECT ID_MOVPRODUTOS, ID_FILIAL, ID_DB, DATA, DATAREPL, SAIDAS_ENTRADAS,
           ID_COMPROVANTE, ID_USUARIOS, ID_TURNOS, ID_ENTIDADE, TOTALVENDA
    FROM dbo.MOVPRODUTOS
    WHERE ID_FILIAL = {int(id_filial)}
      AND DATA >= '{dt_from}' AND DATA < '{dt_to}'
    """
    upsert_mov = """
    INSERT INTO stg.movprodutos (
      id_empresa, id_filial, id_db, id_movprodutos, payload, dt_evento,
      saidas_entradas_shadow, id_comprovante_shadow, id_usuario_shadow,
      id_turno_shadow, id_cliente_shadow, total_venda_shadow,
      ingested_at, received_at
    ) VALUES (
      %s,%s,%s,%s,%s,
      (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'),
      %s,%s,%s,%s,%s,%s, now(), now()
    )
    ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO UPDATE SET
      payload = EXCLUDED.payload,
      dt_evento = EXCLUDED.dt_evento,
      saidas_entradas_shadow = EXCLUDED.saidas_entradas_shadow,
      ingested_at = now(), received_at = now()
    """
    n_mov = 0
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql_mov):
            params = []
            for r in batch:
                mid = _to_int(r.get("ID_MOVPRODUTOS"))
                id_db = _to_int(r.get("ID_DB"))
                if mid is None or id_db is None:
                    continue
                payload = _row_payload(r)
                params.append(
                    (
                        id_empresa,
                        id_filial,
                        id_db,
                        mid,
                        Json(payload),
                        _jsonable(r.get("DATA")),
                        _to_int(r.get("SAIDAS_ENTRADAS")),
                        _to_int(r.get("ID_COMPROVANTE")),
                        _to_int(r.get("ID_USUARIOS")),
                        _to_int(r.get("ID_TURNOS")),
                        _to_int(r.get("ID_ENTIDADE")),
                        float(r["TOTALVENDA"]) if r.get("TOTALVENDA") is not None else None,
                    )
                )
            if params:
                execute_batch(cur, upsert_mov, params, page_size=500)
                n_mov += len(params)
                print(f"  movprodutos +{len(params)} (total {n_mov})", flush=True)
        pg.commit()

    sql_itens = f"""
    SELECT i.ID_ITENSMOVPRODUTOS, i.ID_FILIAL, i.ID_DB, i.ID_MOVPRODUTOS, i.ID_PRODUTOS,
           i.ID_GRUPOPRODUTOS, i.QTDE, i.CFOP, i.VLRCUSTOCOMICMS, i.VLRCUSTO,
           i.VLRUNITARIO, i.TOTAL, i.VLRDESCONTO, i.DATAREPL, m.DATA AS MOV_DATA,
           m.SAIDAS_ENTRADAS
    FROM dbo.ITENSMOVPRODUTOS i
    JOIN dbo.MOVPRODUTOS m
      ON m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS AND m.ID_FILIAL = i.ID_FILIAL AND m.ID_DB = i.ID_DB
    WHERE i.ID_FILIAL = {int(id_filial)}
      AND m.DATA >= '{dt_from}' AND m.DATA < '{dt_to}'
    """
    upsert_itens = """
    INSERT INTO stg.itensmovprodutos (
      id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, payload, dt_evento,
      id_produto_shadow, id_grupo_produto_shadow, qtd_shadow, custo_unitario_shadow,
      valor_unitario_shadow, total_shadow, desconto_shadow,
      ingested_at, received_at
    ) VALUES (
      %s,%s,%s,%s,%s,%s,
      (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'),
      %s,%s,%s,%s,%s,%s,%s, now(), now()
    )
    ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO UPDATE SET
      payload = EXCLUDED.payload,
      dt_evento = EXCLUDED.dt_evento,
      id_produto_shadow = EXCLUDED.id_produto_shadow,
      qtd_shadow = EXCLUDED.qtd_shadow,
      custo_unitario_shadow = EXCLUDED.custo_unitario_shadow,
      ingested_at = now(), received_at = now()
    """
    n_itens = 0
    with pg.cursor() as cur:
        for batch in _fetch(xpert, sql_itens, batch=3000):
            params = []
            for r in batch:
                iid = _to_int(r.get("ID_ITENSMOVPRODUTOS"))
                mid = _to_int(r.get("ID_MOVPRODUTOS"))
                id_db = _to_int(r.get("ID_DB"))
                if iid is None or mid is None or id_db is None:
                    continue
                payload = _row_payload(r)
                payload["ID_MOVPRODUTOS"] = mid
                payload["ID_PRODUTOS"] = _to_int(r.get("ID_PRODUTOS"))
                payload["CFOP"] = r.get("CFOP")
                payload["QTDE"] = _jsonable(r.get("QTDE"))
                payload["VLRCUSTOCOMICMS"] = _jsonable(r.get("VLRCUSTOCOMICMS"))
                custo = r.get("VLRCUSTOCOMICMS")
                if custo is None:
                    custo = r.get("VLRCUSTO")
                params.append(
                    (
                        id_empresa,
                        id_filial,
                        id_db,
                        mid,
                        iid,
                        Json(payload),
                        _jsonable(r.get("MOV_DATA")),
                        _to_int(r.get("ID_PRODUTOS")),
                        _to_int(r.get("ID_GRUPOPRODUTOS")),
                        float(r["QTDE"]) if r.get("QTDE") is not None else None,
                        float(custo) if custo is not None else None,
                        float(r["VLRUNITARIO"]) if r.get("VLRUNITARIO") is not None else None,
                        float(r["TOTAL"]) if r.get("TOTAL") is not None else None,
                        float(r["VLRDESCONTO"]) if r.get("VLRDESCONTO") is not None else None,
                    )
                )
            if params:
                execute_batch(cur, upsert_itens, params, page_size=400)
                n_itens += len(params)
                print(f"  itensmov +{len(params)} (total {n_itens})", flush=True)
                if n_itens % 50000 < 3000:
                    pg.commit()
        pg.commit()
    return n_mov, n_itens


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xpert-env", default="config/source-explorer.env")
    ap.add_argument("--homolog-env", default=os.environ.get("ENV_FILE", "/etc/torqmind/homolog.app.env"))
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--id-filial", type=int, default=14458)
    ap.add_argument("--sales-from", default="2026-02-01", help="Janela comprovantes/formas (inicio)")
    ap.add_argument("--sales-to", default="2026-07-01", help="Janela comprovantes/formas (fim exclusivo)")
    ap.add_argument("--mov-from", default="2025-01-01", help="Janela movprodutos (inicio)")
    ap.add_argument("--mov-to", default="2026-07-01", help="Janela movprodutos (fim exclusivo)")
    ap.add_argument("--skip-mov", action="store_true")
    ap.add_argument("--skip-sales", action="store_true")
    args = ap.parse_args()

    env = _load_env_file(args.homolog_env)
    xcfg = Config(args.xpert_env)
    xpert = get_connection(xcfg)
    pg, execute_batch, Json = _pg_connect(env)

    t0 = time.time()
    print(f"Destino OK: {env.get('POSTGRES_DB')} / APP_ENV={env.get('APP_ENV')}")
    print(f"Filial {args.id_filial} empresa {args.id_empresa}")

    n_conv = seed_convenios(xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial)
    print(f"convenios: {n_conv}")
    n_saldo = seed_saldoclientes(xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial)
    print(f"saldoclientes: {n_saldo}")
    n_cr = seed_contasreceber(xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial)
    print(f"contasreceber: {n_cr}")
    n_cred = seed_credito(xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial)
    print(f"credito(havel): {n_cred}")

    if not args.skip_sales:
        n_comp = seed_comprovantes(
            xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial, args.sales_from, args.sales_to
        )
        print(f"comprovantes: {n_comp}")
        n_formas = seed_formas(
            xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial, args.sales_from, args.sales_to
        )
        print(f"formas_pgto: {n_formas}")

    if not args.skip_mov:
        n_mov, n_itens = seed_movimentos(
            xpert, pg, execute_batch, Json, args.id_empresa, args.id_filial, args.mov_from, args.mov_to
        )
        print(f"movprodutos: {n_mov}")
        print(f"itensmovprodutos: {n_itens}")

    xpert.close()
    pg.close()
    print(f"DONE in {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
