#!/usr/bin/env python3
"""Backfill ANP (homolog): Xpert → STG PG → mart_anp_compliance (CH WRITE).

READ-ONLY no SQL Server. Upsert STG + INSERT mart (CREATE/INSERT only).
"""
from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "apps" / "api"))

XPERT_ENV = ROOT / "config" / "source-explorer.env"
HOMOLOG_ENV = Path("/etc/torqmind/homolog.app.env")
ANALYTICS_ENV = Path("/etc/torqmind/prod.analytics.env")
ID_EMPRESA = int(os.environ.get("ANP_ID_EMPRESA", "1"))
LOOKBACK_DAYS = int(os.environ.get("ANP_LOOKBACK_DAYS", "0"))
# Default: desde 2025-10-01 (buffer ASOF) até hoje; eventos de tela filtram jan/2026+
DEFAULT_CUTOFF = os.environ.get("ANP_CUTOFF", "2025-10-01")
PRECO_DELTA_MIN = 0.03


def _load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _xpert_connect(env: dict[str, str]):
    host = env["SQLSERVER_HOST"]
    port = int(env.get("SQLSERVER_PORT") or 1433)
    database = env["SQLSERVER_DATABASE"]
    user = env["SQLSERVER_USER"]
    password = env["SQLSERVER_PASSWORD"]
    try:
        import pymssql

        return pymssql.connect(
            server=host, port=port, user=user, password=password, database=database, login_timeout=20
        ), "pymssql"
    except Exception:
        import pyodbc

        driver = env.get("SQLSERVER_DRIVER") or "ODBC Driver 17 for SQL Server"
        encrypt = "yes" if env.get("SQLSERVER_ENCRYPT", "no").lower() in ("1", "true", "yes") else "no"
        trust = "yes" if env.get("SQLSERVER_TRUST_CERT", "yes").lower() in ("1", "true", "yes") else "no"
        dsn = (
            f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
            f"UID={user};PWD={password};Encrypt={encrypt};TrustServerCertificate={trust}"
        )
        return pyodbc.connect(dsn, timeout=20), "pyodbc"


def _fetchall(conn, driver: str, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        item = {}
        for i, col in enumerate(cols):
            val = row[i]
            if isinstance(val, Decimal):
                val = float(val)
            elif isinstance(val, datetime):
                val = val
            item[col] = val
        rows.append(item)
    cur.close()
    return rows


def _json_default(obj: Any):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(type(obj))


def _upsert_preco(pg, id_empresa: int, rows: list[dict[str, Any]]) -> int:
    n = 0
    with pg.cursor() as cur:
        for r in rows:
            payload = json.dumps(r, default=_json_default, ensure_ascii=False)
            dt = r.get("DATAALTERACAO") or r.get("TORQMIND_DT_EVENTO")
            cur.execute(
                """
                INSERT INTO stg.preco_bomba_hist (
                  id_empresa, id_filial, id_db, id_produto, id_evento,
                  dt_alteracao_shadow, preco_venda_shadow, preco_anterior_shadow,
                  id_bico_shadow, payload, dt_evento, id_db_shadow, ingested_at
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s, now()
                )
                ON CONFLICT (id_empresa, id_filial, id_db, id_produto, id_evento) DO UPDATE SET
                  dt_alteracao_shadow = EXCLUDED.dt_alteracao_shadow,
                  preco_venda_shadow = EXCLUDED.preco_venda_shadow,
                  id_bico_shadow = EXCLUDED.id_bico_shadow,
                  payload = EXCLUDED.payload,
                  dt_evento = EXCLUDED.dt_evento,
                  ingested_at = now()
                """,
                (
                    id_empresa,
                    int(r["ID_FILIAL"]),
                    int(r.get("ID_DB") or 0),
                    int(r["ID_PRODUTOS"]),
                    int(r["ID_EVENTO"]),
                    dt,
                    float(r["PRECO"]) if r.get("PRECO") is not None else None,
                    None,
                    int(r["ID_BICOS"]) if r.get("ID_BICOS") is not None else None,
                    payload,
                    dt,
                    int(r.get("ID_DB") or 0),
                ),
            )
            n += 1
    return n


def _upsert_nfe(pg, id_empresa: int, rows: list[dict[str, Any]]) -> int:
    n = 0
    with pg.cursor() as cur:
        for r in rows:
            payload = json.dumps(r, default=_json_default, ensure_ascii=False)
            dt = r.get("DATAENTRADA") or r.get("TORQMIND_DT_EVENTO")
            cur.execute(
                """
                INSERT INTO stg.nfe_entrada (
                  id_empresa, id_filial, id_db, id_nota,
                  chave_acesso_shadow, numero_nota_shadow, dt_entrada_shadow, dt_emissao_shadow,
                  payload, dt_evento, id_db_shadow, ingested_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s, now())
                ON CONFLICT (id_empresa, id_filial, id_db, id_nota) DO UPDATE SET
                  chave_acesso_shadow = EXCLUDED.chave_acesso_shadow,
                  numero_nota_shadow = EXCLUDED.numero_nota_shadow,
                  dt_entrada_shadow = EXCLUDED.dt_entrada_shadow,
                  dt_emissao_shadow = EXCLUDED.dt_emissao_shadow,
                  payload = EXCLUDED.payload,
                  dt_evento = EXCLUDED.dt_evento,
                  ingested_at = now()
                """,
                (
                    id_empresa,
                    int(r["ID_FILIAL"]),
                    int(r["ID_DB"]),
                    int(r["ID_NOTA"]),
                    r.get("CHAVEACESSO"),
                    str(r.get("NUMERO") or "")[:20] or None,
                    dt,
                    r.get("DATA"),
                    payload,
                    dt,
                    int(r["ID_DB"]),
                ),
            )
            n += 1
    return n


def _upsert_itens(pg, id_empresa: int, rows: list[dict[str, Any]]) -> int:
    n = 0
    with pg.cursor() as cur:
        for r in rows:
            payload = json.dumps(r, default=_json_default, ensure_ascii=False)
            qtd = float(r["QUANTIDADE"]) if r.get("QUANTIDADE") is not None else None
            custo_u = r.get("CUSTO_UNITARIO")
            if custo_u is None:
                custo_u = r.get("VLRCUSTO") or r.get("VLRUNITARIO")
            custo_u = float(custo_u) if custo_u is not None else None
            custo_t = float(r["VLRTOTALITEM"]) if r.get("VLRTOTALITEM") is not None else None
            if custo_t is None and custo_u is not None and qtd is not None:
                custo_t = custo_u * qtd
            tip = int(r["TIPOCOMBUSTIVEL"] or 0)
            nome = str(r.get("NOMEPRODUTO") or "").upper().strip()
            codigo_anp = str(r.get("CODIGOANP") or "").strip()
            unidade = str(r.get("UNIDADE") or "").strip().upper()
            eh_comb = tip > 0
            if not eh_comb and codigo_anp and unidade == "LT":
                eh_comb = True
            if not eh_comb and nome:
                eh_comb = (
                    nome.startswith("GASOLINA")
                    or nome.startswith("ETANOL")
                    or nome.startswith("OLEO DIESEL")
                    or nome.startswith("DIESEL")
                )
            if not eh_comb:
                continue
            dt = r.get("TORQMIND_DT_EVENTO")
            cur.execute(
                """
                INSERT INTO stg.itens_nfe_entrada (
                  id_empresa, id_filial, id_db, id_nota, id_item,
                  id_produto_shadow, qtd_shadow, custo_unitario_shadow, custo_total_shadow,
                  eh_combustivel_shadow, payload, dt_evento, id_db_shadow, ingested_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s, now())
                ON CONFLICT (id_empresa, id_filial, id_db, id_nota, id_item) DO UPDATE SET
                  id_produto_shadow = EXCLUDED.id_produto_shadow,
                  qtd_shadow = EXCLUDED.qtd_shadow,
                  custo_unitario_shadow = EXCLUDED.custo_unitario_shadow,
                  custo_total_shadow = EXCLUDED.custo_total_shadow,
                  eh_combustivel_shadow = EXCLUDED.eh_combustivel_shadow,
                  payload = EXCLUDED.payload,
                  dt_evento = EXCLUDED.dt_evento,
                  ingested_at = now()
                """,
                (
                    id_empresa,
                    int(r["ID_FILIAL"]),
                    int(r["ID_DB"]),
                    int(r["ID_NOTA"]),
                    int(r["ID_ITEM"]),
                    int(r["ID_PRODUTOS"]) if r.get("ID_PRODUTOS") is not None else None,
                    qtd,
                    custo_u,
                    custo_t,
                    tip > 0,
                    payload,
                    dt,
                    int(r["ID_DB"]),
                ),
            )
            n += 1
    return n


def _build_events_from_stg(pg, id_empresa: int) -> list[dict[str, Any]]:
    """Detecta aumento de PPL (média por produto/dia) e lastro ASOF do custo de entrada."""
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT id_filial, id_produto,
                   (payload->>'NOMEPRODUTO') AS nome_produto,
                   date_trunc('day', dt_alteracao_shadow) AS dia,
                   AVG(preco_venda_shadow)::float8 AS ppl
              FROM stg.preco_bomba_hist
             WHERE id_empresa = %s
               AND preco_venda_shadow > 0
               AND dt_alteracao_shadow IS NOT NULL
             GROUP BY 1,2,3,4
             ORDER BY 1,2,4
            """,
            (id_empresa,),
        )
        series = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT i.id_filial, i.id_produto_shadow AS id_produto,
                   i.custo_unitario_shadow::float8 AS custo,
                   COALESCE(n.dt_entrada_shadow, i.dt_evento) AS dt_entrada,
                   n.chave_acesso_shadow AS chave,
                   n.numero_nota_shadow AS numero,
                   n.cnpj_emitente_shadow AS cnpj
              FROM stg.itens_nfe_entrada i
              JOIN stg.nfe_entrada n
                ON n.id_empresa=i.id_empresa AND n.id_filial=i.id_filial
               AND n.id_db=i.id_db AND n.id_nota=i.id_nota
             WHERE i.id_empresa = %s
               AND COALESCE(i.eh_combustivel_shadow, false)
               AND i.custo_unitario_shadow > 0
             ORDER BY i.id_filial, i.id_produto_shadow, dt_entrada
            """,
            (id_empresa,),
        )
        custos = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id_filial, COALESCE(NULLIF(btrim(apelido), ''), nome) AS nome
              FROM auth.filiais WHERE id_empresa = %s
            """,
            (id_empresa,),
        )
        apelidos = {int(r["id_filial"]): r["nome"] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT limite_alerta_amarelo_perc::float8 AS alerta,
                   limite_abusivo_anp_perc::float8 AS abusivo
              FROM app.anp_compliance_config
             WHERE id_empresa = %s AND id_filial = 0 AND ativo
             LIMIT 1
            """,
            (id_empresa,),
        )
        cfg = cur.fetchone() or {"alerta": 50.0, "abusivo": 70.0}
        alerta = float(cfg["alerta"] if isinstance(cfg, dict) else cfg[0])
        abusivo = float(cfg["abusivo"] if isinstance(cfg, dict) else cfg[1])

    def asof_custo(id_filial: int, id_produto: int, as_of: date):
        best = None
        for c in custos:
            if int(c["id_filial"]) != id_filial or int(c["id_produto"] or 0) != id_produto:
                continue
            dt = c["dt_entrada"]
            if dt is None:
                continue
            d = dt.date() if isinstance(dt, datetime) else dt
            if d <= as_of:
                best = c
        return best

    from collections import defaultdict

    by: dict[tuple[int, int], list] = defaultdict(list)
    for r in series:
        by[(int(r["id_filial"]), int(r["id_produto"]))].append(r)

    now = datetime.now(timezone.utc)
    events = []
    for (fil, prod), rows in by.items():
        rows = sorted(rows, key=lambda x: x["dia"])
        prev = None
        for cur in rows:
            if prev is None:
                prev = cur
                continue
            p_ant = float(prev["ppl"])
            p_novo = float(cur["ppl"])
            if p_novo < p_ant + PRECO_DELTA_MIN:
                prev = cur
                continue
            dia = cur["dia"]
            d_evt = dia.date() if isinstance(dia, datetime) else dia
            d_prev = prev["dia"]
            d_prev = d_prev.date() if isinstance(d_prev, datetime) else d_prev
            c_ant = asof_custo(fil, prod, d_prev)
            c_novo = asof_custo(fil, prod, d_evt)
            if not c_ant or not c_novo:
                # sem lastro NFe → ainda registra com SEM_LASTRO se quiser; pular se sem custo
                status = "SEM_LASTRO"
                custo_ant = custo_novo = 0.0
                margem_ant = margem_nova = 0.0
                variacao = None
                chave = numero = cnpj = ""
                origem = "lmc_sem_nfe"
            else:
                custo_ant = float(c_ant["custo"])
                custo_novo = float(c_novo["custo"])
                margem_ant = p_ant - custo_ant
                margem_nova = p_novo - custo_novo
                if abs(margem_ant) < 1e-6:
                    status = "SEM_LASTRO"
                    variacao = None
                else:
                    variacao = ((margem_nova - margem_ant) / margem_ant) * 100.0
                    if variacao >= abusivo:
                        status = "RISCO_ABUSIVO"
                    elif variacao >= alerta:
                        status = "ALERTA"
                    else:
                        status = "OK"
                chave = c_novo.get("chave") or ""
                numero = c_novo.get("numero") or ""
                cnpj = c_novo.get("cnpj") or ""
                origem = "lmc_asof_entrada"

            events.append(
                {
                    "id_empresa": id_empresa,
                    "id_filial": fil,
                    "id_produto": prod,
                    "nome_resumido": apelidos.get(fil) or "",
                    "nome_produto": cur.get("nome_produto") or "",
                    "dt_alteracao_preco": datetime(d_evt.year, d_evt.month, d_evt.day, 12, 0, 0),
                    "data_alteracao": d_evt.isoformat(),
                    "preco_venda_anterior": round(p_ant, 4),
                    "preco_venda_novo": round(p_novo, 4),
                    "custo_nfe_anterior": round(custo_ant, 6),
                    "custo_nfe_novo": round(custo_novo, 6),
                    "margem_anterior": round(margem_ant, 6),
                    "margem_nova": round(margem_nova, 6),
                    "variacao_margem_pct": None if variacao is None else round(variacao, 4),
                    "limite_alerta_perc": alerta,
                    "limite_abusivo_perc": abusivo,
                    "status": status,
                    "chave_nfe_anterior": "",
                    "chave_nfe_nova": chave or "",
                    "cnpj_emitente_nova": cnpj or "",
                    "numero_nota_nova": str(numero or ""),
                    "dt_entrada_nfe_nova": None,
                    "origem": origem,
                    "published_at": now,
                }
            )
            prev = cur
    events.sort(
        key=lambda e: (
            str(e.get("nome_resumido") or ""),
            int(e.get("id_filial") or 0),
            str(e.get("data_alteracao") or ""),
            e.get("nome_produto") or "",
        )
    )
    return events


def _ch_insert(env: dict[str, str], rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    # Use clickhouse HTTP JSONEachRow
    host = env.get("CLICKHOUSE_HOST", "172.30.0.9")
    port = env.get("CLICKHOUSE_PORT", "8123")
    user = env.get("CLICKHOUSE_USER", "torqmind")
    password = env.get("CLICKHOUSE_PASSWORD", "")
    qs = urllib.parse.urlencode(
        {
            "user": user,
            "password": password,
            "query": "INSERT INTO torqmind_mart_rt.mart_anp_compliance FORMAT JSONEachRow",
        }
    )
    body = "\n".join(json.dumps(r, default=_json_default) for r in rows).encode("utf-8")
    req = urllib.request.Request(f"http://{host}:{port}/?{qs}", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        resp.read()
    return len(rows)


def main() -> None:
    xenv = _load_env(XPERT_ENV)
    henv = _load_env(HOMOLOG_ENV)
    aenv = _load_env(ANALYTICS_ENV)
    if LOOKBACK_DAYS > 0:
        cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    else:
        cutoff = DEFAULT_CUTOFF

    print(f"connecting xpert lookback={LOOKBACK_DAYS}d empresa={ID_EMPRESA}")
    xconn, driver = _xpert_connect(xenv)
    print("xpert", driver, "ok")

    preco_sql = f"""
    SELECT lb.ID_FILIAL, CAST(0 AS int) AS ID_DB, l.ID_PRODUTOS, lb.ID_LMCBICOS AS ID_EVENTO,
           lb.ID_BICOS, lb.PPL AS PRECO, p.NOMEPRODUTO,
           CAST(l.DTACONTA AS datetime2) AS DATAALTERACAO,
           CAST(l.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO
    FROM dbo.LMCBICOS lb WITH (NOLOCK)
    INNER JOIN dbo.LMC l WITH (NOLOCK) ON l.ID_FILIAL=lb.ID_FILIAL AND l.ID_LMC=lb.ID_LMC
    LEFT JOIN dbo.PRODUTOS p WITH (NOLOCK) ON p.ID_FILIAL=l.ID_FILIAL AND p.ID_PRODUTOS=l.ID_PRODUTOS
    WHERE ISNULL(lb.PPL,0) > 0 AND l.DTACONTA >= '{cutoff}'
    """
    nfe_sql = f"""
    SELECT c.ID_FILIAL, c.ID_DB, c.ID_COMPROVANTE AS ID_NOTA, c.NROCOMPROVANTE AS NUMERO,
           ce.CHAVEACESSONFE AS CHAVEACESSO,
           CAST(c.DATA AS datetime2) AS DATAENTRADA,
           CAST(COALESCE(c.DTAEMISSAO, c.DATA) AS datetime2) AS DATA,
           CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO
    FROM dbo.COMPROVANTES c WITH (NOLOCK)
    INNER JOIN dbo.COMPENTRADAS ce WITH (NOLOCK)
      ON ce.ID_FILIAL=c.ID_FILIAL AND ce.ID_DB=c.ID_DB AND ce.ID_COMPROVANTE=c.ID_COMPROVANTE
    WHERE ISNULL(c.SAIDAS_ENTRADAS,0)=1 AND c.DATA >= '{cutoff}'
    """
    itens_sql = f"""
    SELECT i.ID_FILIAL, i.ID_DB, i.ID_COMPROVANTE AS ID_NOTA,
           i.ID_ITENSCOMPROVANTE AS ID_ITEM, i.ID_PRODUTOS, i.QTDE AS QUANTIDADE,
           i.VLRCUSTO, i.VLRUNITARIO, i.VLRTOTALITEM,
           COALESCE(NULLIF(i.VLRCUSTO,0), i.VLRUNITARIO) AS CUSTO_UNITARIO,
           p.NOMEPRODUTO, p.TIPOCOMBUSTIVEL, p.CODIGOANP, p.UNIDADE,
           CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO
    FROM dbo.ITENSCOMPROVANTE i WITH (NOLOCK)
    INNER JOIN dbo.COMPROVANTES c WITH (NOLOCK)
      ON c.ID_FILIAL=i.ID_FILIAL AND c.ID_DB=i.ID_DB AND c.ID_COMPROVANTE=i.ID_COMPROVANTE
    INNER JOIN dbo.COMPENTRADAS ce WITH (NOLOCK)
      ON ce.ID_FILIAL=i.ID_FILIAL AND ce.ID_DB=i.ID_DB AND ce.ID_COMPROVANTE=i.ID_COMPROVANTE
    INNER JOIN dbo.PRODUTOS p WITH (NOLOCK)
      ON p.ID_FILIAL=i.ID_FILIAL AND p.ID_PRODUTOS=i.ID_PRODUTOS
    WHERE ISNULL(c.SAIDAS_ENTRADAS,0)=1 AND c.DATA >= '{cutoff}'
      AND (
        ISNULL(p.TIPOCOMBUSTIVEL,0) > 0
        OR (
          NULLIF(LTRIM(RTRIM(CAST(p.CODIGOANP AS varchar(32)))), '') IS NOT NULL
          AND UPPER(LTRIM(RTRIM(CAST(p.UNIDADE AS varchar(16))))) = 'LT'
        )
        OR p.NOMEPRODUTO LIKE 'GASOLINA%'
        OR p.NOMEPRODUTO LIKE 'ETANOL%'
        OR p.NOMEPRODUTO LIKE 'OLEO DIESEL%'
        OR p.NOMEPRODUTO LIKE 'DIESEL%'
      )
    """

    preco = _fetchall(xconn, driver, preco_sql)
    nfe = _fetchall(xconn, driver, nfe_sql)
    itens = _fetchall(xconn, driver, itens_sql)
    xconn.close()
    print(f"fetched preco={len(preco)} nfe={len(nfe)} itens={len(itens)}")

    import psycopg
    from psycopg.rows import dict_row

    db_url = henv.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("homolog DATABASE_URL missing")
    with psycopg.connect(db_url, row_factory=dict_row) as pg:
        n1 = _upsert_preco(pg, ID_EMPRESA, preco)
        n2 = _upsert_nfe(pg, ID_EMPRESA, nfe)
        n3 = _upsert_itens(pg, ID_EMPRESA, itens)
        pg.commit()
        print(f"stg upsert preco={n1} nfe={n2} itens={n3}")
        events = _build_events_from_stg(pg, ID_EMPRESA)
        print(f"events_built={len(events)}")
        if events:
            from collections import Counter

            print("status", dict(Counter(e["status"] for e in events)))
            sample = events[0]
            print(
                "sample",
                sample.get("nome_resumido"),
                sample.get("nome_produto"),
                sample.get("data_alteracao"),
                sample.get("preco_venda_anterior"),
                "->",
                sample.get("preco_venda_novo"),
                sample.get("variacao_margem_pct"),
                sample.get("status"),
                sample.get("origem"),
            )

    inserted = _ch_insert(aenv, events)
    print(f"ch_inserted={inserted}")
    print("PASS" if n1 > 0 else "WARN_EMPTY_PRECO")


if __name__ == "__main__":
    main()
