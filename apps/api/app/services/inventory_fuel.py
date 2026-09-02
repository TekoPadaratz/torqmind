"""Estoque de combustíveis — publish PG STG → ClickHouse mart_rt.

Fonte canônica (validada migração 100):
- Capacidade / produto: stg.tanques (CAPACIDADE, ID_PRODUTOS)
- Litros base: última LEITURA positiva em stg.movtanques (sensor / abertura)
- Estoque exibido na API: LEITURA + rateio×(entradas − saídas) **do dia de negócio**
  (mesma lógica de movimentação da aferição), via marts diárias CH
- Custo: stg.produtos.CUSTOMEDIO
- Nunca usar stg.estoque para combustível.

Movimentação documentada (aferição / estoque ao vivo):
- Saídas: itens de comprovante de venda comercialmente ativos
  (não cancelado, situacao=1, CFOP saída; CFOP via shadow com fallback payload).
- Entradas: NFe de compra (COMPROVANTES SAIDAS_ENTRADAS=1 + COMPENTRADAS) e
  movimentação MOVPRODUTOS (SAIDAS_ENTRADAS 1/2) para produtos de tanque.
  Comprovante ativo quando resolvível (não cancelado, situacao=1).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.db import get_conn
from app.db_clickhouse import execute_command, insert_batch
from app.sales_semantics import SALE_STATUS

logger = logging.getLogger(__name__)

MART_TABLE = "torqmind_mart_rt.mart_inventory_tanks_rt"
READINGS_TABLE = "torqmind_mart_rt.mart_inventory_tank_readings_rt"
SALES_DAILY_TABLE = "torqmind_mart_rt.mart_inventory_fuel_sales_daily_rt"
ENTRIES_DAILY_TABLE = "torqmind_mart_rt.mart_inventory_fuel_entries_daily_rt"
FRESH_DAYS = 7
READINGS_DAYS = 120

# NFe fiscal: 4=cancelada, 5=inutilização — nunca contam como movimento de tanque.
_NFE_EXCLUDED_STATUSES = (4, 5)


def _comprovante_ativo_sql(alias: str = "c") -> str:
    """Comprovante comercialmente ativo (venda ou entrada)."""
    return (
        f"coalesce({alias}.cancelado_shadow, false) IS NOT TRUE "
        f"AND coalesce({alias}.situacao_shadow, {SALE_STATUS}) = {SALE_STATUS}"
    )


def _item_cfop_sql(alias: str = "i") -> str:
    """CFOP tipado; fallback no payload quando shadow veio 0/NULL (caso combustível)."""
    return (
        f"coalesce("
        f"nullif({alias}.cfop_shadow, 0), "
        f"nullif(replace(coalesce({alias}.payload->>'CFOP',''), '.', ''), '')::int, "
        f"0)"
    )


def _nfe_venda_ok_sql(alias: str = "nfe") -> str:
    excluded = ", ".join(str(s) for s in _NFE_EXCLUDED_STATUSES)
    return (
        f"({alias}.status_shadow IS NULL "
        f"OR {alias}.status_shadow NOT IN ({excluded}))"
    )


def _purge_daily_window(table: str, id_empresa: int, cutoff) -> None:
    """Remove snapshot da janela antes do republish (evita litros fantasma de nota cancelada)."""
    execute_command(
        f"""
        ALTER TABLE {table}
        DELETE WHERE id_empresa = {{id_empresa:Int32}} AND dia >= toDate({{cutoff:String}})
        SETTINGS mutations_sync = 1
        """,
        {"id_empresa": int(id_empresa), "cutoff": str(cutoff)},
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ativo_sql(alias: str = "t") -> str:
    return f"""
              CASE
                WHEN coalesce(nullif({alias}.payload->>'ATIVO',''),'1')
                     IN ('0','false','False','f','N','n')
                THEN 0 ELSE 1
              END
    """


def _produto_nome_sql() -> str:
    return """
              left(coalesce(
                nullif(dim.nome, ''),
                nullif(pr.payload->>'DESCRICAO', ''),
                nullif(pr.payload->>'APELIDO', ''),
                nullif(pr.payload->>'DESCRICAOREDUZIDA', ''),
                ''
              ), 80)
    """


def fetch_tank_snapshot(role: str, id_empresa: int) -> List[Dict[str, Any]]:
    """Mash PG: última leitura por tanque + capacidade + custo."""
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            WITH ult AS (
              SELECT DISTINCT ON (
                m.id_empresa, m.id_filial, (m.payload->>'ID_TANQUES')::int
              )
                m.id_empresa,
                m.id_filial,
                (m.payload->>'ID_TANQUES')::int AS id_tanque,
                etl.safe_numeric(m.payload->>'LEITURA') AS litros,
                (etl.safe_timestamp(m.payload->>'DTACONTA'))::date AS data_leitura
              FROM stg.movtanques m
              WHERE m.id_empresa = %s
                AND etl.safe_numeric(m.payload->>'LEITURA') > 0
                AND etl.safe_timestamp(m.payload->>'DTACONTA') IS NOT NULL
                AND coalesce(nullif(m.payload->>'ID_TANQUES','')::int, 0) > 0
              ORDER BY
                m.id_empresa,
                m.id_filial,
                (m.payload->>'ID_TANQUES')::int,
                etl.safe_timestamp(m.payload->>'DTACONTA') DESC NULLS LAST
            )
            SELECT
              t.id_empresa,
              t.id_filial,
              t.id_tanque,
              coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto,
              left(coalesce(
                nullif(dim.nome, ''),
                nullif(pr.payload->>'DESCRICAO', ''),
                nullif(pr.payload->>'APELIDO', ''),
                nullif(pr.payload->>'DESCRICAOREDUZIDA', ''),
                ''
              ), 80) AS produto_nome,
              coalesce(etl.safe_numeric(t.payload->>'CAPACIDADE'), 0) AS capacidade_l,
              coalesce(u.litros, 0) AS estoque_l,
              coalesce(
                nullif(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'), 0),
                dim.custo_medio,
                0
              ) AS custo_unitario,
              u.data_leitura,
              CASE
                WHEN u.data_leitura IS NOT NULL
                 AND u.data_leitura >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s
                THEN 1 ELSE 0
              END AS leitura_fresca,
              CASE
                WHEN coalesce(nullif(t.payload->>'ATIVO',''),'1')
                     IN ('0','false','False','f','N','n')
                THEN 0 ELSE 1
              END AS ativo
            FROM stg.tanques t
            LEFT JOIN ult u
              ON u.id_empresa = t.id_empresa
             AND u.id_filial = t.id_filial
             AND u.id_tanque = t.id_tanque
            LEFT JOIN stg.produtos pr
              ON pr.id_empresa = t.id_empresa
             AND pr.id_filial = t.id_filial
             AND pr.id_produto = coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0)
            LEFT JOIN dw.dim_produto dim
              ON dim.id_empresa = t.id_empresa
             AND dim.id_filial = t.id_filial
             AND dim.id_produto = coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0)
            WHERE t.id_empresa = %s
              AND coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) > 0
            ORDER BY t.id_filial, t.id_tanque
            """,
            [id_empresa, FRESH_DAYS, id_empresa],
        ).fetchall()
    return [dict(r) for r in rows]


def publish_inventory_tanks(role: str, id_empresa: int) -> int:
    """Publica snapshot de tanques combustível → CH mart_rt."""
    rows = fetch_tank_snapshot(role, id_empresa)
    pub = _now()
    payload: List[Dict[str, Any]] = []
    for r in rows:
        if int(r.get("ativo") or 0) != 1:
            continue
        estoque = float(r.get("estoque_l") or 0)
        custo_u = float(r.get("custo_unitario") or 0)
        payload.append(
            {
                "id_empresa": int(r["id_empresa"]),
                "id_filial": int(r["id_filial"]),
                "id_tanque": int(r["id_tanque"]),
                "id_produto": int(r.get("id_produto") or 0),
                "produto_nome": str(r.get("produto_nome") or ""),
                "capacidade_l": float(r.get("capacidade_l") or 0),
                "estoque_l": estoque,
                "custo_unitario": custo_u,
                "custo_estoque": round(estoque * custo_u, 2),
                "data_leitura": r.get("data_leitura"),
                "leitura_fresca": int(r.get("leitura_fresca") or 0),
                "ativo": 1,
                "published_at": pub,
            }
        )
    n = insert_batch(
        MART_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_tanque"],
    )
    logger.info(
        "inventory_fuel publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        n,
    )
    return n


def fetch_tank_readings_daily(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> List[Dict[str, Any]]:
    """Uma LEITURA por tanque/dia (sensor na abertura — DTACONTA)."""
    days = max(7, min(int(days), 366))
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            f"""
            WITH diarias AS (
              SELECT DISTINCT ON (
                m.id_empresa,
                m.id_filial,
                (m.payload->>'ID_TANQUES')::int,
                (etl.safe_timestamp(m.payload->>'DTACONTA'))::date
              )
                m.id_empresa,
                m.id_filial,
                (m.payload->>'ID_TANQUES')::int AS id_tanque,
                (etl.safe_timestamp(m.payload->>'DTACONTA'))::date AS dia,
                etl.safe_numeric(m.payload->>'LEITURA') AS leitura_l
              FROM stg.movtanques m
              WHERE m.id_empresa = %s
                AND etl.safe_numeric(m.payload->>'LEITURA') > 0
                AND etl.safe_timestamp(m.payload->>'DTACONTA') IS NOT NULL
                AND coalesce(nullif(m.payload->>'ID_TANQUES','')::int, 0) > 0
                AND (etl.safe_timestamp(m.payload->>'DTACONTA'))::date
                    >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s
              ORDER BY
                m.id_empresa,
                m.id_filial,
                (m.payload->>'ID_TANQUES')::int,
                (etl.safe_timestamp(m.payload->>'DTACONTA'))::date,
                etl.safe_timestamp(m.payload->>'DTACONTA') DESC NULLS LAST
            )
            SELECT
              d.id_empresa,
              d.id_filial,
              d.id_tanque,
              coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto,
              {_produto_nome_sql()} AS produto_nome,
              coalesce(etl.safe_numeric(t.payload->>'CAPACIDADE'), 0) AS capacidade_l,
              d.dia,
              d.leitura_l,
              {_ativo_sql("t")} AS ativo
            FROM diarias d
            JOIN stg.tanques t
              ON t.id_empresa = d.id_empresa
             AND t.id_filial = d.id_filial
             AND t.id_tanque = d.id_tanque
            LEFT JOIN stg.produtos pr
              ON pr.id_empresa = t.id_empresa
             AND pr.id_filial = t.id_filial
             AND pr.id_produto = coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0)
            LEFT JOIN dw.dim_produto dim
              ON dim.id_empresa = t.id_empresa
             AND dim.id_filial = t.id_filial
             AND dim.id_produto = coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0)
            WHERE coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) > 0
            ORDER BY d.id_filial, d.id_tanque, d.dia
            """,
            [id_empresa, days],
        ).fetchall()
    return [dict(r) for r in rows]


def publish_tank_readings(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> int:
    """Publica histórico diário de LEITURA → CH mart_rt."""
    rows = fetch_tank_readings_daily(role, id_empresa, days=days)
    pub = _now()
    payload = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_tanque": int(r["id_tanque"]),
            "id_produto": int(r.get("id_produto") or 0),
            "produto_nome": str(r.get("produto_nome") or ""),
            "capacidade_l": float(r.get("capacidade_l") or 0),
            "dia": r["dia"],
            "leitura_l": float(r.get("leitura_l") or 0),
            "ativo": int(r.get("ativo") or 0),
            "published_at": pub,
        }
        for r in rows
        if int(r.get("ativo") or 0) == 1 and float(r.get("capacidade_l") or 0) > 0
    ]
    n = insert_batch(
        READINGS_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_tanque", "dia"],
    )
    logger.info(
        "inventory_fuel readings publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        n,
    )
    return n


def fetch_fuel_sales_daily(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> List[Dict[str, Any]]:
    """Litros vendidos/dia dos produtos de tanque — por filial (evita timeout).

    Raises RuntimeError se alguma filial falhar (evita purge parcial no publish).
    """
    from datetime import date, timedelta

    days = max(7, min(int(days), 366))
    cutoff = date.today() - timedelta(days=days)
    out: List[Dict[str, Any]] = []
    skipped: List[int] = []
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        conn.execute("SET LOCAL statement_timeout = '600s'")
        scopes = conn.execute(
            """
            SELECT DISTINCT
              t.id_filial,
              coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto
            FROM stg.tanques t
            WHERE t.id_empresa = %s
              AND coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) > 0
              AND coalesce((nullif(t.payload->>'CAPACIDADE',''))::numeric, 0) > 0
              AND coalesce(nullif(t.payload->>'ATIVO',''),'1')
                  NOT IN ('0','false','False','f','N','n')
            """,
            [id_empresa],
        ).fetchall()
        by_filial: Dict[int, List[int]] = {}
        for r in scopes:
            by_filial.setdefault(int(r["id_filial"]), []).append(int(r["id_produto"]))

        for id_filial, produtos in sorted(by_filial.items()):
            produtos = sorted({p for p in produtos if p > 0})
            if not produtos:
                continue
            try:
                cfop = _item_cfop_sql("i")
                # Dia de negócio = DTACONTA do comprovante (igual leitura do tanque /
                # sales_daily_rt). dt_evento UTC deslocava litros para o dia errado.
                dia_expr = (
                    "coalesce("
                    "(etl.safe_timestamp(c.payload->>'DTACONTA'))::date, "
                    "(c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date"
                    ")"
                )
                # Ativo = cancelado≠true + situacao=1. Exclui perda/transf/devolução.
                rows = conn.execute(
                    f"""
                    SELECT
                      i.id_empresa,
                      i.id_filial,
                      i.id_produto_shadow AS id_produto,
                      {dia_expr} AS dia,
                      sum(i.qtd_shadow)::numeric AS litros
                    FROM stg.itenscomprovantes i
                    JOIN stg.comprovantes c
                      ON c.id_empresa = i.id_empresa
                     AND c.id_filial = i.id_filial
                     AND c.id_db = i.id_db
                     AND c.id_comprovante = i.id_comprovante
                    WHERE i.id_empresa = %s
                      AND i.id_filial = %s
                      AND i.id_produto_shadow = ANY(%s)
                      AND i.qtd_shadow > 0
                      AND {cfop} > 5000
                      AND {cfop} NOT IN (1652, 5202, 5411, 5927, 5929, 6202, 6411, 6929)
                      AND {dia_expr} >= %s
                      AND {_comprovante_ativo_sql("c")}
                    GROUP BY 1, 2, 3, 4
                    """,
                    [id_empresa, id_filial, produtos, cutoff],
                ).fetchall()
                out.extend(dict(r) for r in rows)
            except Exception as exc:  # noqa: BLE001
                skipped.append(int(id_filial))
                logger.warning(
                    "inventory_fuel sales_daily skip filial=%s: %s",
                    id_filial,
                    str(exc)[:200],
                )
                try:
                    conn.rollback()
                    conn.execute("SET LOCAL statement_timeout = '600s'")
                except Exception:  # noqa: BLE001
                    pass
    if skipped:
        raise RuntimeError(
            f"inventory_fuel sales_daily incomplete; skipped filiais={skipped}"
        )
    return out


def publish_fuel_sales_daily(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> int:
    from datetime import date, timedelta

    days = max(7, min(int(days), 366))
    cutoff = date.today() - timedelta(days=days)
    rows = fetch_fuel_sales_daily(role, id_empresa, days=days)
    _purge_daily_window(SALES_DAILY_TABLE, id_empresa, cutoff)
    pub = _now()
    payload = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_produto": int(r["id_produto"]),
            "dia": r["dia"],
            "litros": float(r.get("litros") or 0),
            "published_at": pub,
        }
        for r in rows
    ]
    n = insert_batch(
        SALES_DAILY_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_produto", "dia"],
    )
    logger.info(
        "inventory_fuel sales_daily publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        n,
    )
    return n


def fetch_fuel_entries_daily(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> List[Dict[str, Any]]:
    """Litros de entrada (NF compra + MOVPRODUTOS) por produto/dia — combustível de tanque."""
    from datetime import date, timedelta

    days = max(7, min(int(days), 366))
    cutoff = date.today() - timedelta(days=days)
    ativo = _comprovante_ativo_sql("c")
    dia_nfe = (
        "coalesce("
        "(etl.safe_timestamp(c.payload->>'DTACONTA'))::date, "
        "(n.dt_entrada_shadow AT TIME ZONE 'America/Sao_Paulo')::date, "
        "(i.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date"
        ")"
    )
    dia_mov = (
        "coalesce("
        "(etl.safe_timestamp(m.payload->>'DTACONTA'))::date, "
        "(m.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date"
        ")"
    )
    se_mov = "COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1)"
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            f"""
            WITH tanque_prods AS (
              SELECT DISTINCT
                t.id_filial,
                coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto
              FROM stg.tanques t
              WHERE t.id_empresa = %s
                AND coalesce(nullif(t.payload->>'ID_PRODUTOS','')::int, 0) > 0
                AND coalesce((nullif(t.payload->>'CAPACIDADE',''))::numeric, 0) > 0
                AND coalesce(nullif(t.payload->>'ATIVO',''),'1')
                    NOT IN ('0','false','False','f','N','n')
            ),
            nfe_lines AS (
              SELECT
                i.id_empresa,
                i.id_filial,
                i.id_produto_shadow AS id_produto,
                {dia_nfe} AS dia,
                sum(coalesce(i.qtd_shadow, 0))::numeric AS litros
              FROM stg.itens_nfe_entrada i
              JOIN stg.nfe_entrada n
                ON n.id_empresa = i.id_empresa
               AND n.id_filial = i.id_filial
               AND n.id_db = i.id_db
               AND n.id_nota = i.id_nota
              JOIN stg.comprovantes c
                ON c.id_empresa = n.id_empresa
               AND c.id_filial = n.id_filial
               AND c.id_db = n.id_db
               AND c.id_comprovante = n.id_nota
              JOIN tanque_prods tp
                ON tp.id_filial = i.id_filial
               AND tp.id_produto = i.id_produto_shadow
              WHERE i.id_empresa = %s
                AND coalesce(i.qtd_shadow, 0) > 0
                AND i.id_produto_shadow IS NOT NULL
                AND {dia_nfe} >= %s
                AND (
                  coalesce(i.eh_combustivel_shadow, false) = true
                  OR i.id_produto_shadow IN (SELECT id_produto FROM tanque_prods)
                )
                AND {ativo}
              GROUP BY 1, 2, 3, 4
            ),
            mov_lines AS (
              SELECT
                im.id_empresa,
                im.id_filial,
                coalesce(im.id_produto_shadow, etl.safe_int(im.payload->>'ID_PRODUTOS')) AS id_produto,
                {dia_mov} AS dia,
                sum(
                  coalesce(
                    im.qtd_shadow,
                    etl.safe_numeric(im.payload->>'QTDE'),
                    0
                  )
                )::numeric AS litros
              FROM stg.itensmovprodutos im
              JOIN stg.movprodutos m
                ON m.id_empresa = im.id_empresa
               AND m.id_filial = im.id_filial
               AND m.id_db = im.id_db
               AND m.id_movprodutos = im.id_movprodutos
              JOIN tanque_prods tp
                ON tp.id_filial = im.id_filial
               AND tp.id_produto = coalesce(im.id_produto_shadow, etl.safe_int(im.payload->>'ID_PRODUTOS'))
              WHERE im.id_empresa = %s
                AND {se_mov} IN (1, 2)
                AND coalesce(im.id_produto_shadow, etl.safe_int(im.payload->>'ID_PRODUTOS')) > 0
                AND {dia_mov} >= %s
              GROUP BY 1, 2, 3, 4
            ),
            combined AS (
              SELECT * FROM nfe_lines
              UNION ALL
              SELECT * FROM mov_lines
            )
            SELECT
              id_empresa,
              id_filial,
              id_produto,
              dia,
              sum(litros)::numeric AS litros
            FROM combined
            GROUP BY 1, 2, 3, 4
            """,
            [id_empresa, id_empresa, id_empresa, cutoff, id_empresa, cutoff],
        ).fetchall()
    return [dict(r) for r in rows]


def publish_fuel_entries_daily(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> int:
    from datetime import date, timedelta

    days = max(7, min(int(days), 366))
    cutoff = date.today() - timedelta(days=days)
    rows = fetch_fuel_entries_daily(role, id_empresa, days=days)
    _purge_daily_window(ENTRIES_DAILY_TABLE, id_empresa, cutoff)
    pub = _now()
    payload = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_produto": int(r["id_produto"]),
            "dia": r["dia"],
            "litros": float(r.get("litros") or 0),
            "published_at": pub,
        }
        for r in rows
    ]
    n = insert_batch(
        ENTRIES_DAILY_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_produto", "dia"],
    )
    logger.info(
        "inventory_fuel entries_daily publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        n,
    )
    return n


def publish_inventory_fuel_bundle(
    role: str, id_empresa: int, days: int = READINGS_DAYS
) -> Dict[str, int]:
    """Snapshot atual + leituras + saídas + entradas diárias dos combustíveis."""
    return {
        "tanks": publish_inventory_tanks(role, id_empresa),
        "readings": publish_tank_readings(role, id_empresa, days=days),
        "sales_daily": publish_fuel_sales_daily(role, id_empresa, days=days),
        "entries_daily": publish_fuel_entries_daily(role, id_empresa, days=days),
    }
