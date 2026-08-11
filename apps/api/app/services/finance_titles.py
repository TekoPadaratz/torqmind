"""Publicação de títulos financeiros do STG PostgreSQL para a mart ClickHouse."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.db import get_conn
from app.db_clickhouse import insert_batch

logger = logging.getLogger(__name__)

MART_TABLE = "torqmind_mart_rt.mart_finance_titles_rt"
DEFAULT_DAYS = 180


def _now() -> datetime:
    return datetime.now(timezone.utc)


def fetch_finance_titles(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> List[Dict[str, Any]]:
    """Lê títulos alinhados ao Xpert Não Pagas/Não Recebidas (DTAPGTO IS NULL).

    Aberto = DTAPGTO nulo e saldo > 0.01 após baixas. Pagos recentes entram
    como tombstone (`status=pago`) para curar fantasma no ClickHouse.
    """
    days = max(7, min(int(days), 366))
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        conn.execute("SET LOCAL statement_timeout = 0")
        rows = conn.execute(
            """
            WITH baixa_receber AS (
              SELECT
                id_empresa, id_filial, id_db,
                etl.safe_int(payload->>'ID_CONTASRECEBER') AS id_titulo,
                sum(coalesce(etl.safe_numeric(payload->>'VALORBAIXA'), 0)) AS total_baixa,
                max((etl.safe_timestamp(payload->>'DATABAIXA'))::date) AS dt_ultima_baixa
              FROM stg.contasreceberbaixa
              WHERE id_empresa = %s
              GROUP BY id_empresa, id_filial, id_db, etl.safe_int(payload->>'ID_CONTASRECEBER')
            ),
            baixa_pagar AS (
              SELECT
                id_empresa, id_filial, id_db,
                etl.safe_int(payload->>'ID_CONTASPAGAR') AS id_titulo,
                sum(coalesce(etl.safe_numeric(payload->>'VALORBAIXA'), 0)) AS total_baixa,
                max((etl.safe_timestamp(payload->>'DATABAIXA'))::date) AS dt_ultima_baixa
              FROM stg.contaspagarbaixa
              WHERE id_empresa = %s
              GROUP BY id_empresa, id_filial, id_db, etl.safe_int(payload->>'ID_CONTASPAGAR')
            ),
            src AS (
              SELECT
                cp.id_empresa,
                cp.id_filial,
                0::smallint AS tipo_titulo,
                cp.id_contaspagar::bigint AS id_titulo,
                cp.id_db,
                coalesce(etl.safe_int(cp.payload->>'ID_ENTIDADE'), 0)::bigint AS id_entidade,
                coalesce(nullif(ent.payload->>'NOMEENTIDADE', ''), '') AS entidade_nome,
                (etl.safe_timestamp(cp.payload->>'DTACONTA'))::date AS dt_lancamento,
                (etl.safe_timestamp(cp.payload->>'DTAVCTO'))::date AS dt_vencimento,
                coalesce(etl.safe_numeric(cp.payload->>'VALOR'), 0)::numeric(18,2) AS valor,
                least(
                  coalesce(etl.safe_numeric(cp.payload->>'VALOR'), 0),
                  coalesce(etl.safe_numeric(cp.payload->>'VLRPAGO'), 0)
                  + coalesce(bp.total_baixa, 0)
                )::numeric(18,2) AS valor_pago,
                greatest(
                  0,
                  coalesce(etl.safe_numeric(cp.payload->>'VALOR'), 0)
                  - coalesce(etl.safe_numeric(cp.payload->>'VLRPAGO'), 0)
                  - coalesce(bp.total_baixa, 0)
                )::numeric(18,2) AS valor_aberto,
                (etl.safe_timestamp(cp.payload->>'DTAPGTO'))::date AS dt_pgto_flag,
                coalesce(
                  (etl.safe_timestamp(cp.payload->>'DTAPGTO'))::date,
                  bp.dt_ultima_baixa
                ) AS dt_pagamento
              FROM stg.contaspagar cp
              LEFT JOIN baixa_pagar bp
                ON bp.id_empresa = cp.id_empresa
               AND bp.id_filial = cp.id_filial
               AND bp.id_db = cp.id_db
               AND bp.id_titulo = cp.id_contaspagar
              LEFT JOIN stg.entidades ent
                ON ent.id_empresa = cp.id_empresa
               AND ent.id_filial = cp.id_filial
               AND ent.id_entidade = coalesce(etl.safe_int(cp.payload->>'ID_ENTIDADE'), 0)
              WHERE cp.id_empresa = %s

              UNION ALL

              SELECT
                cr.id_empresa,
                cr.id_filial,
                1::smallint AS tipo_titulo,
                cr.id_contasreceber::bigint AS id_titulo,
                cr.id_db,
                coalesce(etl.safe_int(cr.payload->>'ID_ENTIDADE'), 0)::bigint AS id_entidade,
                coalesce(nullif(ent.payload->>'NOMEENTIDADE', ''), '') AS entidade_nome,
                (etl.safe_timestamp(cr.payload->>'DTACONTA'))::date AS dt_lancamento,
                (etl.safe_timestamp(cr.payload->>'DTAVCTO'))::date AS dt_vencimento,
                coalesce(etl.safe_numeric(cr.payload->>'VALOR'), 0)::numeric(18,2) AS valor,
                least(
                  coalesce(etl.safe_numeric(cr.payload->>'VALOR'), 0),
                  coalesce(etl.safe_numeric(cr.payload->>'VLRPAGO'), 0)
                  + coalesce(br.total_baixa, 0)
                )::numeric(18,2) AS valor_pago,
                greatest(
                  0,
                  coalesce(etl.safe_numeric(cr.payload->>'VALOR'), 0)
                  - coalesce(etl.safe_numeric(cr.payload->>'VLRPAGO'), 0)
                  - coalesce(br.total_baixa, 0)
                )::numeric(18,2) AS valor_aberto,
                (etl.safe_timestamp(cr.payload->>'DTAPGTO'))::date AS dt_pgto_flag,
                coalesce(
                  (etl.safe_timestamp(cr.payload->>'DTAPGTO'))::date,
                  br.dt_ultima_baixa
                ) AS dt_pagamento
              FROM stg.contasreceber cr
              LEFT JOIN baixa_receber br
                ON br.id_empresa = cr.id_empresa
               AND br.id_filial = cr.id_filial
               AND br.id_db = cr.id_db
               AND br.id_titulo = cr.id_contasreceber
              LEFT JOIN stg.entidades ent
                ON ent.id_empresa = cr.id_empresa
               AND ent.id_filial = cr.id_filial
               AND ent.id_entidade = coalesce(etl.safe_int(cr.payload->>'ID_ENTIDADE'), 0)
              WHERE cr.id_empresa = %s
            )
            SELECT
              id_empresa, id_filial, tipo_titulo, id_titulo, id_db,
              id_entidade, entidade_nome, dt_lancamento, dt_vencimento,
              valor, valor_pago, valor_aberto,
              CASE
                -- Xpert "Não Pagas/Não Recebidas" = DTAPGTO IS NULL.
                WHEN dt_pgto_flag IS NOT NULL OR valor_aberto <= 0.01 THEN 'pago'
                WHEN dt_vencimento < (now() AT TIME ZONE 'America/Sao_Paulo')::date THEN 'vencido'
                ELSE 'a_vencer'
              END AS status
            FROM src
            WHERE dt_vencimento IS NOT NULL
              AND (
                (
                  -- Xpert Não Pagas: DTAPGTO nulo + saldo. DTACONTA futura é válida.
                  valor_aberto > 0.01
                  AND dt_pgto_flag IS NULL
                )
                OR (
                  -- Tombstone no CH: quitado/marcado pago recentemente.
                  (
                    valor_aberto <= 0.01
                    OR dt_pgto_flag IS NOT NULL
                  )
                  AND coalesce(dt_pagamento, dt_pgto_flag)
                      >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s
                )
              )
            ORDER BY id_filial ASC, dt_vencimento DESC, entidade_nome ASC, id_titulo ASC
            """,
            [id_empresa, id_empresa, id_empresa, id_empresa, days],
        ).fetchall()
    return [dict(row) for row in rows]


def publish_finance_titles(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> int:
    """Publica títulos financeiros elegíveis no ClickHouse."""
    rows = fetch_finance_titles(role, id_empresa, days=days)
    published_at = _now()
    payload = [
        {
            "id_empresa": int(row["id_empresa"]),
            "id_filial": int(row["id_filial"]),
            "tipo_titulo": int(row["tipo_titulo"]),
            "id_titulo": int(row["id_titulo"]),
            "id_db": int(row["id_db"]),
            "id_entidade": int(row.get("id_entidade") or 0),
            "entidade_nome": str(row.get("entidade_nome") or ""),
            "dt_lancamento": row.get("dt_lancamento"),
            "dt_vencimento": row["dt_vencimento"],
            "valor": row.get("valor") or 0,
            "valor_pago": row.get("valor_pago") or 0,
            "valor_aberto": row.get("valor_aberto") or 0,
            "status": str(row["status"]),
            "published_at": published_at,
        }
        for row in rows
    ]
    inserted = insert_batch(
        MART_TABLE,
        payload,
        order_by=["id_empresa", "tipo_titulo", "id_filial", "id_db", "id_titulo"],
    )
    logger.info(
        "finance titles publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        inserted,
    )
    return inserted
