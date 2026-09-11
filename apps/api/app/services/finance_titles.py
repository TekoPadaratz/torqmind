"""Publicação de títulos financeiros do STG PostgreSQL para a mart ClickHouse."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.db import get_conn
from app.db_clickhouse import execute_command, insert_batch

logger = logging.getLogger(__name__)

MART_TABLE = "torqmind_mart_rt.mart_finance_titles_rt"
DEFAULT_DAYS = 180
# Confirmação de cobertura STG efetivamente lida (não o horário do INSERT no CH).
FINANCE_TITLES_COVER_DATASET = "finance_titles_publish"

# Xpert: DELETAR=1 remove o título; não pode aparecer como aberto.
_NOT_DELETED = (
    "coalesce(nullif(trim(payload->>'DELETAR'), ''), '0') "
    "NOT IN ('1', 'true', 'True', 't', 'T', 'S', 's', 'Y', 'y')"
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class FinanceStgCoverage:
    """Probe de cobertura STG para CAP/CAR.

    ok=False → falha de consulta (não confundir com STG vazio).
    empty=True + ok → não há received_at nas quatro tabelas.
    covered_through → limite superior da leitura: max(received_at) ou
    clock_timestamp() do snapshot quando vazio (para não “perder” chegada
    durante a publicação).
    """

    ok: bool
    empty: bool
    max_received_at: Optional[datetime]
    covered_through: Optional[datetime]
    error: str = ""


@dataclass(frozen=True)
class FinanceTitlesPublishResult:
    inserted: int
    covered_through: Optional[datetime]
    confirmed: bool
    empty: bool = False
    error: str = ""


def probe_finance_stg_coverage(conn: Any, id_empresa: int) -> FinanceStgCoverage:
    """Lê max(received_at) + clock no mesmo snapshot da conexão."""
    try:
        row = conn.execute(
            """
            SELECT
              greatest(
                (SELECT max(received_at) FROM stg.contasreceber WHERE id_empresa = %s),
                (SELECT max(received_at) FROM stg.contaspagar WHERE id_empresa = %s),
                (SELECT max(received_at) FROM stg.contasreceberbaixa WHERE id_empresa = %s),
                (SELECT max(received_at) FROM stg.contaspagarbaixa WHERE id_empresa = %s)
              ) AS stg_max,
              clock_timestamp() AS read_started_at
            """,
            (int(id_empresa), int(id_empresa), int(id_empresa), int(id_empresa)),
        ).fetchone()
    except Exception as exc:  # noqa: BLE001
        return FinanceStgCoverage(
            ok=False,
            empty=False,
            max_received_at=None,
            covered_through=None,
            error=str(exc)[:200],
        )

    raw_max = (row or {}).get("stg_max")
    raw_clock = (row or {}).get("read_started_at")
    stg_max = _as_utc(raw_max)
    read_started = _as_utc(raw_clock) or _now()
    if stg_max is None:
        return FinanceStgCoverage(
            ok=True,
            empty=True,
            max_received_at=None,
            covered_through=read_started,
        )
    return FinanceStgCoverage(
        ok=True,
        empty=False,
        max_received_at=stg_max,
        covered_through=stg_max,
    )


def _as_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def read_finance_titles_cover_watermark(conn: Any, id_empresa: int) -> tuple[bool, Optional[datetime]]:
    """(ok, covered_through). ok=False se a leitura do watermark falhar."""
    try:
        row = conn.execute(
            """
            SELECT last_ingested_at
            FROM etl.watermark
            WHERE id_empresa = %s AND dataset = %s
            """,
            (int(id_empresa), FINANCE_TITLES_COVER_DATASET),
        ).fetchone()
    except Exception:
        return False, None
    if not row:
        return True, None
    return True, _as_utc((row or {}).get("last_ingested_at"))


def confirm_finance_titles_cover(conn: Any, id_empresa: int, covered_through: datetime) -> None:
    """Persiste o limite STG coberto só após publish CH bem-sucedido."""
    conn.execute(
        "SELECT etl.set_watermark(%s, %s, %s, NULL::bigint)",
        (int(id_empresa), FINANCE_TITLES_COVER_DATASET, covered_through),
    )
    conn.commit()


def fetch_finance_titles(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS, *, conn: Any = None
) -> List[Dict[str, Any]]:
    """Lê títulos alinhados ao Xpert Não Pagas/Não Recebidas (DTAPGTO IS NULL).

    Aberto = DTAPGTO nulo e saldo > 0.01 após baixas.
    Saldo canônico Xpert (docs/solvencia): VALOR = VLRPAGO + Σ VALORBAIXA.
    Baixas parciais vivem em CONTASPAGARBAIXA / CONTASRECEBERBAIXA (não há
    tabela CONTASPAGARBAIXAPARCIAL separada no Xpert — o parcial é a própria BAIXA).
    Pagos recentes entram como tombstone (`status=pago`) para curar fantasma no CH.
    """
    days = max(7, min(int(days), 366))
    sql = f"""
            WITH baixa_receber AS (
              SELECT
                id_empresa, id_db,
                etl.safe_int(payload->>'ID_CONTASRECEBER') AS id_titulo,
                sum(coalesce(etl.safe_numeric(payload->>'VALORBAIXA'), 0)) AS total_baixa,
                max((etl.safe_timestamp(payload->>'DATABAIXA'))::date) AS dt_ultima_baixa
              FROM stg.contasreceberbaixa
              WHERE id_empresa = %s
                AND {_NOT_DELETED}
              GROUP BY id_empresa, id_db, etl.safe_int(payload->>'ID_CONTASRECEBER')
            ),
            baixa_pagar AS (
              SELECT
                id_empresa, id_db,
                etl.safe_int(payload->>'ID_CONTASPAGAR') AS id_titulo,
                sum(coalesce(etl.safe_numeric(payload->>'VALORBAIXA'), 0)) AS total_baixa,
                max((etl.safe_timestamp(payload->>'DATABAIXA'))::date) AS dt_ultima_baixa
              FROM stg.contaspagarbaixa
              WHERE id_empresa = %s
                AND {_NOT_DELETED}
              GROUP BY id_empresa, id_db, etl.safe_int(payload->>'ID_CONTASPAGAR')
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
                coalesce(
                  nullif(trim(cp.payload->>'NRODOC'), ''),
                  nullif(trim(cp.payload->>'DOCUMENTO'), ''),
                  ''
                ) AS nro_documento,
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
               AND bp.id_db = cp.id_db
               AND bp.id_titulo = cp.id_contaspagar
              LEFT JOIN stg.entidades ent
                ON ent.id_empresa = cp.id_empresa
               AND ent.id_filial = cp.id_filial
               AND ent.id_entidade = coalesce(etl.safe_int(cp.payload->>'ID_ENTIDADE'), 0)
              WHERE cp.id_empresa = %s
                AND {_NOT_DELETED.replace("payload->>", "cp.payload->>")}
                AND NOT (cp.payload ? 'TORQMIND_RECONCILED_ABSENT')

              UNION ALL

              SELECT
                cr.id_empresa,
                cr.id_filial,
                1::smallint AS tipo_titulo,
                cr.id_contasreceber::bigint AS id_titulo,
                cr.id_db,
                coalesce(etl.safe_int(cr.payload->>'ID_ENTIDADE'), 0)::bigint AS id_entidade,
                coalesce(nullif(ent.payload->>'NOMEENTIDADE', ''), '') AS entidade_nome,
                coalesce(
                  nullif(trim(cr.payload->>'NRODOC'), ''),
                  nullif(trim(cr.payload->>'DOCUMENTO'), ''),
                  ''
                ) AS nro_documento,
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
               AND br.id_db = cr.id_db
               AND br.id_titulo = cr.id_contasreceber
              LEFT JOIN stg.entidades ent
                ON ent.id_empresa = cr.id_empresa
               AND ent.id_filial = cr.id_filial
               AND ent.id_entidade = coalesce(etl.safe_int(cr.payload->>'ID_ENTIDADE'), 0)
              WHERE cr.id_empresa = %s
                AND {_NOT_DELETED.replace("payload->>", "cr.payload->>")}
                AND NOT (cr.payload ? 'TORQMIND_RECONCILED_ABSENT')
            )
            SELECT
              id_empresa, id_filial, tipo_titulo, id_titulo, id_db,
              id_entidade, entidade_nome, nro_documento, dt_lancamento, dt_vencimento,
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
                  -- Xpert Não Pagas + Data Final=hoje (Data de=Conta): DTACONTA <= hoje.
                  valor_aberto > 0.01
                  AND dt_pgto_flag IS NULL
                  AND (
                    dt_lancamento IS NULL
                    OR dt_lancamento <= (now() AT TIME ZONE 'America/Sao_Paulo')::date
                  )
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
            """
    params = [id_empresa, id_empresa, id_empresa, id_empresa, days]
    if conn is not None:
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as owned:
        owned.execute("SET LOCAL statement_timeout = 0")
        rows = owned.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def publish_finance_titles(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> FinanceTitlesPublishResult:
    """Republica títulos financeiros no ClickHouse (replace por empresa).

    DELETE + INSERT evita fantasma aberto quando o título sumiu/foi pago no STG
    e a ReplacingMergeTree ainda servia a versão antiga.

    Confirmação de cobertura: só após CH ok, grava em ``etl.watermark`` o
    ``covered_through`` capturado **antes** do fetch (mesmo snapshot). Assim
    uma baixa que chega durante o publish permanece pendente no próximo ciclo
    (stg_max > covered_through), em vez de ser mascarada por ``published_at=now()``.
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        conn.execute("SET LOCAL statement_timeout = 0")
        # Cobertura capturada ANTES do fetch na mesma conexão. O watermark
        # confirmado é esse limite (não published_at do CH): baixa com
        # received_at > covered_through permanece pendente no próximo ciclo.
        coverage = probe_finance_stg_coverage(conn, id_empresa)
        if not coverage.ok or coverage.covered_through is None:
            return FinanceTitlesPublishResult(
                inserted=0,
                covered_through=None,
                confirmed=False,
                empty=False,
                error=coverage.error or "finance STG coverage probe failed",
            )
        rows = fetch_finance_titles(role, id_empresa, days=days, conn=conn)

    try:
        execute_command(
            f"""
            ALTER TABLE {MART_TABLE}
            DELETE WHERE id_empresa = {{id_empresa:Int32}}
            SETTINGS mutations_sync = 1
            """,
            {"id_empresa": int(id_empresa)},
        )
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
                "nro_documento": str(row.get("nro_documento") or ""),
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
    except Exception as exc:  # noqa: BLE001 — não confirma cobertura se CH falhou
        logger.warning(
            "finance titles publish interrupted empresa=%s: %s",
            id_empresa,
            str(exc)[:200],
        )
        return FinanceTitlesPublishResult(
            inserted=0,
            covered_through=coverage.covered_through,
            confirmed=False,
            empty=coverage.empty,
            error=str(exc)[:200],
        )

    # Só confirma depois do CH completar (inclui republicação vazia legítima).
    try:
        with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
            confirm_finance_titles_cover(conn, id_empresa, coverage.covered_through)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "finance titles cover confirm failed empresa=%s: %s",
            id_empresa,
            str(exc)[:200],
        )
        return FinanceTitlesPublishResult(
            inserted=int(inserted),
            covered_through=coverage.covered_through,
            confirmed=False,
            empty=len(payload) == 0,
            error=f"cover confirm failed: {str(exc)[:160]}",
        )

    logger.info(
        "finance titles publish empresa=%s rows=%s inserted=%s covered_through=%s empty=%s",
        id_empresa,
        len(payload),
        inserted,
        coverage.covered_through.isoformat(),
        coverage.empty,
    )
    return FinanceTitlesPublishResult(
        inserted=int(inserted),
        covered_through=coverage.covered_through,
        confirmed=True,
        empty=len(payload) == 0,
    )
