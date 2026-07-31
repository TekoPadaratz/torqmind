"""Publicação de despesas (CAP × plano de contas) e funcionários para ClickHouse."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.db import get_conn
from app.db_clickhouse import insert_batch

logger = logging.getLogger(__name__)

DESPESAS_TABLE = "torqmind_mart_rt.mart_finance_despesas_rt"
EMPLOYEES_TABLE = "torqmind_mart_rt.mart_team_employees_rt"
DEFAULT_DAYS = 400


def _now() -> datetime:
    return datetime.now(timezone.utc)


def fetch_finance_despesas(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> List[Dict[str, Any]]:
    """CAP com plano DRE (entra_dre), status pago/aberto/vencido."""
    days = max(30, min(int(days), 800))
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            WITH baixa_pagar AS (
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
                coalesce(
                  nullif(f.payload->>'APELIDO', ''),
                  nullif(f.payload->>'NOMEFILIAL', ''),
                  ''
                ) AS filial_nome,
                cp.id_contaspagar::bigint AS id_titulo,
                cp.id_db,
                d.id_planodecontas,
                coalesce(d.codigo_plano, '') AS codigo_plano,
                coalesce(d.nome_plano, '') AS nome_plano,
                coalesce(d.classificacao_gerencial, '') AS classificacao_gerencial,
                CASE WHEN coalesce(d.entra_custo_operacional, false) THEN 1 ELSE 0 END AS entra_custo_operacional,
                coalesce(nullif(cp.payload->>'HISTORICO', ''), '') AS historico,
                coalesce(nullif(cp.payload->>'DOCUMENTO', ''), '') AS documento,
                (etl.safe_timestamp(cp.payload->>'DTAVCTO'))::date AS dt_vencimento,
                coalesce(
                  (etl.safe_timestamp(cp.payload->>'DTAPGTO'))::date,
                  bp.dt_ultima_baixa
                ) AS dt_pagamento,
                coalesce(etl.safe_numeric(cp.payload->>'VALOR'), 0)::numeric(18,2) AS valor,
                least(
                  coalesce(etl.safe_numeric(cp.payload->>'VALOR'), 0),
                  greatest(
                    coalesce(etl.safe_numeric(cp.payload->>'VLRPAGO'), 0),
                    coalesce(bp.total_baixa, 0)
                  )
                )::numeric(18,2) AS valor_pago,
                greatest(
                  0,
                  coalesce(etl.safe_numeric(cp.payload->>'VALOR'), 0)
                  - greatest(
                    coalesce(etl.safe_numeric(cp.payload->>'VLRPAGO'), 0),
                    coalesce(bp.total_baixa, 0)
                  )
                )::numeric(18,2) AS valor_aberto
              FROM stg.contaspagar cp
              JOIN dw.dim_plano_contas_gerencial d
                ON d.id_empresa = cp.id_empresa
               AND d.id_filial = cp.id_filial
               AND d.id_planodecontas = etl.safe_int(cp.payload->>'ID_PLANODECONTAS')
              LEFT JOIN baixa_pagar bp
                ON bp.id_empresa = cp.id_empresa
               AND bp.id_filial = cp.id_filial
               AND bp.id_db = cp.id_db
               AND bp.id_titulo = cp.id_contaspagar
              LEFT JOIN stg.filiais f
                ON f.id_empresa = cp.id_empresa
               AND f.id_filial = cp.id_filial
              WHERE cp.id_empresa = %s
                AND coalesce(d.entra_dre, false) IS TRUE
                AND (etl.safe_timestamp(cp.payload->>'DTAVCTO'))::date IS NOT NULL
            )
            SELECT
              id_empresa, id_filial, filial_nome, id_titulo, id_db,
              id_planodecontas, codigo_plano, nome_plano, classificacao_gerencial,
              entra_custo_operacional, historico, documento,
              dt_vencimento, dt_pagamento, valor, valor_pago, valor_aberto,
              CASE
                WHEN valor_aberto <= 0.01 THEN 'pago'
                WHEN dt_vencimento < (now() AT TIME ZONE 'America/Sao_Paulo')::date THEN 'vencido'
                ELSE 'a_vencer'
              END AS status,
              (EXTRACT(YEAR FROM dt_vencimento)::int * 100
                + EXTRACT(MONTH FROM dt_vencimento)::int) AS ano_mes_vencimento
            FROM src
            WHERE dt_vencimento >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s
               OR valor_aberto > 0.01
            ORDER BY id_filial, nome_plano, dt_vencimento, id_titulo
            """,
            [id_empresa, id_empresa, days],
        ).fetchall()
    return [dict(row) for row in rows]


def publish_finance_despesas(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> int:
    rows = fetch_finance_despesas(role, id_empresa, days=days)
    published_at = _now()
    payload = [
        {
            "id_empresa": int(row["id_empresa"]),
            "id_filial": int(row["id_filial"]),
            "filial_nome": str(row.get("filial_nome") or ""),
            "id_titulo": int(row["id_titulo"]),
            "id_db": int(row.get("id_db") or 0),
            "id_planodecontas": int(row["id_planodecontas"]),
            "codigo_plano": str(row.get("codigo_plano") or ""),
            "nome_plano": str(row.get("nome_plano") or ""),
            "classificacao_gerencial": str(row.get("classificacao_gerencial") or ""),
            "entra_custo_operacional": int(row.get("entra_custo_operacional") or 0),
            "historico": str(row.get("historico") or ""),
            "documento": str(row.get("documento") or ""),
            "dt_vencimento": row["dt_vencimento"],
            "dt_pagamento": row.get("dt_pagamento"),
            "valor": row.get("valor") or 0,
            "valor_pago": row.get("valor_pago") or 0,
            "valor_aberto": row.get("valor_aberto") or 0,
            "status": str(row.get("status") or "a_vencer"),
            "ano_mes_vencimento": int(row.get("ano_mes_vencimento") or 0),
            "published_at": published_at,
        }
        for row in rows
    ]
    inserted = insert_batch(
        DESPESAS_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_db", "id_titulo"],
    )
    logger.info(
        "finance despesas publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        inserted,
    )
    return inserted


def fetch_team_employees(role: str, id_empresa: int) -> List[Dict[str, Any]]:
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT
              f.id_empresa,
              f.id_filial,
              coalesce(
                nullif(fl.payload->>'APELIDO', ''),
                nullif(fl.payload->>'NOMEFILIAL', ''),
                ''
              ) AS filial_nome,
              f.id_funcionario::bigint AS id_funcionario,
              coalesce(etl.safe_int(f.payload->>'ID_USUARIO'), 0) AS id_usuario,
              coalesce(nullif(f.payload->>'NOMEFUNCIONARIO', ''), '') AS nome,
              coalesce(nullif(f.payload->>'FUNCAO', ''), '') AS funcao,
              CASE
                WHEN lower(coalesce(f.payload->>'ATIVO', '')) IN ('true', '1', 't', 'sim')
                THEN 1 ELSE 0
              END AS ativo,
              coalesce(etl.safe_numeric(f.payload->>'SALARIOBRUTO'), 0)::numeric(18,2) AS salario_bruto,
              coalesce(etl.safe_numeric(f.payload->>'SALARIOTOTAL'), 0)::numeric(18,2) AS salario_total,
              coalesce(etl.safe_numeric(f.payload->>'VALES'), 0)::numeric(18,2) AS vales,
              coalesce(etl.safe_numeric(f.payload->>'HORASEXTRAS'), 0)::numeric(18,2) AS horas_extras
            FROM stg.funcionarios f
            LEFT JOIN stg.filiais fl
              ON fl.id_empresa = f.id_empresa
             AND fl.id_filial = f.id_filial
            WHERE f.id_empresa = %s
              AND lower(coalesce(f.payload->>'ATIVO', '')) IN ('true', '1', 't', 'sim')
              AND coalesce(nullif(f.payload->>'NOMEFUNCIONARIO', ''), '') <> ''
            ORDER BY f.id_filial, f.payload->>'NOMEFUNCIONARIO', f.id_funcionario
            """,
            [id_empresa],
        ).fetchall()
    return [dict(row) for row in rows]


def publish_team_employees(role: str, id_empresa: int) -> int:
    rows = fetch_team_employees(role, id_empresa)
    published_at = _now()
    payload = [
        {
            "id_empresa": int(row["id_empresa"]),
            "id_filial": int(row["id_filial"]),
            "filial_nome": str(row.get("filial_nome") or ""),
            "id_funcionario": int(row["id_funcionario"]),
            "id_usuario": int(row.get("id_usuario") or 0),
            "nome": str(row.get("nome") or ""),
            "funcao": str(row.get("funcao") or ""),
            "ativo": int(row.get("ativo") or 0),
            "salario_bruto": row.get("salario_bruto") or 0,
            "salario_total": row.get("salario_total") or 0,
            "vales": row.get("vales") or 0,
            "horas_extras": row.get("horas_extras") or 0,
            "published_at": published_at,
        }
        for row in rows
    ]
    inserted = insert_batch(
        EMPLOYEES_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_funcionario"],
    )
    logger.info(
        "team employees publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        inserted,
    )
    return inserted
