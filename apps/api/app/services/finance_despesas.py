"""Publicação de despesas (MOVLCTOS × plano DRE) e funcionários para ClickHouse.

Fonte canônica do Razão / DRE Xpert: ``dbo.MOVLCTOS`` + ``DTACONTA``
(docs/product/XPERT_DRE_DESPESAS_MAP.md). ``CONTASPAGAR``/``DTAVCTO`` NÃO
sustentam o relatório de despesas por plano de contas.

Semântica Xpert (Entradas/Saídas do Razão):
- TIPO 0 ou 2 = débito → Entrada
- TIPO 1 = crédito → Saída
- ESTORNO=1 entra no DRE (não filtrar)
- Texto da linha = ``DOCUMENTO`` (não há coluna HISTORICO em MOVLCTOS)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.db import get_conn
from app.db_clickhouse import execute_command, insert_batch

logger = logging.getLogger(__name__)

DESPESAS_TABLE = "torqmind_mart_rt.mart_finance_despesas_rt"
EMPLOYEES_TABLE = "torqmind_mart_rt.mart_team_employees_rt"
DEFAULT_DAYS = 400


def _now() -> datetime:
    return datetime.now(timezone.utc)


def fetch_finance_despesas(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> List[Dict[str, Any]]:
    """Lançamentos MOVLCTOS em contas DRE (entra_dre), por competência DTACONTA."""
    days = max(30, min(int(days), 800))
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            WITH src AS (
              SELECT
                m.id_empresa,
                m.id_filial,
                coalesce(
                  nullif(f.payload->>'APELIDO', ''),
                  nullif(f.payload->>'NOMEFILIAL', ''),
                  ''
                ) AS filial_nome,
                coalesce(
                  etl.safe_int(m.payload->>'ID_MOVLCTOS'),
                  m.id_movlctos
                )::bigint AS id_titulo,
                m.id_db,
                d.id_planodecontas,
                coalesce(d.codigo_plano, '') AS codigo_plano,
                coalesce(d.nome_plano, '') AS nome_plano,
                coalesce(d.classificacao_gerencial, '') AS classificacao_gerencial,
                CASE WHEN coalesce(d.entra_custo_operacional, false) THEN 1 ELSE 0 END
                  AS entra_custo_operacional,
                coalesce(nullif(trim(m.payload->>'DOCUMENTO'), ''), '') AS documento,
                (etl.safe_timestamp(m.payload->>'DTACONTA'))::date AS dt_competencia,
                coalesce(etl.safe_int(m.payload->>'TIPO'), 0) AS tipo,
                coalesce(etl.safe_numeric(m.payload->>'VALOR'), 0)::numeric(18,2) AS valor
              FROM stg.movlctos m
              JOIN dw.dim_plano_contas_gerencial d
                ON d.id_empresa = m.id_empresa
               AND d.id_filial = m.id_filial
               AND d.id_planodecontas = etl.safe_int(m.payload->>'ID_PLANODECONTAS')
              LEFT JOIN stg.filiais f
                ON f.id_empresa = m.id_empresa
               AND f.id_filial = m.id_filial
              WHERE m.id_empresa = %s
                AND coalesce(d.entra_dre, false) IS TRUE
                AND (etl.safe_timestamp(m.payload->>'DTACONTA'))::date IS NOT NULL
            )
            SELECT
              id_empresa, id_filial, filial_nome, id_titulo, id_db,
              id_planodecontas, codigo_plano, nome_plano, classificacao_gerencial,
              entra_custo_operacional,
              documento AS historico,
              documento,
              dt_competencia AS dt_vencimento,
              NULL::date AS dt_pagamento,
              valor,
              CASE WHEN tipo IN (0, 2) THEN valor ELSE 0 END::numeric(18,2) AS valor_pago,
              CASE WHEN tipo = 1 THEN valor ELSE 0 END::numeric(18,2) AS valor_aberto,
              CASE
                WHEN tipo IN (0, 2) THEN 'entrada'
                WHEN tipo = 1 THEN 'saida'
                ELSE 'outro'
              END AS status,
              (EXTRACT(YEAR FROM dt_competencia)::int * 100
                + EXTRACT(MONTH FROM dt_competencia)::int) AS ano_mes_vencimento
            FROM src
            WHERE dt_competencia >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s
            ORDER BY id_filial, nome_plano, dt_competencia DESC, documento, id_titulo
            """,
            [id_empresa, days],
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
            "status": str(row.get("status") or "entrada"),
            "ano_mes_vencimento": int(row.get("ano_mes_vencimento") or 0),
            "published_at": published_at,
        }
        for row in rows
    ]
    # Remove legado CAP (pago/a_vencer/vencido) e republicação anterior da empresa
    # para evitar misturar grãos CONTASPAGAR × MOVLCTOS no ReplacingMergeTree.
    try:
        execute_command(
            f"ALTER TABLE {DESPESAS_TABLE} DELETE WHERE id_empresa = {{id_empresa:Int32}}",
            parameters={"id_empresa": int(id_empresa)},
        )
    except Exception as exc:  # noqa: BLE001 — publish deve seguir; leitura filtra status
        logger.warning(
            "finance despesas CH delete empresa=%s failed: %s", id_empresa, exc
        )
    inserted = insert_batch(
        DESPESAS_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_db", "id_titulo"],
    )
    logger.info(
        "finance despesas publish empresa=%s rows=%s inserted=%s source=movlctos",
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
    return inserted
