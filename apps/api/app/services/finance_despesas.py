"""Publicação de despesas (MOVLCTOS × plano DRE) e funcionários para ClickHouse.

Fonte canônica do Razão / DRE Xpert: ``dbo.MOVLCTOS`` + ``DTACONTA``
(docs/product/XPERT_DRE_DESPESAS_MAP.md). ``CONTASPAGAR``/``DTAVCTO`` NÃO
sustentam o relatório de despesas por plano de contas.

Semântica Xpert (Entradas/Saídas do Razão):
- TIPO 0 ou 2 = débito → Entrada
- TIPO 1 = crédito → Saída
- ESTORNO=1 entra no DRE (não filtrar)
- Texto da linha = ``DOCUMENTO`` (não há coluna HISTORICO em MOVLCTOS)

Publish CH: incremental por mês (idempotente/reentrante), evitando
``statement_timeout`` ao republicar ~400 dias de uma vez.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.db import get_conn
from app.db_clickhouse import execute_command, insert_batch

logger = logging.getLogger(__name__)

DESPESAS_TABLE = "torqmind_mart_rt.mart_finance_despesas_rt"
EMPLOYEES_TABLE = "torqmind_mart_rt.mart_team_employees_rt"
DEFAULT_DAYS = 400
_SP = ZoneInfo("America/Sao_Paulo")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sp_today() -> date:
    return datetime.now(_SP).date()


def _month_windows(days: int) -> List[Tuple[date, date]]:
    """Janelas half-open [start, end) cobrindo os últimos ``days`` até hoje SP."""
    end = _sp_today() + timedelta(days=1)
    start = end - timedelta(days=max(30, min(int(days), 800)))
    windows: List[Tuple[date, date]] = []
    cur = date(start.year, start.month, 1)
    while cur < end:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        w_start = max(cur, start)
        w_end = min(nxt, end)
        if w_start < w_end:
            windows.append((w_start, w_end))
        cur = nxt
    return windows


def fetch_finance_despesas(
    role: str,
    id_empresa: int,
    days: int = DEFAULT_DAYS,
    *,
    dt_from: Optional[date] = None,
    dt_to: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Lançamentos MOVLCTOS em contas DRE (entra_dre), por competência DTACONTA.

    Sem ``dt_from``/``dt_to``: últimos ``days`` dias (compat).
    Com janela explícita: ``dt_from`` inclusivo, ``dt_to`` exclusivo.
    """
    days = max(30, min(int(days), 800))
    params: List[Any] = [id_empresa]
    if dt_from is not None and dt_to is not None:
        date_pred = "dt_competencia >= %s AND dt_competencia < %s"
        params.extend([dt_from, dt_to])
    else:
        date_pred = (
            "dt_competencia >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s"
        )
        params.append(days)

    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            f"""
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
            WHERE {date_pred}
            ORDER BY id_filial, nome_plano, dt_competencia DESC, documento, id_titulo
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def _rows_to_payload(rows: List[Dict[str, Any]], published_at: datetime) -> List[Dict[str, Any]]:
    return [
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


def publish_finance_despesas(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> int:
    """Publica despesas no CH em chunks mensais (idempotente / reentrante).

    Para cada mês: DELETE por ``id_empresa`` + ``ano_mes_vencimento`` e INSERT
    do lote. Evita republicar 400 dias numa única operação (statement_timeout).
    """
    published_at = _now()
    total_inserted = 0
    windows = _month_windows(days)
    # Limpa legado CAP (status pago/a_vencer/vencido) uma vez — escopo pequeno.
    try:
        execute_command(
            f"ALTER TABLE {DESPESAS_TABLE} DELETE WHERE id_empresa = {{id_empresa:Int32}} "
            f"AND status IN ('pago','a_vencer','vencido')",
            parameters={"id_empresa": int(id_empresa)},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "finance despesas CH legacy CAP delete empresa=%s failed: %s",
            id_empresa,
            exc,
        )

    for w_start, w_end in windows:
        rows = fetch_finance_despesas(
            role, id_empresa, days=days, dt_from=w_start, dt_to=w_end
        )
        ano_mes = w_start.year * 100 + w_start.month
        try:
            execute_command(
                f"ALTER TABLE {DESPESAS_TABLE} DELETE WHERE id_empresa = {{id_empresa:Int32}} "
                f"AND ano_mes_vencimento = {{ano_mes:Int32}}",
                parameters={"id_empresa": int(id_empresa), "ano_mes": int(ano_mes)},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "finance despesas CH delete empresa=%s ano_mes=%s failed: %s",
                id_empresa,
                ano_mes,
                exc,
            )
        if not rows:
            logger.info(
                "finance despesas publish empresa=%s ano_mes=%s rows=0 (empty month)",
                id_empresa,
                ano_mes,
            )
            continue
        payload = _rows_to_payload(rows, published_at)
        inserted = insert_batch(
            DESPESAS_TABLE,
            payload,
            order_by=["id_empresa", "id_filial", "id_db", "id_titulo"],
        )
        total_inserted += int(inserted or 0)
        logger.info(
            "finance despesas publish empresa=%s ano_mes=%s rows=%s inserted=%s source=movlctos",
            id_empresa,
            ano_mes,
            len(payload),
            inserted,
        )
    return total_inserted


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
