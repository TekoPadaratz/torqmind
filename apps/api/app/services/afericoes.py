"""Aferições de bico — publish PG STG → ClickHouse mart_rt.

Fonte: stg.afericoes ← dbo.AFERICAO (ato operacional, QTDE/DATA).
Rótulos: stg.bicos, stg.turnos (TURNO operacional), stg.usuarios/funcionarios.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.db import get_conn
from app.db_clickhouse import insert_batch

logger = logging.getLogger(__name__)

MART_TABLE = "torqmind_mart_rt.mart_afericoes_rt"
DEFAULT_DAYS = 120


def _now() -> datetime:
    return datetime.now(timezone.utc)


def fetch_afericoes(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> List[Dict[str, Any]]:
    days = max(7, min(int(days), 366))
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT
              a.id_empresa,
              a.id_filial,
              a.id_afericao,
              coalesce(nullif(a.payload->>'ID_BICOS','')::int, 0) AS id_bico,
              coalesce(nullif(a.payload->>'ID_TURNOS','')::int, 0) AS id_turno,
              CASE
                WHEN t.id_turno IS NULL THEN -1  -- ID_TURNOS órfão / sem cadastro
                ELSE coalesce(
                  nullif(t.payload->>'TURNO','')::int,
                  nullif(t.payload->>'NROTURNO','')::int,
                  nullif(t.payload->>'NO_TURNO','')::int,
                  nullif(t.payload->>'NUMTURNO','')::int,
                  nullif(t.payload->>'NRO_TURNO','')::int,
                  0  -- cadastro existe com TURNO=0 → caixa geral
                )
              END AS turno_operacional,
              coalesce(
                nullif(b.payload->>'DESCRICAO',''),
                nullif(b.payload->>'NOME',''),
                nullif(b.payload->>'BICO',''),
                CASE WHEN coalesce(nullif(a.payload->>'ID_BICOS','')::int, 0) > 0
                     THEN 'Bico ' || (a.payload->>'ID_BICOS')
                     ELSE '' END
              ) AS bico_label,
              left(coalesce(
                nullif(dim.nome, ''),
                nullif(pr.payload->>'DESCRICAO', ''),
                ''
              ), 80) AS produto_nome,
              coalesce(
                etl.safe_numeric(a.payload->>'QTDE'),
                0
              ) AS qtde_l,
              coalesce(
                (a.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date,
                (etl.safe_timestamp(a.payload->>'DATA'))::date
              ) AS dia,
              coalesce(
                a.dt_evento,
                etl.safe_timestamp(a.payload->>'DATA')
              ) AS dt_evento,
              coalesce(nullif(a.payload->>'ID_USUARIOS','')::int, 0) AS id_usuario,
              coalesce(nullif(a.payload->>'ID_USUARIOS_LIB','')::int, 0) AS id_usuario_lib,
              left(coalesce(
                nullif(u.payload->>'NOMEUSUARIOS',''),
                nullif(u.payload->>'NOME',''),
                nullif(u.payload->>'LOGIN',''),
                -- Só cai em funcionário se o usuário apontar ID_FUNCIONARIOS (sem colisão de PK).
                nullif(f.payload->>'NOMEFUNCIONARIO',''),
                nullif(f.payload->>'NOME',''),
                ''
              ), 80) AS operador_nome,
              left(coalesce(
                nullif(ul.payload->>'NOMEUSUARIOS',''),
                nullif(ul.payload->>'NOME',''),
                nullif(ul.payload->>'LOGIN',''),
                nullif(fl.payload->>'NOMEFUNCIONARIO',''),
                nullif(fl.payload->>'NOME',''),
                ''
              ), 80) AS liberador_nome
            FROM stg.afericoes a
            LEFT JOIN stg.bicos b
              ON b.id_empresa = a.id_empresa
             AND b.id_filial = a.id_filial
             AND b.id_bico = coalesce(nullif(a.payload->>'ID_BICOS','')::int, 0)
            LEFT JOIN stg.turnos t
              ON t.id_empresa = a.id_empresa
             AND t.id_filial = a.id_filial
             AND t.id_turno = coalesce(nullif(a.payload->>'ID_TURNOS','')::int, 0)
            LEFT JOIN stg.tanques tq
              ON tq.id_empresa = b.id_empresa
             AND tq.id_filial = b.id_filial
             AND tq.id_tanque = coalesce(nullif(b.payload->>'ID_TANQUES','')::int, 0)
            LEFT JOIN stg.produtos pr
              ON pr.id_empresa = a.id_empresa
             AND pr.id_filial = a.id_filial
             AND pr.id_produto = coalesce(
                   nullif(b.payload->>'ID_PRODUTOS','')::int,
                   nullif(tq.payload->>'ID_PRODUTOS','')::int,
                   0
                 )
            LEFT JOIN dw.dim_produto dim
              ON dim.id_empresa = a.id_empresa
             AND dim.id_filial = a.id_filial
             AND dim.id_produto = coalesce(
                   nullif(b.payload->>'ID_PRODUTOS','')::int,
                   nullif(tq.payload->>'ID_PRODUTOS','')::int,
                   0
                 )
            LEFT JOIN stg.usuarios u
              ON u.id_empresa = a.id_empresa
             AND u.id_filial = a.id_filial
             AND u.id_usuario = coalesce(nullif(a.payload->>'ID_USUARIOS','')::int, 0)
            LEFT JOIN stg.usuarios ul
              ON ul.id_empresa = a.id_empresa
             AND ul.id_filial = a.id_filial
             AND ul.id_usuario = coalesce(nullif(a.payload->>'ID_USUARIOS_LIB','')::int, 0)
            LEFT JOIN stg.funcionarios f
              ON f.id_empresa = a.id_empresa
             AND f.id_filial = a.id_filial
             AND f.id_funcionario = coalesce(
                   nullif(u.payload->>'ID_FUNCIONARIOS','')::int,
                   nullif(u.payload->>'ID_FUNCIONARIO','')::int,
                   0
                 )
            LEFT JOIN stg.funcionarios fl
              ON fl.id_empresa = a.id_empresa
             AND fl.id_filial = a.id_filial
             AND fl.id_funcionario = coalesce(
                   nullif(ul.payload->>'ID_FUNCIONARIOS','')::int,
                   nullif(ul.payload->>'ID_FUNCIONARIO','')::int,
                   0
                 )
            WHERE a.id_empresa = %s
              AND coalesce(
                    (a.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date,
                    (etl.safe_timestamp(a.payload->>'DATA'))::date
                  ) >= (now() AT TIME ZONE 'America/Sao_Paulo')::date - %s
            ORDER BY a.id_filial, dia DESC, a.id_afericao DESC
            """,
            [id_empresa, days],
        ).fetchall()
    return [dict(r) for r in rows]


def publish_afericoes(
    role: str, id_empresa: int, days: int = DEFAULT_DAYS
) -> int:
    rows = fetch_afericoes(role, id_empresa, days=days)
    pub = _now()
    payload: List[Dict[str, Any]] = []
    for r in rows:
        dia = r.get("dia")
        if dia is None:
            continue
        dt_ev = r.get("dt_evento") or dia
        payload.append(
            {
                "id_empresa": int(r["id_empresa"]),
                "id_filial": int(r["id_filial"]),
                "id_afericao": int(r["id_afericao"]),
                "id_bico": int(r.get("id_bico") or 0),
                "id_turno": int(r.get("id_turno") or 0),
                "turno_operacional": int(r.get("turno_operacional") if r.get("turno_operacional") is not None else -1),
                "bico_label": str(r.get("bico_label") or ""),
                "produto_nome": str(r.get("produto_nome") or ""),
                "qtde_l": float(r.get("qtde_l") or 0),
                "dia": dia,
                "dt_evento": dt_ev,
                "id_usuario": int(r.get("id_usuario") or 0),
                "id_usuario_lib": int(r.get("id_usuario_lib") or 0),
                "operador_nome": str(r.get("operador_nome") or ""),
                "liberador_nome": str(r.get("liberador_nome") or ""),
                "published_at": pub,
            }
        )
    n = insert_batch(
        MART_TABLE,
        payload,
        order_by=["id_empresa", "id_filial", "id_afericao"],
    )
    logger.info(
        "afericoes publish empresa=%s rows=%s inserted=%s",
        id_empresa,
        len(payload),
        n,
    )
    return n
