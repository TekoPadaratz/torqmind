"""Override mensal de Vale / Hora extra (app.employee_cost_manual)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from app.db import get_conn

logger = logging.getLogger(__name__)

Key = Tuple[int, int]  # (id_filial, id_funcionario)


def _normalize_money(value: Any) -> float:
    try:
        n = float(value)
    except (TypeError, ValueError):
        raise ValueError("valor inválido") from None
    if n < 0:
        raise ValueError("valor não pode ser negativo")
    if n > 1_000_000_000:
        raise ValueError("valor acima do limite")
    return round(n, 2)


def _validate_ano_mes(ano_mes: int) -> int:
    am = int(ano_mes)
    mes = am % 100
    if am < 200001 or am > 210012 or mes < 1 or mes > 12:
        raise ValueError("ano_mes inválido")
    return am


def fetch_employee_cost_manual(
    role: str,
    id_empresa: int,
    ano_mes: int,
    id_filial: Any = None,
) -> Dict[Key, Dict[str, float]]:
    """Mapa (id_filial, id_funcionario) → {vales, horas_extras} no mês."""
    am = _validate_ano_mes(ano_mes)
    branch_ids: Optional[List[int]] = None
    if id_filial is not None and not (isinstance(id_filial, int) and int(id_filial) == -1):
        if isinstance(id_filial, (list, tuple, set)):
            branch_ids = [int(v) for v in id_filial if v is not None and int(v) != -1]
        else:
            branch_ids = [int(id_filial)]

    sql = """
      SELECT id_filial, id_funcionario, vales, horas_extras
      FROM app.employee_cost_manual
      WHERE id_empresa = %s AND ano_mes = %s
    """
    params: List[Any] = [int(id_empresa), am]
    if branch_ids:
        if len(branch_ids) == 1:
            sql += " AND id_filial = %s"
            params.append(branch_ids[0])
        else:
            sql += " AND id_filial = ANY(%s)"
            params.append(branch_ids)

    out: Dict[Key, Dict[str, float]] = {}
    with get_conn(role=role, tenant_id=int(id_empresa), branch_id=None) as conn:
        rows = conn.execute(sql, params).fetchall()
    for row in rows:
        key = (int(row["id_filial"]), int(row["id_funcionario"]))
        out[key] = {
            "vales": float(row.get("vales") or 0),
            "horas_extras": float(row.get("horas_extras") or 0),
        }
    return out


def upsert_employee_cost_manual(
    role: str,
    id_empresa: int,
    id_filial: int,
    id_funcionario: int,
    ano_mes: int,
    vales: Any,
    horas_extras: Any,
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Grava Vale e Hora extra do funcionário na competência (YYYYMM)."""
    am = _validate_ano_mes(ano_mes)
    fid = int(id_filial)
    eid = int(id_funcionario)
    if fid <= 0 or eid <= 0:
        raise ValueError("funcionário/filial inválidos")
    vales_n = _normalize_money(vales)
    he_n = _normalize_money(horas_extras)

    with get_conn(role=role, tenant_id=int(id_empresa), branch_id=fid) as conn:
        # Garante que o funcionário existe no escopo da empresa (cadastro STG).
        exists = conn.execute(
            """
            SELECT 1
            FROM stg.funcionarios
            WHERE id_empresa = %s AND id_filial = %s AND id_funcionario = %s
            LIMIT 1
            """,
            [int(id_empresa), fid, eid],
        ).fetchone()
        if not exists:
            raise ValueError("funcionário não encontrado no escopo")

        conn.execute(
            """
            INSERT INTO app.employee_cost_manual (
              id_empresa, id_filial, id_funcionario, ano_mes,
              vales, horas_extras, updated_at, updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (id_empresa, id_filial, id_funcionario, ano_mes)
            DO UPDATE SET
              vales = EXCLUDED.vales,
              horas_extras = EXCLUDED.horas_extras,
              updated_at = now(),
              updated_by = EXCLUDED.updated_by
            """,
            [int(id_empresa), fid, eid, am, vales_n, he_n, updated_by],
        )

    return {
        "id_empresa": int(id_empresa),
        "id_filial": fid,
        "id_funcionario": eid,
        "ano_mes": am,
        "vales": vales_n,
        "horas_extras": he_n,
        "ok": True,
    }
