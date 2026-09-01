"""Commission configuration and calculation repository."""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from app.commission_period import data_key_bounds_half_open
from app.db import get_conn
from app.db_clickhouse import query_dict

logger = logging.getLogger(__name__)

CURRENT_DB = "torqmind_current"

# Default tiers when no configuration exists.
# min_sales_amount guarda QUANTIDADE mínima de produtos (coluna histórica).
DEFAULT_TIERS = [
    {"tier_key": "bronze", "tier_name": "Bronze", "min_sales_amount": 50, "commission_percent": 2.0, "sort_order": 1, "is_active": True},
    {"tier_key": "silver", "tier_name": "Prata", "min_sales_amount": 110, "commission_percent": 3.0, "sort_order": 2, "is_active": True},
    {"tier_key": "gold", "tier_name": "Ouro", "min_sales_amount": 160, "commission_percent": 5.0, "sort_order": 3, "is_active": True},
    {"tier_key": "diamond", "tier_name": "Diamante", "min_sales_amount": 300, "commission_percent": 7.0, "sort_order": 4, "is_active": True},
]

# Comprovantes fora da base de quantidade: cancelado, situacao=3 (ignorado), 14 (devolução).
COMMISSION_EXCLUDED_SITUACOES: tuple[int, ...] = (2, 3, 14)

# Fora da base de comissão (= mesma regra comercial de Vendas).
# 5927 perda/baixa; 5929/6929 transferência.
COMMISSION_EXCLUDED_CFOPS: tuple[int, ...] = (5927, 5929, 6929)


def _conn_branch_id(id_filial: Optional[int]) -> Optional[int]:
    return id_filial if id_filial and id_filial > 0 else None


def _cfop_sales_predicate_sql(alias: str = "i") -> str:
    """Vendas comerciais: cfop > 5000, sem 5927/5929/6929."""
    from app.sales_semantics import sales_cfop_filter_sql

    return sales_cfop_filter_sql(alias)


def _situacao_excluidas_sql() -> str:
    return ",".join(str(int(s)) for s in COMMISSION_EXCLUDED_SITUACOES)


def _active_sale_sql(alias: str = "v") -> str:
    """Venda ativa: não cancelada, não ignorada (3), não devolução (14)."""
    return (
        f" AND COALESCE({alias}.cancelado, false) = false"
        f" AND COALESCE({alias}.commercial_eligible, true) = true"
        f" AND COALESCE({alias}.situacao, 0) NOT IN ({_situacao_excluidas_sql()})"
    )


def _query_eligible_sales_ch(
    id_empresa: int,
    id_filiais: Sequence[int],
    dt_ini: date,
    dt_fim: date,
    *,
    include_central_mirror: bool = False,
) -> List[Dict[str, Any]]:
    """Agrega vendas elegíveis no ClickHouse slim (fonte BI canônica).

    Grão: filial + funcionário + grupo + produto (produto permite exclusões
    por config antes da agregação final).
    """
    targets = sorted({int(f) for f in id_filiais if int(f) > 0})
    if not targets:
        return []
    dk_ini, dk_fim = data_key_bounds_half_open(dt_ini, dt_fim)
    filial_list = ", ".join(str(f) for f in targets)
    situacao_list = _situacao_excluidas_sql()
    from app.sales_semantics import (
        central_mirror_exclude_sql,
        commission_sales_cfop_predicate_sql,
    )

    cfop_pred = commission_sales_cfop_predicate_sql(
        "i", "c", include_central_mirror=include_central_mirror
    )
    mirror_sql = ""
    if not include_central_mirror:
        mirror_sql = f" AND {central_mirror_exclude_sql('c')}"
    sql = f"""
      SELECT
        i.id_filial AS id_filial,
        i.id_funcionario AS id_funcionario,
        coalesce(nullIf(f.nome, ''), '(Sem vendedor)') AS nome_vendedor,
        if(i.id_grupo_produto > 0, i.id_grupo_produto, coalesce(p.id_grupo_produto, 0)) AS id_grupo_produto,
        coalesce(nullIf(g.nome, ''), '(Sem grupo)') AS nome_grupo_produto,
        i.id_produto AS id_produto,
        round(sum(i.total), 2) AS venda_total,
        round(sum(i.qtd), 4) AS quantidade_vendas
      FROM {CURRENT_DB}.stg_itenscomprovantes_slim AS i FINAL
      INNER JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
        ON c.id_empresa = i.id_empresa
       AND c.id_filial = i.id_filial
       AND c.id_db = i.id_db
       AND c.id_comprovante = i.id_comprovante
      LEFT JOIN {CURRENT_DB}.dim_funcionario AS f FINAL
        ON f.id_empresa = i.id_empresa
       AND f.id_filial = i.id_filial
       AND f.id_funcionario = i.id_funcionario
      LEFT JOIN {CURRENT_DB}.dim_produto AS p FINAL
        ON p.id_empresa = i.id_empresa
       AND p.id_filial = i.id_filial
       AND p.id_produto = i.id_produto
      LEFT JOIN {CURRENT_DB}.dim_grupo_produto AS g FINAL
        ON g.id_empresa = i.id_empresa
       AND g.id_filial = i.id_filial
       AND g.id_grupo_produto = if(i.id_grupo_produto > 0, i.id_grupo_produto, coalesce(p.id_grupo_produto, 0))
      WHERE i.id_empresa = {{id_empresa:Int32}}
        AND i.id_filial IN ({filial_list})
        AND i.data_key >= {{dk_ini:Int32}}
        AND i.data_key < {{dk_fim:Int32}}
        AND i.is_deleted = 0
        AND c.is_deleted = 0
        AND c.cancelado = 0
        AND c.situacao NOT IN ({situacao_list})
        AND coalesce(c.commercial_eligible, 1) = 1
        AND {cfop_pred}
        AND i.id_funcionario > 0
        {mirror_sql}
      GROUP BY
        id_filial,
        id_funcionario,
        nome_vendedor,
        id_grupo_produto,
        nome_grupo_produto,
        id_produto
    """
    rows = query_dict(
        sql,
        parameters={
            "id_empresa": int(id_empresa),
            "dk_ini": int(dk_ini),
            "dk_fim": int(dk_fim),
        },
    )
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        out.append(
            {
                "id_filial": int(r.get("id_filial") or 0),
                "id_funcionario": int(r.get("id_funcionario") or 0),
                "nome_vendedor": str(r.get("nome_vendedor") or "(Sem vendedor)"),
                "id_grupo_produto": int(r.get("id_grupo_produto") or 0),
                "nome_grupo_produto": str(r.get("nome_grupo_produto") or "(Sem grupo)"),
                "id_produto": int(r.get("id_produto") or 0),
                "venda_total": float(r.get("venda_total") or 0),
                "quantidade_vendas": float(r.get("quantidade_vendas") or 0),
            }
        )
    return out


def _filter_sales_for_config(
    raw_rows: Sequence[Dict[str, Any]],
    group_ids: Sequence[int],
    exclude_ids: Sequence[int],
    excluded_funcionario_ids: Optional[set[int]] = None,
) -> List[Dict[str, Any]]:
    """Aplica grupos/exclusões da config e reagrupa em funcionário+grupo."""
    groups = {int(g) for g in group_ids if int(g) > 0}
    excludes = {int(p) for p in exclude_ids if int(p) > 0}
    func_excludes = excluded_funcionario_ids or set()
    if not groups:
        return []
    agg: Dict[tuple, Dict[str, Any]] = {}
    for r in raw_rows:
        fid = int(r.get("id_funcionario") or 0)
        if func_excludes and fid in func_excludes:
            continue
        gid = int(r.get("id_grupo_produto") or 0)
        if gid not in groups:
            continue
        pid = int(r.get("id_produto") or 0)
        if excludes and pid in excludes:
            continue
        key = (int(r.get("id_funcionario") or 0), gid)
        slot = agg.get(key)
        if slot is None:
            slot = {
                "id_funcionario": int(r.get("id_funcionario") or 0),
                "nome_vendedor": str(r.get("nome_vendedor") or "(Sem vendedor)"),
                "id_grupo_produto": gid,
                "nome_grupo_produto": str(r.get("nome_grupo_produto") or "(Sem grupo)"),
                "venda_total": 0.0,
                "quantidade_vendas": 0.0,
            }
            agg[key] = slot
        slot["venda_total"] += float(r.get("venda_total") or 0)
        slot["quantidade_vendas"] += float(r.get("quantidade_vendas") or 0)
    return list(agg.values())


def _tier_min_qty(tier: Dict[str, Any]) -> float:
    raw = tier.get("min_qty", tier.get("min_sales_amount", 0))
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


def _tier_public(tier: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not tier:
        return None
    qty = _tier_min_qty(tier)
    return {
        "tier_key": tier["tier_key"],
        "tier_name": tier["tier_name"],
        "min_sales_amount": qty,
        "min_qty": qty,
    }


def get_config(id_empresa: int, id_filial: int) -> Optional[Dict[str, Any]]:
    """Get active commission config for empresa/filial."""
    sql = """
          SELECT id, id_empresa, id_filial, name, is_active, default_payment_mode,
              manager_commission_mode, manager_commission_percent,
              include_central_mirror,
             created_at, updated_at
      FROM app.commission_config
      WHERE id_empresa = %s AND id_filial = %s AND is_active = true
      LIMIT 1
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, [id_empresa, id_filial]).fetchone()
    return dict(row) if row else None


def get_config_groups(config_id: int) -> List[Dict[str, Any]]:
    """Get active groups for a config."""
    sql = """
      SELECT id, config_id, id_grupo_produto, nome_grupo_produto_snapshot, is_active
      FROM app.commission_config_group
      WHERE config_id = %s AND is_active = true
      ORDER BY nome_grupo_produto_snapshot
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, [config_id]).fetchall()]


def get_config_product_excludes(config_id: int) -> List[Dict[str, Any]]:
    """Get active product exclusions for a config."""
    sql = """
      SELECT id, config_id, id_produto, nome_produto_snapshot, is_active
      FROM app.commission_config_product_exclude
      WHERE config_id = %s AND is_active = true
      ORDER BY nome_produto_snapshot, id_produto
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, [config_id]).fetchall()]


def _employee_nome_ch_sql() -> str:
    return (
        "coalesce("
        "nullIf(trimBoth(JSONExtractString(payload, 'NOMEFUNCIONARIO')), ''), "
        "nullIf(trimBoth(JSONExtractString(payload, 'NOME')), ''), "
        "''"
        ")"
    )


def _employee_funcao_ch_sql() -> str:
    return (
        "coalesce("
        "nullIf(trimBoth(JSONExtractString(payload, 'FUNCAO')), ''), "
        "nullIf(trimBoth(JSONExtractString(payload, 'CARGO')), ''), "
        "nullIf(trimBoth(JSONExtractString(payload, 'DESCRFUNCAO')), ''), "
        "''"
        ")"
    )


def list_branch_employees_ch(id_empresa: int, id_filial: int) -> List[Dict[str, Any]]:
    """Funcionários ativos da filial com função do Xpert (stg_funcionarios)."""
    funcao_expr = _employee_funcao_ch_sql()
    nome_expr = _employee_nome_ch_sql()
    rows = query_dict(
        f"""
        SELECT
          id_funcionario,
          argMax({nome_expr}, source_ts_ms) AS nome,
          argMax({funcao_expr}, source_ts_ms) AS funcao,
          argMax(
            if(lowerUTF8(JSONExtractString(payload, 'ATIVO')) IN ('true', '1', 't', 's'), 1, 0),
            source_ts_ms
          ) AS ativo
        FROM {CURRENT_DB}.stg_funcionarios FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_filial = {{id_filial:Int32}}
          AND is_deleted = 0
          AND id_funcionario > 0
        GROUP BY id_funcionario
        HAVING argMax(
          if(lowerUTF8(JSONExtractString(payload, 'ATIVO')) IN ('true', '1', 't', 's'), 1, 0),
          source_ts_ms
        ) = 1
        ORDER BY nome ASC, id_funcionario ASC
        """,
        parameters={"id_empresa": int(id_empresa), "id_filial": int(id_filial)},
    )
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        fid = int(r.get("id_funcionario") or 0)
        if fid <= 0:
            continue
        out.append(
            {
                "id_funcionario": fid,
                "nome": _clean_employee_nome(str(r.get("nome") or "").strip(), fid),
                "funcao": str(r.get("funcao") or "").strip(),
            }
        )
    return out


def _clean_employee_nome(nome: str, id_funcionario: int) -> str:
    """Evita exibir código/id como nome quando o payload Xpert está incompleto."""
    text = (nome or "").strip()
    if not text:
        return ""
    if text == str(id_funcionario):
        return ""
    if text.isdigit() and int(text) == int(id_funcionario):
        return ""
    return text


def get_config_employees(config_id: int) -> List[Dict[str, Any]]:
    sql = """
      SELECT id, config_id, id_funcionario, nome_funcionario_snapshot,
             funcao_snapshot, include_in_commission, is_active
      FROM app.commission_config_employee
      WHERE config_id = %s AND is_active = true
      ORDER BY nome_funcionario_snapshot, id_funcionario
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, [config_id]).fetchall()]


def merge_config_employees(
    id_empresa: int,
    id_filial: int,
    config_id: int,
) -> List[Dict[str, Any]]:
    """Lista funcionários Xpert + overrides salvos (include_in_commission)."""
    branch_rows = list_branch_employees_ch(id_empresa, id_filial)
    saved = {
        int(r["id_funcionario"]): r
        for r in get_config_employees(config_id)
        if int(r.get("id_funcionario") or 0) > 0
    }
    merged: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for row in branch_rows:
        fid = int(row["id_funcionario"])
        seen.add(fid)
        saved_row = saved.get(fid)
        snap = saved_row or {}
        merged.append(
            {
                "id_funcionario": fid,
                "nome": _clean_employee_nome(
                    str(snap.get("nome_funcionario_snapshot") or row.get("nome") or ""),
                    fid,
                ),
                "funcao": str(snap.get("funcao_snapshot") or row.get("funcao") or ""),
                "include_in_commission": bool(
                    saved_row.get("include_in_commission") if saved_row else True
                ),
            }
        )
    for fid, saved_row in sorted(saved.items(), key=lambda x: (str(x[1].get("nome_funcionario_snapshot") or ""), x[0])):
        if fid in seen:
            continue
        merged.append(
            {
                "id_funcionario": fid,
                "nome": _clean_employee_nome(
                    str(saved_row.get("nome_funcionario_snapshot") or f"Funcionário {fid}"),
                    fid,
                ) or "Nome não cadastrado",
                "funcao": str(saved_row.get("funcao_snapshot") or ""),
                "include_in_commission": bool(saved_row.get("include_in_commission")),
            }
        )
    return merged


def get_excluded_funcionario_ids(config_id: int) -> Optional[set[int]]:
    """IDs excluídos do cálculo. None = sem overrides (legado: todos com venda entram)."""
    rows = get_config_employees(config_id)
    if not rows:
        return None
    excluded: set[int] = set()
    for r in rows:
        fid = int(r.get("id_funcionario") or 0)
        if fid > 0 and not bool(r.get("include_in_commission")):
            excluded.add(fid)
    return excluded


def get_config_tiers(config_id: int) -> List[Dict[str, Any]]:
    """Get tiers for a config ordered by sort_order."""
    sql = """
      SELECT id, config_id, tier_key, tier_name, min_sales_amount,
             commission_percent, sort_order, is_active
      FROM app.commission_config_tier
      WHERE config_id = %s
      ORDER BY sort_order
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, [config_id]).fetchall()]


def get_available_groups(id_empresa: int, id_filial: int) -> List[Dict[str, Any]]:
    """Grupos da filial com faturamento 30d — ClickHouse slim/dim (PG só fallback)."""
    from datetime import date, timedelta

    dk_fim = int(date.today().strftime("%Y%m%d"))
    dk_ini = int((date.today() - timedelta(days=30)).strftime("%Y%m%d"))
    try:
        rows = query_dict(
            f"""
            SELECT
              g.id_grupo_produto AS id_grupo_produto,
              argMax(g.nome, g.source_ts_ms) AS nome,
              round(coalesce(sum(i.total), 0), 2) AS faturamento_30d
            FROM {CURRENT_DB}.dim_grupo_produto AS g FINAL
            LEFT JOIN {CURRENT_DB}.stg_itenscomprovantes_slim AS i FINAL
              ON i.id_empresa = g.id_empresa
             AND i.id_filial = g.id_filial
             AND i.id_grupo_produto = g.id_grupo_produto
             AND i.data_key >= {{dk_ini:Int32}}
             AND i.data_key <= {{dk_fim:Int32}}
             AND i.is_deleted = 0
             AND {_cfop_sales_predicate_sql("i")}
            LEFT JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
              ON c.id_empresa = i.id_empresa
             AND c.id_filial = i.id_filial
             AND c.id_db = i.id_db
             AND c.id_comprovante = i.id_comprovante
             AND c.is_deleted = 0
             AND c.cancelado = 0
            WHERE g.id_empresa = {{id_empresa:Int32}}
              AND g.id_filial = {{id_filial:Int32}}
              AND g.is_deleted = 0
            GROUP BY g.id_grupo_produto
            ORDER BY nome, id_grupo_produto
            """,
            parameters={
                "id_empresa": int(id_empresa),
                "id_filial": int(id_filial),
                "dk_ini": dk_ini,
                "dk_fim": dk_fim,
            },
        )
        if rows:
            return [
                {
                    "id_grupo_produto": int(r["id_grupo_produto"]),
                    "nome": str(r.get("nome") or f"Grupo {r['id_grupo_produto']}"),
                    "faturamento_30d": float(r.get("faturamento_30d") or 0),
                }
                for r in rows
                if r.get("id_grupo_produto") is not None
            ]
    except Exception as exc:
        logger.warning("get_available_groups CH miss: %s", str(exc)[:180])

    sql = f"""
      SELECT
        g.id_grupo_produto,
        g.nome,
        COALESCE(s.faturamento_30d, 0)::numeric(18,2) AS faturamento_30d
      FROM dw.dim_grupo_produto g
      LEFT JOIN LATERAL (
        SELECT SUM(i.total) AS faturamento_30d
        FROM dw.fact_venda v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa AND i.id_filial = v.id_filial
          AND i.id_db = v.id_db AND i.id_comprovante = v.id_comprovante
        WHERE v.id_empresa = g.id_empresa AND v.id_filial = g.id_filial
          AND i.id_grupo_produto = g.id_grupo_produto
          AND v.data_key >= to_char(CURRENT_DATE - interval '30 days', 'YYYYMMDD')::integer
          AND COALESCE(v.cancelado, false) = false
          AND {_cfop_sales_predicate_sql("i")}
      ) s ON true
      WHERE g.id_empresa = %s AND g.id_filial = %s
      ORDER BY g.nome
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(r) for r in conn.execute(sql, [id_empresa, id_filial]).fetchall()]


def get_group_products(
    id_empresa: int,
    id_filial: int,
    id_grupo_produto: int,
) -> List[Dict[str, Any]]:
    """List products of a group — CH dim primeiro; PG dw fallback."""
    try:
        rows = query_dict(
            f"""
            SELECT
              id_produto,
              coalesce(nullIf(argMax(nome, source_ts_ms), ''), concat('Produto ', toString(id_produto))) AS nome,
              id_grupo_produto
            FROM {CURRENT_DB}.dim_produto FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND id_filial = {{id_filial:Int32}}
              AND id_grupo_produto = {{id_grupo:Int32}}
              AND is_deleted = 0
            GROUP BY id_produto, id_grupo_produto
            ORDER BY nome, id_produto
            """,
            parameters={
                "id_empresa": int(id_empresa),
                "id_filial": int(id_filial),
                "id_grupo": int(id_grupo_produto),
            },
        )
        if rows:
            return [
                {
                    "id_produto": int(r["id_produto"]),
                    "nome": str(r.get("nome") or f"Produto {r['id_produto']}"),
                    "id_grupo_produto": int(r.get("id_grupo_produto") or id_grupo_produto),
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("get_group_products CH miss: %s", str(exc)[:180])

    sql = """
      SELECT
        p.id_produto,
        COALESCE(NULLIF(TRIM(p.nome), ''), 'Produto ' || p.id_produto::text) AS nome,
        p.id_grupo_produto
      FROM dw.dim_produto p
      WHERE p.id_empresa = %s
        AND p.id_filial = %s
        AND p.id_grupo_produto = %s
      ORDER BY p.nome, p.id_produto
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(r) for r in conn.execute(sql, [id_empresa, id_filial, id_grupo_produto]).fetchall()]


def ensure_default_config(id_empresa: int, id_filial: int) -> Dict[str, Any]:
    """Create default config if none exists. Returns active config."""
    existing = get_config(id_empresa, id_filial)
    if existing:
        return existing

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute("""
            INSERT INTO app.commission_config (
                id_empresa,
                id_filial,
                name,
                is_active,
                default_payment_mode,
                manager_commission_mode,
                manager_commission_percent
            )
            VALUES (%s, %s, 'Comissao padrao', true, 'individual_sales', 'use_tiers', 0)
            ON CONFLICT DO NOTHING
            RETURNING id, id_empresa, id_filial, name, is_active, default_payment_mode,
                      manager_commission_mode, manager_commission_percent,
                      created_at, updated_at
        """, [id_empresa, id_filial]).fetchone()

        if not row:
            existing = get_config(id_empresa, id_filial)
            if existing:
                return existing
            raise RuntimeError("Failed to create commission config")

        config = dict(row)
        config_id = config["id"]

        for tier in DEFAULT_TIERS:
            conn.execute("""
              INSERT INTO app.commission_config_tier
                (config_id, tier_key, tier_name, min_sales_amount, commission_percent, sort_order, is_active)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [config_id, tier["tier_key"], tier["tier_name"],
                  tier["min_sales_amount"], tier["commission_percent"],
                  tier["sort_order"], tier["is_active"]])

    return config


def update_preferences(
    id_empresa: int, id_filial: int, *, include_central_mirror: bool
) -> Dict[str, Any]:
    """Atualiza preferências de cálculo (toggle na tela de comissão de vendedores)."""
    config = ensure_default_config(id_empresa, id_filial)
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(
            """
            UPDATE app.commission_config
            SET include_central_mirror = %s, updated_at = now()
            WHERE id = %s
            RETURNING id, id_empresa, id_filial, name, is_active, default_payment_mode,
                      manager_commission_mode, manager_commission_percent,
                      include_central_mirror, created_at, updated_at
            """,
            [bool(include_central_mirror), int(config["id"])],
        ).fetchone()
        conn.commit()
    return dict(row) if row else config


def save_config(
    id_empresa: int,
    id_filial: int,
    groups: List[Dict[str, Any]],
    tiers: List[Dict[str, Any]],
    default_payment_mode: str = "individual_sales",
    manager_commission_mode: str = "use_tiers",
    manager_commission_percent: float = 0.0,
    excluded_products: Optional[Sequence[Dict[str, Any]]] = None,
    employees: Optional[Sequence[Dict[str, Any]]] = None,
    include_central_mirror: Optional[bool] = None,
) -> Dict[str, Any]:
    """Save/update commission configuration atomically."""
    config = ensure_default_config(id_empresa, id_filial)
    config_id = config["id"]
    excluded = list(excluded_products or [])
    employees = list(employees or [])

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        if include_central_mirror is not None:
            conn.execute(
                """
              UPDATE app.commission_config
              SET default_payment_mode = %s,
                  manager_commission_mode = %s,
                  manager_commission_percent = %s,
                  include_central_mirror = %s,
                  updated_at = now()
              WHERE id = %s
            """,
                [
                    default_payment_mode,
                    manager_commission_mode,
                    manager_commission_percent,
                    bool(include_central_mirror),
                    config_id,
                ],
            )
        else:
            conn.execute(
                """
              UPDATE app.commission_config
              SET default_payment_mode = %s,
                  manager_commission_mode = %s,
                  manager_commission_percent = %s,
                  updated_at = now()
              WHERE id = %s
            """,
                [default_payment_mode, manager_commission_mode, manager_commission_percent, config_id],
            )

        conn.execute("""
            UPDATE app.commission_config_group SET is_active = false WHERE config_id = %s
        """, [config_id])

        for g in groups:
            conn.execute("""
                INSERT INTO app.commission_config_group
                    (config_id, id_grupo_produto, nome_grupo_produto_snapshot, is_active)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (config_id, id_grupo_produto) WHERE is_active = true
                DO UPDATE SET is_active = true, nome_grupo_produto_snapshot = EXCLUDED.nome_grupo_produto_snapshot
            """, [config_id, g["id_grupo_produto"], g.get("nome", "")])

        conn.execute("""
            UPDATE app.commission_config_product_exclude SET is_active = false WHERE config_id = %s
        """, [config_id])

        for p in excluded:
            pid = int(p.get("id_produto") or 0)
            if pid <= 0:
                continue
            conn.execute("""
                INSERT INTO app.commission_config_product_exclude
                    (config_id, id_produto, nome_produto_snapshot, is_active)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (config_id, id_produto) WHERE is_active = true
                DO UPDATE SET is_active = true, nome_produto_snapshot = EXCLUDED.nome_produto_snapshot
            """, [config_id, pid, str(p.get("nome") or p.get("nome_produto_snapshot") or "")])

        conn.execute("""
            UPDATE app.commission_config_employee SET is_active = false WHERE config_id = %s
        """, [config_id])
        for emp in employees:
            fid = int(emp.get("id_funcionario") or 0)
            if fid <= 0:
                continue
            conn.execute("""
                INSERT INTO app.commission_config_employee
                    (config_id, id_funcionario, nome_funcionario_snapshot, funcao_snapshot,
                     include_in_commission, is_active, updated_at)
                VALUES (%s, %s, %s, %s, %s, true, now())
                ON CONFLICT (config_id, id_funcionario) WHERE is_active = true
                DO UPDATE SET
                    nome_funcionario_snapshot = EXCLUDED.nome_funcionario_snapshot,
                    funcao_snapshot = EXCLUDED.funcao_snapshot,
                    include_in_commission = EXCLUDED.include_in_commission,
                    is_active = true,
                    updated_at = now()
            """, [
                config_id,
                fid,
                str(emp.get("nome") or emp.get("nome_funcionario_snapshot") or ""),
                str(emp.get("funcao") or emp.get("funcao_snapshot") or ""),
                bool(emp.get("include_in_commission", True)),
            ])

        conn.execute("DELETE FROM app.commission_config_tier WHERE config_id = %s", [config_id])
        for tier in tiers:
            conn.execute("""
              INSERT INTO app.commission_config_tier
                (config_id, tier_key, tier_name, min_sales_amount, commission_percent, sort_order, is_active)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [config_id, tier["tier_key"], tier["tier_name"],
                                    _tier_min_qty(tier), tier["commission_percent"],
                                    tier["sort_order"], tier.get("is_active", True)])

    return get_config(id_empresa, id_filial)


def _determine_tier(total_qty: float, tiers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Highest tier achieved by eligible product quantity (not R$)."""
    active_tiers = [t for t in tiers if t.get("is_active", True)]
    active_tiers.sort(key=lambda t: _tier_min_qty(t), reverse=True)
    for tier in active_tiers:
        if float(total_qty or 0) >= _tier_min_qty(tier):
            return tier
    return None


def _next_tier(current_tier: Optional[Dict[str, Any]], tiers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Get the next tier above current (by quantity threshold)."""
    active_tiers = sorted(
        [t for t in tiers if t.get("is_active", True)],
        key=lambda t: _tier_min_qty(t),
    )
    if current_tier is None:
        return active_tiers[0] if active_tiers else None
    current_min = _tier_min_qty(current_tier)
    for tier in active_tiers:
        if _tier_min_qty(tier) > current_min:
            return tier
    return None


def _sort_sellers_by_tier(employee_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ordena vendedores para o grid de comissão.

    Exceção ao contrato Filial→Data→Nome: ranking por métrica de comissão.
    Filial ASC → comissão DESC → quantidade DESC → venda DESC → nome ASC.
    (Comissão zerada deixa de parecer “bagunçada”: cai para qtd/venda.)
    O frontend agrupa por filial; esta ordem estabiliza a lista plana da API.
    """
    return sorted(
        employee_list,
        key=lambda e: (
            str(e.get("filial_label") or "").casefold(),
            int(e.get("id_filial") or 0),
            -float(e.get("comissao_estimada") or 0),
            -float(e.get("quantidade_vendas") or 0),
            -float(e.get("venda_elegivel") or 0),
            str(e.get("nome_vendedor") or "").casefold(),
            int(e.get("id_funcionario") or 0),
        ),
    )


def _filial_labels(id_empresa: int, id_filiais: Sequence[int]) -> Dict[int, str]:
    """Resolve apelido/nome curto for branch labels (stock/commission grids)."""
    targets = [int(f) for f in id_filiais if int(f) > 0]
    if not targets:
        return {}
    labels: Dict[int, str] = {}
    try:
        with get_conn(tenant_id=id_empresa) as conn:
            rows = conn.execute(
                """
                SELECT id_filial,
                       COALESCE(
                         NULLIF(TRIM(apelido), ''),
                         NULLIF(TRIM(nome), ''),
                         'Filial ' || id_filial::text
                       ) AS label
                FROM auth.filiais
                WHERE id_empresa = %s AND id_filial = ANY(%s)
                """,
                [id_empresa, targets],
            ).fetchall()
            for r in rows:
                labels[int(r["id_filial"])] = str(r["label"])
    except Exception:
        labels = {}
    for fid in targets:
        labels.setdefault(fid, f"Filial {fid}")
    return labels


def calculate_commission_results_multi(
    id_empresa: int,
    id_filiais: Sequence[int],
    dt_ini: date,
    dt_fim: date,
    payment_mode: Optional[str] = None,
    include_central_mirror: Optional[bool] = None,
) -> Dict[str, Any]:
    """Calculate employee commissions for one or many branches.

    Each seller row keeps its own ``id_filial`` / ``filial_label`` (config is
    still per-branch). Rows: filial ASC → comissão DESC → qtd DESC → venda DESC → nome.

    Sales come from ClickHouse slim in a single batched query (not PG fact_*).
    """
    targets = sorted({int(f) for f in id_filiais if int(f) > 0})
    if not targets:
        return _empty_results(dt_ini, dt_fim, 0, reason="no_config")

    labels = _filial_labels(id_empresa, targets)
    valid_modes = ("team_total", "equal_split", "individual_sales")
    explicit_mode = payment_mode if payment_mode in valid_modes else None

    # Uma leitura CH para todas as filiais do escopo (evita N× scan em fact_* PG).
    try:
        if include_central_mirror is not None:
            raw_sales = _query_eligible_sales_ch(
                id_empresa,
                targets,
                dt_ini,
                dt_fim,
                include_central_mirror=bool(include_central_mirror),
            )
        else:
            with_mirror: List[int] = []
            without_mirror: List[int] = []
            for fid in targets:
                cfg = get_config(id_empresa, fid) or {}
                if bool(cfg.get("include_central_mirror")):
                    with_mirror.append(fid)
                else:
                    without_mirror.append(fid)
            raw_sales = []
            if without_mirror:
                raw_sales.extend(
                    _query_eligible_sales_ch(
                        id_empresa,
                        without_mirror,
                        dt_ini,
                        dt_fim,
                        include_central_mirror=False,
                    )
                )
            if with_mirror:
                raw_sales.extend(
                    _query_eligible_sales_ch(
                        id_empresa,
                        with_mirror,
                        dt_ini,
                        dt_fim,
                        include_central_mirror=True,
                    )
                )
    except Exception as exc:
        logger.exception(
            "commission CH sales failed empresa=%s period=%s..%s",
            id_empresa,
            dt_ini.isoformat(),
            dt_fim.isoformat(),
        )
        raise RuntimeError(f"Falha ao consultar vendas elegíveis no analytics: {exc}") from exc
    sales_by_filial: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in raw_sales:
        sales_by_filial[int(row["id_filial"])].append(row)

    all_sellers: List[Dict[str, Any]] = []
    all_groups: List[Dict[str, Any]] = []
    comissao_total = 0.0
    venda_elegivel = 0.0
    quantidade_vendas = 0.0
    branch_results: List[Dict[str, Any]] = []

    for fid in targets:
        mode = explicit_mode
        if mode is None:
            cfg = get_config(id_empresa, fid)
            mode = str((cfg or {}).get("default_payment_mode") or "individual_sales")
            if mode not in valid_modes:
                mode = "individual_sales"
        branch = calculate_commission_results(
            id_empresa,
            fid,
            dt_ini,
            dt_fim,
            mode,
            sales_rows=sales_by_filial.get(fid, []),
            include_manager=False,
            include_central_mirror=(
                bool(include_central_mirror)
                if include_central_mirror is not None
                else None
            ),
        )
        label = labels.get(fid, f"Filial {fid}")
        branch["filial_label"] = label
        for emp in branch.get("vendedores") or []:
            emp["id_filial"] = fid
            emp["filial_label"] = label
            all_sellers.append(emp)
        for g in branch.get("grupos_configurados") or []:
            row = dict(g)
            row["id_filial"] = fid
            row["filial_label"] = label
            all_groups.append(row)
        comissao_total += float(branch.get("comissao_total") or 0)
        venda_elegivel += float(branch.get("venda_elegivel") or 0)
        quantidade_vendas += float(branch.get("quantidade_vendas") or 0)
        branch_results.append(branch)

    all_sellers = _sort_sellers_by_tier(all_sellers)
    all_groups.sort(
        key=lambda g: (
            str(g.get("filial_label") or "").casefold(),
            int(g.get("id_filial") or 0),
            str(g.get("nome") or "").casefold(),
            int(g.get("id_grupo_produto") or 0),
        )
    )

    single = len(targets) == 1
    primary = branch_results[0] if branch_results else {}
    return {
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "id_filial": targets[0] if single else None,
        "id_filiais": targets,
        "multi_filial": not single,
        "payment_mode": explicit_mode or (primary.get("payment_mode") if single else "per_branch"),
        "venda_elegivel": round(venda_elegivel, 2),
        "quantidade_vendas": round(quantidade_vendas, 4),
        "nivel_atingido": primary.get("nivel_atingido") if single else None,
        "percentual_aplicado": primary.get("percentual_aplicado") if single else None,
        "comissao_total": round(comissao_total, 2),
        "proximo_nivel": primary.get("proximo_nivel") if single else None,
        "vendedores": all_sellers,
        "vendedores_elegiveis": len(all_sellers),
        "grupos_configurados": all_groups,
        "produtos_excluidos": primary.get("produtos_excluidos") if single else [],
        "tier_progress": primary.get("tier_progress") if single else [],
        "gerente": primary.get("gerente") if single else None,
        "config": primary.get("config") if single else None,
        "branches": [
            {
                "id_filial": b.get("id_filial"),
                "filial_label": b.get("filial_label"),
                "venda_elegivel": b.get("venda_elegivel"),
                "comissao_total": b.get("comissao_total"),
                "payment_mode": b.get("payment_mode"),
                "message": b.get("message"),
            }
            for b in branch_results
        ],
        "message": (
            None
            if all_sellers
            else "Não há vendas elegíveis para o período/filiais selecionados."
        ),
    }


def calculate_commission_results(
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    payment_mode: str = "individual_sales",
    *,
    sales_rows: Optional[Sequence[Dict[str, Any]]] = None,
    include_manager: bool = False,
    include_central_mirror: Optional[bool] = None,
) -> Dict[str, Any]:
    """Calculate commission results for a date range using individual commission rules.

    ``sales_rows``: pré-leitura CH (grão produto) para reuso no multi-filial.
    ``include_manager``: legado; endpoint de vendedores não usa gerente (há rota própria).
    """
    config = get_config(id_empresa, id_filial)
    if not config:
        return _empty_results(dt_ini, dt_fim, id_filial, reason="no_config")

    config_id = config["id"]
    groups = get_config_groups(config_id)
    tiers = get_config_tiers(config_id)
    excludes = get_config_product_excludes(config_id)

    valid_modes = ("team_total", "equal_split", "individual_sales")
    config_default_mode = str(config.get("default_payment_mode") or "individual_sales")
    effective_mode = payment_mode if payment_mode in valid_modes else config_default_mode
    if effective_mode not in valid_modes:
        effective_mode = "individual_sales"

    if not groups:
        return _empty_results(dt_ini, dt_fim, id_filial, reason="no_groups", payment_mode=effective_mode)

    group_ids = [g["id_grupo_produto"] for g in groups]
    exclude_ids = [int(p["id_produto"]) for p in excludes if int(p.get("id_produto") or 0) > 0]
    excluded_func_ids = get_excluded_funcionario_ids(config_id)

    if sales_rows is None:
        mirror = (
            bool(include_central_mirror)
            if include_central_mirror is not None
            else bool(config.get("include_central_mirror"))
        )
        try:
            sales_rows = _query_eligible_sales_ch(
                id_empresa,
                [id_filial],
                dt_ini,
                dt_fim,
                include_central_mirror=mirror,
            )
        except Exception as exc:
            logger.exception(
                "commission CH sales failed empresa=%s filial=%s period=%s..%s",
                id_empresa,
                id_filial,
                dt_ini.isoformat(),
                dt_fim.isoformat(),
            )
            raise RuntimeError(f"Falha ao consultar vendas elegíveis no analytics: {exc}") from exc

    rows = _filter_sales_for_config(sales_rows, group_ids, exclude_ids, excluded_func_ids)

    manager_sales = 0.0
    manager_qty = 0.0
    manager_tier = None
    manager_mode = str(config.get("manager_commission_mode") or "use_tiers")
    manager_fixed_percent = float(config.get("manager_commission_percent") or 0)
    manager_percent = 0.0
    manager_commission_gross = 0.0

    if include_manager:
        dk_ini, dk_fim = data_key_bounds_half_open(dt_ini, dt_fim)
        manager_sql = f"""
          SELECT
            COALESCE(SUM(i.total), 0)::numeric(18,2) AS venda_total_sem_combustiveis,
            COALESCE(SUM(i.qtd), 0)::numeric(18,4) AS quantidade_sem_combustiveis
          FROM dw.fact_venda v
          JOIN dw.fact_venda_item i
            ON i.id_empresa = v.id_empresa
           AND i.id_filial = v.id_filial
           AND i.id_db = v.id_db
           AND i.id_comprovante = v.id_comprovante
          LEFT JOIN dw.dim_grupo_produto g
            ON g.id_empresa = i.id_empresa
           AND g.id_filial = i.id_filial
           AND g.id_grupo_produto = i.id_grupo_produto
          WHERE v.id_empresa = %s
            AND v.id_filial = %s
            AND v.data_key >= %s
            AND v.data_key < %s
            {_active_sale_sql("v")}
            AND {_cfop_sales_predicate_sql("i")}
            AND COALESCE(UPPER(g.nome), '') NOT LIKE 'COMBUST%%'
        """
        with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            manager_row = conn.execute(manager_sql, [id_empresa, id_filial, dk_ini, dk_fim]).fetchone()
        manager_sales = float((manager_row or {}).get("venda_total_sem_combustiveis") or 0)
        manager_qty = float((manager_row or {}).get("quantidade_sem_combustiveis") or 0)
        manager_tier = _determine_tier(manager_qty, tiers)
        if manager_mode == "fixed_percent":
            manager_percent = manager_fixed_percent
        else:
            manager_percent = float(manager_tier["commission_percent"]) if manager_tier else 0.0
        manager_commission_gross = round(manager_sales * manager_percent / 100, 2)

    if not rows:
        return _empty_results(
            dt_ini,
            dt_fim,
            id_filial,
            reason="no_sales",
            payment_mode=effective_mode,
            groups=groups,
            tiers=tiers,
            manager_data={
                "venda_total_sem_combustiveis": round(manager_sales, 2),
                "nivel_atingido": _tier_public(manager_tier),
                "percentual_aplicado": manager_percent,
                "modo_comissao": manager_mode,
                "percentual_configurado": manager_fixed_percent,
                "comissao_bruta": manager_commission_gross,
            },
        )

    employees: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        emp_id = int(r["id_funcionario"])
        if emp_id not in employees:
            employees[emp_id] = {
                "id_funcionario": emp_id,
                "nome_vendedor": r["nome_vendedor"],
                "venda_elegivel": 0.0,
                "quantidade_vendas": 0,
            }
        employees[emp_id]["venda_elegivel"] += float(r["venda_total"] or 0)
        employees[emp_id]["quantidade_vendas"] += float(r["quantidade_vendas"] or 0)

    for emp in employees.values():
        emp["quantidade_vendas"] = round(float(emp["quantidade_vendas"] or 0), 4)

    employee_list = sorted(employees.values(), key=lambda e: e["venda_elegivel"], reverse=True)
    total_eligible = sum(float(e["venda_elegivel"] or 0) for e in employee_list)
    total_qty = sum(float(e["quantidade_vendas"] or 0) for e in employee_list)
    n_eligible = sum(1 for e in employee_list if float(e["venda_elegivel"] or 0) > 0)

    team_tier_payload = None
    team_percent: Optional[float] = None
    next_tier_payload = None

    if effective_mode == "individual_sales":
        commission_total = 0.0
        for emp in employee_list:
            emp_tier = _determine_tier(float(emp["quantidade_vendas"] or 0), tiers)
            emp_percent = float(emp_tier["commission_percent"]) if emp_tier else 0.0
            emp_commission = round(float(emp["venda_elegivel"]) * emp_percent / 100, 2)
            commission_total += emp_commission
            emp["nivel_atingido"] = _tier_public(emp_tier)
            emp["percentual_aplicado"] = emp_percent
            emp["comissao_estimada"] = emp_commission
        commission_total = round(commission_total, 2)
    else:
        team_tier = _determine_tier(total_qty, tiers)
        team_percent = float(team_tier["commission_percent"]) if team_tier else 0.0
        commission_total = round(total_eligible * team_percent / 100, 2)
        team_tier_payload = _tier_public(team_tier)
        next_tier_raw = _next_tier(team_tier, tiers)
        next_tier_payload = (
            {
                **_tier_public(next_tier_raw),
                "falta": round(max(0.0, _tier_min_qty(next_tier_raw) - total_qty), 4),
            }
            if next_tier_raw
            else None
        )
        equal_share = round(commission_total / n_eligible, 2) if n_eligible else 0.0
        for emp in employee_list:
            emp_eligible = float(emp["venda_elegivel"] or 0)
            emp["nivel_atingido"] = team_tier_payload
            emp["percentual_aplicado"] = team_percent
            if effective_mode == "equal_split":
                emp["comissao_estimada"] = equal_share if emp_eligible > 0 else 0.0
            else:
                emp["comissao_estimada"] = (
                    round(commission_total * (emp_eligible / total_eligible), 2)
                    if total_eligible > 0 else 0.0
                )

    group_totals = {}
    for r in rows:
        gid = int(r["id_grupo_produto"])
        if gid not in group_totals:
            group_totals[gid] = {"id_grupo_produto": gid, "nome": r["nome_grupo_produto"], "venda_total": 0}
        group_totals[gid]["venda_total"] += float(r["venda_total"] or 0)

    filial_label = _filial_labels(id_empresa, [id_filial]).get(id_filial, f"Filial {id_filial}")
    for emp in employee_list:
        emp["id_filial"] = id_filial
        emp["filial_label"] = filial_label
    employee_list = _sort_sellers_by_tier(employee_list)
    groups_out = list(group_totals.values())
    for g in groups_out:
        g["id_filial"] = id_filial
        g["filial_label"] = filial_label

    return {
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "id_filial": id_filial,
        "id_filiais": [id_filial],
        "multi_filial": False,
        "filial_label": filial_label,
        "payment_mode": effective_mode,
        "venda_elegivel": round(total_eligible, 2),
        "quantidade_vendas": round(total_qty, 4),
        "nivel_atingido": team_tier_payload if effective_mode != "individual_sales" else None,
        "percentual_aplicado": team_percent if effective_mode != "individual_sales" else None,
        "comissao_total": round(commission_total, 2),
        "proximo_nivel": next_tier_payload if effective_mode != "individual_sales" else None,
        "vendedores": employee_list,
        "vendedores_elegiveis": len(employee_list),
        "grupos_configurados": groups_out,
        "produtos_excluidos": [
            {"id_produto": int(p["id_produto"]), "nome": p.get("nome_produto_snapshot") or ""}
            for p in excludes
        ],
        "tier_progress": [],
        "gerente": {
            "venda_total_sem_combustiveis": round(manager_sales, 2),
            "nivel_atingido": _tier_public(manager_tier),
            "percentual_aplicado": manager_percent,
            "modo_comissao": manager_mode,
            "percentual_configurado": manager_fixed_percent,
            "comissao_bruta": manager_commission_gross,
        },
        "config": {
            "id": config_id,
            "name": config["name"],
            "default_payment_mode": config["default_payment_mode"],
            "manager_commission_mode": manager_mode,
            "manager_commission_percent": manager_fixed_percent,
            "include_central_mirror": bool(config.get("include_central_mirror")),
        },
    }



def _empty_results(
    dt_ini: date,
    dt_fim: date,
    id_filial: int,
    reason: str = "no_config",
    payment_mode: str = "individual_sales",
    groups: Optional[List] = None,
    tiers: Optional[List] = None,
    manager_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return empty structure with appropriate message."""
    messages = {
        "no_config": "Nenhuma configuração de comissão encontrada para esta filial.",
        "no_groups": "Nenhum grupo configurado para comissão nesta filial. Acesse Configuração para selecionar os grupos participantes.",
        "no_sales": "Não há vendas elegíveis para o período selecionado.",
    }
    return {
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "id_filial": id_filial,
        "payment_mode": payment_mode,
        "venda_elegivel": 0,
        "quantidade_vendas": 0,
        "nivel_atingido": None,
        "percentual_aplicado": None,
        "comissao_total": 0,
        "proximo_nivel": None,
        "vendedores": [],
        "vendedores_elegiveis": 0,
        "grupos_configurados": [],
        "produtos_excluidos": [],
        "tier_progress": [],
        "gerente": manager_data or {
            "venda_total_sem_combustiveis": 0,
            "nivel_atingido": None,
            "percentual_aplicado": 0,
            "modo_comissao": "use_tiers",
            "percentual_configurado": 0,
            "comissao_bruta": 0,
        },
        "config": None,
        "message": messages.get(reason, ""),
    }
