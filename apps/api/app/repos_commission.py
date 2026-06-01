"""Commission configuration and calculation repository."""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from app.db import get_conn

logger = logging.getLogger(__name__)

# Default tiers when no configuration exists
DEFAULT_TIERS = [
    {"tier_key": "bronze", "tier_name": "Bronze", "min_sales_amount": 30000, "commission_percent": 0.5, "sort_order": 1, "is_active": True},
    {"tier_key": "silver", "tier_name": "Prata", "min_sales_amount": 50000, "commission_percent": 1.0, "sort_order": 2, "is_active": True},
    {"tier_key": "gold", "tier_name": "Ouro", "min_sales_amount": 80000, "commission_percent": 1.5, "sort_order": 3, "is_active": True},
    {"tier_key": "diamond", "tier_name": "Diamante", "min_sales_amount": 120000, "commission_percent": 2.0, "sort_order": 4, "is_active": True},
]


def _conn_branch_id(id_filial: Optional[int]) -> Optional[int]:
    return id_filial if id_filial and id_filial > 0 else None


def get_config(id_empresa: int, id_filial: int) -> Optional[Dict[str, Any]]:
    """Get active commission config for empresa/filial."""
    sql = """
          SELECT id, id_empresa, id_filial, name, is_active, default_payment_mode,
              manager_commission_mode, manager_commission_percent,
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
    """Get all product groups available for the filial with recent revenue."""
    sql = """
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
          AND COALESCE(i.cfop, 0) >= 5000
      ) s ON true
      WHERE g.id_empresa = %s AND g.id_filial = %s
      ORDER BY g.nome
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        return [dict(r) for r in conn.execute(sql, [id_empresa, id_filial]).fetchall()]


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
                    VALUES (%s, %s, 'Comissao padrao', true, 'team_total', 'use_tiers', 0)
                    ON CONFLICT DO NOTHING
                    RETURNING id, id_empresa, id_filial, name, is_active, default_payment_mode,
                                        manager_commission_mode, manager_commission_percent,
                                        created_at, updated_at
                """, [id_empresa, id_filial]).fetchone()

        if not row:
            # Conflict: config was created concurrently
            existing = get_config(id_empresa, id_filial)
            if existing:
                return existing
            raise RuntimeError("Failed to create commission config")

        config = dict(row)
        config_id = config["id"]

        # Insert default tiers
        for tier in DEFAULT_TIERS:
            conn.execute("""
              INSERT INTO app.commission_config_tier
                (config_id, tier_key, tier_name, min_sales_amount, commission_percent, sort_order, is_active)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [config_id, tier["tier_key"], tier["tier_name"],
                  tier["min_sales_amount"], tier["commission_percent"],
                  tier["sort_order"], tier["is_active"]])

    return config


def save_config(
    id_empresa: int,
    id_filial: int,
    groups: List[Dict[str, Any]],
    tiers: List[Dict[str, Any]],
    default_payment_mode: str = "team_total",
    manager_commission_mode: str = "use_tiers",
    manager_commission_percent: float = 0.0,
) -> Dict[str, Any]:
    """Save/update commission configuration atomically."""
    config = ensure_default_config(id_empresa, id_filial)
    config_id = config["id"]

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        # Update config
        conn.execute("""
          UPDATE app.commission_config
          SET default_payment_mode = %s,
              manager_commission_mode = %s,
              manager_commission_percent = %s,
              updated_at = now()
          WHERE id = %s
        """, [default_payment_mode, manager_commission_mode, manager_commission_percent, config_id])

        # Replace groups: deactivate all, then insert/activate selected
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

        # Replace tiers
        conn.execute("DELETE FROM app.commission_config_tier WHERE config_id = %s", [config_id])
        for tier in tiers:
            conn.execute("""
              INSERT INTO app.commission_config_tier
                (config_id, tier_key, tier_name, min_sales_amount, commission_percent, sort_order, is_active)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [config_id, tier["tier_key"], tier["tier_name"],
                                    tier["min_sales_amount"], tier["commission_percent"],
                                    tier["sort_order"], tier.get("is_active", True)])

    return get_config(id_empresa, id_filial)


def _determine_tier(total_sales: float, tiers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Determine the highest tier achieved by total_sales."""
    active_tiers = [t for t in tiers if t.get("is_active", True)]
    active_tiers.sort(key=lambda t: float(t["min_sales_amount"]), reverse=True)
    for tier in active_tiers:
        if total_sales >= float(tier["min_sales_amount"]):
            return tier
    return None


def _next_tier(current_tier: Optional[Dict[str, Any]], tiers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Get the next tier above current."""
    active_tiers = sorted(
        [t for t in tiers if t.get("is_active", True)],
        key=lambda t: float(t["min_sales_amount"])
    )
    if current_tier is None:
        return active_tiers[0] if active_tiers else None
    current_min = float(current_tier["min_sales_amount"])
    for tier in active_tiers:
        if float(tier["min_sales_amount"]) > current_min:
            return tier
    return None


def calculate_commission_results(
    id_empresa: int,
    id_filial: int,
    month: int,
    year: int,
    payment_mode: str = "individual_sales",
) -> Dict[str, Any]:
    """Calculate commission results for a month/year using individual commission rules."""
    config = get_config(id_empresa, id_filial)
    if not config:
        return _empty_results(month, year, id_filial, reason="no_config")

    config_id = config["id"]
    groups = get_config_groups(config_id)
    tiers = get_config_tiers(config_id)

    if not groups:
        return _empty_results(month, year, id_filial, reason="no_groups")

    group_ids = [g["id_grupo_produto"] for g in groups]
    ano_mes = year * 100 + month

    # Query monthly sales by configured group and employee (only valid employees)
    placeholders = ",".join(["%s"] * len(group_ids))
    sql = f"""
      SELECT
        id_funcionario,
        nome_vendedor,
        id_grupo_produto,
        nome_grupo_produto,
        SUM(venda_total) AS venda_total,
        SUM(quantidade_vendas) AS quantidade_vendas
      FROM mart.commission_sales_monthly
      WHERE id_empresa = %s
        AND id_filial = %s
        AND ano_mes = %s
        AND id_funcionario > 0
        AND id_grupo_produto IN ({placeholders})
      GROUP BY id_funcionario, nome_vendedor, id_grupo_produto, nome_grupo_produto
    """
    params = [id_empresa, id_filial, ano_mes] + group_ids

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    # Manager base: all sales except combustiveis, independent of employee linkage.
    manager_sql = """
      SELECT COALESCE(SUM(i.total), 0)::numeric(18,2) AS venda_total_sem_combustiveis
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
        AND EXTRACT(YEAR FROM v.data)::integer = %s
        AND EXTRACT(MONTH FROM v.data)::integer = %s
        AND COALESCE(v.cancelado, false) = false
        AND COALESCE(i.cfop, 0) >= 5000
        AND COALESCE(UPPER(g.nome), '') NOT LIKE 'COMBUST%%'
    """

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        manager_row = conn.execute(manager_sql, [id_empresa, id_filial, year, month]).fetchone()

    manager_sales = float((manager_row or {}).get("venda_total_sem_combustiveis") or 0)
    manager_tier = _determine_tier(manager_sales, tiers)
    manager_mode = str(config.get("manager_commission_mode") or "use_tiers")
    manager_fixed_percent = float(config.get("manager_commission_percent") or 0)
    if manager_mode == "fixed_percent":
        manager_percent = manager_fixed_percent
    else:
        manager_percent = float(manager_tier["commission_percent"]) if manager_tier else 0.0
    manager_commission_gross = round(manager_sales * manager_percent / 100, 2)

    if not rows:
        return _empty_results(
            month,
            year,
            id_filial,
            reason="no_sales",
            groups=groups,
            tiers=tiers,
            manager_data={
                "venda_total_sem_combustiveis": round(manager_sales, 2),
                "nivel_atingido": {
                    "tier_key": manager_tier["tier_key"],
                    "tier_name": manager_tier["tier_name"],
                    "min_sales_amount": float(manager_tier["min_sales_amount"]),
                } if manager_tier else None,
                "percentual_aplicado": manager_percent,
                "modo_comissao": manager_mode,
                "percentual_configurado": manager_fixed_percent,
                "comissao_bruta": manager_commission_gross,
            },
        )

    # Per-employee aggregation
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
        employees[emp_id]["quantidade_vendas"] += int(r["quantidade_vendas"] or 0)

    # Individual commission: each employee gets tier/percent based on own sales.
    commission_total = 0.0
    for emp in employees.values():
        emp_tier = _determine_tier(float(emp["venda_elegivel"]), tiers)
        emp_percent = float(emp_tier["commission_percent"]) if emp_tier else 0.0
        emp_commission = round(float(emp["venda_elegivel"]) * emp_percent / 100, 2)
        commission_total += emp_commission

        emp["nivel_atingido"] = {
            "tier_key": emp_tier["tier_key"],
            "tier_name": emp_tier["tier_name"],
            "min_sales_amount": float(emp_tier["min_sales_amount"]),
        } if emp_tier else None
        emp["percentual_aplicado"] = emp_percent
        emp["comissao_estimada"] = emp_commission

    # Sort by venda_elegivel DESC
    employee_list = sorted(employees.values(), key=lambda e: e["venda_elegivel"], reverse=True)

    total_eligible = sum(float(e["venda_elegivel"] or 0) for e in employee_list)

    # Group summary
    group_totals = {}
    for r in rows:
        gid = int(r["id_grupo_produto"])
        if gid not in group_totals:
            group_totals[gid] = {"id_grupo_produto": gid, "nome": r["nome_grupo_produto"], "venda_total": 0}
        group_totals[gid]["venda_total"] += float(r["venda_total"] or 0)

    return {
        "month": month,
        "year": year,
        "id_filial": id_filial,
        "payment_mode": "individual_sales",
        "venda_elegivel": round(total_eligible, 2),
        "nivel_atingido": None,
        "percentual_aplicado": None,
        "comissao_total": round(commission_total, 2),
        "proximo_nivel": None,
        "vendedores": employee_list,
        "vendedores_elegiveis": len(employee_list),
        "grupos_configurados": list(group_totals.values()),
        "tier_progress": [],
        "gerente": {
            "venda_total_sem_combustiveis": round(manager_sales, 2),
            "nivel_atingido": {
                "tier_key": manager_tier["tier_key"],
                "tier_name": manager_tier["tier_name"],
                "min_sales_amount": float(manager_tier["min_sales_amount"]),
            } if manager_tier else None,
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
        },
    }


def _empty_results(
    month: int,
    year: int,
    id_filial: int,
    reason: str = "no_config",
    groups: Optional[List] = None,
    tiers: Optional[List] = None,
    manager_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return empty structure with appropriate message."""
    messages = {
        "no_config": "Nenhuma configuração de comissão encontrada para esta filial.",
        "no_groups": "Nenhum grupo configurado para comissão nesta filial. Acesse Configuração para selecionar os grupos participantes.",
        "no_sales": "Não há vendas elegíveis para o mês selecionado.",
    }
    tier_progress = []
    if tiers:
        for t in sorted(tiers, key=lambda x: x["sort_order"]):
            if t.get("is_active", True):
                tier_progress.append({
                    "tier_key": t["tier_key"],
                    "tier_name": t["tier_name"],
                    "min_sales_amount": float(t["min_sales_amount"]),
                    "commission_percent": float(t["commission_percent"]),
                    "achieved": False,
                })

    return {
        "month": month,
        "year": year,
        "id_filial": id_filial,
        "payment_mode": "individual_sales",
        "venda_elegivel": 0,
        "nivel_atingido": None,
        "percentual_aplicado": None,
        "comissao_total": 0,
        "proximo_nivel": None,
        "vendedores": [],
        "vendedores_elegiveis": 0,
        "grupos_configurados": [],
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
