"""Commission configuration and calculation repository."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from app.db import get_conn

logger = logging.getLogger(__name__)

# Default tiers when no configuration exists
DEFAULT_TIERS = [
    {"tier_key": "bronze", "tier_name": "Bronze", "min_sales_amount": 30000, "commission_percent": 0.5, "sort_order": 1, "is_active": True},
    {"tier_key": "silver", "tier_name": "Prata", "min_sales_amount": 50000, "commission_percent": 1.0, "sort_order": 2, "is_active": True},
    {"tier_key": "gold", "tier_name": "Ouro", "min_sales_amount": 80000, "commission_percent": 1.5, "sort_order": 3, "is_active": True},
    {"tier_key": "diamond", "tier_name": "Diamante", "min_sales_amount": 120000, "commission_percent": 2.0, "sort_order": 4, "is_active": True},
]

# Power BI / LSC allowlist — saídas comerciais (itenscomprovantes / fact_venda_item)
COMMISSION_ELIGIBLE_CFOPS: tuple[int, ...] = (5102, 5405, 5656, 5667, 5929)


def _conn_branch_id(id_filial: Optional[int]) -> Optional[int]:
    return id_filial if id_filial and id_filial > 0 else None


def _cfop_in_sql() -> str:
    return ",".join(str(int(c)) for c in COMMISSION_ELIGIBLE_CFOPS)


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
          AND COALESCE(i.cfop, 0) IN ({_cfop_in_sql()})
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
    """List products of a group for commission drill-down."""
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
            VALUES (%s, %s, 'Comissao padrao', true, 'team_total', 'use_tiers', 0)
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


def save_config(
    id_empresa: int,
    id_filial: int,
    groups: List[Dict[str, Any]],
    tiers: List[Dict[str, Any]],
    default_payment_mode: str = "team_total",
    manager_commission_mode: str = "use_tiers",
    manager_commission_percent: float = 0.0,
    excluded_products: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Save/update commission configuration atomically."""
    config = ensure_default_config(id_empresa, id_filial)
    config_id = config["id"]
    excluded = list(excluded_products or [])

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        conn.execute("""
          UPDATE app.commission_config
          SET default_payment_mode = %s,
              manager_commission_mode = %s,
              manager_commission_percent = %s,
              updated_at = now()
          WHERE id = %s
        """, [default_payment_mode, manager_commission_mode, manager_commission_percent, config_id])

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
    month: int,
    year: int,
    payment_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Calculate employee commissions for one or many branches.

    Each seller row keeps its own ``id_filial`` / ``filial_label`` (config is
    still per-branch). Rows are ordered Filial ASC → Nome ASC.
    """
    targets = sorted({int(f) for f in id_filiais if int(f) > 0})
    if not targets:
        return _empty_results(month, year, 0, reason="no_config")

    labels = _filial_labels(id_empresa, targets)
    valid_modes = ("team_total", "equal_split", "individual_sales")
    explicit_mode = payment_mode if payment_mode in valid_modes else None

    all_sellers: List[Dict[str, Any]] = []
    all_groups: List[Dict[str, Any]] = []
    comissao_total = 0.0
    venda_elegivel = 0.0
    branch_results: List[Dict[str, Any]] = []

    for fid in targets:
        mode = explicit_mode
        if mode is None:
            cfg = get_config(id_empresa, fid)
            mode = str((cfg or {}).get("default_payment_mode") or "team_total")
            if mode not in valid_modes:
                mode = "individual_sales"
        branch = calculate_commission_results(id_empresa, fid, month, year, mode)
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
        branch_results.append(branch)

    all_sellers.sort(
        key=lambda e: (
            str(e.get("filial_label") or "").casefold(),
            int(e.get("id_filial") or 0),
            str(e.get("nome_vendedor") or "").casefold(),
            int(e.get("id_funcionario") or 0),
        )
    )
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
        "month": month,
        "year": year,
        "id_filial": targets[0] if single else None,
        "id_filiais": targets,
        "multi_filial": not single,
        "payment_mode": explicit_mode or (primary.get("payment_mode") if single else "per_branch"),
        "venda_elegivel": round(venda_elegivel, 2),
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
    excludes = get_config_product_excludes(config_id)

    valid_modes = ("team_total", "equal_split", "individual_sales")
    config_default_mode = str(config.get("default_payment_mode") or "team_total")
    effective_mode = payment_mode if payment_mode in valid_modes else config_default_mode
    if effective_mode not in valid_modes:
        effective_mode = "individual_sales"

    if not groups:
        return _empty_results(month, year, id_filial, reason="no_groups", payment_mode=effective_mode)

    group_ids = [g["id_grupo_produto"] for g in groups]
    exclude_ids = [int(p["id_produto"]) for p in excludes if int(p.get("id_produto") or 0) > 0]
    placeholders = ",".join(["%s"] * len(group_ids))

    # Direct fact query so product exclusions apply before aggregation.
    sql = f"""
      SELECT
        COALESCE(i.id_funcionario, -1) AS id_funcionario,
        COALESCE(f.nome, '(Sem vendedor)') AS nome_vendedor,
        COALESCE(i.id_grupo_produto, -1) AS id_grupo_produto,
        COALESCE(g.nome, '(Sem grupo)') AS nome_grupo_produto,
        COALESCE(SUM(i.total), 0)::numeric(18,2) AS venda_total,
        COUNT(DISTINCT v.id_comprovante)::integer AS quantidade_vendas
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
      LEFT JOIN dw.dim_funcionario f
        ON f.id_empresa = i.id_empresa
       AND f.id_filial = i.id_filial
       AND f.id_funcionario = i.id_funcionario
      WHERE v.id_empresa = %s
        AND v.id_filial = %s
        AND EXTRACT(YEAR FROM v.data)::integer = %s
        AND EXTRACT(MONTH FROM v.data)::integer = %s
        AND COALESCE(v.cancelado, false) = false
        AND COALESCE(i.cfop, 0) IN ({_cfop_in_sql()})
        AND COALESCE(i.id_funcionario, 0) > 0
        AND i.id_grupo_produto IN ({placeholders})
    """
    params: List[Any] = [id_empresa, id_filial, year, month] + group_ids
    if exclude_ids:
        ex_ph = ",".join(["%s"] * len(exclude_ids))
        sql += f" AND COALESCE(i.id_produto, 0) NOT IN ({ex_ph})"
        params.extend(exclude_ids)
    sql += """
      GROUP BY
        COALESCE(i.id_funcionario, -1),
        COALESCE(f.nome, '(Sem vendedor)'),
        COALESCE(i.id_grupo_produto, -1),
        COALESCE(g.nome, '(Sem grupo)')
    """

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    # Manager base legado (tiers): all sales except combustiveis.
    manager_sql = f"""
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
        AND COALESCE(i.cfop, 0) IN ({_cfop_in_sql()})
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
            payment_mode=effective_mode,
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

    employee_list = sorted(employees.values(), key=lambda e: e["venda_elegivel"], reverse=True)
    total_eligible = sum(float(e["venda_elegivel"] or 0) for e in employee_list)
    n_eligible = sum(1 for e in employee_list if float(e["venda_elegivel"] or 0) > 0)

    team_tier_payload = None
    team_percent: Optional[float] = None
    next_tier_payload = None

    if effective_mode == "individual_sales":
        commission_total = 0.0
        for emp in employee_list:
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
        commission_total = round(commission_total, 2)
    else:
        team_tier = _determine_tier(total_eligible, tiers)
        team_percent = float(team_tier["commission_percent"]) if team_tier else 0.0
        commission_total = round(total_eligible * team_percent / 100, 2)
        team_tier_payload = {
            "tier_key": team_tier["tier_key"],
            "tier_name": team_tier["tier_name"],
            "min_sales_amount": float(team_tier["min_sales_amount"]),
        } if team_tier else None
        next_tier_raw = _next_tier(team_tier, tiers)
        next_tier_payload = {
            "tier_key": next_tier_raw["tier_key"],
            "tier_name": next_tier_raw["tier_name"],
            "min_sales_amount": float(next_tier_raw["min_sales_amount"]),
            "falta": round(max(0.0, float(next_tier_raw["min_sales_amount"]) - total_eligible), 2),
        } if next_tier_raw else None
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
    groups_out = list(group_totals.values())
    for g in groups_out:
        g["id_filial"] = id_filial
        g["filial_label"] = filial_label

    return {
        "month": month,
        "year": year,
        "id_filial": id_filial,
        "id_filiais": [id_filial],
        "multi_filial": False,
        "filial_label": filial_label,
        "payment_mode": effective_mode,
        "venda_elegivel": round(total_eligible, 2),
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
    payment_mode: str = "individual_sales",
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
    return {
        "month": month,
        "year": year,
        "id_filial": id_filial,
        "payment_mode": payment_mode,
        "venda_elegivel": 0,
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
