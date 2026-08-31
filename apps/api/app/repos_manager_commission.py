"""Manager commission (LSC) — config, CH totals, period overrides."""
from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Set

from app.db import get_conn
from app.db_clickhouse import query_dict

logger = logging.getLogger(__name__)

CURRENT_DB = "torqmind_current"
MART_RT_DB = "torqmind_mart_rt"

# Grupos excluídos por padrão da base de venda (spec LSC).
SALES_BASE_EXCLUDED_GROUP_IDS: Set[int] = {1, 2, 3, 4, 7, 8, 9, 10, 16, 39, 40}

# Fora da base de venda do gerente (= Vendas): perda 5927 + transferências.
SALES_EXCLUDED_CFOPS: tuple[int, ...] = (5927, 5929, 6929)
STOCK_LOSS_CFOP = 5927

DEFAULT_RATE_PCT = 2.0


def _cfop_sales_predicate_sql(alias: str = "i") -> str:
    """Vendas comerciais: cfop > 5000, sem 5927/5929/6929."""
    from app.sales_semantics import sales_cfop_filter_sql

    return sales_cfop_filter_sql(alias)


def _slim_comercial_where_sql(comprovante_alias: str = "c") -> str:
    """Mesmos filtros comerciais da mart ``sales_groups_rt``."""
    from app.sales_semantics import central_mirror_exclude_sql

    mirror = central_mirror_exclude_sql(comprovante_alias)
    return (
        f"i.is_deleted = 0 "
        f"AND {comprovante_alias}.is_deleted = 0 "
        f"AND {comprovante_alias}.commercial_eligible = 1 "
        f"AND {mirror}"
    )


def _slim_loss_where_sql(comprovante_alias: str = "c") -> str:
    """Filtro mínimo para notas de perda (5927) — paridade Xpert LSC."""
    return (
        f"i.is_deleted = 0 "
        f"AND {comprovante_alias}.is_deleted = 0 "
        f"AND {comprovante_alias}.cancelado = 0 "
        f"AND {comprovante_alias}.situacao != 3"
    )


def _sales_group_id_sql(item_alias: str = "i", prod_alias: str = "p") -> str:
    """Grupo: cadastro do produto primeiro (igual ``sales_groups_rt`` / Top grupos)."""
    return (
        f"coalesce(nullIf({prod_alias}.id_grupo_produto, 0), "
        f"nullIf({item_alias}.id_grupo_produto, 0), 0)"
    )


def _loss_group_id_sql(item_alias: str = "i", prod_alias: str = "p") -> str:
    """Grupo do item com fallback no cadastro (paridade slim)."""
    return (
        f"if({item_alias}.id_grupo_produto > 0, {item_alias}.id_grupo_produto, "
        f"coalesce({prod_alias}.id_grupo_produto, 0))"
    )


def _nfe_documento(numero: Any) -> str:
    raw = str(numero or "").strip()
    if not raw or raw in {"0", "None"}:
        return "—"
    return raw


def _date_key_iso(data_key: Any) -> str:
    digits = str(int(data_key or 0)).zfill(8)
    if len(digits) != 8 or digits == "00000000":
        return ""
    return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"


def _slim_item_from_sql() -> str:
    return f"""
        FROM {CURRENT_DB}.stg_itenscomprovantes_slim AS i FINAL
        INNER JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
          ON c.id_empresa = i.id_empresa
         AND c.id_filial = i.id_filial
         AND c.id_db = i.id_db
         AND c.id_comprovante = i.id_comprovante
        LEFT JOIN {CURRENT_DB}.dim_produto AS p FINAL
          ON p.id_empresa = i.id_empresa
         AND p.id_filial = i.id_filial
         AND p.id_produto = i.id_produto
    """


def _conn_branch_id(id_filial: Optional[int]) -> Optional[int]:
    return id_filial if id_filial and id_filial > 0 else None


def _ano_mes(year: int, month: int) -> int:
    return int(year) * 100 + int(month)


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last = monthrange(int(year), int(month))[1]
    return date(int(year), int(month), 1), date(int(year), int(month), last)


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def net_commission(
    *,
    comissao_bruta: float,
    perdas_estoque: float,
    sobras_estoque: float,
    furos_caixa: float,
    sobras_caixa: float,
) -> float:
    """Comissão Líquida = bruta - perdas + sobras_est - furos + sobras_cx."""
    return round(
        float(comissao_bruta)
        - float(perdas_estoque)
        + float(sobras_estoque)
        - float(furos_caixa)
        + float(sobras_caixa),
        2,
    )


def get_rule_config(id_empresa: int, id_filial: int) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT id, id_empresa, id_filial, default_rate_pct, is_active, created_at, updated_at
      FROM app.manager_commission_rule_config
      WHERE id_empresa = %s AND id_filial = %s AND is_active = true
      LIMIT 1
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, [id_empresa, id_filial]).fetchone()
    return dict(row) if row else None


def get_rule_groups(config_id: int, rule_kind: Optional[str] = None) -> List[Dict[str, Any]]:
    sql = """
      SELECT id, config_id, rule_kind, id_grupo_produto, nome_grupo, is_active
      FROM app.manager_commission_rule_group
      WHERE config_id = %s AND is_active = true
    """
    params: List[Any] = [config_id]
    if rule_kind:
        sql += " AND rule_kind = %s"
        params.append(rule_kind)
    sql += " ORDER BY rule_kind, nome_grupo NULLS LAST, id_grupo_produto"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def list_product_groups(id_empresa: int, id_filial: int) -> List[Dict[str, Any]]:
    """Grupos da filial — ClickHouse dim primeiro; PG dw só fallback."""
    try:
        ch_rows = query_dict(
            f"""
            SELECT id_grupo_produto, argMax(nome, source_ts_ms) AS nome
            FROM {CURRENT_DB}.dim_grupo_produto FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND id_filial = {{id_filial:Int32}}
              AND is_deleted = 0
            GROUP BY id_grupo_produto
            ORDER BY nome, id_grupo_produto
            """,
            parameters={"id_empresa": int(id_empresa), "id_filial": int(id_filial)},
        )
        if ch_rows:
            return [
                {
                    "id_grupo_produto": int(r["id_grupo_produto"]),
                    "nome": str(r.get("nome") or f"Grupo {r['id_grupo_produto']}"),
                }
                for r in ch_rows
                if r.get("id_grupo_produto") is not None
            ]
    except Exception as exc:
        logger.warning("list_product_groups CH miss: %s", str(exc)[:180])

    sql = """
      SELECT id_grupo_produto, nome
      FROM dw.dim_grupo_produto
      WHERE id_empresa = %s AND id_filial = %s
      ORDER BY nome NULLS LAST, id_grupo_produto
    """
    try:
        with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
            rows = [dict(r) for r in conn.execute(sql, [id_empresa, id_filial]).fetchall()]
        return [
            {
                "id_grupo_produto": int(r["id_grupo_produto"]),
                "nome": str(r.get("nome") or f"Grupo {r['id_grupo_produto']}"),
            }
            for r in rows
            if r.get("id_grupo_produto") is not None
        ]
    except Exception as exc:
        logger.warning("list_product_groups PG fallback miss: %s", str(exc)[:180])
        return []


def _default_selected(groups: List[Dict[str, Any]], rule_kind: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for g in groups:
        gid = int(g["id_grupo_produto"])
        nome = str(g.get("nome") or "")
        if rule_kind == "sales_base":
            selected = gid not in SALES_BASE_EXCLUDED_GROUP_IDS
        else:
            selected = nome.strip().upper() != "INSUMOS"
        out.append(
            {
                "id_grupo_produto": gid,
                "nome": nome,
                "selected": selected,
            }
        )
    return out


def ensure_rule_config(id_empresa: int, id_filial: int) -> Dict[str, Any]:
    existing = get_rule_config(id_empresa, id_filial)
    if existing:
        return existing

    groups = list_product_groups(id_empresa, id_filial)
    sales_sel = [g for g in _default_selected(groups, "sales_base") if g["selected"]]
    loss_sel = [g for g in _default_selected(groups, "stock_loss") if g["selected"]]

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(
            """
            INSERT INTO app.manager_commission_rule_config
              (id_empresa, id_filial, default_rate_pct, is_active)
            VALUES (%s, %s, %s, true)
            RETURNING id, id_empresa, id_filial, default_rate_pct, is_active, created_at, updated_at
            """,
            [id_empresa, id_filial, DEFAULT_RATE_PCT],
        ).fetchone()
        config = dict(row)
        config_id = int(config["id"])
        for kind, selected in (("sales_base", sales_sel), ("stock_loss", loss_sel)):
            for g in selected:
                conn.execute(
                    """
                    INSERT INTO app.manager_commission_rule_group
                      (config_id, rule_kind, id_grupo_produto, nome_grupo, is_active)
                    VALUES (%s, %s, %s, %s, true)
                    """,
                    [config_id, kind, g["id_grupo_produto"], g.get("nome")],
                )
        conn.commit()
    return config


def save_rule_config(
    id_empresa: int,
    id_filial: int,
    *,
    default_rate_pct: float,
    sales_groups: Sequence[Dict[str, Any]],
    stock_loss_groups: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    config = ensure_rule_config(id_empresa, id_filial)
    config_id = int(config["id"])
    rate = float(default_rate_pct)
    if rate < 0 or rate > 100:
        raise ValueError("default_rate_pct must be between 0 and 100")

    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        conn.execute(
            """
            UPDATE app.manager_commission_rule_config
            SET default_rate_pct = %s, updated_at = now()
            WHERE id = %s
            """,
            [rate, config_id],
        )
        conn.execute(
            "UPDATE app.manager_commission_rule_group SET is_active = false WHERE config_id = %s",
            [config_id],
        )
        for kind, groups in (("sales_base", sales_groups), ("stock_loss", stock_loss_groups)):
            for g in groups:
                gid = int(g.get("id_grupo_produto") or 0)
                if gid <= 0:
                    continue
                nome = g.get("nome") or g.get("nome_grupo")
                conn.execute(
                    """
                    INSERT INTO app.manager_commission_rule_group
                      (config_id, rule_kind, id_grupo_produto, nome_grupo, is_active)
                    VALUES (%s, %s, %s, %s, true)
                    """,
                    [config_id, kind, gid, nome],
                )
        conn.commit()
    refreshed = get_rule_config(id_empresa, id_filial)
    assert refreshed
    # Após mudar grupos, materializa o mês corrente na mart CH (não no GET).
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        publish_manager_commission_month(id_empresa, id_filial, now.year, now.month)
    except Exception as exc:
        logger.warning("publish after save_rule_config failed: %s", str(exc)[:180])
    return refreshed


def get_period_override(
    id_empresa: int, id_filial: int, dt_ini: date, dt_fim: date
) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT id_empresa, id_filial, dt_ini, dt_fim, year, month, rate_pct,
             perdas_estoque, sobras_estoque, sobras_caixa, furos_caixa,
             updated_at, updated_by
      FROM app.manager_commission_period_override
      WHERE id_empresa = %s AND id_filial = %s AND dt_ini = %s AND dt_fim = %s
      LIMIT 1
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, [id_empresa, id_filial, dt_ini, dt_fim]).fetchone()
    return dict(row) if row else None


def upsert_period_override(
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    fields: Dict[str, Any],
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    allowed = (
        "rate_pct",
        "perdas_estoque",
        "sobras_estoque",
        "sobras_caixa",
        "furos_caixa",
    )
    payload = {k: fields.get(k) for k in allowed if k in fields}
    if not payload:
        existing = get_period_override(id_empresa, id_filial, dt_ini, dt_fim)
        return existing or {}

    year = int(dt_fim.year)
    month = int(dt_fim.month)
    cols = [
        "id_empresa",
        "id_filial",
        "dt_ini",
        "dt_fim",
        "year",
        "month",
    ] + list(payload.keys()) + ["updated_at", "updated_by"]
    placeholders = ["%s", "%s", "%s", "%s", "%s", "%s"] + ["%s"] * len(payload) + ["now()", "%s"]
    value_params = [
        id_empresa,
        id_filial,
        dt_ini,
        dt_fim,
        year,
        month,
    ] + [payload[k] for k in payload] + [updated_by]

    set_clause = ", ".join(f"{k} = EXCLUDED.{k}" for k in payload.keys())
    set_clause += ", updated_at = now(), updated_by = EXCLUDED.updated_by, year = EXCLUDED.year, month = EXCLUDED.month"

    sql = f"""
      INSERT INTO app.manager_commission_period_override
        ({", ".join(cols)})
      VALUES ({", ".join(placeholders)})
      ON CONFLICT (id_empresa, id_filial, dt_ini, dt_fim) DO UPDATE
        SET {set_clause}
      RETURNING id_empresa, id_filial, dt_ini, dt_fim, year, month, rate_pct,
                perdas_estoque, sobras_estoque, sobras_caixa, furos_caixa,
                updated_at, updated_by
    """
    with get_conn(tenant_id=id_empresa, branch_id=_conn_branch_id(id_filial)) as conn:
        row = conn.execute(sql, value_params).fetchone()
        conn.commit()
    return dict(row) if row else {}


def _sum_metric_from_slim(
    *,
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    group_ids: Sequence[int],
    cfops: Optional[Sequence[int]] = None,
    cfop_exclude: Optional[Sequence[int]] = None,
) -> float:
    ini, fim = _date_key(dt_ini), _date_key(dt_fim)
    if cfop_exclude is not None:
        # Base de venda: espelha tela de Vendas + exclui transferência.
        scope_sql = _slim_comercial_where_sql("c")
        group_list = ", ".join(str(int(g)) for g in group_ids if int(g) > 0)
        if not group_list or not cfop_exclude:
            return 0.0
        cfop_pred = _cfop_sales_predicate_sql("i")
        group_expr = _sales_group_id_sql("i", "p")
        group_filter = f"AND {group_expr} IN ({group_list})"
    else:
        scope_sql = _slim_loss_where_sql("c")
        include = ", ".join(str(int(c)) for c in (cfops or ()))
        if not include:
            return 0.0
        cfop_pred = f"i.cfop IN ({include})"
        # Xpert LSC: nota de perda 5927 entra pelo valor integral do comprovante.
        group_filter = ""
    rows = query_dict(
        f"""
        SELECT sum(i.total) AS valor
        {_slim_item_from_sql()}
        WHERE i.id_empresa = {{id_empresa:Int32}}
          AND i.id_filial = {{id_filial:Int32}}
          AND i.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND {scope_sql}
          AND {cfop_pred}
          {group_filter}
        """,
        parameters={
            "id_empresa": int(id_empresa),
            "id_filial": int(id_filial),
            "ini": ini,
            "fim": fim,
        },
    )
    if not rows:
        return 0.0
    return round(float(rows[0].get("valor") or 0), 2)


def _sales_groups_breakdown(
    *,
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    sales_groups: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Totais por grupo da base de venda (mesmos filtros do KPI)."""
    configured = []
    seen: Set[int] = set()
    for g in sales_groups:
        gid = int(g.get("id_grupo_produto") or 0)
        if gid <= 0 or gid in seen:
            continue
        seen.add(gid)
        configured.append(
            {
                "id_grupo_produto": gid,
                "nome": str(g.get("nome_grupo") or g.get("nome") or f"Grupo {gid}"),
                "valor": 0.0,
            }
        )
    if not configured:
        return []
    group_list = ", ".join(str(g["id_grupo_produto"]) for g in configured)
    group_expr = _sales_group_id_sql("i", "p")
    comercial = _slim_comercial_where_sql("c")
    rows = query_dict(
        f"""
        SELECT
          {group_expr} AS id_grupo_produto,
          round(sum(i.total), 2) AS valor
        {_slim_item_from_sql()}
        WHERE i.id_empresa = {{id_empresa:Int32}}
          AND i.id_filial = {{id_filial:Int32}}
          AND i.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND {comercial}
          AND {_cfop_sales_predicate_sql("i")}
          AND {group_expr} IN ({group_list})
        GROUP BY id_grupo_produto
        """,
        parameters={
            "id_empresa": int(id_empresa),
            "id_filial": int(id_filial),
            "ini": _date_key(dt_ini),
            "fim": _date_key(dt_fim),
        },
    )
    by_id = {int(r["id_grupo_produto"]): round(float(r.get("valor") or 0), 2) for r in rows}
    for item in configured:
        item["valor"] = by_id.get(item["id_grupo_produto"], 0.0)
    configured = [g for g in configured if abs(float(g.get("valor") or 0)) > 0.009]
    configured.sort(key=lambda r: (str(r["nome"] or "").casefold(), int(r["id_grupo_produto"])))
    return configured


def _loss_notes_breakdown(
    *,
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    loss_groups: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Notas de perda (CFOP 5927) — valor integral do comprovante (paridade Xpert LSC)."""
    _ = loss_groups  # config permanece na UI; cálculo segue todas as notas 5927 do período.
    loss_scope = _slim_loss_where_sql("c")
    rows = query_dict(
        f"""
        SELECT
          i.id_filial AS id_filial,
          i.id_db AS id_db,
          i.id_comprovante AS id_comprovante,
          min(i.data_key) AS data_key,
          round(sum(i.total), 2) AS valor,
          any(n.numero_nfe_res) AS numero_nfe,
          any(n.data_emissao_res) AS data_emissao
        {_slim_item_from_sql()}
        LEFT JOIN (
          SELECT
            id_empresa, id_filial, id_db, id_comprovante,
            argMax(numero_nfe, source_ts_ms) AS numero_nfe_res,
            argMax(data_emissao, source_ts_ms) AS data_emissao_res
          FROM {CURRENT_DB}.stg_nfe_slim
          WHERE id_empresa = {{id_empresa:Int32}}
            AND id_filial = {{id_filial:Int32}}
            AND is_deleted = 0
            AND status != 5
            AND numero_nfe != ''
            AND numero_nfe != '0'
          GROUP BY id_empresa, id_filial, id_db, id_comprovante
        ) AS n
          ON n.id_empresa = i.id_empresa
         AND n.id_filial = i.id_filial
         AND n.id_db = i.id_db
         AND n.id_comprovante = i.id_comprovante
        WHERE i.id_empresa = {{id_empresa:Int32}}
          AND i.id_filial = {{id_filial:Int32}}
          AND i.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
          AND {loss_scope}
          AND i.cfop = {int(STOCK_LOSS_CFOP)}
        GROUP BY i.id_filial, i.id_db, i.id_comprovante
        HAVING abs(valor) > 0.009
        ORDER BY data_key DESC, numero_nfe ASC, i.id_comprovante ASC
        LIMIT 500
        """,
        parameters={
            "id_empresa": int(id_empresa),
            "id_filial": int(id_filial),
            "ini": _date_key(dt_ini),
            "fim": _date_key(dt_fim),
        },
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        data_key = int(r.get("data_key") or 0)
        emissao = r.get("data_emissao")
        data_iso = ""
        if emissao is not None:
            try:
                data_iso = emissao.date().isoformat() if hasattr(emissao, "date") else str(emissao)[:10]
            except Exception:
                data_iso = ""
        if not data_iso:
            data_iso = _date_key_iso(data_key)
        out.append(
            {
                "id_filial": int(r.get("id_filial") or id_filial),
                "id_comprovante": int(r.get("id_comprovante") or 0),
                "data": data_iso,
                "data_key": data_key,
                "documento": _nfe_documento(r.get("numero_nfe")),
                "valor": round(float(r.get("valor") or 0), 2),
            }
        )
    return out


def calc_branch_drilldown(
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    *,
    filial_label: Optional[str] = None,
) -> Dict[str, Any]:
    """Detalhe operacional da linha de comissão de gerente (grupos + notas de perda)."""
    summary = calc_branch_row(
        id_empresa, id_filial, dt_ini, dt_fim, filial_label=filial_label, publish=False
    )
    config = ensure_rule_config(id_empresa, id_filial)
    config_id = int(config["id"])
    sales_groups = get_rule_groups(config_id, "sales_base")
    loss_groups = get_rule_groups(config_id, "stock_loss")
    groups = _sales_groups_breakdown(
        id_empresa=id_empresa,
        id_filial=id_filial,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        sales_groups=sales_groups,
    )
    notes = _loss_notes_breakdown(
        id_empresa=id_empresa,
        id_filial=id_filial,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        loss_groups=loss_groups,
    )
    grupos_total = round(sum(float(g["valor"]) for g in groups), 2)
    notas_total = round(sum(float(n["valor"]) for n in notes), 2)
    perdas_tela = round(float(summary.get("perdas_estoque") or 0), 2)
    return {
        "id_empresa": id_empresa,
        "id_filial": id_filial,
        "filial_label": summary.get("filial_label") or filial_label or f"Filial {id_filial}",
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "venda_bruta_total": round(float(summary.get("venda_bruta_total") or 0), 2),
        "grupos": groups,
        "grupos_total": grupos_total,
        "perdas_notas": notes,
        "perdas_notas_total": notas_total,
        "perdas_estoque": perdas_tela,
        "perdas_divergente": abs(perdas_tela - notas_total) > 0.009,
    }


def publish_manager_commission_month(
    id_empresa: int, id_filial: int, year: int, month: int
) -> Dict[str, Any]:
    """Materializa totais por grupo no CH mart_rt (sales_base + stock_loss)."""
    from app.db_clickhouse import insert_batch

    dt_ini, dt_fim = _month_bounds(year, month)
    ini = _date_key(dt_ini)
    fim = _date_key(dt_fim)
    ano_mes = _ano_mes(year, month)

    specs = (
        ("sales_base", None, SALES_EXCLUDED_CFOPS),
        ("stock_loss", (STOCK_LOSS_CFOP,), None),
    )
    inserted = 0
    for metric_kind, cfops_include, cfops_exclude in specs:
        if cfops_exclude is not None:
            cfop_pred = _cfop_sales_predicate_sql("i")
            group_expr = _sales_group_id_sql("i", "p")
            scope_sql = _slim_comercial_where_sql("c")
        else:
            cfop_list = ", ".join(str(int(c)) for c in (cfops_include or ()))
            cfop_pred = f"i.cfop IN ({cfop_list})"
            group_expr = _loss_group_id_sql("i", "p")
            scope_sql = _slim_loss_where_sql("c")
        rows = query_dict(
            f"""
            SELECT
              i.id_empresa AS id_empresa,
              i.id_filial AS id_filial,
              {group_expr} AS id_grupo_produto,
              sum(i.total) AS valor,
              count() AS qtd_itens
            {_slim_item_from_sql()}
            WHERE i.id_empresa = {{id_empresa:Int32}}
              AND i.id_filial = {{id_filial:Int32}}
              AND i.data_key BETWEEN {{ini:Int32}} AND {{fim:Int32}}
              AND {scope_sql}
              AND {cfop_pred}
            GROUP BY i.id_empresa, i.id_filial, id_grupo_produto
            HAVING id_grupo_produto > 0
            """,
            parameters={
                "id_empresa": int(id_empresa),
                "id_filial": int(id_filial),
                "ini": ini,
                "fim": fim,
            },
        )
        if not rows:
            continue
        batch = [
            {
                "id_empresa": int(r["id_empresa"]),
                "id_filial": int(r["id_filial"]),
                "ano_mes": ano_mes,
                "id_grupo_produto": int(r["id_grupo_produto"]),
                "metric_kind": metric_kind,
                "valor": float(r.get("valor") or 0),
                "qtd_itens": int(r.get("qtd_itens") or 0),
            }
            for r in rows
        ]
        insert_batch(
            f"{MART_RT_DB}.manager_commission_group_month_rt",
            batch,
            order_by=["id_empresa", "id_filial", "ano_mes", "metric_kind", "id_grupo_produto"],
        )
        inserted += len(batch)
    return {"ok": True, "inserted": inserted, "ano_mes": ano_mes}


def _sum_from_mart(
    *,
    id_empresa: int,
    id_filial: int,
    year: int,
    month: int,
    group_ids: Sequence[int],
    metric_kind: str,
) -> Optional[float]:
    if not group_ids:
        return 0.0
    group_list = ", ".join(str(int(g)) for g in group_ids if int(g) > 0)
    if not group_list:
        return 0.0
    try:
        rows = query_dict(
            f"""
            SELECT sum(valor) AS valor, count() AS n
            FROM {MART_RT_DB}.manager_commission_group_month_rt FINAL
            WHERE id_empresa = {{id_empresa:Int32}}
              AND id_filial = {{id_filial:Int32}}
              AND ano_mes = {{ano_mes:Int32}}
              AND metric_kind = {{metric_kind:String}}
              AND id_grupo_produto IN ({group_list})
            """,
            parameters={
                "id_empresa": int(id_empresa),
                "id_filial": int(id_filial),
                "ano_mes": _ano_mes(year, month),
                "metric_kind": metric_kind,
            },
        )
        # SUM sobre zero linhas no CH devolve 1 row com valor NULL/0 — tratar como miss
        # para cair no slim até o publish popular a mart.
        if not rows or int(rows[0].get("n") or 0) <= 0:
            return None
        return round(float(rows[0].get("valor") or 0), 2)
    except Exception as exc:
        logger.warning("manager_commission mart miss: %s", str(exc)[:180])
        return None


def calc_branch_row(
    id_empresa: int,
    id_filial: int,
    dt_ini: date,
    dt_fim: date,
    *,
    filial_label: Optional[str] = None,
    publish: bool = False,
) -> Dict[str, Any]:
    """Calcula comissão de gerente para uma filial.

    Hot path: sempre lê ClickHouse slim (mesma fonte viva dos vendedores), para
    refletir regra de CFOP na hora. ``publish=True`` ainda materializa a mart
    (ETL / refresh explícito) sem ser a fonte da resposta.
    """
    config = ensure_rule_config(id_empresa, id_filial)
    config_id = int(config["id"])
    sales_groups = get_rule_groups(config_id, "sales_base")
    loss_groups = get_rule_groups(config_id, "stock_loss")
    sales_ids = [int(g["id_grupo_produto"]) for g in sales_groups]
    loss_ids = [int(g["id_grupo_produto"]) for g in loss_groups]

    if publish:
        try:
            publish_manager_commission_month(id_empresa, id_filial, dt_fim.year, dt_fim.month)
        except Exception as exc:
            logger.warning("publish_manager_commission_month failed: %s", str(exc)[:200])

    # Sempre slim no GET: evita mart stale após mudança de regra (ex.: CFOP).
    venda = _sum_metric_from_slim(
        id_empresa=id_empresa,
        id_filial=id_filial,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        group_ids=sales_ids,
        cfop_exclude=SALES_EXCLUDED_CFOPS,
    )

    perdas_default = _sum_metric_from_slim(
        id_empresa=id_empresa,
        id_filial=id_filial,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        group_ids=loss_ids,
        cfops=(STOCK_LOSS_CFOP,),
    )

    # Furos/sobras de caixa: sem fonte STG (TURNOS sem campos) — default 0.
    sobras_estoque_default = 0.0
    sobras_caixa_default = 0.0
    furos_caixa_default = 0.0

    rate_default = float(config.get("default_rate_pct") or DEFAULT_RATE_PCT)
    override = get_period_override(id_empresa, id_filial, dt_ini, dt_fim) or {}

    def _coalesce_num(key: str, default: float) -> float:
        if key in override and override.get(key) is not None:
            return float(override[key])
        return float(default)

    rate = _coalesce_num("rate_pct", rate_default)
    perdas = _coalesce_num("perdas_estoque", perdas_default)
    sobras_est = _coalesce_num("sobras_estoque", sobras_estoque_default)
    sobras_cx = _coalesce_num("sobras_caixa", sobras_caixa_default)
    furos = _coalesce_num("furos_caixa", furos_caixa_default)

    bruta = round(float(venda) * float(rate) / 100.0, 2)
    liquida = net_commission(
        comissao_bruta=bruta,
        perdas_estoque=perdas,
        sobras_estoque=sobras_est,
        furos_caixa=furos,
        sobras_caixa=sobras_cx,
    )

    return {
        "id_empresa": id_empresa,
        "id_filial": id_filial,
        "filial_label": filial_label or f"Filial {id_filial}",
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "venda_bruta_total": round(float(venda), 2),
        "rate_pct": rate,
        "rate_pct_default": rate_default,
        "comissao_bruta": bruta,
        "perdas_estoque": perdas,
        "perdas_estoque_default": round(float(perdas_default), 2),
        "sobras_estoque": sobras_est,
        "sobras_estoque_default": sobras_estoque_default,
        "sobras_caixa": sobras_cx,
        "sobras_caixa_default": sobras_caixa_default,
        "furos_caixa": furos,
        "furos_caixa_default": furos_caixa_default,
        "comissao_liquida": liquida,
        "cash_source": "unavailable_pending_agent_dataset",
        "has_override": bool(override),
        "groups_sales_count": len(sales_ids),
        "groups_loss_count": len(loss_ids),
    }


def build_config_response(id_empresa: int, id_filial: int) -> Dict[str, Any]:
    config = ensure_rule_config(id_empresa, id_filial)
    config_id = int(config["id"])
    all_groups = list_product_groups(id_empresa, id_filial)
    sales_sel = {int(g["id_grupo_produto"]) for g in get_rule_groups(config_id, "sales_base")}
    loss_sel = {int(g["id_grupo_produto"]) for g in get_rule_groups(config_id, "stock_loss")}

    sales_groups = [
        {
            "id_grupo_produto": g["id_grupo_produto"],
            "nome": g["nome"],
            "selected": g["id_grupo_produto"] in sales_sel,
        }
        for g in all_groups
    ]
    loss_groups = [
        {
            "id_grupo_produto": g["id_grupo_produto"],
            "nome": g["nome"],
            "selected": g["id_grupo_produto"] in loss_sel,
        }
        for g in all_groups
    ]
    return {
        "config": {
            "id": config_id,
            "id_empresa": id_empresa,
            "id_filial": id_filial,
            "default_rate_pct": float(config.get("default_rate_pct") or DEFAULT_RATE_PCT),
        },
        "sales_base_groups": sales_groups,
        "stock_loss_groups": loss_groups,
        "sales_base_excluded_defaults": sorted(SALES_BASE_EXCLUDED_GROUP_IDS),
        "cash_adj_note": (
            "Furos e sobras de caixa não estão no STG (TURNOS sem colunas). "
            "Campos editáveis no grid; dataset agent pendente."
        ),
    }
