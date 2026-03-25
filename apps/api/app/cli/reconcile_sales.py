from __future__ import annotations

import argparse
import json
import unicodedata
from datetime import date
from decimal import Decimal
from typing import Any

from app import repos_mart
from app.db import get_conn


NORMALIZE_SQL = (
    "TRANSLATE(UPPER(COALESCE(NULLIF({expr}, ''), '')), "
    "'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC')"
)


def _normalize_group_name(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_only = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(ascii_only.upper().split())


def _legacy_group_bucket(raw_group: str | None) -> str:
    normalized = _normalize_group_name(raw_group)
    if any(token in normalized for token in ("GASOL", "ETANOL", "DIESEL", "GNV", "COMBUST")):
        return "COMBUSTIVEIS"
    if any(token in normalized for token in ("TROCA", "LAVAG", "DUCHA", "SERV", "OFIC")):
        return "SERVICOS"
    if any(token in normalized for token in ("CONVENI", "BEBID", "ALIMENT", "SALG", "CHOC", "TABAC", "CIGARR", "LOJA", "MERCE")):
        return "CONVENIENCIA"
    return normalized or "(SEM GRUPO)"


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def _fetch_source_total(conn, tenant_id: int, data_key: int, branch_id: int | None, group_normalized: str) -> dict[str, Any]:
    group_expr = NORMALIZE_SQL.format(expr="COALESCE(g.payload->>'GRUPOPRODUTOS', g.payload->>'DESCRICAO', '(Sem grupo)')")
    branch_sql = "AND i.id_filial = %s" if branch_id is not None else ""
    params: list[Any] = [tenant_id, data_key]
    if branch_id is not None:
        params.append(branch_id)
    params.append(group_normalized)
    row = conn.execute(
        f"""
        WITH source_items AS (
          SELECT
            COALESCE(i.total_shadow, etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2), 0)::numeric(18,2) AS total_item,
            COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'), i.id_movprodutos) AS doc_ref
          FROM stg.itensmovprodutos i
          JOIN stg.movprodutos m
            ON m.id_empresa = i.id_empresa
           AND m.id_filial = i.id_filial
           AND m.id_db = i.id_db
           AND m.id_movprodutos = i.id_movprodutos
          LEFT JOIN stg.grupoprodutos g
            ON g.id_empresa = i.id_empresa
           AND g.id_filial = i.id_filial
           AND g.id_grupoprodutos = COALESCE(i.id_grupo_produto_shadow, etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS'))
          LEFT JOIN stg.comprovantes c
            ON c.id_empresa = m.id_empresa
           AND c.id_filial = m.id_filial
           AND c.id_db = m.id_db
           AND c.id_comprovante = COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'))
          WHERE i.id_empresa = %s
            AND COALESCE(
              etl.date_key(COALESCE(i.dt_evento, m.dt_evento, etl.sales_business_ts(m.payload, m.dt_evento))),
              0
            ) = %s
            {branch_sql}
            AND COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) >= 5000
            AND COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) = false
            AND {group_expr} = %s
        )
        SELECT
          COALESCE(SUM(total_item), 0)::numeric(18,2) AS total,
          COUNT(*)::int AS items,
          COUNT(DISTINCT doc_ref)::int AS comprovantes
        FROM source_items
        """,
        params,
    ).fetchone()
    return dict(row or {})


def _fetch_dw_total(conn, tenant_id: int, data_key: int, branch_id: int | None, group_normalized: str) -> dict[str, Any]:
    group_expr = NORMALIZE_SQL.format(expr="COALESCE(g.nome, '(Sem grupo)')")
    branch_sql = "AND v.id_filial = %s" if branch_id is not None else ""
    params: list[Any] = [tenant_id, data_key]
    if branch_id is not None:
        params.append(branch_id)
    params.append(group_normalized)
    row = conn.execute(
        f"""
        SELECT
          COALESCE(SUM(i.total), 0)::numeric(18,2) AS total,
          COUNT(*)::int AS items,
          COUNT(DISTINCT COALESCE(v.id_comprovante, v.id_movprodutos))::int AS comprovantes
        FROM dw.fact_venda v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa
         AND i.id_filial = v.id_filial
         AND i.id_db = v.id_db
         AND i.id_movprodutos = v.id_movprodutos
        LEFT JOIN dw.dim_produto p
          ON p.id_empresa = i.id_empresa
         AND p.id_filial = i.id_filial
         AND p.id_produto = i.id_produto
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = i.id_empresa
         AND g.id_filial = i.id_filial
         AND g.id_grupo_produto = COALESCE(i.id_grupo_produto, p.id_grupo_produto)
        WHERE v.id_empresa = %s
          AND v.data_key = %s
          {branch_sql}
          AND COALESCE(v.cancelado, false) = false
          AND COALESCE(i.cfop, 0) >= 5000
          AND {group_expr} = %s
        """,
        params,
    ).fetchone()
    return dict(row or {})


def _fetch_mart_groups(conn, tenant_id: int, data_key: int, branch_id: int | None) -> list[dict[str, Any]]:
    branch_sql = "AND id_filial = %s" if branch_id is not None else ""
    params: list[Any] = [tenant_id, data_key]
    if branch_id is not None:
        params.append(branch_id)
    rows = conn.execute(
        f"""
        SELECT
          id_filial,
          grupo_nome,
          SUM(faturamento)::numeric(18,2) AS faturamento
        FROM mart.agg_grupos_diaria
        WHERE id_empresa = %s
          AND data_key = %s
          {branch_sql}
        GROUP BY id_filial, grupo_nome
        ORDER BY faturamento DESC, grupo_nome
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_top_extra_items(
    conn,
    tenant_id: int,
    data_key: int,
    branch_id: int | None,
    extra_groups: list[str],
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    if not extra_groups:
        return {"items": [], "comprovantes": []}

    branch_sql = "AND v.id_filial = %s" if branch_id is not None else ""
    params_items: list[Any] = [tenant_id, data_key]
    if branch_id is not None:
        params_items.append(branch_id)
    params_items.extend([extra_groups, limit])
    items = conn.execute(
        f"""
        SELECT
          COALESCE(v.id_comprovante, v.id_movprodutos) AS comprovante_ref,
          v.id_movprodutos,
          i.id_itensmovprodutos,
          COALESCE(g.nome, '(Sem grupo)') AS grupo_nome,
          COALESCE(NULLIF(p.nome, ''), '#ID ' || i.id_produto::text) AS produto_nome,
          i.total::numeric(18,2) AS total_item
        FROM dw.fact_venda v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa
         AND i.id_filial = v.id_filial
         AND i.id_db = v.id_db
         AND i.id_movprodutos = v.id_movprodutos
        LEFT JOIN dw.dim_produto p
          ON p.id_empresa = i.id_empresa
         AND p.id_filial = i.id_filial
         AND p.id_produto = i.id_produto
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = i.id_empresa
         AND g.id_filial = i.id_filial
         AND g.id_grupo_produto = COALESCE(i.id_grupo_produto, p.id_grupo_produto)
        WHERE v.id_empresa = %s
          AND v.data_key = %s
          {branch_sql}
          AND COALESCE(v.cancelado, false) = false
          AND COALESCE(i.cfop, 0) >= 5000
          AND COALESCE(g.nome, '(Sem grupo)') = ANY(%s)
        ORDER BY i.total DESC, comprovante_ref DESC
        LIMIT %s
        """,
        params_items,
    ).fetchall()

    params_docs: list[Any] = [tenant_id, data_key]
    if branch_id is not None:
        params_docs.append(branch_id)
    params_docs.extend([extra_groups, limit])
    comprovantes = conn.execute(
        f"""
        SELECT
          COALESCE(v.id_comprovante, v.id_movprodutos) AS comprovante_ref,
          COALESCE(g.nome, '(Sem grupo)') AS grupo_nome,
          SUM(i.total)::numeric(18,2) AS total_extra
        FROM dw.fact_venda v
        JOIN dw.fact_venda_item i
          ON i.id_empresa = v.id_empresa
         AND i.id_filial = v.id_filial
         AND i.id_db = v.id_db
         AND i.id_movprodutos = v.id_movprodutos
        LEFT JOIN dw.dim_produto p
          ON p.id_empresa = i.id_empresa
         AND p.id_filial = i.id_filial
         AND p.id_produto = i.id_produto
        LEFT JOIN dw.dim_grupo_produto g
          ON g.id_empresa = i.id_empresa
         AND g.id_filial = i.id_filial
         AND g.id_grupo_produto = COALESCE(i.id_grupo_produto, p.id_grupo_produto)
        WHERE v.id_empresa = %s
          AND v.data_key = %s
          {branch_sql}
          AND COALESCE(v.cancelado, false) = false
          AND COALESCE(i.cfop, 0) >= 5000
          AND COALESCE(g.nome, '(Sem grupo)') = ANY(%s)
        GROUP BY comprovante_ref, grupo_nome
        ORDER BY total_extra DESC, comprovante_ref DESC
        LIMIT %s
        """,
        params_docs,
    ).fetchall()
    return {"items": [dict(row) for row in items], "comprovantes": [dict(row) for row in comprovantes]}


def reconcile_sales(
    tenant_id: int,
    target_date: date,
    branch_id: int | None = None,
    group: str = "COMBUSTIVEIS",
    detail_limit: int = 10,
) -> dict[str, Any]:
    data_key = int(target_date.strftime("%Y%m%d"))
    group_normalized = _normalize_group_name(group)

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute("SET max_parallel_workers_per_gather = 0")
        source_total = _fetch_source_total(conn, tenant_id, data_key, branch_id, group_normalized)
        dw_total = _fetch_dw_total(conn, tenant_id, data_key, branch_id, group_normalized)
        mart_rows = _fetch_mart_groups(conn, tenant_id, data_key, branch_id)

    mart_exact_total = 0.0
    matched_raw_groups: list[dict[str, Any]] = []
    legacy_bucket_total = 0.0
    legacy_extra_groups: list[dict[str, Any]] = []
    legacy_bucket = _legacy_group_bucket(group)
    for row in mart_rows:
        raw_group = row.get("grupo_nome")
        normalized_raw = _normalize_group_name(raw_group)
        row_total = float(row.get("faturamento") or 0)
        if normalized_raw == group_normalized:
            mart_exact_total += row_total
            matched_raw_groups.append({"grupo_nome": raw_group, "faturamento": row_total})
        if _legacy_group_bucket(raw_group) == legacy_bucket:
            legacy_bucket_total += row_total
            if normalized_raw != group_normalized:
                legacy_extra_groups.append({"grupo_nome": raw_group, "faturamento": row_total})

    endpoint_rows = repos_mart.sales_top_groups("MASTER", tenant_id, branch_id, target_date, target_date, limit=500)
    endpoint_total = 0.0
    for row in endpoint_rows:
        if _normalize_group_name(row.get("grupo_nome")) == group_normalized:
            endpoint_total += float(row.get("faturamento") or 0)

    extra_group_names = [str(item["grupo_nome"]) for item in legacy_extra_groups]
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute("SET max_parallel_workers_per_gather = 0")
        extra_details = _fetch_top_extra_items(
            conn,
            tenant_id,
            data_key,
            branch_id,
            extra_group_names,
            detail_limit,
        )

    totals = {
        "source_operational": float(source_total.get("total") or 0),
        "dw": float(dw_total.get("total") or 0),
        "mart": float(mart_exact_total),
        "endpoint": float(endpoint_total),
        "legacy_bucket": float(legacy_bucket_total),
    }
    deltas = {
        "source_vs_dw": round(totals["source_operational"] - totals["dw"], 2),
        "dw_vs_mart": round(totals["dw"] - totals["mart"], 2),
        "mart_vs_endpoint": round(totals["mart"] - totals["endpoint"], 2),
        "legacy_bucket_extra": round(totals["legacy_bucket"] - totals["mart"], 2),
    }

    return {
        "date": target_date,
        "data_key": data_key,
        "tenant_id": tenant_id,
        "branch_id": branch_id,
        "group_requested": group,
        "group_normalized": group_normalized,
        "matched_raw_groups": matched_raw_groups,
        "totals": totals,
        "deltas": deltas,
        "counts": {
            "source_items": int(source_total.get("items") or 0),
            "dw_items": int(dw_total.get("items") or 0),
            "source_comprovantes": int(source_total.get("comprovantes") or 0),
            "dw_comprovantes": int(dw_total.get("comprovantes") or 0),
        },
        "legacy_bucket": {
            "bucket_name": legacy_bucket,
            "total": totals["legacy_bucket"],
            "extra_groups": legacy_extra_groups,
            "extra_items": extra_details["items"],
            "extra_comprovantes": extra_details["comprovantes"],
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile sales totals across source capture, DW, mart and endpoint semantics.")
    parser.add_argument("--tenant-id", type=int, required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--branch-id", type=int, default=None)
    parser.add_argument("--group", default="COMBUSTIVEIS")
    parser.add_argument("--detail-limit", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = reconcile_sales(
        tenant_id=args.tenant_id,
        target_date=args.date,
        branch_id=args.branch_id,
        group=args.group,
        detail_limit=args.detail_limit,
    )
    print(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
