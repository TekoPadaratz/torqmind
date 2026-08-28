"""Gestão de Produtos — estoque parado (leitura ClickHouse)."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from app.db_clickhouse import insert_batch, query_dict

MART_STOCK = "torqmind_mart_rt.product_stock_idle"
MART_PURCHASE = "torqmind_mart_rt.product_purchase_recent"
CURRENT_DB = "torqmind_current"


def _filial_filter_sql(
    id_filial: Optional[int],
    id_filiais: Optional[List[int]],
    params: Dict[str, Any],
    *,
    column: str = "s.id_filial",
) -> str:
    if id_filial is not None:
        if not isinstance(id_filial, int):
            raise ValueError("id_filial deve ser inteiro")
        params["id_filial"] = int(id_filial)
        return f"AND {column} = {{id_filial:Int32}}"
    if id_filiais:
        ids = [int(x) for x in id_filiais if int(x) > 0]
        if ids:
            params["id_filiais"] = ids
            return f"AND {column} IN {{id_filiais:Array(Int32)}}"
    return ""


def list_product_stock_idle(
    id_empresa: int,
    id_filial: Optional[int] = None,
    id_filiais: Optional[List[int]] = None,
    min_dias_sem_venda: int = 7,
    setor: Optional[str] = None,
    limit: int = 2000,
    offset: int = 0,
) -> Dict[str, Any]:
    min_dias = max(0, int(min_dias_sem_venda or 0))
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "min_dias": min_dias,
        "limit": max(1, min(int(limit or 2000), 5000)),
        "offset": max(0, int(offset or 0)),
    }
    filial_filter = _filial_filter_sql(id_filial, id_filiais, params, column="s.id_filial")

    setor_filter = ""
    if setor:
        params["setor"] = str(setor).strip().lower()
        setor_filter = "AND s.setor_gerencial = {setor:String}"

    count_row = query_dict(
        f"""
        SELECT count() AS total
        FROM {MART_STOCK} AS s
        WHERE s.id_empresa = {{id_empresa:Int32}}
          {filial_filter}
          AND s.dias_sem_venda >= {{min_dias:Int32}}
          {setor_filter}
        """,
        parameters=params,
    )
    total = int((count_row[0] or {}).get("total") or 0)

    rows = query_dict(
        f"""
        SELECT
          s.id_filial,
          coalesce(f.nome, concat('Filial ', toString(s.id_filial))) AS filial_label,
          s.id_produto,
          s.nome_produto,
          s.setor_gerencial,
          s.qtd_estoque,
          s.last_sale_date,
          s.dias_sem_venda,
          s.custo_medio_compra,
          s.preco_venda
        FROM {MART_STOCK} AS s
        LEFT JOIN (
          SELECT id_empresa, id_filial, any(nome) AS nome
          FROM {CURRENT_DB}.dim_filial
          WHERE id_empresa = {{id_empresa:Int32}}
          GROUP BY id_empresa, id_filial
        ) AS f ON f.id_empresa = s.id_empresa AND f.id_filial = s.id_filial
        WHERE s.id_empresa = {{id_empresa:Int32}}
          {filial_filter}
          AND s.dias_sem_venda >= {{min_dias:Int32}}
          {setor_filter}
        ORDER BY s.id_filial ASC, s.dias_sem_venda DESC, s.nome_produto ASC, s.id_produto ASC
        LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}
        """,
        parameters=params,
    )

    produtos: List[Dict[str, Any]] = []
    for r in rows or []:
        qtd = float(r.get("qtd_estoque") or 0)
        custo_u = float(r.get("custo_medio_compra") or 0)
        preco = float(r.get("preco_venda") or 0)
        setor_key = str(r.get("setor_gerencial") or "outros")
        produtos.append(
            {
                "id_filial": int(r.get("id_filial") or 0),
                "filial_label": str(r.get("filial_label") or ""),
                "id_produto": int(r.get("id_produto") or 0),
                "nome_produto": str(r.get("nome_produto") or ""),
                "setor": setor_key,
                "setor_label": _setor_label(setor_key),
                "qtd_estoque": round(qtd, 3),
                "dias_sem_venda": int(r.get("dias_sem_venda") or 0),
                "last_sale_date": _fmt_date(r.get("last_sale_date")),
                "custo_medio": round(custo_u, 4),
                "custo_medio_total": round(qtd * custo_u, 2),
                "preco_venda": round(preco, 4),
                "receita_total": round(qtd * preco, 2),
            }
        )

    setores_rows = query_dict(
        f"""
        SELECT DISTINCT s.setor_gerencial
        FROM {MART_STOCK} AS s
        WHERE s.id_empresa = {{id_empresa:Int32}}
          {filial_filter}
          AND s.dias_sem_venda >= {{min_dias:Int32}}
        ORDER BY s.setor_gerencial ASC
        """,
        parameters=params,
    )
    setores = [
        {
            "key": str(r.get("setor_gerencial") or "outros"),
            "label": _setor_label(str(r.get("setor_gerencial") or "outros")),
        }
        for r in setores_rows or []
    ]

    return {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "min_dias_sem_venda": min_dias,
        "total": total,
        "produtos": produtos,
        "setores": setores,
    }


def list_product_purchases_recent(
    id_empresa: int,
    id_filial: int,
    id_produto: int,
) -> Dict[str, Any]:
    rows = query_dict(
        f"""
        SELECT
          rank,
          numero_documento,
          data_compra,
          qtd,
          valor_unitario,
          valor_total
        FROM {MART_PURCHASE}
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_filial = {{id_filial:Int32}}
          AND id_produto = {{id_produto:Int32}}
        ORDER BY rank ASC
        """,
        parameters={
            "id_empresa": int(id_empresa),
            "id_filial": int(id_filial),
            "id_produto": int(id_produto),
        },
    )
    compras = []
    for r in rows or []:
        compras.append(
            {
                "rank": int(r.get("rank") or 0),
                "numero_documento": str(r.get("numero_documento") or "—"),
                "data_compra": _fmt_date(r.get("data_compra")),
                "qtd": float(r.get("qtd") or 0),
                "valor_unitario": float(r.get("valor_unitario") or 0),
                "valor_total": float(r.get("valor_total") or 0),
            }
        )
    return {
        "id_filial": int(id_filial),
        "id_produto": int(id_produto),
        "compras": compras,
    }


def _setor_label(key: str) -> str:
    mapping = {
        "combustivel": "Combustível",
        "conveniencia": "Conveniência",
        "automotivo": "Automotivo",
        "cigarro": "Cigarro",
        "servico": "Serviço",
        "outros": "Outros",
    }
    return mapping.get(key, key.replace("_", " ").capitalize())


def _fmt_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def refresh_and_publish_product_stock_idle(role: str, id_empresa: int) -> Dict[str, Any]:
    """Refresh PG mart + publish to ClickHouse."""
    from app.db import get_conn

    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        conn.execute("SET statement_timeout = '600s'")
        row = conn.execute(
            "SELECT etl.refresh_product_stock_idle(%s) AS rows",
            [int(id_empresa)],
        ).fetchone()
    refreshed = int((row or {}).get("rows") or 0)

    pub = datetime.now(timezone.utc)
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        stock_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT id_empresa, id_filial, id_produto, nome_produto, setor_gerencial,
                       qtd_estoque, last_sale_date, dias_sem_venda,
                       custo_medio_compra, preco_venda
                FROM mart.product_stock_idle
                WHERE id_empresa = %s
                """,
                [int(id_empresa)],
            ).fetchall()
        ]
        purchase_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT id_empresa, id_filial, id_produto, rank,
                       numero_documento, data_compra, qtd, valor_unitario, valor_total
                FROM mart.product_purchase_recent
                WHERE id_empresa = %s
                """,
                [int(id_empresa)],
            ).fetchall()
        ]

    stock_ch = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_produto": int(r["id_produto"]),
            "nome_produto": str(r.get("nome_produto") or ""),
            "setor_gerencial": str(r.get("setor_gerencial") or "outros"),
            "qtd_estoque": float(r.get("qtd_estoque") or 0),
            "last_sale_date": r.get("last_sale_date"),
            "dias_sem_venda": int(r.get("dias_sem_venda") or 0),
            "custo_medio_compra": float(r.get("custo_medio_compra") or 0),
            "preco_venda": float(r.get("preco_venda") or 0),
            "published_at": pub,
        }
        for r in stock_rows
    ]
    purchase_ch = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_produto": int(r["id_produto"]),
            "rank": int(r["rank"]),
            "numero_documento": str(r.get("numero_documento") or "—"),
            "data_compra": r.get("data_compra"),
            "qtd": float(r.get("qtd") or 0),
            "valor_unitario": float(r.get("valor_unitario") or 0),
            "valor_total": float(r.get("valor_total") or 0),
            "published_at": pub,
        }
        for r in purchase_rows
    ]

    n_stock = insert_batch(
        MART_STOCK,
        stock_ch,
        order_by=["id_empresa", "id_filial", "setor_gerencial", "nome_produto", "id_produto"],
    )
    n_purchase = insert_batch(
        MART_PURCHASE,
        purchase_ch,
        order_by=["id_empresa", "id_filial", "id_produto", "rank"],
    )
    return {
        "refreshed_rows": refreshed,
        "published_stock": n_stock,
        "published_purchases": n_purchase,
    }
