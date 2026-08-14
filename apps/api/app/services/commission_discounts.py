"""Descontos em vendas e preços fixos para a área de comissões (somente leitura CH).

Não altera a fórmula de comissão. Grãos separados:
- ``venda``: VLRDESCONTO do item (stg_itenscomprovantes_slim.desconto)
- ``preco_fixo``: desconto econômico da mart de preço fixo
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from app.db_clickhouse import query_dict
from app.filial_apelido import apelido_for

CURRENT_DB = "torqmind_current"
MART_RT = "torqmind_mart_rt"


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last = monthrange(int(year), int(month))[1]
    return date(int(year), int(month), 1), date(int(year), int(month), last)


def _branch_sql(
    id_filial: Optional[int],
    id_filiais: Optional[Sequence[int]],
    col: str = "id_filial",
) -> tuple[str, Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if id_filial:
        params["id_filial"] = int(id_filial)
        return f"AND {col} = {{id_filial:Int32}}", params
    if id_filiais:
        ids = sorted({int(x) for x in id_filiais if int(x) > 0})
        if ids:
            params["id_filiais"] = ids
            return f"AND {col} IN {{id_filiais:Array(Int32)}}", params
    return "", params


def _label(fid: int) -> str:
    return apelido_for(fid) or f"Filial {fid}"


def commission_discounts_overview(
    id_empresa: int,
    year: int,
    month: int,
    *,
    id_filial: Optional[int] = None,
    id_filiais: Optional[Sequence[int]] = None,
    limit: int = 200,
) -> Dict[str, Any]:
    """Lista unificada (tipo explícito) sem misturar comissão. Sem custo/margem."""
    dt_ini, dt_fim = _month_bounds(year, month)
    limit = max(1, min(int(limit), 500))
    branch_sql, branch_params = _branch_sql(id_filial, id_filiais, "i.id_filial")
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "limit": limit,
        **branch_params,
    }

    venda_rows = query_dict(
        f"""
        SELECT
          i.id_filial AS id_filial,
          toDate(c.dt_evento_local) AS dt_venda,
          ifNull(toString(n.numero_nfe), '') AS documento,
          ifNull(
            nullIf(trimBoth(JSONExtractString(e.payload, 'NOMEENTIDADE')), ''),
            nullIf(trimBoth(JSONExtractString(e.payload, 'RAZAOSOCIALENTIDADE')), '')
          ) AS cliente,
          ifNull(
            nullIf(trimBoth(JSONExtractString(f.payload, 'NOMEFUNCIONARIO')), ''),
            ''
          ) AS vendedor,
          ifNull(
            nullIf(trimBoth(JSONExtractString(p.payload, 'DESCRICAO')), ''),
            nullIf(trimBoth(JSONExtractString(p.payload, 'NOMEPRODUTO')), '')
          ) AS produto,
          if(i.qtd > 0, toFloat64(i.total) / toFloat64(i.qtd), 0.0) AS preco_aplicado,
          toFloat64(i.desconto) AS desconto_rs,
          if(
            toFloat64(i.total) + toFloat64(i.desconto) > 0,
            round(toFloat64(i.desconto) / (toFloat64(i.total) + toFloat64(i.desconto)) * 100, 2),
            0.0
          ) AS desconto_pct
        FROM {CURRENT_DB}.stg_itenscomprovantes_slim AS i FINAL
        INNER JOIN {CURRENT_DB}.stg_comprovantes_slim AS c FINAL
          ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
         AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
         AND c.is_deleted = 0
        LEFT JOIN {CURRENT_DB}.stg_nfe_slim AS n FINAL
          ON n.id_empresa = c.id_empresa AND n.id_filial = c.id_filial
         AND n.id_db = c.id_db AND n.id_comprovante = c.id_comprovante
         AND n.is_deleted = 0
        LEFT JOIN {CURRENT_DB}.stg_entidades AS e FINAL
          ON e.id_empresa = c.id_empresa AND e.id_filial = c.id_filial
         AND e.id_entidade = c.id_cliente AND e.is_deleted = 0
        LEFT JOIN {CURRENT_DB}.stg_funcionarios AS f FINAL
          ON f.id_empresa = i.id_empresa AND f.id_filial = i.id_filial
         AND f.id_funcionario = i.id_funcionario AND f.is_deleted = 0
        LEFT JOIN {CURRENT_DB}.stg_produtos AS p FINAL
          ON p.id_empresa = i.id_empresa AND p.id_filial = i.id_filial
         AND p.id_produto = i.id_produto AND p.is_deleted = 0
        WHERE i.id_empresa = {{id_empresa:Int32}}
          {branch_sql}
          AND i.is_deleted = 0
          AND i.desconto > 0
          AND toDate(c.dt_evento_local) >= toDate({{dt_ini:String}})
          AND toDate(c.dt_evento_local) <= toDate({{dt_fim:String}})
          AND ifNull(c.situacao, 0) != 3
        ORDER BY i.id_filial ASC, dt_venda DESC, documento ASC
        LIMIT {{limit:UInt32}}
        """,
        parameters=params,
        tenant_id=id_empresa,
    )

    branch_sql2, branch_params2 = _branch_sql(id_filial, id_filiais, "id_filial")
    params2 = {
        "id_empresa": int(id_empresa),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "limit": limit,
        **branch_params2,
    }
    fixo_rows = query_dict(
        f"""
        SELECT
          id_filial,
          dt_venda,
          documento_label AS documento,
          cliente_nome AS cliente,
          produto_nome AS produto,
          toFloat64(preco_bomba) AS preco_referencia,
          toFloat64(preco_pago) AS preco_aplicado,
          toFloat64(desconto_total) AS desconto_rs,
          if(
            toFloat64(preco_bomba) > 0,
            round((toFloat64(preco_bomba) - toFloat64(preco_pago)) / toFloat64(preco_bomba) * 100, 2),
            0.0
          ) AS desconto_pct
        FROM {MART_RT}.mart_cliente_preco_fixo_item FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch_sql2}
          AND dt_venda >= toDate({{dt_ini:String}})
          AND dt_venda <= toDate({{dt_fim:String}})
          AND desconto_total > 0
        ORDER BY id_filial ASC, dt_venda DESC, documento ASC
        LIMIT {{limit:UInt32}}
        """,
        parameters=params2,
        tenant_id=id_empresa,
    )

    items: List[Dict[str, Any]] = []
    for r in venda_rows:
        fid = int(r.get("id_filial") or 0)
        items.append(
            {
                "tipo": "venda",
                "tipo_label": "Desconto na venda",
                "id_filial": fid,
                "filial_label": _label(fid) if fid else "—",
                "dt_venda": str(r.get("dt_venda") or ""),
                "documento": str(r.get("documento") or "").strip() or "—",
                "cliente": str(r.get("cliente") or "—") or "—",
                "vendedor": str(r.get("vendedor") or "—") or "—",
                "produto": str(r.get("produto") or "—") or "—",
                "preco_referencia": None,
                "preco_aplicado": float(r.get("preco_aplicado") or 0),
                "desconto_rs": float(r.get("desconto_rs") or 0),
                "desconto_pct": float(r.get("desconto_pct") or 0),
            }
        )
    for r in fixo_rows:
        fid = int(r.get("id_filial") or 0)
        items.append(
            {
                "tipo": "preco_fixo",
                "tipo_label": "Preço fixo",
                "id_filial": fid,
                "filial_label": _label(fid) if fid else "—",
                "dt_venda": str(r.get("dt_venda") or ""),
                "documento": str(r.get("documento") or "").strip() or "—",
                "cliente": str(r.get("cliente") or "—") or "—",
                "vendedor": "—",
                "produto": str(r.get("produto") or "—") or "—",
                "preco_referencia": float(r.get("preco_referencia") or 0),
                "preco_aplicado": float(r.get("preco_aplicado") or 0),
                "desconto_rs": float(r.get("desconto_rs") or 0),
                "desconto_pct": float(r.get("desconto_pct") or 0),
            }
        )

    items.sort(
        key=lambda x: (
            str(x.get("filial_label") or "").casefold(),
            str(x.get("dt_venda") or ""),
            str(x.get("documento") or "").casefold(),
        )
    )
    # Filial ASC, Data DESC, Documento ASC
    from collections import defaultdict

    by_fil: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for it in items:
        by_fil[str(it.get("filial_label") or "")].append(it)
    ordered: List[Dict[str, Any]] = []
    for fil in sorted(by_fil.keys(), key=lambda s: s.casefold()):
        chunk = by_fil[fil]
        chunk.sort(
            key=lambda x: (str(x.get("dt_venda") or ""), str(x.get("documento") or "").casefold()),
            reverse=True,
        )
        # documento ASC within same date
        chunk.sort(
            key=lambda x: (
                # negate date via reverse groups
                str(x.get("dt_venda") or ""),
            ),
            reverse=True,
        )
        # stable secondary ASC on documento
        from itertools import groupby as gb

        for _, g in gb(chunk, key=lambda x: str(x.get("dt_venda") or "")):
            ordered.extend(
                sorted(list(g), key=lambda x: str(x.get("documento") or "").casefold())
            )

    return {
        "ano": int(year),
        "mes": int(month),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "items": ordered[:limit],
        "total": len(ordered),
        "source": "clickhouse",
    }
