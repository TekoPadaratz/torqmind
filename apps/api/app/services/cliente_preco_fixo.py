"""Cliente com preço fixo (combustível): desconto econômico implícito.

Regra de negócio
----------------
No Xpert o preço fixo do cliente NÃO entra como VLRDESCONTO na NF — o item
já sai com VLRUNITARIO = preço negociado (ex.: bomba 6,52 / cliente 6,32).

Desconto econômico por item =
  (preço bomba ASOF do dia − VLRUNITARIO pago) × QTDE
somente quando:
  - cliente tem cadastro VALORFIXO=1 + ATIVO=1 para o produto;
  - produto é combustível (grupo COMBUST*);
  - venda não cancelada (situacao ≠ 3);
  - diferença unitária > R$ 0,005.

Fontes
------
- Cadastro: stg.descontos_entidades_itens ← dbo.DESCONTOSENTIDADESITENS
- Preço bomba: stg.preco_bomba_hist ← LMC/LMCBICOS (ASOF)
- Vendas: ClickHouse torqmind_current.stg_comprovantes + stg_itenscomprovantes
- Custo: i.custo_unitario_shadow (VLRCUSTO*) — null honesto se ausente
- Documento: **somente** número da NF-e/NFC-e (`stg_nfe_slim`). Sem NF → `—`.
  Proibido `NROCOMPROVANTE` / `id_comprovante` (ver `.cursor/rules/07-documento-nota-fiscal.mdc`).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from html import unescape
from itertools import groupby
from typing import Any, Dict, List, Optional, Sequence

from app.db import get_conn
from app.db_clickhouse import execute_command, insert_batch, query_dict
from app.filial_apelido import apelido_for

logger = logging.getLogger(__name__)

CURRENT_DB = "torqmind_current"
MART_DB = "torqmind_mart_rt"
DELTA_MIN = 0.005


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _branch_clause(
    id_filial: Optional[int],
    id_filiais: Optional[Sequence[int]],
    col: str = "id_filial",
) -> tuple[str, Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if id_filial is not None:
        params["id_filial"] = int(id_filial)
        return f"AND {col} = {{id_filial:Int32}}", params
    if id_filiais:
        ids = sorted({int(x) for x in id_filiais if int(x) > 0})
        if ids:
            params["id_filiais"] = ids
            return f"AND {col} IN {{id_filiais:Array(Int32)}}", params
    return "", params


def publish_cadastro(role: str, id_empresa: int) -> int:
    """Publica cadastro VALORFIXO ativo → CH mart (1 linha por entidade+produto+filial)."""
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (
              d.id_empresa,
              d.id_filial,
              COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0),
              COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0)
            )
              d.id_empresa,
              d.id_filial,
              COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0) AS id_entidade,
              COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto,
              COALESCE((d.payload->>'VALOR')::numeric, 0) AS valor_fixo,
              CASE
                WHEN COALESCE(d.payload->>'ATIVO','1') IN ('1','true','True','t') THEN 1
                ELSE 0
              END AS ativo
            FROM stg.descontos_entidades_itens d
            WHERE d.id_empresa = %s
              AND COALESCE(d.payload->>'VALORFIXO','0') IN ('1','true','True','t')
              AND COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0) > 0
              AND COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0) > 0
            ORDER BY
              d.id_empresa,
              d.id_filial,
              COALESCE(NULLIF(d.payload->>'ID_ENTIDADE','')::int, 0),
              COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0),
              CASE
                WHEN COALESCE(d.payload->>'ATIVO','1') IN ('1','true','True','t') THEN 0
                ELSE 1
              END,
              COALESCE((d.payload->>'VALOR')::numeric, 0) ASC,
              COALESCE(NULLIF(d.payload->>'ID_DESCONTOENTIDADESITENS','')::int, 0) DESC
            """,
            [id_empresa],
        ).fetchall()
    pub = _now()
    payload = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_entidade": int(r["id_entidade"]),
            "id_produto": int(r["id_produto"]),
            "valor_fixo": float(r["valor_fixo"] or 0),
            "ativo": int(r["ativo"] or 0),
            "published_at": pub,
        }
        for r in rows
        if int(r.get("ativo") or 0) == 1
    ]
    return insert_batch(
        f"{MART_DB}.mart_cliente_preco_fixo_cadastro",
        payload,
        order_by=["id_empresa", "id_filial", "id_entidade", "id_produto"],
    )


def publish_preco_bomba_dia(role: str, id_empresa: int, days: int = 400) -> int:
    """Expande ASOF diário do hist. de bomba só para produtos com preço fixo."""
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        rows = conn.execute(
            """
            WITH alvos AS (
              SELECT DISTINCT
                d.id_empresa,
                d.id_filial,
                COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0) AS id_produto
              FROM stg.descontos_entidades_itens d
              WHERE d.id_empresa = %s
                AND COALESCE(d.payload->>'VALORFIXO','0') IN ('1','true','True','t')
                AND COALESCE(d.payload->>'ATIVO','1') IN ('1','true','True','t')
                AND COALESCE(NULLIF(d.payload->>'ID_PRODUTOS','')::int, 0) > 0
            ),
            dias AS (
              SELECT generate_series(
                (CURRENT_DATE - (%s || ' days')::interval)::date,
                CURRENT_DATE,
                '1 day'::interval
              )::date AS dt
            ),
            hist AS (
              SELECT
                h.id_empresa,
                h.id_filial,
                h.id_produto,
                (h.dt_alteracao_shadow AT TIME ZONE 'America/Sao_Paulo')::date AS dt_alt,
                h.preco_venda_shadow AS preco
              FROM stg.preco_bomba_hist h
              INNER JOIN alvos a
                ON a.id_empresa = h.id_empresa
               AND a.id_filial = h.id_filial
               AND a.id_produto = h.id_produto
              WHERE h.id_empresa = %s
                AND h.preco_venda_shadow IS NOT NULL
                AND h.preco_venda_shadow > 0
            )
            SELECT DISTINCT ON (a.id_empresa, a.id_filial, a.id_produto, d.dt)
              a.id_empresa,
              a.id_filial,
              a.id_produto,
              d.dt,
              h.preco AS preco_venda
            FROM alvos a
            CROSS JOIN dias d
            INNER JOIN hist h
              ON h.id_empresa = a.id_empresa
             AND h.id_filial = a.id_filial
             AND h.id_produto = a.id_produto
             AND h.dt_alt <= d.dt
            ORDER BY a.id_empresa, a.id_filial, a.id_produto, d.dt, h.dt_alt DESC
            """,
            [id_empresa, int(days), id_empresa],
        ).fetchall()
    pub = _now()
    payload = [
        {
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_produto": int(r["id_produto"]),
            "dt": r["dt"],
            "preco_venda": float(r["preco_venda"] or 0),
            "published_at": pub,
        }
        for r in rows
        if r.get("dt") and float(r.get("preco_venda") or 0) > 0
    ]
    by_ym: Dict[str, List[Dict[str, Any]]] = {}
    for row in payload:
        dt = row["dt"]
        key = f"{dt.year:04d}{dt.month:02d}"
        by_ym.setdefault(key, []).append(row)
    total = 0
    for key in sorted(by_ym):
        total += insert_batch(
            f"{MART_DB}.mart_preco_bomba_dia",
            by_ym[key],
            order_by=["id_empresa", "id_filial", "id_produto", "dt"],
        )
    return total


def rebuild_itens(
    id_empresa: int,
    dt_ini: date,
    dt_fim: date,
    *,
    id_filial: Optional[int] = None,
    id_filiais: Optional[Sequence[int]] = None,
) -> int:
    """Materializa itens com desconto econômico no período (CH INSERT SELECT)."""
    branch_sql, branch_params = _branch_clause(id_filial, id_filiais, "c.id_filial")
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "delta_min": DELTA_MIN,
        **branch_params,
    }
    sql = f"""
    INSERT INTO {MART_DB}.mart_cliente_preco_fixo_item
    (
      id_empresa, id_filial, id_db, id_entidade, id_comprovante, id_itemcomprovante,
      id_produto, data_key, dt_venda, dt_evento, cliente_nome, produto_nome,
      documento_label, qtd, preco_bomba, preco_pago, desconto_unitario, desconto_total,
      custo_unitario, margem_unitaria_pct, margem_bomba_pct,
      published_at
    )
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      cad.id_entidade,
      c.id_comprovante,
      i.id_itemcomprovante,
      toInt32(i.id_produto_shadow) AS id_produto,
      toYYYYMMDD(toDate(c.dt_evento, 'America/Sao_Paulo')) AS data_key,
      toDate(c.dt_evento, 'America/Sao_Paulo') AS dt_venda,
      c.dt_evento,
      coalesce(
        nullIf(JSONExtractString(ent.payload, 'NOMEENTIDADE'), ''),
        nullIf(JSONExtractString(ent.payload, 'NOME'), ''),
        ''
      ) AS cliente_nome,
      coalesce(
        nullIf(JSONExtractString(p.payload, 'NOMEPRODUTO'), ''),
        ''
      ) AS produto_nome,
      coalesce(
        nullIf(nfe.numero_nfe_res, ''),
        '—'
      ) AS documento_label,
      toDecimal64(coalesce(i.qtd_shadow, 0), 3) AS qtd,
      toDecimal64(bomba.preco_venda, 4) AS preco_bomba,
      toDecimal64(coalesce(i.valor_unitario_shadow, 0), 4) AS preco_pago,
      toDecimal64(bomba.preco_venda - coalesce(i.valor_unitario_shadow, 0), 4) AS desconto_unitario,
      toDecimal64(
        (bomba.preco_venda - coalesce(i.valor_unitario_shadow, 0)) * coalesce(i.qtd_shadow, 0),
        2
      ) AS desconto_total,
      if(
        coalesce(i.custo_unitario_shadow, 0) > 0,
        toDecimal64(i.custo_unitario_shadow, 6),
        CAST(NULL, 'Nullable(Decimal(18,6))')
      ) AS custo_unitario,
      if(
        coalesce(i.custo_unitario_shadow, 0) > 0
        AND coalesce(i.valor_unitario_shadow, 0) > 0,
        toDecimal64(
          ((coalesce(i.valor_unitario_shadow, 0) - i.custo_unitario_shadow)
            / coalesce(i.valor_unitario_shadow, 0)) * 100,
          4
        ),
        CAST(NULL, 'Nullable(Decimal(18,4))')
      ) AS margem_unitaria_pct,
      if(
        coalesce(i.custo_unitario_shadow, 0) > 0
        AND bomba.preco_venda > 0,
        toDecimal64(
          ((bomba.preco_venda - i.custo_unitario_shadow) / bomba.preco_venda) * 100,
          4
        ),
        CAST(NULL, 'Nullable(Decimal(18,4))')
      ) AS margem_bomba_pct,
      now64(3) AS published_at
    FROM {CURRENT_DB}.stg_itenscomprovantes AS i FINAL
    INNER JOIN {CURRENT_DB}.stg_comprovantes AS c FINAL
      ON c.id_empresa = i.id_empresa
     AND c.id_filial = i.id_filial
     AND c.id_db = i.id_db
     AND c.id_comprovante = i.id_comprovante
    INNER JOIN {MART_DB}.mart_cliente_preco_fixo_cadastro AS cad FINAL
      ON cad.id_empresa = i.id_empresa
     AND cad.id_filial = i.id_filial
     AND cad.id_entidade = coalesce(c.id_cliente_shadow, 0)
     AND cad.id_produto = coalesce(i.id_produto_shadow, 0)
     AND cad.ativo = 1
    INNER JOIN {MART_DB}.mart_preco_bomba_dia AS bomba FINAL
      ON bomba.id_empresa = i.id_empresa
     AND bomba.id_filial = i.id_filial
     AND bomba.id_produto = coalesce(i.id_produto_shadow, 0)
     AND bomba.dt = toDate(c.dt_evento, 'America/Sao_Paulo')
    LEFT JOIN {CURRENT_DB}.stg_produtos AS p FINAL
      ON p.id_empresa = i.id_empresa
     AND p.id_filial = i.id_filial
     AND p.id_produto = coalesce(i.id_produto_shadow, 0)
    LEFT JOIN {CURRENT_DB}.stg_grupoprodutos AS g FINAL
      ON g.id_empresa = p.id_empresa
     AND g.id_filial = p.id_filial
     AND g.id_grupoprodutos = coalesce(
           i.id_grupo_produto_shadow,
           toInt32OrZero(JSONExtractString(p.payload, 'ID_GRUPOPRODUTOS'))
         )
    LEFT JOIN {CURRENT_DB}.stg_entidades AS ent FINAL
      ON ent.id_empresa = c.id_empresa
     AND ent.id_filial = c.id_filial
     AND ent.id_entidade = cad.id_entidade
    LEFT JOIN (
      SELECT
        id_filial,
        id_db,
        id_comprovante,
        argMax(numero_nfe, source_ts_ms) AS numero_nfe_res
      FROM {CURRENT_DB}.stg_nfe_slim
      WHERE id_empresa = {{id_empresa:Int32}}
        AND is_deleted = 0
        AND numero_nfe != ''
        AND numero_nfe != '0'
        AND status != 5
      GROUP BY id_filial, id_db, id_comprovante
    ) AS nfe
      ON nfe.id_filial = c.id_filial
     AND nfe.id_db = c.id_db
     AND nfe.id_comprovante = c.id_comprovante
    WHERE i.id_empresa = {{id_empresa:Int32}}
      {branch_sql}
      AND i.is_deleted = 0
      AND c.is_deleted = 0
      AND coalesce(c.situacao_shadow, 0) != 3
      AND coalesce(c.cancelado_shadow, 0) = 0
      AND coalesce(i.qtd_shadow, 0) > 0
      AND coalesce(i.valor_unitario_shadow, 0) > 0
      AND bomba.preco_venda > 0
      AND (bomba.preco_venda - coalesce(i.valor_unitario_shadow, 0)) > {{delta_min:Float64}}
      AND toDate(c.dt_evento, 'America/Sao_Paulo') >= toDate({{dt_ini:String}})
      AND toDate(c.dt_evento, 'America/Sao_Paulo') <= toDate({{dt_fim:String}})
      AND (
        upperUTF8(coalesce(JSONExtractString(g.payload, 'NOMEGRUPOPRODUTOS'), '')) LIKE '%%COMBUST%%'
        OR toInt32OrZero(JSONExtractString(p.payload, 'TIPOCOMBUSTIVEL')) > 0
      )
    """
    execute_command(sql, parameters=params)
    count_rows = query_dict(
        f"""
        SELECT count() AS n
        FROM {MART_DB}.mart_cliente_preco_fixo_item FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND dt_venda >= toDate({{dt_ini:String}})
          AND dt_venda <= toDate({{dt_fim:String}})
        """,
        parameters={
            "id_empresa": int(id_empresa),
            "dt_ini": dt_ini.isoformat(),
            "dt_fim": dt_fim.isoformat(),
        },
        tenant_id=id_empresa,
    )
    return int(count_rows[0]["n"]) if count_rows else 0


def publish_and_rebuild(
    role: str,
    id_empresa: int,
    *,
    days: int = 120,
    id_filial: Optional[int] = None,
    id_filiais: Optional[Sequence[int]] = None,
) -> Dict[str, Any]:
    dt_fim = date.today()
    dt_ini = dt_fim - timedelta(days=int(days))
    n_cad = publish_cadastro(role, id_empresa)
    n_bomba = publish_preco_bomba_dia(role, id_empresa, days=max(days + 30, 180))
    n_itens = rebuild_itens(
        id_empresa,
        dt_ini,
        dt_fim,
        id_filial=id_filial,
        id_filiais=id_filiais,
    )
    out = {
        "cadastro": n_cad,
        "bomba_dia": n_bomba,
        "itens": n_itens,
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
    }
    logger.info("cliente_preco_fixo publish empresa=%s %s", id_empresa, out)
    return out


def overview(
    id_empresa: int,
    dt_ini: date,
    dt_fim: date,
    *,
    id_filial: Optional[int] = None,
    id_filiais: Optional[Sequence[int]] = None,
    page: int = 0,
    page_size: int = 20,
    search: str = "",
) -> Dict[str, Any]:
    """Grid resumido: filial, cliente, litros e desconto total no período."""
    page = max(0, int(page))
    page_size = max(1, min(100, int(page_size)))
    branch_sql, branch_params = _branch_clause(id_filial, id_filiais)
    search = (search or "").strip()
    search_sql = ""
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "limit": page_size,
        "offset": page * page_size,
        **branch_params,
    }
    if search:
        params["search"] = f"%{search.upper()}%"
        search_sql = "AND upperUTF8(cliente_nome) LIKE {search:String}"

    base_agg = f"""
        SELECT
          id_filial,
          id_entidade,
          any(cliente_nome) AS cliente_nome,
          sum(desconto_total) AS desconto_total_agg,
          sum(qtd) AS qtd_litros_agg,
          count() AS qtd_itens,
          uniqExact(id_comprovante) AS qtd_vendas
        FROM {MART_DB}.mart_cliente_preco_fixo_item FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          {branch_sql}
          AND dt_venda >= toDate({{dt_ini:String}})
          AND dt_venda <= toDate({{dt_fim:String}})
          AND desconto_total > 0
        GROUP BY id_filial, id_entidade
    """
    total_row = query_dict(
        f"""
        SELECT
          count() AS n,
          sum(desconto_total_agg) AS desconto_total_sum,
          sum(qtd_litros_agg) AS qtd_litros_sum
        FROM ({base_agg}) AS agg
        WHERE 1=1
          {search_sql}
        """,
        parameters=params,
        tenant_id=id_empresa,
    )
    total = int(total_row[0]["n"]) if total_row else 0
    desconto_periodo = float(total_row[0]["desconto_total_sum"] or 0) if total_row else 0.0
    litros_periodo = float(total_row[0]["qtd_litros_sum"] or 0) if total_row else 0.0

    rows = query_dict(
        f"""
        SELECT
          id_filial,
          id_entidade,
          cliente_nome,
          desconto_total_agg AS desconto_total,
          qtd_litros_agg AS qtd_litros,
          qtd_itens,
          qtd_vendas
        FROM ({base_agg}) AS agg
        WHERE 1=1
          {search_sql}
        ORDER BY id_filial ASC, upperUTF8(cliente_nome) ASC, desconto_total_agg DESC
        LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}
        """,
        parameters=params,
        tenant_id=id_empresa,
    )

    items = []
    for r in rows:
        fid = int(r["id_filial"])
        items.append(
            {
                "id_filial": fid,
                "filial_label": apelido_for(fid) or str(fid),
                "id_entidade": int(r["id_entidade"]),
                "cliente_nome": unescape(str(r.get("cliente_nome") or f"Cliente {r['id_entidade']}")),
                "desconto_total": round(float(r.get("desconto_total") or 0), 2),
                "qtd_litros": round(float(r.get("qtd_litros") or 0), 3),
                "qtd_itens": int(r.get("qtd_itens") or 0),
                "qtd_vendas": int(r.get("qtd_vendas") or 0),
            }
        )

    total_pages = (total + page_size - 1) // page_size if page_size else 0
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "summary": {
            "clientes": total,
            "desconto_total": round(desconto_periodo, 2),
            "qtd_litros": round(litros_periodo, 3),
        },
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "source": "clickhouse_mart",
    }


def _map_detail_item(r: Dict[str, Any]) -> Dict[str, Any]:
    custo_raw = r.get("custo_unitario")
    custo = float(custo_raw) if custo_raw is not None else None
    if custo is not None and custo <= 0:
        custo = None
    margem = r.get("margem_unitaria_pct")
    margem_bomba = r.get("margem_bomba_pct")
    return {
        "row_kind": "item",
        "id_comprovante": int(r["id_comprovante"]),
        "id_itemcomprovante": int(r["id_itemcomprovante"]),
        "id_produto": int(r["id_produto"]),
        "dt_venda": str(r.get("dt_venda") or ""),
        "dt_evento": str(r.get("dt_evento") or ""),
        "cliente_nome": unescape(str(r.get("cliente_nome") or "")),
        "produto_nome": unescape(str(r.get("produto_nome") or "")),
        "documento_label": str(r.get("documento_label") or "—"),
        "qtd": float(r.get("qtd") or 0),
        "preco_bomba": float(r.get("preco_bomba") or 0),
        "preco_pago": float(r.get("preco_pago") or 0),
        "desconto_unitario": float(r.get("desconto_unitario") or 0),
        "desconto_total": float(r.get("desconto_total") or 0),
        "custo_unitario": custo,
        "margem_unitaria_pct": float(margem) if margem is not None else None,
        "margem_bomba_pct": float(margem_bomba) if margem_bomba is not None else None,
    }


def _date_sort_key(value: Any) -> int:
    raw = str(value or "").strip()[:10].replace("-", "")
    if raw.isdigit():
        return int(raw)
    return 0


def _build_detail_rows_with_subtotals(raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Agrupa por produto (nome ASC); dentro: Data DESC → Documento ASC + subtotal."""
    items = [_map_detail_item(r) for r in raw_items]
    items.sort(
        key=lambda x: (
            str(x.get("produto_nome") or "").upper(),
            int(x.get("id_produto") or 0),
            -_date_sort_key(x.get("dt_venda")),
            str(x.get("documento_label") or ""),
            int(x.get("id_comprovante") or 0),
            int(x.get("id_itemcomprovante") or 0),
        )
    )
    out: List[Dict[str, Any]] = []
    for (_pid, _nome), group_iter in groupby(
        items, key=lambda x: (int(x["id_produto"]), str(x.get("produto_nome") or ""))
    ):
        group = list(group_iter)
        out.extend(group)
        qtd = sum(float(i.get("qtd") or 0) for i in group)
        desconto = sum(float(i.get("desconto_total") or 0) for i in group)
        out.append(
            {
                "row_kind": "subtotal",
                "id_produto": int(group[0]["id_produto"]),
                "produto_nome": group[0].get("produto_nome") or "",
                "qtd": round(qtd, 3),
                "desconto_total": round(desconto, 2),
                "documento_label": "",
                "dt_venda": "",
                "preco_bomba": None,
                "preco_pago": None,
                "desconto_unitario": None,
                "custo_unitario": None,
                "margem_unitaria_pct": None,
                "margem_bomba_pct": None,
            }
        )
    return out


def detail(
    id_empresa: int,
    id_filial: int,
    id_entidade: int,
    dt_ini: date,
    dt_fim: date,
    *,
    page: int = 0,
    page_size: int = 200,
) -> Dict[str, Any]:
    """Drill-down: itens agrupados por produto com subtotal (Data→Documento)."""
    page = max(0, int(page))
    page_size = max(1, min(500, int(page_size)))
    params = {
        "id_empresa": int(id_empresa),
        "id_filial": int(id_filial),
        "id_entidade": int(id_entidade),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "limit": page_size,
        "offset": page * page_size,
    }
    total_row = query_dict(
        f"""
        SELECT
          count() AS n,
          sum(desconto_total) AS desconto_total_sum,
          sum(qtd) AS qtd_litros_sum
        FROM {MART_DB}.mart_cliente_preco_fixo_item FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_filial = {{id_filial:Int32}}
          AND id_entidade = {{id_entidade:Int32}}
          AND dt_venda >= toDate({{dt_ini:String}})
          AND dt_venda <= toDate({{dt_fim:String}})
          AND desconto_total > 0
        """,
        parameters=params,
        tenant_id=id_empresa,
    )
    total = int(total_row[0]["n"]) if total_row else 0
    desconto_total = float(total_row[0]["desconto_total_sum"] or 0) if total_row else 0.0
    qtd_litros = float(total_row[0]["qtd_litros_sum"] or 0) if total_row else 0.0

    rows = query_dict(
        f"""
        SELECT
          id_comprovante,
          id_itemcomprovante,
          id_produto,
          dt_venda,
          dt_evento,
          cliente_nome,
          produto_nome,
          documento_label,
          qtd,
          preco_bomba,
          preco_pago,
          desconto_unitario,
          desconto_total,
          custo_unitario,
          margem_unitaria_pct,
          margem_bomba_pct
        FROM {MART_DB}.mart_cliente_preco_fixo_item FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          AND id_filial = {{id_filial:Int32}}
          AND id_entidade = {{id_entidade:Int32}}
          AND dt_venda >= toDate({{dt_ini:String}})
          AND dt_venda <= toDate({{dt_fim:String}})
          AND desconto_total > 0
        ORDER BY
          produto_nome ASC,
          id_produto ASC,
          dt_venda DESC,
          documento_label ASC,
          id_comprovante ASC,
          id_itemcomprovante ASC
        LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}
        """,
        parameters=params,
        tenant_id=id_empresa,
    )
    items = _build_detail_rows_with_subtotals(rows)
    cliente_nome = ""
    for it in items:
        if it.get("row_kind") == "item" and it.get("cliente_nome"):
            cliente_nome = it["cliente_nome"]
            break
    total_pages = (total + page_size - 1) // page_size if page_size else 0
    return {
        "id_filial": int(id_filial),
        "filial_label": apelido_for(int(id_filial)) or str(id_filial),
        "id_entidade": int(id_entidade),
        "cliente_nome": cliente_nome,
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "summary": {
            "desconto_total": round(desconto_total, 2),
            "qtd_litros": round(qtd_litros, 3),
        },
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "source": "clickhouse_mart",
    }
