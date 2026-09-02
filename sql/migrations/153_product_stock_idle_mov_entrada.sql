-- 153: Compras recentes — UNION nfe_entrada + movprodutos (loja; paridade Xpert)
BEGIN;

CREATE OR REPLACE FUNCTION etl.refresh_product_stock_idle(p_id_empresa integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_today date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_product_stock_idle'), p_id_empresa);
  PERFORM set_config('statement_timeout', '1200000', true);

  DELETE FROM mart.product_purchase_recent WHERE id_empresa = p_id_empresa;
  DELETE FROM mart.product_stock_idle WHERE id_empresa = p_id_empresa;

  WITH stock AS (
    SELECT
      e.id_empresa,
      e.id_filial,
      COALESCE(
        e.id_produto,
        e.id_produto_shadow,
        etl.safe_int(e.payload->>'ID_PRODUTOS')
      ) AS id_produto,
      GREATEST(
        COALESCE(
          e.qtd_atual_shadow,
          etl.safe_numeric(e.payload->>'QTDEATUAL'),
          e.quantidade,
          0
        ),
        0
      )::numeric(18, 3) AS qtd_estoque
    FROM stg.estoque e
    WHERE e.id_empresa = p_id_empresa
  ),
  stock_pos AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_produto,
      SUM(s.qtd_estoque)::numeric(18, 3) AS qtd_estoque
    FROM stock s
    WHERE s.id_produto > 0
      AND s.qtd_estoque > 0
      AND NOT EXISTS (
        SELECT 1
        FROM stg.tanques t
        WHERE t.id_empresa = s.id_empresa
          AND t.id_filial = s.id_filial
          AND etl.safe_int(t.payload->>'ID_PRODUTOS') = s.id_produto
      )
    GROUP BY s.id_empresa, s.id_filial, s.id_produto
    HAVING SUM(s.qtd_estoque) > 0
  ),
  last_sale AS (
    SELECT DISTINCT ON (fvi.id_empresa, fvi.id_filial, fvi.id_produto)
      fvi.id_empresa,
      fvi.id_filial,
      fvi.id_produto,
      to_date(fvi.data_key::text, 'YYYYMMDD') AS last_sale_date,
      COALESCE(
        NULLIF(fvi.preco_praticado_unitario, 0),
        NULLIF(fvi.valor_unitario, 0),
        0
      )::numeric(18, 4) AS preco_venda
    FROM dw.fact_venda_item fvi
    INNER JOIN stock_pos sp
      ON sp.id_empresa = fvi.id_empresa
     AND sp.id_filial = fvi.id_filial
     AND sp.id_produto = fvi.id_produto
    WHERE fvi.id_empresa = p_id_empresa
      AND fvi.data_key >= 20190101
      AND fvi.data_key < 20310101
      AND COALESCE(fvi.qtd, 0) > 0
      AND COALESCE(fvi.total, 0) > 0
    ORDER BY fvi.id_empresa, fvi.id_filial, fvi.id_produto, fvi.data_key DESC
  ),
  prod AS (
    SELECT DISTINCT ON (p.id_empresa, p.id_filial, p.id_produto)
      p.id_empresa,
      p.id_filial,
      p.id_produto,
      COALESCE(
        NULLIF(trim(both FROM dp.nome), ''),
        NULLIF(trim(both FROM p.payload->>'NOMEPRODUTO'), ''),
        NULLIF(trim(both FROM p.payload->>'NOME'), ''),
        NULLIF(trim(both FROM p.payload->>'DESCRICAO'), ''),
        'Produto ' || p.id_produto::text
      ) AS nome_produto,
      etl.map_setor_gerencial(
        COALESCE(
          dp.id_grupo_produto,
          etl.safe_int(p.payload->>'ID_GRUPOPRODUTOS'),
          etl.safe_int(p.payload->>'ID_GRUPO_PRODUTO'),
          0
        )
      ) AS setor_gerencial,
      COALESCE(
        NULLIF(etl.safe_numeric(p.payload->>'PRECOVENDA'), 0),
        NULLIF(etl.safe_numeric(p.payload->>'PRECO'), 0),
        NULLIF(etl.safe_numeric(p.payload->>'PPL'), 0),
        0
      )::numeric(18, 4) AS preco_stg,
      COALESCE(
        NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'), 0),
        NULLIF(dp.custo_medio, 0),
        0
      )::numeric(18, 4) AS custo_stg
    FROM stg.produtos p
    INNER JOIN stock_pos sp
      ON sp.id_empresa = p.id_empresa
     AND sp.id_filial = p.id_filial
     AND sp.id_produto = p.id_produto
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = p.id_empresa
     AND dp.id_filial = p.id_filial
     AND dp.id_produto = p.id_produto
    WHERE p.id_empresa = p_id_empresa
    ORDER BY p.id_empresa, p.id_filial, p.id_produto, p.dt_evento DESC NULLS LAST, p.id_db_shadow DESC
  ),
  purchase_lines_nfe AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      COALESCE(
        NULLIF(trim(both FROM n.numero_nota_shadow), ''),
        NULLIF(trim(both FROM n.payload->>'NUMERO'), ''),
        '—'
      ) AS numero_documento,
      COALESCE(
        n.dt_entrada_shadow::date,
        n.dt_evento::date,
        etl.safe_timestamp(n.payload->>'DATAENTRADA')::date,
        etl.safe_timestamp(n.payload->>'DATA')::date
      ) AS data_compra,
      GREATEST(
        COALESCE(
          i.qtd_shadow,
          etl.safe_numeric(i.payload->>'QUANTIDADE'),
          etl.safe_numeric(i.payload->>'QTDE'),
          0
        ),
        0
      )::numeric(18, 3) AS qtd,
      GREATEST(
        COALESCE(
          i.custo_unitario_shadow,
          etl.safe_numeric(i.payload->>'VLRCUSTO'),
          etl.safe_numeric(i.payload->>'VLRUNITARIO'),
          etl.safe_numeric(i.payload->>'CUSTO_UNITARIO'),
          0
        ),
        0
      )::numeric(18, 4) AS valor_unitario,
      GREATEST(
        COALESCE(
          i.custo_total_shadow,
          etl.safe_numeric(i.payload->>'VLRTOTALITEM'),
          etl.safe_numeric(i.payload->>'VALORTOTAL'),
          0
        ),
        0
      )::numeric(18, 2) AS valor_total,
      COALESCE(
        n.dt_entrada_shadow,
        n.dt_evento,
        etl.safe_timestamp(n.payload->>'DATAENTRADA'),
        etl.safe_timestamp(n.payload->>'DATA')
      ) AS sort_ts
    FROM stg.itens_nfe_entrada i
    INNER JOIN stg.nfe_entrada n
      ON n.id_empresa = i.id_empresa
     AND n.id_filial = i.id_filial
     AND n.id_db = i.id_db
     AND n.id_nota = i.id_nota
    INNER JOIN stock_pos sp
      ON sp.id_empresa = i.id_empresa
     AND sp.id_filial = i.id_filial
     AND sp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
    WHERE i.id_empresa = p_id_empresa
      AND COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) > 0
  ),
  purchase_lines_mov AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      COALESCE(
        NULLIF(trim(both FROM n.numero_nota_shadow), ''),
        NULLIF(trim(both FROM n.payload->>'NUMERO'), ''),
        '—'
      ) AS numero_documento,
      COALESCE(
        m.dt_evento::date,
        etl.safe_timestamp(m.payload->>'DATA')::date
      ) AS data_compra,
      GREATEST(
        COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE'), 0),
        0
      )::numeric(18, 3) AS qtd,
      GREATEST(
        COALESCE(
          etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS'),
          etl.safe_numeric(i.payload->>'VLRCUSTO'),
          etl.safe_numeric(i.payload->>'VLRUNITARIO'),
          0
        ),
        0
      )::numeric(18, 4) AS valor_unitario,
      GREATEST(
        COALESCE(
          etl.safe_numeric(i.payload->>'VLRTOTALITEM'),
          etl.safe_numeric(i.payload->>'VALORTOTAL'),
          0
        ),
        0
      )::numeric(18, 2) AS valor_total,
      COALESCE(
        m.dt_evento,
        etl.safe_timestamp(m.payload->>'DATA')
      ) AS sort_ts
    FROM stg.itensmovprodutos i
    INNER JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa
     AND m.id_filial = i.id_filial
     AND m.id_db = i.id_db
     AND m.id_movprodutos = etl.safe_int(i.payload->>'ID_MOVPRODUTOS')
    LEFT JOIN stg.nfe_entrada n
      ON n.id_empresa = m.id_empresa
     AND n.id_filial = m.id_filial
     AND n.id_db = m.id_db
     AND n.id_nota = etl.safe_int(m.payload->>'ID_COMPROVANTE')
    INNER JOIN stock_pos sp
      ON sp.id_empresa = i.id_empresa
     AND sp.id_filial = i.id_filial
     AND sp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
    WHERE i.id_empresa = p_id_empresa
      AND COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) > 0
      AND COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) IN (1, 2)
  ),
  purchase_lines AS (
    SELECT * FROM purchase_lines_nfe
    UNION ALL
    SELECT * FROM purchase_lines_mov
  ),
  purchase_ranked AS (
    SELECT
      pl.*,
      ROW_NUMBER() OVER (
        PARTITION BY pl.id_empresa, pl.id_filial, pl.id_produto
        ORDER BY pl.sort_ts DESC NULLS LAST, pl.data_compra DESC, pl.numero_documento DESC
      ) AS rn
    FROM purchase_lines pl
    WHERE pl.data_compra IS NOT NULL
      AND pl.qtd > 0
  ),
  purchase_top3 AS (
    SELECT * FROM purchase_ranked WHERE rn <= 3
  ),
  custo_avg AS (
    SELECT
      id_empresa,
      id_filial,
      id_produto,
      COALESCE(
        AVG(valor_unitario) FILTER (WHERE valor_unitario > 0),
        0
      )::numeric(18, 4) AS custo_medio_compra
    FROM purchase_top3
    GROUP BY id_empresa, id_filial, id_produto
  ),
  merged AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_produto,
      COALESCE(pr.nome_produto, 'Produto ' || s.id_produto::text) AS nome_produto,
      COALESCE(pr.setor_gerencial, 'outros') AS setor_gerencial,
      s.qtd_estoque,
      ls.last_sale_date,
      CASE
        WHEN ls.last_sale_date IS NULL THEN 9999
        ELSE GREATEST((v_today - ls.last_sale_date), 0)
      END AS dias_sem_venda,
      COALESCE(NULLIF(ca.custo_medio_compra, 0), NULLIF(pr.custo_stg, 0), 0) AS custo_medio_compra,
      COALESCE(NULLIF(ls.preco_venda, 0), NULLIF(pr.preco_stg, 0), 0) AS preco_venda
    FROM stock_pos s
    LEFT JOIN prod pr
      ON pr.id_empresa = s.id_empresa
     AND pr.id_filial = s.id_filial
     AND pr.id_produto = s.id_produto
    LEFT JOIN last_sale ls
      ON ls.id_empresa = s.id_empresa
     AND ls.id_filial = s.id_filial
     AND ls.id_produto = s.id_produto
    LEFT JOIN custo_avg ca
      ON ca.id_empresa = s.id_empresa
     AND ca.id_filial = s.id_filial
     AND ca.id_produto = s.id_produto
    WHERE COALESCE(pr.setor_gerencial, 'outros') <> 'interno'
  ),
  ins_idle AS (
    INSERT INTO mart.product_stock_idle (
      id_empresa, id_filial, id_produto, nome_produto, setor_gerencial,
      qtd_estoque, last_sale_date, dias_sem_venda, custo_medio_compra, preco_venda, updated_at
    )
    SELECT
      id_empresa, id_filial, id_produto, nome_produto, setor_gerencial,
      qtd_estoque, last_sale_date, dias_sem_venda, custo_medio_compra, preco_venda, now()
    FROM merged
    RETURNING 1
  ),
  ins_purchase AS (
    INSERT INTO mart.product_purchase_recent (
      id_empresa, id_filial, id_produto, rank,
      numero_documento, data_compra, qtd, valor_unitario, valor_total, updated_at
    )
    SELECT
      id_empresa,
      id_filial,
      id_produto,
      rn::smallint,
      numero_documento,
      data_compra,
      qtd,
      valor_unitario,
      CASE
        WHEN valor_total > 0 THEN valor_total
        WHEN valor_unitario > 0 AND qtd > 0 THEN ROUND(valor_unitario * qtd, 2)
        ELSE 0
      END,
      now()
    FROM purchase_top3
    RETURNING 1
  )
  SELECT count(*)::integer INTO v_rows FROM ins_idle;

  RETURN COALESCE(v_rows, 0);
END;
$$;

COMMENT ON FUNCTION etl.refresh_product_stock_idle(integer) IS
  'Estoque parado: compras via nfe_entrada + movprodutos (loja); timeout 20min.';

COMMIT;
