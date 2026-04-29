BEGIN;

CREATE OR REPLACE FUNCTION etl.resolve_item_group_produto(
  p_group_shadow integer,
  p_payload jsonb,
  p_dim_group integer
)
RETURNS integer
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    p_group_shadow,
    etl.safe_int(p_payload->>'ID_GRUPOPRODUTOS'),
    etl.safe_int(p_payload->>'ID_GRUPO_PRODUTO'),
    p_dim_group
  )
$$;

CREATE OR REPLACE FUNCTION etl.repair_fact_venda_item_group_from_dim(
  p_id_empresa integer,
  p_id_filial integer DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_comprovante,
      i.id_itemcomprovante,
      etl.resolve_item_group_produto(
        i.id_grupo_produto_shadow,
        i.payload,
        dp.id_grupo_produto
      ) AS id_grupo_produto
    FROM stg.itenscomprovantes i
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
    WHERE i.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR i.id_filial = p_id_filial)
  ),
  repaired AS (
    UPDATE dw.fact_venda_item f
       SET id_grupo_produto = s.id_grupo_produto,
           updated_at = now()
      FROM src s
     WHERE f.id_empresa = s.id_empresa
       AND f.id_filial = s.id_filial
       AND f.id_db = s.id_db
       AND f.id_comprovante = s.id_comprovante
       AND f.id_itemcomprovante = s.id_itemcomprovante
       AND s.id_grupo_produto IS NOT NULL
       AND f.id_grupo_produto IS DISTINCT FROM s.id_grupo_produto
    RETURNING 1
  )
  SELECT COUNT(*)::int
    INTO v_rows
  FROM repaired;

  RETURN jsonb_build_object(
    'id_empresa', p_id_empresa,
    'id_filial', p_id_filial,
    'rows_repaired', COALESCE(v_rows, 0)
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item_range_detail(
  p_id_empresa int,
  p_id_comprovante_from int DEFAULT NULL,
  p_id_comprovante_to int DEFAULT NULL,
  p_update_watermark boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_cutoff date;
  v_candidate_count integer := 0;
  v_conflict_count integer := 0;
  v_upsert_inserts integer := 0;
  v_upsert_updates integer := 0;
  v_total_ms integer := 0;
  v_started timestamptz := clock_timestamp();
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itenscomprovantes_sales_fact'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  WITH src AS MATERIALIZED (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_comprovante AS id_movprodutos,
      i.id_itemcomprovante AS id_itensmovprodutos,
      i.id_comprovante,
      i.id_itemcomprovante,
      COALESCE(
        v.data_key,
        c.data_key,
        etl.business_date_key(i.dt_evento)
      ) AS data_key,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      etl.resolve_item_group_produto(
        i.id_grupo_produto_shadow,
        i.payload,
        dp.id_grupo_produto
      ) AS id_grupo_produto,
      COALESCE(i.id_local_venda_shadow, etl.safe_int(i.payload->>'ID_LOCALVENDAS')) AS id_local_venda,
      COALESCE(i.id_funcionario_shadow, etl.safe_int(i.payload->>'ID_FUNCIONARIOS')) AS id_funcionario,
      COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP')) AS cfop,
      COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)) AS qtd,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS valor_unitario,
      etl.resolve_item_total(i.total_shadow, i.payload)::numeric(18,2) AS total,
      COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2)) AS desconto,
      COALESCE(
        (
          etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6)
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2),
        (
          dp.custo_medio
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2)
      ) AS custo_total,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS preco_praticado_unitario,
      NULL::numeric(18,4) AS preco_lista_unitario,
      CASE
        WHEN COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')) > 0 THEN (
          COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2))
          / NULLIF(COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)), 0)
        )::numeric(18,4)
        ELSE NULL::numeric(18,4)
      END AS desconto_unitario,
      COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2)) AS desconto_total,
      CASE
        WHEN COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2), 0) > 0
          THEN 'payload_explicit_discount'
        ELSE NULL
      END AS discount_source,
      i.payload
    FROM stg.itenscomprovantes i
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_comprovante = i.id_comprovante
    LEFT JOIN dw.fact_comprovante c
      ON c.id_empresa = i.id_empresa
     AND c.id_filial = i.id_filial
     AND c.id_db = i.id_db
     AND c.id_comprovante = i.id_comprovante
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
    WHERE i.id_empresa = p_id_empresa
      AND COALESCE(etl.business_date(i.dt_evento), v_cutoff) >= v_cutoff
      AND (
        i.received_at > v_wm
        OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
      AND (p_id_comprovante_from IS NULL OR i.id_comprovante >= p_id_comprovante_from)
      AND (p_id_comprovante_to IS NULL OR i.id_comprovante <= p_id_comprovante_to)
  ), prepared AS MATERIALIZED (
    SELECT
      s.*,
      f.id_movprodutos AS current_id_movprodutos,
      f.id_itensmovprodutos AS current_id_itensmovprodutos,
      f.data_key AS current_data_key,
      f.id_produto AS current_id_produto,
      f.id_grupo_produto AS current_id_grupo_produto,
      f.id_local_venda AS current_id_local_venda,
      f.id_funcionario AS current_id_funcionario,
      f.cfop AS current_cfop,
      f.qtd AS current_qtd,
      f.valor_unitario AS current_valor_unitario,
      f.total AS current_total,
      f.desconto AS current_desconto,
      f.custo_total AS current_custo_total,
      f.preco_lista_unitario AS current_preco_lista_unitario,
      f.preco_praticado_unitario AS current_preco_praticado_unitario,
      f.desconto_unitario AS current_desconto_unitario,
      f.desconto_total AS current_desconto_total,
      f.discount_source AS current_discount_source,
      f.payload AS current_payload
    FROM src s
    LEFT JOIN dw.fact_venda_item f
      ON f.id_empresa = s.id_empresa
     AND f.id_filial = s.id_filial
     AND f.id_db = s.id_db
     AND f.id_comprovante = s.id_comprovante
     AND f.id_itemcomprovante = s.id_itemcomprovante
  ), to_upsert AS MATERIALIZED (
    SELECT *
    FROM prepared
    WHERE ROW(
      current_id_movprodutos,
      current_id_itensmovprodutos,
      current_data_key,
      current_id_produto,
      current_id_grupo_produto,
      current_id_local_venda,
      current_id_funcionario,
      current_cfop,
      current_qtd,
      current_valor_unitario,
      current_total,
      current_desconto,
      current_custo_total,
      current_preco_lista_unitario,
      current_preco_praticado_unitario,
      current_desconto_unitario,
      current_desconto_total,
      current_discount_source,
      current_payload
    ) IS DISTINCT FROM ROW(
      id_movprodutos,
      id_itensmovprodutos,
      data_key,
      id_produto,
      id_grupo_produto,
      id_local_venda,
      id_funcionario,
      cfop,
      qtd,
      valor_unitario,
      total,
      desconto,
      custo_total,
      preco_lista_unitario,
      preco_praticado_unitario,
      desconto_unitario,
      desconto_total,
      discount_source,
      payload
    )
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      id_itensmovprodutos,
      id_comprovante,
      id_itemcomprovante,
      data_key,
      id_produto,
      id_grupo_produto,
      id_local_venda,
      id_funcionario,
      cfop,
      qtd,
      valor_unitario,
      total,
      desconto,
      custo_total,
      margem,
      preco_lista_unitario,
      preco_praticado_unitario,
      desconto_unitario,
      desconto_total,
      discount_source,
      payload
    )
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      id_itensmovprodutos,
      id_comprovante,
      id_itemcomprovante,
      data_key,
      id_produto,
      id_grupo_produto,
      id_local_venda,
      id_funcionario,
      cfop,
      qtd,
      valor_unitario,
      total,
      desconto,
      custo_total,
      (COALESCE(total, 0) - COALESCE(custo_total, 0))::numeric(18,2),
      preco_lista_unitario,
      preco_praticado_unitario,
      desconto_unitario,
      desconto_total,
      discount_source,
      payload
    FROM to_upsert
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
    DO UPDATE SET
      id_movprodutos = EXCLUDED.id_movprodutos,
      id_itensmovprodutos = EXCLUDED.id_itensmovprodutos,
      data_key = EXCLUDED.data_key,
      id_produto = EXCLUDED.id_produto,
      id_grupo_produto = EXCLUDED.id_grupo_produto,
      id_local_venda = EXCLUDED.id_local_venda,
      id_funcionario = EXCLUDED.id_funcionario,
      cfop = EXCLUDED.cfop,
      qtd = EXCLUDED.qtd,
      valor_unitario = EXCLUDED.valor_unitario,
      total = EXCLUDED.total,
      desconto = EXCLUDED.desconto,
      custo_total = EXCLUDED.custo_total,
      margem = EXCLUDED.margem,
      preco_lista_unitario = EXCLUDED.preco_lista_unitario,
      preco_praticado_unitario = EXCLUDED.preco_praticado_unitario,
      desconto_unitario = EXCLUDED.desconto_unitario,
      desconto_total = EXCLUDED.desconto_total,
      discount_source = EXCLUDED.discount_source,
      payload = EXCLUDED.payload
    WHERE ROW(
      dw.fact_venda_item.id_movprodutos,
      dw.fact_venda_item.id_itensmovprodutos,
      dw.fact_venda_item.data_key,
      dw.fact_venda_item.id_produto,
      dw.fact_venda_item.id_grupo_produto,
      dw.fact_venda_item.id_local_venda,
      dw.fact_venda_item.id_funcionario,
      dw.fact_venda_item.cfop,
      dw.fact_venda_item.qtd,
      dw.fact_venda_item.valor_unitario,
      dw.fact_venda_item.total,
      dw.fact_venda_item.desconto,
      dw.fact_venda_item.custo_total,
      dw.fact_venda_item.preco_lista_unitario,
      dw.fact_venda_item.preco_praticado_unitario,
      dw.fact_venda_item.desconto_unitario,
      dw.fact_venda_item.desconto_total,
      dw.fact_venda_item.discount_source,
      dw.fact_venda_item.payload
    ) IS DISTINCT FROM ROW(
      EXCLUDED.id_movprodutos,
      EXCLUDED.id_itensmovprodutos,
      EXCLUDED.data_key,
      EXCLUDED.id_produto,
      EXCLUDED.id_grupo_produto,
      EXCLUDED.id_local_venda,
      EXCLUDED.id_funcionario,
      EXCLUDED.cfop,
      EXCLUDED.qtd,
      EXCLUDED.valor_unitario,
      EXCLUDED.total,
      EXCLUDED.desconto,
      EXCLUDED.custo_total,
      EXCLUDED.preco_lista_unitario,
      EXCLUDED.preco_praticado_unitario,
      EXCLUDED.desconto_unitario,
      EXCLUDED.desconto_total,
      EXCLUDED.discount_source,
      EXCLUDED.payload
    )
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM src), 0),
    COALESCE((SELECT COUNT(*) FROM prepared WHERE current_payload IS NOT NULL), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE inserted), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE NOT inserted), 0)
  INTO v_candidate_count, v_conflict_count, v_upsert_inserts, v_upsert_updates;

  IF p_update_watermark THEN
    SELECT MAX(received_at) INTO v_max
    FROM stg.itenscomprovantes
    WHERE id_empresa = p_id_empresa
      AND received_at > v_wm;

    PERFORM etl.set_watermark(
      p_id_empresa,
      'itenscomprovantes_sales_fact',
      COALESCE(v_max, v_wm),
      NULL::bigint
    );
  END IF;

  v_total_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_started)) * 1000)::int;

  RETURN jsonb_build_object(
    'rows', COALESCE(v_upsert_inserts, 0) + COALESCE(v_upsert_updates, 0),
    'candidate_count', COALESCE(v_candidate_count, 0),
    'conflict_count', COALESCE(v_conflict_count, 0),
    'upsert_inserts', COALESCE(v_upsert_inserts, 0),
    'upsert_updates', COALESCE(v_upsert_updates, 0),
    'range_from', p_id_comprovante_from,
    'range_to', p_id_comprovante_to,
    'watermark_updated', p_update_watermark,
    'total_ms', COALESCE(v_total_ms, 0)
  );
END;
$$;

COMMIT;
 