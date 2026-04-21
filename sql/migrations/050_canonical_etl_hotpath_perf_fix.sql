-- @nontransactional

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_movprodutos_emp_filial_db_comp_payload
  ON stg.movprodutos (
    id_empresa,
    id_filial,
    id_db,
    (etl.safe_int(payload->>'ID_COMPROVANTE'))
  )
  WHERE id_comprovante_shadow IS NULL
    AND etl.safe_int(payload->>'ID_COMPROVANTE') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_itensmovprodutos_emp_filial_db_mov_item_payload
  ON stg.itensmovprodutos (
    id_empresa,
    id_filial,
    id_db,
    id_movprodutos,
    (etl.safe_int(payload->>'ID_ITENS_COMPROVANTE'))
  )
  WHERE etl.safe_int(payload->>'ID_ITENS_COMPROVANTE') IS NOT NULL;

CREATE OR REPLACE FUNCTION etl.backfill_itenscomprovantes_from_legacy(p_id_empresa integer DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH src AS (
    SELECT
      src_rows.id_empresa,
      src_rows.id_filial,
      src_rows.id_db,
      src_rows.id_comprovante,
      src_rows.id_itemcomprovante,
      src_rows.payload,
      src_rows.ingested_at,
      src_rows.dt_evento,
      src_rows.id_db_shadow,
      src_rows.id_chave_natural,
      src_rows.received_at,
      src_rows.id_produto_shadow,
      src_rows.id_grupo_produto_shadow,
      src_rows.id_local_venda_shadow,
      src_rows.id_funcionario_shadow,
      src_rows.cfop_shadow,
      src_rows.qtd_shadow,
      src_rows.valor_unitario_shadow,
      src_rows.total_shadow,
      src_rows.desconto_shadow,
      src_rows.custo_unitario_shadow
    FROM (
      SELECT DISTINCT ON (
        i.id_empresa,
        i.id_filial,
        i.id_db,
        COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')),
        COALESCE(etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'), i.id_itensmovprodutos)
      )
        i.id_empresa,
        i.id_filial,
        i.id_db,
        COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) AS id_comprovante,
        COALESCE(etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'), i.id_itensmovprodutos) AS id_itemcomprovante,
        jsonb_strip_nulls(
          i.payload
          || jsonb_build_object(
            'ID_COMPROVANTE', COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')),
            'ID_ITENS_COMPROVANTE', COALESCE(etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'), i.id_itensmovprodutos)
          )
        ) AS payload,
        COALESCE(i.ingested_at, now()) AS ingested_at,
        COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento)) AS dt_evento,
        i.id_db_shadow,
        COALESCE(
          NULLIF(i.id_chave_natural, ''),
          format(
            'legacy:%s:%s:%s:%s:%s',
            i.id_empresa,
            i.id_filial,
            i.id_db,
            COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')),
            COALESCE(etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'), i.id_itensmovprodutos)
          )
        ) AS id_chave_natural,
        COALESCE(i.received_at, now()) AS received_at,
        i.id_produto_shadow,
        i.id_grupo_produto_shadow,
        i.id_local_venda_shadow,
        i.id_funcionario_shadow,
        i.cfop_shadow,
        i.qtd_shadow,
        i.valor_unitario_shadow,
        i.total_shadow,
        i.desconto_shadow,
        i.custo_unitario_shadow
      FROM stg.itensmovprodutos i
      JOIN stg.movprodutos m
        ON m.id_empresa = i.id_empresa
       AND m.id_filial = i.id_filial
       AND m.id_db = i.id_db
       AND m.id_movprodutos = i.id_movprodutos
      WHERE (p_id_empresa IS NULL OR i.id_empresa = p_id_empresa)
        AND COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) IS NOT NULL
        AND COALESCE(etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'), i.id_itensmovprodutos) IS NOT NULL
      ORDER BY
        i.id_empresa,
        i.id_filial,
        i.id_db,
        COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')),
        COALESCE(etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'), i.id_itensmovprodutos),
        COALESCE(i.received_at, i.ingested_at, i.dt_evento, m.received_at, m.ingested_at, m.dt_evento, now()) DESC,
        COALESCE(i.dt_evento, m.dt_evento, now()) DESC,
        i.id_itensmovprodutos DESC
    ) src_rows
  ), upserted AS (
    INSERT INTO stg.itenscomprovantes (
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      id_itemcomprovante,
      payload,
      ingested_at,
      dt_evento,
      id_db_shadow,
      id_chave_natural,
      received_at,
      id_produto_shadow,
      id_grupo_produto_shadow,
      id_local_venda_shadow,
      id_funcionario_shadow,
      cfop_shadow,
      qtd_shadow,
      valor_unitario_shadow,
      total_shadow,
      desconto_shadow,
      custo_unitario_shadow
    )
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      id_itemcomprovante,
      payload,
      ingested_at,
      dt_evento,
      id_db_shadow,
      id_chave_natural,
      received_at,
      id_produto_shadow,
      id_grupo_produto_shadow,
      id_local_venda_shadow,
      id_funcionario_shadow,
      cfop_shadow,
      qtd_shadow,
      valor_unitario_shadow,
      total_shadow,
      desconto_shadow,
      custo_unitario_shadow
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
    DO UPDATE SET
      payload = EXCLUDED.payload,
      ingested_at = EXCLUDED.ingested_at,
      dt_evento = EXCLUDED.dt_evento,
      id_db_shadow = EXCLUDED.id_db_shadow,
      id_chave_natural = EXCLUDED.id_chave_natural,
      received_at = EXCLUDED.received_at,
      id_produto_shadow = EXCLUDED.id_produto_shadow,
      id_grupo_produto_shadow = EXCLUDED.id_grupo_produto_shadow,
      id_local_venda_shadow = EXCLUDED.id_local_venda_shadow,
      id_funcionario_shadow = EXCLUDED.id_funcionario_shadow,
      cfop_shadow = EXCLUDED.cfop_shadow,
      qtd_shadow = EXCLUDED.qtd_shadow,
      valor_unitario_shadow = EXCLUDED.valor_unitario_shadow,
      total_shadow = EXCLUDED.total_shadow,
      desconto_shadow = EXCLUDED.desconto_shadow,
      custo_unitario_shadow = EXCLUDED.custo_unitario_shadow
    WHERE stg.itenscomprovantes.payload IS DISTINCT FROM EXCLUDED.payload
      OR stg.itenscomprovantes.dt_evento IS DISTINCT FROM EXCLUDED.dt_evento
      OR stg.itenscomprovantes.total_shadow IS DISTINCT FROM EXCLUDED.total_shadow
      OR stg.itenscomprovantes.qtd_shadow IS DISTINCT FROM EXCLUDED.qtd_shadow
      OR stg.itenscomprovantes.custo_unitario_shadow IS DISTINCT FROM EXCLUDED.custo_unitario_shadow
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  RETURN COALESCE(v_rows, 0);
END;
$$;

CREATE OR REPLACE FUNCTION etl.run_legacy_sales_transition_backfill(p_id_empresa integer DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_bridge jsonb := '{}'::jsonb;
  v_items integer := 0;
BEGIN
  v_bridge := COALESCE(etl.sync_legacy_sales_bridge(p_id_empresa), '{}'::jsonb);
  v_items := COALESCE(etl.backfill_itenscomprovantes_from_legacy(p_id_empresa), 0);
  RETURN jsonb_build_object(
    'bridge', v_bridge,
    'items_backfilled', v_items
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes_sales_fact'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_comprovantes_sales;
  CREATE TEMP TABLE tmp_etl_candidate_comprovantes_sales (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_comprovante int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_comprovantes_sales
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) >= v_cutoff
    AND (
      c.received_at > v_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH bridge_candidates AS (
    SELECT
      tc.id_empresa,
      tc.id_filial,
      tc.id_db,
      tc.id_comprovante,
      legacy.id_movprodutos_legacy
    FROM tmp_etl_candidate_comprovantes_sales tc
    JOIN LATERAL (
      SELECT
        m.id_movprodutos AS id_movprodutos_legacy
      FROM stg.movprodutos m
      WHERE m.id_empresa = tc.id_empresa
        AND m.id_filial = tc.id_filial
        AND m.id_db = tc.id_db
        AND (
          m.id_comprovante_shadow = tc.id_comprovante
          OR (
            m.id_comprovante_shadow IS NULL
            AND etl.safe_int(m.payload->>'ID_COMPROVANTE') = tc.id_comprovante
          )
        )
      ORDER BY
        COALESCE(m.received_at, m.ingested_at, m.dt_evento, now()) DESC,
        COALESCE(m.dt_evento, now()) DESC,
        m.id_movprodutos DESC
      LIMIT 1
    ) legacy ON true
  )
  INSERT INTO etl.comprovante_sales_bridge (
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    id_movprodutos_legacy,
    source,
    updated_at
  )
  SELECT
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    id_movprodutos_legacy,
    'legacy_movprodutos_incremental',
    now()
  FROM bridge_candidates
  ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
  DO UPDATE SET
    id_movprodutos_legacy = EXCLUDED.id_movprodutos_legacy,
    source = EXCLUDED.source,
    updated_at = now()
  WHERE etl.comprovante_sales_bridge.id_movprodutos_legacy IS DISTINCT FROM EXCLUDED.id_movprodutos_legacy;

  WITH current_ids AS (
    SELECT DISTINCT ON (fv.id_empresa, fv.id_filial, fv.id_db, fv.id_comprovante)
      fv.id_empresa,
      fv.id_filial,
      fv.id_db,
      fv.id_comprovante,
      fv.id_movprodutos
    FROM dw.fact_venda fv
    JOIN tmp_etl_candidate_comprovantes_sales tc
      ON tc.id_empresa = fv.id_empresa
     AND tc.id_filial = fv.id_filial
     AND tc.id_db = fv.id_db
     AND tc.id_comprovante = fv.id_comprovante
    WHERE fv.id_empresa = p_id_empresa
      AND fv.id_comprovante IS NOT NULL
    ORDER BY
      fv.id_empresa,
      fv.id_filial,
      fv.id_db,
      fv.id_comprovante,
      fv.id_movprodutos DESC
  ), src AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      COALESCE(b.id_movprodutos_legacy, cur.id_movprodutos, c.id_comprovante) AS id_movprodutos,
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      c.id_comprovante,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(etl.safe_int(c.payload->>'SAIDAS_ENTRADAS'), 0) AS saidas_entradas,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS total_venda,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS cancelado,
      COALESCE(
        c.situacao_shadow,
        etl.safe_int(c.payload->>'SITUACAO'),
        etl.safe_int(c.payload->>'situacao'),
        etl.safe_int(c.payload->>'STATUS'),
        etl.safe_int(c.payload->>'status')
      ) AS situacao,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes_sales tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
    LEFT JOIN current_ids cur
      ON cur.id_empresa = c.id_empresa
     AND cur.id_filial = c.id_filial
     AND cur.id_db = c.id_db
     AND cur.id_comprovante = c.id_comprovante
    LEFT JOIN etl.comprovante_sales_bridge b
      ON b.id_empresa = c.id_empresa
     AND b.id_filial = c.id_filial
     AND b.id_db = c.id_db
     AND b.id_comprovante = c.id_comprovante
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      data,
      data_key,
      id_usuario,
      id_cliente,
      id_comprovante,
      id_turno,
      saidas_entradas,
      total_venda,
      situacao,
      cancelado,
      payload
    )
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      data,
      data_key,
      id_usuario,
      id_cliente,
      id_comprovante,
      id_turno,
      saidas_entradas,
      total_venda,
      situacao,
      cancelado,
      payload
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
    DO UPDATE SET
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_cliente = EXCLUDED.id_cliente,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      saidas_entradas = EXCLUDED.saidas_entradas,
      total_venda = EXCLUDED.total_venda,
      situacao = EXCLUDED.situacao,
      cancelado = EXCLUDED.cancelado,
      payload = EXCLUDED.payload
    WHERE dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
      OR dw.fact_venda.id_comprovante IS DISTINCT FROM EXCLUDED.id_comprovante
      OR dw.fact_venda.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_venda.situacao IS DISTINCT FROM EXCLUDED.situacao
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(
    p_id_empresa,
    'comprovantes_sales_fact',
    COALESCE(v_max, v_wm),
    NULL::bigint
  );
  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itenscomprovantes_sales_fact'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_itenscomprovantes;
  CREATE TEMP TABLE tmp_etl_candidate_itenscomprovantes (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_comprovante int NOT NULL,
    id_itemcomprovante int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_itenscomprovantes
  SELECT
    i.id_empresa,
    i.id_filial,
    i.id_db,
    i.id_comprovante,
    i.id_itemcomprovante
  FROM stg.itenscomprovantes i
  WHERE i.id_empresa = p_id_empresa
    AND COALESCE(etl.business_date(i.dt_evento), v_cutoff) >= v_cutoff
    AND (
      i.received_at > v_wm
      OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH bridge_candidates AS (
    SELECT
      ti.id_empresa,
      ti.id_filial,
      ti.id_db,
      ti.id_comprovante,
      ti.id_itemcomprovante,
      legacy.id_itensmovprodutos_legacy
    FROM tmp_etl_candidate_itenscomprovantes ti
    JOIN LATERAL (
      SELECT
        i.id_itensmovprodutos AS id_itensmovprodutos_legacy
      FROM stg.movprodutos m
      JOIN stg.itensmovprodutos i
        ON i.id_empresa = m.id_empresa
       AND i.id_filial = m.id_filial
       AND i.id_db = m.id_db
       AND i.id_movprodutos = m.id_movprodutos
      WHERE m.id_empresa = ti.id_empresa
        AND m.id_filial = ti.id_filial
        AND m.id_db = ti.id_db
        AND (
          m.id_comprovante_shadow = ti.id_comprovante
          OR (
            m.id_comprovante_shadow IS NULL
            AND etl.safe_int(m.payload->>'ID_COMPROVANTE') = ti.id_comprovante
          )
        )
        AND (
          etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE') = ti.id_itemcomprovante
          OR i.id_itensmovprodutos = ti.id_itemcomprovante
        )
      ORDER BY
        COALESCE(i.received_at, i.ingested_at, i.dt_evento, m.received_at, m.ingested_at, m.dt_evento, now()) DESC,
        COALESCE(i.dt_evento, m.dt_evento, now()) DESC,
        i.id_itensmovprodutos DESC
      LIMIT 1
    ) legacy ON true
  )
  INSERT INTO etl.comprovante_item_bridge (
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    id_itemcomprovante,
    id_itensmovprodutos_legacy,
    source,
    updated_at
  )
  SELECT
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    id_itemcomprovante,
    id_itensmovprodutos_legacy,
    'legacy_itensmovprodutos_incremental',
    now()
  FROM bridge_candidates
  ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
  DO UPDATE SET
    id_itensmovprodutos_legacy = EXCLUDED.id_itensmovprodutos_legacy,
    source = EXCLUDED.source,
    updated_at = now()
  WHERE etl.comprovante_item_bridge.id_itensmovprodutos_legacy IS DISTINCT FROM EXCLUDED.id_itensmovprodutos_legacy;

  WITH current_sales AS (
    SELECT DISTINCT ON (fv.id_empresa, fv.id_filial, fv.id_db, fv.id_comprovante)
      fv.id_empresa,
      fv.id_filial,
      fv.id_db,
      fv.id_comprovante,
      fv.id_movprodutos,
      fv.data_key
    FROM dw.fact_venda fv
    JOIN (
      SELECT DISTINCT id_empresa, id_filial, id_db, id_comprovante
      FROM tmp_etl_candidate_itenscomprovantes
    ) tc
      ON tc.id_empresa = fv.id_empresa
     AND tc.id_filial = fv.id_filial
     AND tc.id_db = fv.id_db
     AND tc.id_comprovante = fv.id_comprovante
    WHERE fv.id_empresa = p_id_empresa
      AND fv.id_comprovante IS NOT NULL
    ORDER BY
      fv.id_empresa,
      fv.id_filial,
      fv.id_db,
      fv.id_comprovante,
      fv.id_movprodutos DESC
  ), src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      COALESCE(v.id_movprodutos, bdoc.id_movprodutos_legacy, i.id_comprovante) AS id_movprodutos,
      COALESCE(bitem.id_itensmovprodutos_legacy, i.id_itemcomprovante) AS id_itensmovprodutos,
      COALESCE(
        v.data_key,
        c.data_key,
        etl.business_date_key(i.dt_evento)
      ) AS data_key,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      COALESCE(i.id_grupo_produto_shadow, etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS')) AS id_grupo_produto,
      COALESCE(i.id_local_venda_shadow, etl.safe_int(i.payload->>'ID_LOCALVENDAS')) AS id_local_venda,
      COALESCE(i.id_funcionario_shadow, etl.safe_int(i.payload->>'ID_FUNCIONARIOS')) AS id_funcionario,
      COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP')) AS cfop,
      COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)) AS qtd,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS valor_unitario,
      COALESCE(i.total_shadow, etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2)) AS total,
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
    JOIN tmp_etl_candidate_itenscomprovantes ti
      ON ti.id_empresa = i.id_empresa
     AND ti.id_filial = i.id_filial
     AND ti.id_db = i.id_db
     AND ti.id_comprovante = i.id_comprovante
     AND ti.id_itemcomprovante = i.id_itemcomprovante
    LEFT JOIN current_sales v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_comprovante = i.id_comprovante
    LEFT JOIN dw.fact_comprovante c
      ON c.id_empresa = i.id_empresa
     AND c.id_filial = i.id_filial
     AND c.id_db = i.id_db
     AND c.id_comprovante = i.id_comprovante
    LEFT JOIN etl.comprovante_sales_bridge bdoc
      ON bdoc.id_empresa = i.id_empresa
     AND bdoc.id_filial = i.id_filial
     AND bdoc.id_db = i.id_db
     AND bdoc.id_comprovante = i.id_comprovante
    LEFT JOIN etl.comprovante_item_bridge bitem
      ON bitem.id_empresa = i.id_empresa
     AND bitem.id_filial = i.id_filial
     AND bitem.id_db = i.id_db
     AND bitem.id_comprovante = i.id_comprovante
     AND bitem.id_itemcomprovante = i.id_itemcomprovante
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,
      id_filial,
      id_db,
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
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
    DO UPDATE SET
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
    WHERE dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
      OR dw.fact_venda_item.desconto_total IS DISTINCT FROM EXCLUDED.desconto_total
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

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
  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;
