ALTER TABLE dw.fact_venda_item
  ADD COLUMN IF NOT EXISTS id_comprovante integer,
  ADD COLUMN IF NOT EXISTS id_itemcomprovante integer;

CREATE OR REPLACE FUNCTION etl.fact_venda_sync_canonical_keys()
RETURNS trigger AS $$
BEGIN
  IF NEW.id_comprovante IS NULL AND NEW.id_movprodutos IS NOT NULL THEN
    NEW.id_comprovante := NEW.id_movprodutos;
  END IF;
  IF NEW.id_movprodutos IS NULL AND NEW.id_comprovante IS NOT NULL THEN
    NEW.id_movprodutos := NEW.id_comprovante;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.fact_venda_item_sync_canonical_keys()
RETURNS trigger AS $$
DECLARE
  v_doc_key integer;
BEGIN
  IF NEW.id_comprovante IS NULL AND NEW.id_movprodutos IS NOT NULL THEN
    SELECT v.id_comprovante
    INTO v_doc_key
    FROM dw.fact_venda v
    WHERE v.id_empresa = NEW.id_empresa
      AND v.id_filial = NEW.id_filial
      AND v.id_db = NEW.id_db
      AND v.id_movprodutos = NEW.id_movprodutos
    ORDER BY v.updated_at DESC NULLS LAST, v.created_at DESC NULLS LAST
    LIMIT 1;
    NEW.id_comprovante := COALESCE(v_doc_key, NEW.id_movprodutos);
  END IF;
  IF NEW.id_movprodutos IS NULL AND NEW.id_comprovante IS NOT NULL THEN
    NEW.id_movprodutos := NEW.id_comprovante;
  END IF;

  IF NEW.id_itemcomprovante IS NULL AND NEW.id_itensmovprodutos IS NOT NULL THEN
    NEW.id_itemcomprovante := NEW.id_itensmovprodutos;
  END IF;
  IF NEW.id_itensmovprodutos IS NULL AND NEW.id_itemcomprovante IS NOT NULL THEN
    NEW.id_itensmovprodutos := NEW.id_itemcomprovante;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dw_fact_venda_sync_canonical_keys ON dw.fact_venda;
CREATE TRIGGER trg_dw_fact_venda_sync_canonical_keys
BEFORE INSERT OR UPDATE ON dw.fact_venda
FOR EACH ROW EXECUTE FUNCTION etl.fact_venda_sync_canonical_keys();

DROP TRIGGER IF EXISTS trg_dw_fact_venda_item_sync_canonical_keys ON dw.fact_venda_item;
CREATE TRIGGER trg_dw_fact_venda_item_sync_canonical_keys
BEFORE INSERT OR UPDATE ON dw.fact_venda_item
FOR EACH ROW EXECUTE FUNCTION etl.fact_venda_item_sync_canonical_keys();

UPDATE dw.fact_venda
SET id_comprovante = COALESCE(id_comprovante, id_movprodutos)
WHERE id_comprovante IS NULL;

UPDATE dw.fact_venda_item i
SET
  id_comprovante = COALESCE(i.id_comprovante, v.id_comprovante, i.id_movprodutos),
  id_itemcomprovante = COALESCE(
    i.id_itemcomprovante,
    etl.safe_int(i.payload->>'ID_ITENSCOMPROVANTE'),
    etl.safe_int(i.payload->>'ID_ITENS_COMPROVANTE'),
    etl.safe_int(i.payload->>'ID_ITEMCOMPROVANTE'),
    i.id_itensmovprodutos
  )
FROM dw.fact_venda v
WHERE v.id_empresa = i.id_empresa
  AND v.id_filial = i.id_filial
  AND v.id_db = i.id_db
  AND v.id_movprodutos = i.id_movprodutos
  AND (i.id_comprovante IS NULL OR i.id_itemcomprovante IS NULL);

UPDATE dw.fact_venda_item
SET
  id_comprovante = COALESCE(id_comprovante, id_movprodutos),
  id_itemcomprovante = COALESCE(
    id_itemcomprovante,
    etl.safe_int(payload->>'ID_ITENSCOMPROVANTE'),
    etl.safe_int(payload->>'ID_ITENS_COMPROVANTE'),
    etl.safe_int(payload->>'ID_ITEMCOMPROVANTE'),
    id_itensmovprodutos
  )
WHERE id_comprovante IS NULL
   OR id_itemcomprovante IS NULL;

WITH ranked AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY id_empresa, id_filial, id_db, id_comprovante
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id_movprodutos DESC
    ) AS rn
  FROM dw.fact_venda
)
DELETE FROM dw.fact_venda d
USING ranked r
WHERE d.ctid = r.ctid
  AND r.rn > 1;

WITH ranked AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id_movprodutos DESC, id_itensmovprodutos DESC
    ) AS rn
  FROM dw.fact_venda_item
)
DELETE FROM dw.fact_venda_item d
USING ranked r
WHERE d.ctid = r.ctid
  AND r.rn > 1;

UPDATE dw.fact_venda
SET id_movprodutos = -id_comprovante
WHERE id_movprodutos IS DISTINCT FROM -id_comprovante;

UPDATE dw.fact_venda
SET id_movprodutos = id_comprovante
WHERE id_movprodutos < 0;

UPDATE dw.fact_venda_item
SET
  id_movprodutos = -id_comprovante,
  id_itensmovprodutos = -id_itemcomprovante
WHERE id_movprodutos IS DISTINCT FROM -id_comprovante
   OR id_itensmovprodutos IS DISTINCT FROM -id_itemcomprovante;

UPDATE dw.fact_venda_item
SET
  id_movprodutos = id_comprovante,
  id_itensmovprodutos = id_itemcomprovante
WHERE id_movprodutos < 0
   OR id_itensmovprodutos < 0;

ALTER TABLE dw.fact_venda
  ALTER COLUMN id_comprovante SET NOT NULL;

ALTER TABLE dw.fact_venda_item
  ALTER COLUMN id_comprovante SET NOT NULL,
  ALTER COLUMN id_itemcomprovante SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_venda_canonical_doc
  ON dw.fact_venda (id_empresa, id_filial, id_db, id_comprovante);

CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_venda_item_canonical_doc_item
  ON dw.fact_venda_item (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante);

CREATE INDEX IF NOT EXISTS ix_fact_venda_item_join_comprovante
  ON dw.fact_venda_item (id_empresa, id_filial, id_db, id_comprovante)
  INCLUDE (
    id_itemcomprovante,
    total,
    cfop,
    margem,
    qtd,
    id_produto,
    id_grupo_produto,
    id_funcionario,
    updated_at,
    created_at
  );

CREATE INDEX IF NOT EXISTS ix_fact_venda_item_live_overlay_canonical
  ON dw.fact_venda_item (id_empresa, id_filial, id_db, id_comprovante)
  INCLUDE (
    id_itemcomprovante,
    cfop,
    total,
    margem,
    qtd,
    id_produto,
    id_grupo_produto,
    id_funcionario,
    updated_at,
    created_at
  )
  WHERE COALESCE(cfop, 0) >= 5000;

ANALYZE dw.fact_venda;
ANALYZE dw.fact_venda_item;

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

  WITH src AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      c.id_comprovante AS id_movprodutos,
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
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
    DO UPDATE SET
      id_movprodutos = EXCLUDED.id_movprodutos,
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_cliente = EXCLUDED.id_cliente,
      id_turno = EXCLUDED.id_turno,
      saidas_entradas = EXCLUDED.saidas_entradas,
      total_venda = EXCLUDED.total_venda,
      situacao = EXCLUDED.situacao,
      cancelado = EXCLUDED.cancelado,
      payload = EXCLUDED.payload
    WHERE dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
      OR dw.fact_venda.id_movprodutos IS DISTINCT FROM EXCLUDED.id_movprodutos
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

  WITH src AS (
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
    FROM src
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
    WHERE dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.id_movprodutos IS DISTINCT FROM EXCLUDED.id_movprodutos
      OR dw.fact_venda_item.id_itensmovprodutos IS DISTINCT FROM EXCLUDED.id_itensmovprodutos
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
