BEGIN;

ALTER TABLE stg.comprovantes
  ADD COLUMN IF NOT EXISTS referencia_shadow bigint,
  ADD COLUMN IF NOT EXISTS id_usuario_shadow integer,
  ADD COLUMN IF NOT EXISTS id_turno_shadow integer,
  ADD COLUMN IF NOT EXISTS id_cliente_shadow integer,
  ADD COLUMN IF NOT EXISTS valor_total_shadow numeric(18,2),
  ADD COLUMN IF NOT EXISTS cancelado_shadow boolean,
  ADD COLUMN IF NOT EXISTS situacao_shadow integer;

ALTER TABLE stg.movprodutos
  ADD COLUMN IF NOT EXISTS id_comprovante_shadow integer,
  ADD COLUMN IF NOT EXISTS id_usuario_shadow integer,
  ADD COLUMN IF NOT EXISTS id_turno_shadow integer,
  ADD COLUMN IF NOT EXISTS id_cliente_shadow integer,
  ADD COLUMN IF NOT EXISTS saidas_entradas_shadow integer,
  ADD COLUMN IF NOT EXISTS total_venda_shadow numeric(18,2);

ALTER TABLE stg.itensmovprodutos
  ADD COLUMN IF NOT EXISTS id_produto_shadow integer,
  ADD COLUMN IF NOT EXISTS id_grupo_produto_shadow integer,
  ADD COLUMN IF NOT EXISTS id_local_venda_shadow integer,
  ADD COLUMN IF NOT EXISTS id_funcionario_shadow integer,
  ADD COLUMN IF NOT EXISTS cfop_shadow integer,
  ADD COLUMN IF NOT EXISTS qtd_shadow numeric(18,3),
  ADD COLUMN IF NOT EXISTS valor_unitario_shadow numeric(18,4),
  ADD COLUMN IF NOT EXISTS total_shadow numeric(18,2),
  ADD COLUMN IF NOT EXISTS desconto_shadow numeric(18,2),
  ADD COLUMN IF NOT EXISTS custo_unitario_shadow numeric(18,6);

ALTER TABLE stg.formas_pgto_comprovantes
  ADD COLUMN IF NOT EXISTS valor_shadow numeric(18,2),
  ADD COLUMN IF NOT EXISTS nsu_shadow text,
  ADD COLUMN IF NOT EXISTS autorizacao_shadow text,
  ADD COLUMN IF NOT EXISTS bandeira_shadow text,
  ADD COLUMN IF NOT EXISTS rede_shadow text,
  ADD COLUMN IF NOT EXISTS tef_shadow text;

ALTER TABLE dw.fact_venda_item
  ADD COLUMN IF NOT EXISTS preco_lista_unitario numeric(18,4),
  ADD COLUMN IF NOT EXISTS preco_praticado_unitario numeric(18,4),
  ADD COLUMN IF NOT EXISTS desconto_unitario numeric(18,4),
  ADD COLUMN IF NOT EXISTS desconto_total numeric(18,2),
  ADD COLUMN IF NOT EXISTS discount_source text;

UPDATE stg.comprovantes
SET
  referencia_shadow = COALESCE(referencia_shadow, etl.safe_int(payload->>'REFERENCIA')),
  id_usuario_shadow = COALESCE(id_usuario_shadow, etl.safe_int(payload->>'ID_USUARIOS')),
  id_turno_shadow = COALESCE(id_turno_shadow, etl.safe_int(payload->>'ID_TURNOS')),
  id_cliente_shadow = COALESCE(id_cliente_shadow, etl.safe_int(payload->>'ID_ENTIDADE')),
  valor_total_shadow = COALESCE(valor_total_shadow, etl.safe_numeric(payload->>'VLRTOTAL')::numeric(18,2)),
  cancelado_shadow = COALESCE(cancelado_shadow, etl.to_bool(payload->>'CANCELADO')),
  situacao_shadow = COALESCE(situacao_shadow, etl.safe_int(payload->>'SITUACAO'))
WHERE referencia_shadow IS NULL
   OR id_usuario_shadow IS NULL
   OR id_turno_shadow IS NULL
   OR id_cliente_shadow IS NULL
   OR valor_total_shadow IS NULL
   OR cancelado_shadow IS NULL
   OR situacao_shadow IS NULL;

UPDATE stg.movprodutos
SET
  id_comprovante_shadow = COALESCE(id_comprovante_shadow, etl.safe_int(payload->>'ID_COMPROVANTE')),
  id_usuario_shadow = COALESCE(id_usuario_shadow, etl.safe_int(payload->>'ID_USUARIOS')),
  id_turno_shadow = COALESCE(id_turno_shadow, etl.safe_int(payload->>'ID_TURNOS')),
  id_cliente_shadow = COALESCE(id_cliente_shadow, etl.safe_int(payload->>'ID_ENTIDADE')),
  saidas_entradas_shadow = COALESCE(saidas_entradas_shadow, etl.safe_int(payload->>'SAIDAS_ENTRADAS')),
  total_venda_shadow = COALESCE(total_venda_shadow, etl.safe_numeric(payload->>'TOTALVENDA')::numeric(18,2))
WHERE id_comprovante_shadow IS NULL
   OR id_usuario_shadow IS NULL
   OR id_turno_shadow IS NULL
   OR id_cliente_shadow IS NULL
   OR saidas_entradas_shadow IS NULL
   OR total_venda_shadow IS NULL;

UPDATE stg.itensmovprodutos
SET
  id_produto_shadow = COALESCE(id_produto_shadow, etl.safe_int(payload->>'ID_PRODUTOS')),
  id_grupo_produto_shadow = COALESCE(id_grupo_produto_shadow, etl.safe_int(payload->>'ID_GRUPOPRODUTOS')),
  id_local_venda_shadow = COALESCE(id_local_venda_shadow, etl.safe_int(payload->>'ID_LOCALVENDAS')),
  id_funcionario_shadow = COALESCE(id_funcionario_shadow, etl.safe_int(payload->>'ID_FUNCIONARIOS')),
  cfop_shadow = COALESCE(cfop_shadow, etl.safe_int(payload->>'CFOP')),
  qtd_shadow = COALESCE(qtd_shadow, etl.safe_numeric(payload->>'QTDE')::numeric(18,3)),
  valor_unitario_shadow = COALESCE(valor_unitario_shadow, etl.safe_numeric(payload->>'VLRUNITARIO')::numeric(18,4)),
  total_shadow = COALESCE(total_shadow, etl.safe_numeric(payload->>'TOTAL')::numeric(18,2)),
  desconto_shadow = COALESCE(desconto_shadow, etl.safe_numeric(payload->>'VLRDESCONTO')::numeric(18,2)),
  custo_unitario_shadow = COALESCE(custo_unitario_shadow, etl.safe_numeric(payload->>'VLRCUSTO')::numeric(18,6))
WHERE id_produto_shadow IS NULL
   OR id_grupo_produto_shadow IS NULL
   OR id_local_venda_shadow IS NULL
   OR id_funcionario_shadow IS NULL
   OR cfop_shadow IS NULL
   OR qtd_shadow IS NULL
   OR valor_unitario_shadow IS NULL
   OR total_shadow IS NULL
   OR desconto_shadow IS NULL
   OR custo_unitario_shadow IS NULL;

UPDATE stg.formas_pgto_comprovantes
SET
  valor_shadow = COALESCE(
    valor_shadow,
    COALESCE(
      etl.safe_numeric(payload->>'VALOR'),
      etl.safe_numeric(payload->>'VALOR_PAGO'),
      etl.safe_numeric(payload->>'VALORPAGO'),
      etl.safe_numeric(payload->>'VLR'),
      etl.safe_numeric(payload->>'VLR_PAGO'),
      etl.safe_numeric(payload->>'VLRPAGO')
    )::numeric(18,2)
  ),
  nsu_shadow = COALESCE(nsu_shadow, payload->>'NSU', payload->>'nsu'),
  autorizacao_shadow = COALESCE(autorizacao_shadow, payload->>'AUTORIZACAO', payload->>'autorizacao'),
  bandeira_shadow = COALESCE(bandeira_shadow, payload->>'BANDEIRA', payload->>'bandeira'),
  rede_shadow = COALESCE(rede_shadow, payload->>'REDE', payload->>'rede'),
  tef_shadow = COALESCE(tef_shadow, payload->>'TEF', payload->>'tef')
WHERE valor_shadow IS NULL
   OR nsu_shadow IS NULL
   OR autorizacao_shadow IS NULL
   OR bandeira_shadow IS NULL
   OR rede_shadow IS NULL
   OR tef_shadow IS NULL;

DROP INDEX IF EXISTS ix_stg_comprovantes_emp_filial_referencia;
CREATE INDEX IF NOT EXISTS ix_stg_comprovantes_emp_filial_ref_shadow
  ON stg.comprovantes (id_empresa, id_filial, referencia_shadow)
  WHERE referencia_shadow IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_stg_movprodutos_emp_filial_db_comp_shadow
  ON stg.movprodutos (id_empresa, id_filial, id_db, id_comprovante_shadow)
  WHERE id_comprovante_shadow IS NOT NULL;

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_comprovantes;
  CREATE TEMP TABLE tmp_etl_candidate_comprovantes (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_comprovante int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_comprovantes
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND COALESCE(c.dt_evento::date, etl.sales_business_ts(c.payload, c.dt_evento)::date) >= v_cutoff
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
      c.id_comprovante,
      COALESCE(c.dt_evento, etl.sales_business_ts(c.payload, c.dt_evento)) AS data,
      etl.date_key(COALESCE(c.dt_evento, etl.sales_business_ts(c.payload, c.dt_evento))) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS valor_total,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS cancelado,
      COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')) AS situacao,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
    DO UPDATE SET
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_turno = EXCLUDED.id_turno,
      id_cliente = EXCLUDED.id_cliente,
      valor_total = EXCLUDED.valor_total,
      cancelado = EXCLUDED.cancelado,
      situacao = EXCLUDED.situacao,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_comprovante.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_comprovante.valor_total IS DISTINCT FROM EXCLUDED.valor_total
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_movimentos;
  CREATE TEMP TABLE tmp_etl_candidate_movimentos (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_movprodutos int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_movimentos
  SELECT
    m.id_empresa,
    m.id_filial,
    m.id_db,
    m.id_movprodutos
  FROM stg.movprodutos m
  WHERE m.id_empresa = p_id_empresa
    AND COALESCE(m.dt_evento::date, etl.sales_business_ts(m.payload, m.dt_evento)::date) >= v_cutoff
    AND (
      m.received_at > v_wm
      OR (m.dt_evento IS NOT NULL AND m.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH src AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      COALESCE(m.dt_evento, etl.sales_business_ts(m.payload, m.dt_evento)) AS data,
      etl.date_key(COALESCE(m.dt_evento, etl.sales_business_ts(m.payload, m.dt_evento))) AS data_key,
      COALESCE(m.id_usuario_shadow, etl.safe_int(m.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(m.id_cliente_shadow, etl.safe_int(m.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) AS id_comprovante,
      COALESCE(m.id_turno_shadow, etl.safe_int(m.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(m.saidas_entradas_shadow, etl.safe_int(m.payload->>'SAIDAS_ENTRADAS')) AS saidas_entradas,
      COALESCE(m.total_venda_shadow, etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2)) AS total_venda,
      m.payload
    FROM stg.movprodutos m
    JOIN tmp_etl_candidate_movimentos tm
      ON tm.id_empresa = m.id_empresa
     AND tm.id_filial = m.id_filial
     AND tm.id_db = m.id_db
     AND tm.id_movprodutos = m.id_movprodutos
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
    DO UPDATE SET
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_cliente = EXCLUDED.id_cliente,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      saidas_entradas = EXCLUDED.saidas_entradas,
      total_venda = EXCLUDED.total_venda,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
      OR dw.fact_venda.id_comprovante IS DISTINCT FROM EXCLUDED.id_comprovante
    RETURNING id_empresa,id_filial,id_db,id_movprodutos,id_comprovante
  ), updated_cancel AS (
    UPDATE dw.fact_venda v
    SET cancelado = c.cancelado
    FROM dw.fact_comprovante c, upserted u
    WHERE u.id_empresa = v.id_empresa
      AND u.id_filial = v.id_filial
      AND u.id_db = v.id_db
      AND u.id_movprodutos = v.id_movprodutos
      AND c.id_empresa = v.id_empresa
      AND c.id_filial = v.id_filial
      AND c.id_db = v.id_db
      AND c.id_comprovante = v.id_comprovante
      AND v.cancelado IS DISTINCT FROM c.cancelado
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM upserted), 0) + COALESCE((SELECT COUNT(*) FROM updated_cancel), 0)
  INTO v_rows;

  SELECT MAX(received_at) INTO v_max
  FROM stg.movprodutos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'movprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itensmovprodutos'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_movimentos;
  CREATE TEMP TABLE tmp_etl_candidate_movimentos (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_movprodutos int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_movimentos
  SELECT
    m.id_empresa,
    m.id_filial,
    m.id_db,
    m.id_movprodutos
  FROM stg.movprodutos m
  WHERE m.id_empresa = p_id_empresa
    AND COALESCE(m.dt_evento::date, etl.sales_business_ts(m.payload, m.dt_evento)::date) >= v_cutoff
    AND (
      m.received_at > COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz)
      OR (m.dt_evento IS NOT NULL AND m.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  DROP TABLE IF EXISTS tmp_etl_candidate_itens;
  CREATE TEMP TABLE tmp_etl_candidate_itens (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_movprodutos int NOT NULL,
    id_itensmovprodutos int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_itens
  SELECT
    i.id_empresa,
    i.id_filial,
    i.id_db,
    i.id_movprodutos,
    i.id_itensmovprodutos
  FROM stg.itensmovprodutos i
  LEFT JOIN stg.movprodutos m
    ON m.id_empresa = i.id_empresa
   AND m.id_filial = i.id_filial
   AND m.id_db = i.id_db
   AND m.id_movprodutos = i.id_movprodutos
  WHERE i.id_empresa = p_id_empresa
    AND COALESCE(i.dt_evento::date, m.dt_evento::date, etl.sales_business_ts(m.payload, m.dt_evento)::date) >= v_cutoff
    AND (
      i.received_at > v_wm
      OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  INSERT INTO tmp_etl_candidate_itens
  SELECT
    i.id_empresa,
    i.id_filial,
    i.id_db,
    i.id_movprodutos,
    i.id_itensmovprodutos
  FROM stg.itensmovprodutos i
  JOIN tmp_etl_candidate_movimentos tm
    ON tm.id_empresa = i.id_empresa
   AND tm.id_filial = i.id_filial
   AND tm.id_db = i.id_db
   AND tm.id_movprodutos = i.id_movprodutos
  ON CONFLICT DO NOTHING;

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      i.id_itensmovprodutos,
      COALESCE(
        v.data_key,
        etl.date_key(COALESCE(i.dt_evento, m.dt_evento, etl.sales_business_ts(m.payload, m.dt_evento)))
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
          COALESCE(i.custo_unitario_shadow, etl.safe_numeric(i.payload->>'VLRCUSTO')::numeric(18,6))
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2),
        (dp.custo_medio * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6)))::numeric(18,2)
      ) AS custo_total,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS preco_praticado_unitario,
      NULL::numeric(18,4) AS preco_lista_unitario,
      CASE
        WHEN COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')) > 0
          THEN (
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
    FROM stg.itensmovprodutos i
    JOIN tmp_etl_candidate_itens ti
      ON ti.id_empresa = i.id_empresa
     AND ti.id_filial = i.id_filial
     AND ti.id_db = i.id_db
     AND ti.id_movprodutos = i.id_movprodutos
     AND ti.id_itensmovprodutos = i.id_itensmovprodutos
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_movprodutos = i.id_movprodutos
    LEFT JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa
     AND m.id_filial = i.id_filial
     AND m.id_db = i.id_db
     AND m.id_movprodutos = i.id_movprodutos
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,margem,
      preco_lista_unitario,preco_praticado_unitario,desconto_unitario,desconto_total,discount_source,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,(COALESCE(total,0) - COALESCE(custo_total,0))::numeric(18,2),
      preco_lista_unitario,preco_praticado_unitario,desconto_unitario,desconto_total,discount_source,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
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
    WHERE
      dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
      OR dw.fact_venda_item.desconto_total IS DISTINCT FROM EXCLUDED.desconto_total
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.itensmovprodutos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'itensmovprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_comp_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);
  v_comp_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_pag_refs;
  CREATE TEMP TABLE tmp_etl_candidate_pag_refs (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    referencia bigint NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, referencia)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_pag_refs
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_referencia
  FROM stg.formas_pgto_comprovantes s
  WHERE s.id_empresa = p_id_empresa
    AND COALESCE(s.dt_evento::date, CURRENT_DATE) >= v_cutoff
    AND (
      s.received_at > v_wm
      OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  INSERT INTO tmp_etl_candidate_pag_refs
  SELECT
    c.id_empresa,
    c.id_filial,
    c.referencia_shadow
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND c.referencia_shadow IS NOT NULL
    AND COALESCE(c.dt_evento::date, etl.sales_business_ts(c.payload, c.dt_evento)::date) >= v_cutoff
    AND (
      c.received_at > v_comp_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH src_raw AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_referencia AS referencia,
      s.tipo_forma AS tipo_forma,
      COALESCE(
        s.id_db_shadow,
        etl.safe_int(s.payload->>'ID_DB'),
        etl.safe_int(s.payload->>'id_db')
      ) AS id_db,
      COALESCE(
        s.valor_shadow,
        etl.safe_numeric(s.payload->>'VALOR')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VALOR_PAGO')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VALORPAGO')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VLR')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VLR_PAGO')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VLRPAGO')::numeric(18,2),
        0::numeric(18,2)
      ) AS valor,
      COALESCE(
        s.dt_evento,
        etl.safe_timestamp(s.payload->>'TORQMIND_DT_EVENTO'),
        etl.safe_timestamp(s.payload->>'DT_EVENTO'),
        etl.safe_timestamp(s.payload->>'DATAHORA'),
        etl.safe_timestamp(s.payload->>'DATA')
      ) AS dt_evento_src,
      COALESCE(s.nsu_shadow, s.payload->>'NSU', s.payload->>'nsu') AS nsu,
      COALESCE(s.autorizacao_shadow, s.payload->>'AUTORIZACAO', s.payload->>'autorizacao') AS autorizacao,
      COALESCE(s.bandeira_shadow, s.payload->>'BANDEIRA', s.payload->>'bandeira') AS bandeira,
      COALESCE(s.rede_shadow, s.payload->>'REDE', s.payload->>'rede') AS rede,
      COALESCE(s.tef_shadow, s.payload->>'TEF', s.payload->>'tef') AS tef,
      s.payload,
      s.received_at
    FROM stg.formas_pgto_comprovantes s
    JOIN tmp_etl_candidate_pag_refs r
      ON r.id_empresa = s.id_empresa
     AND r.id_filial = s.id_filial
     AND r.referencia = s.id_referencia
    WHERE s.id_empresa = p_id_empresa
  ), comp_ref AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.referencia_shadow AS referencia,
      c.id_comprovante AS id_comprovante,
      c.id_db AS id_db,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.dt_evento, etl.sales_business_ts(c.payload, c.dt_evento)) AS data_comp,
      row_number() OVER (
        PARTITION BY c.id_empresa, c.id_filial, c.referencia_shadow
        ORDER BY c.received_at DESC
      ) AS rn
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_pag_refs r
      ON r.id_empresa = c.id_empresa
     AND r.id_filial = c.id_filial
     AND r.referencia = c.referencia_shadow
    WHERE c.id_empresa = p_id_empresa
      AND c.referencia_shadow IS NOT NULL
  ), src AS (
    SELECT
      r.id_empresa,
      r.id_filial,
      r.referencia,
      COALESCE(cr.id_db, r.id_db) AS id_db,
      cr.id_comprovante,
      cr.id_turno,
      cr.id_usuario,
      r.tipo_forma,
      r.valor,
      COALESCE(cr.data_comp, r.dt_evento_src, r.received_at) AS dt_evento,
      etl.date_key(COALESCE(cr.data_comp, r.dt_evento_src, r.received_at)) AS data_key,
      r.nsu,
      r.autorizacao,
      r.bandeira,
      r.rede,
      r.tef,
      r.payload
    FROM src_raw r
    LEFT JOIN comp_ref cr
      ON cr.id_empresa = r.id_empresa
     AND cr.id_filial = r.id_filial
     AND cr.referencia = r.referencia
     AND cr.rn = 1
    WHERE COALESCE(cr.data_comp::date, r.dt_evento_src::date, CURRENT_DATE) >= v_cutoff
  ), upserted AS (
    INSERT INTO dw.fact_pagamento_comprovante (
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      nsu,autorizacao,bandeira,rede,tef,payload
    )
    SELECT
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      nsu,autorizacao,bandeira,rede,tef,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,referencia,tipo_forma)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      valor = EXCLUDED.valor,
      dt_evento = EXCLUDED.dt_evento,
      data_key = EXCLUDED.data_key,
      nsu = EXCLUDED.nsu,
      autorizacao = EXCLUDED.autorizacao,
      bandeira = EXCLUDED.bandeira,
      rede = EXCLUDED.rede,
      tef = EXCLUDED.tef,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.fact_pagamento_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_pagamento_comprovante.valor IS DISTINCT FROM EXCLUDED.valor
      OR dw.fact_pagamento_comprovante.dt_evento IS DISTINCT FROM EXCLUDED.dt_evento
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.formas_pgto_comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'formas_pgto_comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
