-- Keep dw.fact_venda.cancelado aligned with dw.fact_comprovante even when only comprovante changes.
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
    RETURNING 1
  ), updated_cancel AS (
    UPDATE dw.fact_venda v
    SET cancelado = c.cancelado
    FROM dw.fact_comprovante c
    WHERE v.id_empresa = p_id_empresa
      AND c.id_empresa = v.id_empresa
      AND c.id_filial = v.id_filial
      AND c.id_db = v.id_db
      AND v.id_comprovante IS NOT NULL
      AND c.id_comprovante = v.id_comprovante
      AND COALESCE(v.data::date, c.data::date) >= v_cutoff
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
