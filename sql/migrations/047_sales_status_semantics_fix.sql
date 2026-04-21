BEGIN;

ALTER TABLE stg.movprodutos
  ADD COLUMN IF NOT EXISTS situacao_shadow integer;

ALTER TABLE dw.fact_venda
  ADD COLUMN IF NOT EXISTS situacao integer;

CREATE OR REPLACE FUNCTION etl.movimento_venda_situacao(
  p_situacao_shadow integer,
  p_payload jsonb
)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    p_situacao_shadow,
    etl.safe_int(p_payload->>'SITUACAO'),
    etl.safe_int(p_payload->>'situacao'),
    etl.safe_int(p_payload->>'STATUS'),
    etl.safe_int(p_payload->>'status')
  );
$$;

CREATE OR REPLACE FUNCTION etl.movimento_venda_is_cancelled(p_situacao integer)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_situacao = 2 THEN true
    ELSE false
  END;
$$;

UPDATE stg.movprodutos
SET situacao_shadow = etl.movimento_venda_situacao(situacao_shadow, payload)
WHERE situacao_shadow IS NULL;

UPDATE stg.comprovantes
SET situacao_shadow = COALESCE(
  situacao_shadow,
  etl.safe_int(payload->>'SITUACAO'),
  etl.safe_int(payload->>'situacao'),
  etl.safe_int(payload->>'STATUS'),
  etl.safe_int(payload->>'status')
)
WHERE situacao_shadow IS NULL;

WITH src AS (
  SELECT
    v.id_empresa,
    v.id_filial,
    v.id_db,
    v.id_movprodutos,
    COALESCE(
      etl.movimento_venda_situacao(m.situacao_shadow, m.payload),
      etl.movimento_venda_situacao(v.situacao, v.payload)
    ) AS situacao
  FROM dw.fact_venda v
  LEFT JOIN stg.movprodutos m
    ON m.id_empresa = v.id_empresa
   AND m.id_filial = v.id_filial
   AND m.id_db = v.id_db
   AND m.id_movprodutos = v.id_movprodutos
)
UPDATE dw.fact_venda v
SET
  situacao = src.situacao,
  cancelado = etl.movimento_venda_is_cancelled(src.situacao)
FROM src
WHERE v.id_empresa = src.id_empresa
  AND v.id_filial = src.id_filial
  AND v.id_db = src.id_db
  AND v.id_movprodutos = src.id_movprodutos
  AND (
    v.situacao IS DISTINCT FROM src.situacao
    OR v.cancelado IS DISTINCT FROM etl.movimento_venda_is_cancelled(src.situacao)
  );

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
    AND etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento)) >= v_cutoff
    AND (
      m.received_at > v_wm
      OR (m.dt_evento IS NOT NULL AND m.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH base AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      etl.sales_business_ts(m.payload, m.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(m.payload, m.dt_evento)) AS data_key,
      COALESCE(m.id_usuario_shadow, etl.safe_int(m.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(m.id_cliente_shadow, etl.safe_int(m.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) AS id_comprovante,
      COALESCE(m.id_turno_shadow, etl.safe_int(m.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(m.saidas_entradas_shadow, etl.safe_int(m.payload->>'SAIDAS_ENTRADAS')) AS saidas_entradas,
      COALESCE(m.total_venda_shadow, etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2)) AS total_venda,
      etl.movimento_venda_situacao(m.situacao_shadow, m.payload) AS situacao,
      m.payload
    FROM stg.movprodutos m
    JOIN tmp_etl_candidate_movimentos tm
      ON tm.id_empresa = m.id_empresa
     AND tm.id_filial = m.id_filial
     AND tm.id_db = m.id_db
     AND tm.id_movprodutos = m.id_movprodutos
  ), src AS (
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
      etl.movimento_venda_is_cancelled(situacao) AS cancelado,
      payload
    FROM base
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,situacao,cancelado,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,situacao,cancelado,payload
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
      situacao = EXCLUDED.situacao,
      cancelado = EXCLUDED.cancelado,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
      OR dw.fact_venda.id_comprovante IS DISTINCT FROM EXCLUDED.id_comprovante
      OR dw.fact_venda.situacao IS DISTINCT FROM EXCLUDED.situacao
      OR dw.fact_venda.cancelado IS DISTINCT FROM EXCLUDED.cancelado
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.movprodutos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'movprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_bridge_rows integer;
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
    AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) >= v_cutoff
    AND (
      c.received_at > v_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH base AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      c.id_comprovante,
      c.referencia_shadow AS referencia,
      c.received_at AS source_received_at,
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.sales_event_timestamptz(c.payload, c.dt_evento) AS data_comp,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS valor_total,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS raw_cancelado,
      COALESCE(
        c.situacao_shadow,
        etl.safe_int(c.payload->>'SITUACAO'),
        etl.safe_int(c.payload->>'situacao'),
        etl.safe_int(c.payload->>'STATUS'),
        etl.safe_int(c.payload->>'status')
      ) AS situacao,
      etl.comprovante_data_conta(c.payload, NULL) AS data_conta,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
  ), src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      referencia,
      source_received_at,
      data,
      data_comp,
      data_key,
      id_usuario,
      id_turno,
      id_cliente,
      valor_total,
      etl.comprovante_is_cancelled(raw_cancelado, situacao) AS cancelado,
      situacao,
      data_conta,
      etl.comprovante_cash_eligible(data, data_conta, id_turno) AS cash_eligible,
      etl.pagamento_comprovante_bridge_hash(
        id_comprovante,
        id_db,
        id_turno,
        id_usuario,
        data_comp,
        data_conta,
        etl.comprovante_cash_eligible(data, data_conta, id_turno)
      ) AS bridge_source_hash,
      payload
    FROM base
  ), src_bridge AS (
    SELECT DISTINCT ON (id_empresa, id_filial, referencia)
      id_empresa,
      id_filial,
      referencia,
      id_comprovante,
      id_db,
      id_turno,
      id_usuario,
      data_comp,
      data_conta,
      cash_eligible,
      source_received_at,
      bridge_source_hash
    FROM src
    WHERE referencia IS NOT NULL
    ORDER BY id_empresa, id_filial, referencia, source_received_at DESC, id_db DESC, id_comprovante DESC
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,data_conta,cash_eligible,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,data_conta,cash_eligible,payload
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
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_comprovante.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_comprovante.situacao IS DISTINCT FROM EXCLUDED.situacao
      OR dw.fact_comprovante.valor_total IS DISTINCT FROM EXCLUDED.valor_total
      OR dw.fact_comprovante.data_conta IS DISTINCT FROM EXCLUDED.data_conta
      OR dw.fact_comprovante.cash_eligible IS DISTINCT FROM EXCLUDED.cash_eligible
    RETURNING 1
  ), upserted_bridge AS (
    INSERT INTO etl.pagamento_comprovante_bridge (
      id_empresa,
      id_filial,
      referencia,
      id_comprovante,
      id_db,
      id_turno,
      id_usuario,
      data_comp,
      data_conta,
      cash_eligible,
      source_received_at,
      source_hash,
      updated_at
    )
    SELECT
      id_empresa,
      id_filial,
      referencia,
      id_comprovante,
      id_db,
      id_turno,
      id_usuario,
      data_comp,
      data_conta,
      cash_eligible,
      source_received_at,
      bridge_source_hash,
      now()
    FROM src_bridge
    ON CONFLICT (id_empresa, id_filial, referencia)
    DO UPDATE SET
      id_comprovante = EXCLUDED.id_comprovante,
      id_db = EXCLUDED.id_db,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      data_comp = EXCLUDED.data_comp,
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      source_received_at = EXCLUDED.source_received_at,
      source_hash = EXCLUDED.source_hash,
      updated_at = now()
    WHERE etl.pagamento_comprovante_bridge.source_hash IS DISTINCT FROM EXCLUDED.source_hash
    RETURNING 1
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM upserted), 0),
    COALESCE((SELECT COUNT(*) FROM upserted_bridge), 0)
  INTO v_rows, v_bridge_rows;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
