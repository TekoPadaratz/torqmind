BEGIN;

CREATE OR REPLACE FUNCTION etl.comprovante_is_cancelled(
  p_cancelado boolean,
  p_situacao integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_situacao = 2 THEN true
    WHEN p_situacao IN (3, 5) THEN false
    ELSE COALESCE(p_cancelado, false)
  END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_synced integer;
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
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS valor_total,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS raw_cancelado,
      COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')) AS situacao,
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
      data,
      data_key,
      id_usuario,
      id_turno,
      id_cliente,
      valor_total,
      etl.comprovante_is_cancelled(raw_cancelado, situacao) AS cancelado,
      situacao,
      data_conta,
      etl.comprovante_cash_eligible(data, data_conta, id_turno) AS cash_eligible,
      payload
    FROM base
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
  ), synced_venda_cancel AS (
    UPDATE dw.fact_venda v
    SET cancelado = s.cancelado
    FROM src s
    WHERE v.id_empresa = s.id_empresa
      AND v.id_filial = s.id_filial
      AND v.id_db = s.id_db
      AND v.id_comprovante = s.id_comprovante
      AND v.cancelado IS DISTINCT FROM s.cancelado
    RETURNING 1
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM upserted), 0),
    COALESCE((SELECT COUNT(*) FROM synced_venda_cancel), 0)
  INTO v_rows, v_synced;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
