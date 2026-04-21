BEGIN;

INSERT INTO app.payment_type_map (id_empresa, tipo_forma, label, category, severity_hint, active)
VALUES
  (NULL, 0, 'DINHEIRO', 'DINHEIRO', 'INFO', true)
ON CONFLICT (id_empresa_nk, tipo_forma)
DO UPDATE SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  severity_hint = EXCLUDED.severity_hint,
  active = EXCLUDED.active,
  updated_at = now();

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);

  WITH src_raw AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_referencia AS referencia,
      etl.safe_int(s.payload->>'TIPO_FORMA') AS tipo_forma,
      COALESCE(
        etl.safe_int(s.payload->>'ID_DB'),
        etl.safe_int(s.payload->>'id_db'),
        etl.safe_int(s.id_db_shadow::text)
      ) AS id_db,
      COALESCE(
        etl.safe_numeric(s.payload->>'VALOR'),
        etl.safe_numeric(s.payload->>'VALOR_PAGO'),
        etl.safe_numeric(s.payload->>'VALORPAGO'),
        etl.safe_numeric(s.payload->>'VLR'),
        etl.safe_numeric(s.payload->>'VLR_PAGO'),
        etl.safe_numeric(s.payload->>'VLRPAGO'),
        0
      )::numeric(18,2) AS valor,
      COALESCE(
        s.dt_evento,
        etl.safe_timestamp(s.payload->>'DATAREPL'),
        etl.safe_timestamp(s.payload->>'DATAHORA'),
        etl.safe_timestamp(s.payload->>'DATA')
      ) AS dt_evento_src,
      COALESCE(s.payload->>'NSU', s.payload->>'nsu') AS nsu,
      COALESCE(s.payload->>'AUTORIZACAO', s.payload->>'autorizacao', s.payload->>'COD_AUTORIZACAO') AS autorizacao,
      COALESCE(s.payload->>'BANDEIRA', s.payload->>'bandeira') AS bandeira,
      COALESCE(s.payload->>'REDE', s.payload->>'rede') AS rede,
      COALESCE(s.payload->>'TEF', s.payload->>'tef') AS tef,
      s.payload,
      s.received_at
    FROM stg.formas_pgto_comprovantes s
    WHERE s.id_empresa = p_id_empresa
      AND (
        s.received_at > v_wm
        OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), src_refs AS (
    SELECT DISTINCT id_empresa, id_filial, referencia
    FROM src_raw
    WHERE referencia IS NOT NULL
  ), comp_ref AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      etl.safe_int(c.payload->>'REFERENCIA') AS referencia,
      etl.safe_int(c.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(c.payload->>'ID_DB') AS id_db,
      etl.safe_int(c.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(c.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_timestamp(c.payload->>'DATA') AS data_comp,
      row_number() OVER (
        PARTITION BY c.id_empresa, c.id_filial, etl.safe_int(c.payload->>'REFERENCIA')
        ORDER BY c.received_at DESC
      ) AS rn
    FROM stg.comprovantes c
    JOIN src_refs r
      ON r.id_empresa = c.id_empresa
     AND r.id_filial = c.id_filial
     AND r.referencia = etl.safe_int(c.payload->>'REFERENCIA')
    WHERE c.id_empresa = p_id_empresa
      AND etl.safe_int(c.payload->>'REFERENCIA') IS NOT NULL
  ), src AS (
    SELECT
      r.id_empresa,
      r.id_filial,
      r.referencia,
      r.id_db,
      cr.id_comprovante,
      cr.id_turno,
      cr.id_usuario,
      r.tipo_forma,
      r.valor,
      COALESCE(r.dt_evento_src, cr.data_comp, r.received_at) AS dt_evento,
      etl.date_key(COALESCE(r.dt_evento_src, cr.data_comp, r.received_at)::timestamp) AS data_key,
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
    WHERE r.tipo_forma IS NOT NULL
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
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.formas_pgto_comprovantes
  WHERE id_empresa = p_id_empresa;

  PERFORM etl.set_watermark(p_id_empresa, 'formas_pgto_comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

COMMIT;
