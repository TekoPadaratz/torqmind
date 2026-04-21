-- @nontransactional

CREATE OR REPLACE FUNCTION etl.bigint_hash64(p_input text)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_input IS NULL THEN NULL::bigint
    ELSE (('x' || substr(md5(p_input), 1, 16))::bit(64)::bigint)
  END;
$$;

CREATE OR REPLACE FUNCTION etl.pagamento_comprovante_bridge_hash(
  p_id_comprovante integer,
  p_id_db integer,
  p_id_turno integer,
  p_id_usuario integer,
  p_data_comp timestamptz,
  p_data_conta date,
  p_cash_eligible boolean
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT md5(
    jsonb_build_array(
      p_id_comprovante,
      p_id_db,
      p_id_turno,
      p_id_usuario,
      CASE
        WHEN p_data_comp IS NULL THEN NULL
        ELSE to_char(p_data_comp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      p_data_conta,
      p_cash_eligible
    )::text
  );
$$;

CREATE OR REPLACE FUNCTION etl.fact_pagamento_comprovante_hash(
  p_id_db integer,
  p_id_comprovante integer,
  p_id_turno integer,
  p_id_usuario integer,
  p_tipo_forma integer,
  p_valor numeric,
  p_dt_evento timestamptz,
  p_data_key integer,
  p_data_conta date,
  p_cash_eligible boolean,
  p_nsu text,
  p_autorizacao text,
  p_bandeira text,
  p_rede text,
  p_tef text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT md5(
    jsonb_build_array(
      p_id_db,
      p_id_comprovante,
      p_id_turno,
      p_id_usuario,
      p_tipo_forma,
      p_valor,
      CASE
        WHEN p_dt_evento IS NULL THEN NULL
        ELSE to_char(p_dt_evento AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      p_data_key,
      p_data_conta,
      p_cash_eligible,
      p_nsu,
      p_autorizacao,
      p_bandeira,
      p_rede,
      p_tef
    )::text
  );
$$;

CREATE TABLE IF NOT EXISTS etl.pagamento_comprovante_bridge (
  id_empresa integer NOT NULL,
  id_filial integer NOT NULL,
  referencia bigint NOT NULL,
  id_comprovante integer NULL,
  id_db integer NULL,
  id_turno integer NULL,
  id_usuario integer NULL,
  data_comp timestamptz NULL,
  data_conta date NULL,
  cash_eligible boolean NULL,
  source_received_at timestamptz NOT NULL,
  source_hash text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, referencia)
);

ALTER TABLE dw.fact_pagamento_comprovante
  ADD COLUMN IF NOT EXISTS row_hash text NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_formas_pgto_comp_emp_received_ref
  ON stg.formas_pgto_comprovantes (id_empresa, received_at DESC, id_filial, id_referencia)
  INCLUDE (tipo_forma, dt_evento, id_db_shadow, valor_shadow, nsu_shadow, autorizacao_shadow, bandeira_shadow, rede_shadow, tef_shadow);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_comprovantes_emp_filial_ref_received
  ON stg.comprovantes (id_empresa, id_filial, referencia_shadow, received_at DESC, id_db, id_comprovante)
  WHERE referencia_shadow IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_etl_pag_comp_bridge_updated
  ON etl.pagamento_comprovante_bridge (id_empresa, updated_at DESC, id_filial, referencia);

CREATE OR REPLACE FUNCTION etl.analyze_hot_tables()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  ANALYZE stg.comprovantes;
  ANALYZE stg.movprodutos;
  ANALYZE stg.itensmovprodutos;
  ANALYZE stg.formas_pgto_comprovantes;
  ANALYZE etl.pagamento_comprovante_bridge;
  ANALYZE dw.fact_comprovante;
  ANALYZE dw.fact_venda;
  ANALYZE dw.fact_venda_item;
  ANALYZE dw.fact_pagamento_comprovante;
  ANALYZE mart.customer_sales_daily;
  ANALYZE mart.customer_rfm_daily;
  ANALYZE mart.customer_churn_risk_daily;

  RETURN jsonb_build_object(
    'ok', true,
    'tables', jsonb_build_array(
      'stg.comprovantes',
      'stg.movprodutos',
      'stg.itensmovprodutos',
      'stg.formas_pgto_comprovantes',
      'etl.pagamento_comprovante_bridge',
      'dw.fact_comprovante',
      'dw.fact_venda',
      'dw.fact_venda_item',
      'dw.fact_pagamento_comprovante',
      'mart.customer_sales_daily',
      'mart.customer_rfm_daily',
      'mart.customer_churn_risk_daily'
    )
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_synced integer;
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
    COALESCE((SELECT COUNT(*) FROM synced_venda_cancel), 0),
    COALESCE((SELECT COUNT(*) FROM upserted_bridge), 0)
  INTO v_rows, v_synced, v_bridge_rows;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante_detail(p_id_empresa int)
RETURNS jsonb AS $$
DECLARE
  v_wm timestamptz;
  v_bridge_wm timestamptz;
  v_max timestamptz;
  v_bridge_max timestamptz;
  v_cutoff date;
  v_started timestamptz := clock_timestamp();
  v_bridge_started timestamptz;
  v_candidate_count integer := 0;
  v_conflict_count integer := 0;
  v_upsert_inserts integer := 0;
  v_upsert_updates integer := 0;
  v_bridge_miss_count integer := 0;
  v_bridge_resolve_ms integer := 0;
  v_total_ms integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);
  v_bridge_wm := COALESCE(etl.get_watermark(p_id_empresa, 'pagamento_comprovante_bridge'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_pag_refs;
  CREATE TEMP TABLE tmp_etl_candidate_pag_refs (
    id_empresa integer NOT NULL,
    id_filial integer NOT NULL,
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
    AND COALESCE(
      etl.business_date(
        etl.coalesce_operational_timestamptz(
          s.dt_evento,
          s.payload->>'TORQMIND_DT_EVENTO',
          s.payload->>'DT_EVENTO',
          s.payload->>'DATAREPL',
          s.payload->>'DATAHORA',
          s.payload->>'DATA'
        )
      ),
      CURRENT_DATE
    ) >= v_cutoff
    AND (
      s.received_at > v_wm
      OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  INSERT INTO tmp_etl_candidate_pag_refs
  SELECT
    b.id_empresa,
    b.id_filial,
    b.referencia
  FROM etl.pagamento_comprovante_bridge b
  WHERE b.id_empresa = p_id_empresa
    AND COALESCE(etl.business_date(b.data_comp), CURRENT_DATE) >= v_cutoff
    AND b.updated_at > v_bridge_wm
  ON CONFLICT DO NOTHING;

  v_bridge_started := clock_timestamp();

  DROP TABLE IF EXISTS tmp_etl_missing_pag_bridge_refs;
  CREATE TEMP TABLE tmp_etl_missing_pag_bridge_refs (
    id_empresa integer NOT NULL,
    id_filial integer NOT NULL,
    referencia bigint NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, referencia)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_missing_pag_bridge_refs
  SELECT
    r.id_empresa,
    r.id_filial,
    r.referencia
  FROM tmp_etl_candidate_pag_refs r
  LEFT JOIN etl.pagamento_comprovante_bridge b
    ON b.id_empresa = r.id_empresa
   AND b.id_filial = r.id_filial
   AND b.referencia = r.referencia
  WHERE b.referencia IS NULL;

  SELECT COUNT(*)::int INTO v_bridge_miss_count
  FROM tmp_etl_missing_pag_bridge_refs;

  IF v_bridge_miss_count > 0 THEN
    WITH latest_comp AS (
      SELECT DISTINCT ON (c.id_empresa, c.id_filial, c.referencia_shadow)
        c.id_empresa,
        c.id_filial,
        c.referencia_shadow AS referencia,
        c.id_db,
        c.id_comprovante,
        c.received_at AS source_received_at,
        etl.sales_event_timestamptz(c.payload, c.dt_evento) AS data_comp
      FROM stg.comprovantes c
      JOIN tmp_etl_missing_pag_bridge_refs m
        ON m.id_empresa = c.id_empresa
       AND m.id_filial = c.id_filial
       AND m.referencia = c.referencia_shadow
      WHERE c.id_empresa = p_id_empresa
        AND c.referencia_shadow IS NOT NULL
      ORDER BY c.id_empresa, c.id_filial, c.referencia_shadow, c.received_at DESC, c.id_db DESC, c.id_comprovante DESC
    ), bridge_src AS (
      SELECT
        l.id_empresa,
        l.id_filial,
        l.referencia,
        l.id_comprovante,
        l.id_db,
        fc.id_turno,
        fc.id_usuario,
        l.data_comp,
        fc.data_conta,
        fc.cash_eligible,
        l.source_received_at,
        etl.pagamento_comprovante_bridge_hash(
          l.id_comprovante,
          l.id_db,
          fc.id_turno,
          fc.id_usuario,
          l.data_comp,
          fc.data_conta,
          fc.cash_eligible
        ) AS source_hash
      FROM latest_comp l
      LEFT JOIN dw.fact_comprovante fc
        ON fc.id_empresa = l.id_empresa
       AND fc.id_filial = l.id_filial
       AND fc.id_db = l.id_db
       AND fc.id_comprovante = l.id_comprovante
    )
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
      source_hash,
      now()
    FROM bridge_src
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
    WHERE etl.pagamento_comprovante_bridge.source_hash IS DISTINCT FROM EXCLUDED.source_hash;
  END IF;

  v_bridge_resolve_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_bridge_started)) * 1000)::int;

  DROP TABLE IF EXISTS tmp_etl_candidate_pagamentos;
  CREATE TEMP TABLE tmp_etl_candidate_pagamentos AS
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_referencia AS referencia,
    s.tipo_forma,
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
    etl.coalesce_operational_timestamptz(
      s.dt_evento,
      s.payload->>'TORQMIND_DT_EVENTO',
      s.payload->>'DT_EVENTO',
      s.payload->>'DATAREPL',
      s.payload->>'DATAHORA',
      s.payload->>'DATA'
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
    AND COALESCE(
      etl.business_date(
        etl.coalesce_operational_timestamptz(
          s.dt_evento,
          s.payload->>'TORQMIND_DT_EVENTO',
          s.payload->>'DT_EVENTO',
          s.payload->>'DATAREPL',
          s.payload->>'DATAHORA',
          s.payload->>'DATA'
        )
      ),
      CURRENT_DATE
    ) >= v_cutoff;

  SELECT COUNT(*)::int INTO v_candidate_count
  FROM tmp_etl_candidate_pagamentos;

  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.referencia,
      COALESCE(b.id_db, p.id_db)::integer AS id_db,
      b.id_comprovante,
      b.id_turno,
      b.id_usuario,
      p.tipo_forma,
      p.valor,
      COALESCE(b.data_comp, p.dt_evento_src, p.received_at) AS dt_evento,
      etl.business_date_key(COALESCE(b.data_comp, p.dt_evento_src, p.received_at)) AS data_key,
      b.data_conta,
      COALESCE(b.cash_eligible, false) AS cash_eligible,
      p.nsu,
      p.autorizacao,
      p.bandeira,
      p.rede,
      p.tef,
      p.payload,
      etl.fact_pagamento_comprovante_hash(
        COALESCE(b.id_db, p.id_db)::integer,
        b.id_comprovante,
        b.id_turno,
        b.id_usuario,
        p.tipo_forma,
        p.valor,
        COALESCE(b.data_comp, p.dt_evento_src, p.received_at),
        etl.business_date_key(COALESCE(b.data_comp, p.dt_evento_src, p.received_at)),
        b.data_conta,
        COALESCE(b.cash_eligible, false),
        p.nsu,
        p.autorizacao,
        p.bandeira,
        p.rede,
        p.tef
      ) AS row_hash
    FROM tmp_etl_candidate_pagamentos p
    LEFT JOIN etl.pagamento_comprovante_bridge b
      ON b.id_empresa = p.id_empresa
     AND b.id_filial = p.id_filial
     AND b.referencia = p.referencia
    WHERE COALESCE(etl.business_date(b.data_comp), etl.business_date(p.dt_evento_src), CURRENT_DATE) >= v_cutoff
  ), prepared AS (
    SELECT
      s.*,
      f.row_hash AS current_hash
    FROM src s
    LEFT JOIN dw.fact_pagamento_comprovante f
      ON f.id_empresa = s.id_empresa
     AND f.id_filial = s.id_filial
     AND f.referencia = s.referencia
     AND f.tipo_forma = s.tipo_forma
  ), to_upsert AS (
    SELECT *
    FROM prepared
    WHERE current_hash IS DISTINCT FROM row_hash
  ), upserted AS (
    INSERT INTO dw.fact_pagamento_comprovante (
      id_empresa,
      id_filial,
      referencia,
      id_db,
      id_comprovante,
      id_turno,
      id_usuario,
      tipo_forma,
      valor,
      dt_evento,
      data_key,
      data_conta,
      cash_eligible,
      nsu,
      autorizacao,
      bandeira,
      rede,
      tef,
      row_hash,
      payload
    )
    SELECT
      id_empresa,
      id_filial,
      referencia,
      id_db,
      id_comprovante,
      id_turno,
      id_usuario,
      tipo_forma,
      valor,
      dt_evento,
      data_key,
      data_conta,
      cash_eligible,
      nsu,
      autorizacao,
      bandeira,
      rede,
      tef,
      row_hash,
      payload
    FROM to_upsert
    ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      valor = EXCLUDED.valor,
      dt_evento = EXCLUDED.dt_evento,
      data_key = EXCLUDED.data_key,
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      nsu = EXCLUDED.nsu,
      autorizacao = EXCLUDED.autorizacao,
      bandeira = EXCLUDED.bandeira,
      rede = EXCLUDED.rede,
      tef = EXCLUDED.tef,
      row_hash = EXCLUDED.row_hash,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE dw.fact_pagamento_comprovante.row_hash IS DISTINCT FROM EXCLUDED.row_hash
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM prepared WHERE current_hash IS NOT NULL), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE inserted), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE NOT inserted), 0)
  INTO v_conflict_count, v_upsert_inserts, v_upsert_updates;

  SELECT MAX(received_at) INTO v_max
  FROM stg.formas_pgto_comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  SELECT MAX(updated_at) INTO v_bridge_max
  FROM etl.pagamento_comprovante_bridge
  WHERE id_empresa = p_id_empresa
    AND updated_at > v_bridge_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'formas_pgto_comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  PERFORM etl.set_watermark(p_id_empresa, 'pagamento_comprovante_bridge', COALESCE(v_bridge_max, v_bridge_wm), NULL::bigint);

  v_total_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_started)) * 1000)::int;

  RETURN jsonb_build_object(
    'rows', COALESCE(v_upsert_inserts, 0) + COALESCE(v_upsert_updates, 0),
    'candidate_count', COALESCE(v_candidate_count, 0),
    'bridge_miss_count', COALESCE(v_bridge_miss_count, 0),
    'bridge_resolve_ms', COALESCE(v_bridge_resolve_ms, 0),
    'upsert_inserts', COALESCE(v_upsert_inserts, 0),
    'upsert_updates', COALESCE(v_upsert_updates, 0),
    'conflict_count', COALESCE(v_conflict_count, 0),
    'total_ms', COALESCE(v_total_ms, 0)
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_result jsonb;
BEGIN
  v_result := etl.load_fact_pagamento_comprovante_detail(p_id_empresa);
  RETURN COALESCE((v_result->>'rows')::int, 0);
END;
$$ LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS mart.pagamentos_anomalias_diaria CASCADE;

CREATE MATERIALIZED VIEW mart.pagamentos_anomalias_diaria AS
WITH base_ref AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    COUNT(*)::int AS qtd_formas,
    COUNT(*) FILTER (WHERE COALESCE(m.category, 'NAO_IDENTIFICADO') = 'NAO_IDENTIFICADO')::int AS qtd_desconhecido,
    COALESCE(SUM(f.valor),0)::numeric(18,2) AS valor_total,
    COALESCE(SUM(CASE WHEN COALESCE(m.category, 'NAO_IDENTIFICADO') = 'PIX' THEN f.valor ELSE 0 END),0)::numeric(18,2) AS valor_pix,
    COALESCE(MIN(f.id_turno), -1) AS id_turno
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
  WHERE etl.resolve_cash_eligible(f.cash_eligible, f.dt_evento, f.data_conta, f.id_turno)
  GROUP BY 1,2,3,4
), split_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    AVG(qtd_formas)::numeric(10,2) AS avg_formas,
    COUNT(*) FILTER (WHERE qtd_formas >= 3)::int AS comprovantes_multiplos,
    COUNT(*)::int AS comprovantes_total,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
  FROM base_ref
  GROUP BY 1,2,3
), split_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'SPLIT_INCOMUM'::text AS event_type,
    CASE WHEN avg_formas >= 2.4 THEN 'CRITICAL' WHEN avg_formas >= 1.8 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, ROUND((avg_formas - 1.4) * 55 + comprovantes_multiplos * 0.8)))::int AS score,
    COALESCE(valor_total,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'avg_formas_por_comprovante', avg_formas,
      'comprovantes_multiplos', comprovantes_multiplos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM split_daily
  WHERE comprovantes_total >= 20
), unknown_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    'FORMA_NAO_IDENTIFICADA'::text AS event_type,
    CASE WHEN qtd_desconhecido >= 4 THEN 'CRITICAL' WHEN qtd_desconhecido >= 2 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, qtd_desconhecido * 18 + (valor_total / 500)::int))::int AS score,
    valor_total AS impacto_estimado,
    jsonb_build_object(
      'qtd_formas_nao_identificadas', qtd_desconhecido,
      'valor_total', valor_total
    ) AS reasons
  FROM (
    SELECT
      id_empresa,
      id_filial,
      data_key,
      id_turno,
      SUM(qtd_desconhecido)::int AS qtd_desconhecido,
      COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
    FROM base_ref
    WHERE qtd_desconhecido > 0
    GROUP BY 1,2,3,4
  ) u
), pix_signal AS (
  SELECT
    b.id_empresa,
    b.id_filial,
    b.data_key,
    b.id_turno,
    'PIX_DESVIO_TURNO'::text AS event_type,
    CASE WHEN (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.80 THEN 'CRITICAL'
         WHEN (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.60 THEN 'WARN'
         ELSE 'INFO'
    END AS severity,
    LEAST(100, GREATEST(0, ROUND((b.valor_pix / NULLIF(b.valor_total, 0)) * 100)))::int AS score,
    b.valor_pix::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'valor_pix', b.valor_pix,
      'valor_total', b.valor_total,
      'share_pix', ROUND((b.valor_pix / NULLIF(b.valor_total, 0))::numeric, 4)
    ) AS reasons
  FROM (
    SELECT
      id_empresa,
      id_filial,
      data_key,
      id_turno,
      COALESCE(SUM(valor_pix),0)::numeric(18,2) AS valor_pix,
      COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
    FROM base_ref
    GROUP BY 1,2,3,4
  ) b
  WHERE b.valor_pix > 0
    AND b.valor_total > 0
    AND (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.60
)
SELECT
  s.id_empresa,
  s.id_filial,
  s.data_key,
  s.id_turno,
  s.event_type,
  s.severity,
  s.score,
  s.impacto_estimado,
  s.reasons,
  concat_ws(
    '|',
    'PAYMENT_ANOMALY',
    s.id_empresa::text,
    s.id_filial::text,
    s.data_key::text,
    COALESCE(s.id_turno, -1)::text,
    s.event_type
  ) AS insight_id,
  etl.bigint_hash64(
    concat_ws(
      '|',
      'PAYMENT_ANOMALY',
      s.id_empresa::text,
      s.id_filial::text,
      s.data_key::text,
      COALESCE(s.id_turno, -1)::text,
      s.event_type
    )
  ) AS insight_id_hash,
  now() AS updated_at
FROM (
  SELECT * FROM split_signal
  UNION ALL
  SELECT * FROM unknown_signal
  UNION ALL
  SELECT * FROM pix_signal
) s;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_pagamentos_anomalias_diaria
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, event_type, COALESCE(id_turno,-1));
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_lookup
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, severity, score DESC);
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_insight
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, insight_id_hash);

CREATE OR REPLACE FUNCTION etl.sync_payment_anomaly_notifications(
  p_id_empresa int,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      COALESCE(
        p.insight_id_hash,
        CASE WHEN p.insight_id IS NOT NULL THEN etl.bigint_hash64(p.insight_id) END
      ) AS insight_id,
      'CRITICAL'::text AS severity,
      format('Anomalia de pagamento (%s)', p.event_type) AS title,
      format('Score %s | Impacto estimado R$ %s', p.score, to_char(COALESCE(p.impacto_estimado,0), 'FM999G999G990D00')) AS body,
      '/fraud'::text AS url
    FROM mart.pagamentos_anomalias_diaria p
    WHERE p.id_empresa = p_id_empresa
      AND p.severity = 'CRITICAL'
      AND p.data_key >= to_char((COALESCE(p_ref_date, CURRENT_DATE) - interval '2 day')::date, 'YYYYMMDD')::int
      AND COALESCE(
        p.insight_id_hash,
        CASE WHEN p.insight_id IS NOT NULL THEN etl.bigint_hash64(p.insight_id) END
      ) IS NOT NULL
  ), upserted AS (
    INSERT INTO app.notifications (id_empresa, id_filial, insight_id, severity, title, body, url)
    SELECT id_empresa, id_filial, insight_id, severity, title, body, url
    FROM src
    ON CONFLICT (id_empresa, id_filial, insight_id)
    WHERE insight_id IS NOT NULL
    DO UPDATE SET
      severity = EXCLUDED.severity,
      title = EXCLUDED.title,
      body = EXCLUDED.body,
      url = EXCLUDED.url,
      created_at = now(),
      read_at = NULL
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

ANALYZE stg.comprovantes;
ANALYZE stg.formas_pgto_comprovantes;
ANALYZE etl.pagamento_comprovante_bridge;
ANALYZE dw.fact_pagamento_comprovante;
ANALYZE mart.pagamentos_anomalias_diaria;
