BEGIN;

CREATE OR REPLACE FUNCTION etl.runtime_from_date(p_default date DEFAULT NULL)
RETURNS date AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.from_date', true), '')::date, p_default);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.runtime_to_date(p_default date DEFAULT NULL)
RETURNS date AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.to_date', true), '')::date, p_default);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.runtime_branch_id(p_default integer DEFAULT NULL)
RETURNS integer AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.branch_id', true), '')::integer, p_default);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.runtime_force_full_scan(p_default boolean DEFAULT false)
RETURNS boolean AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.force_full_scan', true), '')::boolean, p_default);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.runtime_branch_matches(
  p_id_filial integer,
  p_default_branch_id integer DEFAULT NULL
)
RETURNS boolean AS $$
  SELECT etl.runtime_branch_id(p_default_branch_id) IS NULL
    OR p_id_filial = etl.runtime_branch_id(p_default_branch_id);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.runtime_business_date_in_range(
  p_business_date date,
  p_default_from_date date DEFAULT NULL,
  p_default_to_date date DEFAULT NULL
)
RETURNS boolean AS $$
  WITH bounds AS (
    SELECT
      etl.runtime_from_date(p_default_from_date) AS from_date,
      etl.runtime_to_date(p_default_to_date) AS to_date
  )
  SELECT COALESCE(
    (from_date IS NULL OR p_business_date >= from_date)
    AND (to_date IS NULL OR p_business_date <= to_date),
    false
  )
  FROM bounds;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.runtime_watermark_updates_enabled()
RETURNS boolean AS $$
  SELECT etl.runtime_branch_id(NULL) IS NULL
    AND etl.runtime_to_date(NULL) IS NULL;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.sales_cutoff_date(
  p_id_empresa integer,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS date AS $$
  SELECT COALESCE(
    etl.runtime_from_date(NULL),
    etl.runtime_ref_date(COALESCE(p_ref_date, CURRENT_DATE))
      - GREATEST(etl.sales_history_days(p_id_empresa) - 1, 0)
  );
$$ LANGUAGE sql STABLE;

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
    AND etl.runtime_branch_matches(c.id_filial)
    AND etl.runtime_business_date_in_range(
      etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR c.received_at > v_wm
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

  IF etl.runtime_watermark_updates_enabled() THEN
    SELECT MAX(received_at) INTO v_max
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND received_at > v_wm;

    PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  END IF;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

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
    AND etl.runtime_branch_matches(c.id_filial)
    AND etl.runtime_business_date_in_range(
      etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR c.received_at > v_wm
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

  IF etl.runtime_watermark_updates_enabled() THEN
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
  END IF;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.fact_pagamento_comprovante_pending_bounds(p_id_empresa int)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_wm timestamptz;
  v_bridge_wm timestamptz;
  v_cutoff date;
  v_candidate_refs bigint := 0;
  v_min_referencia bigint;
  v_max_referencia bigint;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);
  v_bridge_wm := COALESCE(etl.get_watermark(p_id_empresa, 'pagamento_comprovante_bridge'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  WITH refs AS (
    SELECT DISTINCT
      s.id_referencia AS referencia
    FROM stg.formas_pgto_comprovantes s
    WHERE s.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(s.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(
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
        ),
        v_cutoff,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR s.received_at > v_wm
        OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )

    UNION

    SELECT b.referencia
    FROM etl.pagamento_comprovante_bridge b
    WHERE b.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(b.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(etl.business_date(b.data_comp), CURRENT_DATE),
        v_cutoff,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR b.updated_at > v_bridge_wm
      )
  )
  SELECT
    COUNT(*)::bigint,
    MIN(referencia),
    MAX(referencia)
  INTO v_candidate_refs, v_min_referencia, v_max_referencia
  FROM refs;

  RETURN jsonb_build_object(
    'candidate_refs', COALESCE(v_candidate_refs, 0),
    'min_referencia', v_min_referencia,
    'max_referencia', v_max_referencia,
    'watermark_before', v_wm,
    'bridge_watermark_before', v_bridge_wm,
    'cutoff_date', v_cutoff
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante_range_detail(
  p_id_empresa int,
  p_referencia_from bigint DEFAULT NULL,
  p_referencia_to bigint DEFAULT NULL,
  p_update_watermarks boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_wm timestamptz;
  v_bridge_wm timestamptz;
  v_max timestamptz;
  v_bridge_max timestamptz;
  v_cutoff date;
  v_started timestamptz := clock_timestamp();
  v_bridge_started timestamptz;
  v_candidate_count integer := 0;
  v_candidate_refs integer := 0;
  v_conflict_count integer := 0;
  v_upsert_inserts integer := 0;
  v_upsert_updates integer := 0;
  v_bridge_miss_count integer := 0;
  v_bridge_rows integer := 0;
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
    AND etl.runtime_branch_matches(s.id_filial)
    AND (p_referencia_from IS NULL OR s.id_referencia >= p_referencia_from)
    AND (p_referencia_to IS NULL OR s.id_referencia <= p_referencia_to)
    AND etl.runtime_business_date_in_range(
      COALESCE(
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
      ),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR s.received_at > v_wm
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
    AND etl.runtime_branch_matches(b.id_filial)
    AND (p_referencia_from IS NULL OR b.referencia >= p_referencia_from)
    AND (p_referencia_to IS NULL OR b.referencia <= p_referencia_to)
    AND etl.runtime_business_date_in_range(
      COALESCE(etl.business_date(b.data_comp), CURRENT_DATE),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR b.updated_at > v_bridge_wm
    )
  ON CONFLICT DO NOTHING;

  SELECT COUNT(*)::int INTO v_candidate_refs
  FROM tmp_etl_candidate_pag_refs;

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
      ORDER BY
        c.id_empresa,
        c.id_filial,
        c.referencia_shadow,
        c.received_at DESC,
        c.id_db DESC,
        c.id_comprovante DESC
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
      WHERE etl.pagamento_comprovante_bridge.source_hash IS DISTINCT FROM EXCLUDED.source_hash
      RETURNING 1
    )
    SELECT COUNT(*)::int INTO v_bridge_rows
    FROM upserted_bridge;
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
    AND etl.runtime_business_date_in_range(
      COALESCE(
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
      ),
      v_cutoff,
      NULL::date
    );

  SELECT COUNT(*)::int INTO v_candidate_count
  FROM tmp_etl_candidate_pagamentos;

  WITH src AS MATERIALIZED (
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
    WHERE etl.runtime_business_date_in_range(
      COALESCE(etl.business_date(b.data_comp), etl.business_date(p.dt_evento_src), CURRENT_DATE),
      v_cutoff,
      NULL::date
    )
  ), prepared AS MATERIALIZED (
    SELECT
      s.*,
      f.row_hash AS current_hash
    FROM src s
    LEFT JOIN dw.fact_pagamento_comprovante f
      ON f.id_empresa = s.id_empresa
     AND f.id_filial = s.id_filial
     AND f.referencia = s.referencia
     AND f.tipo_forma = s.tipo_forma
  ), to_upsert AS MATERIALIZED (
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

  IF p_update_watermarks AND etl.runtime_watermark_updates_enabled() THEN
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
  END IF;

  v_total_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_started)) * 1000)::int;

  RETURN jsonb_build_object(
    'rows', COALESCE(v_upsert_inserts, 0) + COALESCE(v_upsert_updates, 0),
    'candidate_refs', COALESCE(v_candidate_refs, 0),
    'candidate_count', COALESCE(v_candidate_count, 0),
    'bridge_miss_count', COALESCE(v_bridge_miss_count, 0),
    'bridge_rows', COALESCE(v_bridge_rows, 0),
    'bridge_resolve_ms', COALESCE(v_bridge_resolve_ms, 0),
    'upsert_inserts', COALESCE(v_upsert_inserts, 0),
    'upsert_updates', COALESCE(v_upsert_updates, 0),
    'conflict_count', COALESCE(v_conflict_count, 0),
    'range_from', p_referencia_from,
    'range_to', p_referencia_to,
    'watermark_updated', p_update_watermarks AND etl.runtime_watermark_updates_enabled(),
    'total_ms', COALESCE(v_total_ms, 0)
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante_detail(p_id_empresa int)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN etl.load_fact_pagamento_comprovante_range_detail(
    p_id_empresa,
    NULL::bigint,
    NULL::bigint,
    etl.runtime_watermark_updates_enabled()
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_result jsonb;
BEGIN
  v_result := etl.load_fact_pagamento_comprovante_detail(p_id_empresa);
  RETURN COALESCE((v_result->>'rows')::int, 0);
END;
$$;

CREATE OR REPLACE FUNCTION etl.fact_venda_item_pending_bounds(p_id_empresa int)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_wm timestamptz;
  v_cutoff date;
  v_candidate_rows bigint := 0;
  v_min_id_comprovante integer;
  v_max_id_comprovante integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itenscomprovantes_sales_fact'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  SELECT
    COUNT(*)::bigint,
    MIN(i.id_comprovante),
    MAX(i.id_comprovante)
  INTO v_candidate_rows, v_min_id_comprovante, v_max_id_comprovante
  FROM stg.itenscomprovantes i
  WHERE i.id_empresa = p_id_empresa
    AND etl.runtime_branch_matches(i.id_filial)
    AND etl.runtime_business_date_in_range(
      COALESCE(etl.business_date(i.dt_evento), v_cutoff),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR i.received_at > v_wm
      OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    );

  RETURN jsonb_build_object(
    'candidate_rows', COALESCE(v_candidate_rows, 0),
    'min_id_comprovante', v_min_id_comprovante,
    'max_id_comprovante', v_max_id_comprovante,
    'watermark_before', v_wm,
    'cutoff_date', v_cutoff
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
      AND etl.runtime_branch_matches(i.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(etl.business_date(i.dt_evento), v_cutoff),
        v_cutoff,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR i.received_at > v_wm
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
    WHERE dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.id_movprodutos IS DISTINCT FROM EXCLUDED.id_movprodutos
      OR dw.fact_venda_item.id_itensmovprodutos IS DISTINCT FROM EXCLUDED.id_itensmovprodutos
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
      OR dw.fact_venda_item.desconto_total IS DISTINCT FROM EXCLUDED.desconto_total
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM src), 0),
    COALESCE((SELECT COUNT(*) FROM prepared WHERE current_payload IS NOT NULL), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE inserted), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE NOT inserted), 0)
  INTO v_candidate_count, v_conflict_count, v_upsert_inserts, v_upsert_updates;

  IF p_update_watermark AND etl.runtime_watermark_updates_enabled() THEN
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
    'watermark_updated', p_update_watermark AND etl.runtime_watermark_updates_enabled(),
    'total_ms', COALESCE(v_total_ms, 0)
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item_detail(p_id_empresa int)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN etl.load_fact_venda_item_range_detail(
    p_id_empresa,
    NULL::int,
    NULL::int,
    etl.runtime_watermark_updates_enabled()
  );
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_result jsonb;
BEGIN
  v_result := etl.load_fact_venda_item_detail(p_id_empresa);
  RETURN COALESCE((v_result->>'rows')::int, 0);
END;
$$;

CREATE OR REPLACE FUNCTION etl.load_fact_caixa_turno(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'turnos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      t.id_empresa,
      t.id_filial,
      t.id_turno,
      COALESCE(
        etl.safe_int(t.payload->>'ID_DB'),
        etl.safe_int(t.id_db_shadow::text)
      ) AS id_db,
      COALESCE(
        etl.safe_int(t.payload->>'ID_USUARIOS'),
        etl.safe_int(t.payload->>'ID_USUARIO')
      ) AS id_usuario,
      etl.coalesce_operational_timestamptz(
        NULL::timestamptz,
        t.payload->>'DATA',
        t.payload->>'DTABERTURA',
        t.payload->>'DATAABERTURA',
        t.payload->>'DTHRABERTURA',
        t.payload->>'DTHR_ABERTURA',
        t.payload->>'ABERTURA',
        t.payload->>'INICIO'
      ) AS abertura_ts,
      etl.coalesce_operational_timestamptz(
        NULL::timestamptz,
        t.payload->>'DTFECHAMENTO',
        t.payload->>'DATAFECHAMENTO',
        t.payload->>'DTHRFECHAMENTO',
        t.payload->>'DTHR_FECHAMENTO',
        t.payload->>'FECHAMENTO',
        t.payload->>'FIM'
      ) AS fechamento_ts,
      etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO') AS encerrante_fechamento,
      UPPER(COALESCE(
        NULLIF(t.payload->>'STATUS', ''),
        NULLIF(t.payload->>'STATUSTURNO', ''),
        NULLIF(t.payload->>'STATUS_TURNO_WEB', ''),
        NULLIF(t.payload->>'SITUACAO', ''),
        NULLIF(t.payload->>'SITUACAO_TURNO', ''),
        NULLIF(t.payload->>'ST', '')
      )) AS status_raw,
      t.payload,
      t.received_at
    FROM stg.turnos t
    WHERE t.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(t.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(
          etl.business_date(
            etl.coalesce_operational_timestamptz(
              NULL::timestamptz,
              t.payload->>'DATA',
              t.payload->>'DTABERTURA',
              t.payload->>'DATAABERTURA',
              t.payload->>'DTHRABERTURA',
              t.payload->>'DTHR_ABERTURA',
              t.payload->>'ABERTURA',
              t.payload->>'INICIO'
            )
          ),
          etl.business_date(
            etl.coalesce_operational_timestamptz(
              NULL::timestamptz,
              t.payload->>'DTFECHAMENTO',
              t.payload->>'DATAFECHAMENTO',
              t.payload->>'DTHRFECHAMENTO',
              t.payload->>'DTHR_FECHAMENTO',
              t.payload->>'FECHAMENTO',
              t.payload->>'FIM'
            )
          ),
          CURRENT_DATE
        ),
        NULL::date,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR t.received_at > v_wm
        OR COALESCE(etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO'), 0) = 0
      )
  ), normalized AS (
    SELECT
      id_empresa,
      id_filial,
      id_turno,
      id_db,
      id_usuario,
      abertura_ts,
      fechamento_ts,
      etl.business_date_key(abertura_ts) AS data_key_abertura,
      etl.business_date_key(fechamento_ts) AS data_key_fechamento,
      encerrante_fechamento,
      CASE
        WHEN encerrante_fechamento = 0 THEN true
        ELSE false
      END AS is_aberto,
      status_raw,
      payload
    FROM src
  ), upserted AS (
    INSERT INTO dw.fact_caixa_turno (
      id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts, fechamento_ts,
      data_key_abertura, data_key_fechamento, encerrante_fechamento, is_aberto, status_raw, payload
    )
    SELECT
      id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts, fechamento_ts,
      data_key_abertura, data_key_fechamento, encerrante_fechamento, is_aberto, status_raw, payload
    FROM normalized
    ON CONFLICT (id_empresa, id_filial, id_turno)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_usuario = EXCLUDED.id_usuario,
      abertura_ts = EXCLUDED.abertura_ts,
      fechamento_ts = EXCLUDED.fechamento_ts,
      data_key_abertura = EXCLUDED.data_key_abertura,
      data_key_fechamento = EXCLUDED.data_key_fechamento,
      encerrante_fechamento = EXCLUDED.encerrante_fechamento,
      is_aberto = EXCLUDED.is_aberto,
      status_raw = EXCLUDED.status_raw,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.fact_caixa_turno.id_usuario IS DISTINCT FROM EXCLUDED.id_usuario
      OR dw.fact_caixa_turno.abertura_ts IS DISTINCT FROM EXCLUDED.abertura_ts
      OR dw.fact_caixa_turno.fechamento_ts IS DISTINCT FROM EXCLUDED.fechamento_ts
      OR dw.fact_caixa_turno.encerrante_fechamento IS DISTINCT FROM EXCLUDED.encerrante_fechamento
      OR dw.fact_caixa_turno.is_aberto IS DISTINCT FROM EXCLUDED.is_aberto
      OR dw.fact_caixa_turno.status_raw IS DISTINCT FROM EXCLUDED.status_raw
      OR dw.fact_caixa_turno.payload IS DISTINCT FROM EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  IF etl.runtime_watermark_updates_enabled() THEN
    SELECT MAX(received_at) INTO v_max
    FROM stg.turnos
    WHERE id_empresa = p_id_empresa
      AND received_at > v_wm;

    PERFORM etl.set_watermark(p_id_empresa, 'turnos', COALESCE(v_max, v_wm), NULL::bigint);
  END IF;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_financeiro(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max_a timestamptz;
  v_max_b timestamptz;
  v_max_c timestamptz;
  v_max_final timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'financeiro'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_db,
      f.tipo_titulo,
      f.id_titulo,
      etl.safe_int(f.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(f.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(f.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(f.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(f.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(f.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      f.payload
    FROM stg.financeiro f
    WHERE f.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(f.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(
          (etl.safe_timestamp(f.payload->>'DTAPGTO'))::date,
          (etl.safe_timestamp(f.payload->>'DTAVCTO'))::date,
          (etl.safe_timestamp(f.payload->>'DTACONTA'))::date
        ),
        NULL::date,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR f.received_at > v_wm
        OR (f.dt_evento IS NOT NULL AND f.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )

    UNION ALL

    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_db,
      0 AS tipo_titulo,
      p.id_contaspagar AS id_titulo,
      etl.safe_int(p.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(p.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(p.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(p.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(p.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      p.payload
    FROM stg.contaspagar p
    WHERE p.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(p.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(
          (etl.safe_timestamp(p.payload->>'DTAPGTO'))::date,
          (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date,
          (etl.safe_timestamp(p.payload->>'DTACONTA'))::date
        ),
        NULL::date,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR p.received_at > v_wm
        OR (p.dt_evento IS NOT NULL AND p.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )

    UNION ALL

    SELECT
      r.id_empresa,
      r.id_filial,
      r.id_db,
      1 AS tipo_titulo,
      r.id_contasreceber AS id_titulo,
      etl.safe_int(r.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(r.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(r.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(r.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(r.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(r.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      r.payload
    FROM stg.contasreceber r
    WHERE r.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(r.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(
          (etl.safe_timestamp(r.payload->>'DTAPGTO'))::date,
          (etl.safe_timestamp(r.payload->>'DTAVCTO'))::date,
          (etl.safe_timestamp(r.payload->>'DTACONTA'))::date
        ),
        NULL::date,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR r.received_at > v_wm
        OR (r.dt_evento IS NOT NULL AND r.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), upserted AS (
    INSERT INTO dw.fact_financeiro (
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    )
    SELECT
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,tipo_titulo,id_titulo)
    DO UPDATE SET
      id_entidade=EXCLUDED.id_entidade,
      data_emissao=EXCLUDED.data_emissao,
      data_key_emissao=EXCLUDED.data_key_emissao,
      vencimento=EXCLUDED.vencimento,
      data_key_venc=EXCLUDED.data_key_venc,
      data_pagamento=EXCLUDED.data_pagamento,
      data_key_pgto=EXCLUDED.data_key_pgto,
      valor=EXCLUDED.valor,
      valor_pago=EXCLUDED.valor_pago,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_financeiro.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_financeiro.valor IS DISTINCT FROM EXCLUDED.valor
      OR dw.fact_financeiro.valor_pago IS DISTINCT FROM EXCLUDED.valor_pago
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  IF etl.runtime_watermark_updates_enabled() THEN
    SELECT MAX(received_at) INTO v_max_a FROM stg.financeiro WHERE id_empresa = p_id_empresa AND received_at > v_wm;
    SELECT MAX(received_at) INTO v_max_b FROM stg.contaspagar WHERE id_empresa = p_id_empresa AND received_at > v_wm;
    SELECT MAX(received_at) INTO v_max_c FROM stg.contasreceber WHERE id_empresa = p_id_empresa AND received_at > v_wm;

    v_max_final := GREATEST(COALESCE(v_max_a, '1970-01-01'::timestamptz), COALESCE(v_max_b, '1970-01-01'::timestamptz), COALESCE(v_max_c, '1970-01-01'::timestamptz));
    PERFORM etl.set_watermark(p_id_empresa, 'financeiro', COALESCE(v_max_final, v_wm), NULL::bigint);
  END IF;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;