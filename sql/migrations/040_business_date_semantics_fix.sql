BEGIN;

-- Operational business date semantics are fixed to America/Sao_Paulo.
-- These helpers must stay independent from the PostgreSQL session timezone.

CREATE OR REPLACE FUNCTION etl.business_timezone_name()
RETURNS text AS $$
  SELECT 'America/Sao_Paulo';
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.safe_operational_timestamptz(p_text text)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_text text;
  v_ts timestamptz;
  v_local_ts timestamp;
BEGIN
  IF p_text IS NULL THEN
    RETURN NULL;
  END IF;

  v_text := btrim(p_text);
  IF v_text = '' THEN
    RETURN NULL;
  END IF;

  -- Explicit offsets/Z must be preserved as-is.
  IF v_text ~* '([T[:space:]]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)\s*(?:[+-]\d{2}(?::?\d{2})?|[Zz])$' THEN
    BEGIN
      v_ts := v_text::timestamptz;
      RETURN v_ts;
    EXCEPTION WHEN others THEN
      RETURN NULL;
    END;
  END IF;

  -- Naive operational payloads are São Paulo wall-clock time.
  BEGIN
    v_local_ts := v_text::timestamp;
    RETURN v_local_ts AT TIME ZONE etl.business_timezone_name();
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END;
$$;

CREATE OR REPLACE FUNCTION etl.coalesce_operational_timestamptz(
  p_fallback timestamptz,
  VARIADIC p_candidates text[]
)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_candidate text;
  v_ts timestamptz;
BEGIN
  IF p_fallback IS NOT NULL THEN
    RETURN p_fallback;
  END IF;

  IF p_candidates IS NULL THEN
    RETURN NULL;
  END IF;

  FOREACH v_candidate IN ARRAY p_candidates LOOP
    v_ts := etl.safe_operational_timestamptz(v_candidate);
    IF v_ts IS NOT NULL THEN
      RETURN v_ts;
    END IF;
  END LOOP;

  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION etl.sales_event_timestamptz(
  p_payload jsonb,
  p_fallback timestamptz DEFAULT NULL
)
RETURNS timestamptz AS $$
  SELECT etl.coalesce_operational_timestamptz(
    p_fallback,
    p_payload->>'TORQMIND_DT_EVENTO',
    p_payload->>'DT_EVENTO',
    p_payload->>'DATA',
    p_payload->>'DATAMOV',
    p_payload->>'DTMOV'
  );
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.business_timestamp(p_ts timestamptz)
RETURNS timestamp AS $$
  SELECT CASE
    WHEN p_ts IS NULL THEN NULL
    ELSE p_ts AT TIME ZONE etl.business_timezone_name()
  END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.business_timestamp(p_ts timestamp)
RETURNS timestamp AS $$
  SELECT p_ts;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.business_date(p_ts timestamptz)
RETURNS date AS $$
  SELECT CASE
    WHEN p_ts IS NULL THEN NULL
    ELSE etl.business_timestamp(p_ts)::date
  END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.business_date(p_ts timestamp)
RETURNS date AS $$
  SELECT CASE
    WHEN p_ts IS NULL THEN NULL
    ELSE p_ts::date
  END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.business_date_key(p_ts timestamptz)
RETURNS integer AS $$
  SELECT CASE
    WHEN p_ts IS NULL THEN NULL
    ELSE to_char(etl.business_date(p_ts), 'YYYYMMDD')::int
  END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.business_date_key(p_ts timestamp)
RETURNS integer AS $$
  SELECT CASE
    WHEN p_ts IS NULL THEN NULL
    ELSE to_char(etl.business_date(p_ts), 'YYYYMMDD')::int
  END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.date_key(p_ts timestamptz)
RETURNS integer AS $$
  SELECT etl.business_date_key(p_ts);
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.sales_business_ts(
  p_payload jsonb,
  p_fallback timestamp DEFAULT NULL
)
RETURNS timestamp AS $$
  SELECT COALESCE(
    p_fallback,
    etl.business_timestamp(etl.sales_event_timestamptz(p_payload, NULL::timestamptz))
  );
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.sales_business_ts(
  p_payload jsonb,
  p_fallback timestamptz
)
RETURNS timestamp AS $$
  SELECT etl.business_timestamp(etl.sales_event_timestamptz(p_payload, p_fallback));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.purge_sales_history(
  p_id_empresa integer DEFAULT NULL,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
  v_tenants jsonb := '[]'::jsonb;
  v_tenant_count integer := 0;
  v_stg_formas_deleted integer := 0;
  v_stg_itens_deleted integer := 0;
  v_dw_pagamentos_deleted integer := 0;
  v_dw_venda_item_deleted integer := 0;
  v_dw_venda_deleted integer := 0;
  v_dw_comprovante_deleted integer := 0;
  v_stg_movprodutos_deleted integer := 0;
  v_stg_comprovantes_deleted integer := 0;
  v_customer_sales_deleted integer := 0;
  v_customer_rfm_deleted integer := 0;
  v_customer_churn_deleted integer := 0;
  v_refresh_changed jsonb := '{}'::jsonb;
  v_refresh_candidate_domains jsonb := '{}'::jsonb;
  v_refresh_domains jsonb := jsonb_build_object(
    'sales', false,
    'payments', false,
    'churn', false,
    'finance', false,
    'risk', false,
    'cash', false
  );
  v_refresh_meta jsonb := jsonb_build_object(
    'refreshed_any', false,
    'refresh_scope', CASE WHEN p_id_empresa IS NULL THEN 'global' ELSE 'tenant' END,
    'sales_marts_refreshed', false,
    'payments_marts_refreshed', false,
    'churn_mart_refreshed', false,
    'refresh_domains', jsonb_build_object(
      'sales', false,
      'payments', false,
      'churn', false,
      'finance', false,
      'risk', false,
      'cash', false
    )
  );
  v_marts_refreshed text[] := ARRAY[]::text[];
  v_sales_refresh boolean := false;
  v_payments_refresh boolean := false;
BEGIN
  SELECT
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'id_empresa', t.id_empresa,
          'cutoff_date', etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date)
        )
        ORDER BY t.id_empresa
      ),
      '[]'::jsonb
    ),
    COUNT(*)::int
  INTO v_tenants, v_tenant_count
  FROM app.tenants t
  WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa;

  IF v_tenant_count = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'ref_date', v_effective_ref_date,
      'tenants', v_tenants,
      'message', 'No target tenants found.'
    );
  END IF;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM stg.formas_pgto_comprovantes f
    USING tenant_cutoff tc
    WHERE f.id_empresa = tc.id_empresa
      AND (
        (
          f.dt_evento IS NOT NULL
          AND etl.business_date(f.dt_evento) < tc.cutoff_date
        )
        OR EXISTS (
          SELECT 1
          FROM stg.comprovantes c
          WHERE c.id_empresa = f.id_empresa
            AND c.id_filial = f.id_filial
            AND etl.safe_int(c.payload->>'REFERENCIA') = f.id_referencia
            AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) < tc.cutoff_date
        )
        OR EXISTS (
          SELECT 1
          FROM dw.fact_pagamento_comprovante p
          WHERE p.id_empresa = f.id_empresa
            AND p.id_filial = f.id_filial
            AND p.referencia = f.id_referencia
            AND p.tipo_forma = f.tipo_forma
            AND etl.business_date(p.dt_evento) < tc.cutoff_date
        )
      )
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_stg_formas_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM stg.itensmovprodutos i
    USING tenant_cutoff tc
    WHERE i.id_empresa = tc.id_empresa
      AND (
        (
          i.dt_evento IS NOT NULL
          AND etl.business_date(i.dt_evento) < tc.cutoff_date
        )
        OR EXISTS (
          SELECT 1
          FROM stg.movprodutos m
          WHERE m.id_empresa = i.id_empresa
            AND m.id_filial = i.id_filial
            AND m.id_db = i.id_db
            AND m.id_movprodutos = i.id_movprodutos
            AND etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento)) < tc.cutoff_date
        )
        OR EXISTS (
          SELECT 1
          FROM dw.fact_venda v
          WHERE v.id_empresa = i.id_empresa
            AND v.id_filial = i.id_filial
            AND v.id_db = i.id_db
            AND v.id_movprodutos = i.id_movprodutos
            AND etl.business_date(v.data) < tc.cutoff_date
        )
      )
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_stg_itens_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM dw.fact_pagamento_comprovante p
    USING tenant_cutoff tc
    WHERE p.id_empresa = tc.id_empresa
      AND etl.business_date(p.dt_evento) < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_dw_pagamentos_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM dw.fact_venda_item i
    USING tenant_cutoff tc, dw.fact_venda v
    WHERE i.id_empresa = tc.id_empresa
      AND v.id_empresa = i.id_empresa
      AND v.id_filial = i.id_filial
      AND v.id_db = i.id_db
      AND v.id_movprodutos = i.id_movprodutos
      AND etl.business_date(v.data) < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_dw_venda_item_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM dw.fact_venda v
    USING tenant_cutoff tc
    WHERE v.id_empresa = tc.id_empresa
      AND etl.business_date(v.data) < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_dw_venda_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM dw.fact_comprovante c
    USING tenant_cutoff tc
    WHERE c.id_empresa = tc.id_empresa
      AND etl.business_date(c.data) < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_dw_comprovante_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM stg.movprodutos m
    USING tenant_cutoff tc
    WHERE m.id_empresa = tc.id_empresa
      AND etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento)) < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_stg_movprodutos_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM stg.comprovantes c
    USING tenant_cutoff tc
    WHERE c.id_empresa = tc.id_empresa
      AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_stg_comprovantes_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM mart.customer_sales_daily s
    USING tenant_cutoff tc
    WHERE s.id_empresa = tc.id_empresa
      AND s.dt_ref < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_customer_sales_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM mart.customer_rfm_daily s
    USING tenant_cutoff tc
    WHERE s.id_empresa = tc.id_empresa
      AND s.dt_ref < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_customer_rfm_deleted FROM deleted;

  WITH tenant_cutoff AS (
    SELECT
      t.id_empresa,
      etl.sales_cutoff_date(t.id_empresa, v_effective_ref_date) AS cutoff_date
    FROM app.tenants t
    WHERE p_id_empresa IS NULL OR t.id_empresa = p_id_empresa
  ), deleted AS (
    DELETE FROM mart.customer_churn_risk_daily s
    USING tenant_cutoff tc
    WHERE s.id_empresa = tc.id_empresa
      AND s.dt_ref < tc.cutoff_date
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_customer_churn_deleted FROM deleted;

  v_refresh_changed := jsonb_build_object(
    'fact_comprovante', v_dw_comprovante_deleted,
    'fact_venda', v_dw_venda_deleted,
    'fact_venda_item', v_dw_venda_item_deleted,
    'fact_pagamento_comprovante', v_dw_pagamentos_deleted
  );
  v_refresh_candidate_domains := etl.change_domains(v_refresh_changed);
  v_sales_refresh := COALESCE((v_refresh_candidate_domains->>'sales')::boolean, false);
  v_payments_refresh := COALESCE((v_refresh_candidate_domains->>'payments')::boolean, false);

  IF v_sales_refresh THEN
    REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria;
    REFRESH MATERIALIZED VIEW mart.insights_base_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_vendas_hora;
    REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
    REFRESH MATERIALIZED VIEW mart.clientes_churn_risco;
    v_marts_refreshed := v_marts_refreshed || ARRAY[
      'mart.agg_vendas_diaria',
      'mart.insights_base_diaria',
      'mart.agg_vendas_hora',
      'mart.agg_produtos_diaria',
      'mart.agg_grupos_diaria',
      'mart.agg_funcionarios_diaria',
      'mart.fraude_cancelamentos_diaria',
      'mart.fraude_cancelamentos_eventos',
      'mart.clientes_churn_risco'
    ];
  END IF;

  IF v_payments_refresh THEN
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
    REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
    v_marts_refreshed := v_marts_refreshed || ARRAY[
      'mart.agg_pagamentos_diaria',
      'mart.agg_pagamentos_turno',
      'mart.pagamentos_anomalias_diaria'
    ];
  END IF;

  v_refresh_domains := jsonb_build_object(
    'sales', v_sales_refresh,
    'payments', v_payments_refresh,
    'churn', v_sales_refresh,
    'finance', false,
    'risk', false,
    'cash', false
  );
  v_refresh_meta := jsonb_build_object(
    'refreshed_any', (v_sales_refresh OR v_payments_refresh),
    'refresh_scope', CASE WHEN p_id_empresa IS NULL THEN 'global' ELSE 'tenant' END,
    'sales_marts_refreshed', v_sales_refresh,
    'payments_marts_refreshed', v_payments_refresh,
    'churn_mart_refreshed', v_sales_refresh,
    'refresh_domains', v_refresh_domains,
    'candidate_domains', v_refresh_candidate_domains,
    'marts_refreshed', to_jsonb(v_marts_refreshed)
  );

  PERFORM etl.analyze_hot_tables();

  RETURN jsonb_build_object(
    'ok', true,
    'ref_date', v_effective_ref_date,
    'tenants', v_tenants,
    'stg_formas_pgto_comprovantes_deleted', v_stg_formas_deleted,
    'stg_itensmovprodutos_deleted', v_stg_itens_deleted,
    'dw_fact_pagamento_comprovante_deleted', v_dw_pagamentos_deleted,
    'dw_fact_venda_item_deleted', v_dw_venda_item_deleted,
    'dw_fact_venda_deleted', v_dw_venda_deleted,
    'dw_fact_comprovante_deleted', v_dw_comprovante_deleted,
    'stg_movprodutos_deleted', v_stg_movprodutos_deleted,
    'stg_comprovantes_deleted', v_stg_comprovantes_deleted,
    'mart_customer_sales_daily_deleted', v_customer_sales_deleted,
    'mart_customer_rfm_daily_deleted', v_customer_rfm_deleted,
    'mart_customer_churn_risk_daily_deleted', v_customer_churn_deleted,
    'duration_ms', GREATEST(0, FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_started)) * 1000)::int),
    'refresh_meta', v_refresh_meta,
    'marts_refreshed', to_jsonb(v_marts_refreshed)
  );
END;
$$;

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
      c.id_comprovante,
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
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
    AND etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento)) >= v_cutoff
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
      etl.sales_business_ts(m.payload, m.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(m.payload, m.dt_evento)) AS data_key,
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
      AND COALESCE(etl.business_date(v.data), etl.business_date(c.data)) >= v_cutoff
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
    AND etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento)) >= v_cutoff
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
    AND COALESCE(
      etl.business_date(i.dt_evento),
      etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento))
    ) >= v_cutoff
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
        etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento)))
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
    c.id_empresa,
    c.id_filial,
    c.referencia_shadow
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND c.referencia_shadow IS NOT NULL
    AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) >= v_cutoff
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
  ), comp_ref AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.referencia_shadow AS referencia,
      c.id_comprovante AS id_comprovante,
      c.id_db AS id_db,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      etl.sales_event_timestamptz(c.payload, c.dt_evento) AS data_comp,
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
      etl.business_date_key(COALESCE(cr.data_comp, r.dt_evento_src, r.received_at)) AS data_key,
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
    WHERE COALESCE(etl.business_date(cr.data_comp), etl.business_date(r.dt_evento_src), CURRENT_DATE) >= v_cutoff
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
      AND (
        t.received_at > v_wm
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

  SELECT MAX(received_at) INTO v_max
  FROM stg.turnos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'turnos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
