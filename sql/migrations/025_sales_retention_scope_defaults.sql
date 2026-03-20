BEGIN;

ALTER TABLE app.tenants
  ADD COLUMN IF NOT EXISTS sales_history_days integer,
  ADD COLUMN IF NOT EXISTS default_product_scope_days integer;

UPDATE app.tenants
SET sales_history_days = COALESCE(NULLIF(sales_history_days, 0), 365),
    default_product_scope_days = COALESCE(NULLIF(default_product_scope_days, 0), 30);

ALTER TABLE app.tenants
  ALTER COLUMN sales_history_days SET DEFAULT 365,
  ALTER COLUMN sales_history_days SET NOT NULL,
  ALTER COLUMN default_product_scope_days SET DEFAULT 30,
  ALTER COLUMN default_product_scope_days SET NOT NULL;

ALTER TABLE app.tenants
  DROP CONSTRAINT IF EXISTS ck_app_tenants_sales_history_days,
  DROP CONSTRAINT IF EXISTS ck_app_tenants_default_product_scope_days;

ALTER TABLE app.tenants
  ADD CONSTRAINT ck_app_tenants_sales_history_days
    CHECK (sales_history_days BETWEEN 1 AND 3650),
  ADD CONSTRAINT ck_app_tenants_default_product_scope_days
    CHECK (default_product_scope_days BETWEEN 1 AND 365);

CREATE INDEX IF NOT EXISTS ix_stg_comprovantes_emp_filial_referencia
  ON stg.comprovantes (id_empresa, id_filial, (etl.safe_int(payload->>'REFERENCIA')))
  WHERE etl.safe_int(payload->>'REFERENCIA') IS NOT NULL;

CREATE OR REPLACE FUNCTION etl.sales_history_days(p_id_empresa integer)
RETURNS integer AS $$
  SELECT GREATEST(
    1,
    COALESCE(
      (SELECT sales_history_days FROM app.tenants WHERE id_empresa = p_id_empresa),
      365
    )
  );
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.default_product_scope_days(p_id_empresa integer)
RETURNS integer AS $$
  SELECT GREATEST(
    1,
    COALESCE(
      (SELECT default_product_scope_days FROM app.tenants WHERE id_empresa = p_id_empresa),
      30
    )
  );
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.sales_cutoff_date(
  p_id_empresa integer,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS date AS $$
  SELECT COALESCE(p_ref_date, CURRENT_DATE) - GREATEST(etl.sales_history_days(p_id_empresa) - 1, 0);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.sales_business_ts(
  p_payload jsonb,
  p_fallback timestamp DEFAULT NULL
)
RETURNS timestamp AS $$
  SELECT COALESCE(
    etl.safe_timestamp(p_payload->>'TORQMIND_DT_EVENTO'),
    etl.safe_timestamp(p_payload->>'DT_EVENTO'),
    etl.safe_timestamp(p_payload->>'DATA'),
    etl.safe_timestamp(p_payload->>'DATAMOV'),
    etl.safe_timestamp(p_payload->>'DTMOV'),
    p_fallback
  );
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.analyze_hot_tables()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  ANALYZE stg.comprovantes;
  ANALYZE stg.movprodutos;
  ANALYZE stg.itensmovprodutos;
  ANALYZE stg.formas_pgto_comprovantes;
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
        (f.dt_evento IS NOT NULL AND f.dt_evento::date < tc.cutoff_date)
        OR EXISTS (
          SELECT 1
          FROM stg.comprovantes c
          WHERE c.id_empresa = f.id_empresa
            AND c.id_filial = f.id_filial
            AND etl.safe_int(c.payload->>'REFERENCIA') = f.id_referencia
            AND etl.sales_business_ts(c.payload, c.dt_evento)::date < tc.cutoff_date
        )
        OR EXISTS (
          SELECT 1
          FROM dw.fact_pagamento_comprovante p
          WHERE p.id_empresa = f.id_empresa
            AND p.id_filial = f.id_filial
            AND p.referencia = f.id_referencia
            AND p.tipo_forma = f.tipo_forma
            AND p.dt_evento::date < tc.cutoff_date
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
        (i.dt_evento IS NOT NULL AND i.dt_evento::date < tc.cutoff_date)
        OR EXISTS (
          SELECT 1
          FROM stg.movprodutos m
          WHERE m.id_empresa = i.id_empresa
            AND m.id_filial = i.id_filial
            AND m.id_db = i.id_db
            AND m.id_movprodutos = i.id_movprodutos
            AND etl.sales_business_ts(m.payload, m.dt_evento)::date < tc.cutoff_date
        )
        OR EXISTS (
          SELECT 1
          FROM dw.fact_venda v
          WHERE v.id_empresa = i.id_empresa
            AND v.id_filial = i.id_filial
            AND v.id_db = i.id_db
            AND v.id_movprodutos = i.id_movprodutos
            AND v.data::date < tc.cutoff_date
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
      AND p.dt_evento::date < tc.cutoff_date
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
      AND v.data::date < tc.cutoff_date
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
      AND v.data::date < tc.cutoff_date
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
      AND c.data::date < tc.cutoff_date
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
      AND etl.sales_business_ts(m.payload, m.dt_evento)::date < tc.cutoff_date
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
      AND etl.sales_business_ts(c.payload, c.dt_evento)::date < tc.cutoff_date
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

  REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria;
  REFRESH MATERIALIZED VIEW mart.insights_base_diaria;
  REFRESH MATERIALIZED VIEW mart.agg_vendas_hora;
  REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria;
  REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria;
  REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria;
  REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
  REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
  REFRESH MATERIALIZED VIEW mart.clientes_churn_risco;
  REFRESH MATERIALIZED VIEW mart.anonymous_retention_daily;
  REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
  REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
  REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
  REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
  REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
  REFRESH MATERIALIZED VIEW mart.agg_caixa_cancelamentos;
  REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;

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
    'marts_refreshed', jsonb_build_array(
      'mart.agg_vendas_diaria',
      'mart.insights_base_diaria',
      'mart.agg_vendas_hora',
      'mart.agg_produtos_diaria',
      'mart.agg_grupos_diaria',
      'mart.agg_funcionarios_diaria',
      'mart.fraude_cancelamentos_diaria',
      'mart.fraude_cancelamentos_eventos',
      'mart.clientes_churn_risco',
      'mart.anonymous_retention_daily',
      'mart.agg_pagamentos_diaria',
      'mart.agg_pagamentos_turno',
      'mart.pagamentos_anomalias_diaria',
      'mart.agg_caixa_turno_aberto',
      'mart.agg_caixa_forma_pagamento',
      'mart.agg_caixa_cancelamentos',
      'mart.alerta_caixa_aberto'
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
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      etl.sales_business_ts(payload, dt_evento) AS data,
      etl.date_key(etl.sales_business_ts(payload, dt_evento)) AS data_key,
      etl.safe_int(payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_numeric(payload->>'VLRTOTAL')::numeric(18,2) AS valor_total,
      etl.to_bool(payload->>'CANCELADO') AS cancelado,
      etl.safe_int(payload->>'SITUACAO') AS situacao,
      payload
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND etl.sales_business_ts(payload, dt_evento)::date >= v_cutoff
      AND (
        received_at > v_wm
        OR (
          etl.sales_business_ts(payload, dt_evento) IS NOT NULL
          AND etl.sales_business_ts(payload, dt_evento) >= now() - make_interval(days => etl.hot_window_days())
        )
      )
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_turno=EXCLUDED.id_turno,
      id_cliente=EXCLUDED.id_cliente,
      valor_total=EXCLUDED.valor_total,
      cancelado=EXCLUDED.cancelado,
      situacao=EXCLUDED.situacao,
      payload=EXCLUDED.payload
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

  WITH src AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      etl.sales_business_ts(m.payload, m.dt_evento) AS data,
      etl.date_key(etl.sales_business_ts(m.payload, m.dt_evento)) AS data_key,
      etl.safe_int(m.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(m.payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_int(m.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(m.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(m.payload->>'SAIDAS_ENTRADAS') AS saidas_entradas,
      etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2) AS total_venda,
      m.payload
    FROM stg.movprodutos m
    WHERE m.id_empresa = p_id_empresa
      AND etl.sales_business_ts(m.payload, m.dt_evento)::date >= v_cutoff
      AND (
        m.received_at > v_wm
        OR (
          etl.sales_business_ts(m.payload, m.dt_evento) IS NOT NULL
          AND etl.sales_business_ts(m.payload, m.dt_evento) >= now() - make_interval(days => etl.hot_window_days())
        )
      )
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_cliente=EXCLUDED.id_cliente,
      id_comprovante=EXCLUDED.id_comprovante,
      id_turno=EXCLUDED.id_turno,
      saidas_entradas=EXCLUDED.saidas_entradas,
      total_venda=EXCLUDED.total_venda,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
    RETURNING id_empresa,id_filial,id_db,id_movprodutos,id_comprovante
  ), updated_cancel AS (
    UPDATE dw.fact_venda v
    SET cancelado = c.cancelado
    FROM dw.fact_comprovante c, upserted u
    WHERE v.id_empresa = p_id_empresa
      AND u.id_empresa=v.id_empresa
      AND u.id_filial=v.id_filial
      AND u.id_db=v.id_db
      AND u.id_movprodutos=v.id_movprodutos
      AND v.id_empresa = c.id_empresa
      AND v.id_filial = c.id_filial
      AND v.id_db = c.id_db
      AND v.id_comprovante IS NOT NULL
      AND v.id_comprovante = c.id_comprovante
      AND v.cancelado IS DISTINCT FROM c.cancelado
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM upserted),0) + COALESCE((SELECT COUNT(*) FROM updated_cancel),0)
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

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      i.id_itensmovprodutos,
      COALESCE(
        v.data_key,
        etl.date_key(COALESCE(i.dt_evento, etl.sales_business_ts(m.payload, m.dt_evento)))
      ) AS data_key,
      etl.safe_int(i.payload->>'ID_PRODUTOS') AS id_produto,
      etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(i.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_int(i.payload->>'ID_FUNCIONARIOS') AS id_funcionario,
      etl.safe_int(i.payload->>'CFOP') AS cfop,
      etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3) AS qtd,
      etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4) AS valor_unitario,
      etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2) AS total,
      etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2) AS desconto,
      COALESCE(
        (etl.safe_numeric(i.payload->>'VLRCUSTO')::numeric(18,6) * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2),
        (dp.custo_medio * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2)
      ) AS custo_total,
      i.payload
    FROM stg.itensmovprodutos i
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa=i.id_empresa AND v.id_filial=i.id_filial AND v.id_db=i.id_db AND v.id_movprodutos=i.id_movprodutos
    LEFT JOIN stg.movprodutos m
      ON m.id_empresa=i.id_empresa AND m.id_filial=i.id_filial AND m.id_db=i.id_db AND m.id_movprodutos=i.id_movprodutos
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa=i.id_empresa AND dp.id_filial=i.id_filial AND dp.id_produto=etl.safe_int(i.payload->>'ID_PRODUTOS')
    WHERE i.id_empresa = p_id_empresa
      AND COALESCE(i.dt_evento::date, v.data::date, etl.sales_business_ts(m.payload, m.dt_evento)::date) >= v_cutoff
      AND (
        i.received_at > v_wm
        OR (
          COALESCE(i.dt_evento, v.data, etl.sales_business_ts(m.payload, m.dt_evento)) IS NOT NULL
          AND COALESCE(i.dt_evento, v.data, etl.sales_business_ts(m.payload, m.dt_evento)) >= now() - make_interval(days => etl.hot_window_days())
        )
      )
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,margem,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,
      (COALESCE(total,0) - COALESCE(custo_total,0))::numeric(18,2) AS margem,
      payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
    DO UPDATE SET
      data_key=EXCLUDED.data_key,
      id_produto=EXCLUDED.id_produto,
      id_grupo_produto=EXCLUDED.id_grupo_produto,
      id_local_venda=EXCLUDED.id_local_venda,
      id_funcionario=EXCLUDED.id_funcionario,
      cfop=EXCLUDED.cfop,
      qtd=EXCLUDED.qtd,
      valor_unitario=EXCLUDED.valor_unitario,
      total=EXCLUDED.total,
      desconto=EXCLUDED.desconto,
      custo_total=EXCLUDED.custo_total,
      margem=EXCLUDED.margem,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
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
  v_max timestamptz;
  v_rows integer := 0;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

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
        etl.safe_timestamp(s.payload->>'TORQMIND_DT_EVENTO'),
        etl.safe_timestamp(s.payload->>'DT_EVENTO'),
        s.dt_evento,
        etl.safe_timestamp(s.payload->>'DATAHORA'),
        etl.safe_timestamp(s.payload->>'DATA')
      ) AS dt_evento_src,
      COALESCE(s.payload->>'NSU', s.payload->>'nsu') AS nsu,
      COALESCE(s.payload->>'AUTORIZACAO', s.payload->>'autorizacao') AS autorizacao,
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
      etl.sales_business_ts(c.payload, c.dt_evento) AS data_comp,
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
    WHERE r.tipo_forma IS NOT NULL
      AND COALESCE(cr.data_comp::date, r.dt_evento_src::date) >= v_cutoff
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
