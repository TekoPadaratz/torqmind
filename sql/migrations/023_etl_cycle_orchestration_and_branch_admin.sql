BEGIN;

CREATE OR REPLACE FUNCTION etl.runtime_ref_date(p_default date DEFAULT CURRENT_DATE)
RETURNS date AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.ref_date', true), '')::date, p_default);
$$ LANGUAGE sql STABLE;

DROP MATERIALIZED VIEW IF EXISTS mart.clientes_churn_risco CASCADE;
CREATE MATERIALIZED VIEW mart.clientes_churn_risco AS
WITH ref AS (
  SELECT etl.runtime_ref_date() AS ref_date
), sales AS (
  SELECT
    v.id_empresa,
    v.id_filial,
    COALESCE(v.id_cliente, -1) AS id_cliente,
    MAX(v.data::date) AS last_purchase,
    COUNT(DISTINCT CASE WHEN v.data::date >= ref.ref_date - interval '30 day' THEN v.id_comprovante END)::int AS compras_30d,
    COUNT(DISTINCT CASE WHEN v.data::date >= ref.ref_date - interval '60 day' AND v.data::date < ref.ref_date - interval '30 day' THEN v.id_comprovante END)::int AS compras_60_30,
    COALESCE(SUM(CASE WHEN v.data::date >= ref.ref_date - interval '30 day' THEN i.total ELSE 0 END),0)::numeric(18,2) AS faturamento_30d,
    COALESCE(SUM(CASE WHEN v.data::date >= ref.ref_date - interval '60 day' AND v.data::date < ref.ref_date - interval '30 day' THEN i.total ELSE 0 END),0)::numeric(18,2) AS faturamento_60_30
  FROM dw.fact_venda v
  CROSS JOIN ref
  JOIN dw.fact_venda_item i
    ON i.id_empresa = v.id_empresa
   AND i.id_filial = v.id_filial
   AND i.id_db = v.id_db
   AND i.id_movprodutos = v.id_movprodutos
  WHERE COALESCE(v.cancelado,false) = false
    AND COALESCE(i.cfop,0) >= 5000
    AND v.data::date >= ref.ref_date - interval '120 day'
  GROUP BY 1,2,3
), churn AS (
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_cliente,
    c.nome AS cliente_nome,
    s.last_purchase,
    s.compras_30d,
    s.compras_60_30,
    s.faturamento_30d,
    s.faturamento_60_30,
    LEAST(100,
      GREATEST(0,
        CASE
          WHEN s.id_cliente = -1 THEN 0
          WHEN s.last_purchase < ref.ref_date - interval '60 day' THEN 95
          WHEN s.last_purchase < ref.ref_date - interval '30 day' THEN 80
          ELSE 40
        END
        + CASE WHEN s.compras_60_30 > 0 AND s.compras_30d = 0 THEN 20 ELSE 0 END
        + CASE WHEN s.faturamento_60_30 > 0 AND s.faturamento_30d < (s.faturamento_60_30 * 0.60) THEN 20 ELSE 0 END
      )
    )::int AS churn_score,
    jsonb_build_object(
      'ref_date', ref.ref_date,
      'last_purchase', s.last_purchase,
      'compras_30d', s.compras_30d,
      'compras_60_30', s.compras_60_30,
      'faturamento_30d', s.faturamento_30d,
      'faturamento_60_30', s.faturamento_60_30
    ) AS reasons,
    now() AS updated_at
  FROM sales s
  CROSS JOIN ref
  LEFT JOIN dw.dim_cliente c
    ON c.id_empresa = s.id_empresa
   AND c.id_filial = s.id_filial
   AND c.id_cliente = s.id_cliente
)
SELECT * FROM churn;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_clientes_churn_risco
  ON mart.clientes_churn_risco (id_empresa, id_filial, id_cliente);
CREATE INDEX IF NOT EXISTS ix_mart_clientes_churn_risco_score
  ON mart.clientes_churn_risco (id_empresa, id_filial, churn_score DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_produtos_diaria
  ON mart.agg_produtos_diaria (id_empresa, id_filial, data_key, COALESCE(id_produto, -1));

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_grupos_diaria
  ON mart.agg_grupos_diaria (id_empresa, id_filial, data_key, id_grupo_produto);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_funcionarios_diaria
  ON mart.agg_funcionarios_diaria (id_empresa, id_filial, data_key, id_funcionario);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_fraude_cancelamentos_eventos
  ON mart.fraude_cancelamentos_eventos (id_empresa, id_filial, id_db, id_comprovante);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_financeiro_vencimentos_diaria
  ON mart.financeiro_vencimentos_diaria (id_empresa, id_filial, data_key, tipo_titulo);

CREATE OR REPLACE FUNCTION etl.load_dim_filial(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      COALESCE(payload->>'NOMEFILIAL', payload->>'NOME', payload->>'RAZAOSOCIALFILIAL', '') AS nome,
      COALESCE(payload->>'CNPJ', payload->>'cnpj', payload->>'CNPJCPF', NULL) AS cnpj,
      COALESCE(payload->>'RAZAOSOCIALFILIAL', NULL) AS razao_social
    FROM stg.filiais
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_filial (id_empresa,id_filial,nome,cnpj,razao_social)
  SELECT id_empresa,id_filial,nome,cnpj,razao_social FROM src
  ON CONFLICT (id_empresa,id_filial)
  DO UPDATE SET
    nome = EXCLUDED.nome,
    cnpj = EXCLUDED.cnpj,
    razao_social = EXCLUDED.razao_social;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO auth.filiais (id_empresa,id_filial,nome,cnpj,is_active,valid_from)
  SELECT id_empresa,id_filial,nome,cnpj,true,CURRENT_DATE
  FROM dw.dim_filial
  WHERE id_empresa = p_id_empresa
  ON CONFLICT (id_empresa,id_filial) DO NOTHING;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.change_domains(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
  SELECT jsonb_build_object(
    'sales',
      COALESCE((p_changed->>'force_full')::boolean, false)
      OR COALESCE((p_changed->>'dim_grupos')::int,0) > 0
      OR COALESCE((p_changed->>'dim_produtos')::int,0) > 0
      OR COALESCE((p_changed->>'dim_funcionarios')::int,0) > 0
      OR COALESCE((p_changed->>'dim_clientes')::int,0) > 0
      OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0
      OR COALESCE((p_changed->>'fact_venda')::int,0) > 0
      OR COALESCE((p_changed->>'fact_venda_item')::int,0) > 0,
    'finance',
      COALESCE((p_changed->>'force_full')::boolean, false)
      OR COALESCE((p_changed->>'fact_financeiro')::int,0) > 0,
    'risk',
      COALESCE((p_changed->>'force_full')::boolean, false)
      OR COALESCE((p_changed->>'risk_events')::int,0) > 0
      OR COALESCE((p_changed->>'dim_funcionarios')::int,0) > 0,
    'payments',
      COALESCE((p_changed->>'force_full')::boolean, false)
      OR COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
      OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0,
    'cash',
      COALESCE((p_changed->>'force_full')::boolean, false)
      OR COALESCE((p_changed->>'fact_caixa_turno')::int,0) > 0
      OR COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
      OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0
      OR COALESCE((p_changed->>'dim_usuario_caixa')::int,0) > 0
  );
$$ LANGUAGE sql STABLE;

DROP FUNCTION IF EXISTS etl.refresh_marts(jsonb);

CREATE OR REPLACE FUNCTION etl.refresh_marts(
  p_changed jsonb DEFAULT '{}'::jsonb,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb AS $$
DECLARE
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
  v_domains jsonb := etl.change_domains(p_changed);
  v_meta jsonb := jsonb_build_object(
    'ref_date', v_effective_ref_date,
    'sales_marts_refreshed', false,
    'finance_mart_refreshed', false,
    'risk_marts_refreshed', false,
    'payments_marts_refreshed', false,
    'cash_marts_refreshed', false,
    'anonymous_retention_refreshed', false,
    'concurrent_candidates', jsonb_build_array(
      'mart.agg_vendas_diaria',
      'mart.insights_base_diaria',
      'mart.agg_vendas_hora',
      'mart.agg_produtos_diaria',
      'mart.agg_grupos_diaria',
      'mart.agg_funcionarios_diaria',
      'mart.fraude_cancelamentos_diaria',
      'mart.fraude_cancelamentos_eventos',
      'mart.clientes_churn_risco',
      'mart.financeiro_vencimentos_diaria',
      'mart.agg_risco_diaria',
      'mart.risco_top_funcionarios_diaria',
      'mart.risco_turno_local_diaria',
      'mart.agg_pagamentos_diaria',
      'mart.agg_pagamentos_turno',
      'mart.pagamentos_anomalias_diaria',
      'mart.agg_caixa_turno_aberto',
      'mart.agg_caixa_forma_pagamento',
      'mart.agg_caixa_cancelamentos',
      'mart.alerta_caixa_aberto',
      'mart.anonymous_retention_daily'
    ),
    'concurrent_enabled', false
  );
  v_sales_changed boolean := COALESCE((v_domains->>'sales')::boolean, false);
  v_fin_changed boolean := COALESCE((v_domains->>'finance')::boolean, false);
  v_risk_changed boolean := COALESCE((v_domains->>'risk')::boolean, false);
  v_payment_changed boolean := COALESCE((v_domains->>'payments')::boolean, false);
  v_cash_changed boolean := COALESCE((v_domains->>'cash')::boolean, false);
BEGIN
  PERFORM set_config('etl.ref_date', v_effective_ref_date::text, true);

  IF v_sales_changed THEN
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
    v_meta := v_meta || jsonb_build_object(
      'sales_marts_refreshed', true,
      'anonymous_retention_refreshed', true
    );
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', true);
  END IF;

  IF v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', true);
  END IF;

  IF v_payment_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
    REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', true);
  END IF;

  IF v_cash_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
    REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
    REFRESH MATERIALIZED VIEW mart.agg_caixa_cancelamentos;
    REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;
    v_meta := v_meta || jsonb_build_object('cash_marts_refreshed', true);
  END IF;

  RETURN v_meta || jsonb_build_object(
    'refresh_domains', v_domains,
    'refreshed_any', (v_sales_changed OR v_fin_changed OR v_risk_changed OR v_payment_changed OR v_cash_changed)
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.run_tenant_phase(
  p_id_empresa integer,
  p_force_full boolean DEFAULT false,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb
LANGUAGE plpgsql
AS $function$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_meta jsonb := jsonb_build_object('force_full', p_force_full);
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
BEGIN
  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_filial(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_filial', v_rows, 'dim_filial_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_filial', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_grupos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_grupos', v_rows, 'dim_grupos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_grupos', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_localvendas(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_localvendas', v_rows, 'dim_localvendas_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_localvendas', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_produtos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_produtos', v_rows, 'dim_produtos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_produtos', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_funcionarios(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', v_rows, 'dim_funcionarios_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_funcionarios', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_usuario_caixa(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_usuario_caixa', v_rows, 'dim_usuario_caixa_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_usuario_caixa', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_clientes(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_clientes', v_rows, 'dim_clientes_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_clientes', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_comprovante', v_rows, 'fact_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_comprovante', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_caixa_turno(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_caixa_turno', v_rows, 'fact_caixa_turno_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_caixa_turno', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_pagamento_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_pagamento_comprovante', v_rows, 'fact_pagamento_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_pagamento_comprovante', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_venda(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda', v_rows, 'fact_venda_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_venda_item(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda_item', v_rows, 'fact_venda_item_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda_item', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_financeiro(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_financeiro', v_rows, 'fact_financeiro_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_financeiro', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  IF p_force_full
     OR COALESCE((v_meta->>'fact_comprovante')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda_item')::int,0) > 0
     OR COALESCE((v_meta->>'fact_pagamento_comprovante')::int,0) > 0
  THEN
    v_step_started := clock_timestamp();
    v_rows := etl.compute_risk_events(p_id_empresa, p_force_full, 14, NULL);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('risk_events', v_rows, 'risk_events_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'risk_events', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_rows := 0;
    v_meta := v_meta || jsonb_build_object('risk_events', 0, 'risk_events_skipped', true, 'risk_events_skip_reason', 'no_fact_changes');
    PERFORM etl.log_step(p_id_empresa, 'risk_events', clock_timestamp(), clock_timestamp(), 'ok', 0, NULL, jsonb_build_object('skipped', true, 'reason', 'no_fact_changes'));
  END IF;

  v_meta := v_meta || jsonb_build_object('refresh_domains', etl.change_domains(v_meta));

  PERFORM etl.log_step(
    p_id_empresa,
    'run_tenant_phase',
    v_started,
    clock_timestamp(),
    'ok',
    1,
    NULL,
    jsonb_build_object('force_full', p_force_full, 'meta', v_meta)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'ref_date', v_effective_ref_date,
    'hot_window_days', etl.hot_window_days(),
    'started_at', v_started,
    'finished_at', clock_timestamp(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM etl.log_step(
    p_id_empresa,
    'run_tenant_phase',
    v_started,
    clock_timestamp(),
    'failed',
    0,
    SQLERRM,
    jsonb_build_object('meta_partial', v_meta)
  );
  RAISE;
END;
$function$;

CREATE OR REPLACE FUNCTION etl.run_tenant_post_refresh(
  p_id_empresa integer,
  p_changed jsonb DEFAULT '{}'::jsonb,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb
LANGUAGE plpgsql
AS $function$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_meta jsonb := jsonb_build_object(
    'customer_sales_daily_refreshed', false,
    'customer_rfm_refreshed', false,
    'customer_churn_risk_refreshed', false,
    'finance_aging_refreshed', false,
    'health_score_refreshed', false,
    'payment_notifications', 0,
    'cash_notifications', 0,
    'insights_generated', 0
  );
  v_domains jsonb := etl.change_domains(p_changed);
  v_sales_changed boolean := COALESCE((v_domains->>'sales')::boolean, false);
  v_fin_changed boolean := COALESCE((v_domains->>'finance')::boolean, false);
  v_risk_changed boolean := COALESCE((v_domains->>'risk')::boolean, false);
  v_payment_changed boolean := COALESCE((v_domains->>'payments')::boolean, false);
  v_cash_changed boolean := COALESCE((v_domains->>'cash')::boolean, false);
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
  v_window_days integer := GREATEST(1, etl.hot_window_days());
  v_window_start date := COALESCE(p_ref_date, CURRENT_DATE) - GREATEST(1, etl.hot_window_days());
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
BEGIN
  IF v_sales_changed THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_sales_daily_range(p_id_empresa, v_window_start, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'customer_sales_daily_refreshed', true,
      'customer_sales_daily_rows', v_rows,
      'customer_sales_daily_ms', v_step_ms
    );
    PERFORM etl.log_step(p_id_empresa, 'customer_sales_daily_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_window_start, 'end_dt_ref', v_effective_ref_date));

    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_rfm_range(p_id_empresa, v_window_start, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'customer_rfm_refreshed', true,
      'customer_rfm_rows', v_rows,
      'customer_rfm_ms', v_step_ms
    );
    PERFORM etl.log_step(p_id_empresa, 'customer_rfm_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_window_start, 'end_dt_ref', v_effective_ref_date));

    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_churn_risk_range(p_id_empresa, v_window_start, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'customer_churn_risk_refreshed', true,
      'customer_churn_risk_rows', v_rows,
      'customer_churn_risk_ms', v_step_ms
    );
    PERFORM etl.log_step(p_id_empresa, 'customer_churn_risk_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_window_start, 'end_dt_ref', v_effective_ref_date));
  ELSE
    v_meta := v_meta || jsonb_build_object(
      'customer_sales_daily_skipped', true,
      'customer_rfm_skipped', true,
      'customer_churn_risk_skipped', true
    );
  END IF;

  IF v_fin_changed THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_finance_aging_range(p_id_empresa, v_window_start, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'finance_aging_refreshed', true,
      'finance_aging_rows', v_rows,
      'finance_aging_ms', v_step_ms
    );
    PERFORM etl.log_step(p_id_empresa, 'finance_aging_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_window_start, 'end_dt_ref', v_effective_ref_date));
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_aging_skipped', true);
  END IF;

  IF v_sales_changed OR v_fin_changed OR v_risk_changed THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_health_score_range(p_id_empresa, v_window_start, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'health_score_refreshed', true,
      'health_score_rows', v_rows,
      'health_score_ms', v_step_ms
    );
    PERFORM etl.log_step(p_id_empresa, 'health_score_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_window_start, 'end_dt_ref', v_effective_ref_date));
  ELSE
    v_meta := v_meta || jsonb_build_object('health_score_skipped', true);
  END IF;

  IF v_payment_changed THEN
    v_step_started := clock_timestamp();
    v_rows := etl.sync_payment_anomaly_notifications(p_id_empresa, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('payment_notifications', v_rows, 'payment_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'payment_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_meta := v_meta || jsonb_build_object('payment_notifications_skipped', true);
  END IF;

  IF v_cash_changed THEN
    v_step_started := clock_timestamp();
    v_rows := etl.sync_cash_open_notifications(p_id_empresa);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('cash_notifications', v_rows, 'cash_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'cash_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_meta := v_meta || jsonb_build_object('cash_notifications_skipped', true);
  END IF;

  IF v_sales_changed OR v_fin_changed OR v_risk_changed THEN
    v_step_started := clock_timestamp();
    v_rows := etl.generate_insights(p_id_empresa, v_effective_ref_date, 7);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('insights_generated', v_rows, 'insights_generated_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'insights_generated', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_meta := v_meta || jsonb_build_object('insights_generated_skipped', true);
  END IF;

  PERFORM etl.log_step(
    p_id_empresa,
    'run_tenant_post_refresh',
    v_started,
    clock_timestamp(),
    'ok',
    1,
    NULL,
    jsonb_build_object('ref_date', v_effective_ref_date, 'window_days', v_window_days, 'window_start', v_window_start, 'meta', v_meta)
  );

  RETURN v_meta || jsonb_build_object(
    'snapshot_window_days', v_window_days,
    'snapshot_window_start_dt_ref', v_window_start,
    'snapshot_window_end_dt_ref', v_effective_ref_date
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM etl.log_step(
    p_id_empresa,
    'run_tenant_post_refresh',
    v_started,
    clock_timestamp(),
    'failed',
    0,
    SQLERRM,
    jsonb_build_object('meta_partial', v_meta, 'ref_date', v_effective_ref_date)
  );
  RAISE;
END;
$function$;

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa integer,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb
LANGUAGE plpgsql
AS $function$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
  v_phase_result jsonb;
  v_phase_meta jsonb := '{}'::jsonb;
  v_refresh_meta jsonb := jsonb_build_object(
    'refreshed_any', false,
    'sales_marts_refreshed', false,
    'finance_mart_refreshed', false,
    'risk_marts_refreshed', false,
    'payments_marts_refreshed', false,
    'cash_marts_refreshed', false,
    'anonymous_retention_refreshed', false
  );
  v_post_meta jsonb := jsonb_build_object(
    'customer_sales_daily_refreshed', false,
    'customer_rfm_refreshed', false,
    'customer_churn_risk_refreshed', false,
    'finance_aging_refreshed', false,
    'health_score_refreshed', false,
    'payment_notifications', 0,
    'cash_notifications', 0,
    'insights_generated', 0
  );
  v_meta jsonb := '{}'::jsonb;
BEGIN
  v_phase_result := etl.run_tenant_phase(p_id_empresa, p_force_full, v_effective_ref_date);
  v_phase_meta := COALESCE(v_phase_result->'meta', '{}'::jsonb);

  IF p_refresh_mart THEN
    v_refresh_meta := etl.refresh_marts(v_phase_meta || jsonb_build_object('force_full', p_force_full), v_effective_ref_date);
    IF COALESCE((v_refresh_meta->>'refreshed_any')::boolean, false) THEN
      v_post_meta := etl.run_tenant_post_refresh(p_id_empresa, v_phase_meta || jsonb_build_object('force_full', p_force_full), v_effective_ref_date);
    ELSE
      v_post_meta := v_post_meta || jsonb_build_object(
        'payment_notifications_skipped', true,
        'cash_notifications_skipped', true,
        'insights_generated_skipped', true
      );
    END IF;
  ELSE
    v_refresh_meta := v_refresh_meta || jsonb_build_object('refresh_requested', false);
    v_post_meta := v_post_meta || jsonb_build_object(
      'payment_notifications_skipped', true,
      'cash_notifications_skipped', true,
      'insights_generated_skipped', true
    );
  END IF;

  v_meta := v_phase_meta
    || jsonb_build_object(
      'mart_refreshed', COALESCE((v_refresh_meta->>'refreshed_any')::boolean, false),
      'mart_refresh', v_refresh_meta
    )
    || v_post_meta;

  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    clock_timestamp(),
    'ok',
    1,
    NULL,
    jsonb_build_object('force_full', p_force_full, 'refresh_mart', p_refresh_mart, 'meta', v_meta)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'ref_date', v_effective_ref_date,
    'hot_window_days', COALESCE((v_phase_result->>'hot_window_days')::int, etl.hot_window_days()),
    'started_at', v_started,
    'finished_at', clock_timestamp(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    clock_timestamp(),
    'failed',
    0,
    SQLERRM,
    jsonb_build_object('meta_partial', v_meta)
  );
  RAISE;
END;
$function$;

COMMIT;
