BEGIN;

CREATE OR REPLACE FUNCTION etl.runtime_now(p_default timestamptz DEFAULT now())
RETURNS timestamptz AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.now', true), '')::timestamptz, p_default);
$$ LANGUAGE sql STABLE;

DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_turno_aberto CASCADE;

CREATE MATERIALIZED VIEW mart.agg_caixa_turno_aberto AS
WITH runtime AS (
  SELECT etl.runtime_now() AS clock_ts
), comprovantes_caixa AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_turno,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool), 0)::numeric(18,2) AS total_vendas,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool)::int AS qtd_vendas,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS qtd_cancelamentos
  FROM (
    SELECT
      fc.id_empresa,
      fc.id_filial,
      fc.id_turno,
      fc.valor_total,
      COALESCE(fc.cancelado, false) AS cancelado_bool,
      etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
    FROM dw.fact_comprovante fc
    WHERE fc.id_turno IS NOT NULL
  ) c
  GROUP BY c.id_empresa, c.id_filial, c.id_turno
), pagamentos_turno AS (
  SELECT
    p.id_empresa,
    p.id_filial,
    p.id_turno,
    COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
  FROM dw.fact_pagamento_comprovante p
  WHERE p.id_turno IS NOT NULL
  GROUP BY p.id_empresa, p.id_filial, p.id_turno
)
SELECT
  t.id_empresa,
  t.id_filial,
  COALESCE(f.nome, '') AS filial_nome,
  t.id_turno,
  t.id_usuario,
  COALESCE(NULLIF(u.nome, ''), format('Usuário %s', t.id_usuario)) AS usuario_nome,
  t.abertura_ts,
  t.fechamento_ts,
  ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2)::numeric(10,2) AS horas_aberto,
  CASE
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'CRITICAL'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'HIGH'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  CASE
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'Crítico'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'Atenção alta'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'Monitorar'
    ELSE 'Dentro da janela'
  END AS status_label,
  COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
  COALESCE(c.qtd_vendas, 0)::int AS qtd_vendas,
  COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
  COALESCE(c.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
  COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos,
  runtime.clock_ts AS updated_at
FROM dw.fact_caixa_turno t
CROSS JOIN runtime
LEFT JOIN auth.filiais f
  ON f.id_empresa = t.id_empresa
 AND f.id_filial = t.id_filial
LEFT JOIN dw.dim_usuario_caixa u
  ON u.id_empresa = t.id_empresa
 AND u.id_filial = t.id_filial
 AND u.id_usuario = t.id_usuario
LEFT JOIN comprovantes_caixa c
  ON c.id_empresa = t.id_empresa
 AND c.id_filial = t.id_filial
 AND c.id_turno = t.id_turno
LEFT JOIN pagamentos_turno p
  ON p.id_empresa = t.id_empresa
 AND p.id_filial = t.id_filial
 AND p.id_turno = t.id_turno
WHERE t.is_aberto = true
  AND t.abertura_ts IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_turno_aberto
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_turno_aberto_lookup
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, severity, horas_aberto DESC);

CREATE MATERIALIZED VIEW mart.alerta_caixa_aberto AS
WITH runtime AS (
  SELECT etl.runtime_now() AS clock_ts
)
SELECT
  a.id_empresa,
  a.id_filial,
  a.filial_nome,
  a.id_turno,
  a.id_usuario,
  a.usuario_nome,
  a.abertura_ts,
  a.horas_aberto,
  'CRITICAL'::text AS severity,
  format('Caixa %s aberto há %s horas', a.id_turno, trim(to_char(a.horas_aberto, 'FM999999990D00'))) AS title,
  format(
    'O caixa %s da filial %s está aberto há %s horas. Operador: %s.',
    a.id_turno,
    COALESCE(NULLIF(a.filial_nome, ''), format('Filial %s', a.id_filial)),
    trim(to_char(a.horas_aberto, 'FM999999990D00')),
    COALESCE(NULLIF(a.usuario_nome, ''), 'não identificado')
  ) AS body,
  '/cash'::text AS url,
  (
    ('x' || substr(md5(
      'CASH_OPEN_OVER_24H|' || a.id_empresa::text || '|' || a.id_filial::text || '|' || a.id_turno::text
    ), 1, 16))::bit(64)::bigint
  ) AS insight_id_hash,
  runtime.clock_ts AS updated_at
FROM mart.agg_caixa_turno_aberto a
CROSS JOIN runtime
WHERE a.horas_aberto >= 24;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_alerta_caixa_aberto
  ON mart.alerta_caixa_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_alerta_caixa_aberto_lookup
  ON mart.alerta_caixa_aberto (id_empresa, severity, horas_aberto DESC);

CREATE OR REPLACE FUNCTION etl.daily_rollover_window(
  p_last_dt_ref date,
  p_ref_date date DEFAULT CURRENT_DATE,
  p_window_days integer DEFAULT NULL,
  p_allow_bootstrap boolean DEFAULT false
)
RETURNS TABLE(start_dt_ref date, end_dt_ref date)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
  v_window_days integer := GREATEST(1, COALESCE(p_window_days, etl.hot_window_days()));
BEGIN
  IF p_last_dt_ref IS NULL THEN
    IF p_allow_bootstrap THEN
      start_dt_ref := v_effective_ref_date;
      end_dt_ref := v_effective_ref_date;
      RETURN NEXT;
    END IF;
    RETURN;
  END IF;

  IF p_last_dt_ref >= v_effective_ref_date THEN
    RETURN;
  END IF;

  start_dt_ref := p_last_dt_ref + 1;
  end_dt_ref := LEAST(v_effective_ref_date, start_dt_ref + GREATEST(v_window_days - 1, 0));
  RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION etl.collect_tenant_clock_meta(
  p_id_empresa integer,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_effective_ref_date date := COALESCE(p_ref_date, CURRENT_DATE);
  v_window_days integer := GREATEST(1, etl.hot_window_days());
  v_churn_snapshot_base_exists boolean := false;
  v_churn_mart_base_exists boolean := false;
  v_finance_base_exists boolean := false;
  v_health_base_exists boolean := false;
  v_open_cash_turns boolean := false;
  v_churn_last_dt date;
  v_finance_last_dt date;
  v_health_last_dt date;
  v_churn_start date;
  v_churn_end date;
  v_finance_start date;
  v_finance_end date;
  v_health_candidate_start date;
  v_health_candidate_end date;
  v_health_start date;
  v_health_end date;
  v_churn_constraint_end date;
  v_finance_constraint_end date;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM mart.customer_sales_daily
    WHERE id_empresa = p_id_empresa
    LIMIT 1
  )
  INTO v_churn_snapshot_base_exists;

  SELECT EXISTS (
    SELECT 1
    FROM dw.fact_venda
    WHERE id_empresa = p_id_empresa
      AND id_cliente IS NOT NULL
      AND COALESCE(cancelado, false) = false
    LIMIT 1
  )
  INTO v_churn_mart_base_exists;

  SELECT EXISTS (
    SELECT 1
    FROM dw.fact_financeiro
    WHERE id_empresa = p_id_empresa
    LIMIT 1
  )
  INTO v_finance_base_exists;

  SELECT EXISTS (
    SELECT 1
    FROM dw.fact_caixa_turno
    WHERE id_empresa = p_id_empresa
      AND is_aberto = true
      AND abertura_ts IS NOT NULL
    LIMIT 1
  )
  INTO v_open_cash_turns;

  SELECT MAX(dt_ref)
  INTO v_churn_last_dt
  FROM mart.customer_churn_risk_daily
  WHERE id_empresa = p_id_empresa;

  SELECT MAX(dt_ref)
  INTO v_finance_last_dt
  FROM mart.finance_aging_daily
  WHERE id_empresa = p_id_empresa;

  SELECT MAX(dt_ref)
  INTO v_health_last_dt
  FROM mart.health_score_daily
  WHERE id_empresa = p_id_empresa;

  SELECT (
    EXISTS (SELECT 1 FROM mart.agg_vendas_diaria WHERE id_empresa = p_id_empresa LIMIT 1)
    OR EXISTS (SELECT 1 FROM mart.agg_risco_diaria WHERE id_empresa = p_id_empresa LIMIT 1)
    OR v_churn_snapshot_base_exists
    OR v_finance_base_exists
    OR EXISTS (SELECT 1 FROM mart.health_score_daily WHERE id_empresa = p_id_empresa LIMIT 1)
  )
  INTO v_health_base_exists;

  SELECT start_dt_ref, end_dt_ref
  INTO v_churn_start, v_churn_end
  FROM etl.daily_rollover_window(v_churn_last_dt, v_effective_ref_date, v_window_days, v_churn_snapshot_base_exists);

  SELECT start_dt_ref, end_dt_ref
  INTO v_finance_start, v_finance_end
  FROM etl.daily_rollover_window(v_finance_last_dt, v_effective_ref_date, v_window_days, v_finance_base_exists);

  SELECT start_dt_ref, end_dt_ref
  INTO v_health_candidate_start, v_health_candidate_end
  FROM etl.daily_rollover_window(v_health_last_dt, v_effective_ref_date, v_window_days, v_health_base_exists);

  IF v_churn_snapshot_base_exists THEN
    v_churn_constraint_end := COALESCE(
      v_churn_end,
      CASE
        WHEN v_churn_last_dt IS NOT NULL AND v_churn_last_dt >= v_effective_ref_date THEN v_effective_ref_date
        ELSE NULL
      END
    );
  END IF;

  IF v_finance_base_exists THEN
    v_finance_constraint_end := COALESCE(
      v_finance_end,
      CASE
        WHEN v_finance_last_dt IS NOT NULL AND v_finance_last_dt >= v_effective_ref_date THEN v_effective_ref_date
        ELSE NULL
      END
    );
  END IF;

  IF v_health_candidate_start IS NOT NULL THEN
    v_health_end := LEAST(
      COALESCE(v_health_candidate_end, DATE '9999-12-31'),
      COALESCE(v_churn_constraint_end, DATE '9999-12-31'),
      COALESCE(v_finance_constraint_end, DATE '9999-12-31')
    );

    IF v_health_candidate_start <= v_health_end THEN
      v_health_start := v_health_candidate_start;
    ELSE
      v_health_start := NULL;
      v_health_end := NULL;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'clock_ref_date', v_effective_ref_date,
    'clock_window_days', v_window_days,
    'clock_daily_rollover', (v_churn_start IS NOT NULL OR v_finance_start IS NOT NULL OR v_health_start IS NOT NULL),
    'clock_open_cash_turns', v_open_cash_turns,
    'clock_churn_mart_refresh', (v_churn_mart_base_exists AND (v_churn_last_dt IS NULL OR v_churn_last_dt < v_effective_ref_date)),
    'clock_cash_open_refresh', v_open_cash_turns,
    'clock_customer_rfm_start_dt_ref', v_churn_start,
    'clock_customer_rfm_end_dt_ref', v_churn_end,
    'clock_customer_churn_risk_start_dt_ref', v_churn_start,
    'clock_customer_churn_risk_end_dt_ref', v_churn_end,
    'clock_finance_aging_start_dt_ref', v_finance_start,
    'clock_finance_aging_end_dt_ref', v_finance_end,
    'clock_health_score_start_dt_ref', v_health_start,
    'clock_health_score_end_dt_ref', v_health_end,
    'clock_cash_notifications', v_open_cash_turns,
    'clock_churn_last_dt_ref', v_churn_last_dt,
    'clock_finance_last_dt_ref', v_finance_last_dt,
    'clock_health_last_dt_ref', v_health_last_dt
  );
END;
$$;

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
    'churn_clock_mart_refreshed', false,
    'cash_open_alert_marts_refreshed', false,
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
  v_clock_churn_refresh boolean := COALESCE((p_changed->>'clock_churn_mart_refresh')::boolean, false);
  v_clock_cash_open_refresh boolean := COALESCE((p_changed->>'clock_cash_open_refresh')::boolean, false);
  v_churn_clock_executed boolean := false;
  v_cash_open_clock_executed boolean := false;
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
  ELSIF v_clock_churn_refresh THEN
    REFRESH MATERIALIZED VIEW mart.clientes_churn_risco;
    v_churn_clock_executed := true;
    v_meta := v_meta || jsonb_build_object('churn_clock_mart_refreshed', true);
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
  ELSIF v_clock_cash_open_refresh THEN
    REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
    REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;
    v_cash_open_clock_executed := true;
    v_meta := v_meta || jsonb_build_object('cash_open_alert_marts_refreshed', true);
  END IF;

  RETURN v_meta || jsonb_build_object(
    'refresh_domains', v_domains,
    'clock_refresh', jsonb_build_object(
      'churn_mart', v_clock_churn_refresh,
      'cash_open', v_clock_cash_open_refresh
    ),
    'refreshed_any', (v_sales_changed OR v_fin_changed OR v_risk_changed OR v_payment_changed OR v_cash_changed OR v_churn_clock_executed OR v_cash_open_clock_executed)
  );
END;
$$ LANGUAGE plpgsql;

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
    'insights_generated', 0,
    'customer_rfm_clock_driven', false,
    'customer_churn_risk_clock_driven', false,
    'finance_aging_clock_driven', false,
    'health_score_clock_driven', false,
    'cash_notifications_clock_driven', false
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
  v_clock_customer_rfm_start date := NULLIF(p_changed->>'clock_customer_rfm_start_dt_ref', '')::date;
  v_clock_customer_rfm_end date := NULLIF(p_changed->>'clock_customer_rfm_end_dt_ref', '')::date;
  v_clock_customer_churn_start date := NULLIF(p_changed->>'clock_customer_churn_risk_start_dt_ref', '')::date;
  v_clock_customer_churn_end date := NULLIF(p_changed->>'clock_customer_churn_risk_end_dt_ref', '')::date;
  v_clock_finance_start date := NULLIF(p_changed->>'clock_finance_aging_start_dt_ref', '')::date;
  v_clock_finance_end date := NULLIF(p_changed->>'clock_finance_aging_end_dt_ref', '')::date;
  v_clock_health_start date := NULLIF(p_changed->>'clock_health_score_start_dt_ref', '')::date;
  v_clock_health_end date := NULLIF(p_changed->>'clock_health_score_end_dt_ref', '')::date;
  v_clock_cash_notifications boolean := COALESCE((p_changed->>'clock_cash_notifications')::boolean, false);
  v_customer_sales_start date;
  v_customer_sales_end date;
  v_customer_rfm_start date;
  v_customer_rfm_end date;
  v_customer_churn_start date;
  v_customer_churn_end date;
  v_finance_start date;
  v_finance_end date;
  v_health_start date;
  v_health_end date;
  v_snapshot_window_start date;
  v_snapshot_window_end date;
  v_snapshot_window_days integer := 0;
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
BEGIN
  IF v_sales_changed THEN
    v_customer_sales_start := v_window_start;
    v_customer_sales_end := v_effective_ref_date;
    v_customer_rfm_start := v_window_start;
    v_customer_rfm_end := v_effective_ref_date;
    v_customer_churn_start := v_window_start;
    v_customer_churn_end := v_effective_ref_date;
  ELSE
    v_customer_rfm_start := v_clock_customer_rfm_start;
    v_customer_rfm_end := v_clock_customer_rfm_end;
    v_customer_churn_start := v_clock_customer_churn_start;
    v_customer_churn_end := v_clock_customer_churn_end;
  END IF;

  IF v_fin_changed THEN
    v_finance_start := v_window_start;
    v_finance_end := v_effective_ref_date;
  ELSE
    v_finance_start := v_clock_finance_start;
    v_finance_end := v_clock_finance_end;
  END IF;

  IF v_sales_changed OR v_fin_changed OR v_risk_changed THEN
    v_health_start := v_window_start;
    v_health_end := v_effective_ref_date;
  ELSE
    v_health_start := v_clock_health_start;
    v_health_end := v_clock_health_end;
  END IF;

  IF v_customer_sales_start IS NOT NULL AND v_customer_sales_end IS NOT NULL AND v_customer_sales_start <= v_customer_sales_end THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_sales_daily_range(p_id_empresa, v_customer_sales_start, v_customer_sales_end);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'customer_sales_daily_refreshed', true,
      'customer_sales_daily_rows', v_rows,
      'customer_sales_daily_ms', v_step_ms
    );
    PERFORM etl.log_step(p_id_empresa, 'customer_sales_daily_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_customer_sales_start, 'end_dt_ref', v_customer_sales_end));
  ELSE
    v_meta := v_meta || jsonb_build_object('customer_sales_daily_skipped', true);
  END IF;

  IF v_customer_rfm_start IS NOT NULL AND v_customer_rfm_end IS NOT NULL AND v_customer_rfm_start <= v_customer_rfm_end THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_rfm_range(p_id_empresa, v_customer_rfm_start, v_customer_rfm_end);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'customer_rfm_refreshed', true,
      'customer_rfm_rows', v_rows,
      'customer_rfm_ms', v_step_ms,
      'customer_rfm_clock_driven', (NOT v_sales_changed)
    );
    PERFORM etl.log_step(p_id_empresa, 'customer_rfm_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_customer_rfm_start, 'end_dt_ref', v_customer_rfm_end, 'clock_driven', (NOT v_sales_changed)));
  ELSE
    v_meta := v_meta || jsonb_build_object('customer_rfm_skipped', true);
  END IF;

  IF v_customer_churn_start IS NOT NULL AND v_customer_churn_end IS NOT NULL AND v_customer_churn_start <= v_customer_churn_end THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_churn_risk_range(p_id_empresa, v_customer_churn_start, v_customer_churn_end);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'customer_churn_risk_refreshed', true,
      'customer_churn_risk_rows', v_rows,
      'customer_churn_risk_ms', v_step_ms,
      'customer_churn_risk_clock_driven', (NOT v_sales_changed)
    );
    PERFORM etl.log_step(p_id_empresa, 'customer_churn_risk_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_customer_churn_start, 'end_dt_ref', v_customer_churn_end, 'clock_driven', (NOT v_sales_changed)));
  ELSE
    v_meta := v_meta || jsonb_build_object('customer_churn_risk_skipped', true);
  END IF;

  IF v_finance_start IS NOT NULL AND v_finance_end IS NOT NULL AND v_finance_start <= v_finance_end THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_finance_aging_range(p_id_empresa, v_finance_start, v_finance_end);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'finance_aging_refreshed', true,
      'finance_aging_rows', v_rows,
      'finance_aging_ms', v_step_ms,
      'finance_aging_clock_driven', (NOT v_fin_changed)
    );
    PERFORM etl.log_step(p_id_empresa, 'finance_aging_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_finance_start, 'end_dt_ref', v_finance_end, 'clock_driven', (NOT v_fin_changed)));
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_aging_skipped', true);
  END IF;

  IF v_health_start IS NOT NULL AND v_health_end IS NOT NULL AND v_health_start <= v_health_end THEN
    v_step_started := clock_timestamp();
    v_rows := etl.backfill_health_score_range(p_id_empresa, v_health_start, v_health_end);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'health_score_refreshed', true,
      'health_score_rows', v_rows,
      'health_score_ms', v_step_ms,
      'health_score_clock_driven', (NOT (v_sales_changed OR v_fin_changed OR v_risk_changed))
    );
    PERFORM etl.log_step(p_id_empresa, 'health_score_snapshot', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'start_dt_ref', v_health_start, 'end_dt_ref', v_health_end, 'clock_driven', (NOT (v_sales_changed OR v_fin_changed OR v_risk_changed))));
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

  IF v_cash_changed OR v_clock_cash_notifications THEN
    v_step_started := clock_timestamp();
    v_rows := etl.sync_cash_open_notifications(p_id_empresa);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object(
      'cash_notifications', v_rows,
      'cash_notifications_ms', v_step_ms,
      'cash_notifications_clock_driven', (NOT v_cash_changed AND v_clock_cash_notifications)
    );
    PERFORM etl.log_step(p_id_empresa, 'cash_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms, 'clock_driven', (NOT v_cash_changed AND v_clock_cash_notifications)));
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

  SELECT MIN(bounds.dt_ref), MAX(bounds.dt_ref)
  INTO v_snapshot_window_start, v_snapshot_window_end
  FROM (
    VALUES
      (v_customer_sales_start),
      (v_customer_sales_end),
      (v_customer_rfm_start),
      (v_customer_rfm_end),
      (v_customer_churn_start),
      (v_customer_churn_end),
      (v_finance_start),
      (v_finance_end),
      (v_health_start),
      (v_health_end)
  ) AS bounds(dt_ref)
  WHERE bounds.dt_ref IS NOT NULL;

  IF v_snapshot_window_start IS NOT NULL AND v_snapshot_window_end IS NOT NULL THEN
    v_snapshot_window_days := GREATEST(1, (v_snapshot_window_end - v_snapshot_window_start) + 1);
  END IF;

  PERFORM etl.log_step(
    p_id_empresa,
    'run_tenant_post_refresh',
    v_started,
    clock_timestamp(),
    'ok',
    1,
    NULL,
    jsonb_build_object('ref_date', v_effective_ref_date, 'window_days', v_snapshot_window_days, 'window_start', v_snapshot_window_start, 'meta', v_meta)
  );

  RETURN v_meta || jsonb_build_object(
    'snapshot_window_days', v_snapshot_window_days,
    'snapshot_window_start_dt_ref', v_snapshot_window_start,
    'snapshot_window_end_dt_ref', v_snapshot_window_end,
    'clock_daily_rollover', COALESCE((p_changed->>'clock_daily_rollover')::boolean, false),
    'clock_open_cash_turns', COALESCE((p_changed->>'clock_open_cash_turns')::boolean, false)
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
  v_clock_meta jsonb := '{}'::jsonb;
  v_changed_meta jsonb := '{}'::jsonb;
  v_refresh_meta jsonb := jsonb_build_object(
    'refreshed_any', false,
    'sales_marts_refreshed', false,
    'finance_mart_refreshed', false,
    'risk_marts_refreshed', false,
    'payments_marts_refreshed', false,
    'cash_marts_refreshed', false,
    'anonymous_retention_refreshed', false,
    'churn_clock_mart_refreshed', false,
    'cash_open_alert_marts_refreshed', false
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
  v_clock_meta := etl.collect_tenant_clock_meta(p_id_empresa, v_effective_ref_date);
  v_changed_meta := v_phase_meta || v_clock_meta || jsonb_build_object('force_full', p_force_full);

  IF p_refresh_mart THEN
    v_refresh_meta := etl.refresh_marts(v_changed_meta, v_effective_ref_date);
    IF COALESCE((v_refresh_meta->>'refreshed_any')::boolean, false) THEN
      v_post_meta := etl.run_tenant_post_refresh(p_id_empresa, v_changed_meta, v_effective_ref_date);
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
      'clock_meta', v_clock_meta,
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
