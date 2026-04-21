BEGIN;

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
    'fraud_cancel_marts_refreshed', false,
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
  v_fraud_cancel_marts_refreshed boolean := false;
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
    v_fraud_cancel_marts_refreshed := true;
    v_meta := v_meta || jsonb_build_object(
      'sales_marts_refreshed', true,
      'anonymous_retention_refreshed', true,
      'fraud_cancel_marts_refreshed', true
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
    IF NOT v_sales_changed THEN
      REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
      REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
      v_fraud_cancel_marts_refreshed := true;
    END IF;

    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object(
      'risk_marts_refreshed', true,
      'fraud_cancel_marts_refreshed', v_fraud_cancel_marts_refreshed
    );
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
    'fraud_cancel_marts_refreshed', v_fraud_cancel_marts_refreshed,
    'refreshed_any', (
      v_sales_changed
      OR v_fin_changed
      OR v_risk_changed
      OR v_payment_changed
      OR v_cash_changed
      OR v_churn_clock_executed
      OR v_cash_open_clock_executed
    )
  );
END;
$$ LANGUAGE plpgsql;

COMMIT;
