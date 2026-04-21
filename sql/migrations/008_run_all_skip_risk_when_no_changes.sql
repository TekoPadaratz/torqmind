BEGIN;

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_meta jsonb := '{}'::jsonb;
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
  v_refresh_meta jsonb := '{}'::jsonb;
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
  THEN
    v_step_started := clock_timestamp();
    v_rows := etl.compute_risk_events(p_id_empresa, p_force_full, 14);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('risk_events', v_rows, 'risk_events_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'risk_events', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_rows := 0;
    v_meta := v_meta || jsonb_build_object('risk_events', 0, 'risk_events_skipped', true, 'risk_events_skip_reason', 'no_fact_changes');
    PERFORM etl.log_step(p_id_empresa, 'risk_events', clock_timestamp(), clock_timestamp(), 'ok', 0, NULL, jsonb_build_object('skipped', true, 'reason', 'no_fact_changes'));
  END IF;

  IF p_refresh_mart THEN
    v_step_started := clock_timestamp();
    v_refresh_meta := etl.refresh_marts(v_meta);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true, 'mart_refresh', v_refresh_meta, 'mart_refresh_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'refresh_marts', v_step_started, clock_timestamp(), 'ok', 1, NULL, jsonb_build_object('ms', v_step_ms, 'refresh', v_refresh_meta));
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false);
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.generate_insights(p_id_empresa, CURRENT_DATE, 7);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('insights_generated', v_rows, 'insights_generated_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'insights_generated', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

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
    'hot_window_days', etl.hot_window_days(),
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
$$ LANGUAGE plpgsql;

COMMIT;
