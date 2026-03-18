DROP MATERIALIZED VIEW IF EXISTS mart.pagamentos_anomalias_diaria;

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
), unioned AS (
  SELECT * FROM split_signal
  UNION ALL
  SELECT * FROM unknown_signal
  UNION ALL
  SELECT * FROM pix_signal
), keyed AS (
  SELECT
    u.*,
    (
      u.event_type || '|' || u.id_empresa::text || '|' || u.id_filial::text || '|' || u.data_key::text || '|' || COALESCE(u.id_turno::text, '-')
    ) AS insight_id
  FROM unioned u
)
SELECT
  k.id_empresa,
  k.id_filial,
  k.data_key,
  k.id_turno,
  k.event_type,
  k.severity,
  k.score,
  k.impacto_estimado,
  k.reasons,
  k.insight_id,
  (('x' || substr(md5(k.insight_id), 1, 16))::bit(64)::bigint) AS insight_id_hash,
  now() AS updated_at
FROM keyed k
WHERE k.severity IN ('WARN','CRITICAL');

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
        ((
          'x' || substr(
            md5(
              p.event_type || '|' || p.id_empresa::text || '|' || p.id_filial::text || '|' || p.data_key::text || '|' || COALESCE(p.id_turno::text, '-')
            ),
            1,
            16
          )
        )::bit(64)::bigint)
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
        ((
          'x' || substr(
            md5(
              p.event_type || '|' || p.id_empresa::text || '|' || p.id_filial::text || '|' || p.data_key::text || '|' || COALESCE(p.id_turno::text, '-')
            ),
            1,
            16
          )
        )::bit(64)::bigint)
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
  v_meta jsonb := '{}'::jsonb;
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
  v_refresh_meta jsonb := '{}'::jsonb;
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

  IF p_refresh_mart THEN
    v_step_started := clock_timestamp();
    v_refresh_meta := etl.refresh_marts(v_meta);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true, 'mart_refresh', v_refresh_meta, 'mart_refresh_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'refresh_marts', v_step_started, clock_timestamp(), 'ok', 1, NULL, jsonb_build_object('ms', v_step_ms, 'refresh', v_refresh_meta));

    v_step_started := clock_timestamp();
    v_rows := etl.sync_payment_anomaly_notifications(p_id_empresa, v_effective_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('payment_notifications', v_rows, 'payment_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'payment_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

    v_step_started := clock_timestamp();
    v_rows := etl.sync_cash_open_notifications(p_id_empresa);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('cash_notifications', v_rows, 'cash_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'cash_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_meta := v_meta || jsonb_build_object(
      'mart_refreshed', false,
      'payment_notifications', 0,
      'payment_notifications_skipped', true,
      'cash_notifications', 0,
      'cash_notifications_skipped', true
    );
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.generate_insights(p_id_empresa, v_effective_ref_date, 7);
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
    'ref_date', v_effective_ref_date,
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
$function$;
