CREATE OR REPLACE FUNCTION etl.compute_risk_events(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_lookback_days int DEFAULT 14,
  p_end_ts timestamptz DEFAULT NULL
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
  v_wm timestamptz;
  v_start_ts timestamptz;
  v_min_source_ts timestamptz;
  v_max_source_ts timestamptz;
  v_end_ts timestamptz;
  v_end_key integer;
  v_start_key integer;
  v_start_key_14 integer;
  v_start_key_30 integer;
BEGIN
  v_wm := etl.get_watermark(p_id_empresa, 'risk_events');
  v_end_ts := COALESCE(p_end_ts, 'infinity'::timestamptz);
  v_end_key := CASE
    WHEN p_end_ts IS NULL THEN 99991231
    ELSE etl.date_key((v_end_ts AT TIME ZONE 'UTC')::timestamp)
  END;

  SELECT
    MIN(src.min_ts),
    MAX(src.max_ts)
  INTO v_min_source_ts, v_max_source_ts
  FROM (
    SELECT
      MIN(c.data) AS min_ts,
      MAX(c.data) AS max_ts
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND c.data_key IS NOT NULL
      AND c.data_key <= v_end_key
      AND c.data < (v_end_ts + interval '1 day')

    UNION ALL

    SELECT
      MIN(v.data) AS min_ts,
      MAX(v.data) AS max_ts
    FROM dw.fact_venda v
    WHERE v.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND v.data_key IS NOT NULL
      AND v.data_key <= v_end_key
      AND v.data < (v_end_ts + interval '1 day')
  ) src;

  IF p_force_full THEN
    v_start_ts := COALESCE(v_min_source_ts, now() - interval '90 days');
  ELSE
    v_start_ts := COALESCE(v_wm, COALESCE(v_max_source_ts, now()) - make_interval(days => p_lookback_days)) - interval '1 day';
  END IF;

  v_start_key := etl.date_key((v_start_ts AT TIME ZONE 'UTC')::timestamp);
  v_start_key_14 := etl.date_key(((v_start_ts - interval '14 days') AT TIME ZONE 'UTC')::timestamp);
  v_start_key_30 := etl.date_key(((v_start_ts - interval '30 days') AT TIME ZONE 'UTC')::timestamp);

  WITH
  user_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_usuario,
      COUNT(*)::int AS docs_total,
      COUNT(*) FILTER (WHERE c.cancelado = true)::int AS cancels,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data_key BETWEEN v_start_key_14 AND v_end_key
      AND c.data >= (v_start_ts - interval '14 days')
      AND c.data < (v_end_ts + interval '1 day')
      AND c.id_usuario IS NOT NULL
    GROUP BY 1,2,3
  ),
  filial_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      COUNT(*)::int AS docs_total,
      COUNT(*) FILTER (WHERE c.cancelado = true)::int AS cancels,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data_key BETWEEN v_start_key_14 AND v_end_key
      AND c.data >= (v_start_ts - interval '14 days')
      AND c.data < (v_end_ts + interval '1 day')
    GROUP BY 1,2
  ),
  hour_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      EXTRACT(HOUR FROM c.data)::int AS hour_key,
      COUNT(*) FILTER (WHERE c.cancelado = true)::int AS cancel_count,
      COUNT(*)::int AS docs_total,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data_key BETWEEN v_start_key_30 AND v_end_key
      AND c.data >= (v_start_ts - interval '30 days')
      AND c.data < (v_end_ts + interval '1 day')
      AND c.data IS NOT NULL
    GROUP BY 1,2,3
  ),
  hour_stats_norm AS (
    SELECT
      h.*,
      AVG(h.cancel_rate) OVER (PARTITION BY h.id_empresa, h.id_filial) AS avg_rate,
      COALESCE(STDDEV_POP(h.cancel_rate) OVER (PARTITION BY h.id_empresa, h.id_filial), 0) AS std_rate
    FROM hour_stats h
  ),
  cancel_day_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      percentile_cont(0.90) WITHIN GROUP (ORDER BY c.valor_total) AS p90_valor
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.cancelado = true
      AND c.valor_total IS NOT NULL
      AND c.data_key BETWEEN v_start_key AND v_end_key
    GROUP BY 1,2,3
  ),
  cancel_base AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      (c.data AT TIME ZONE 'UTC') AS data,
      c.id_db,
      c.id_comprovante,
      v.id_movprodutos,
      c.id_usuario,
      COALESCE(v.id_turno, c.id_turno) AS id_turno,
      c.id_cliente,
      fi.id_funcionario,
      c.valor_total,
      COALESCE(p90.p90_valor, 0) AS p90_valor,
      COALESCE(us.cancel_rate, 0) AS usr_cancel_rate,
      COALESCE(us.cancels, 0) AS usr_cancel_count,
      COALESCE(fs.cancel_rate, 0) AS filial_cancel_rate,
      COALESCE(hn.cancel_rate, 0) AS hour_cancel_rate,
      COALESCE(hn.avg_rate, 0) AS hour_avg_rate,
      COALESCE(hn.std_rate, 0) AS hour_std_rate,
      EXISTS (
        SELECT 1
        FROM dw.fact_venda v2
        WHERE v2.id_empresa = c.id_empresa
          AND v2.id_filial = c.id_filial
          AND v2.id_usuario = c.id_usuario
          AND v2.data_key BETWEEN c.data_key AND etl.date_key(((c.data + interval '2 minutes') AT TIME ZONE 'UTC')::timestamp)
          AND v2.data IS NOT NULL
          AND v2.data >= c.data
          AND v2.data <= c.data + interval '2 minutes'
          AND COALESCE(v2.cancelado, false) = false
      ) AS quick_resale
    FROM dw.fact_comprovante c
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa = c.id_empresa
     AND v.id_filial = c.id_filial
     AND v.id_db = c.id_db
     AND v.id_comprovante = c.id_comprovante
    LEFT JOIN LATERAL (
      SELECT MAX(i.id_funcionario) AS id_funcionario
      FROM dw.fact_venda_item i
      WHERE i.id_empresa = c.id_empresa
        AND i.id_filial = c.id_filial
        AND i.id_db = COALESCE(v.id_db, c.id_db)
        AND i.id_movprodutos = v.id_movprodutos
    ) fi ON true
    LEFT JOIN cancel_day_stats p90
      ON p90.id_empresa = c.id_empresa
     AND p90.id_filial = c.id_filial
     AND p90.data_key = c.data_key
    LEFT JOIN user_stats us
      ON us.id_empresa = c.id_empresa
     AND us.id_filial = c.id_filial
     AND us.id_usuario = c.id_usuario
    LEFT JOIN filial_stats fs
      ON fs.id_empresa = c.id_empresa
     AND fs.id_filial = c.id_filial
    LEFT JOIN hour_stats_norm hn
      ON hn.id_empresa = c.id_empresa
     AND hn.id_filial = c.id_filial
     AND hn.hour_key = EXTRACT(HOUR FROM c.data)::int
    WHERE c.id_empresa = p_id_empresa
      AND c.cancelado = true
      AND c.data_key BETWEEN v_start_key AND v_end_key
      AND c.data IS NOT NULL
      AND c.data >= v_start_ts
      AND c.data < (v_end_ts + interval '1 day')
  ),
  cancel_scored AS (
    SELECT
      b.*,
      CASE WHEN COALESCE(b.valor_total, 0) >= b.p90_valor AND b.p90_valor > 0 THEN 20 ELSE 0 END AS p_high_value,
      CASE
        WHEN b.usr_cancel_count >= 3 AND b.usr_cancel_rate >= GREATEST(0.15, b.filial_cancel_rate * 2) THEN 20
        ELSE 0
      END AS p_user_outlier,
      CASE WHEN b.quick_resale THEN 15 ELSE 0 END AS p_quick_resale,
      CASE
        WHEN b.hour_cancel_rate >= GREATEST(0.20, b.hour_avg_rate + b.hour_std_rate) THEN 10
        ELSE 0
      END AS p_risk_hour
    FROM cancel_base b
  ),
  cancel_final AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.data_key,
      s.data,
      'CANCELAMENTO'::text AS event_type,
      'DW'::text AS source,
      s.id_db,
      s.id_comprovante,
      s.id_movprodutos,
      s.id_usuario,
      s.id_funcionario,
      s.id_turno,
      s.id_cliente,
      COALESCE(s.valor_total,0)::numeric(18,2) AS valor_total,
      (COALESCE(s.valor_total,0) * 0.70)::numeric(18,2) AS impacto_estimado,
      LEAST(100, 20 + s.p_high_value + s.p_user_outlier + s.p_quick_resale + s.p_risk_hour) AS score_risco,
      jsonb_build_object(
        'base_cancelamento', 20,
        'high_value_p90', s.p_high_value,
        'user_outlier_ratio', s.p_user_outlier,
        'quick_resale_lt_2m', s.p_quick_resale,
        'risk_hour_bonus', s.p_risk_hour,
        'metrics', jsonb_build_object(
          'p90_valor', s.p90_valor,
          'valor_total', s.valor_total,
          'usr_cancel_rate', round(s.usr_cancel_rate::numeric, 4),
          'filial_cancel_rate', round(s.filial_cancel_rate::numeric, 4),
          'hour_cancel_rate', round(s.hour_cancel_rate::numeric, 4)
        )
      ) AS reasons
    FROM cancel_scored s
  ),
  discount_raw AS (
    SELECT
      v.id_empresa,
      v.id_filial,
      v.data_key,
      (v.data AT TIME ZONE 'UTC') AS data,
      v.id_db,
      v.id_comprovante,
      v.id_movprodutos,
      v.id_usuario,
      v.id_turno,
      v.id_cliente,
      MAX(i.id_funcionario) AS id_funcionario,
      SUM(COALESCE(i.total,0))::numeric(18,2) AS valor_total,
      SUM(GREATEST(COALESCE(i.desconto,0), 0))::numeric(18,2) AS desconto_total,
      AVG(NULLIF(i.valor_unitario,0))::numeric(18,4) AS avg_unit_price
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = p_id_empresa
      AND v.data_key BETWEEN v_start_key AND v_end_key
      AND v.data IS NOT NULL
      AND v.data >= v_start_ts
      AND v.data < (v_end_ts + interval '1 day')
      AND COALESCE(v.cancelado,false) = false
      AND COALESCE(i.cfop,0) >= 5000
    GROUP BY 1,2,3,4,5,6,7,8,9,10
  ),
  discount_day_stats AS (
    SELECT
      d.id_empresa,
      d.id_filial,
      d.data_key,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY d.desconto_total) AS p95_desconto
    FROM discount_raw d
    WHERE d.desconto_total > 0
    GROUP BY 1,2,3
  ),
  discount_days AS (
    SELECT DISTINCT
      d.id_empresa,
      d.id_filial,
      d.data_key,
      to_date(d.data_key::text, 'YYYYMMDD')::timestamp AS dt_ref
    FROM discount_raw d
  ),
  price_reference_30d AS (
    SELECT
      dd.id_empresa,
      dd.id_filial,
      dd.data_key,
      percentile_cont(0.10) WITHIN GROUP (ORDER BY i2.valor_unitario) AS p10_price
    FROM discount_days dd
    JOIN dw.fact_venda v2
      ON v2.id_empresa = dd.id_empresa
     AND v2.id_filial = dd.id_filial
     AND v2.data_key BETWEEN etl.date_key((dd.dt_ref - interval '30 days'))
                         AND etl.date_key((dd.dt_ref - interval '1 second'))
     AND v2.data >= (dd.dt_ref - interval '30 days')
     AND v2.data < dd.dt_ref
    JOIN dw.fact_venda_item i2
      ON i2.id_empresa = v2.id_empresa
     AND i2.id_filial = v2.id_filial
     AND i2.id_db = v2.id_db
     AND i2.id_movprodutos = v2.id_movprodutos
    WHERE i2.valor_unitario IS NOT NULL
    GROUP BY 1,2,3
  ),
  discount_scored AS (
    SELECT
      d.*,
      COALESCE(p95.p95_desconto, 0) AS p95_desconto,
      COALESCE(px.p10_price, 0) AS p10_price,
      CASE WHEN d.desconto_total > 0 AND d.desconto_total >= COALESCE(p95.p95_desconto, 0) AND COALESCE(p95.p95_desconto,0) > 0 THEN 25 ELSE 0 END AS p_desc_p95,
      CASE WHEN COALESCE(px.p10_price,0) > 0 AND d.avg_unit_price <= (px.p10_price * 0.90) THEN 10 ELSE 0 END AS p_price_outlier
    FROM discount_raw d
    LEFT JOIN discount_day_stats p95
      ON p95.id_empresa = d.id_empresa
     AND p95.id_filial = d.id_filial
     AND p95.data_key = d.data_key
    LEFT JOIN price_reference_30d px
      ON px.id_empresa = d.id_empresa
     AND px.id_filial = d.id_filial
     AND px.data_key = d.data_key
  ),
  discount_final AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.data_key,
      s.data,
      'DESCONTO_ALTO'::text AS event_type,
      'DW'::text AS source,
      s.id_db,
      s.id_comprovante,
      s.id_movprodutos,
      s.id_usuario,
      s.id_funcionario,
      s.id_turno,
      s.id_cliente,
      s.valor_total,
      GREATEST(COALESCE(s.desconto_total,0), COALESCE(s.valor_total,0) * 0.08)::numeric(18,2) AS impacto_estimado,
      LEAST(100, 25 + s.p_desc_p95 + s.p_price_outlier) AS score_risco,
      jsonb_build_object(
        'base_desconto', 25,
        'discount_p95_bonus', s.p_desc_p95,
        'unit_price_outlier_bonus', s.p_price_outlier,
        'metrics', jsonb_build_object(
          'desconto_total', s.desconto_total,
          'p95_desconto_dia', s.p95_desconto,
          'avg_unit_price', s.avg_unit_price,
          'p10_unit_price_30d', s.p10_price,
          'discount_hook_ready', true
        )
      ) AS reasons
    FROM discount_scored s
    WHERE s.p_desc_p95 > 0 OR s.p_price_outlier > 0
  ),
  risk_rows AS (
    SELECT * FROM cancel_final
    UNION ALL
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'CANCELAMENTO_SEGUIDO_VENDA'::text,
      c.source,
      c.id_db,
      c.id_comprovante,
      c.id_movprodutos,
      c.id_usuario,
      c.id_funcionario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      c.impacto_estimado,
      GREATEST(c.score_risco, 80),
      c.reasons || jsonb_build_object('pattern', 'cancelamento_seguido_venda_rapida')
    FROM cancel_final c
    WHERE COALESCE((c.reasons->>'quick_resale_lt_2m')::int, 0) > 0

    UNION ALL

    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'HORARIO_RISCO'::text,
      c.source,
      c.id_db,
      c.id_comprovante,
      c.id_movprodutos,
      c.id_usuario,
      c.id_funcionario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      c.impacto_estimado,
      GREATEST(c.score_risco, 70),
      c.reasons || jsonb_build_object('pattern', 'horario_critico')
    FROM cancel_final c
    WHERE COALESCE((c.reasons->>'risk_hour_bonus')::int, 0) > 0

    UNION ALL

    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'FUNCIONARIO_OUTLIER'::text,
      c.source,
      c.id_db,
      c.id_comprovante,
      c.id_movprodutos,
      c.id_usuario,
      c.id_funcionario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      c.impacto_estimado,
      GREATEST(c.score_risco, 85),
      c.reasons || jsonb_build_object('pattern', 'funcionario_outlier')
    FROM cancel_final c
    WHERE COALESCE((c.reasons->>'user_outlier_ratio')::int, 0) > 0

    UNION ALL

    SELECT * FROM discount_final
  ),
  upserted AS (
    INSERT INTO dw.fact_risco_evento (
      id_empresa,
      id_filial,
      data_key,
      data,
      event_type,
      source,
      id_db,
      id_comprovante,
      id_movprodutos,
      id_usuario,
      id_funcionario,
      id_turno,
      id_cliente,
      valor_total,
      impacto_estimado,
      score_risco,
      score_level,
      reasons
    )
    SELECT
      r.id_empresa,
      r.id_filial,
      r.data_key,
      r.data,
      r.event_type,
      r.source,
      r.id_db,
      r.id_comprovante,
      r.id_movprodutos,
      r.id_usuario,
      r.id_funcionario,
      r.id_turno,
      r.id_cliente,
      r.valor_total,
      r.impacto_estimado,
      r.score_risco,
      CASE
        WHEN r.score_risco >= 80 THEN 'ALTO'
        WHEN r.score_risco >= 60 THEN 'SUSPEITO'
        WHEN r.score_risco >= 40 THEN 'ATENCAO'
        ELSE 'NORMAL'
      END AS score_level,
      r.reasons
    FROM risk_rows r
    ON CONFLICT ON CONSTRAINT uq_fact_risco_evento_nk
    DO UPDATE SET
      data_key = EXCLUDED.data_key,
      data = EXCLUDED.data,
      id_usuario = EXCLUDED.id_usuario,
      id_funcionario = EXCLUDED.id_funcionario,
      id_turno = EXCLUDED.id_turno,
      id_cliente = EXCLUDED.id_cliente,
      valor_total = EXCLUDED.valor_total,
      impacto_estimado = EXCLUDED.impacto_estimado,
      score_risco = EXCLUDED.score_risco,
      score_level = EXCLUDED.score_level,
      reasons = EXCLUDED.reasons,
      created_at = now()
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  PERFORM etl.set_watermark(p_id_empresa, 'risk_events', now());

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;
