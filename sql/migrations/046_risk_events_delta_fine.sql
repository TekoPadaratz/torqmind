BEGIN;

CREATE INDEX IF NOT EXISTS ix_fact_comprovante_risk_delta_updated
  ON dw.fact_comprovante (id_empresa, updated_at DESC)
  INCLUDE (id_filial, id_db, id_comprovante, data, data_key, id_usuario, id_turno, id_cliente, valor_total, cancelado)
  WHERE data IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_comprovante_risk_user_window_full
  ON dw.fact_comprovante (id_empresa, id_filial, id_usuario, data_key, data)
  INCLUDE (cancelado, id_db, id_comprovante, id_turno, id_cliente, valor_total)
  WHERE data IS NOT NULL AND id_usuario IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_comprovante_risk_branch_window_full
  ON dw.fact_comprovante (id_empresa, id_filial, data_key, data)
  INCLUDE (id_usuario, cancelado, id_db, id_comprovante, id_turno, id_cliente, valor_total)
  WHERE data IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_venda_risk_delta_updated
  ON dw.fact_venda (id_empresa, updated_at DESC)
  INCLUDE (id_filial, id_db, id_movprodutos, id_comprovante, data, data_key, id_usuario, id_turno, id_cliente, cancelado, total_venda)
  WHERE data IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_venda_risk_branch_window
  ON dw.fact_venda (id_empresa, id_filial, data_key, data)
  INCLUDE (id_db, id_movprodutos, id_comprovante, id_usuario, id_turno, id_cliente, cancelado, total_venda)
  WHERE data IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_venda_item_risk_delta_updated
  ON dw.fact_venda_item (id_empresa, updated_at DESC, id_filial, id_db, id_movprodutos)
  INCLUDE (id_funcionario, desconto, valor_unitario, total, cfop);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_doc_lookup
  ON dw.fact_risco_evento (id_empresa, id_filial, id_db_nk, id_comprovante_nk, event_type)
  WHERE event_type IN ('CANCELAMENTO', 'CANCELAMENTO_SEGUIDO_VENDA', 'HORARIO_RISCO', 'FUNCIONARIO_OUTLIER');

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_mov_lookup
  ON dw.fact_risco_evento (id_empresa, id_filial, data_key, id_db_nk, id_movprodutos_nk, event_type)
  WHERE event_type = 'DESCONTO_ALTO';

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
  v_processed_ts timestamptz := clock_timestamp();
  v_min_source_ts timestamptz;
  v_max_source_ts timestamptz;
  v_changed_min_ts timestamptz;
  v_changed_max_ts timestamptz;
  v_candidate_start_ts timestamptz;
  v_candidate_end_ts timestamptz;
  v_effective_end_limit_ts timestamptz;
  v_target_window_end_ts timestamptz;
  v_target_start_key integer;
  v_target_end_key integer;
  v_baseline_14_key integer;
  v_baseline_30_key integer;
  v_target_end_date date;
  v_candidate_cancel_docs integer := 0;
  v_candidate_discount_days integer := 0;
BEGIN
  v_wm := etl.get_watermark(p_id_empresa, 'risk_events');

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
      AND (p_end_ts IS NULL OR c.data < (p_end_ts + interval '1 day'))

    UNION ALL

    SELECT
      MIN(v.data) AS min_ts,
      MAX(v.data) AS max_ts
    FROM dw.fact_venda v
    WHERE v.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND (p_end_ts IS NULL OR v.data < (p_end_ts + interval '1 day'))
  ) src;

  IF v_max_source_ts IS NULL THEN
    PERFORM etl.set_watermark(p_id_empresa, 'risk_events', v_processed_ts, NULL::bigint);
    RETURN 0;
  END IF;

  v_effective_end_limit_ts := COALESCE(
    LEAST(v_max_source_ts, COALESCE(p_end_ts, v_max_source_ts)),
    v_max_source_ts
  );

  IF p_force_full THEN
    v_candidate_start_ts := COALESCE(v_min_source_ts, v_effective_end_limit_ts - interval '90 days');
    v_candidate_end_ts := v_effective_end_limit_ts;
  ELSIF v_wm IS NULL THEN
    v_candidate_end_ts := v_effective_end_limit_ts;
    v_candidate_start_ts := COALESCE(v_candidate_end_ts, v_effective_end_limit_ts)
      - make_interval(days => GREATEST(p_lookback_days, 1));
  ELSE
    SELECT
      MIN(src.min_ts),
      MAX(src.max_ts)
    INTO v_changed_min_ts, v_changed_max_ts
    FROM (
      SELECT
        MIN(c.data) AS min_ts,
        MAX(c.data) AS max_ts
      FROM dw.fact_comprovante c
      WHERE c.id_empresa = p_id_empresa
        AND c.data IS NOT NULL
        AND c.updated_at > v_wm
        AND (p_end_ts IS NULL OR c.data < (p_end_ts + interval '1 day'))

      UNION ALL

      SELECT
        MIN(v.data) AS min_ts,
        MAX(v.data) AS max_ts
      FROM dw.fact_venda v
      WHERE v.id_empresa = p_id_empresa
        AND v.data IS NOT NULL
        AND v.updated_at > v_wm
        AND (p_end_ts IS NULL OR v.data < (p_end_ts + interval '1 day'))

      UNION ALL

      SELECT
        MIN(v.data) AS min_ts,
        MAX(v.data) AS max_ts
      FROM dw.fact_venda_item i
      JOIN dw.fact_venda v
        ON v.id_empresa = i.id_empresa
       AND v.id_filial = i.id_filial
       AND v.id_db = i.id_db
       AND v.id_movprodutos = i.id_movprodutos
      WHERE i.id_empresa = p_id_empresa
        AND i.updated_at > v_wm
        AND v.data IS NOT NULL
        AND (p_end_ts IS NULL OR v.data < (p_end_ts + interval '1 day'))
    ) src;

    IF v_changed_max_ts IS NULL THEN
      PERFORM etl.set_watermark(p_id_empresa, 'risk_events', v_processed_ts, NULL::bigint);
      RETURN 0;
    END IF;

    v_candidate_start_ts := v_changed_min_ts - interval '1 day';
    v_candidate_end_ts := LEAST(v_effective_end_limit_ts, v_changed_max_ts + interval '30 days');
  END IF;

  IF v_candidate_end_ts < v_candidate_start_ts THEN
    v_candidate_end_ts := v_candidate_start_ts;
  END IF;

  v_target_start_key := etl.date_key((v_candidate_start_ts AT TIME ZONE 'UTC')::timestamp);
  v_target_end_key := etl.date_key((v_candidate_end_ts AT TIME ZONE 'UTC')::timestamp);
  v_baseline_14_key := etl.date_key(((v_candidate_start_ts - interval '14 days') AT TIME ZONE 'UTC')::timestamp);
  v_baseline_30_key := etl.date_key(((v_candidate_start_ts - interval '30 days') AT TIME ZONE 'UTC')::timestamp);
  v_target_end_date := to_date(v_target_end_key::text, 'YYYYMMDD');
  v_target_window_end_ts := (v_target_end_date::timestamp + interval '1 day');

  DROP TABLE IF EXISTS tmp_risk_changed_doc_days;
  CREATE TEMP TABLE tmp_risk_changed_doc_days ON COMMIT DROP AS
  SELECT DISTINCT
    src.id_empresa,
    src.id_filial,
    src.data_key,
    src.dt_ref
  FROM (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      to_date(c.data_key::text, 'YYYYMMDD') AS dt_ref
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND c.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND c.data >= v_candidate_start_ts
      AND c.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR c.updated_at > v_wm)

    UNION ALL

    SELECT
      v.id_empresa,
      v.id_filial,
      v.data_key,
      to_date(v.data_key::text, 'YYYYMMDD') AS dt_ref
    FROM dw.fact_venda v
    WHERE v.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND v.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND v.data >= v_candidate_start_ts
      AND v.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR v.updated_at > v_wm)

    UNION ALL

    SELECT
      v.id_empresa,
      v.id_filial,
      v.data_key,
      to_date(v.data_key::text, 'YYYYMMDD') AS dt_ref
    FROM dw.fact_venda_item i
    JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_movprodutos = i.id_movprodutos
    WHERE i.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND v.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND v.data >= v_candidate_start_ts
      AND v.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR i.updated_at > v_wm)
  ) src;

  CREATE INDEX tmp_risk_changed_doc_days_idx
    ON tmp_risk_changed_doc_days (id_filial, data_key);

  DROP TABLE IF EXISTS tmp_risk_changed_discount_days;
  CREATE TEMP TABLE tmp_risk_changed_discount_days ON COMMIT DROP AS
  SELECT DISTINCT
    src.id_empresa,
    src.id_filial,
    src.data_key,
    src.dt_ref
  FROM (
    SELECT
      v.id_empresa,
      v.id_filial,
      v.data_key,
      to_date(v.data_key::text, 'YYYYMMDD') AS dt_ref
    FROM dw.fact_venda v
    WHERE v.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND v.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND v.data >= v_candidate_start_ts
      AND v.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR v.updated_at > v_wm)

    UNION ALL

    SELECT
      v.id_empresa,
      v.id_filial,
      v.data_key,
      to_date(v.data_key::text, 'YYYYMMDD') AS dt_ref
    FROM dw.fact_venda_item i
    JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_movprodutos = i.id_movprodutos
    WHERE i.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND v.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND v.data >= v_candidate_start_ts
      AND v.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR i.updated_at > v_wm)
  ) src;

  CREATE INDEX tmp_risk_changed_discount_days_idx
    ON tmp_risk_changed_discount_days (id_filial, data_key);

  DROP TABLE IF EXISTS tmp_risk_changed_sales_anchor;
  CREATE TEMP TABLE tmp_risk_changed_sales_anchor ON COMMIT DROP AS
  SELECT DISTINCT
    src.id_empresa,
    src.id_filial,
    src.id_usuario,
    src.data,
    etl.date_key((((src.data - interval '1 day') AT TIME ZONE 'UTC')::timestamp)) AS start_key,
    etl.date_key(((src.data AT TIME ZONE 'UTC')::timestamp)) AS end_key
  FROM (
    SELECT
      v.id_empresa,
      v.id_filial,
      v.id_usuario,
      v.data
    FROM dw.fact_venda v
    WHERE v.id_empresa = p_id_empresa
      AND v.id_usuario IS NOT NULL
      AND v.data IS NOT NULL
      AND v.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND v.data >= v_candidate_start_ts
      AND v.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR v.updated_at > v_wm)

    UNION ALL

    SELECT
      v.id_empresa,
      v.id_filial,
      v.id_usuario,
      v.data
    FROM dw.fact_venda_item i
    JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_movprodutos = i.id_movprodutos
    WHERE i.id_empresa = p_id_empresa
      AND v.id_usuario IS NOT NULL
      AND v.data IS NOT NULL
      AND v.data_key BETWEEN v_target_start_key AND v_target_end_key
      AND v.data >= v_candidate_start_ts
      AND v.data < v_target_window_end_ts
      AND (p_force_full OR v_wm IS NULL OR i.updated_at > v_wm)
  ) src;

  CREATE INDEX tmp_risk_changed_sales_anchor_idx
    ON tmp_risk_changed_sales_anchor (id_filial, id_usuario, data);

  DROP TABLE IF EXISTS tmp_risk_affected_cancel_days;
  CREATE TEMP TABLE tmp_risk_affected_cancel_days ON COMMIT DROP AS
  SELECT DISTINCT
    d.id_empresa,
    d.id_filial,
    etl.date_key(gs.day_ts::timestamp) AS data_key,
    gs.day_ts::date AS dt_ref
  FROM tmp_risk_changed_doc_days d
  CROSS JOIN LATERAL generate_series(
    d.dt_ref::timestamp,
    LEAST((d.dt_ref + 30)::timestamp, v_target_end_date::timestamp),
    interval '1 day'
  ) AS gs(day_ts);

  CREATE INDEX tmp_risk_affected_cancel_days_idx
    ON tmp_risk_affected_cancel_days (id_filial, data_key);

  DROP TABLE IF EXISTS tmp_risk_affected_discount_days;
  CREATE TEMP TABLE tmp_risk_affected_discount_days ON COMMIT DROP AS
  SELECT DISTINCT
    d.id_empresa,
    d.id_filial,
    etl.date_key(gs.day_ts::timestamp) AS data_key,
    gs.day_ts::date AS dt_ref
  FROM tmp_risk_changed_discount_days d
  CROSS JOIN LATERAL generate_series(
    d.dt_ref::timestamp,
    LEAST((d.dt_ref + 30)::timestamp, v_target_end_date::timestamp),
    interval '1 day'
  ) AS gs(day_ts);

  CREATE INDEX tmp_risk_affected_discount_days_idx
    ON tmp_risk_affected_discount_days (id_filial, data_key);

  DROP TABLE IF EXISTS tmp_risk_candidate_comprovantes;
  CREATE TEMP TABLE tmp_risk_candidate_comprovantes ON COMMIT DROP AS
  SELECT DISTINCT
    src.id_empresa,
    src.id_filial,
    src.data_key,
    src.data,
    src.id_db,
    src.id_comprovante,
    src.id_usuario,
    src.id_turno,
    src.id_cliente,
    src.valor_total,
    src.cancelado
  FROM (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      c.id_db,
      c.id_comprovante,
      c.id_usuario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      COALESCE(c.cancelado, false) AS cancelado
    FROM dw.fact_comprovante c
    JOIN tmp_risk_affected_cancel_days d
      ON d.id_empresa = c.id_empresa
     AND d.id_filial = c.id_filial
     AND d.data_key = c.data_key
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND COALESCE(c.cancelado, false) = true

    UNION ALL

    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      c.id_db,
      c.id_comprovante,
      c.id_usuario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      COALESCE(c.cancelado, false) AS cancelado
    FROM dw.fact_comprovante c
    JOIN tmp_risk_changed_sales_anchor s
      ON s.id_empresa = c.id_empresa
     AND s.id_filial = c.id_filial
     AND s.id_usuario = c.id_usuario
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND COALESCE(c.cancelado, false) = true
      AND c.data_key BETWEEN s.start_key AND s.end_key
      AND c.data >= (s.data - interval '2 minutes')
      AND c.data <= s.data
      AND c.data >= (v_candidate_start_ts - interval '1 day')
      AND c.data < v_target_window_end_ts
  ) src;

  CREATE INDEX tmp_risk_candidate_comprovantes_doc_idx
    ON tmp_risk_candidate_comprovantes (id_filial, id_db, id_comprovante);
  CREATE INDEX tmp_risk_candidate_comprovantes_user_idx
    ON tmp_risk_candidate_comprovantes (id_filial, id_usuario, data);

  DROP TABLE IF EXISTS tmp_risk_candidate_users;
  CREATE TEMP TABLE tmp_risk_candidate_users ON COMMIT DROP AS
  SELECT DISTINCT id_empresa, id_filial, id_usuario
  FROM tmp_risk_candidate_comprovantes
  WHERE id_usuario IS NOT NULL;

  CREATE INDEX tmp_risk_candidate_users_idx
    ON tmp_risk_candidate_users (id_filial, id_usuario);

  DROP TABLE IF EXISTS tmp_risk_candidate_branches;
  CREATE TEMP TABLE tmp_risk_candidate_branches ON COMMIT DROP AS
  SELECT DISTINCT id_empresa, id_filial
  FROM tmp_risk_candidate_comprovantes;

  CREATE INDEX tmp_risk_candidate_branches_idx
    ON tmp_risk_candidate_branches (id_filial);

  DROP TABLE IF EXISTS tmp_risk_relevant_sales;
  CREATE TEMP TABLE tmp_risk_relevant_sales ON COMMIT DROP AS
  SELECT DISTINCT
    v.id_empresa,
    v.id_filial,
    v.data_key,
    v.data,
    v.id_db,
    v.id_comprovante,
    v.id_movprodutos,
    v.id_usuario,
    v.id_turno,
    v.id_cliente,
    COALESCE(v.cancelado, false) AS cancelado
  FROM dw.fact_venda v
  LEFT JOIN tmp_risk_affected_cancel_days cd
    ON cd.id_empresa = v.id_empresa
   AND cd.id_filial = v.id_filial
   AND cd.data_key = v.data_key
  LEFT JOIN tmp_risk_affected_discount_days dd
    ON dd.id_empresa = v.id_empresa
   AND dd.id_filial = v.id_filial
   AND dd.data_key = v.data_key
  WHERE v.id_empresa = p_id_empresa
    AND v.data IS NOT NULL
    AND (cd.data_key IS NOT NULL OR dd.data_key IS NOT NULL);

  CREATE INDEX tmp_risk_relevant_sales_doc_idx
    ON tmp_risk_relevant_sales (id_filial, id_db, id_comprovante);
  CREATE INDEX tmp_risk_relevant_sales_mov_idx
    ON tmp_risk_relevant_sales (id_filial, id_db, id_movprodutos);
  CREATE INDEX tmp_risk_relevant_sales_user_idx
    ON tmp_risk_relevant_sales (id_filial, id_usuario, data);

  DROP TABLE IF EXISTS tmp_risk_target_sale_items;
  CREATE TEMP TABLE tmp_risk_target_sale_items ON COMMIT DROP AS
  SELECT
    s.id_empresa,
    s.id_filial,
    s.data_key,
    s.data,
    s.id_db,
    s.id_comprovante,
    s.id_movprodutos,
    s.id_usuario,
    s.id_turno,
    s.id_cliente,
    s.cancelado,
    i.id_funcionario,
    i.cfop,
    i.total,
    i.desconto,
    i.valor_unitario
  FROM tmp_risk_relevant_sales s
  JOIN dw.fact_venda_item i
    ON i.id_empresa = s.id_empresa
   AND i.id_filial = s.id_filial
   AND i.id_db = s.id_db
   AND i.id_movprodutos = s.id_movprodutos;

  CREATE INDEX tmp_risk_target_sale_items_mov_idx
    ON tmp_risk_target_sale_items (id_filial, id_db, id_movprodutos);
  CREATE INDEX tmp_risk_target_sale_items_data_idx
    ON tmp_risk_target_sale_items (id_filial, data_key, data);

  ANALYZE tmp_risk_changed_doc_days;
  ANALYZE tmp_risk_changed_discount_days;
  ANALYZE tmp_risk_changed_sales_anchor;
  ANALYZE tmp_risk_affected_cancel_days;
  ANALYZE tmp_risk_affected_discount_days;
  ANALYZE tmp_risk_candidate_comprovantes;
  ANALYZE tmp_risk_relevant_sales;
  ANALYZE tmp_risk_target_sale_items;

  SELECT COUNT(*)::int INTO v_candidate_cancel_docs FROM tmp_risk_candidate_comprovantes;
  SELECT COUNT(*)::int INTO v_candidate_discount_days FROM tmp_risk_affected_discount_days;

  IF v_candidate_cancel_docs = 0 AND v_candidate_discount_days = 0 THEN
    PERFORM etl.set_watermark(p_id_empresa, 'risk_events', v_processed_ts, NULL::bigint);
    RETURN 0;
  END IF;

  DELETE FROM dw.fact_risco_evento r
  USING tmp_risk_candidate_comprovantes c
  WHERE r.id_empresa = c.id_empresa
    AND r.id_filial = c.id_filial
    AND r.event_type IN ('CANCELAMENTO', 'CANCELAMENTO_SEGUIDO_VENDA', 'HORARIO_RISCO', 'FUNCIONARIO_OUTLIER')
    AND r.id_db_nk = COALESCE(c.id_db, -1)
    AND r.id_comprovante_nk = COALESCE(c.id_comprovante, -1);

  DELETE FROM dw.fact_risco_evento r
  USING tmp_risk_affected_discount_days d
  WHERE r.id_empresa = d.id_empresa
    AND r.id_filial = d.id_filial
    AND r.event_type = 'DESCONTO_ALTO'
    AND r.data_key = d.data_key;

  WITH
  user_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_usuario,
      COUNT(*)::int AS docs_total,
      COUNT(*) FILTER (WHERE c.cancelado)::int AS cancels,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    JOIN tmp_risk_candidate_users u
      ON u.id_empresa = c.id_empresa
     AND u.id_filial = c.id_filial
     AND u.id_usuario = c.id_usuario
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND c.data_key BETWEEN v_baseline_14_key AND v_target_end_key
      AND c.data >= (v_candidate_start_ts - interval '14 days')
      AND c.data < v_target_window_end_ts
    GROUP BY 1,2,3
  ),
  filial_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      COUNT(*)::int AS docs_total,
      COUNT(*) FILTER (WHERE c.cancelado)::int AS cancels,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    JOIN tmp_risk_candidate_branches b
      ON b.id_empresa = c.id_empresa
     AND b.id_filial = c.id_filial
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND c.data_key BETWEEN v_baseline_14_key AND v_target_end_key
      AND c.data >= (v_candidate_start_ts - interval '14 days')
      AND c.data < v_target_window_end_ts
    GROUP BY 1,2
  ),
  hour_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      EXTRACT(HOUR FROM c.data)::int AS hour_key,
      COUNT(*) FILTER (WHERE c.cancelado)::int AS cancel_count,
      COUNT(*)::int AS docs_total,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    JOIN tmp_risk_candidate_branches b
      ON b.id_empresa = c.id_empresa
     AND b.id_filial = c.id_filial
    WHERE c.id_empresa = p_id_empresa
      AND c.data IS NOT NULL
      AND c.data_key BETWEEN v_baseline_30_key AND v_target_end_key
      AND c.data >= (v_candidate_start_ts - interval '30 days')
      AND c.data < v_target_window_end_ts
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
    FROM tmp_risk_candidate_comprovantes c
    WHERE c.valor_total IS NOT NULL
    GROUP BY 1,2,3
  ),
  movement_employee_map AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      MAX(i.id_funcionario) AS id_funcionario
    FROM tmp_risk_target_sale_items i
    WHERE i.id_funcionario IS NOT NULL
    GROUP BY 1,2,3,4
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
      me.id_funcionario,
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
        FROM tmp_risk_relevant_sales v2
        WHERE v2.id_empresa = c.id_empresa
          AND v2.id_filial = c.id_filial
          AND v2.id_usuario = c.id_usuario
          AND v2.data IS NOT NULL
          AND v2.data >= c.data
          AND v2.data <= c.data + interval '2 minutes'
          AND COALESCE(v2.cancelado, false) = false
      ) AS quick_resale
    FROM tmp_risk_candidate_comprovantes c
    LEFT JOIN tmp_risk_relevant_sales v
      ON v.id_empresa = c.id_empresa
     AND v.id_filial = c.id_filial
     AND v.id_db = c.id_db
     AND v.id_comprovante = c.id_comprovante
    LEFT JOIN movement_employee_map me
      ON me.id_empresa = c.id_empresa
     AND me.id_filial = c.id_filial
     AND me.id_db = COALESCE(v.id_db, c.id_db)
     AND me.id_movprodutos = v.id_movprodutos
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
      COALESCE(s.valor_total, 0)::numeric(18,2) AS valor_total,
      (COALESCE(s.valor_total, 0) * 0.70)::numeric(18,2) AS impacto_estimado,
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
      i.id_empresa,
      i.id_filial,
      i.data_key,
      (i.data AT TIME ZONE 'UTC') AS data,
      i.id_db,
      i.id_comprovante,
      i.id_movprodutos,
      i.id_usuario,
      i.id_turno,
      i.id_cliente,
      MAX(i.id_funcionario) AS id_funcionario,
      SUM(COALESCE(i.total, 0))::numeric(18,2) AS valor_total,
      SUM(GREATEST(COALESCE(i.desconto, 0), 0))::numeric(18,2) AS desconto_total,
      AVG(NULLIF(i.valor_unitario, 0))::numeric(18,4) AS avg_unit_price
    FROM tmp_risk_target_sale_items i
    JOIN tmp_risk_affected_discount_days d
      ON d.id_empresa = i.id_empresa
     AND d.id_filial = i.id_filial
     AND d.data_key = i.data_key
    WHERE COALESCE(i.cancelado, false) = false
      AND COALESCE(i.cfop, 0) >= 5000
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
  price_reference_30d AS (
    SELECT
      dd.id_empresa,
      dd.id_filial,
      dd.data_key,
      percentile_cont(0.10) WITHIN GROUP (ORDER BY i2.valor_unitario) AS p10_price
    FROM tmp_risk_affected_discount_days dd
    JOIN dw.fact_venda v2
      ON v2.id_empresa = dd.id_empresa
     AND v2.id_filial = dd.id_filial
     AND v2.data IS NOT NULL
     AND v2.data >= (dd.dt_ref::timestamp - interval '30 days')
     AND v2.data < dd.dt_ref::timestamp
     AND COALESCE(v2.cancelado, false) = false
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
      CASE
        WHEN d.desconto_total > 0
         AND d.desconto_total >= COALESCE(p95.p95_desconto, 0)
         AND COALESCE(p95.p95_desconto, 0) > 0
        THEN 25
        ELSE 0
      END AS p_desc_p95,
      CASE
        WHEN COALESCE(px.p10_price, 0) > 0 AND d.avg_unit_price <= (px.p10_price * 0.90) THEN 10
        ELSE 0
      END AS p_price_outlier
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
      GREATEST(COALESCE(s.desconto_total, 0), COALESCE(s.valor_total, 0) * 0.08)::numeric(18,2) AS impacto_estimado,
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
    WHERE (
      dw.fact_risco_evento.data_key,
      dw.fact_risco_evento.data,
      dw.fact_risco_evento.id_usuario,
      dw.fact_risco_evento.id_funcionario,
      dw.fact_risco_evento.id_turno,
      dw.fact_risco_evento.id_cliente,
      dw.fact_risco_evento.valor_total,
      dw.fact_risco_evento.impacto_estimado,
      dw.fact_risco_evento.score_risco,
      dw.fact_risco_evento.score_level,
      dw.fact_risco_evento.reasons
    ) IS DISTINCT FROM (
      EXCLUDED.data_key,
      EXCLUDED.data,
      EXCLUDED.id_usuario,
      EXCLUDED.id_funcionario,
      EXCLUDED.id_turno,
      EXCLUDED.id_cliente,
      EXCLUDED.valor_total,
      EXCLUDED.impacto_estimado,
      EXCLUDED.score_risco,
      EXCLUDED.score_level,
      EXCLUDED.reasons
    )
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  PERFORM etl.set_watermark(
    p_id_empresa,
    'risk_events',
    v_processed_ts,
    NULL::bigint
  );

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.compute_risk_events(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_lookback_days int DEFAULT 14
)
RETURNS integer AS $$
BEGIN
  RETURN etl.compute_risk_events(
    p_id_empresa,
    p_force_full,
    p_lookback_days,
    NULL::timestamptz
  );
END;
$$ LANGUAGE plpgsql;

ANALYZE dw.fact_comprovante;
ANALYZE dw.fact_venda;
ANALYZE dw.fact_venda_item;
ANALYZE dw.fact_risco_evento;

COMMIT;
