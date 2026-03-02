BEGIN;

-- =========================
-- DW: Risk event fact
-- =========================

CREATE TABLE IF NOT EXISTS dw.fact_risco_evento (
  id                 bigserial PRIMARY KEY,
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  data_key           integer NOT NULL,
  data               timestamptz NULL,

  event_type         text NOT NULL,
  source             text NOT NULL DEFAULT 'DW',
  id_db              integer NULL,
  id_comprovante     integer NULL,
  id_movprodutos     integer NULL,

  id_usuario         integer NULL,
  id_funcionario     integer NULL,
  id_turno           integer NULL,
  id_cliente         integer NULL,

  valor_total        numeric(18,2) NULL,
  impacto_estimado   numeric(18,2) NOT NULL DEFAULT 0,

  score_risco        integer NOT NULL CHECK (score_risco BETWEEN 0 AND 100),
  score_level        text NOT NULL CHECK (score_level IN ('NORMAL','ATENCAO','SUSPEITO','ALTO')),
  reasons            jsonb NOT NULL DEFAULT '{}'::jsonb,

  id_db_nk           integer GENERATED ALWAYS AS (COALESCE(id_db, -1)) STORED,
  id_comprovante_nk  integer GENERATED ALWAYS AS (COALESCE(id_comprovante, -1)) STORED,
  id_movprodutos_nk  integer GENERATED ALWAYS AS (COALESCE(id_movprodutos, -1)) STORED,

  created_at         timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT uq_fact_risco_evento_nk UNIQUE (
    id_empresa,
    id_filial,
    event_type,
    id_db_nk,
    id_comprovante_nk,
    id_movprodutos_nk
  )
);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_empresa_data_filial
  ON dw.fact_risco_evento (id_empresa, data_key, id_filial);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_score
  ON dw.fact_risco_evento (id_empresa, id_filial, score_risco DESC, data DESC);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_func_data
  ON dw.fact_risco_evento (id_empresa, id_filial, id_funcionario, data_key);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_event_data
  ON dw.fact_risco_evento (id_empresa, id_filial, event_type, data_key);

-- =========================
-- APP: persisted insights
-- =========================

CREATE TABLE IF NOT EXISTS app.insights_gerados (
  id                bigserial PRIMARY KEY,
  created_at        timestamptz NOT NULL DEFAULT now(),
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  insight_type      text NOT NULL,
  severity          text NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
  dt_ref            date NOT NULL,
  impacto_estimado  numeric(18,2) NOT NULL DEFAULT 0,
  title             text NOT NULL,
  message           text NOT NULL,
  recommendation    text NOT NULL,
  status            text NOT NULL DEFAULT 'NOVO' CHECK (status IN ('NOVO','LIDO','RESOLVIDO')),
  meta              jsonb NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT uq_insights_gerados_nk UNIQUE (id_empresa, id_filial, insight_type, dt_ref)
);

CREATE INDEX IF NOT EXISTS ix_insights_gerados_lookup
  ON app.insights_gerados (id_empresa, id_filial, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_insights_gerados_status
  ON app.insights_gerados (id_empresa, id_filial, status, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_insights_gerados_severity
  ON app.insights_gerados (id_empresa, id_filial, severity, dt_ref DESC);

-- =========================
-- ETL: Risk Scoring
-- =========================

CREATE OR REPLACE FUNCTION etl.compute_risk_events(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_lookback_days int DEFAULT 14
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
  v_wm timestamptz;
  v_start_ts timestamptz;
BEGIN
  v_wm := etl.get_watermark(p_id_empresa, 'risk_events');

  IF p_force_full THEN
    v_start_ts := now() - interval '90 days';
  ELSE
    v_start_ts := COALESCE(v_wm, now() - make_interval(days => p_lookback_days)) - interval '1 day';
  END IF;

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
      AND c.data >= (v_start_ts - interval '14 days')
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
      AND c.data >= (v_start_ts - interval '14 days')
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
      AND c.data >= (v_start_ts - interval '30 days')
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
    LEFT JOIN LATERAL (
      SELECT percentile_cont(0.90) WITHIN GROUP (ORDER BY c2.valor_total) AS p90_valor
      FROM dw.fact_comprovante c2
      WHERE c2.id_empresa = c.id_empresa
        AND c2.id_filial = c.id_filial
        AND c2.data_key = c.data_key
        AND c2.cancelado = true
        AND c2.valor_total IS NOT NULL
    ) p90 ON true
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
      AND c.data IS NOT NULL
      AND c.data >= v_start_ts
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
      AND v.data IS NOT NULL
      AND v.data >= v_start_ts
      AND COALESCE(v.cancelado,false) = false
      AND COALESCE(i.cfop,0) >= 5000
    GROUP BY 1,2,3,4,5,6,7,8,9,10
  ),
  discount_scored AS (
    SELECT
      d.*,
      COALESCE(p95.p95_desconto, 0) AS p95_desconto,
      COALESCE(px.p10_price, 0) AS p10_price,
      CASE WHEN d.desconto_total > 0 AND d.desconto_total >= COALESCE(p95.p95_desconto, 0) AND COALESCE(p95.p95_desconto,0) > 0 THEN 25 ELSE 0 END AS p_desc_p95,
      CASE WHEN COALESCE(px.p10_price,0) > 0 AND d.avg_unit_price <= (px.p10_price * 0.90) THEN 10 ELSE 0 END AS p_price_outlier
    FROM discount_raw d
    LEFT JOIN LATERAL (
      SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY dr2.desconto_total) AS p95_desconto
      FROM discount_raw dr2
      WHERE dr2.id_empresa = d.id_empresa
        AND dr2.id_filial = d.id_filial
        AND dr2.data_key = d.data_key
        AND dr2.desconto_total > 0
    ) p95 ON true
    LEFT JOIN LATERAL (
      SELECT percentile_cont(0.10) WITHIN GROUP (ORDER BY i2.valor_unitario) AS p10_price
      FROM dw.fact_venda_item i2
      JOIN dw.fact_venda v2
        ON v2.id_empresa = i2.id_empresa
       AND v2.id_filial = i2.id_filial
       AND v2.id_db = i2.id_db
       AND v2.id_movprodutos = i2.id_movprodutos
      WHERE i2.id_empresa = d.id_empresa
        AND i2.id_filial = d.id_filial
        AND i2.valor_unitario IS NOT NULL
        AND v2.data >= (d.data - interval '30 days')
        AND v2.data < d.data
    ) px ON true
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

-- =========================
-- MART: Risk aggregates
-- =========================

DROP MATERIALIZED VIEW IF EXISTS mart.agg_risco_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.agg_risco_diaria AS
SELECT
  r.id_empresa,
  r.id_filial,
  r.data_key,
  COUNT(*)::int AS eventos_risco_total,
  COUNT(*) FILTER (WHERE r.score_risco >= 80)::int AS eventos_alto_risco,
  COALESCE(SUM(r.impacto_estimado),0)::numeric(18,2) AS impacto_estimado_total,
  COALESCE(AVG(r.score_risco),0)::numeric(10,2) AS score_medio,
  COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY r.score_risco),0)::numeric(10,2) AS p95_score,
  now() AS updated_at
FROM dw.fact_risco_evento r
GROUP BY 1,2,3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_risco_diaria
  ON mart.agg_risco_diaria (id_empresa, id_filial, data_key);
CREATE INDEX IF NOT EXISTS ix_mart_agg_risco_diaria_lookup
  ON mart.agg_risco_diaria (id_empresa, data_key, id_filial);

DROP MATERIALIZED VIEW IF EXISTS mart.risco_top_funcionarios_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.risco_top_funcionarios_diaria AS
SELECT
  r.id_empresa,
  r.id_filial,
  r.data_key,
  COALESCE(r.id_funcionario, -1) AS id_funcionario,
  COALESCE(df.nome, '(Sem funcionário)') AS funcionario_nome,
  COUNT(*)::int AS eventos,
  COUNT(*) FILTER (WHERE r.score_risco >= 80)::int AS alto_risco,
  COALESCE(SUM(r.impacto_estimado),0)::numeric(18,2) AS impacto_estimado,
  COALESCE(AVG(r.score_risco),0)::numeric(10,2) AS score_medio,
  now() AS updated_at
FROM dw.fact_risco_evento r
LEFT JOIN dw.dim_funcionario df
  ON df.id_empresa = r.id_empresa
 AND df.id_filial = r.id_filial
 AND df.id_funcionario = r.id_funcionario
GROUP BY 1,2,3,4,5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_risco_top_funcionarios_diaria
  ON mart.risco_top_funcionarios_diaria (id_empresa, id_filial, data_key, id_funcionario);
CREATE INDEX IF NOT EXISTS ix_mart_risco_top_funcionarios_lookup
  ON mart.risco_top_funcionarios_diaria (id_empresa, data_key, impacto_estimado DESC);

DROP VIEW IF EXISTS mart.risco_eventos_recentes CASCADE;
CREATE VIEW mart.risco_eventos_recentes AS
SELECT
  r.id,
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
  COALESCE(df.nome, '(Sem funcionário)') AS funcionario_nome,
  r.id_turno,
  r.id_cliente,
  r.valor_total,
  r.impacto_estimado,
  r.score_risco,
  r.score_level,
  r.reasons,
  r.created_at
FROM dw.fact_risco_evento r
LEFT JOIN dw.dim_funcionario df
  ON df.id_empresa = r.id_empresa
 AND df.id_filial = r.id_filial
 AND df.id_funcionario = r.id_funcionario;

-- =========================
-- ETL: Insight Engine
-- =========================

CREATE OR REPLACE FUNCTION etl.generate_insights(
  p_id_empresa int,
  p_dt_ref date DEFAULT CURRENT_DATE,
  p_days_back int DEFAULT 7
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH ref_days AS (
    SELECT
      d::date AS dt_ref,
      to_char(d::date, 'YYYYMMDD')::int AS data_key
    FROM generate_series(p_dt_ref - make_interval(days => GREATEST(p_days_back - 1, 0)), p_dt_ref, interval '1 day') d
  ),
  risk_now AS (
    SELECT r.*
    FROM mart.agg_risco_diaria r
    JOIN ref_days d ON d.data_key = r.data_key
    WHERE r.id_empresa = p_id_empresa
  ),
  risk_prev AS (
    SELECT
      cur.id_empresa,
      cur.id_filial,
      cur.data_key,
      COALESCE(AVG(prev.eventos_alto_risco),0) AS avg_prev_high
    FROM risk_now cur
    LEFT JOIN mart.agg_risco_diaria prev
      ON prev.id_empresa = cur.id_empresa
     AND prev.id_filial = cur.id_filial
     AND prev.data_key BETWEEN (cur.data_key - 30) AND (cur.data_key - 1)
    GROUP BY 1,2,3
  ),
  cancel_abn AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      to_date(n.data_key::text, 'YYYYMMDD') AS dt_ref,
      'CANCELAMENTO_ANORMAL'::text AS insight_type,
      CASE WHEN n.eventos_alto_risco >= 10 THEN 'CRITICAL' ELSE 'WARN' END AS severity,
      COALESCE(n.impacto_estimado_total,0)::numeric(18,2) AS impacto_estimado,
      format('Cancelamentos de alto risco fora da curva na filial %s', n.id_filial) AS title,
      format('Hoje houve %s eventos de alto risco (média recente %s). Impacto estimado: R$ %s.',
             n.eventos_alto_risco,
             round(rp.avg_prev_high::numeric, 2),
             to_char(COALESCE(n.impacto_estimado_total,0),'FM999G999G990D00')) AS message,
      'Revisar caixas/turnos críticos e aprovações de cancelamento imediatamente.'::text AS recommendation,
      jsonb_build_object(
        'eventos_alto_risco', n.eventos_alto_risco,
        'media_30d', round(rp.avg_prev_high::numeric,2),
        'score_medio', n.score_medio
      ) AS meta
    FROM risk_now n
    JOIN risk_prev rp
      ON rp.id_empresa = n.id_empresa
     AND rp.id_filial = n.id_filial
     AND rp.data_key = n.data_key
    WHERE n.eventos_alto_risco > GREATEST(3, rp.avg_prev_high * 1.5)
  ),
  top_func_raw AS (
    SELECT
      t.id_empresa,
      t.id_filial,
      to_date(t.data_key::text, 'YYYYMMDD') AS dt_ref,
      t.id_funcionario,
      t.funcionario_nome,
      t.eventos,
      t.alto_risco,
      t.impacto_estimado,
      t.score_medio,
      row_number() OVER (PARTITION BY t.id_empresa, t.id_filial, t.data_key ORDER BY t.impacto_estimado DESC, t.score_medio DESC) AS rn
    FROM mart.risco_top_funcionarios_diaria t
    JOIN ref_days d ON d.data_key = t.data_key
    WHERE t.id_empresa = p_id_empresa
      AND t.score_medio >= 75
  ),
  top_func AS (
    SELECT
      id_empresa,
      id_filial,
      dt_ref,
      'FUNCIONARIO_RISCO_ALTO'::text AS insight_type,
      CASE WHEN score_medio >= 85 THEN 'CRITICAL' ELSE 'WARN' END AS severity,
      COALESCE(impacto_estimado,0)::numeric(18,2) AS impacto_estimado,
      format('Funcionário %s com risco elevado', funcionario_nome) AS title,
      format('Score médio %s com %s eventos (%s alto risco). Impacto estimado R$ %s.',
             round(score_medio::numeric, 1), eventos, alto_risco, to_char(COALESCE(impacto_estimado,0),'FM999G999G990D00')) AS message,
      'Auditar descontos/cancelamentos do colaborador e validar permissões no turno.'::text AS recommendation,
      jsonb_build_object(
        'id_funcionario', id_funcionario,
        'eventos', eventos,
        'alto_risco', alto_risco,
        'score_medio', score_medio
      ) AS meta
    FROM top_func_raw
    WHERE rn <= 3
  ),
  vendas_now AS (
    SELECT
      a.id_empresa,
      a.id_filial,
      a.data_key,
      to_date(a.data_key::text, 'YYYYMMDD') AS dt_ref,
      COALESCE(a.faturamento,0)::numeric(18,2) AS faturamento,
      COALESCE(a.margem,0)::numeric(18,2) AS margem,
      COALESCE(a.ticket_medio,0)::numeric(18,2) AS ticket_medio,
      CASE WHEN COALESCE(a.faturamento,0) > 0 THEN (a.margem / a.faturamento) ELSE 0 END AS margem_pct
    FROM mart.agg_vendas_diaria a
    JOIN ref_days d ON d.data_key = a.data_key
    WHERE a.id_empresa = p_id_empresa
  ),
  vendas_prev AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.data_key,
      COALESCE(AVG(p.ticket_medio),0)::numeric(18,2) AS avg_ticket_prev,
      COALESCE(AVG(CASE WHEN p.faturamento > 0 THEN (p.margem / p.faturamento) ELSE 0 END),0) AS avg_margem_pct_prev,
      COALESCE(AVG(p.faturamento),0)::numeric(18,2) AS avg_fat_prev
    FROM vendas_now n
    LEFT JOIN mart.agg_vendas_diaria p
      ON p.id_empresa = n.id_empresa
     AND p.id_filial = n.id_filial
     AND p.data_key BETWEEN (n.data_key - 7) AND (n.data_key - 1)
    GROUP BY 1,2,3
  ),
  margem_baixa AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.dt_ref,
      'MARGEM_BAIXA'::text AS insight_type,
      'WARN'::text AS severity,
      GREATEST(0, n.faturamento * (vp.avg_margem_pct_prev - n.margem_pct))::numeric(18,2) AS impacto_estimado,
      format('Margem pressionada na filial %s', n.id_filial) AS title,
      format('Margem atual %s%% vs histórico %s%%.', round((n.margem_pct * 100)::numeric, 2), round((vp.avg_margem_pct_prev * 100)::numeric, 2)) AS message,
      'Revisar mix, custos e descontos concedidos no período.'::text AS recommendation,
      jsonb_build_object('margem_pct', n.margem_pct, 'margem_pct_historica', vp.avg_margem_pct_prev) AS meta
    FROM vendas_now n
    JOIN vendas_prev vp
      ON vp.id_empresa = n.id_empresa
     AND vp.id_filial = n.id_filial
     AND vp.data_key = n.data_key
    WHERE n.margem_pct < (vp.avg_margem_pct_prev - 0.03)
  ),
  ticket_queda AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.dt_ref,
      'TICKET_QUEDA'::text AS insight_type,
      'WARN'::text AS severity,
      GREATEST(0, (vp.avg_ticket_prev - n.ticket_medio) * 20)::numeric(18,2) AS impacto_estimado,
      format('Ticket médio em queda na filial %s', n.id_filial) AS title,
      format('Ticket atual R$ %s vs média recente R$ %s.',
             to_char(n.ticket_medio,'FM999G999G990D00'),
             to_char(vp.avg_ticket_prev,'FM999G999G990D00')) AS message,
      'Ativar estratégia de upsell e combos no time comercial.'::text AS recommendation,
      jsonb_build_object('ticket_atual', n.ticket_medio, 'ticket_media_7d', vp.avg_ticket_prev) AS meta
    FROM vendas_now n
    JOIN vendas_prev vp
      ON vp.id_empresa = n.id_empresa
     AND vp.id_filial = n.id_filial
     AND vp.data_key = n.data_key
    WHERE n.ticket_medio < (vp.avg_ticket_prev * 0.90)
  ),
  faturamento_queda AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.dt_ref,
      'FATURAMENTO_QUEDA'::text AS insight_type,
      CASE WHEN n.faturamento < (vp.avg_fat_prev * 0.70) THEN 'CRITICAL' ELSE 'WARN' END AS severity,
      GREATEST(0, vp.avg_fat_prev - n.faturamento)::numeric(18,2) AS impacto_estimado,
      format('Faturamento abaixo do ritmo na filial %s', n.id_filial) AS title,
      format('Faturamento atual R$ %s vs média da semana R$ %s.',
             to_char(n.faturamento,'FM999G999G990D00'),
             to_char(vp.avg_fat_prev,'FM999G999G990D00')) AS message,
      'Reforçar campanha local e checar ruptura/preço nas categorias críticas.'::text AS recommendation,
      jsonb_build_object('faturamento_atual', n.faturamento, 'faturamento_media_7d', vp.avg_fat_prev) AS meta
    FROM vendas_now n
    JOIN vendas_prev vp
      ON vp.id_empresa = n.id_empresa
     AND vp.id_filial = n.id_filial
     AND vp.data_key = n.data_key
    WHERE n.faturamento < (vp.avg_fat_prev * 0.85)
  ),
  all_insights AS (
    SELECT * FROM cancel_abn
    UNION ALL
    SELECT * FROM top_func
    UNION ALL
    SELECT * FROM margem_baixa
    UNION ALL
    SELECT * FROM ticket_queda
    UNION ALL
    SELECT * FROM faturamento_queda
  ),
  upserted AS (
    INSERT INTO app.insights_gerados (
      id_empresa,
      id_filial,
      insight_type,
      severity,
      dt_ref,
      impacto_estimado,
      title,
      message,
      recommendation,
      status,
      meta
    )
    SELECT
      i.id_empresa,
      i.id_filial,
      i.insight_type,
      i.severity,
      i.dt_ref,
      i.impacto_estimado,
      i.title,
      i.message,
      i.recommendation,
      'NOVO'::text,
      i.meta
    FROM all_insights i
    ON CONFLICT ON CONSTRAINT uq_insights_gerados_nk
    DO UPDATE SET
      severity = EXCLUDED.severity,
      impacto_estimado = EXCLUDED.impacto_estimado,
      title = EXCLUDED.title,
      message = EXCLUDED.message,
      recommendation = EXCLUDED.recommendation,
      meta = EXCLUDED.meta
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- =========================
-- ETL: run_all integration
-- =========================

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := now();
  v_meta jsonb := '{}'::jsonb;
  v_id bigint;
BEGIN
  INSERT INTO etl.run_log (id_empresa, meta) VALUES (
    p_id_empresa,
    jsonb_build_object('status','running', 'force_full', p_force_full)
  )
  RETURNING id INTO v_id;

  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_meta := v_meta || jsonb_build_object('dim_filial', etl.load_dim_filial(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_grupos', etl.load_dim_grupos(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_localvendas', etl.load_dim_localvendas(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_produtos', etl.load_dim_produtos(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', etl.load_dim_funcionarios(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_clientes', etl.load_dim_clientes(p_id_empresa));

  v_meta := v_meta || jsonb_build_object('fact_comprovante', etl.load_fact_comprovante(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_venda', etl.load_fact_venda(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_venda_item', etl.load_fact_venda_item(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_financeiro', etl.load_fact_financeiro(p_id_empresa));

  v_meta := v_meta || jsonb_build_object('risk_events', etl.compute_risk_events(p_id_empresa, p_force_full, 14));

  IF p_refresh_mart THEN
    PERFORM etl.refresh_marts();
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false);
  END IF;

  v_meta := v_meta || jsonb_build_object('insights_generated', etl.generate_insights(p_id_empresa, CURRENT_DATE, 7));

  UPDATE etl.run_log
  SET finished_at = now(), meta = v_meta
  WHERE id = v_id;

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'started_at', v_started,
    'finished_at', now(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  UPDATE etl.run_log
  SET
    finished_at = now(),
    meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
      'status', 'failed',
      'error', SQLERRM,
      'meta_partial', v_meta
    )
  WHERE id = v_id;
  RAISE;
END;
$$ LANGUAGE plpgsql;

COMMIT;
