BEGIN;

-- ==========================================
-- Performance indexes for historical snapshots
-- ==========================================

CREATE INDEX IF NOT EXISTS ix_fact_venda_customer_day_active
  ON dw.fact_venda (id_empresa, id_cliente, id_filial, data_key)
  WHERE COALESCE(cancelado, false) = false AND id_cliente IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_venda_join_item
  ON dw.fact_venda (id_empresa, id_filial, id_db, id_movprodutos, id_comprovante);

CREATE INDEX IF NOT EXISTS ix_fact_venda_item_join_cover
  ON dw.fact_venda_item (id_empresa, id_filial, id_db, id_movprodutos)
  INCLUDE (total, cfop);

CREATE INDEX IF NOT EXISTS ix_fact_financeiro_snapshot_lookup
  ON dw.fact_financeiro (id_empresa, id_filial, COALESCE(vencimento, data_emissao), tipo_titulo, data_pagamento);

CREATE INDEX IF NOT EXISTS brin_fact_venda_data_key
  ON dw.fact_venda USING brin (data_key);

CREATE INDEX IF NOT EXISTS brin_fact_financeiro_venc
  ON dw.fact_financeiro USING brin (data_key_venc);

CREATE INDEX IF NOT EXISTS ix_mart_customer_rfm_dt_lookup
  ON mart.customer_rfm_daily (id_empresa, id_filial, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_mart_customer_churn_dt_lookup
  ON mart.customer_churn_risk_daily (id_empresa, id_filial, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_mart_finance_aging_dt_lookup
  ON mart.finance_aging_daily (id_empresa, id_filial, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_mart_health_score_dt_lookup
  ON mart.health_score_daily (id_empresa, id_filial, dt_ref DESC);

CREATE TABLE IF NOT EXISTS mart.customer_sales_daily (
  dt_ref             date NOT NULL,
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_cliente         integer NOT NULL,
  compras_dia        integer NOT NULL DEFAULT 0,
  valor_dia          numeric(18,2) NOT NULL DEFAULT 0,
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dt_ref, id_empresa, id_filial, id_cliente)
);

CREATE INDEX IF NOT EXISTS ix_mart_customer_sales_lookup
  ON mart.customer_sales_daily (id_empresa, id_filial, id_cliente, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_mart_customer_sales_dt_lookup
  ON mart.customer_sales_daily (id_empresa, dt_ref DESC, id_filial);

CREATE INDEX IF NOT EXISTS ix_dim_cliente_lookup_latest
  ON dw.dim_cliente (id_empresa, id_cliente, id_filial, updated_at DESC)
  INCLUDE (nome);

-- ==========================================
-- Resumable backfill control
-- ==========================================

CREATE TABLE IF NOT EXISTS app.snapshot_backfill_runs (
  id                  bigserial PRIMARY KEY,
  id_empresa          integer NOT NULL,
  range_start_dt_ref  date NOT NULL,
  range_end_dt_ref    date NOT NULL,
  step_days           integer NOT NULL DEFAULT 7,
  next_dt_ref         date NOT NULL,
  status              text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','failed','completed','cancelled')),
  started_at          timestamptz NOT NULL DEFAULT now(),
  finished_at         timestamptz NULL,
  last_error          text NULL,
  meta                jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_snapshot_backfill_runs_lookup
  ON app.snapshot_backfill_runs (id_empresa, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS app.snapshot_backfill_steps (
  id                  bigserial PRIMARY KEY,
  run_id              bigint NOT NULL REFERENCES app.snapshot_backfill_runs(id) ON DELETE CASCADE,
  snapshot_name       text NOT NULL,
  chunk_start_dt_ref  date NOT NULL,
  chunk_end_dt_ref    date NOT NULL,
  status              text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','failed','completed')),
  rows_written        integer NOT NULL DEFAULT 0,
  started_at          timestamptz NOT NULL DEFAULT now(),
  finished_at         timestamptz NULL,
  duration_ms         integer NULL,
  error_message       text NULL,
  UNIQUE (run_id, snapshot_name, chunk_start_dt_ref, chunk_end_dt_ref)
);

CREATE INDEX IF NOT EXISTS ix_snapshot_backfill_steps_lookup
  ON app.snapshot_backfill_steps (run_id, chunk_start_dt_ref, snapshot_name);

CREATE OR REPLACE FUNCTION etl.backfill_customer_sales_daily_range(
  p_id_empresa integer,
  p_start_date date,
  p_end_date date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
  v_start_key integer := to_char(p_start_date, 'YYYYMMDD')::integer;
  v_end_key integer := to_char(p_end_date, 'YYYYMMDD')::integer;
BEGIN
  DELETE FROM mart.customer_sales_daily
  WHERE dt_ref BETWEEN p_start_date AND p_end_date
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH sales AS (
    SELECT
      to_date(v.data_key::text, 'YYYYMMDD') AS dt_ref,
      v.id_empresa,
      v.id_filial,
      v.id_cliente,
      COUNT(DISTINCT v.id_comprovante)::int AS compras_dia,
      COALESCE(SUM(i.total),0)::numeric(18,2) AS valor_dia
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE (p_id_empresa IS NULL OR v.id_empresa = p_id_empresa)
      AND v.data_key BETWEEN v_start_key AND v_end_key
      AND COALESCE(v.cancelado, false) = false
      AND v.id_cliente IS NOT NULL
      AND COALESCE(i.cfop, 0) >= 5000
    GROUP BY 1,2,3,4
  ), inserted AS (
    INSERT INTO mart.customer_sales_daily (
      dt_ref, id_empresa, id_filial, id_cliente, compras_dia, valor_dia, updated_at
    )
    SELECT
      dt_ref, id_empresa, id_filial, id_cliente, compras_dia, valor_dia, now()
    FROM sales
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.backfill_customer_rfm_day(
  p_id_empresa integer,
  p_dt_ref date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  DELETE FROM mart.customer_rfm_daily
  WHERE dt_ref = p_dt_ref
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH customer_names AS (
    SELECT DISTINCT ON (d.id_empresa, d.id_filial, d.id_cliente)
      d.id_empresa,
      d.id_filial,
      d.id_cliente,
      d.nome
    FROM dw.dim_cliente d
    WHERE p_id_empresa IS NULL OR d.id_empresa = p_id_empresa
    ORDER BY d.id_empresa, d.id_filial, d.id_cliente, d.updated_at DESC
  ), customer_names_global AS (
    SELECT DISTINCT ON (d.id_empresa, d.id_cliente)
      d.id_empresa,
      d.id_cliente,
      d.nome
    FROM dw.dim_cliente d
    WHERE p_id_empresa IS NULL OR d.id_empresa = p_id_empresa
    ORDER BY d.id_empresa, d.id_cliente, d.updated_at DESC
  ), sales AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.dt_ref AS dt_compra,
      s.compras_dia,
      s.valor_dia
    FROM mart.customer_sales_daily s
    WHERE s.dt_ref BETWEEN p_dt_ref - 180 AND p_dt_ref
      AND (p_id_empresa IS NULL OR s.id_empresa = p_id_empresa)
  ), purchase_history AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.dt_compra,
      LAG(s.dt_compra) OVER (
        PARTITION BY s.id_empresa, s.id_filial, s.id_cliente
        ORDER BY s.dt_compra
      ) AS prev_dt
    FROM sales s
  ), agg AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      MAX(s.dt_compra) AS last_purchase,
      SUM(CASE WHEN s.dt_compra BETWEEN p_dt_ref - 29 AND p_dt_ref THEN s.compras_dia ELSE 0 END)::int AS frequency_30,
      SUM(CASE WHEN s.dt_compra BETWEEN p_dt_ref - 89 AND p_dt_ref THEN s.compras_dia ELSE 0 END)::int AS frequency_90,
      SUM(CASE WHEN s.dt_compra BETWEEN p_dt_ref - 29 AND p_dt_ref THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_30,
      SUM(CASE WHEN s.dt_compra BETWEEN p_dt_ref - 89 AND p_dt_ref THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_90
    FROM sales s
    WHERE s.dt_compra BETWEEN p_dt_ref - 179 AND p_dt_ref
    GROUP BY 1,2,3
  ), expected_cycle AS (
    SELECT
      ph.id_empresa,
      ph.id_filial,
      ph.id_cliente,
      COALESCE(
        percentile_cont(0.5) WITHIN GROUP (
          ORDER BY GREATEST(1, (ph.dt_compra - ph.prev_dt))
        ),
        30
      )::numeric(10,2) AS expected_cycle_days
    FROM purchase_history ph
    WHERE ph.prev_dt IS NOT NULL
      AND ph.dt_compra BETWEEN p_dt_ref - 179 AND p_dt_ref
    GROUP BY 1,2,3
  ), inserted AS (
    INSERT INTO mart.customer_rfm_daily (
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      trend_frequency,trend_monetary,updated_at
    )
    SELECT
      p_dt_ref,
      a.id_empresa,
      a.id_filial,
      a.id_cliente,
      COALESCE(NULLIF(cf.nome, ''), NULLIF(cg.nome, ''), '#ID ' || a.id_cliente::text) AS cliente_nome,
      a.last_purchase,
      GREATEST(0, (p_dt_ref - a.last_purchase))::int AS recency_days,
      a.frequency_30,
      a.frequency_90,
      a.monetary_30,
      a.monetary_90,
      CASE WHEN a.frequency_30 > 0 THEN (a.monetary_30 / a.frequency_30)::numeric(18,2) ELSE 0::numeric(18,2) END AS ticket_30,
      COALESCE(ec.expected_cycle_days, 30)::numeric(10,2) AS expected_cycle_days,
      GREATEST(0, a.frequency_30 - GREATEST(0, a.frequency_90 - a.frequency_30))::int AS trend_frequency,
      (a.monetary_30 - GREATEST(0::numeric, a.monetary_90 - a.monetary_30))::numeric(18,2) AS trend_monetary,
      now()
    FROM agg a
    LEFT JOIN expected_cycle ec
      ON ec.id_empresa = a.id_empresa
     AND ec.id_filial = a.id_filial
     AND ec.id_cliente = a.id_cliente
    LEFT JOIN customer_names cf
      ON cf.id_empresa = a.id_empresa
     AND cf.id_filial = a.id_filial
     AND cf.id_cliente = a.id_cliente
    LEFT JOIN customer_names_global cg
      ON cg.id_empresa = a.id_empresa
     AND cg.id_cliente = a.id_cliente
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.backfill_customer_rfm_range(
  p_id_empresa integer,
  p_start_date date,
  p_end_date date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  DELETE FROM mart.customer_rfm_daily
  WHERE dt_ref BETWEEN p_start_date AND p_end_date
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH refs AS (
    SELECT d::date AS dt_ref
    FROM generate_series(p_start_date, p_end_date, interval '1 day') d
  ), customer_names AS (
    SELECT DISTINCT ON (d.id_empresa, d.id_filial, d.id_cliente)
      d.id_empresa,
      d.id_filial,
      d.id_cliente,
      d.nome
    FROM dw.dim_cliente d
    WHERE p_id_empresa IS NULL OR d.id_empresa = p_id_empresa
    ORDER BY d.id_empresa, d.id_filial, d.id_cliente, d.updated_at DESC
  ), customer_names_global AS (
    SELECT DISTINCT ON (d.id_empresa, d.id_cliente)
      d.id_empresa,
      d.id_cliente,
      d.nome
    FROM dw.dim_cliente d
    WHERE p_id_empresa IS NULL OR d.id_empresa = p_id_empresa
    ORDER BY d.id_empresa, d.id_cliente, d.updated_at DESC
  ), sales AS (
    SELECT
      s.dt_ref AS dt_compra,
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.compras_dia,
      s.valor_dia
    FROM mart.customer_sales_daily s
    WHERE s.dt_ref BETWEEN p_start_date - 180 AND p_end_date
      AND (p_id_empresa IS NULL OR s.id_empresa = p_id_empresa)
  ), purchase_history AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.dt_compra,
      LAG(s.dt_compra) OVER (
        PARTITION BY s.id_empresa, s.id_filial, s.id_cliente
        ORDER BY s.dt_compra
      ) AS prev_dt
    FROM sales s
  ), agg AS (
    SELECT
      r.dt_ref,
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      MAX(s.dt_compra) AS last_purchase,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 29 AND r.dt_ref THEN s.compras_dia ELSE 0 END)::int AS frequency_30,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 89 AND r.dt_ref THEN s.compras_dia ELSE 0 END)::int AS frequency_90,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 29 AND r.dt_ref THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_30,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 89 AND r.dt_ref THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_90
    FROM refs r
    JOIN sales s
      ON s.dt_compra BETWEEN r.dt_ref - 179 AND r.dt_ref
    GROUP BY 1,2,3,4
  ), expected_cycle AS (
    SELECT
      r.dt_ref,
      ph.id_empresa,
      ph.id_filial,
      ph.id_cliente,
      COALESCE(
        percentile_cont(0.5) WITHIN GROUP (
          ORDER BY GREATEST(1, (ph.dt_compra - ph.prev_dt))
        ),
        30
      )::numeric(10,2) AS expected_cycle_days
    FROM refs r
    JOIN purchase_history ph
      ON ph.prev_dt IS NOT NULL
     AND ph.dt_compra BETWEEN r.dt_ref - 179 AND r.dt_ref
    GROUP BY 1,2,3,4
  ), inserted AS (
    INSERT INTO mart.customer_rfm_daily (
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      trend_frequency,trend_monetary,updated_at
    )
    SELECT
      a.dt_ref,
      a.id_empresa,
      a.id_filial,
      a.id_cliente,
      COALESCE(NULLIF(cf.nome, ''), NULLIF(cg.nome, ''), '#ID ' || a.id_cliente::text) AS cliente_nome,
      a.last_purchase,
      GREATEST(0, (a.dt_ref - a.last_purchase))::int AS recency_days,
      a.frequency_30,
      a.frequency_90,
      a.monetary_30,
      a.monetary_90,
      CASE WHEN a.frequency_30 > 0 THEN (a.monetary_30 / a.frequency_30)::numeric(18,2) ELSE 0::numeric(18,2) END AS ticket_30,
      COALESCE(ec.expected_cycle_days, 30)::numeric(10,2) AS expected_cycle_days,
      GREATEST(0, a.frequency_30 - GREATEST(0, a.frequency_90 - a.frequency_30))::int AS trend_frequency,
      (a.monetary_30 - GREATEST(0::numeric, a.monetary_90 - a.monetary_30))::numeric(18,2) AS trend_monetary,
      now()
    FROM agg a
    LEFT JOIN expected_cycle ec
      ON ec.dt_ref = a.dt_ref
     AND ec.id_empresa = a.id_empresa
     AND ec.id_filial = a.id_filial
     AND ec.id_cliente = a.id_cliente
    LEFT JOIN customer_names cf
      ON cf.id_empresa = a.id_empresa
     AND cf.id_filial = a.id_filial
     AND cf.id_cliente = a.id_cliente
    LEFT JOIN customer_names_global cg
      ON cg.id_empresa = a.id_empresa
     AND cg.id_cliente = a.id_cliente
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.backfill_customer_churn_risk_day(
  p_id_empresa integer,
  p_dt_ref date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  DELETE FROM mart.customer_churn_risk_daily
  WHERE dt_ref = p_dt_ref
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH base AS (
    SELECT
      r.*,
      GREATEST(0, r.frequency_90 - r.frequency_30)::int AS frequency_prev_60,
      GREATEST(0::numeric, r.monetary_90 - r.monetary_30)::numeric(18,2) AS monetary_prev_60
    FROM mart.customer_rfm_daily r
    WHERE r.dt_ref = p_dt_ref
      AND (p_id_empresa IS NULL OR r.id_empresa = p_id_empresa)
  ), scored AS (
    SELECT
      b.*,
      LEAST(45,
        CASE
          WHEN b.expected_cycle_days > 0 THEN GREATEST(0, ((b.recency_days - b.expected_cycle_days) / NULLIF(b.expected_cycle_days,0)::numeric) * 25)
          ELSE 0
        END
      ) AS p_cycle_break,
      LEAST(30,
        CASE
          WHEN b.frequency_prev_60 > 0 THEN GREATEST(0, ((b.frequency_prev_60 - b.frequency_30)::numeric / b.frequency_prev_60) * 30)
          ELSE CASE WHEN b.frequency_30 = 0 THEN 10 ELSE 0 END
        END
      ) AS p_freq_drop,
      LEAST(25,
        CASE
          WHEN b.monetary_prev_60 > 0 THEN GREATEST(0, ((b.monetary_prev_60 - b.monetary_30) / b.monetary_prev_60) * 25)
          ELSE 0
        END
      ) AS p_monetary_drop
    FROM base b
  ), inserted AS (
    INSERT INTO mart.customer_churn_risk_daily (
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      churn_score,revenue_at_risk_30d,recommendation,reasons,updated_at
    )
    SELECT
      p_dt_ref,
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.cliente_nome,
      s.last_purchase,
      s.recency_days,
      s.frequency_30,
      s.frequency_90,
      s.monetary_30,
      s.monetary_90,
      s.ticket_30,
      s.expected_cycle_days,
      LEAST(100, GREATEST(0, s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop + LEAST(15, LN(1 + s.monetary_90 / 1000.0) * 4)))::int AS churn_score,
      (GREATEST(0::numeric, s.monetary_prev_60) * LEAST(1.0, (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop)/100.0))::numeric(18,2) AS revenue_at_risk_30d,
      CASE
        WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 70 THEN 'Contato imediato + oferta de recuperação em 24h'
        WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 50 THEN 'Campanha personalizada e follow-up comercial em 7 dias'
        ELSE 'Monitorar jornada e reforçar frequência com benefícios'
      END AS recommendation,
      jsonb_build_object(
        'cycle_break', round(s.p_cycle_break::numeric,2),
        'frequency_drop', round(s.p_freq_drop::numeric,2),
        'monetary_drop', round(s.p_monetary_drop::numeric,2),
        'recency_days', s.recency_days,
        'expected_cycle_days', s.expected_cycle_days,
        'frequency_30', s.frequency_30,
        'frequency_90', s.frequency_90,
        'monetary_30', s.monetary_30,
        'monetary_90', s.monetary_90
      ) AS reasons,
      now()
    FROM scored s
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.backfill_customer_churn_risk_range(
  p_id_empresa integer,
  p_start_date date,
  p_end_date date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  DELETE FROM mart.customer_churn_risk_daily
  WHERE dt_ref BETWEEN p_start_date AND p_end_date
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH base AS (
    SELECT
      r.*,
      GREATEST(0, r.frequency_90 - r.frequency_30)::int AS frequency_prev_60,
      GREATEST(0::numeric, r.monetary_90 - r.monetary_30)::numeric(18,2) AS monetary_prev_60
    FROM mart.customer_rfm_daily r
    WHERE r.dt_ref BETWEEN p_start_date AND p_end_date
      AND (p_id_empresa IS NULL OR r.id_empresa = p_id_empresa)
  ), scored AS (
    SELECT
      b.*,
      LEAST(45,
        CASE
          WHEN b.expected_cycle_days > 0 THEN GREATEST(0, ((b.recency_days - b.expected_cycle_days) / NULLIF(b.expected_cycle_days,0)::numeric) * 25)
          ELSE 0
        END
      ) AS p_cycle_break,
      LEAST(30,
        CASE
          WHEN b.frequency_prev_60 > 0 THEN GREATEST(0, ((b.frequency_prev_60 - b.frequency_30)::numeric / b.frequency_prev_60) * 30)
          ELSE CASE WHEN b.frequency_30 = 0 THEN 10 ELSE 0 END
        END
      ) AS p_freq_drop,
      LEAST(25,
        CASE
          WHEN b.monetary_prev_60 > 0 THEN GREATEST(0, ((b.monetary_prev_60 - b.monetary_30) / b.monetary_prev_60) * 25)
          ELSE 0
        END
      ) AS p_monetary_drop
    FROM base b
  ), inserted AS (
    INSERT INTO mart.customer_churn_risk_daily (
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      churn_score,revenue_at_risk_30d,recommendation,reasons,updated_at
    )
    SELECT
      s.dt_ref,
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.cliente_nome,
      s.last_purchase,
      s.recency_days,
      s.frequency_30,
      s.frequency_90,
      s.monetary_30,
      s.monetary_90,
      s.ticket_30,
      s.expected_cycle_days,
      LEAST(100, GREATEST(0, s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop + LEAST(15, LN(1 + s.monetary_90 / 1000.0) * 4)))::int AS churn_score,
      (GREATEST(0::numeric, s.monetary_prev_60) * LEAST(1.0, (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop)/100.0))::numeric(18,2) AS revenue_at_risk_30d,
      CASE
        WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 70 THEN 'Contato imediato + oferta de recuperação em 24h'
        WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 50 THEN 'Campanha personalizada e follow-up comercial em 7 dias'
        ELSE 'Monitorar jornada e reforçar frequência com benefícios'
      END AS recommendation,
      jsonb_build_object(
        'cycle_break', round(s.p_cycle_break::numeric,2),
        'frequency_drop', round(s.p_freq_drop::numeric,2),
        'monetary_drop', round(s.p_monetary_drop::numeric,2),
        'recency_days', s.recency_days,
        'expected_cycle_days', s.expected_cycle_days,
        'frequency_30', s.frequency_30,
        'frequency_90', s.frequency_90,
        'monetary_30', s.monetary_30,
        'monetary_90', s.monetary_90
      ) AS reasons,
      now()
    FROM scored s
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.backfill_finance_aging_range(
  p_id_empresa integer,
  p_start_date date,
  p_end_date date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  DELETE FROM mart.finance_aging_daily
  WHERE dt_ref BETWEEN p_start_date AND p_end_date
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH refs AS (
    SELECT d::date AS dt_ref
    FROM generate_series(p_start_date, p_end_date, interval '1 day') d
  ), base AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.tipo_titulo,
      f.id_entidade,
      COALESCE(f.vencimento, f.data_emissao) AS vencimento,
      COALESCE(f.valor,0)::numeric(18,2) AS valor_total,
      COALESCE(f.valor_pago,0)::numeric(18,2) AS valor_pago,
      f.data_pagamento
    FROM dw.fact_financeiro f
    WHERE (p_id_empresa IS NULL OR f.id_empresa = p_id_empresa)
      AND COALESCE(f.vencimento, f.data_emissao) IS NOT NULL
      AND COALESCE(f.vencimento, f.data_emissao) <= p_end_date
  ), titles_by_ref AS (
    SELECT
      r.dt_ref,
      b.id_empresa,
      b.id_filial,
      b.tipo_titulo,
      b.id_entidade,
      b.vencimento,
      CASE
        WHEN b.data_pagamento IS NULL THEN GREATEST(0::numeric, b.valor_total - b.valor_pago)
        WHEN b.data_pagamento > r.dt_ref THEN GREATEST(0::numeric, b.valor_total)
        ELSE GREATEST(0::numeric, b.valor_total - b.valor_pago)
      END::numeric(18,2) AS valor_aberto
    FROM refs r
    JOIN base b
      ON b.vencimento <= r.dt_ref
     AND (b.data_pagamento IS NULL OR b.data_pagamento > r.dt_ref OR (b.valor_total - b.valor_pago) > 0)
  ), open_titles AS (
    SELECT *
    FROM titles_by_ref
    WHERE valor_aberto > 0
  ), overdue_rank AS (
    SELECT
      o.dt_ref,
      o.id_empresa,
      o.id_filial,
      o.valor_aberto,
      ROW_NUMBER() OVER (
        PARTITION BY o.dt_ref, o.id_empresa, o.id_filial
        ORDER BY o.valor_aberto DESC
      ) AS rn
    FROM open_titles o
    WHERE o.tipo_titulo = 1
      AND o.vencimento < o.dt_ref
  ), top5 AS (
    SELECT
      dt_ref,
      id_empresa,
      id_filial,
      COALESCE(SUM(valor_aberto),0)::numeric(18,2) AS top5_vencido
    FROM overdue_rank
    WHERE rn <= 5
    GROUP BY 1,2,3
  ), totals AS (
    SELECT
      o.dt_ref,
      o.id_empresa,
      o.id_filial,
      SUM(CASE WHEN o.tipo_titulo = 1 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS receber_total_aberto,
      SUM(CASE WHEN o.tipo_titulo = 1 AND o.vencimento < o.dt_ref THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS receber_total_vencido,
      SUM(CASE WHEN o.tipo_titulo = 0 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS pagar_total_aberto,
      SUM(CASE WHEN o.tipo_titulo = 0 AND o.vencimento < o.dt_ref THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS pagar_total_vencido,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 0 AND 7 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_0_7,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 8 AND 15 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_8_15,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 16 AND 30 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_16_30,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 31 AND 60 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_31_60,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) > 60 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_60_plus
    FROM open_titles o
    GROUP BY 1,2,3
  ), keys AS (
    SELECT DISTINCT r.dt_ref, k.id_empresa, k.id_filial
    FROM refs r
    CROSS JOIN (
      SELECT DISTINCT id_empresa, id_filial FROM dw.fact_financeiro WHERE p_id_empresa IS NULL OR id_empresa = p_id_empresa
      UNION
      SELECT DISTINCT id_empresa, id_filial FROM dw.fact_venda WHERE p_id_empresa IS NULL OR id_empresa = p_id_empresa
    ) k
  ), inserted AS (
    INSERT INTO mart.finance_aging_daily (
      dt_ref,id_empresa,id_filial,receber_total_aberto,receber_total_vencido,pagar_total_aberto,pagar_total_vencido,
      bucket_0_7,bucket_8_15,bucket_16_30,bucket_31_60,bucket_60_plus,top5_concentration_pct,data_gaps,updated_at
    )
    SELECT
      k.dt_ref,
      k.id_empresa,
      k.id_filial,
      COALESCE(t.receber_total_aberto,0)::numeric(18,2),
      COALESCE(t.receber_total_vencido,0)::numeric(18,2),
      COALESCE(t.pagar_total_aberto,0)::numeric(18,2),
      COALESCE(t.pagar_total_vencido,0)::numeric(18,2),
      COALESCE(t.bucket_0_7,0)::numeric(18,2),
      COALESCE(t.bucket_8_15,0)::numeric(18,2),
      COALESCE(t.bucket_16_30,0)::numeric(18,2),
      COALESCE(t.bucket_31_60,0)::numeric(18,2),
      COALESCE(t.bucket_60_plus,0)::numeric(18,2),
      CASE WHEN COALESCE(t.receber_total_vencido,0) > 0
        THEN (COALESCE(tp.top5_vencido,0) / NULLIF(t.receber_total_vencido,0) * 100)::numeric(10,2)
        ELSE 0::numeric(10,2)
      END AS top5_concentration_pct,
      (COALESCE(t.receber_total_aberto,0)=0 AND COALESCE(t.pagar_total_aberto,0)=0) AS data_gaps,
      now()
    FROM keys k
    LEFT JOIN totals t
      ON t.dt_ref = k.dt_ref
     AND t.id_empresa = k.id_empresa
     AND t.id_filial = k.id_filial
    LEFT JOIN top5 tp
      ON tp.dt_ref = k.dt_ref
     AND tp.id_empresa = k.id_empresa
     AND tp.id_filial = k.id_filial
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.backfill_health_score_range(
  p_id_empresa integer,
  p_start_date date,
  p_end_date date
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
  v_min_sales_key integer := to_char((p_start_date - 29), 'YYYYMMDD')::integer;
  v_max_day_key integer := to_char(p_end_date, 'YYYYMMDD')::integer;
BEGIN
  DELETE FROM mart.health_score_daily
  WHERE dt_ref BETWEEN p_start_date AND p_end_date
    AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH refs AS (
    SELECT d::date AS dt_ref
    FROM generate_series(p_start_date, p_end_date, interval '1 day') d
  ), keys AS (
    SELECT DISTINCT r.dt_ref, k.id_empresa, k.id_filial
    FROM refs r
    CROSS JOIN (
      SELECT DISTINCT id_empresa, id_filial FROM mart.agg_vendas_diaria WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
      UNION
      SELECT DISTINCT id_empresa, id_filial FROM mart.agg_risco_diaria WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
      UNION
      SELECT DISTINCT id_empresa, id_filial FROM mart.finance_aging_daily WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
    ) k
  ), sales_src AS (
    SELECT *
    FROM mart.agg_vendas_diaria
    WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
      AND data_key BETWEEN v_min_sales_key AND v_max_day_key
  ), risk_src AS (
    SELECT *
    FROM mart.agg_risco_diaria
    WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
      AND data_key BETWEEN v_min_sales_key AND v_max_day_key
  ), sales AS (
    SELECT
      r.dt_ref,
      v.id_empresa,
      v.id_filial,
      COALESCE(SUM(v.faturamento),0)::numeric(18,2) AS fat_30d,
      COALESCE(SUM(v.margem),0)::numeric(18,2) AS margem_30d,
      COALESCE(AVG(v.ticket_medio),0)::numeric(18,2) AS ticket_30d
    FROM refs r
    JOIN sales_src v
      ON to_date(v.data_key::text, 'YYYYMMDD') BETWEEN r.dt_ref - 29 AND r.dt_ref
    GROUP BY 1,2,3
  ), risk AS (
    SELECT
      r.dt_ref,
      x.id_empresa,
      x.id_filial,
      COALESCE(SUM(x.eventos_alto_risco),0)::int AS high_risk_30d,
      COALESCE(SUM(x.eventos_risco_total),0)::int AS total_risk_30d,
      COALESCE(SUM(x.impacto_estimado_total),0)::numeric(18,2) AS impacto_risco_30d
    FROM refs r
    JOIN risk_src x
      ON to_date(x.data_key::text, 'YYYYMMDD') BETWEEN r.dt_ref - 29 AND r.dt_ref
    GROUP BY 1,2,3
  ), churn AS (
    SELECT
      dt_ref,
      id_empresa,
      id_filial,
      COALESCE(AVG(churn_score),0)::numeric(10,2) AS churn_score_avg,
      COALESCE(SUM(revenue_at_risk_30d),0)::numeric(18,2) AS revenue_at_risk_30d
    FROM mart.customer_churn_risk_daily
    WHERE dt_ref BETWEEN p_start_date AND p_end_date
      AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
    GROUP BY 1,2,3
  ), fin AS (
    SELECT
      dt_ref,
      id_empresa,
      id_filial,
      receber_total_aberto,
      receber_total_vencido,
      pagar_total_aberto,
      pagar_total_vencido,
      data_gaps
    FROM mart.finance_aging_daily
    WHERE dt_ref BETWEEN p_start_date AND p_end_date
      AND (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
  ), comp AS (
    SELECT
      k.dt_ref,
      k.id_empresa,
      k.id_filial,
      COALESCE(s.fat_30d,0) AS fat_30d,
      COALESCE(s.margem_30d,0) AS margem_30d,
      COALESCE(s.ticket_30d,0) AS ticket_30d,
      COALESCE(r.high_risk_30d,0) AS high_risk_30d,
      COALESCE(r.total_risk_30d,0) AS total_risk_30d,
      COALESCE(r.impacto_risco_30d,0) AS impacto_risco_30d,
      COALESCE(c.churn_score_avg,0) AS churn_score_avg,
      COALESCE(c.revenue_at_risk_30d,0) AS revenue_at_risk_30d,
      COALESCE(f.receber_total_aberto,0) AS receber_total_aberto,
      COALESCE(f.receber_total_vencido,0) AS receber_total_vencido,
      COALESCE(f.pagar_total_aberto,0) AS pagar_total_aberto,
      COALESCE(f.pagar_total_vencido,0) AS pagar_total_vencido,
      COALESCE(f.data_gaps, true) AS finance_data_gaps
    FROM keys k
    LEFT JOIN sales s
      ON s.dt_ref = k.dt_ref
     AND s.id_empresa = k.id_empresa
     AND s.id_filial = k.id_filial
    LEFT JOIN risk r
      ON r.dt_ref = k.dt_ref
     AND r.id_empresa = k.id_empresa
     AND r.id_filial = k.id_filial
    LEFT JOIN churn c
      ON c.dt_ref = k.dt_ref
     AND c.id_empresa = k.id_empresa
     AND c.id_filial = k.id_filial
    LEFT JOIN fin f
      ON f.dt_ref = k.dt_ref
     AND f.id_empresa = k.id_empresa
     AND f.id_filial = k.id_filial
  ), scored AS (
    SELECT
      c.*,
      LEAST(100, GREATEST(0, CASE WHEN c.fat_30d > 0 THEN (c.margem_30d / c.fat_30d) * 500 ELSE 0 END))::numeric(10,2) AS comp_margem,
      LEAST(100, GREATEST(0, 100 - (CASE WHEN c.total_risk_30d > 0 THEN (c.high_risk_30d::numeric / c.total_risk_30d) * 120 ELSE 0 END) - (c.impacto_risco_30d / GREATEST(c.fat_30d,1)) * 100))::numeric(10,2) AS comp_fraude,
      LEAST(100, GREATEST(0, 100 - c.churn_score_avg))::numeric(10,2) AS comp_churn,
      LEAST(100, GREATEST(0, 100 - (CASE WHEN c.receber_total_aberto > 0 THEN (c.receber_total_vencido / c.receber_total_aberto) * 120 ELSE 0 END)))::numeric(10,2) AS comp_finance,
      LEAST(100, GREATEST(0, (c.ticket_30d / 120) * 100))::numeric(10,2) AS comp_operacao,
      CASE WHEN c.finance_data_gaps THEN 45::numeric(10,2) ELSE 90::numeric(10,2) END AS comp_dados
    FROM comp c
  ), inserted AS (
    INSERT INTO mart.health_score_daily (
      dt_ref,id_empresa,id_filial,comp_margem,comp_fraude,comp_churn,comp_finance,comp_operacao,comp_dados,
      score_total,components,reasons,updated_at
    )
    SELECT
      s.dt_ref,
      s.id_empresa,
      s.id_filial,
      s.comp_margem,
      s.comp_fraude,
      s.comp_churn,
      s.comp_finance,
      s.comp_operacao,
      s.comp_dados,
      LEAST(
        CASE
          WHEN s.receber_total_aberto > 0 AND (s.receber_total_vencido / s.receber_total_aberto) > 0.60 THEN 55
          WHEN s.receber_total_aberto > 0 AND (s.receber_total_vencido / s.receber_total_aberto) > 0.40 THEN 65
          ELSE 100
        END,
        CASE
          WHEN s.comp_dados < 60 THEN 75
          ELSE 100
        END,
        GREATEST(0, (s.comp_margem*0.22 + s.comp_fraude*0.23 + s.comp_churn*0.20 + s.comp_finance*0.20 + s.comp_operacao*0.10 + s.comp_dados*0.05))
      )::numeric(10,2) AS score_total,
      jsonb_build_object(
        'margem', s.comp_margem,
        'fraude', s.comp_fraude,
        'churn', s.comp_churn,
        'finance', s.comp_finance,
        'operacao', s.comp_operacao,
        'dados', s.comp_dados
      ) AS components,
      jsonb_build_object(
        'finance_overdue_ratio', CASE WHEN s.receber_total_aberto > 0 THEN round((s.receber_total_vencido/s.receber_total_aberto)::numeric,4) ELSE 0 END,
        'finance_data_gaps', s.finance_data_gaps,
        'revenue_at_risk_30d', s.revenue_at_risk_30d,
        'high_risk_30d', s.high_risk_30d
      ) AS reasons,
      now()
    FROM scored s
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

DROP ROUTINE IF EXISTS etl.run_operational_snapshot_backfill(integer, date, date, integer, boolean, boolean);

CREATE OR REPLACE PROCEDURE etl.run_operational_snapshot_backfill(
  p_id_empresa integer,
  p_start_date date,
  p_end_date date,
  p_step_days integer DEFAULT 7,
  p_resume boolean DEFAULT true,
  p_force_restart boolean DEFAULT false
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_run_id bigint;
  v_next_date date;
  v_chunk_end date;
  v_started timestamptz := clock_timestamp();
  v_step_started timestamptz;
  v_rows integer;
  v_total_rows integer := 0;
  v_snapshot text;
  v_duration_ms integer;
  v_sales_start date;
BEGIN
  IF p_id_empresa IS NULL THEN
    RAISE EXCEPTION 'p_id_empresa is required for resumable backfill';
  END IF;
  IF p_start_date IS NULL OR p_end_date IS NULL OR p_start_date > p_end_date THEN
    RAISE EXCEPTION 'invalid backfill range: % - %', p_start_date, p_end_date;
  END IF;
  IF COALESCE(p_step_days, 0) <= 0 THEN
    RAISE EXCEPTION 'p_step_days must be > 0';
  END IF;

  IF p_force_restart THEN
    UPDATE app.snapshot_backfill_runs
    SET status = 'cancelled',
        finished_at = now(),
        updated_at = now(),
        last_error = 'Superseded by forced restart'
    WHERE id_empresa = p_id_empresa
      AND range_start_dt_ref = p_start_date
      AND range_end_dt_ref = p_end_date
      AND step_days = p_step_days
      AND status IN ('pending','running','failed');
  END IF;

  IF p_resume THEN
    SELECT id, next_dt_ref
      INTO v_run_id, v_next_date
    FROM app.snapshot_backfill_runs
    WHERE id_empresa = p_id_empresa
      AND range_start_dt_ref = p_start_date
      AND range_end_dt_ref = p_end_date
      AND step_days = p_step_days
      AND status IN ('pending','running','failed')
    ORDER BY id DESC
    LIMIT 1;
  END IF;

  IF v_run_id IS NULL THEN
    INSERT INTO app.snapshot_backfill_runs (
      id_empresa, range_start_dt_ref, range_end_dt_ref, step_days, next_dt_ref, status, started_at, meta, updated_at
    )
    VALUES (
      p_id_empresa, p_start_date, p_end_date, p_step_days, p_start_date, 'running', now(),
      jsonb_build_object('precision_mode', jsonb_build_object(
        'customer_sales_daily', 'exact',
        'customer_rfm_daily', 'exact',
        'customer_churn_risk_daily', 'exact',
        'health_score_daily', 'snapshot',
        'finance_aging_daily', 'best_effort'
      )),
      now()
    )
    RETURNING id, next_dt_ref INTO v_run_id, v_next_date;
  ELSE
    UPDATE app.snapshot_backfill_runs
    SET status = 'running',
        updated_at = now(),
        last_error = NULL
    WHERE id = v_run_id;
  END IF;

  UPDATE app.snapshot_backfill_steps
  SET status = 'failed',
      finished_at = now(),
      duration_ms = COALESCE(duration_ms, GREATEST(0, FLOOR(EXTRACT(epoch FROM (clock_timestamp() - started_at)) * 1000)::int)),
      error_message = COALESCE(error_message, 'Interrupted before completion; eligible for resume')
  WHERE run_id = v_run_id
    AND status = 'running';

  v_sales_start := p_start_date - 180;

  COMMIT;

  PERFORM 1
  FROM app.snapshot_backfill_steps
  WHERE run_id = v_run_id
    AND snapshot_name = 'customer_sales_daily'
    AND chunk_start_dt_ref = v_sales_start
    AND chunk_end_dt_ref = p_end_date
    AND status = 'completed';

  IF NOT FOUND THEN
    DELETE FROM app.snapshot_backfill_steps
    WHERE run_id = v_run_id
      AND snapshot_name = 'customer_sales_daily'
      AND chunk_start_dt_ref = v_sales_start
      AND chunk_end_dt_ref = p_end_date
      AND status <> 'completed';

    INSERT INTO app.snapshot_backfill_steps (
      run_id, snapshot_name, chunk_start_dt_ref, chunk_end_dt_ref, status, started_at
    )
    VALUES (v_run_id, 'customer_sales_daily', v_sales_start, p_end_date, 'running', now());

    UPDATE app.snapshot_backfill_runs
    SET status = 'running',
        updated_at = now(),
        meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
          'current_snapshot', 'customer_sales_daily',
          'current_chunk_start', v_sales_start,
          'current_chunk_end', p_end_date
        )
    WHERE id = v_run_id;

    COMMIT;

    v_step_started := clock_timestamp();
    v_rows := etl.backfill_customer_sales_daily_range(p_id_empresa, v_sales_start, p_end_date);
    v_duration_ms := GREATEST(0, FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int);
    v_total_rows := v_total_rows + COALESCE(v_rows, 0);

    UPDATE app.snapshot_backfill_steps
    SET status = 'completed',
        rows_written = COALESCE(v_rows, 0),
        finished_at = now(),
        duration_ms = v_duration_ms,
        error_message = NULL
    WHERE run_id = v_run_id
      AND snapshot_name = 'customer_sales_daily'
      AND chunk_start_dt_ref = v_sales_start
      AND chunk_end_dt_ref = p_end_date;

    UPDATE app.snapshot_backfill_runs
    SET updated_at = now(),
        meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
          'rows_written_total', v_total_rows,
          'last_completed_snapshot', 'customer_sales_daily',
          'last_completed_chunk_end', p_end_date
        )
    WHERE id = v_run_id;

    COMMIT;
  END IF;

  WHILE v_next_date <= p_end_date LOOP
    v_chunk_end := LEAST(v_next_date + (p_step_days - 1), p_end_date);

    FOREACH v_snapshot IN ARRAY ARRAY[
      'customer_rfm_daily',
      'customer_churn_risk_daily',
      'finance_aging_daily',
      'health_score_daily'
    ]
    LOOP
      PERFORM 1
      FROM app.snapshot_backfill_steps
      WHERE run_id = v_run_id
        AND snapshot_name = v_snapshot
        AND chunk_start_dt_ref = v_next_date
        AND chunk_end_dt_ref = v_chunk_end
        AND status = 'completed';

      IF FOUND THEN
        CONTINUE;
      END IF;

      DELETE FROM app.snapshot_backfill_steps
      WHERE run_id = v_run_id
        AND snapshot_name = v_snapshot
        AND chunk_start_dt_ref = v_next_date
        AND chunk_end_dt_ref = v_chunk_end
        AND status <> 'completed';

      INSERT INTO app.snapshot_backfill_steps (
        run_id, snapshot_name, chunk_start_dt_ref, chunk_end_dt_ref, status, started_at
      )
      VALUES (v_run_id, v_snapshot, v_next_date, v_chunk_end, 'running', now());

      UPDATE app.snapshot_backfill_runs
      SET status = 'running',
          updated_at = now(),
          meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
            'current_snapshot', v_snapshot,
            'current_chunk_start', v_next_date,
            'current_chunk_end', v_chunk_end
          )
      WHERE id = v_run_id;

      COMMIT;

      v_step_started := clock_timestamp();
      CASE v_snapshot
        WHEN 'customer_rfm_daily' THEN
          v_rows := etl.backfill_customer_rfm_range(p_id_empresa, v_next_date, v_chunk_end);
        WHEN 'customer_churn_risk_daily' THEN
          v_rows := etl.backfill_customer_churn_risk_range(p_id_empresa, v_next_date, v_chunk_end);
        WHEN 'finance_aging_daily' THEN
          v_rows := etl.backfill_finance_aging_range(p_id_empresa, v_next_date, v_chunk_end);
        WHEN 'health_score_daily' THEN
          v_rows := etl.backfill_health_score_range(p_id_empresa, v_next_date, v_chunk_end);
        ELSE
          RAISE EXCEPTION 'unsupported snapshot %', v_snapshot;
      END CASE;

      v_duration_ms := GREATEST(0, FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int);
      v_total_rows := v_total_rows + COALESCE(v_rows, 0);

      UPDATE app.snapshot_backfill_steps
      SET status = 'completed',
          rows_written = COALESCE(v_rows, 0),
          finished_at = now(),
          duration_ms = v_duration_ms,
          error_message = NULL
      WHERE run_id = v_run_id
        AND snapshot_name = v_snapshot
        AND chunk_start_dt_ref = v_next_date
        AND chunk_end_dt_ref = v_chunk_end;

      UPDATE app.snapshot_backfill_runs
      SET updated_at = now(),
          meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
            'rows_written_total', v_total_rows,
            'last_completed_snapshot', v_snapshot,
            'last_completed_chunk_end', v_chunk_end
          )
      WHERE id = v_run_id;

      COMMIT;
    END LOOP;

    v_next_date := v_chunk_end + 1;
    UPDATE app.snapshot_backfill_runs
    SET next_dt_ref = v_next_date,
        updated_at = now(),
        meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
          'last_completed_chunk_end', v_chunk_end,
          'rows_written_total', v_total_rows
        )
    WHERE id = v_run_id;

    COMMIT;
  END LOOP;

  ANALYZE mart.customer_rfm_daily;
  ANALYZE mart.customer_churn_risk_daily;
  ANALYZE mart.finance_aging_daily;
  ANALYZE mart.health_score_daily;

  UPDATE app.snapshot_backfill_runs
  SET status = 'completed',
      finished_at = now(),
      updated_at = now(),
      next_dt_ref = p_end_date + 1,
      meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
        'rows_written_total', v_total_rows,
        'duration_ms', GREATEST(0, FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_started)) * 1000)::int)
      )
  WHERE id = v_run_id;
END;
$$;

CREATE OR REPLACE FUNCTION etl.refresh_marts(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
DECLARE
  v_meta jsonb := '{}'::jsonb;
  v_sales_changed boolean := COALESCE((p_changed->>'fact_venda')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_venda_item')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_fin_changed boolean := COALESCE((p_changed->>'fact_financeiro')::int,0) > 0;
  v_risk_changed boolean := COALESCE((p_changed->>'risk_events')::int,0) > 0;
  v_payment_changed boolean := COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
                            OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_snapshot_start date := current_date - 45;
  v_fin_snapshot_start date := current_date - 120;
BEGIN
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
    PERFORM etl.backfill_customer_sales_daily_range(NULL, v_snapshot_start - 180, current_date);
    PERFORM etl.backfill_customer_rfm_range(NULL, v_snapshot_start, current_date);
    PERFORM etl.backfill_customer_churn_risk_range(NULL, v_snapshot_start, current_date);
    v_meta := v_meta || jsonb_build_object(
      'sales_marts_refreshed', true,
      'customer_sales_daily_refreshed', true,
      'churn_marts_refreshed', true,
      'snapshot_sales_start_dt_ref', v_snapshot_start
    );
  ELSE
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', false, 'customer_sales_daily_refreshed', false, 'churn_marts_refreshed', false);
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    PERFORM etl.backfill_finance_aging_range(NULL, v_fin_snapshot_start, current_date);
    v_meta := v_meta || jsonb_build_object(
      'finance_mart_refreshed', true,
      'finance_aging_refreshed', true,
      'snapshot_finance_start_dt_ref', v_fin_snapshot_start
    );
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', false, 'finance_aging_refreshed', false);
  END IF;

  IF v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', false);
  END IF;

  IF v_payment_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
    REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', false);
  END IF;

  IF v_sales_changed OR v_fin_changed OR v_risk_changed THEN
    PERFORM etl.backfill_health_score_range(NULL, v_snapshot_start, current_date);
    v_meta := v_meta || jsonb_build_object(
      'health_score_refreshed', true,
      'snapshot_health_start_dt_ref', v_snapshot_start
    );
  ELSE
    v_meta := v_meta || jsonb_build_object('health_score_refreshed', false);
  END IF;

  RETURN v_meta;
END;
$$ LANGUAGE plpgsql;

COMMIT;
