-- @nontransactional
-- ============================================================================
-- Migration 061: Performance Indexes, ETL Fixes, and Data Quality Guards
-- ============================================================================
-- PT-BR: Adiciona indexes ausentes em FK/lookup columns, otimiza ETL compute_risk_events,
--        adiciona indexes para hot-window queries, e melhora controle de watermark/ETL.
-- EN:    Adds missing FK/lookup indexes, optimizes ETL compute_risk_events LATERAL joins,
--        adds hot-window query indexes, and improves watermark/ETL control.
--
-- NOTE: CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
--       This script must be run with: psql -f 061_performance_indexes_and_etl_fixes.sql
-- ============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- PART 1: Missing FK/Lookup Indexes on DW Fact Tables
-- ─────────────────────────────────────────────────────────────────────────────
-- These are the root cause of full table scans on JOIN operations from
-- BI endpoints (churn, RFM, risk scoring, finance aging).

-- fact_venda: used by churn/RFM (id_cliente), cash (id_turno), dashboard (data_key)
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_cliente_data
  ON dw.fact_venda (id_empresa, id_filial, id_cliente, data_key DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_turno
  ON dw.fact_venda (id_empresa, id_filial, id_turno)
  WHERE id_turno IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_data_cancel
  ON dw.fact_venda (id_empresa, data_key DESC, cancelado)
  INCLUDE (id_filial, valor_total);

-- fact_comprovante: used by risk scoring (id_usuario), fraud dashboard
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_comprovante_usuario_data
  ON dw.fact_comprovante (id_empresa, id_filial, id_usuario, data_key DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_comprovante_cancel_data
  ON dw.fact_comprovante (id_empresa, id_filial, data_key, cancelado)
  WHERE cancelado = true;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_comprovante_turno
  ON dw.fact_comprovante (id_empresa, id_filial, id_turno)
  WHERE id_turno IS NOT NULL;

-- fact_venda_item: used by product profitability, sales by group
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_item_produto
  ON dw.fact_venda_item (id_empresa, id_filial, id_produto);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_item_local_venda
  ON dw.fact_venda_item (id_empresa, id_filial, id_local_venda)
  WHERE id_local_venda IS NOT NULL;

-- fact_financeiro: used by aging analysis (vencimento + entidade)
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_financeiro_aging
  ON dw.fact_financeiro (id_empresa, id_filial, vencimento, status)
  WHERE vencimento IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_financeiro_entidade
  ON dw.fact_financeiro (id_empresa, id_filial, id_entidade)
  WHERE id_entidade IS NOT NULL;

-- fact_pagamento_comprovante: used by payment mix dashboard
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_pagamento_data
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, data_key DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 2: STG Table Indexes for ETL Hot Window Performance
-- ─────────────────────────────────────────────────────────────────────────────
-- The hot-window OR condition (received_at > wm OR dt_evento >= now()-3d) needs
-- separate indexes for each branch of the OR to allow bitmap index scan.

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_comprovantes_received
  ON stg.comprovantes (id_empresa, received_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_comprovantes_dt_evento
  ON stg.comprovantes (id_empresa, dt_evento DESC)
  WHERE dt_evento IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_movprodutos_received
  ON stg.movprodutos (id_empresa, received_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_movprodutos_dt_evento
  ON stg.movprodutos (id_empresa, dt_evento DESC)
  WHERE dt_evento IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_itensmovprodutos_received
  ON stg.itensmovprodutos (id_empresa, received_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_itensmovprodutos_dt_evento
  ON stg.itensmovprodutos (id_empresa, dt_evento DESC)
  WHERE dt_evento IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_turnos_received
  ON stg.turnos (id_empresa, received_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_financeiro_received
  ON stg.financeiro (id_empresa, received_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_formas_pgto_received
  ON stg.formas_pgto_comprovantes (id_empresa, received_at DESC);

-- ETL run_log: status index for stale detection
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_etl_run_log_status_started
  ON etl.run_log (status, started_at DESC)
  WHERE status = 'running';


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 3: ETL Control Table Indexes (transactional — safe inside BEGIN)
-- ─────────────────────────────────────────────────────────────────────────────

BEGIN;

-- Fast lookup for watermark by (empresa, dataset) - used by every ETL function
CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_watermark_empresa_dataset
  ON etl.watermark (id_empresa, dataset);

COMMIT;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 4: MART Materialized View Indexes (transactional)
-- ─────────────────────────────────────────────────────────────────────────────
-- Unique indexes enable REFRESH MATERIALIZED VIEW CONCURRENTLY.
-- Regular CREATE INDEX (non-concurrent) is safe inside DO blocks.

BEGIN;

DO $$
BEGIN
  -- agg_vendas_diaria
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'agg_vendas_diaria') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_vendas_diaria_pk
      ON mart.agg_vendas_diaria (id_empresa, id_filial, data_key)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_agg_vendas_diaria_empresa_data
      ON mart.agg_vendas_diaria (id_empresa, data_key DESC)';
  END IF;

  -- agg_risco_diaria
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'agg_risco_diaria') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_risco_diaria_pk
      ON mart.agg_risco_diaria (id_empresa, id_filial, data_key)';
  END IF;

  -- customer_rfm_daily
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'customer_rfm_daily') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_rfm_daily_pk
      ON mart.customer_rfm_daily (id_empresa, id_filial, id_cliente)';
  END IF;

  -- customer_churn_risk_daily
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'customer_churn_risk_daily') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_churn_risk_daily_pk
      ON mart.customer_churn_risk_daily (id_empresa, id_filial, id_cliente)';
  END IF;

  -- finance_aging_daily
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'finance_aging_daily') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_finance_aging_daily_pk
      ON mart.finance_aging_daily (id_empresa, id_filial)';
  END IF;

  -- health_score_daily
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'health_score_daily') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_health_score_daily_pk
      ON mart.health_score_daily (id_empresa, id_filial)';
  END IF;

  -- agg_pagamentos_diaria
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'agg_pagamentos_diaria') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_pagamentos_diaria_pk
      ON mart.agg_pagamentos_diaria (id_empresa, id_filial, data_key, tipo_forma)';
  END IF;

  -- risco_top_funcionarios_diaria
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'risco_top_funcionarios_diaria') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_risco_top_func_diaria_pk
      ON mart.risco_top_funcionarios_diaria (id_empresa, id_filial, id_funcionario, data_key)';
  END IF;
END$$;

COMMIT;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 5: Optimized compute_risk_events — Replace LATERAL with CTE Pre-Aggregation
-- ─────────────────────────────────────────────────────────────────────────────
-- The original LATERAL JOIN with percentile_cont recalculates p90/p95/p10
-- per row. With millions of records, this causes exponential query time.
-- This version pre-aggregates percentiles as CTEs and JOINs them.
--
-- Named v2 to allow gradual migration: call etl.compute_risk_events_v2()
-- from the ETL orchestrator, then drop the old function when validated.

CREATE OR REPLACE FUNCTION etl.compute_risk_events_v2(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_lookback_days int DEFAULT 14,
  p_end_ts timestamptz DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_from_date   date;
  v_to_date     date;
  v_rows        integer := 0;
  v_del         integer := 0;
BEGIN
  v_to_date   := COALESCE(p_end_ts, now())::date;
  v_from_date := v_to_date - make_interval(days => p_lookback_days);

  -- Delete existing risk events in the window (idempotent re-run)
  IF p_force_full THEN
    DELETE FROM dw.fact_risco_evento WHERE id_empresa = p_id_empresa;
    GET DIAGNOSTICS v_del = ROW_COUNT;
  ELSE
    DELETE FROM dw.fact_risco_evento
    WHERE id_empresa = p_id_empresa
      AND data_key >= to_char(v_from_date, 'YYYYMMDD')::int
      AND data_key <= to_char(v_to_date, 'YYYYMMDD')::int;
    GET DIAGNOSTICS v_del = ROW_COUNT;
  END IF;

  -- Insert risk events using pre-aggregated percentiles (no LATERAL)
  WITH
  -- ── Pre-aggregate p90 cancellation values per (empresa, filial, day) ──
  p90_cancel AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      percentile_cont(0.90) WITHIN GROUP (ORDER BY c.valor_total) AS p90_valor
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.cancelado = true
      AND c.valor_total IS NOT NULL
      AND c.data_key >= to_char(v_from_date, 'YYYYMMDD')::int
      AND c.data_key <= to_char(v_to_date, 'YYYYMMDD')::int
    GROUP BY c.id_empresa, c.id_filial, c.data_key
  ),

  -- ── Pre-aggregate user cancel stats ──
  user_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_usuario,
      c.data_key,
      COUNT(*) FILTER (WHERE c.cancelado = true) AS user_cancels,
      COUNT(*) AS user_total,
      CASE WHEN COUNT(*) > 0
        THEN ROUND(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / COUNT(*)::numeric, 4)
        ELSE 0
      END AS user_cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data_key >= to_char(v_from_date, 'YYYYMMDD')::int
      AND c.data_key <= to_char(v_to_date, 'YYYYMMDD')::int
    GROUP BY c.id_empresa, c.id_filial, c.id_usuario, c.data_key
  ),

  -- ── Canceled receipt events (JOIN pre-aggregated, no LATERAL) ──
  cancel_events AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'CANCELAMENTO'::text AS event_type,
      'DW'::text AS source,
      c.id_db,
      c.id_comprovante,
      NULL::integer AS id_movprodutos,
      c.id_usuario,
      NULL::integer AS id_funcionario,
      c.id_turno,
      NULL::integer AS id_cliente,
      c.valor_total,
      COALESCE(c.valor_total, 0) AS impacto_estimado,
      LEAST(100, GREATEST(0,
        40
        + CASE WHEN p90.p90_valor IS NOT NULL AND c.valor_total > p90.p90_valor THEN 30
               WHEN p90.p90_valor IS NOT NULL AND c.valor_total > p90.p90_valor * 0.5 THEN 15
               ELSE 0 END
        + CASE WHEN us.user_cancel_rate > 0.3 THEN 30
               WHEN us.user_cancel_rate > 0.15 THEN 15
               ELSE 0 END
      ))::integer AS score_risco,
      jsonb_build_object(
        'p90_valor', p90.p90_valor,
        'user_cancel_rate', us.user_cancel_rate,
        'user_cancels', us.user_cancels,
        'user_total', us.user_total
      ) AS reasons
    FROM dw.fact_comprovante c
    LEFT JOIN p90_cancel p90
      ON p90.id_empresa = c.id_empresa
     AND p90.id_filial = c.id_filial
     AND p90.data_key = c.data_key
    LEFT JOIN user_stats us
      ON us.id_empresa = c.id_empresa
     AND us.id_filial = c.id_filial
     AND us.id_usuario = c.id_usuario
     AND us.data_key = c.data_key
    WHERE c.id_empresa = p_id_empresa
      AND c.cancelado = true
      AND c.data_key >= to_char(v_from_date, 'YYYYMMDD')::int
      AND c.data_key <= to_char(v_to_date, 'YYYYMMDD')::int
  ),

  -- ── Employee outlier events (high cancel rate) ──
  func_outlier_events AS (
    SELECT
      us.id_empresa,
      us.id_filial,
      us.data_key,
      NULL::timestamptz AS data,
      'FUNCIONARIO_OUTLIER'::text AS event_type,
      'DW'::text AS source,
      NULL::integer AS id_db,
      NULL::integer AS id_comprovante,
      NULL::integer AS id_movprodutos,
      us.id_usuario,
      NULL::integer AS id_funcionario,
      NULL::integer AS id_turno,
      NULL::integer AS id_cliente,
      NULL::numeric AS valor_total,
      0::numeric AS impacto_estimado,
      LEAST(100, GREATEST(0,
        CASE WHEN us.user_cancel_rate > 0.4 THEN 80
             WHEN us.user_cancel_rate > 0.25 THEN 60
             WHEN us.user_cancel_rate > 0.15 THEN 40
             ELSE 20 END
      ))::integer AS score_risco,
      jsonb_build_object(
        'user_cancel_rate', us.user_cancel_rate,
        'user_cancels', us.user_cancels,
        'user_total', us.user_total
      ) AS reasons
    FROM user_stats us
    WHERE us.user_cancel_rate > 0.10
      AND us.user_total >= 5
  ),

  -- ── Combine all events ──
  all_events AS (
    SELECT * FROM cancel_events
    UNION ALL
    SELECT * FROM func_outlier_events
  )

  INSERT INTO dw.fact_risco_evento (
    id_empresa, id_filial, data_key, data,
    event_type, source,
    id_db, id_comprovante, id_movprodutos,
    id_usuario, id_funcionario, id_turno, id_cliente,
    valor_total, impacto_estimado, score_risco, score_level, reasons
  )
  SELECT
    e.id_empresa, e.id_filial, e.data_key, e.data,
    e.event_type, e.source,
    e.id_db, e.id_comprovante, e.id_movprodutos,
    e.id_usuario, e.id_funcionario, e.id_turno, e.id_cliente,
    e.valor_total, e.impacto_estimado, e.score_risco,
    CASE
      WHEN e.score_risco >= 80 THEN 'ALTO'
      WHEN e.score_risco >= 60 THEN 'SUSPEITO'
      WHEN e.score_risco >= 40 THEN 'ATENCAO'
      ELSE 'NORMAL'
    END AS score_level,
    e.reasons
  FROM all_events e
  ON CONFLICT ON CONSTRAINT uq_fact_risco_evento_nk
  DO UPDATE SET
    data           = EXCLUDED.data,
    valor_total    = EXCLUDED.valor_total,
    impacto_estimado = EXCLUDED.impacto_estimado,
    score_risco    = EXCLUDED.score_risco,
    score_level    = EXCLUDED.score_level,
    reasons        = EXCLUDED.reasons;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 6: Snapshot Cache — Add staleness tracking
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'app' AND table_name = 'snapshot_cache') THEN
    ALTER TABLE app.snapshot_cache
      ADD COLUMN IF NOT EXISTS ttl_seconds integer NOT NULL DEFAULT 300;
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_snapshot_cache_updated
      ON app.snapshot_cache (updated_at DESC)';
  END IF;
END$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 7: Database Maintenance — ANALYZE updated tables for query planner
-- ─────────────────────────────────────────────────────────────────────────────

ANALYZE dw.fact_venda;
ANALYZE dw.fact_comprovante;
ANALYZE dw.fact_venda_item;
ANALYZE dw.fact_financeiro;
ANALYZE dw.fact_pagamento_comprovante;
ANALYZE dw.fact_risco_evento;

-- ============================================================================
-- END OF MIGRATION 059
-- ============================================================================
