-- ============================================================================
-- Migration 090: Cash-open notifications refresh their own marts (realtime fix)
-- ----------------------------------------------------------------------------
-- Root cause:
--   The Telegram "caixa aberto > 24h" alert reads mart.alerta_caixa_aberto,
--   which derives from mart.agg_caixa_turno_aberto. Those PostgreSQL
--   materialized views were ONLY refreshed by etl.refresh_marts (legacy
--   publication path). After the ClickHouse-first cutover the legacy path is
--   gated off (_legacy_pg_marts_enabled() = false), so the operational
--   fast-path never refreshed these marts. They went stale/empty and
--   etl.sync_cash_open_notifications() read no rows -> no notification ->
--   no Telegram dispatch, even though dw.fact_caixa_turno was alive and
--   caixas were genuinely open past 24h.
--
-- Fix:
--   Make etl.sync_cash_open_notifications() refresh the two cash-open marts it
--   depends on (agg_caixa_turno_aberto -> alerta_caixa_aberto) before reading.
--   The operational fast-path already calls this function every cycle while any
--   caixa is open (cash_changed OR clock_cash_notifications), so the alert now
--   stays time-accurate "mesmo sem ingestao nova". Refresh is cheap (source is
--   dw.fact_caixa_turno filtered to is_aberto) and the is_operational_live
--   (<=96h activity) filter in mart.alerta_caixa_aberto keeps stale noise out.
--
-- Idempotent: pure CREATE OR REPLACE FUNCTION; safe to re-run.
-- ============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION etl.sync_cash_open_notifications(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  -- Keep the cash-open marts time-accurate in the ClickHouse-first operational
  -- fast-path, where etl.refresh_marts (legacy publication) does not run.
  -- alerta_caixa_aberto depends on agg_caixa_turno_aberto, so refresh in order.
  REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
  REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;

  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      insight_id_hash AS insight_id,
      severity,
      title,
      body,
      url
    FROM mart.alerta_caixa_aberto
    WHERE id_empresa = p_id_empresa
      AND insight_id_hash IS NOT NULL
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
  SELECT COUNT(*) INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
