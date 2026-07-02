-- Migration 087: ETL statement_timeout hardening (defense-in-depth)
--
-- Root cause (contas a receber / dashboards travados):
--   The batch ETL session opens via app.db.get_conn(), which applies a 55s
--   interactive statement_timeout (apps/api/app/db.py) meant for API requests.
--   After a bulk / full-snapshot reingestion (e.g. 600k rows landing in
--   stg.contasreceber or stg.comprovantes with the same ingested_at), the heavy
--   single-statement fact loaders run longer than 55s and are cancelled. The
--   orchestrator rolls back the whole step, so the watermark never advances ->
--   permanent failure loop and dw facts + marts stop updating.
--
-- Primary fix lives in code: etl_orchestrator raises statement_timeout for the
-- ETL session (settings.etl_statement_timeout_seconds, default 600s). This
-- migration adds a defense-in-depth, image-independent guarantee by attaching a
-- generous statement_timeout to the heavy ETL functions themselves. A
-- function-level SET overrides the session value only while the function runs
-- and is automatically restored afterwards, so the protective 55s session cap
-- still applies to every API query.
--
-- Idempotent: ALTER FUNCTION simply (re)sets the attribute. Safe to re-run.

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT n.nspname AS schema_name,
           p.proname AS func_name,
           pg_get_function_identity_arguments(p.oid) AS args
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'etl'
      AND (
        p.proname LIKE 'load_fact_%'
        OR p.proname LIKE 'backfill_%'
        OR p.proname IN (
          'refresh_customer_delinquency_summary',
          'refresh_customer_screen_summary',
          'refresh_anonymous_retention',
          'refresh_marts'
        )
      )
  LOOP
    EXECUTE format(
      'ALTER FUNCTION %I.%I(%s) SET statement_timeout = %L',
      r.schema_name, r.func_name, r.args, '600s'
    );
  END LOOP;
END
$$;
