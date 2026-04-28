-- TorqMind - Full Reset (DEV/HOMOLOG ONLY)
-- Rebuilds the database by replaying the official migration chain.
--
-- Usage:
--   psql -v ON_ERROR_STOP=1 -U postgres -d TORQMIND -f sql/torqmind_reset_db_v2.sql
--
-- Notes:
-- - This script is destructive and must never be used in production.
-- - After reset, run the application seed to bootstrap master/tenant users:
--     docker compose exec api python -m app.cli.seed

\set ON_ERROR_STOP on

DO $reset_guard$
DECLARE
  v_allow_reset text := :'TM_ALLOW_RESET';
  v_reset_env text := lower(COALESCE(:'TM_RESET_ENV', ''));
BEGIN
  IF v_allow_reset <> '1' THEN
    RAISE EXCEPTION 'Hard reset refused: TM_ALLOW_RESET=1 is required.';
  END IF;
  IF v_reset_env NOT IN ('dev', 'homolog') THEN
    RAISE EXCEPTION 'Hard reset refused: TM_RESET_ENV must be dev or homolog.';
  END IF;
END;
$reset_guard$;

DROP SCHEMA IF EXISTS mart CASCADE;
DROP SCHEMA IF EXISTS dw CASCADE;
DROP SCHEMA IF EXISTS stg CASCADE;
DROP SCHEMA IF EXISTS etl CASCADE;
DROP SCHEMA IF EXISTS billing CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;
DROP SCHEMA IF EXISTS app CASCADE;
DROP SCHEMA IF EXISTS auth CASCADE;

\ir migrations/001_auth.sql
\ir migrations/002_app_rls.sql
\ir migrations/003_mart_demo.sql
\ir migrations/004_risk_insights.sql
\ir migrations/005_etl_incremental_scalable.sql
\ir migrations/006_ingest_shadow_columns.sql
\ir migrations/007_etl_incremental_hot_received.sql
\ir migrations/008_run_all_skip_risk_when_no_changes.sql
\ir migrations/009_phase4_moneyleak_health.sql
\ir migrations/010_phase5_ai_engine.sql
\ir migrations/011_phase7_notifications.sql
\ir migrations/012_phase1_telegram_critical.sql
\ir migrations/013_phase3_anonymous_retention.sql
\ir migrations/014_phase8_competitor_pricing.sql
\ir migrations/015_payments_by_comprovante.sql
\ir migrations/016_historical_risk_backfill.sql
\ir migrations/017_fix_payment_values.sql
\ir migrations/018_cash_module.sql
\ir migrations/019_operational_truth_alignment.sql
\ir migrations/020_snapshot_backfill_and_perf.sql
\ir migrations/021_platform_backoffice.sql
\ir migrations/022_etl_incremental_notification_hardening.sql
\ir migrations/023_etl_cycle_orchestration_and_branch_admin.sql
\ir migrations/024_etl_clock_driven_rollover.sql
\ir migrations/025_sales_retention_scope_defaults.sql
\ir migrations/026_sales_retention_hotfixes.sql
\ir migrations/027_sales_retention_date_key_hotfix.sql
\ir migrations/028_prod_ready_hotpaths.sql
\ir migrations/029_product_global_role_constraints.sql
\ir migrations/030_user_validity_nullable.sql
\ir migrations/031_risk_events_window_perf.sql
\ir migrations/032_etl_track_domains.sql
\ir migrations/033_cash_fraud_operational_truth.sql
\ir migrations/034_snapshot_cache_table.sql
\ir migrations/035_cash_operational_truth_schema_alignment.sql
\ir migrations/036_operational_publication_overlay_indexes.sql
\ir migrations/037_risk_publication_refresh.sql
\ir migrations/038_auth_usernames.sql
\ir migrations/039_fact_venda_cancel_sync.sql
\ir migrations/040_business_date_semantics_fix.sql
\ir migrations/041_financial_semantics_operational_dashboards.sql
\ir migrations/042_operational_incremental_fastpath.sql
\ir migrations/043_operational_incremental_semantics_fix.sql
\ir migrations/044_payment_comprovante_hotpath_refactor.sql
\ir migrations/045_default_scope_today.sql
\ir migrations/046_risk_events_delta_fine.sql
\ir migrations/047_sales_status_semantics_fix.sql
\ir migrations/048_active_product_status_dimension.sql
\ir migrations/049_canonical_comprovante_sales_backbone.sql
\ir migrations/050_canonical_etl_hotpath_perf_fix.sql
\ir migrations/051_canonical_sales_dw_decoupling.sql
\ir migrations/052_canonical_sales_bulk_chunking.sql
\ir migrations/053_canonical_sales_bulk_maintenance_indexes.sql
\ir migrations/054_canonical_sales_marts_cleanup.sql
\ir migrations/055_canonical_stock_pipeline.sql
\ir migrations/056_canonical_sales_item_total_repair.sql
\ir migrations/057_canonical_item_total_shadow_trigger.sql
\ir migrations/058_hot_route_snapshot_and_finance_indexes.sql
\ir migrations/059_canonical_sales_group_backfill.sql
\ir migrations/059_performance_indexes_and_etl_fixes.sql
\ir migrations/060_enable_rls_tenant_isolation.sql
\ir migrations/061_performance_indexes_and_etl_fixes.sql
\ir migrations/062_fix_risk_engine.sql
\ir migrations/063_clickhouse_dw_replica_identity.sql
\ir migrations/064_core_identity_constraints.sql
\ir migrations/065_upsert_target_constraints.sql
\ir migrations/066_operational_upsert_constraints.sql
\ir migrations/067_dw_canonical_sales_trigger_repair.sql
\ir migrations/068_operational_compatibility_repair.sql
\ir migrations/069_remaining_operational_indexes.sql
\ir migrations/070_payment_notification_hash_repair.sql
\ir migrations/071_payment_notification_hash_schema_compat.sql
