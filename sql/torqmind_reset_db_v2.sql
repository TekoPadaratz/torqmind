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
