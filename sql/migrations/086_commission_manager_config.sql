-- Migration 086: Manager commission configuration
-- Adds manager commission mode and fixed percent options to app.commission_config

BEGIN;

ALTER TABLE app.commission_config
  ADD COLUMN IF NOT EXISTS manager_commission_mode text NOT NULL DEFAULT 'use_tiers'
    CHECK (manager_commission_mode IN ('use_tiers', 'fixed_percent'));

ALTER TABLE app.commission_config
  ADD COLUMN IF NOT EXISTS manager_commission_percent numeric(5,2) NOT NULL DEFAULT 0
    CHECK (manager_commission_percent >= 0 AND manager_commission_percent <= 100);

COMMIT;
