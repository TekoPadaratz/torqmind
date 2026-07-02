-- ============================================================
-- Migration 078: Add updated_at to competitor_price_items
-- Fix: UPDATE was overwriting created_at instead of a proper
--       updated_at column. This migration adds the missing column.
-- ============================================================

BEGIN;

ALTER TABLE app.competitor_price_items
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NULL;

COMMIT;
