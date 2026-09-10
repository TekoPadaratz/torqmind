-- ============================================================================
-- Migration 155: MFA attempt buckets (shared across API workers) + TOTP replay counter
-- ----------------------------------------------------------------------------
-- Replaces process-local MFA attempt maps with Postgres-backed buckets so
-- multi-worker / multi-replica deployments share the same throttle.
-- Adds totp_last_counter to reject replay of an already-accepted TOTP step.
-- Additive / idempotent. Does NOT set totp_required on existing users.
-- ============================================================================

BEGIN;

ALTER TABLE auth.users
  ADD COLUMN IF NOT EXISTS totp_last_counter bigint;

CREATE TABLE IF NOT EXISTS auth.security_attempt_buckets (
  bucket_key         text PRIMARY KEY,
  attempt_count      integer NOT NULL DEFAULT 0,
  window_started_at  timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

COMMIT;
