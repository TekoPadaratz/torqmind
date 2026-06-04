-- ============================================================================
-- Migration 091: Two-factor authentication (TOTP) — additive, opt-in
-- ----------------------------------------------------------------------------
-- Adds TOTP/2FA columns to auth.users and a one-time recovery-codes table.
-- Everything is nullable / defaulted so existing logins are unaffected; 2FA is
-- strictly opt-in (totp_enabled defaults false). The TOTP secret is stored
-- ENCRYPTED (Fernet) — never in plaintext.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE TABLE/INDEX IF NOT EXISTS.
-- ============================================================================

BEGIN;

ALTER TABLE auth.users
  ADD COLUMN IF NOT EXISTS totp_enabled        boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS totp_secret_encrypted text,
  ADD COLUMN IF NOT EXISTS totp_confirmed_at   timestamptz,
  ADD COLUMN IF NOT EXISTS totp_required       boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS totp_last_used_at   timestamptz,
  ADD COLUMN IF NOT EXISTS mfa_reset_required  boolean NOT NULL DEFAULT false;

-- One-time recovery codes (hash only; plaintext shown once at generation).
CREATE TABLE IF NOT EXISTS auth.user_recovery_codes (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  code_hash   text NOT NULL,
  used_at     timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_user_recovery_codes_user
  ON auth.user_recovery_codes (user_id) WHERE used_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_recovery_code_hash
  ON auth.user_recovery_codes (user_id, code_hash);

COMMIT;
