-- Migration 129: SMTP configurável na plataforma (admin).
-- Senha fica criptografada (Fernet / TOTP_ENCRYPTION_KEY); nunca retornada na API.
-- Env SMTP_* continua como bootstrap/fallback até o admin salvar host no perfil.

ALTER TABLE app.platform_email_profile
  ADD COLUMN IF NOT EXISTS smtp_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS smtp_host text,
  ADD COLUMN IF NOT EXISTS smtp_port integer NOT NULL DEFAULT 587,
  ADD COLUMN IF NOT EXISTS smtp_user text,
  ADD COLUMN IF NOT EXISTS smtp_password_encrypted text,
  ADD COLUMN IF NOT EXISTS smtp_use_ssl boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS smtp_use_tls boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS smtp_from_name text NOT NULL DEFAULT 'TorqMind',
  ADD COLUMN IF NOT EXISTS smtp_timeout_seconds integer NOT NULL DEFAULT 20;

COMMENT ON COLUMN app.platform_email_profile.smtp_password_encrypted IS
  'Senha SMTP criptografada (Fernet). Nunca expor em API/logs/audit.';

COMMENT ON TABLE app.platform_email_profile IS
  'Perfil + SMTP administrativo TorqMind. Senha só criptografada; env SMTP_* é fallback.';
