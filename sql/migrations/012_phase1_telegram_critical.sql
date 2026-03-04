-- Phase 1: Telegram critical alerts (owner channel per company)

CREATE TABLE IF NOT EXISTS app.telegram_settings (
  id_empresa integer PRIMARY KEY,
  chat_id text,
  is_enabled boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.telegram_dispatch_log (
  id bigserial PRIMARY KEY,
  id_empresa integer NOT NULL,
  id_filial integer,
  event_type text NOT NULL,
  event_date date NOT NULL,
  insight_id bigint,
  dedupe_hash text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  sent_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_telegram_dispatch_dedupe
  ON app.telegram_dispatch_log (id_empresa, dedupe_hash);

CREATE INDEX IF NOT EXISTS ix_telegram_dispatch_lookup
  ON app.telegram_dispatch_log (id_empresa, event_date DESC, id_filial);

CREATE OR REPLACE FUNCTION app.trg_set_updated_at_telegram_settings()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_updated_at_telegram_settings ON app.telegram_settings;
CREATE TRIGGER trg_set_updated_at_telegram_settings
BEFORE UPDATE ON app.telegram_settings
FOR EACH ROW
EXECUTE FUNCTION app.trg_set_updated_at_telegram_settings();
