-- Phase 8: competitor pricing manual input + simulation support
-- Idempotent migration

CREATE TABLE IF NOT EXISTS app.competitor_fuel_prices (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_produto        integer NOT NULL,
  competitor_price  numeric(18,4) NOT NULL CHECK (competitor_price > 0),
  updated_by        text NULL,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_produto)
);

CREATE INDEX IF NOT EXISTS ix_app_competitor_fuel_prices_lookup
  ON app.competitor_fuel_prices (id_empresa, id_filial, updated_at DESC);
