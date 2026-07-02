-- ============================================================
-- Migration 077: Rebuild competitor pricing schema (clean)
-- Drops empty 076 tables and recreates with simplified schema.
-- Tables from 076 had 0 rows in production.
-- Legacy app.competitor_fuel_prices (014) is preserved.
-- ============================================================

BEGIN;

-- Drop 076 tables only if they have 0 rows (safety guard)
-- Uses to_regclass to avoid error when table does not exist.
DO $$
DECLARE
  _tbl TEXT;
  _cnt BIGINT;
BEGIN
  FOR _tbl IN
    SELECT unnest(ARRAY[
      'app.competitor_price_capture_evidence',
      'app.competitor_price_capture_item_revisions',
      'app.competitor_price_capture_items',
      'app.competitor_price_captures',
      'app.competitor_stations'
    ])
  LOOP
    IF to_regclass(_tbl) IS NOT NULL THEN
      EXECUTE format('SELECT count(*) FROM %s', _tbl) INTO _cnt;
      IF _cnt > 0 THEN
        RAISE EXCEPTION 'Table % has % rows - refusing to DROP. Aborting migration 077.', _tbl, _cnt;
      END IF;
    END IF;
  END LOOP;
END
$$;

DROP TABLE IF EXISTS app.competitor_price_capture_evidence CASCADE;
DROP TABLE IF EXISTS app.competitor_price_capture_item_revisions CASCADE;
DROP TABLE IF EXISTS app.competitor_price_capture_items CASCADE;
DROP TABLE IF EXISTS app.competitor_price_captures CASCADE;
DROP TABLE IF EXISTS app.competitor_stations CASCADE;

-- ============================================================
-- 1. competitor_stations
-- ============================================================
CREATE TABLE app.competitor_stations (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa              INT NOT NULL,
    id_filial               INT NOT NULL,
    station_name            TEXT NOT NULL,
    station_name_normalized TEXT NOT NULL,
    active                  BOOLEAN NOT NULL DEFAULT true,
    created_by_user_id      TEXT NULL,
    created_by_user_name    TEXT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by_user_id      TEXT NULL,
    updated_by_user_name    TEXT NULL,
    updated_at              TIMESTAMPTZ NULL,
    deleted_at              TIMESTAMPTZ NULL
);

CREATE INDEX idx_cs_empresa_filial_active
    ON app.competitor_stations (id_empresa, id_filial, active);

CREATE INDEX idx_cs_empresa_filial_normalized
    ON app.competitor_stations (id_empresa, id_filial, station_name_normalized);

CREATE UNIQUE INDEX uq_cs_empresa_filial_normalized_alive
    ON app.competitor_stations (id_empresa, id_filial, station_name_normalized)
    WHERE deleted_at IS NULL;

-- ============================================================
-- 2. competitor_price_captures
-- ============================================================
CREATE TABLE app.competitor_price_captures (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa              INT NOT NULL,
    id_filial               INT NOT NULL,
    station_id              UUID NOT NULL REFERENCES app.competitor_stations(id),
    station_name_snapshot   TEXT NOT NULL,
    station_name_normalized TEXT NOT NULL,
    capture_date            DATE NOT NULL,
    captured_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    status                  TEXT NOT NULL DEFAULT 'CONFIRMED',
    registered_by_user_id   TEXT NULL,
    registered_by_user_name TEXT NULL,
    registered_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    observation             TEXT NULL,
    client_ip               TEXT NULL,
    user_agent              TEXT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_cpc_empresa_filial_date
    ON app.competitor_price_captures (id_empresa, id_filial, capture_date);

CREATE INDEX idx_cpc_empresa_filial_station_date
    ON app.competitor_price_captures (id_empresa, id_filial, station_id, capture_date);

CREATE INDEX idx_cpc_registered_by
    ON app.competitor_price_captures (registered_by_user_id, registered_at);

-- ============================================================
-- 3. competitor_price_items
-- ============================================================
CREATE TABLE app.competitor_price_items (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa              INT NOT NULL,
    id_filial               INT NOT NULL,
    capture_id              UUID NOT NULL REFERENCES app.competitor_price_captures(id),
    station_id              UUID NOT NULL REFERENCES app.competitor_stations(id),
    station_name_snapshot   TEXT NOT NULL,
    capture_date            DATE NOT NULL,
    product_id              INT NOT NULL,
    product_code_origin     TEXT NULL,
    product_name_snapshot   TEXT NOT NULL,
    fuel_type_snapshot      TEXT NULL,
    price                   NUMERIC(12,4) NOT NULL CHECK (price > 0),
    is_valid                BOOLEAN NOT NULL DEFAULT true,
    created_by_user_id      TEXT NULL,
    created_by_user_name    TEXT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_cpi_empresa_filial_date
    ON app.competitor_price_items (id_empresa, id_filial, capture_date);

CREATE INDEX idx_cpi_empresa_filial_station_date
    ON app.competitor_price_items (id_empresa, id_filial, station_id, capture_date);

CREATE INDEX idx_cpi_empresa_filial_product_date
    ON app.competitor_price_items (id_empresa, id_filial, product_id, capture_date);

CREATE INDEX idx_cpi_capture_id
    ON app.competitor_price_items (capture_id);

-- ============================================================
-- 4. competitor_price_item_revisions
-- ============================================================
CREATE TABLE app.competitor_price_item_revisions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa              INT NOT NULL,
    id_filial               INT NOT NULL,
    capture_id              UUID NOT NULL REFERENCES app.competitor_price_captures(id),
    item_id                 UUID NULL REFERENCES app.competitor_price_items(id),
    station_id              UUID NOT NULL REFERENCES app.competitor_stations(id),
    station_name_snapshot   TEXT NOT NULL,
    capture_date            DATE NOT NULL,
    product_id              INT NOT NULL,
    product_name_snapshot   TEXT NOT NULL,
    revision_number         INT NOT NULL,
    action_type             TEXT NOT NULL CHECK (action_type IN ('CREATE', 'UPDATE_PRICE', 'INVALIDATE')),
    old_price               NUMERIC(12,4) NULL,
    new_price               NUMERIC(12,4) NOT NULL,
    changed_by_user_id      TEXT NULL,
    changed_by_user_name    TEXT NULL,
    changed_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    change_reason           TEXT NULL,
    client_ip               TEXT NULL,
    user_agent              TEXT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_cpir_empresa_filial_date
    ON app.competitor_price_item_revisions (id_empresa, id_filial, capture_date);

CREATE INDEX idx_cpir_station_product_date
    ON app.competitor_price_item_revisions (station_id, product_id, capture_date);

CREATE INDEX idx_cpir_item_revision
    ON app.competitor_price_item_revisions (item_id, revision_number);

CREATE INDEX idx_cpir_changed_by
    ON app.competitor_price_item_revisions (changed_by_user_id, changed_at);

COMMIT;
