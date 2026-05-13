-- ============================================================================
-- Migration 076: Competitor Pricing — Transactional Tables
-- ============================================================================
-- Creates the full competitor pricing schema in app.* for PostgreSQL.
-- Tables: competitor_stations, competitor_price_captures,
--         competitor_price_capture_items, competitor_price_capture_item_revisions
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Competitor Stations
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.competitor_stations (
    id                              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa                      integer NOT NULL,
    id_filial                       integer NOT NULL,
    station_name                    text NOT NULL,
    station_name_normalized         text NOT NULL,
    document_number                 text,
    address_text                    text,
    city                            text,
    state                           text,
    latitude                        numeric(10,7),
    longitude                       numeric(10,7),
    is_active                       boolean NOT NULL DEFAULT true,
    created_by_user_id              uuid NOT NULL REFERENCES auth.users(id),
    created_by_user_name_snapshot   text NOT NULL,
    created_at                      timestamptz NOT NULL DEFAULT now(),
    updated_by_user_id              uuid REFERENCES auth.users(id),
    updated_by_user_name_snapshot   text,
    updated_at                      timestamptz,
    deleted_at                      timestamptz
);

CREATE INDEX IF NOT EXISTS ix_competitor_stations_tenant_branch
    ON app.competitor_stations (id_empresa, id_filial, is_active);

CREATE INDEX IF NOT EXISTS ix_competitor_stations_name_norm
    ON app.competitor_stations (id_empresa, id_filial, station_name_normalized);

CREATE UNIQUE INDEX IF NOT EXISTS uq_competitor_stations_name_active
    ON app.competitor_stations (id_empresa, id_filial, station_name_normalized)
    WHERE deleted_at IS NULL;

-- --------------------------------------------------------------------------
-- 2. Competitor Price Captures (header per station+date)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.competitor_price_captures (
    id                                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa                          integer NOT NULL,
    id_filial                           integer NOT NULL,
    station_id                          uuid NOT NULL REFERENCES app.competitor_stations(id),
    capture_date                        date NOT NULL,
    captured_at                         timestamptz NOT NULL DEFAULT now(),
    status                              text NOT NULL DEFAULT 'CONFIRMED'
                                            CHECK (status IN ('CONFIRMED','DELETED')),
    registered_by_user_id               uuid NOT NULL REFERENCES auth.users(id),
    registered_by_user_name_snapshot    text NOT NULL,
    registered_at                       timestamptz NOT NULL DEFAULT now(),
    last_updated_by_user_id             uuid REFERENCES auth.users(id),
    last_updated_by_user_name_snapshot  text,
    last_updated_at                     timestamptz,
    observation                         text,
    source                              text NOT NULL DEFAULT 'WEB',
    client_ip                           text,
    user_agent                          text,
    geo_latitude                        numeric(10,7),
    geo_longitude                       numeric(10,7),
    geo_accuracy_meters                 numeric(8,2),
    created_at                          timestamptz NOT NULL DEFAULT now(),
    updated_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_competitor_captures_tenant_date
    ON app.competitor_price_captures (id_empresa, id_filial, capture_date);

CREATE INDEX IF NOT EXISTS ix_competitor_captures_station_date
    ON app.competitor_price_captures (id_empresa, id_filial, station_id, capture_date);

CREATE INDEX IF NOT EXISTS ix_competitor_captures_user
    ON app.competitor_price_captures (registered_by_user_id, registered_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_competitor_captures_station_date
    ON app.competitor_price_captures (id_empresa, id_filial, station_id, capture_date)
    WHERE status <> 'DELETED';

-- --------------------------------------------------------------------------
-- 3. Competitor Price Capture Items (price per product per capture)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.competitor_price_capture_items (
    id                                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa                              integer NOT NULL,
    id_filial                               integer NOT NULL,
    capture_id                              uuid NOT NULL REFERENCES app.competitor_price_captures(id),
    station_id                              uuid NOT NULL REFERENCES app.competitor_stations(id),
    capture_date                            date NOT NULL,

    id_produto                              integer NOT NULL,
    product_code_origin                     text,
    product_name_snapshot                    text NOT NULL,
    fuel_type_snapshot                       text,

    original_price                          numeric(12,4) NOT NULL CHECK (original_price > 0),
    current_price                           numeric(12,4) NOT NULL CHECK (current_price > 0),

    original_registered_by_user_id          uuid NOT NULL REFERENCES auth.users(id),
    original_registered_by_user_name_snapshot text NOT NULL,
    original_registered_at                  timestamptz NOT NULL,

    last_updated_by_user_id                 uuid REFERENCES auth.users(id),
    last_updated_by_user_name_snapshot      text,
    last_updated_at                         timestamptz,

    revision_number                         integer NOT NULL DEFAULT 1,
    is_active                               boolean NOT NULL DEFAULT true,
    created_at                              timestamptz NOT NULL DEFAULT now(),
    updated_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_capture_items_product
    ON app.competitor_price_capture_items (capture_id, id_produto);

CREATE INDEX IF NOT EXISTS ix_capture_items_tenant_date
    ON app.competitor_price_capture_items (id_empresa, id_filial, capture_date);

CREATE INDEX IF NOT EXISTS ix_capture_items_station_date
    ON app.competitor_price_capture_items (id_empresa, id_filial, station_id, capture_date);

CREATE INDEX IF NOT EXISTS ix_capture_items_product_date
    ON app.competitor_price_capture_items (id_empresa, id_filial, id_produto, capture_date);

CREATE INDEX IF NOT EXISTS ix_capture_items_capture
    ON app.competitor_price_capture_items (capture_id);

-- --------------------------------------------------------------------------
-- 4. Competitor Price Capture Item Revisions (audit trail)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.competitor_price_capture_item_revisions (
    id                              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa                      integer NOT NULL,
    id_filial                       integer NOT NULL,
    capture_id                      uuid NOT NULL REFERENCES app.competitor_price_captures(id),
    capture_item_id                 uuid NOT NULL REFERENCES app.competitor_price_capture_items(id),
    station_id                      uuid NOT NULL REFERENCES app.competitor_stations(id),
    capture_date                    date NOT NULL,

    id_produto                      integer NOT NULL,
    product_code_origin             text,
    product_name_snapshot           text NOT NULL,
    fuel_type_snapshot              text,

    revision_number                 integer NOT NULL,
    action_type                     text NOT NULL
                                        CHECK (action_type IN ('CREATE','UPDATE_PRICE','DEACTIVATE','REACTIVATE')),

    old_price                       numeric(12,4),
    new_price                       numeric(12,4) NOT NULL,

    changed_by_user_id              uuid NOT NULL REFERENCES auth.users(id),
    changed_by_user_name_snapshot   text NOT NULL,
    changed_at                      timestamptz NOT NULL DEFAULT now(),

    change_reason                   text,
    client_ip                       text,
    user_agent                      text,
    request_id                      text
);

CREATE INDEX IF NOT EXISTS ix_capture_revisions_item
    ON app.competitor_price_capture_item_revisions (capture_item_id, revision_number);

CREATE INDEX IF NOT EXISTS ix_capture_revisions_tenant_date
    ON app.competitor_price_capture_item_revisions (id_empresa, id_filial, capture_date);

CREATE INDEX IF NOT EXISTS ix_capture_revisions_station_product
    ON app.competitor_price_capture_item_revisions (station_id, id_produto, capture_date);

CREATE INDEX IF NOT EXISTS ix_capture_revisions_user
    ON app.competitor_price_capture_item_revisions (changed_by_user_id, changed_at);

-- --------------------------------------------------------------------------
-- 5. Competitor Price Capture Evidence (future-proof)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.competitor_price_capture_evidence (
    id                              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_empresa                      integer NOT NULL,
    id_filial                       integer NOT NULL,
    capture_id                      uuid NOT NULL REFERENCES app.competitor_price_captures(id),
    evidence_type                   text NOT NULL
                                        CHECK (evidence_type IN ('PHOTO','LOCATION','NOTE')),
    file_url                        text,
    note                            text,
    geo_latitude                    numeric(10,7),
    geo_longitude                   numeric(10,7),
    geo_accuracy_meters             numeric(8,2),
    created_by_user_id              uuid NOT NULL REFERENCES auth.users(id),
    created_by_user_name_snapshot   text NOT NULL,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_capture_evidence_capture
    ON app.competitor_price_capture_evidence (capture_id);

-- --------------------------------------------------------------------------
-- 6. RLS Policies (follow existing project pattern)
-- --------------------------------------------------------------------------
ALTER TABLE app.competitor_stations ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.competitor_price_captures ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.competitor_price_capture_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.competitor_price_capture_item_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.competitor_price_capture_evidence ENABLE ROW LEVEL SECURITY;

-- Migration tracking handled by the migration runner.
