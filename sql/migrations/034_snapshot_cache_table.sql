-- ==========================================
-- Snapshot cache for BI endpoints
-- ==========================================

BEGIN;

CREATE TABLE IF NOT EXISTS app.snapshot_cache (
  snapshot_key text NOT NULL,
  id_empresa integer NOT NULL,
  id_filial integer NULL,
  scope_signature text NOT NULL,
  scope_context jsonb NOT NULL,
  snapshot_data jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (snapshot_key, id_empresa, scope_signature)
);

CREATE INDEX IF NOT EXISTS ix_snapshot_cache_lookup
  ON app.snapshot_cache (snapshot_key, id_empresa, id_filial, updated_at);

COMMIT;
