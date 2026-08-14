-- GitHub Actions / ephemeral Postgres only.
-- stg.estoque exists in production but was never created by sql/migrations.
-- 101_estoque_ingest_align.sql ALTERs it. Applied by migrate bootstrap when
-- APP_ENV=test and TM_EPHEMERAL_LOCAL=1, after 003_mart_demo.sql.
CREATE TABLE IF NOT EXISTS stg.estoque (
  id_empresa  integer NOT NULL,
  id_filial   integer NOT NULL,
  id_estoque  integer NOT NULL,
  id_produto  integer,
  quantidade  numeric,
  custo_medio numeric,
  payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_estoque)
);
CREATE INDEX IF NOT EXISTS ix_stg_estoque_ing ON stg.estoque (id_empresa, ingested_at);
