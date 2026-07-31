-- Migration 131: shadow columns em stg.afericoes / stg.bicos
-- A 130 criou as tabelas sem id_db_shadow/id_chave_natural; o ingest genérico
-- (_batch_columns) sempre grava esses campos → HTTP 500 e spool do agent trava.

ALTER TABLE stg.afericoes
  ADD COLUMN IF NOT EXISTS id_db_shadow bigint,
  ADD COLUMN IF NOT EXISTS id_chave_natural text;

ALTER TABLE stg.bicos
  ADD COLUMN IF NOT EXISTS id_db_shadow bigint,
  ADD COLUMN IF NOT EXISTS id_chave_natural text;

CREATE INDEX IF NOT EXISTS ix_stg_afericoes_emp_iddbshadow
  ON stg.afericoes (id_empresa, id_db_shadow);

CREATE INDEX IF NOT EXISTS ix_stg_bicos_emp_iddbshadow
  ON stg.bicos (id_empresa, id_db_shadow);
