BEGIN;

DO $$
DECLARE
  r record;
  idx_base text;
BEGIN
  FOR r IN
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'stg' AND table_type = 'BASE TABLE'
  LOOP
    EXECUTE format('ALTER TABLE stg.%I ADD COLUMN IF NOT EXISTS dt_evento timestamptz', r.table_name);
    EXECUTE format('ALTER TABLE stg.%I ADD COLUMN IF NOT EXISTS id_db_shadow bigint', r.table_name);
    EXECUTE format('ALTER TABLE stg.%I ADD COLUMN IF NOT EXISTS id_chave_natural text', r.table_name);
    EXECUTE format('ALTER TABLE stg.%I ADD COLUMN IF NOT EXISTS received_at timestamptz NOT NULL DEFAULT now()', r.table_name);

    idx_base := replace(r.table_name, '.', '_');
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS ix_stg_%s_emp_filial_evento ON stg.%I (id_empresa, id_filial, dt_evento)',
      idx_base, r.table_name
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS ix_stg_%s_emp_evento ON stg.%I (id_empresa, dt_evento)',
      idx_base, r.table_name
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS ix_stg_%s_emp_iddbshadow ON stg.%I (id_empresa, id_db_shadow)',
      idx_base, r.table_name
    );
  END LOOP;
END $$;

COMMIT;
