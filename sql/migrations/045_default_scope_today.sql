BEGIN;

UPDATE app.tenants
SET
  default_product_scope_days = 1,
  updated_at = now()
WHERE COALESCE(default_product_scope_days, 0) IN (0, 30);

ALTER TABLE app.tenants
  ALTER COLUMN default_product_scope_days SET DEFAULT 1;

CREATE OR REPLACE FUNCTION etl.default_product_scope_days(p_id_empresa integer)
RETURNS integer AS $$
  SELECT GREATEST(
    1,
    COALESCE(
      (SELECT default_product_scope_days FROM app.tenants WHERE id_empresa = p_id_empresa),
      1
    )
  );
$$ LANGUAGE sql STABLE;

COMMIT;
