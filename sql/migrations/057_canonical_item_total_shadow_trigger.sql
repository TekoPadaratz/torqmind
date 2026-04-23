BEGIN;

CREATE OR REPLACE FUNCTION etl.normalize_itenscomprovantes_total_shadow()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.total_shadow := etl.resolve_item_total(NEW.total_shadow, NEW.payload);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_itenscomprovantes_total_shadow_normalize ON stg.itenscomprovantes;

CREATE TRIGGER trg_itenscomprovantes_total_shadow_normalize
BEFORE INSERT OR UPDATE OF total_shadow, payload
ON stg.itenscomprovantes
FOR EACH ROW
EXECUTE FUNCTION etl.normalize_itenscomprovantes_total_shadow();

COMMIT;
