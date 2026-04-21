BEGIN;

CREATE OR REPLACE FUNCTION etl.date_key(p_ts timestamptz)
RETURNS integer AS $$
  SELECT etl.date_key(p_ts::timestamp);
$$ LANGUAGE sql IMMUTABLE;

COMMIT;
