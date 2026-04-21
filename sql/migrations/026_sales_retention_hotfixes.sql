BEGIN;

CREATE OR REPLACE FUNCTION etl.sales_business_ts(
  p_payload jsonb,
  p_fallback timestamp DEFAULT NULL
)
RETURNS timestamp AS $$
  SELECT COALESCE(
    etl.safe_timestamp(p_payload->>'TORQMIND_DT_EVENTO'),
    etl.safe_timestamp(p_payload->>'DT_EVENTO'),
    etl.safe_timestamp(p_payload->>'DATA'),
    etl.safe_timestamp(p_payload->>'DATAMOV'),
    etl.safe_timestamp(p_payload->>'DTMOV'),
    p_fallback
  );
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.sales_business_ts(
  p_payload jsonb,
  p_fallback timestamptz
)
RETURNS timestamp AS $$
  SELECT etl.sales_business_ts(p_payload, p_fallback::timestamp);
$$ LANGUAGE sql IMMUTABLE;

COMMIT;
