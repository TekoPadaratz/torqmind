CREATE SCHEMA IF NOT EXISTS app;

CREATE OR REPLACE FUNCTION app.current_role()
RETURNS text AS $$
  SELECT current_setting('app.role', true);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION app.current_tenant()
RETURNS integer AS $$
  SELECT NULLIF(current_setting('app.tenant_id', true), '')::integer;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION app.current_branch()
RETURNS integer AS $$
  SELECT NULLIF(current_setting('app.branch_id', true), '')::integer;
$$ LANGUAGE sql STABLE;
