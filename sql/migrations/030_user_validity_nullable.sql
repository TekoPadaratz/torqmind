ALTER TABLE auth.users
  ALTER COLUMN valid_from DROP NOT NULL;

ALTER TABLE auth.user_tenants
  ALTER COLUMN valid_from DROP NOT NULL;
