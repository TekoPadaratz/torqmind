CREATE SCHEMA IF NOT EXISTS auth;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS auth.users (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email             text UNIQUE NOT NULL,
  password_hash     text NOT NULL,
  is_active         boolean NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auth.user_tenants (
  user_id           uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  role              text NOT NULL CHECK (role IN ('MASTER','OWNER','MANAGER')),
  id_empresa        integer NULL,
  id_filial         integer NULL,
  id_empresa_pk     integer GENERATED ALWAYS AS (COALESCE(id_empresa, -1)) STORED,
  id_filial_pk      integer GENERATED ALWAYS AS (COALESCE(id_filial, -1)) STORED,
  PRIMARY KEY (user_id, role, id_empresa_pk, id_filial_pk),
  CONSTRAINT ck_auth_user_tenants_role_scope CHECK (
    (role = 'MASTER'  AND id_empresa IS NULL AND id_filial IS NULL) OR
    (role = 'OWNER'   AND id_empresa IS NOT NULL AND id_filial IS NULL) OR
    (role = 'MANAGER' AND id_empresa IS NOT NULL AND id_filial IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS auth.password_reset_tokens (
  token             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id           uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  expires_at        timestamptz NOT NULL,
  used_at           timestamptz NULL,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auth.audit_log (
  id                bigserial PRIMARY KEY,
  ts                timestamptz NOT NULL DEFAULT now(),
  actor_user_id     uuid NULL,
  action            text NOT NULL,
  meta              jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS auth.filiais (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  is_active         boolean NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial)
);

CREATE OR REPLACE FUNCTION auth.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_auth_filiais_updated_at ON auth.filiais;
CREATE TRIGGER trg_auth_filiais_updated_at
BEFORE UPDATE ON auth.filiais
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();
