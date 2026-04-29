-- ============================================================================
-- Migration 064: Core identity constraints repair
-- ============================================================================
-- PT-BR:
-- Repara bancos criados antes da cadeia atual em que tabelas app/auth já
-- existiam sem PK/unique constraints. Esses constraints são necessários para
-- ON CONFLICT, seeds, testes e integridade multi-tenant.
-- ============================================================================

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'app.tenants'::regclass
      AND contype = 'p'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM app.tenants
      GROUP BY id_empresa
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add primary key app.tenants(id_empresa): duplicate rows exist';
    END IF;

    ALTER TABLE app.tenants
      ADD CONSTRAINT tenants_pkey PRIMARY KEY (id_empresa);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'app.tenants'::regclass
      AND conname = 'tenants_ingest_key_key'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM app.tenants
      GROUP BY ingest_key
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add unique constraint app.tenants(ingest_key): duplicate rows exist';
    END IF;

    ALTER TABLE app.tenants
      ADD CONSTRAINT tenants_ingest_key_key UNIQUE (ingest_key);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'auth.users'::regclass
      AND contype = 'p'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM auth.users
      GROUP BY id
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add primary key auth.users(id): duplicate rows exist';
    END IF;

    ALTER TABLE auth.users
      ADD CONSTRAINT users_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'auth.users'::regclass
      AND conname = 'users_email_key'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM auth.users
      GROUP BY email
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add unique constraint auth.users(email): duplicate rows exist';
    END IF;

    ALTER TABLE auth.users
      ADD CONSTRAINT users_email_key UNIQUE (email);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'auth'
      AND c.relname = 'uq_auth_users_username'
      AND c.relkind = 'i'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM auth.users
      GROUP BY username
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add unique index auth.users(username): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX uq_auth_users_username ON auth.users (username);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'auth.filiais'::regclass
      AND contype = 'p'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM auth.filiais
      GROUP BY id_empresa, id_filial
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add primary key auth.filiais(id_empresa, id_filial): duplicate rows exist';
    END IF;

    ALTER TABLE auth.filiais
      ADD CONSTRAINT filiais_pkey PRIMARY KEY (id_empresa, id_filial);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'auth.user_tenants'::regclass
      AND contype = 'p'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM auth.user_tenants
      GROUP BY user_id, role, id_empresa_pk, id_filial_pk
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add primary key auth.user_tenants(user_id, role, id_empresa_pk, id_filial_pk): duplicate rows exist';
    END IF;

    ALTER TABLE auth.user_tenants
      ADD CONSTRAINT user_tenants_pkey PRIMARY KEY (user_id, role, id_empresa_pk, id_filial_pk);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'app.user_notification_settings'::regclass
      AND contype = 'p'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM app.user_notification_settings
      GROUP BY user_id
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot add primary key app.user_notification_settings(user_id): duplicate rows exist';
    END IF;

    ALTER TABLE app.user_notification_settings
      ADD CONSTRAINT user_notification_settings_pkey PRIMARY KEY (user_id);
  END IF;
END $$;

COMMIT;
