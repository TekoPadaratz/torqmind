ALTER TABLE auth.users
  DROP CONSTRAINT IF EXISTS ck_auth_users_role;

ALTER TABLE auth.users
  ADD CONSTRAINT ck_auth_users_role CHECK (
    role IN (
      'platform_master',
      'platform_admin',
      'product_global',
      'channel_admin',
      'tenant_admin',
      'tenant_manager',
      'tenant_viewer'
    )
  );

ALTER TABLE auth.user_tenants
  DROP CONSTRAINT IF EXISTS ck_auth_user_tenants_role_scope,
  DROP CONSTRAINT IF EXISTS ck_auth_user_tenants_role;

ALTER TABLE auth.user_tenants
  ADD CONSTRAINT ck_auth_user_tenants_role CHECK (
    role IN (
      'platform_master',
      'platform_admin',
      'product_global',
      'channel_admin',
      'tenant_admin',
      'tenant_manager',
      'tenant_viewer'
    )
  ),
  ADD CONSTRAINT ck_auth_user_tenants_role_scope CHECK (
    (
      role IN ('platform_master', 'platform_admin', 'product_global')
      AND channel_id IS NULL
      AND id_empresa IS NULL
      AND id_filial IS NULL
    )
    OR (
      role = 'channel_admin'
      AND channel_id IS NOT NULL
      AND id_empresa IS NULL
      AND id_filial IS NULL
    )
    OR (
      role = 'tenant_admin'
      AND channel_id IS NULL
      AND id_empresa IS NOT NULL
      AND id_filial IS NULL
    )
    OR (
      role IN ('tenant_manager', 'tenant_viewer')
      AND channel_id IS NULL
      AND id_empresa IS NOT NULL
      AND id_filial IS NOT NULL
    )
  );
