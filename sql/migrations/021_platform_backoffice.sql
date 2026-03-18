CREATE SCHEMA IF NOT EXISTS billing;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS app.channels (
  id                bigserial PRIMARY KEY,
  name              text NOT NULL,
  contact_name      text NULL,
  email             text NULL,
  phone             text NULL,
  is_enabled        boolean NOT NULL DEFAULT true,
  notes             text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_app_channels_updated_at ON app.channels;
CREATE TRIGGER trg_app_channels_updated_at
BEFORE UPDATE ON app.channels
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

ALTER TABLE auth.users
  ADD COLUMN IF NOT EXISTS nome text,
  ADD COLUMN IF NOT EXISTS role text,
  ADD COLUMN IF NOT EXISTS valid_from date,
  ADD COLUMN IF NOT EXISTS valid_until date,
  ADD COLUMN IF NOT EXISTS must_change_password boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS last_login_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS failed_login_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS locked_until timestamptz NULL,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

UPDATE auth.users
SET nome = COALESCE(NULLIF(nome, ''), initcap(replace(split_part(email, '@', 1), '.', ' ')))
WHERE nome IS NULL OR btrim(nome) = '';

UPDATE auth.users u
SET role = mapped.role
FROM (
  SELECT DISTINCT ON (user_id)
    user_id,
    CASE role
      WHEN 'MASTER' THEN 'platform_master'
      WHEN 'OWNER' THEN 'tenant_admin'
      WHEN 'MANAGER' THEN 'tenant_manager'
      ELSE role
    END AS role,
    CASE role
      WHEN 'MASTER' THEN 0
      WHEN 'platform_master' THEN 0
      WHEN 'platform_admin' THEN 1
      WHEN 'channel_admin' THEN 2
      WHEN 'OWNER' THEN 3
      WHEN 'tenant_admin' THEN 3
      WHEN 'MANAGER' THEN 4
      WHEN 'tenant_manager' THEN 4
      WHEN 'tenant_viewer' THEN 5
      ELSE 9
    END AS priority
  FROM auth.user_tenants
  ORDER BY user_id, priority, id_empresa NULLS FIRST, id_filial NULLS FIRST
) mapped
WHERE u.id = mapped.user_id
  AND (u.role IS NULL OR btrim(u.role) = '');

UPDATE auth.users
SET role = 'tenant_viewer'
WHERE role IS NULL OR btrim(role) = '';

UPDATE auth.users
SET valid_from = COALESCE(valid_from, created_at::date),
    updated_at = COALESCE(updated_at, created_at);

ALTER TABLE auth.users
  ALTER COLUMN role SET DEFAULT 'tenant_viewer',
  ALTER COLUMN role SET NOT NULL,
  ALTER COLUMN valid_from SET DEFAULT CURRENT_DATE,
  ALTER COLUMN valid_from SET NOT NULL;

ALTER TABLE auth.users
  DROP CONSTRAINT IF EXISTS ck_auth_users_role;

ALTER TABLE auth.users
  ADD CONSTRAINT ck_auth_users_role CHECK (
    role IN (
      'platform_master',
      'platform_admin',
      'channel_admin',
      'tenant_admin',
      'tenant_manager',
      'tenant_viewer'
    )
  );

DROP TRIGGER IF EXISTS trg_auth_users_updated_at ON auth.users;
CREATE TRIGGER trg_auth_users_updated_at
BEFORE UPDATE ON auth.users
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

ALTER TABLE auth.user_tenants
  ADD COLUMN IF NOT EXISTS channel_id bigint NULL,
  ADD COLUMN IF NOT EXISTS is_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS valid_from date,
  ADD COLUMN IF NOT EXISTS valid_until date,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_auth_user_tenants_channel'
      AND conrelid = 'auth.user_tenants'::regclass
  ) THEN
    ALTER TABLE auth.user_tenants
      ADD CONSTRAINT fk_auth_user_tenants_channel
      FOREIGN KEY (channel_id) REFERENCES app.channels(id);
  END IF;
END $$;

UPDATE auth.user_tenants
SET id_empresa = NULL
WHERE id_empresa = -1;

UPDATE auth.user_tenants
SET id_filial = NULL
WHERE id_filial = -1;

UPDATE auth.user_tenants
SET channel_id = NULL
WHERE channel_id = -1;

ALTER TABLE auth.user_tenants
  DROP CONSTRAINT IF EXISTS user_tenants_role_check,
  DROP CONSTRAINT IF EXISTS ck_auth_user_tenants_role_scope;

UPDATE auth.user_tenants
SET role = CASE role
  WHEN 'MASTER' THEN 'platform_master'
  WHEN 'OWNER' THEN 'tenant_admin'
  WHEN 'MANAGER' THEN 'tenant_manager'
  ELSE role
END;

UPDATE auth.user_tenants
SET valid_from = COALESCE(valid_from, CURRENT_DATE),
    updated_at = COALESCE(updated_at, created_at);

ALTER TABLE auth.user_tenants
  ALTER COLUMN valid_from SET DEFAULT CURRENT_DATE,
  ALTER COLUMN valid_from SET NOT NULL;

ALTER TABLE auth.user_tenants
  DROP CONSTRAINT IF EXISTS ck_auth_user_tenants_role_scope,
  DROP CONSTRAINT IF EXISTS ck_auth_user_tenants_role;

ALTER TABLE auth.user_tenants
  ADD CONSTRAINT ck_auth_user_tenants_role CHECK (
    role IN (
      'platform_master',
      'platform_admin',
      'channel_admin',
      'tenant_admin',
      'tenant_manager',
      'tenant_viewer'
    )
  ),
  ADD CONSTRAINT ck_auth_user_tenants_role_scope CHECK (
    (
      role IN ('platform_master', 'platform_admin')
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
    )
  );

DROP TRIGGER IF EXISTS trg_auth_user_tenants_updated_at ON auth.user_tenants;
CREATE TRIGGER trg_auth_user_tenants_updated_at
BEFORE UPDATE ON auth.user_tenants
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

ALTER TABLE auth.filiais
  ADD COLUMN IF NOT EXISTS cnpj text NULL,
  ADD COLUMN IF NOT EXISTS valid_from date,
  ADD COLUMN IF NOT EXISTS valid_until date,
  ADD COLUMN IF NOT EXISTS blocked_reason text NULL;

UPDATE auth.filiais
SET valid_from = COALESCE(valid_from, created_at::date);

ALTER TABLE auth.filiais
  ALTER COLUMN valid_from SET DEFAULT CURRENT_DATE,
  ALTER COLUMN valid_from SET NOT NULL;

ALTER TABLE app.tenants
  ADD COLUMN IF NOT EXISTS cnpj text NULL,
  ADD COLUMN IF NOT EXISTS status text,
  ADD COLUMN IF NOT EXISTS valid_from date,
  ADD COLUMN IF NOT EXISTS valid_until date,
  ADD COLUMN IF NOT EXISTS billing_status text,
  ADD COLUMN IF NOT EXISTS grace_until date NULL,
  ADD COLUMN IF NOT EXISTS suspended_reason text NULL,
  ADD COLUMN IF NOT EXISTS suspended_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS reactivated_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS channel_id bigint NULL,
  ADD COLUMN IF NOT EXISTS plan_name text NULL,
  ADD COLUMN IF NOT EXISTS monthly_amount numeric(14,2) NULL,
  ADD COLUMN IF NOT EXISTS billing_day smallint NULL,
  ADD COLUMN IF NOT EXISTS issue_day smallint NULL,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_app_tenants_channel'
      AND conrelid = 'app.tenants'::regclass
  ) THEN
    ALTER TABLE app.tenants
      ADD CONSTRAINT fk_app_tenants_channel
      FOREIGN KEY (channel_id) REFERENCES app.channels(id);
  END IF;
END $$;

UPDATE app.tenants
SET status = COALESCE(status, CASE WHEN is_active THEN 'active' ELSE 'cancelled' END),
    valid_from = COALESCE(valid_from, created_at::date),
    billing_status = COALESCE(billing_status, 'current'),
    updated_at = COALESCE(updated_at, created_at);

SELECT setval(
  pg_get_serial_sequence('app.tenants', 'id_empresa'),
  GREATEST(COALESCE((SELECT MAX(id_empresa) FROM app.tenants), 0), 1),
  true
);

ALTER TABLE app.tenants
  ALTER COLUMN status SET DEFAULT 'active',
  ALTER COLUMN valid_from SET DEFAULT CURRENT_DATE,
  ALTER COLUMN billing_status SET DEFAULT 'current',
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN valid_from SET NOT NULL,
  ALTER COLUMN billing_status SET NOT NULL;

ALTER TABLE app.tenants
  DROP CONSTRAINT IF EXISTS ck_app_tenants_status,
  DROP CONSTRAINT IF EXISTS ck_app_tenants_billing_day,
  DROP CONSTRAINT IF EXISTS ck_app_tenants_issue_day;

ALTER TABLE app.tenants
  ADD CONSTRAINT ck_app_tenants_status CHECK (
    status IN (
      'active',
      'trial',
      'overdue',
      'grace',
      'suspended_readonly',
      'suspended_total',
      'cancelled'
    )
  ),
  ADD CONSTRAINT ck_app_tenants_billing_day CHECK (
    billing_day IS NULL OR billing_day BETWEEN 1 AND 31
  ),
  ADD CONSTRAINT ck_app_tenants_issue_day CHECK (
    issue_day IS NULL OR issue_day BETWEEN 1 AND 31
  );

DROP TRIGGER IF EXISTS trg_app_tenants_updated_at ON app.tenants;
CREATE TRIGGER trg_app_tenants_updated_at
BEFORE UPDATE ON app.tenants
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

ALTER TABLE app.user_notification_settings
  ADD COLUMN IF NOT EXISTS telegram_username text NULL,
  ADD COLUMN IF NOT EXISTS email text NULL,
  ADD COLUMN IF NOT EXISTS phone text NULL;

UPDATE app.user_notification_settings s
SET email = COALESCE(s.email, u.email)
FROM auth.users u
WHERE u.id = s.user_id
  AND s.email IS NULL;

CREATE TABLE IF NOT EXISTS app.notification_subscriptions (
  id                bigserial PRIMARY KEY,
  user_id           uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  tenant_id         integer NULL REFERENCES app.tenants(id_empresa) ON DELETE CASCADE,
  branch_id         integer NULL,
  event_type        text NOT NULL,
  channel           text NOT NULL,
  severity_min      text NULL,
  is_enabled        boolean NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_app_notification_subscriptions_channel CHECK (
    channel IN ('telegram', 'email', 'phone', 'in_app')
  ),
  CONSTRAINT ck_app_notification_subscriptions_severity CHECK (
    severity_min IS NULL OR severity_min IN ('INFO', 'WARN', 'CRITICAL')
  ),
  CONSTRAINT ck_app_notification_subscriptions_branch_scope CHECK (
    branch_id IS NULL OR tenant_id IS NOT NULL
  ),
  CONSTRAINT fk_app_notification_subscriptions_branch
    FOREIGN KEY (tenant_id, branch_id)
    REFERENCES auth.filiais(id_empresa, id_filial)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_app_notification_subscriptions_scope
  ON app.notification_subscriptions (user_id, COALESCE(tenant_id, -1), COALESCE(branch_id, -1), event_type, channel);

DROP TRIGGER IF EXISTS trg_app_notification_subscriptions_updated_at ON app.notification_subscriptions;
CREATE TRIGGER trg_app_notification_subscriptions_updated_at
BEFORE UPDATE ON app.notification_subscriptions
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'auth'
      AND table_name = 'audit_log'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'audit'
      AND table_name = 'audit_log'
  ) THEN
    ALTER TABLE auth.audit_log SET SCHEMA audit;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS audit.audit_log (
  id                bigserial PRIMARY KEY,
  actor_user_id     uuid NULL,
  actor_role        text NULL,
  action            text NOT NULL,
  entity_type       text NOT NULL,
  entity_id         text NOT NULL,
  old_values        jsonb NULL,
  new_values        jsonb NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  ip                text NULL
);

ALTER TABLE audit.audit_log
  ADD COLUMN IF NOT EXISTS actor_role text NULL,
  ADD COLUMN IF NOT EXISTS entity_type text NULL,
  ADD COLUMN IF NOT EXISTS entity_id text NULL,
  ADD COLUMN IF NOT EXISTS old_values jsonb NULL,
  ADD COLUMN IF NOT EXISTS new_values jsonb NULL,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS ip text NULL;

UPDATE audit.audit_log
SET created_at = COALESCE(created_at, now())
WHERE created_at IS NULL;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'audit'
      AND table_name = 'audit_log'
      AND column_name = 'ts'
  ) THEN
    EXECUTE $sql$
      UPDATE audit.audit_log
      SET created_at = COALESCE(created_at, ts, now())
      WHERE created_at IS NULL
    $sql$;
  END IF;
END $$;

ALTER TABLE audit.audit_log
  ALTER COLUMN created_at SET DEFAULT now(),
  ALTER COLUMN created_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS ix_audit_audit_log_entity
  ON audit.audit_log (entity_type, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_audit_audit_log_actor
  ON audit.audit_log (actor_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS billing.contracts (
  id                             bigserial PRIMARY KEY,
  tenant_id                      integer NOT NULL REFERENCES app.tenants(id_empresa) ON DELETE CASCADE,
  channel_id                     bigint NULL REFERENCES app.channels(id),
  plan_name                      text NOT NULL,
  monthly_amount                 numeric(14,2) NOT NULL CHECK (monthly_amount >= 0),
  billing_day                    smallint NOT NULL CHECK (billing_day BETWEEN 1 AND 31),
  issue_day                      smallint NOT NULL CHECK (issue_day BETWEEN 1 AND 31),
  start_date                     date NOT NULL,
  end_date                       date NULL,
  is_enabled                     boolean NOT NULL DEFAULT true,
  commission_first_year_pct      numeric(7,4) NOT NULL DEFAULT 0 CHECK (commission_first_year_pct BETWEEN 0 AND 100),
  commission_recurring_pct       numeric(7,4) NOT NULL DEFAULT 0 CHECK (commission_recurring_pct BETWEEN 0 AND 100),
  notes                          text NULL,
  created_at                     timestamptz NOT NULL DEFAULT now(),
  updated_at                     timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_billing_contracts_date_window CHECK (end_date IS NULL OR end_date >= start_date)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_contracts_active_tenant
  ON billing.contracts (tenant_id)
  WHERE is_enabled = true;

CREATE INDEX IF NOT EXISTS ix_billing_contracts_lookup
  ON billing.contracts (tenant_id, start_date DESC, updated_at DESC);

DROP TRIGGER IF EXISTS trg_billing_contracts_updated_at ON billing.contracts;
CREATE TRIGGER trg_billing_contracts_updated_at
BEFORE UPDATE ON billing.contracts
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE IF NOT EXISTS billing.receivables (
  id                bigserial PRIMARY KEY,
  tenant_id         integer NOT NULL REFERENCES app.tenants(id_empresa) ON DELETE CASCADE,
  contract_id       bigint NOT NULL REFERENCES billing.contracts(id) ON DELETE CASCADE,
  competence_month  date NOT NULL,
  issue_date        date NOT NULL,
  due_date          date NOT NULL,
  amount            numeric(14,2) NOT NULL CHECK (amount >= 0),
  status            text NOT NULL DEFAULT 'planned' CHECK (status IN ('planned', 'open', 'issued', 'overdue', 'paid', 'cancelled')),
  is_emitted        boolean NOT NULL DEFAULT false,
  emitted_at        timestamptz NULL,
  paid_at           timestamptz NULL,
  received_amount   numeric(14,2) NULL CHECK (received_amount IS NULL OR received_amount >= 0),
  payment_method    text NULL,
  notes             text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_billing_receivables_competence_month CHECK (
    competence_month = date_trunc('month', competence_month)::date
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_receivables_tenant_contract_competence
  ON billing.receivables (tenant_id, contract_id, competence_month);

CREATE INDEX IF NOT EXISTS ix_billing_receivables_lookup
  ON billing.receivables (status, due_date, competence_month DESC);

DROP TRIGGER IF EXISTS trg_billing_receivables_updated_at ON billing.receivables;
CREATE TRIGGER trg_billing_receivables_updated_at
BEFORE UPDATE ON billing.receivables
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE IF NOT EXISTS billing.channel_payables (
  id                bigserial PRIMARY KEY,
  tenant_id         integer NOT NULL REFERENCES app.tenants(id_empresa) ON DELETE CASCADE,
  channel_id        bigint NOT NULL REFERENCES app.channels(id),
  receivable_id     bigint NOT NULL REFERENCES billing.receivables(id) ON DELETE CASCADE,
  competence_month  date NOT NULL,
  commission_pct    numeric(7,4) NOT NULL CHECK (commission_pct BETWEEN 0 AND 100),
  gross_amount      numeric(14,2) NOT NULL CHECK (gross_amount >= 0),
  payable_amount    numeric(14,2) NOT NULL CHECK (payable_amount >= 0),
  status            text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'released', 'paid', 'cancelled')),
  due_date          date NULL,
  paid_at           timestamptz NULL,
  notes             text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_billing_channel_payables_competence_month CHECK (
    competence_month = date_trunc('month', competence_month)::date
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_channel_payables_receivable
  ON billing.channel_payables (receivable_id);

CREATE INDEX IF NOT EXISTS ix_billing_channel_payables_lookup
  ON billing.channel_payables (channel_id, status, competence_month DESC);

DROP TRIGGER IF EXISTS trg_billing_channel_payables_updated_at ON billing.channel_payables;
CREATE TRIGGER trg_billing_channel_payables_updated_at
BEFORE UPDATE ON billing.channel_payables
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();
