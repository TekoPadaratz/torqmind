-- TorqMind - Full Reset (DROP + CREATE) - v2
-- PT-BR/EN: Execute este arquivo no PGAdmin (Query Tool) conectado ao banco torqmind.
-- This script DROPS and RECREATES all TorqMind schemas (auth/app/stg/dw/mart/etl).
--
-- ✅ Multi-tenant: id_empresa (tenant) + id_filial (branch)
-- ✅ STG: raw JSONB + ingested_at
-- ✅ DW: dims + facts
-- ✅ MART: materialized views (fast dashboards)
-- ✅ ETL: watermark + run_all()

BEGIN;

-- 0) Extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Drop schemas (order matters)
DROP SCHEMA IF EXISTS mart CASCADE;
DROP SCHEMA IF EXISTS dw CASCADE;
DROP SCHEMA IF EXISTS stg CASCADE;
DROP SCHEMA IF EXISTS etl CASCADE;
DROP SCHEMA IF EXISTS billing CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;
DROP SCHEMA IF EXISTS app CASCADE;
DROP SCHEMA IF EXISTS auth CASCADE;

-- 2) Schemas
CREATE SCHEMA auth;
CREATE SCHEMA app;
CREATE SCHEMA audit;
CREATE SCHEMA billing;
CREATE SCHEMA stg;
CREATE SCHEMA dw;
CREATE SCHEMA mart;
CREATE SCHEMA etl;

-- =========================
-- APP helpers (for RLS / session context)
-- =========================
-- PT-BR: estas funções permitem que a API injete o escopo (role/tenant/branch) na sessão via SET LOCAL.
-- EN: these helpers allow the API to push role/tenant/branch to the DB session (SET LOCAL) to support RLS later.

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

-- =========================
-- AUTH (users + scopes)
-- =========================

CREATE TABLE auth.users (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email             text UNIQUE NOT NULL,
  nome              text NOT NULL DEFAULT '',
  password_hash     text NOT NULL,
  role              text NOT NULL CHECK (role IN (
                        'platform_master',
                        'platform_admin',
                        'channel_admin',
                        'tenant_admin',
                        'tenant_manager',
                        'tenant_viewer'
                      )),
  is_active         boolean NOT NULL DEFAULT true,
  valid_from        date NOT NULL DEFAULT CURRENT_DATE,
  valid_until       date NULL,
  must_change_password boolean NOT NULL DEFAULT false,
  last_login_at     timestamptz NULL,
  failed_login_count integer NOT NULL DEFAULT 0,
  locked_until      timestamptz NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE auth.user_tenants (
  user_id           uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  role              text NOT NULL CHECK (role IN (
                        'platform_master',
                        'platform_admin',
                        'channel_admin',
                        'tenant_admin',
                        'tenant_manager',
                        'tenant_viewer'
                      )),
  channel_id        bigint NULL,
  id_empresa        integer NULL,
  id_filial         integer NULL,
  is_enabled        boolean NOT NULL DEFAULT true,
  valid_from        date NOT NULL DEFAULT CURRENT_DATE,
  valid_until       date NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  id_empresa_pk     integer GENERATED ALWAYS AS (COALESCE(id_empresa, -1)) STORED,
  id_filial_pk      integer GENERATED ALWAYS AS (COALESCE(id_filial, -1)) STORED,
  PRIMARY KEY (user_id, role, id_empresa_pk, id_filial_pk),
  CONSTRAINT ck_auth_user_tenants_role_scope CHECK (
    (role IN ('platform_master', 'platform_admin') AND channel_id IS NULL AND id_empresa IS NULL AND id_filial IS NULL) OR
    (role = 'channel_admin' AND channel_id IS NOT NULL AND id_empresa IS NULL AND id_filial IS NULL) OR
    (role = 'tenant_admin' AND channel_id IS NULL AND id_empresa IS NOT NULL AND id_filial IS NULL) OR
    (role IN ('tenant_manager', 'tenant_viewer') AND channel_id IS NULL AND id_empresa IS NOT NULL)
  )
);

CREATE TABLE audit.audit_log (
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

-- Branch catalog for access/UI convenience (kept small)
CREATE TABLE auth.filiais (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  cnpj              text NULL,
  is_active         boolean NOT NULL DEFAULT true,
  valid_from        date NOT NULL DEFAULT CURRENT_DATE,
  valid_until       date NULL,
  blocked_reason    text NULL,
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

DROP TRIGGER IF EXISTS trg_auth_users_updated_at ON auth.users;
CREATE TRIGGER trg_auth_users_updated_at
BEFORE UPDATE ON auth.users
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

DROP TRIGGER IF EXISTS trg_auth_user_tenants_updated_at ON auth.user_tenants;
CREATE TRIGGER trg_auth_user_tenants_updated_at
BEFORE UPDATE ON auth.user_tenants
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- =========================
-- APP (tenants + notifications + goals)
-- =========================

CREATE TABLE app.tenants (
  id_empresa        integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
  nome              text NOT NULL,
  ingest_key        uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  source_system     text NOT NULL DEFAULT 'XPERT',
  is_active         boolean NOT NULL DEFAULT true,
  cnpj              text NULL,
  status            text NOT NULL DEFAULT 'active' CHECK (status IN (
                        'active',
                        'trial',
                        'overdue',
                        'grace',
                        'suspended_readonly',
                        'suspended_total',
                        'cancelled'
                      )),
  valid_from        date NOT NULL DEFAULT CURRENT_DATE,
  valid_until       date NULL,
  billing_status    text NOT NULL DEFAULT 'current',
  grace_until       date NULL,
  suspended_reason  text NULL,
  suspended_at      timestamptz NULL,
  reactivated_at    timestamptz NULL,
  channel_id        bigint NULL,
  plan_name         text NULL,
  monthly_amount    numeric(14,2) NULL,
  billing_day       smallint NULL CHECK (billing_day BETWEEN 1 AND 31),
  issue_day         smallint NULL CHECK (issue_day BETWEEN 1 AND 31),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  metadata          jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE app.channels (
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

ALTER TABLE auth.user_tenants
  ADD CONSTRAINT fk_auth_user_tenants_channel
  FOREIGN KEY (channel_id) REFERENCES app.channels(id);

ALTER TABLE app.tenants
  ADD CONSTRAINT fk_app_tenants_channel
  FOREIGN KEY (channel_id) REFERENCES app.channels(id);

CREATE TABLE app.user_notification_settings (
  user_id           uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  telegram_chat_id  text NULL,
  telegram_username text NULL,
  telegram_enabled  boolean NOT NULL DEFAULT false,
  email             text NULL,
  phone             text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_app_user_notification_updated_at ON app.user_notification_settings;
CREATE TRIGGER trg_app_user_notification_updated_at
BEFORE UPDATE ON app.user_notification_settings
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

DROP TRIGGER IF EXISTS trg_app_channels_updated_at ON app.channels;
CREATE TRIGGER trg_app_channels_updated_at
BEFORE UPDATE ON app.channels
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

DROP TRIGGER IF EXISTS trg_app_tenants_updated_at ON app.tenants;
CREATE TRIGGER trg_app_tenants_updated_at
BEFORE UPDATE ON app.tenants
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- Real-time alert table (idempotent per comprovante)
CREATE TABLE app.alert_comprovante_cancelado (
  id                bigserial PRIMARY KEY,
  created_at        timestamptz NOT NULL DEFAULT now(),
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_comprovante    integer NOT NULL,
  comprovante_data  timestamp NULL,
  valor_total       numeric(18,2) NULL,
  id_usuario        integer NULL,
  id_turno          integer NULL,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (id_empresa, id_filial, id_db, id_comprovante)
);

-- Goals (for gamification / team)
CREATE TABLE app.goals (
  id                bigserial PRIMARY KEY,
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  goal_date         date NOT NULL,
  goal_type         text NOT NULL CHECK (goal_type IN (
                        'FATURAMENTO','MARGEM','TICKET','CANCELAMENTOS_MAX','INADIMPLENCIA_MAX'
                      )),
  target_value      numeric(18,2) NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id_empresa, id_filial, goal_date, goal_type)
);

CREATE TABLE app.notification_subscriptions (
  id                bigserial PRIMARY KEY,
  user_id           uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  tenant_id         integer NULL REFERENCES app.tenants(id_empresa) ON DELETE CASCADE,
  branch_id         integer NULL,
  event_type        text NOT NULL,
  channel           text NOT NULL CHECK (channel IN ('telegram', 'email', 'phone', 'in_app')),
  severity_min      text NULL CHECK (severity_min IS NULL OR severity_min IN ('INFO', 'WARN', 'CRITICAL')),
  is_enabled        boolean NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, branch_id)
    REFERENCES auth.filiais(id_empresa, id_filial)
    ON DELETE CASCADE
);

DROP TRIGGER IF EXISTS trg_app_notification_subscriptions_updated_at ON app.notification_subscriptions;
CREATE TRIGGER trg_app_notification_subscriptions_updated_at
BEFORE UPDATE ON app.notification_subscriptions
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE billing.contracts (
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
  updated_at                     timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_billing_contracts_updated_at ON billing.contracts;
CREATE TRIGGER trg_billing_contracts_updated_at
BEFORE UPDATE ON billing.contracts
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE billing.receivables (
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
  received_amount   numeric(14,2) NULL,
  payment_method    text NULL,
  notes             text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_billing_receivables_updated_at ON billing.receivables;
CREATE TRIGGER trg_billing_receivables_updated_at
BEFORE UPDATE ON billing.receivables
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE billing.channel_payables (
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
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_billing_contracts_active_tenant
  ON billing.contracts (tenant_id)
  WHERE is_enabled = true;

CREATE UNIQUE INDEX ux_billing_receivables_tenant_contract_competence
  ON billing.receivables (tenant_id, contract_id, competence_month);

CREATE UNIQUE INDEX ux_billing_channel_payables_receivable
  ON billing.channel_payables (receivable_id);

DROP TRIGGER IF EXISTS trg_billing_channel_payables_updated_at ON billing.channel_payables;
CREATE TRIGGER trg_billing_channel_payables_updated_at
BEFORE UPDATE ON billing.channel_payables
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- Competitor pricing manual inputs (per fuel/product)
CREATE TABLE app.competitor_fuel_prices (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_produto        integer NOT NULL,
  competitor_price  numeric(18,4) NOT NULL CHECK (competitor_price > 0),
  updated_by        text NULL,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_produto)
);
CREATE INDEX ix_app_competitor_fuel_prices_lookup
  ON app.competitor_fuel_prices (id_empresa, id_filial, updated_at DESC);

-- =========================
-- STG (raw JSONB)
-- =========================

-- NOTE: We keep only last version per PK (UPSERT). If you want full history, we can add a surrogate key later.

CREATE TABLE stg.filiais (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial)
);
CREATE INDEX ix_stg_filiais_ing ON stg.filiais (id_empresa, ingested_at);

CREATE TABLE stg.funcionarios (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_funcionario    integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_funcionario)
);
CREATE INDEX ix_stg_funcionarios_ing ON stg.funcionarios (id_empresa, ingested_at);

CREATE TABLE stg.entidades (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_entidade       integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_entidade)
);
CREATE INDEX ix_stg_entidades_ing ON stg.entidades (id_empresa, ingested_at);

CREATE TABLE stg.grupoprodutos (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_grupoprodutos  integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_grupoprodutos)
);
CREATE INDEX ix_stg_grupoprodutos_ing ON stg.grupoprodutos (id_empresa, ingested_at);

CREATE TABLE stg.localvendas (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_localvendas    integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_localvendas)
);
CREATE INDEX ix_stg_localvendas_ing ON stg.localvendas (id_empresa, ingested_at);

CREATE TABLE stg.produtos (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_produto        integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_produto)
);
CREATE INDEX ix_stg_produtos_ing ON stg.produtos (id_empresa, ingested_at);

CREATE TABLE stg.turnos (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_turno          integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_turno)
);
CREATE INDEX ix_stg_turnos_ing ON stg.turnos (id_empresa, ingested_at);

CREATE TABLE stg.comprovantes (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_comprovante    integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
);
CREATE INDEX ix_stg_comprovantes_ing ON stg.comprovantes (id_empresa, ingested_at);

CREATE TABLE stg.movprodutos (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_movprodutos    integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos)
);
CREATE INDEX ix_stg_movprodutos_ing ON stg.movprodutos (id_empresa, ingested_at);

CREATE TABLE stg.itensmovprodutos (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_movprodutos    integer NOT NULL,
  id_itensmovprodutos integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
);
CREATE INDEX ix_stg_itensmovprodutos_ing ON stg.itensmovprodutos (id_empresa, ingested_at);

-- Finance raw tables (optional)
CREATE TABLE stg.contaspagar (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_contaspagar    integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_contaspagar)
);
CREATE INDEX ix_stg_contaspagar_ing ON stg.contaspagar (id_empresa, ingested_at);

CREATE TABLE stg.contasreceber (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_contasreceber  integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_contasreceber)
);
CREATE INDEX ix_stg_contasreceber_ing ON stg.contasreceber (id_empresa, ingested_at);

-- Unified financeiro dataset (optional alternative)
CREATE TABLE stg.financeiro (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  tipo_titulo       integer NOT NULL, -- 0 pagar, 1 receber
  id_titulo         integer NOT NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, tipo_titulo, id_titulo)
);
CREATE INDEX ix_stg_financeiro_ing ON stg.financeiro (id_empresa, ingested_at);

-- =========================
-- DW (dims + facts)
-- =========================

CREATE TABLE dw.dim_filial (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  cnpj              text NULL,
  razao_social      text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial)
);

DROP TRIGGER IF EXISTS trg_dw_dim_filial_updated_at ON dw.dim_filial;
CREATE TRIGGER trg_dw_dim_filial_updated_at
BEFORE UPDATE ON dw.dim_filial
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.dim_grupo_produto (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_grupo_produto  integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_grupo_produto)
);
DROP TRIGGER IF EXISTS trg_dw_dim_grupo_updated_at ON dw.dim_grupo_produto;
CREATE TRIGGER trg_dw_dim_grupo_updated_at
BEFORE UPDATE ON dw.dim_grupo_produto
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.dim_local_venda (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_local_venda    integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_local_venda)
);
DROP TRIGGER IF EXISTS trg_dw_dim_local_updated_at ON dw.dim_local_venda;
CREATE TRIGGER trg_dw_dim_local_updated_at
BEFORE UPDATE ON dw.dim_local_venda
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.dim_produto (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_produto        integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  unidade           text NULL,
  id_grupo_produto  integer NULL,
  id_local_venda    integer NULL,
  custo_medio       numeric(18,6) NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_produto)
);
CREATE INDEX ix_dim_produto_grupo ON dw.dim_produto (id_empresa, id_filial, id_grupo_produto);
DROP TRIGGER IF EXISTS trg_dw_dim_produto_updated_at ON dw.dim_produto;
CREATE TRIGGER trg_dw_dim_produto_updated_at
BEFORE UPDATE ON dw.dim_produto
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.dim_funcionario (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_funcionario    integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_funcionario)
);
DROP TRIGGER IF EXISTS trg_dw_dim_func_updated_at ON dw.dim_funcionario;
CREATE TRIGGER trg_dw_dim_func_updated_at
BEFORE UPDATE ON dw.dim_funcionario
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.dim_cliente (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_cliente        integer NOT NULL,
  nome              text NOT NULL DEFAULT '',
  documento         text NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_cliente)
);
DROP TRIGGER IF EXISTS trg_dw_dim_cliente_updated_at ON dw.dim_cliente;
CREATE TRIGGER trg_dw_dim_cliente_updated_at
BEFORE UPDATE ON dw.dim_cliente
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.fact_comprovante (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_comprovante    integer NOT NULL,
  data              timestamp NULL,
  data_key          integer NULL,
  id_usuario        integer NULL,
  id_turno          integer NULL,
  id_cliente        integer NULL,
  valor_total       numeric(18,2) NULL,
  cancelado         boolean NOT NULL DEFAULT false,
  situacao          integer NULL,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
);
CREATE INDEX ix_fact_comprovante_dt ON dw.fact_comprovante (id_empresa, data_key);
DROP TRIGGER IF EXISTS trg_dw_fact_comprovante_updated_at ON dw.fact_comprovante;
CREATE TRIGGER trg_dw_fact_comprovante_updated_at
BEFORE UPDATE ON dw.fact_comprovante
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.fact_venda (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_movprodutos    integer NOT NULL,
  data              timestamp NULL,
  data_key          integer NULL,
  id_usuario        integer NULL,
  id_cliente        integer NULL,
  id_comprovante    integer NULL,
  id_turno          integer NULL,
  saidas_entradas   integer NULL,
  total_venda       numeric(18,2) NULL,
  cancelado         boolean NOT NULL DEFAULT false,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos)
);
CREATE INDEX ix_fact_venda_dt ON dw.fact_venda (id_empresa, data_key);
DROP TRIGGER IF EXISTS trg_dw_fact_venda_updated_at ON dw.fact_venda;
CREATE TRIGGER trg_dw_fact_venda_updated_at
BEFORE UPDATE ON dw.fact_venda
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE dw.fact_venda_item (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  id_movprodutos    integer NOT NULL,
  id_itensmovprodutos integer NOT NULL,
  data_key          integer NULL,
  id_produto        integer NOT NULL,
  id_grupo_produto  integer NULL,
  id_local_venda    integer NULL,
  id_funcionario    integer NULL,
  cfop              integer NULL,
  qtd               numeric(18,3) NULL,
  valor_unitario    numeric(18,4) NULL,
  total             numeric(18,2) NULL,
  desconto          numeric(18,2) NULL,
  custo_total       numeric(18,2) NULL,
  margem            numeric(18,2) NULL,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
);
CREATE INDEX ix_fact_venda_item_dt ON dw.fact_venda_item (id_empresa, data_key);
CREATE INDEX ix_fact_venda_item_prod ON dw.fact_venda_item (id_empresa, id_produto);
DROP TRIGGER IF EXISTS trg_dw_fact_venda_item_updated_at ON dw.fact_venda_item;
CREATE TRIGGER trg_dw_fact_venda_item_updated_at
BEFORE UPDATE ON dw.fact_venda_item
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- Finance unified fact
CREATE TABLE dw.fact_financeiro (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_db             integer NOT NULL,
  tipo_titulo       integer NOT NULL, -- 0 pagar, 1 receber
  id_titulo         integer NOT NULL,
  id_entidade       integer NULL,
  data_emissao      date NULL,
  data_key_emissao  integer NULL,
  vencimento        date NULL,
  data_key_venc     integer NULL,
  data_pagamento    date NULL,
  data_key_pgto     integer NULL,
  valor             numeric(18,2) NULL,
  valor_pago        numeric(18,2) NULL,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, tipo_titulo, id_titulo)
);
CREATE INDEX ix_fact_fin_emissao ON dw.fact_financeiro (id_empresa, data_key_emissao);
CREATE INDEX ix_fact_fin_venc ON dw.fact_financeiro (id_empresa, data_key_venc);
DROP TRIGGER IF EXISTS trg_dw_fact_fin_updated_at ON dw.fact_financeiro;
CREATE TRIGGER trg_dw_fact_fin_updated_at
BEFORE UPDATE ON dw.fact_financeiro
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- =========================
-- ETL (watermark + helpers)
-- =========================

CREATE TABLE etl.watermark (
  id_empresa        integer NOT NULL,
  dataset           text NOT NULL,
  last_ingested_at  timestamptz NOT NULL DEFAULT '1970-01-01'::timestamptz,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, dataset)
);

CREATE TABLE etl.run_log (
  id                bigserial PRIMARY KEY,
  started_at        timestamptz NOT NULL DEFAULT now(),
  finished_at       timestamptz NULL,
  id_empresa        integer NOT NULL,
  meta              jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE OR REPLACE FUNCTION etl.date_key(p_ts timestamp)
RETURNS integer AS $$
  SELECT CASE WHEN p_ts IS NULL THEN NULL ELSE to_char(p_ts::date,'YYYYMMDD')::int END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.to_bool(p_text text)
RETURNS boolean AS $$
  SELECT CASE
    WHEN p_text IS NULL THEN false
    WHEN lower(p_text) IN ('1','true','t','yes','y') THEN true
    ELSE false
  END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.safe_int(p_text text)
RETURNS integer AS $$
DECLARE v integer;
BEGIN
  IF p_text IS NULL OR btrim(p_text) = '' THEN
    RETURN NULL;
  END IF;
  BEGIN
    v := p_text::integer;
    RETURN v;
  EXCEPTION WHEN others THEN
    BEGIN
      v := regexp_replace(p_text, '[^0-9-]', '', 'g')::integer;
      RETURN v;
    EXCEPTION WHEN others THEN
      RETURN NULL;
    END;
  END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.safe_numeric(p_text text)
RETURNS numeric AS $$
DECLARE v numeric;
BEGIN
  IF p_text IS NULL OR btrim(p_text) = '' THEN
    RETURN NULL;
  END IF;
  BEGIN
    -- numeric supports exponent in most cases; if not, fallback to float
    v := p_text::numeric;
    RETURN v;
  EXCEPTION WHEN others THEN
    BEGIN
      v := (p_text::double precision)::numeric;
      RETURN v;
    EXCEPTION WHEN others THEN
      RETURN NULL;
    END;
  END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.safe_timestamp(p_text text)
RETURNS timestamp AS $$
DECLARE v timestamp;
BEGIN
  IF p_text IS NULL OR btrim(p_text) = '' THEN
    RETURN NULL;
  END IF;
  BEGIN
    v := p_text::timestamp;
    RETURN v;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION etl.get_watermark(p_id_empresa int, p_dataset text)
RETURNS timestamptz AS $$
  SELECT last_ingested_at FROM etl.watermark WHERE id_empresa = p_id_empresa AND dataset = p_dataset;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.set_watermark(p_id_empresa int, p_dataset text, p_ts timestamptz)
RETURNS void AS $$
BEGIN
  INSERT INTO etl.watermark (id_empresa, dataset, last_ingested_at)
  VALUES (p_id_empresa, p_dataset, COALESCE(p_ts, '1970-01-01'::timestamptz))
  ON CONFLICT (id_empresa, dataset)
  DO UPDATE SET last_ingested_at = EXCLUDED.last_ingested_at, updated_at = now();
END;
$$ LANGUAGE plpgsql;

-- =========================
-- ETL Loads (STG -> DW)
-- =========================

CREATE OR REPLACE FUNCTION etl.load_dim_filial(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      COALESCE(payload->>'NOMEFILIAL', payload->>'NOME', payload->>'RAZAOSOCIALFILIAL', '') AS nome,
      COALESCE(payload->>'CNPJ', payload->>'cnpj', payload->>'CNPJCPF', NULL) AS cnpj,
      COALESCE(payload->>'RAZAOSOCIALFILIAL', NULL) AS razao_social
    FROM stg.filiais
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_filial (id_empresa,id_filial,nome,cnpj,razao_social)
  SELECT id_empresa,id_filial,nome,cnpj,razao_social FROM src
  ON CONFLICT (id_empresa,id_filial)
  DO UPDATE SET nome=EXCLUDED.nome, cnpj=EXCLUDED.cnpj, razao_social=EXCLUDED.razao_social;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  -- keep auth.filiais in sync (for UI/access)
  INSERT INTO auth.filiais (id_empresa,id_filial,nome)
  SELECT id_empresa,id_filial,nome FROM dw.dim_filial WHERE id_empresa = p_id_empresa
  ON CONFLICT (id_empresa,id_filial) DO UPDATE SET nome=EXCLUDED.nome, is_active=true;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_dim_grupos(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_grupoprodutos AS id_grupo_produto,
      COALESCE(payload->>'NOMEGRUPOPRODUTOS','') AS nome
    FROM stg.grupoprodutos
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_grupo_produto (id_empresa,id_filial,id_grupo_produto,nome)
  SELECT id_empresa,id_filial,id_grupo_produto,nome FROM src
  ON CONFLICT (id_empresa,id_filial,id_grupo_produto)
  DO UPDATE SET nome=EXCLUDED.nome;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_dim_localvendas(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_localvendas AS id_local_venda,
      COALESCE(payload->>'NOMELOCALVENDAS','') AS nome
    FROM stg.localvendas
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_local_venda (id_empresa,id_filial,id_local_venda,nome)
  SELECT id_empresa,id_filial,id_local_venda,nome FROM src
  ON CONFLICT (id_empresa,id_filial,id_local_venda)
  DO UPDATE SET nome=EXCLUDED.nome;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_dim_produtos(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_produto,
      COALESCE(p.payload->>'NOMEPRODUTO', p.payload->>'NOME', '') AS nome,
      NULLIF(p.payload->>'UNIDADE','') AS unidade,
      etl.safe_int(p.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(p.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_numeric(p.payload->>'customedio') AS custo_medio
    FROM stg.produtos p
    WHERE p.id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_produto (id_empresa,id_filial,id_produto,nome,unidade,id_grupo_produto,id_local_venda,custo_medio)
  SELECT id_empresa,id_filial,id_produto,nome,unidade,id_grupo_produto,id_local_venda,custo_medio FROM src
  ON CONFLICT (id_empresa,id_filial,id_produto)
  DO UPDATE SET
    nome=EXCLUDED.nome,
    unidade=EXCLUDED.unidade,
    id_grupo_produto=EXCLUDED.id_grupo_produto,
    id_local_venda=EXCLUDED.id_local_venda,
    custo_medio=EXCLUDED.custo_medio;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_dim_funcionarios(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_funcionario,
      COALESCE(payload->>'NOMEFUNCIONARIO', payload->>'NOME', '') AS nome
    FROM stg.funcionarios
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_funcionario (id_empresa,id_filial,id_funcionario,nome)
  SELECT id_empresa,id_filial,id_funcionario,nome FROM src
  ON CONFLICT (id_empresa,id_filial,id_funcionario)
  DO UPDATE SET nome=EXCLUDED.nome;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_dim_clientes(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_entidade AS id_cliente,
      COALESCE(payload->>'NOMEENTIDADE', payload->>'NOME', '') AS nome,
      COALESCE(payload->>'CNPJCPF', payload->>'DOCUMENTO', NULL) AS documento
    FROM stg.entidades
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_cliente (id_empresa,id_filial,id_cliente,nome,documento)
  SELECT id_empresa,id_filial,id_cliente,nome,documento FROM src
  ON CONFLICT (id_empresa,id_filial,id_cliente)
  DO UPDATE SET nome=EXCLUDED.nome, documento=EXCLUDED.documento;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- Facts incremental loads using watermark (ingested_at)

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      etl.safe_timestamp(payload->>'DATA') AS data,
      etl.date_key(etl.safe_timestamp(payload->>'DATA')) AS data_key,
      etl.safe_int(payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_numeric(payload->>'VLRTOTAL')::numeric(18,2) AS valor_total,
      etl.to_bool(payload->>'CANCELADO') AS cancelado,
      etl.safe_int(payload->>'SITUACAO') AS situacao,
      payload,
      ingested_at
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND ingested_at > v_wm
  )
  INSERT INTO dw.fact_comprovante (
    id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
  )
  SELECT
    id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
  FROM src
  ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
  DO UPDATE SET
    data=EXCLUDED.data,
    data_key=EXCLUDED.data_key,
    id_usuario=EXCLUDED.id_usuario,
    id_turno=EXCLUDED.id_turno,
    id_cliente=EXCLUDED.id_cliente,
    valor_total=EXCLUDED.valor_total,
    cancelado=EXCLUDED.cancelado,
    situacao=EXCLUDED.situacao,
    payload=EXCLUDED.payload;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  SELECT MAX(ingested_at) INTO v_max FROM stg.comprovantes WHERE id_empresa = p_id_empresa;
  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm));

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      etl.safe_timestamp(m.payload->>'DATA') AS data,
      etl.date_key(etl.safe_timestamp(m.payload->>'DATA')) AS data_key,
      etl.safe_int(m.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(m.payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_int(m.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(m.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(m.payload->>'SAIDAS_ENTRADAS') AS saidas_entradas,
      etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2) AS total_venda,
      m.payload,
      m.ingested_at
    FROM stg.movprodutos m
    WHERE m.id_empresa = p_id_empresa
      AND m.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_cliente=EXCLUDED.id_cliente,
      id_comprovante=EXCLUDED.id_comprovante,
      id_turno=EXCLUDED.id_turno,
      saidas_entradas=EXCLUDED.saidas_entradas,
      total_venda=EXCLUDED.total_venda,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  -- Update cancelado flag from comprovantes (if present)
  UPDATE dw.fact_venda v
  SET cancelado = c.cancelado
  FROM dw.fact_comprovante c
  WHERE v.id_empresa = p_id_empresa
    AND v.id_empresa = c.id_empresa
    AND v.id_filial = c.id_filial
    AND v.id_db = c.id_db
    AND v.id_comprovante IS NOT NULL
    AND v.id_comprovante = c.id_comprovante;

  SELECT MAX(ingested_at) INTO v_max FROM stg.movprodutos WHERE id_empresa = p_id_empresa;
  PERFORM etl.set_watermark(p_id_empresa, 'movprodutos', COALESCE(v_max, v_wm));

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itensmovprodutos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      i.id_itensmovprodutos,
      v.data_key,
      etl.safe_int(i.payload->>'ID_PRODUTOS') AS id_produto,
      etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(i.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_int(i.payload->>'ID_FUNCIONARIOS') AS id_funcionario,
      etl.safe_int(i.payload->>'CFOP') AS cfop,
      etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3) AS qtd,
      etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4) AS valor_unitario,
      etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2) AS total,
      etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2) AS desconto,
      -- custo_total: prefer VLRCUSTO, fallback dim_produto.custo_medio
      COALESCE(
        (etl.safe_numeric(i.payload->>'VLRCUSTO')::numeric(18,6) * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2),
        (dp.custo_medio * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2)
      ) AS custo_total,
      i.payload,
      i.ingested_at
    FROM stg.itensmovprodutos i
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa=i.id_empresa AND v.id_filial=i.id_filial AND v.id_db=i.id_db AND v.id_movprodutos=i.id_movprodutos
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa=i.id_empresa AND dp.id_filial=i.id_filial AND dp.id_produto=etl.safe_int(i.payload->>'ID_PRODUTOS')
    WHERE i.id_empresa = p_id_empresa
      AND i.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,margem,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,
      (COALESCE(total,0) - COALESCE(custo_total,0))::numeric(18,2) AS margem,
      payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
    DO UPDATE SET
      data_key=EXCLUDED.data_key,
      id_produto=EXCLUDED.id_produto,
      id_grupo_produto=EXCLUDED.id_grupo_produto,
      id_local_venda=EXCLUDED.id_local_venda,
      id_funcionario=EXCLUDED.id_funcionario,
      cfop=EXCLUDED.cfop,
      qtd=EXCLUDED.qtd,
      valor_unitario=EXCLUDED.valor_unitario,
      total=EXCLUDED.total,
      desconto=EXCLUDED.desconto,
      custo_total=EXCLUDED.custo_total,
      margem=EXCLUDED.margem,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max FROM stg.itensmovprodutos WHERE id_empresa = p_id_empresa;
  PERFORM etl.set_watermark(p_id_empresa, 'itensmovprodutos', COALESCE(v_max, v_wm));

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- Finance: supports either stg.financeiro OR stg.contaspagar/contasreceber
CREATE OR REPLACE FUNCTION etl.load_fact_financeiro(p_id_empresa int)
RETURNS integer AS $$
DECLARE v_rows integer := 0;
DECLARE v_rows_a integer := 0;
DECLARE v_rows_b integer := 0;
BEGIN
  -- A) Unified dataset
  WITH src AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_db,
      f.tipo_titulo,
      f.id_titulo,
      etl.safe_int(f.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(f.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(f.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(f.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(f.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(f.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      f.payload
    FROM stg.financeiro f
    WHERE f.id_empresa = p_id_empresa
  )
  INSERT INTO dw.fact_financeiro (
    id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
    data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
    valor,valor_pago,payload
  )
  SELECT
    id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
    data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
    valor,valor_pago,payload
  FROM src
  ON CONFLICT (id_empresa,id_filial,id_db,tipo_titulo,id_titulo)
  DO UPDATE SET
    id_entidade=EXCLUDED.id_entidade,
    data_emissao=EXCLUDED.data_emissao,
    data_key_emissao=EXCLUDED.data_key_emissao,
    vencimento=EXCLUDED.vencimento,
    data_key_venc=EXCLUDED.data_key_venc,
    data_pagamento=EXCLUDED.data_pagamento,
    data_key_pgto=EXCLUDED.data_key_pgto,
    valor=EXCLUDED.valor,
    valor_pago=EXCLUDED.valor_pago,
    payload=EXCLUDED.payload;

  GET DIAGNOSTICS v_rows_a = ROW_COUNT;

  -- B) Raw contaspagar/contasreceber (if unified not used)
  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_db,
      0 AS tipo_titulo,
      p.id_contaspagar AS id_titulo,
      etl.safe_int(p.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(p.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(p.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(p.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(p.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      p.payload
    FROM stg.contaspagar p
    WHERE p.id_empresa = p_id_empresa

    UNION ALL

    SELECT
      r.id_empresa,
      r.id_filial,
      r.id_db,
      1 AS tipo_titulo,
      r.id_contasreceber AS id_titulo,
      etl.safe_int(r.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(r.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(r.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(r.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(r.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(r.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      r.payload
    FROM stg.contasreceber r
    WHERE r.id_empresa = p_id_empresa
  )
  INSERT INTO dw.fact_financeiro (
    id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
    data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
    valor,valor_pago,payload
  )
  SELECT
    id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
    data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
    valor,valor_pago,payload
  FROM src
  ON CONFLICT (id_empresa,id_filial,id_db,tipo_titulo,id_titulo)
  DO UPDATE SET
    id_entidade=EXCLUDED.id_entidade,
    data_emissao=EXCLUDED.data_emissao,
    data_key_emissao=EXCLUDED.data_key_emissao,
    vencimento=EXCLUDED.vencimento,
    data_key_venc=EXCLUDED.data_key_venc,
    data_pagamento=EXCLUDED.data_pagamento,
    data_key_pgto=EXCLUDED.data_key_pgto,
    valor=EXCLUDED.valor,
    valor_pago=EXCLUDED.valor_pago,
    payload=EXCLUDED.payload;

  GET DIAGNOSTICS v_rows_b = ROW_COUNT;

  v_rows := v_rows_a + v_rows_b;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- =========================
-- MART (materialized views)
-- =========================

-- IMPORTANT: for speed, dashboards read MART; ETL ends with refresh_mart().

CREATE MATERIALIZED VIEW mart.agg_vendas_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
  COALESCE(COUNT(*),0)::int AS quantidade_itens,
  COALESCE(SUM(i.margem),0)::numeric(18,2) AS margem,
  CASE WHEN COUNT(DISTINCT v.id_comprovante) = 0 THEN 0
       ELSE (SUM(i.total) / COUNT(DISTINCT v.id_comprovante))::numeric(18,2)
  END AS ticket_medio,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado,false) = false
  AND COALESCE(i.cfop,0) >= 5000
GROUP BY 1,2,3;

CREATE UNIQUE INDEX ux_mart_agg_vendas_diaria ON mart.agg_vendas_diaria (id_empresa,id_filial,data_key);
CREATE INDEX ix_mart_agg_vendas_diaria_empresa_data ON mart.agg_vendas_diaria (id_empresa, data_key);

CREATE MATERIALIZED VIEW mart.insights_base_diaria AS
WITH daily AS (
  SELECT
    a.id_empresa,
    a.id_filial,
    a.data_key,
    to_date(a.data_key::text,'YYYYMMDD') AS dt,
    a.faturamento AS faturamento_dia
  FROM mart.agg_vendas_diaria a
), daily_cum AS (
  SELECT
    d.*,
    SUM(d.faturamento_dia) OVER (
      PARTITION BY d.id_empresa, d.id_filial, date_trunc('month', d.dt)
      ORDER BY d.dt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::numeric(18,2) AS faturamento_mes_acum
  FROM daily d
), prev_join AS (
  SELECT
    cur.id_empresa,
    cur.id_filial,
    cur.data_key,
    cur.faturamento_dia,
    cur.faturamento_mes_acum,
    COALESCE(prev.faturamento_mes_acum, 0)::numeric(18,2) AS faturamento_mes_anterior_acum
  FROM daily_cum cur
  LEFT JOIN daily_cum prev
    ON prev.id_empresa = cur.id_empresa
   AND prev.id_filial = cur.id_filial
   AND prev.dt = (cur.dt - interval '1 month')::date
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  faturamento_dia,
  faturamento_mes_acum,
  (faturamento_mes_acum - faturamento_mes_anterior_acum)::numeric(18,2) AS comparativo_mes_anterior,
  NULL::text AS top_vendedor_key,
  NULL::numeric(18,2) AS top_vendedor_valor,
  NULL::numeric(18,2) AS inadimplencia_valor,
  NULL::numeric(9,4)  AS inadimplencia_pct,
  NULL::text AS cliente_em_risco_key,
  NULL::numeric(9,4) AS margem_media_pct,
  NULL::numeric(18,2) AS giro_estoque,
  now() AS updated_at,
  '{}'::jsonb AS batch_info
FROM prev_join;

CREATE UNIQUE INDEX ux_mart_insights_base_diaria ON mart.insights_base_diaria (id_empresa,id_filial,data_key);
CREATE INDEX ix_mart_insights_base_diaria_empresa_data ON mart.insights_base_diaria (id_empresa, data_key);

-- Sales by hour
CREATE MATERIALIZED VIEW mart.agg_vendas_hora AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  EXTRACT(HOUR FROM v.data)::int AS hora,
  COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem),0)::numeric(18,2) AS margem,
  COALESCE(COUNT(DISTINCT v.id_comprovante),0)::int AS vendas,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
WHERE v.data IS NOT NULL AND v.data_key IS NOT NULL
  AND COALESCE(v.cancelado,false) = false
  AND COALESCE(i.cfop,0) >= 5000
GROUP BY 1,2,3,4;

CREATE UNIQUE INDEX ux_mart_agg_vendas_hora ON mart.agg_vendas_hora (id_empresa,id_filial,data_key,hora);

-- Top products (daily)
CREATE MATERIALIZED VIEW mart.agg_produtos_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  i.id_produto,
  COALESCE(p.nome,'') AS produto_nome,
  COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem),0)::numeric(18,2) AS margem,
  COALESCE(SUM(i.qtd),0)::numeric(18,3) AS qtd,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
LEFT JOIN dw.dim_produto p
  ON p.id_empresa=i.id_empresa AND p.id_filial=i.id_filial AND p.id_produto=i.id_produto
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado,false) = false
  AND COALESCE(i.cfop,0) >= 5000
GROUP BY 1,2,3,4,5;

CREATE INDEX ix_mart_agg_produtos_diaria_lookup ON mart.agg_produtos_diaria (id_empresa, data_key, faturamento DESC);

-- Top groups (daily)
CREATE MATERIALIZED VIEW mart.agg_grupos_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  COALESCE(i.id_grupo_produto, -1) AS id_grupo_produto,
  COALESCE(g.nome,'(Sem grupo)') AS grupo_nome,
  COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem),0)::numeric(18,2) AS margem,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
LEFT JOIN dw.dim_grupo_produto g
  ON g.id_empresa=i.id_empresa AND g.id_filial=i.id_filial AND g.id_grupo_produto=i.id_grupo_produto
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado,false) = false
  AND COALESCE(i.cfop,0) >= 5000
GROUP BY 1,2,3,4,5;

CREATE INDEX ix_mart_agg_grupos_diaria_lookup ON mart.agg_grupos_diaria (id_empresa, data_key, faturamento DESC);

-- Team (employees) daily
CREATE MATERIALIZED VIEW mart.agg_funcionarios_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  COALESCE(i.id_funcionario, -1) AS id_funcionario,
  COALESCE(f.nome,'(Sem funcionário)') AS funcionario_nome,
  COALESCE(SUM(i.total),0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem),0)::numeric(18,2) AS margem,
  COALESCE(COUNT(DISTINCT v.id_comprovante),0)::int AS vendas,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
LEFT JOIN dw.dim_funcionario f
  ON f.id_empresa=i.id_empresa AND f.id_filial=i.id_filial AND f.id_funcionario=i.id_funcionario
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado,false) = false
  AND COALESCE(i.cfop,0) >= 5000
GROUP BY 1,2,3,4,5;

CREATE INDEX ix_mart_agg_funcionarios_diaria_lookup ON mart.agg_funcionarios_diaria (id_empresa, data_key, faturamento DESC);

-- Fraud: cancelamentos (daily)
CREATE MATERIALIZED VIEW mart.fraude_cancelamentos_diaria AS
SELECT
  c.id_empresa,
  c.id_filial,
  c.data_key,
  COUNT(*)::int AS cancelamentos,
  COALESCE(SUM(c.valor_total),0)::numeric(18,2) AS valor_cancelado,
  now() AS updated_at
FROM dw.fact_comprovante c
WHERE c.data_key IS NOT NULL
  AND c.cancelado = true
GROUP BY 1,2,3;

CREATE UNIQUE INDEX ux_mart_fraude_cancelamentos_diaria ON mart.fraude_cancelamentos_diaria (id_empresa,id_filial,data_key);

-- Fraud: last cancel events (lightweight view)
CREATE MATERIALIZED VIEW mart.fraude_cancelamentos_eventos AS
SELECT
  c.id_empresa,
  c.id_filial,
  c.id_db,
  c.id_comprovante,
  c.data,
  c.data_key,
  c.id_usuario,
  c.id_turno,
  c.valor_total,
  now() AS updated_at
FROM dw.fact_comprovante c
WHERE c.cancelado = true;

CREATE INDEX ix_mart_fraude_eventos_dt ON mart.fraude_cancelamentos_eventos (id_empresa, data DESC);

-- Finance: vencimentos (daily)
CREATE MATERIALIZED VIEW mart.financeiro_vencimentos_diaria AS
SELECT
  f.id_empresa,
  f.id_filial,
  f.data_key_venc AS data_key,
  f.tipo_titulo,
  COALESCE(SUM(f.valor),0)::numeric(18,2) AS valor_total,
  COALESCE(SUM(f.valor_pago),0)::numeric(18,2) AS valor_pago,
  COALESCE(SUM(CASE WHEN f.data_pagamento IS NULL THEN COALESCE(f.valor,0) - COALESCE(f.valor_pago,0) ELSE 0 END),0)::numeric(18,2) AS valor_aberto,
  now() AS updated_at
FROM dw.fact_financeiro f
WHERE f.data_key_venc IS NOT NULL
GROUP BY 1,2,3,4;

CREATE INDEX ix_mart_fin_venc_lookup ON mart.financeiro_vencimentos_diaria (id_empresa, data_key, tipo_titulo);

-- =========================
-- ETL: refresh marts
-- =========================

CREATE OR REPLACE FUNCTION etl.refresh_marts()
RETURNS void AS $$
DECLARE
  v_mv record;
BEGIN
  FOR v_mv IN
    SELECT schemaname, matviewname
    FROM pg_matviews
    WHERE schemaname = 'mart'
    ORDER BY matviewname
  LOOP
    EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', v_mv.schemaname, v_mv.matviewname);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := now();
  v_meta jsonb := '{}'::jsonb;
  v_id bigint;
BEGIN
  INSERT INTO etl.run_log (id_empresa, meta) VALUES (
    p_id_empresa,
    jsonb_build_object('status','running', 'force_full', p_force_full)
  )
  RETURNING id INTO v_id;

  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_meta := v_meta || jsonb_build_object('dim_filial', etl.load_dim_filial(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_grupos', etl.load_dim_grupos(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_localvendas', etl.load_dim_localvendas(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_produtos', etl.load_dim_produtos(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', etl.load_dim_funcionarios(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_clientes', etl.load_dim_clientes(p_id_empresa));

  v_meta := v_meta || jsonb_build_object('fact_comprovante', etl.load_fact_comprovante(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_venda', etl.load_fact_venda(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_venda_item', etl.load_fact_venda_item(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_financeiro', etl.load_fact_financeiro(p_id_empresa));

  IF p_refresh_mart THEN
    PERFORM etl.refresh_marts();
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false);
  END IF;

  UPDATE etl.run_log
  SET finished_at = now(), meta = v_meta
  WHERE id = v_id;

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'started_at', v_started,
    'finished_at', now(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  UPDATE etl.run_log
  SET
    finished_at = now(),
    meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
      'status', 'failed',
      'error', SQLERRM,
      'meta_partial', v_meta
    )
  WHERE id = v_id;
  RAISE;
END;
$$ LANGUAGE plpgsql;


-- =========================
-- DW: Risk event fact
-- =========================

CREATE TABLE IF NOT EXISTS dw.fact_risco_evento (
  id                 bigserial PRIMARY KEY,
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  data_key           integer NOT NULL,
  data               timestamptz NULL,

  event_type         text NOT NULL,
  source             text NOT NULL DEFAULT 'DW',
  id_db              integer NULL,
  id_comprovante     integer NULL,
  id_movprodutos     integer NULL,

  id_usuario         integer NULL,
  id_funcionario     integer NULL,
  id_turno           integer NULL,
  id_cliente         integer NULL,

  valor_total        numeric(18,2) NULL,
  impacto_estimado   numeric(18,2) NOT NULL DEFAULT 0,

  score_risco        integer NOT NULL CHECK (score_risco BETWEEN 0 AND 100),
  score_level        text NOT NULL CHECK (score_level IN ('NORMAL','ATENCAO','SUSPEITO','ALTO')),
  reasons            jsonb NOT NULL DEFAULT '{}'::jsonb,

  id_db_nk           integer GENERATED ALWAYS AS (COALESCE(id_db, -1)) STORED,
  id_comprovante_nk  integer GENERATED ALWAYS AS (COALESCE(id_comprovante, -1)) STORED,
  id_movprodutos_nk  integer GENERATED ALWAYS AS (COALESCE(id_movprodutos, -1)) STORED,

  created_at         timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT uq_fact_risco_evento_nk UNIQUE (
    id_empresa,
    id_filial,
    event_type,
    id_db_nk,
    id_comprovante_nk,
    id_movprodutos_nk
  )
);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_empresa_data_filial
  ON dw.fact_risco_evento (id_empresa, data_key, id_filial);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_score
  ON dw.fact_risco_evento (id_empresa, id_filial, score_risco DESC, data DESC);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_func_data
  ON dw.fact_risco_evento (id_empresa, id_filial, id_funcionario, data_key);

CREATE INDEX IF NOT EXISTS ix_fact_risco_evento_event_data
  ON dw.fact_risco_evento (id_empresa, id_filial, event_type, data_key);

-- =========================
-- APP: persisted insights
-- =========================

CREATE TABLE IF NOT EXISTS app.insights_gerados (
  id                bigserial PRIMARY KEY,
  created_at        timestamptz NOT NULL DEFAULT now(),
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  insight_type      text NOT NULL,
  severity          text NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
  dt_ref            date NOT NULL,
  impacto_estimado  numeric(18,2) NOT NULL DEFAULT 0,
  title             text NOT NULL,
  message           text NOT NULL,
  recommendation    text NOT NULL,
  status            text NOT NULL DEFAULT 'NOVO' CHECK (status IN ('NOVO','LIDO','RESOLVIDO')),
  meta              jsonb NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT uq_insights_gerados_nk UNIQUE (id_empresa, id_filial, insight_type, dt_ref)
);

CREATE INDEX IF NOT EXISTS ix_insights_gerados_lookup
  ON app.insights_gerados (id_empresa, id_filial, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_insights_gerados_status
  ON app.insights_gerados (id_empresa, id_filial, status, dt_ref DESC);

CREATE INDEX IF NOT EXISTS ix_insights_gerados_severity
  ON app.insights_gerados (id_empresa, id_filial, severity, dt_ref DESC);

-- =========================
-- ETL: Risk Scoring
-- =========================

CREATE OR REPLACE FUNCTION etl.compute_risk_events(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_lookback_days int DEFAULT 14
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
  v_wm timestamptz;
  v_start_ts timestamptz;
BEGIN
  v_wm := etl.get_watermark(p_id_empresa, 'risk_events');

  IF p_force_full THEN
    v_start_ts := now() - interval '90 days';
  ELSE
    v_start_ts := COALESCE(v_wm, now() - make_interval(days => p_lookback_days)) - interval '1 day';
  END IF;

  WITH
  user_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_usuario,
      COUNT(*)::int AS docs_total,
      COUNT(*) FILTER (WHERE c.cancelado = true)::int AS cancels,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data >= (v_start_ts - interval '14 days')
      AND c.id_usuario IS NOT NULL
    GROUP BY 1,2,3
  ),
  filial_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      COUNT(*)::int AS docs_total,
      COUNT(*) FILTER (WHERE c.cancelado = true)::int AS cancels,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data >= (v_start_ts - interval '14 days')
    GROUP BY 1,2
  ),
  hour_stats AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      EXTRACT(HOUR FROM c.data)::int AS hour_key,
      COUNT(*) FILTER (WHERE c.cancelado = true)::int AS cancel_count,
      COUNT(*)::int AS docs_total,
      COALESCE(COUNT(*) FILTER (WHERE c.cancelado = true)::numeric / NULLIF(COUNT(*)::numeric, 0), 0) AS cancel_rate
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data >= (v_start_ts - interval '30 days')
      AND c.data IS NOT NULL
    GROUP BY 1,2,3
  ),
  hour_stats_norm AS (
    SELECT
      h.*,
      AVG(h.cancel_rate) OVER (PARTITION BY h.id_empresa, h.id_filial) AS avg_rate,
      COALESCE(STDDEV_POP(h.cancel_rate) OVER (PARTITION BY h.id_empresa, h.id_filial), 0) AS std_rate
    FROM hour_stats h
  ),
  cancel_base AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      (c.data AT TIME ZONE 'UTC') AS data,
      c.id_db,
      c.id_comprovante,
      v.id_movprodutos,
      c.id_usuario,
      COALESCE(v.id_turno, c.id_turno) AS id_turno,
      c.id_cliente,
      fi.id_funcionario,
      c.valor_total,
      COALESCE(p90.p90_valor, 0) AS p90_valor,
      COALESCE(us.cancel_rate, 0) AS usr_cancel_rate,
      COALESCE(us.cancels, 0) AS usr_cancel_count,
      COALESCE(fs.cancel_rate, 0) AS filial_cancel_rate,
      COALESCE(hn.cancel_rate, 0) AS hour_cancel_rate,
      COALESCE(hn.avg_rate, 0) AS hour_avg_rate,
      COALESCE(hn.std_rate, 0) AS hour_std_rate,
      EXISTS (
        SELECT 1
        FROM dw.fact_venda v2
        WHERE v2.id_empresa = c.id_empresa
          AND v2.id_filial = c.id_filial
          AND v2.id_usuario = c.id_usuario
          AND v2.data IS NOT NULL
          AND v2.data >= c.data
          AND v2.data <= c.data + interval '2 minutes'
          AND COALESCE(v2.cancelado, false) = false
      ) AS quick_resale
    FROM dw.fact_comprovante c
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa = c.id_empresa
     AND v.id_filial = c.id_filial
     AND v.id_db = c.id_db
     AND v.id_comprovante = c.id_comprovante
    LEFT JOIN LATERAL (
      SELECT MAX(i.id_funcionario) AS id_funcionario
      FROM dw.fact_venda_item i
      WHERE i.id_empresa = c.id_empresa
        AND i.id_filial = c.id_filial
        AND i.id_db = COALESCE(v.id_db, c.id_db)
        AND i.id_movprodutos = v.id_movprodutos
    ) fi ON true
    LEFT JOIN LATERAL (
      SELECT percentile_cont(0.90) WITHIN GROUP (ORDER BY c2.valor_total) AS p90_valor
      FROM dw.fact_comprovante c2
      WHERE c2.id_empresa = c.id_empresa
        AND c2.id_filial = c.id_filial
        AND c2.data_key = c.data_key
        AND c2.cancelado = true
        AND c2.valor_total IS NOT NULL
    ) p90 ON true
    LEFT JOIN user_stats us
      ON us.id_empresa = c.id_empresa
     AND us.id_filial = c.id_filial
     AND us.id_usuario = c.id_usuario
    LEFT JOIN filial_stats fs
      ON fs.id_empresa = c.id_empresa
     AND fs.id_filial = c.id_filial
    LEFT JOIN hour_stats_norm hn
      ON hn.id_empresa = c.id_empresa
     AND hn.id_filial = c.id_filial
     AND hn.hour_key = EXTRACT(HOUR FROM c.data)::int
    WHERE c.id_empresa = p_id_empresa
      AND c.cancelado = true
      AND c.data IS NOT NULL
      AND c.data >= v_start_ts
  ),
  cancel_scored AS (
    SELECT
      b.*,
      CASE WHEN COALESCE(b.valor_total, 0) >= b.p90_valor AND b.p90_valor > 0 THEN 20 ELSE 0 END AS p_high_value,
      CASE
        WHEN b.usr_cancel_count >= 3 AND b.usr_cancel_rate >= GREATEST(0.15, b.filial_cancel_rate * 2) THEN 20
        ELSE 0
      END AS p_user_outlier,
      CASE WHEN b.quick_resale THEN 15 ELSE 0 END AS p_quick_resale,
      CASE
        WHEN b.hour_cancel_rate >= GREATEST(0.20, b.hour_avg_rate + b.hour_std_rate) THEN 10
        ELSE 0
      END AS p_risk_hour
    FROM cancel_base b
  ),
  cancel_final AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.data_key,
      s.data,
      'CANCELAMENTO'::text AS event_type,
      'DW'::text AS source,
      s.id_db,
      s.id_comprovante,
      s.id_movprodutos,
      s.id_usuario,
      s.id_funcionario,
      s.id_turno,
      s.id_cliente,
      COALESCE(s.valor_total,0)::numeric(18,2) AS valor_total,
      (COALESCE(s.valor_total,0) * 0.70)::numeric(18,2) AS impacto_estimado,
      LEAST(100, 20 + s.p_high_value + s.p_user_outlier + s.p_quick_resale + s.p_risk_hour) AS score_risco,
      jsonb_build_object(
        'base_cancelamento', 20,
        'high_value_p90', s.p_high_value,
        'user_outlier_ratio', s.p_user_outlier,
        'quick_resale_lt_2m', s.p_quick_resale,
        'risk_hour_bonus', s.p_risk_hour,
        'metrics', jsonb_build_object(
          'p90_valor', s.p90_valor,
          'valor_total', s.valor_total,
          'usr_cancel_rate', round(s.usr_cancel_rate::numeric, 4),
          'filial_cancel_rate', round(s.filial_cancel_rate::numeric, 4),
          'hour_cancel_rate', round(s.hour_cancel_rate::numeric, 4)
        )
      ) AS reasons
    FROM cancel_scored s
  ),
  discount_raw AS (
    SELECT
      v.id_empresa,
      v.id_filial,
      v.data_key,
      (v.data AT TIME ZONE 'UTC') AS data,
      v.id_db,
      v.id_comprovante,
      v.id_movprodutos,
      v.id_usuario,
      v.id_turno,
      v.id_cliente,
      MAX(i.id_funcionario) AS id_funcionario,
      SUM(COALESCE(i.total,0))::numeric(18,2) AS valor_total,
      SUM(GREATEST(COALESCE(i.desconto,0), 0))::numeric(18,2) AS desconto_total,
      AVG(NULLIF(i.valor_unitario,0))::numeric(18,4) AS avg_unit_price
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = p_id_empresa
      AND v.data IS NOT NULL
      AND v.data >= v_start_ts
      AND COALESCE(v.cancelado,false) = false
      AND COALESCE(i.cfop,0) >= 5000
    GROUP BY 1,2,3,4,5,6,7,8,9,10
  ),
  discount_scored AS (
    SELECT
      d.*,
      COALESCE(p95.p95_desconto, 0) AS p95_desconto,
      COALESCE(px.p10_price, 0) AS p10_price,
      CASE WHEN d.desconto_total > 0 AND d.desconto_total >= COALESCE(p95.p95_desconto, 0) AND COALESCE(p95.p95_desconto,0) > 0 THEN 25 ELSE 0 END AS p_desc_p95,
      CASE WHEN COALESCE(px.p10_price,0) > 0 AND d.avg_unit_price <= (px.p10_price * 0.90) THEN 10 ELSE 0 END AS p_price_outlier
    FROM discount_raw d
    LEFT JOIN LATERAL (
      SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY dr2.desconto_total) AS p95_desconto
      FROM discount_raw dr2
      WHERE dr2.id_empresa = d.id_empresa
        AND dr2.id_filial = d.id_filial
        AND dr2.data_key = d.data_key
        AND dr2.desconto_total > 0
    ) p95 ON true
    LEFT JOIN LATERAL (
      SELECT percentile_cont(0.10) WITHIN GROUP (ORDER BY i2.valor_unitario) AS p10_price
      FROM dw.fact_venda_item i2
      JOIN dw.fact_venda v2
        ON v2.id_empresa = i2.id_empresa
       AND v2.id_filial = i2.id_filial
       AND v2.id_db = i2.id_db
       AND v2.id_movprodutos = i2.id_movprodutos
      WHERE i2.id_empresa = d.id_empresa
        AND i2.id_filial = d.id_filial
        AND i2.valor_unitario IS NOT NULL
        AND v2.data >= (d.data - interval '30 days')
        AND v2.data < d.data
    ) px ON true
  ),
  discount_final AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.data_key,
      s.data,
      'DESCONTO_ALTO'::text AS event_type,
      'DW'::text AS source,
      s.id_db,
      s.id_comprovante,
      s.id_movprodutos,
      s.id_usuario,
      s.id_funcionario,
      s.id_turno,
      s.id_cliente,
      s.valor_total,
      GREATEST(COALESCE(s.desconto_total,0), COALESCE(s.valor_total,0) * 0.08)::numeric(18,2) AS impacto_estimado,
      LEAST(100, 25 + s.p_desc_p95 + s.p_price_outlier) AS score_risco,
      jsonb_build_object(
        'base_desconto', 25,
        'discount_p95_bonus', s.p_desc_p95,
        'unit_price_outlier_bonus', s.p_price_outlier,
        'metrics', jsonb_build_object(
          'desconto_total', s.desconto_total,
          'p95_desconto_dia', s.p95_desconto,
          'avg_unit_price', s.avg_unit_price,
          'p10_unit_price_30d', s.p10_price,
          'discount_hook_ready', true
        )
      ) AS reasons
    FROM discount_scored s
    WHERE s.p_desc_p95 > 0 OR s.p_price_outlier > 0
  ),
  risk_rows AS (
    SELECT * FROM cancel_final
    UNION ALL
    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'CANCELAMENTO_SEGUIDO_VENDA'::text,
      c.source,
      c.id_db,
      c.id_comprovante,
      c.id_movprodutos,
      c.id_usuario,
      c.id_funcionario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      c.impacto_estimado,
      GREATEST(c.score_risco, 80),
      c.reasons || jsonb_build_object('pattern', 'cancelamento_seguido_venda_rapida')
    FROM cancel_final c
    WHERE COALESCE((c.reasons->>'quick_resale_lt_2m')::int, 0) > 0

    UNION ALL

    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'HORARIO_RISCO'::text,
      c.source,
      c.id_db,
      c.id_comprovante,
      c.id_movprodutos,
      c.id_usuario,
      c.id_funcionario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      c.impacto_estimado,
      GREATEST(c.score_risco, 70),
      c.reasons || jsonb_build_object('pattern', 'horario_critico')
    FROM cancel_final c
    WHERE COALESCE((c.reasons->>'risk_hour_bonus')::int, 0) > 0

    UNION ALL

    SELECT
      c.id_empresa,
      c.id_filial,
      c.data_key,
      c.data,
      'FUNCIONARIO_OUTLIER'::text,
      c.source,
      c.id_db,
      c.id_comprovante,
      c.id_movprodutos,
      c.id_usuario,
      c.id_funcionario,
      c.id_turno,
      c.id_cliente,
      c.valor_total,
      c.impacto_estimado,
      GREATEST(c.score_risco, 85),
      c.reasons || jsonb_build_object('pattern', 'funcionario_outlier')
    FROM cancel_final c
    WHERE COALESCE((c.reasons->>'user_outlier_ratio')::int, 0) > 0

    UNION ALL

    SELECT * FROM discount_final
  ),
  upserted AS (
    INSERT INTO dw.fact_risco_evento (
      id_empresa,
      id_filial,
      data_key,
      data,
      event_type,
      source,
      id_db,
      id_comprovante,
      id_movprodutos,
      id_usuario,
      id_funcionario,
      id_turno,
      id_cliente,
      valor_total,
      impacto_estimado,
      score_risco,
      score_level,
      reasons
    )
    SELECT
      r.id_empresa,
      r.id_filial,
      r.data_key,
      r.data,
      r.event_type,
      r.source,
      r.id_db,
      r.id_comprovante,
      r.id_movprodutos,
      r.id_usuario,
      r.id_funcionario,
      r.id_turno,
      r.id_cliente,
      r.valor_total,
      r.impacto_estimado,
      r.score_risco,
      CASE
        WHEN r.score_risco >= 80 THEN 'ALTO'
        WHEN r.score_risco >= 60 THEN 'SUSPEITO'
        WHEN r.score_risco >= 40 THEN 'ATENCAO'
        ELSE 'NORMAL'
      END AS score_level,
      r.reasons
    FROM risk_rows r
    ON CONFLICT ON CONSTRAINT uq_fact_risco_evento_nk
    DO UPDATE SET
      data_key = EXCLUDED.data_key,
      data = EXCLUDED.data,
      id_usuario = EXCLUDED.id_usuario,
      id_funcionario = EXCLUDED.id_funcionario,
      id_turno = EXCLUDED.id_turno,
      id_cliente = EXCLUDED.id_cliente,
      valor_total = EXCLUDED.valor_total,
      impacto_estimado = EXCLUDED.impacto_estimado,
      score_risco = EXCLUDED.score_risco,
      score_level = EXCLUDED.score_level,
      reasons = EXCLUDED.reasons,
      created_at = now()
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  PERFORM etl.set_watermark(p_id_empresa, 'risk_events', now());

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- =========================
-- MART: Risk aggregates
-- =========================

DROP MATERIALIZED VIEW IF EXISTS mart.agg_risco_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.agg_risco_diaria AS
SELECT
  r.id_empresa,
  r.id_filial,
  r.data_key,
  COUNT(*)::int AS eventos_risco_total,
  COUNT(*) FILTER (WHERE r.score_risco >= 80)::int AS eventos_alto_risco,
  COALESCE(SUM(r.impacto_estimado),0)::numeric(18,2) AS impacto_estimado_total,
  COALESCE(AVG(r.score_risco),0)::numeric(10,2) AS score_medio,
  COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY r.score_risco),0)::numeric(10,2) AS p95_score,
  now() AS updated_at
FROM dw.fact_risco_evento r
GROUP BY 1,2,3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_risco_diaria
  ON mart.agg_risco_diaria (id_empresa, id_filial, data_key);
CREATE INDEX IF NOT EXISTS ix_mart_agg_risco_diaria_lookup
  ON mart.agg_risco_diaria (id_empresa, data_key, id_filial);

DROP MATERIALIZED VIEW IF EXISTS mart.risco_top_funcionarios_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.risco_top_funcionarios_diaria AS
SELECT
  r.id_empresa,
  r.id_filial,
  r.data_key,
  COALESCE(r.id_funcionario, -1) AS id_funcionario,
  COALESCE(df.nome, '(Sem funcionário)') AS funcionario_nome,
  COUNT(*)::int AS eventos,
  COUNT(*) FILTER (WHERE r.score_risco >= 80)::int AS alto_risco,
  COALESCE(SUM(r.impacto_estimado),0)::numeric(18,2) AS impacto_estimado,
  COALESCE(AVG(r.score_risco),0)::numeric(10,2) AS score_medio,
  now() AS updated_at
FROM dw.fact_risco_evento r
LEFT JOIN dw.dim_funcionario df
  ON df.id_empresa = r.id_empresa
 AND df.id_filial = r.id_filial
 AND df.id_funcionario = r.id_funcionario
GROUP BY 1,2,3,4,5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_risco_top_funcionarios_diaria
  ON mart.risco_top_funcionarios_diaria (id_empresa, id_filial, data_key, id_funcionario);
CREATE INDEX IF NOT EXISTS ix_mart_risco_top_funcionarios_lookup
  ON mart.risco_top_funcionarios_diaria (id_empresa, data_key, impacto_estimado DESC);

DROP VIEW IF EXISTS mart.risco_eventos_recentes CASCADE;
CREATE VIEW mart.risco_eventos_recentes AS
SELECT
  r.id,
  r.id_empresa,
  r.id_filial,
  r.data_key,
  r.data,
  r.event_type,
  r.source,
  r.id_db,
  r.id_comprovante,
  r.id_movprodutos,
  r.id_usuario,
  r.id_funcionario,
  COALESCE(df.nome, '(Sem funcionário)') AS funcionario_nome,
  r.id_turno,
  r.id_cliente,
  r.valor_total,
  r.impacto_estimado,
  r.score_risco,
  r.score_level,
  r.reasons,
  r.created_at
FROM dw.fact_risco_evento r
LEFT JOIN dw.dim_funcionario df
  ON df.id_empresa = r.id_empresa
 AND df.id_filial = r.id_filial
 AND df.id_funcionario = r.id_funcionario;

-- =========================
-- ETL: Insight Engine
-- =========================

CREATE OR REPLACE FUNCTION etl.generate_insights(
  p_id_empresa int,
  p_dt_ref date DEFAULT CURRENT_DATE,
  p_days_back int DEFAULT 7
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH ref_days AS (
    SELECT
      d::date AS dt_ref,
      to_char(d::date, 'YYYYMMDD')::int AS data_key
    FROM generate_series(p_dt_ref - make_interval(days => GREATEST(p_days_back - 1, 0)), p_dt_ref, interval '1 day') d
  ),
  risk_now AS (
    SELECT r.*
    FROM mart.agg_risco_diaria r
    JOIN ref_days d ON d.data_key = r.data_key
    WHERE r.id_empresa = p_id_empresa
  ),
  risk_prev AS (
    SELECT
      cur.id_empresa,
      cur.id_filial,
      cur.data_key,
      COALESCE(AVG(prev.eventos_alto_risco),0) AS avg_prev_high
    FROM risk_now cur
    LEFT JOIN mart.agg_risco_diaria prev
      ON prev.id_empresa = cur.id_empresa
     AND prev.id_filial = cur.id_filial
     AND prev.data_key BETWEEN (cur.data_key - 30) AND (cur.data_key - 1)
    GROUP BY 1,2,3
  ),
  cancel_abn AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      to_date(n.data_key::text, 'YYYYMMDD') AS dt_ref,
      'CANCELAMENTO_ANORMAL'::text AS insight_type,
      CASE WHEN n.eventos_alto_risco >= 10 THEN 'CRITICAL' ELSE 'WARN' END AS severity,
      COALESCE(n.impacto_estimado_total,0)::numeric(18,2) AS impacto_estimado,
      format('Cancelamentos de alto risco fora da curva na filial %s', n.id_filial) AS title,
      format('Hoje houve %s eventos de alto risco (média recente %s). Impacto estimado: R$ %s.',
             n.eventos_alto_risco,
             round(rp.avg_prev_high::numeric, 2),
             to_char(COALESCE(n.impacto_estimado_total,0),'FM999G999G990D00')) AS message,
      'Revisar caixas/turnos críticos e aprovações de cancelamento imediatamente.'::text AS recommendation,
      jsonb_build_object(
        'eventos_alto_risco', n.eventos_alto_risco,
        'media_30d', round(rp.avg_prev_high::numeric,2),
        'score_medio', n.score_medio
      ) AS meta
    FROM risk_now n
    JOIN risk_prev rp
      ON rp.id_empresa = n.id_empresa
     AND rp.id_filial = n.id_filial
     AND rp.data_key = n.data_key
    WHERE n.eventos_alto_risco > GREATEST(3, rp.avg_prev_high * 1.5)
  ),
  top_func_raw AS (
    SELECT
      t.id_empresa,
      t.id_filial,
      to_date(t.data_key::text, 'YYYYMMDD') AS dt_ref,
      t.id_funcionario,
      t.funcionario_nome,
      t.eventos,
      t.alto_risco,
      t.impacto_estimado,
      t.score_medio,
      row_number() OVER (PARTITION BY t.id_empresa, t.id_filial, t.data_key ORDER BY t.impacto_estimado DESC, t.score_medio DESC) AS rn
    FROM mart.risco_top_funcionarios_diaria t
    JOIN ref_days d ON d.data_key = t.data_key
    WHERE t.id_empresa = p_id_empresa
      AND t.score_medio >= 75
  ),
  top_func AS (
    SELECT
      id_empresa,
      id_filial,
      dt_ref,
      'FUNCIONARIO_RISCO_ALTO'::text AS insight_type,
      CASE WHEN score_medio >= 85 THEN 'CRITICAL' ELSE 'WARN' END AS severity,
      COALESCE(impacto_estimado,0)::numeric(18,2) AS impacto_estimado,
      format('Funcionário %s com risco elevado', funcionario_nome) AS title,
      format('Score médio %s com %s eventos (%s alto risco). Impacto estimado R$ %s.',
             round(score_medio::numeric, 1), eventos, alto_risco, to_char(COALESCE(impacto_estimado,0),'FM999G999G990D00')) AS message,
      'Auditar descontos/cancelamentos do colaborador e validar permissões no turno.'::text AS recommendation,
      jsonb_build_object(
        'id_funcionario', id_funcionario,
        'eventos', eventos,
        'alto_risco', alto_risco,
        'score_medio', score_medio
      ) AS meta
    FROM top_func_raw
    WHERE rn <= 3
  ),
  vendas_now AS (
    SELECT
      a.id_empresa,
      a.id_filial,
      a.data_key,
      to_date(a.data_key::text, 'YYYYMMDD') AS dt_ref,
      COALESCE(a.faturamento,0)::numeric(18,2) AS faturamento,
      COALESCE(a.margem,0)::numeric(18,2) AS margem,
      COALESCE(a.ticket_medio,0)::numeric(18,2) AS ticket_medio,
      CASE WHEN COALESCE(a.faturamento,0) > 0 THEN (a.margem / a.faturamento) ELSE 0 END AS margem_pct
    FROM mart.agg_vendas_diaria a
    JOIN ref_days d ON d.data_key = a.data_key
    WHERE a.id_empresa = p_id_empresa
  ),
  vendas_prev AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.data_key,
      COALESCE(AVG(p.ticket_medio),0)::numeric(18,2) AS avg_ticket_prev,
      COALESCE(AVG(CASE WHEN p.faturamento > 0 THEN (p.margem / p.faturamento) ELSE 0 END),0) AS avg_margem_pct_prev,
      COALESCE(AVG(p.faturamento),0)::numeric(18,2) AS avg_fat_prev
    FROM vendas_now n
    LEFT JOIN mart.agg_vendas_diaria p
      ON p.id_empresa = n.id_empresa
     AND p.id_filial = n.id_filial
     AND p.data_key BETWEEN (n.data_key - 7) AND (n.data_key - 1)
    GROUP BY 1,2,3
  ),
  margem_baixa AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.dt_ref,
      'MARGEM_BAIXA'::text AS insight_type,
      'WARN'::text AS severity,
      GREATEST(0, n.faturamento * (vp.avg_margem_pct_prev - n.margem_pct))::numeric(18,2) AS impacto_estimado,
      format('Margem pressionada na filial %s', n.id_filial) AS title,
      format('Margem atual %s%% vs histórico %s%%.', round((n.margem_pct * 100)::numeric, 2), round((vp.avg_margem_pct_prev * 100)::numeric, 2)) AS message,
      'Revisar mix, custos e descontos concedidos no período.'::text AS recommendation,
      jsonb_build_object('margem_pct', n.margem_pct, 'margem_pct_historica', vp.avg_margem_pct_prev) AS meta
    FROM vendas_now n
    JOIN vendas_prev vp
      ON vp.id_empresa = n.id_empresa
     AND vp.id_filial = n.id_filial
     AND vp.data_key = n.data_key
    WHERE n.margem_pct < (vp.avg_margem_pct_prev - 0.03)
  ),
  ticket_queda AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.dt_ref,
      'TICKET_QUEDA'::text AS insight_type,
      'WARN'::text AS severity,
      GREATEST(0, (vp.avg_ticket_prev - n.ticket_medio) * 20)::numeric(18,2) AS impacto_estimado,
      format('Ticket médio em queda na filial %s', n.id_filial) AS title,
      format('Ticket atual R$ %s vs média recente R$ %s.',
             to_char(n.ticket_medio,'FM999G999G990D00'),
             to_char(vp.avg_ticket_prev,'FM999G999G990D00')) AS message,
      'Ativar estratégia de upsell e combos no time comercial.'::text AS recommendation,
      jsonb_build_object('ticket_atual', n.ticket_medio, 'ticket_media_7d', vp.avg_ticket_prev) AS meta
    FROM vendas_now n
    JOIN vendas_prev vp
      ON vp.id_empresa = n.id_empresa
     AND vp.id_filial = n.id_filial
     AND vp.data_key = n.data_key
    WHERE n.ticket_medio < (vp.avg_ticket_prev * 0.90)
  ),
  faturamento_queda AS (
    SELECT
      n.id_empresa,
      n.id_filial,
      n.dt_ref,
      'FATURAMENTO_QUEDA'::text AS insight_type,
      CASE WHEN n.faturamento < (vp.avg_fat_prev * 0.70) THEN 'CRITICAL' ELSE 'WARN' END AS severity,
      GREATEST(0, vp.avg_fat_prev - n.faturamento)::numeric(18,2) AS impacto_estimado,
      format('Faturamento abaixo do ritmo na filial %s', n.id_filial) AS title,
      format('Faturamento atual R$ %s vs média da semana R$ %s.',
             to_char(n.faturamento,'FM999G999G990D00'),
             to_char(vp.avg_fat_prev,'FM999G999G990D00')) AS message,
      'Reforçar campanha local e checar ruptura/preço nas categorias críticas.'::text AS recommendation,
      jsonb_build_object('faturamento_atual', n.faturamento, 'faturamento_media_7d', vp.avg_fat_prev) AS meta
    FROM vendas_now n
    JOIN vendas_prev vp
      ON vp.id_empresa = n.id_empresa
     AND vp.id_filial = n.id_filial
     AND vp.data_key = n.data_key
    WHERE n.faturamento < (vp.avg_fat_prev * 0.85)
  ),
  all_insights AS (
    SELECT * FROM cancel_abn
    UNION ALL
    SELECT * FROM top_func
    UNION ALL
    SELECT * FROM margem_baixa
    UNION ALL
    SELECT * FROM ticket_queda
    UNION ALL
    SELECT * FROM faturamento_queda
  ),
  upserted AS (
    INSERT INTO app.insights_gerados (
      id_empresa,
      id_filial,
      insight_type,
      severity,
      dt_ref,
      impacto_estimado,
      title,
      message,
      recommendation,
      status,
      meta
    )
    SELECT
      i.id_empresa,
      i.id_filial,
      i.insight_type,
      i.severity,
      i.dt_ref,
      i.impacto_estimado,
      i.title,
      i.message,
      i.recommendation,
      'NOVO'::text,
      i.meta
    FROM all_insights i
    ON CONFLICT ON CONSTRAINT uq_insights_gerados_nk
    DO UPDATE SET
      severity = EXCLUDED.severity,
      impacto_estimado = EXCLUDED.impacto_estimado,
      title = EXCLUDED.title,
      message = EXCLUDED.message,
      recommendation = EXCLUDED.recommendation,
      meta = EXCLUDED.meta
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- =========================
-- ETL: run_all integration
-- =========================

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := now();
  v_meta jsonb := '{}'::jsonb;
  v_id bigint;
BEGIN
  INSERT INTO etl.run_log (id_empresa, meta) VALUES (
    p_id_empresa,
    jsonb_build_object('status','running', 'force_full', p_force_full)
  )
  RETURNING id INTO v_id;

  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_meta := v_meta || jsonb_build_object('dim_filial', etl.load_dim_filial(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_grupos', etl.load_dim_grupos(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_localvendas', etl.load_dim_localvendas(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_produtos', etl.load_dim_produtos(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', etl.load_dim_funcionarios(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('dim_clientes', etl.load_dim_clientes(p_id_empresa));

  v_meta := v_meta || jsonb_build_object('fact_comprovante', etl.load_fact_comprovante(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_venda', etl.load_fact_venda(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_venda_item', etl.load_fact_venda_item(p_id_empresa));
  v_meta := v_meta || jsonb_build_object('fact_financeiro', etl.load_fact_financeiro(p_id_empresa));

  v_meta := v_meta || jsonb_build_object('risk_events', etl.compute_risk_events(p_id_empresa, p_force_full, 14));

  IF p_refresh_mart THEN
    PERFORM etl.refresh_marts();
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false);
  END IF;

  v_meta := v_meta || jsonb_build_object('insights_generated', etl.generate_insights(p_id_empresa, CURRENT_DATE, 7));

  UPDATE etl.run_log
  SET finished_at = now(), meta = v_meta
  WHERE id = v_id;

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'started_at', v_started,
    'finished_at', now(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  UPDATE etl.run_log
  SET
    finished_at = now(),
    meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
      'status', 'failed',
      'error', SQLERRM,
      'meta_partial', v_meta
    )
  WHERE id = v_id;
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- Seed a default tenant (empresa 1) for dev
INSERT INTO app.tenants (id_empresa, nome) VALUES (1, 'Empresa 1 (dev)')
ON CONFLICT (id_empresa) DO NOTHING;

-- Keep at least one filial placeholder for MANAGER seed
INSERT INTO auth.filiais (id_empresa,id_filial,nome) VALUES (1,1,'Filial 1')
ON CONFLICT DO NOTHING;
INSERT INTO dw.dim_filial (id_empresa,id_filial,nome) VALUES (1,1,'Filial 1')
ON CONFLICT DO NOTHING;

COMMIT;

-- =========================
-- Phase 15: Payments by comprovante (delta sync with migration 015)
-- =========================

-- ==========================================
-- Configurable mapping for payment types
-- ==========================================

CREATE TABLE IF NOT EXISTS app.payment_type_map (
  id                bigserial PRIMARY KEY,
  id_empresa        integer NULL,
  id_empresa_nk     integer GENERATED ALWAYS AS (COALESCE(id_empresa, -1)) STORED,
  tipo_forma        integer NOT NULL,
  label             text NOT NULL,
  category          text NOT NULL,
  severity_hint     text NOT NULL DEFAULT 'INFO' CHECK (severity_hint IN ('INFO','WARN','CRITICAL')),
  active            boolean NOT NULL DEFAULT true,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id_empresa_nk, tipo_forma)
);

CREATE INDEX IF NOT EXISTS ix_payment_type_map_lookup
  ON app.payment_type_map (id_empresa_nk, tipo_forma, active, updated_at DESC);

INSERT INTO app.payment_type_map (id_empresa, tipo_forma, label, category, severity_hint, active)
VALUES
  (NULL, 0, 'DINHEIRO', 'DINHEIRO', 'INFO', true),
  (NULL, 1, 'PRAZO', 'PRAZO', 'WARN', true),
  (NULL, 2, 'CHEQUE PRE', 'CHEQUE_PRE', 'WARN', true),
  (NULL, 3, 'CARTÃO DE CRÉDITO', 'CARTAO_CREDITO', 'INFO', true),
  (NULL, 4, 'CARTÃO DE DÉBITO', 'CARTAO_DEBITO', 'INFO', true),
  (NULL, 5, 'CARTA FRETE', 'CARTA_FRETE', 'WARN', true),
  (NULL, 6, 'CHEQUE A PAGAR', 'CHEQUE_A_PAGAR', 'WARN', true),
  (NULL, 7, 'CHEQUE A VISTA', 'CHEQUE_A_VISTA', 'WARN', true),
  (NULL, 8, 'MOEDAS DIFERESAS', 'MOEDAS_DIFERESAS', 'WARN', true),
  (NULL, 9, 'OUTROS PAGOS', 'OUTROS_PAGOS', 'WARN', true),
  (NULL, 10, 'CHEQUE PRÓPRIO', 'CHEQUE_PROPRIO', 'WARN', true),
  (NULL, 28, 'PIX', 'PIX', 'INFO', true)
ON CONFLICT (id_empresa_nk, tipo_forma)
DO UPDATE SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  severity_hint = EXCLUDED.severity_hint,
  active = EXCLUDED.active,
  updated_at = now();

-- ==========================================
-- STG: formas de pagamento por comprovante
-- ==========================================

CREATE TABLE IF NOT EXISTS stg.formas_pgto_comprovantes (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_referencia     bigint NOT NULL,
  tipo_forma        integer NOT NULL,
  id_db_shadow      bigint NULL,
  id_chave_natural  text NULL,
  dt_evento         timestamptz NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  received_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_referencia, tipo_forma)
);

CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_received
  ON stg.formas_pgto_comprovantes (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_evento
  ON stg.formas_pgto_comprovantes (id_empresa, dt_evento);
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_filial_evento
  ON stg.formas_pgto_comprovantes (id_empresa, id_filial, dt_evento);
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_ref
  ON stg.formas_pgto_comprovantes (id_empresa, id_filial, id_referencia);

-- ==========================================
-- DW fact: pagamento por comprovante
-- ==========================================

CREATE TABLE IF NOT EXISTS dw.fact_pagamento_comprovante (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  referencia        bigint NOT NULL,
  id_db             integer NULL,
  id_comprovante    integer NULL,
  id_turno          integer NULL,
  id_usuario        integer NULL,
  tipo_forma        integer NOT NULL,
  valor             numeric(18,2) NOT NULL DEFAULT 0,
  dt_evento         timestamptz NOT NULL,
  data_key          integer NOT NULL,
  nsu               text NULL,
  autorizacao       text NULL,
  bandeira          text NULL,
  rede              text NULL,
  tef               text NULL,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, referencia, tipo_forma)
);

CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_lookup
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, data_key, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_turno
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, id_turno, data_key);
CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_ref
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, referencia);

DROP TRIGGER IF EXISTS trg_dw_fact_pagamento_comprovante_updated_at ON dw.fact_pagamento_comprovante;
CREATE TRIGGER trg_dw_fact_pagamento_comprovante_updated_at
BEFORE UPDATE ON dw.fact_pagamento_comprovante
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- ==========================================
-- ETL incremental: STG -> DW
-- ==========================================

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);

  WITH src_raw AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_referencia AS referencia,
      etl.safe_int(s.payload->>'TIPO_FORMA') AS tipo_forma,
      COALESCE(
        etl.safe_int(s.payload->>'ID_DB'),
        etl.safe_int(s.payload->>'id_db'),
        etl.safe_int(s.id_db_shadow::text)
      ) AS id_db,
      COALESCE(
        etl.safe_numeric(s.payload->>'VALOR'),
        etl.safe_numeric(s.payload->>'VALOR_PAGO'),
        etl.safe_numeric(s.payload->>'VALORPAGO'),
        etl.safe_numeric(s.payload->>'VLR'),
        etl.safe_numeric(s.payload->>'VLR_PAGO'),
        etl.safe_numeric(s.payload->>'VLRPAGO'),
        0
      )::numeric(18,2) AS valor,
      COALESCE(
        s.dt_evento,
        etl.safe_timestamp(s.payload->>'DATAREPL'),
        etl.safe_timestamp(s.payload->>'DATAHORA'),
        etl.safe_timestamp(s.payload->>'DATA')
      ) AS dt_evento_src,
      COALESCE(s.payload->>'NSU', s.payload->>'nsu') AS nsu,
      COALESCE(s.payload->>'AUTORIZACAO', s.payload->>'autorizacao') AS autorizacao,
      COALESCE(s.payload->>'BANDEIRA', s.payload->>'bandeira') AS bandeira,
      COALESCE(s.payload->>'REDE', s.payload->>'rede') AS rede,
      COALESCE(s.payload->>'TEF', s.payload->>'tef') AS tef,
      s.payload,
      s.received_at
    FROM stg.formas_pgto_comprovantes s
    WHERE s.id_empresa = p_id_empresa
      AND (
        s.received_at > v_wm
        OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), src_refs AS (
    SELECT DISTINCT id_empresa, id_filial, referencia
    FROM src_raw
    WHERE referencia IS NOT NULL
  ), comp_ref AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      etl.safe_int(c.payload->>'REFERENCIA') AS referencia,
      etl.safe_int(c.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(c.payload->>'ID_DB') AS id_db,
      etl.safe_int(c.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(c.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_timestamp(c.payload->>'DATA') AS data_comp,
      row_number() OVER (
        PARTITION BY c.id_empresa, c.id_filial, etl.safe_int(c.payload->>'REFERENCIA')
        ORDER BY c.received_at DESC
      ) AS rn
    FROM stg.comprovantes c
    JOIN src_refs r
      ON r.id_empresa = c.id_empresa
     AND r.id_filial = c.id_filial
     AND r.referencia = etl.safe_int(c.payload->>'REFERENCIA')
    WHERE c.id_empresa = p_id_empresa
      AND etl.safe_int(c.payload->>'REFERENCIA') IS NOT NULL
  ), src AS (
    SELECT
      r.id_empresa,
      r.id_filial,
      r.referencia,
      r.id_db,
      cr.id_comprovante,
      cr.id_turno,
      cr.id_usuario,
      r.tipo_forma,
      r.valor,
      COALESCE(r.dt_evento_src, cr.data_comp, r.received_at) AS dt_evento,
      etl.date_key(COALESCE(r.dt_evento_src, cr.data_comp, r.received_at)::timestamp) AS data_key,
      r.nsu,
      r.autorizacao,
      r.bandeira,
      r.rede,
      r.tef,
      r.payload
    FROM src_raw r
    LEFT JOIN comp_ref cr
      ON cr.id_empresa = r.id_empresa
     AND cr.id_filial = r.id_filial
     AND cr.referencia = r.referencia
     AND cr.rn = 1
    WHERE r.tipo_forma IS NOT NULL
  ), upserted AS (
    INSERT INTO dw.fact_pagamento_comprovante (
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      nsu,autorizacao,bandeira,rede,tef,payload
    )
    SELECT
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      nsu,autorizacao,bandeira,rede,tef,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,referencia,tipo_forma)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      valor = EXCLUDED.valor,
      dt_evento = EXCLUDED.dt_evento,
      data_key = EXCLUDED.data_key,
      nsu = EXCLUDED.nsu,
      autorizacao = EXCLUDED.autorizacao,
      bandeira = EXCLUDED.bandeira,
      rede = EXCLUDED.rede,
      tef = EXCLUDED.tef,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.fact_pagamento_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_pagamento_comprovante.valor IS DISTINCT FROM EXCLUDED.valor
      OR dw.fact_pagamento_comprovante.dt_evento IS DISTINCT FROM EXCLUDED.dt_evento
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.formas_pgto_comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'formas_pgto_comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- MART: payment mix and payment risk/anomalies
-- ==========================================

DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.agg_pagamentos_diaria AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, 'NÃO IDENTIFICADO') AS label,
    COALESCE(m.category, 'NAO_IDENTIFICADO') AS category,
    COALESCE(m.severity_hint, 'WARN') AS severity_hint
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category, severity_hint
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  CASE WHEN COALESCE(SUM(SUM(valor)) OVER (PARTITION BY id_empresa,id_filial,data_key),0) = 0 THEN 0
       ELSE ((SUM(valor) / NULLIF(SUM(SUM(valor)) OVER (PARTITION BY id_empresa,id_filial,data_key),0)) * 100)
  END::numeric(10,2) AS share_percent,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_diaria
  ON mart.agg_pagamentos_diaria (id_empresa, id_filial, data_key, category, label);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_diaria_lookup
  ON mart.agg_pagamentos_diaria (id_empresa, data_key, id_filial, total_valor DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_turno CASCADE;
CREATE MATERIALIZED VIEW mart.agg_pagamentos_turno AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    COALESCE(f.id_turno, -1) AS id_turno,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, 'NÃO IDENTIFICADO') AS label,
    COALESCE(m.category, 'NAO_IDENTIFICADO') AS category
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  id_turno,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, id_turno, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_turno
  ON mart.agg_pagamentos_turno (id_empresa, id_filial, data_key, id_turno, category, label);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_turno_lookup
  ON mart.agg_pagamentos_turno (id_empresa, data_key, id_filial, id_turno);

DROP MATERIALIZED VIEW IF EXISTS mart.pagamentos_anomalias_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.pagamentos_anomalias_diaria AS
WITH base_ref AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    COUNT(*)::int AS qtd_formas,
    COUNT(*) FILTER (WHERE UPPER(COALESCE(m.category, 'NAO_IDENTIFICADO')) = 'NAO_IDENTIFICADO')::int AS qtd_desconhecido,
    COALESCE(SUM(f.valor),0)::numeric(18,2) AS valor_total,
    COALESCE(SUM(CASE WHEN UPPER(COALESCE(m.category, 'NAO_IDENTIFICADO')) = 'PIX' THEN f.valor ELSE 0 END),0)::numeric(18,2) AS valor_pix,
    COALESCE(MIN(f.id_turno), -1) AS id_turno
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
  GROUP BY 1,2,3,4
), split_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    AVG(qtd_formas)::numeric(10,2) AS avg_formas,
    COUNT(*) FILTER (WHERE qtd_formas >= 3)::int AS comprovantes_multiplos,
    COUNT(*)::int AS comprovantes_total,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
  FROM base_ref
  GROUP BY 1,2,3
), split_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'SPLIT_INCOMUM'::text AS event_type,
    CASE WHEN avg_formas >= 2.4 THEN 'CRITICAL' WHEN avg_formas >= 1.8 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, ROUND((avg_formas - 1.4) * 55 + comprovantes_multiplos * 0.8)))::int AS score,
    COALESCE(valor_total,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'avg_formas_por_comprovante', avg_formas,
      'comprovantes_multiplos', comprovantes_multiplos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM split_daily
  WHERE comprovantes_total >= 20
    AND avg_formas >= 1.8
), unknown_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    COALESCE(SUM(CASE WHEN qtd_desconhecido > 0 THEN valor_total ELSE 0 END),0)::numeric(18,2) AS valor_desconhecido,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total,
    COUNT(*) FILTER (WHERE qtd_desconhecido > 0)::int AS comprovantes_desconhecidos,
    COUNT(*)::int AS comprovantes_total
  FROM base_ref
  GROUP BY 1,2,3
), unknown_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'FORMA_NAO_IDENTIFICADA'::text AS event_type,
    CASE
      WHEN (valor_desconhecido / NULLIF(valor_total,0)) >= 0.22 THEN 'CRITICAL'
      WHEN (valor_desconhecido / NULLIF(valor_total,0)) >= 0.12 THEN 'WARN'
      ELSE 'INFO'
    END AS severity,
    LEAST(100, ROUND((valor_desconhecido / NULLIF(valor_total,0)) * 280))::int AS score,
    COALESCE(valor_desconhecido,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'share_desconhecido_pct', ROUND((valor_desconhecido / NULLIF(valor_total,0)) * 100, 2),
      'valor_desconhecido', valor_desconhecido,
      'valor_total', valor_total,
      'comprovantes_desconhecidos', comprovantes_desconhecidos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM unknown_daily
  WHERE valor_total > 0
    AND (valor_desconhecido / NULLIF(valor_total,0)) >= 0.12
), turno_pix AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    COALESCE(SUM(valor_pix),0)::numeric(18,2) AS valor_pix,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total,
    CASE WHEN COALESCE(SUM(valor_total),0) = 0 THEN 0
         ELSE COALESCE(SUM(valor_pix),0) / NULLIF(COALESCE(SUM(valor_total),0),0)
    END AS pix_share
  FROM base_ref
  GROUP BY 1,2,3,4
), turno_pix_sig AS (
  SELECT
    t.*,
    AVG(pix_share) OVER (
      PARTITION BY id_empresa,id_filial,id_turno
      ORDER BY data_key
      ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS pix_share_prev_7
  FROM turno_pix t
), turno_pix_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    'PIX_DESVIO_TURNO'::text AS event_type,
    CASE
      WHEN pix_share_prev_7 IS NOT NULL AND pix_share > pix_share_prev_7 * 2.2 THEN 'CRITICAL'
      WHEN pix_share_prev_7 IS NOT NULL AND pix_share > pix_share_prev_7 * 1.7 THEN 'WARN'
      ELSE 'INFO'
    END AS severity,
    LEAST(100, GREATEST(0, ROUND((pix_share - COALESCE(pix_share_prev_7,0)) * 250)))::int AS score,
    GREATEST(0, (pix_share - COALESCE(pix_share_prev_7,0)) * valor_total)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'pix_share_pct', ROUND(pix_share * 100, 2),
      'pix_share_prev_7_pct', ROUND(COALESCE(pix_share_prev_7,0) * 100, 2),
      'valor_pix', valor_pix,
      'valor_total', valor_total
    ) AS reasons
  FROM turno_pix_sig
  WHERE pix_share_prev_7 IS NOT NULL
    AND valor_total >= 1000
    AND pix_share > pix_share_prev_7 * 1.7
), unioned AS (
  SELECT * FROM split_signal
  UNION ALL
  SELECT * FROM unknown_signal
  UNION ALL
  SELECT * FROM turno_pix_signal
)
SELECT
  u.id_empresa,
  u.id_filial,
  u.data_key,
  u.id_turno,
  u.event_type,
  u.severity,
  u.score,
  u.impacto_estimado,
  u.reasons,
  (
    u.event_type || '|' || u.id_empresa::text || '|' || u.id_filial::text || '|' || u.data_key::text || '|' || COALESCE(u.id_turno::text, '-')
  ) AS insight_id,
  (('x' || substr(md5(u.event_type || '|' || u.id_empresa::text || '|' || u.id_filial::text || '|' || u.data_key::text || '|' || COALESCE(u.id_turno::text, '-')), 1, 16))::bit(64)::bigint) AS insight_id_hash,
  now() AS updated_at
FROM unioned u
WHERE u.severity IN ('WARN','CRITICAL');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_pagamentos_anomalias_diaria
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, event_type, COALESCE(id_turno,-1));
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_lookup
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, severity, score DESC);
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_insight
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, insight_id_hash);

-- ==========================================
-- Notifications from CRITICAL payment anomalies
-- ==========================================

CREATE OR REPLACE FUNCTION etl.sync_payment_anomaly_notifications(
  p_id_empresa int,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.insight_id_hash AS insight_id,
      'CRITICAL'::text AS severity,
      format('Anomalia de pagamento (%s)', p.event_type) AS title,
      format('Score %s | Impacto estimado R$ %s', p.score, to_char(COALESCE(p.impacto_estimado,0), 'FM999G999G990D00')) AS body,
      '/fraud'::text AS url
    FROM mart.pagamentos_anomalias_diaria p
    WHERE p.id_empresa = p_id_empresa
      AND p.severity = 'CRITICAL'
      AND p.data_key >= to_char((p_ref_date - interval '2 day')::date, 'YYYYMMDD')::int
      AND p.insight_id_hash IS NOT NULL
  ), upserted AS (
    INSERT INTO app.notifications (id_empresa, id_filial, insight_id, severity, title, body, url)
    SELECT id_empresa, id_filial, insight_id, severity, title, body, url
    FROM src
    ON CONFLICT (id_empresa, id_filial, insight_id)
    WHERE insight_id IS NOT NULL
    DO UPDATE SET
      severity = EXCLUDED.severity,
      title = EXCLUDED.title,
      body = EXCLUDED.body,
      url = EXCLUDED.url,
      created_at = now(),
      read_at = NULL
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Integrate with mart refresh + run_all
-- ==========================================

DROP FUNCTION IF EXISTS etl.run_all(integer, boolean, boolean);

CREATE OR REPLACE FUNCTION etl.refresh_marts(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
DECLARE
  v_meta jsonb := '{}'::jsonb;
  v_sales_changed boolean := COALESCE((p_changed->>'fact_venda')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_venda_item')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_fin_changed boolean := COALESCE((p_changed->>'fact_financeiro')::int,0) > 0;
  v_risk_changed boolean := COALESCE((p_changed->>'risk_events')::int,0) > 0;
  v_payment_changed boolean := COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
                            OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
BEGIN
  IF v_sales_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria;
    REFRESH MATERIALIZED VIEW mart.insights_base_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_vendas_hora;
    REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
    REFRESH MATERIALIZED VIEW mart.clientes_churn_risco;
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', false);
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', false);
  END IF;

  IF v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', false);
  END IF;

  IF v_payment_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
    REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', false);
  END IF;

  RETURN v_meta;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_meta jsonb := '{}'::jsonb;
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
  v_refresh_meta jsonb := '{}'::jsonb;
BEGIN
  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_filial(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_filial', v_rows, 'dim_filial_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_filial', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_grupos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_grupos', v_rows, 'dim_grupos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_grupos', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_localvendas(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_localvendas', v_rows, 'dim_localvendas_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_localvendas', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_produtos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_produtos', v_rows, 'dim_produtos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_produtos', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_funcionarios(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', v_rows, 'dim_funcionarios_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_funcionarios', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_clientes(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_clientes', v_rows, 'dim_clientes_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_clientes', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_comprovante', v_rows, 'fact_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_comprovante', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_pagamento_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_pagamento_comprovante', v_rows, 'fact_pagamento_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_pagamento_comprovante', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_venda(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda', v_rows, 'fact_venda_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_venda_item(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda_item', v_rows, 'fact_venda_item_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda_item', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_financeiro(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_financeiro', v_rows, 'fact_financeiro_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_financeiro', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  IF p_force_full
     OR COALESCE((v_meta->>'fact_comprovante')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda_item')::int,0) > 0
     OR COALESCE((v_meta->>'fact_pagamento_comprovante')::int,0) > 0
  THEN
    v_step_started := clock_timestamp();
    v_rows := etl.compute_risk_events(p_id_empresa, p_force_full, 14);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('risk_events', v_rows, 'risk_events_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'risk_events', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_rows := 0;
    v_meta := v_meta || jsonb_build_object('risk_events', 0, 'risk_events_skipped', true, 'risk_events_skip_reason', 'no_fact_changes');
    PERFORM etl.log_step(p_id_empresa, 'risk_events', clock_timestamp(), clock_timestamp(), 'ok', 0, NULL, jsonb_build_object('skipped', true, 'reason', 'no_fact_changes'));
  END IF;

  IF p_refresh_mart THEN
    v_step_started := clock_timestamp();
    v_refresh_meta := etl.refresh_marts(v_meta);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true, 'mart_refresh', v_refresh_meta, 'mart_refresh_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'refresh_marts', v_step_started, clock_timestamp(), 'ok', 1, NULL, jsonb_build_object('ms', v_step_ms, 'refresh', v_refresh_meta));

    v_step_started := clock_timestamp();
    v_rows := etl.sync_payment_anomaly_notifications(p_id_empresa, p_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('payment_notifications', v_rows, 'payment_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'payment_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false, 'payment_notifications', 0, 'payment_notifications_skipped', true);
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.generate_insights(p_id_empresa, p_ref_date, 7);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('insights_generated', v_rows, 'insights_generated_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'insights_generated', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    clock_timestamp(),
    'ok',
    1,
    NULL,
    jsonb_build_object('force_full', p_force_full, 'refresh_mart', p_refresh_mart, 'meta', v_meta)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'ref_date', p_ref_date,
    'hot_window_days', etl.hot_window_days(),
    'started_at', v_started,
    'finished_at', clock_timestamp(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    clock_timestamp(),
    'failed',
    0,
    SQLERRM,
    jsonb_build_object('meta_partial', v_meta)
  );
  RAISE;
END;
$$ LANGUAGE plpgsql;
\ir migrations/019_operational_truth_alignment.sql
\ir migrations/020_snapshot_backfill_and_perf.sql
