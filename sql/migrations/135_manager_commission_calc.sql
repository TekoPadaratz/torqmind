-- Migration 135: Comissão de gerentes (LSC) — config de grupos + overrides mensais
-- Idempotente. App-owned (OLTP). Não destrutivo.
--
-- Venda base e perdas vêm de marts/CH (itens+comprovantes). Furos/sobras de caixa
-- ainda não têm fonte em STG (TURNOS sem colunas) — overrides editáveis cobrem
-- até existir dataset agent dedicado.

BEGIN;

CREATE TABLE IF NOT EXISTS app.manager_commission_rule_config (
  id              bigserial PRIMARY KEY,
  id_empresa      integer        NOT NULL,
  id_filial       integer        NOT NULL,
  default_rate_pct numeric(5,2)  NOT NULL DEFAULT 2.00
    CHECK (default_rate_pct >= 0 AND default_rate_pct <= 100),
  is_active       boolean        NOT NULL DEFAULT true,
  created_at      timestamptz    NOT NULL DEFAULT now(),
  updated_at      timestamptz    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_manager_commission_rule_active
  ON app.manager_commission_rule_config (id_empresa, id_filial)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_manager_commission_rule_empresa
  ON app.manager_commission_rule_config (id_empresa, id_filial);

CREATE TABLE IF NOT EXISTS app.manager_commission_rule_group (
  id                 bigserial PRIMARY KEY,
  config_id          bigint         NOT NULL
    REFERENCES app.manager_commission_rule_config(id) ON DELETE CASCADE,
  rule_kind          text           NOT NULL
    CHECK (rule_kind IN ('sales_base', 'stock_loss')),
  id_grupo_produto   integer        NOT NULL,
  nome_grupo         text,
  is_active          boolean        NOT NULL DEFAULT true,
  created_at         timestamptz    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_manager_commission_rule_group
  ON app.manager_commission_rule_group (config_id, rule_kind, id_grupo_produto)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_manager_commission_rule_group_config
  ON app.manager_commission_rule_group (config_id, rule_kind);

-- Overrides por competência (YYYY + month). NULL = usar default da mart/config.
CREATE TABLE IF NOT EXISTS app.manager_commission_period_override (
  id_empresa           integer        NOT NULL,
  id_filial            integer        NOT NULL,
  year                 integer        NOT NULL CHECK (year BETWEEN 2000 AND 2100),
  month                integer        NOT NULL CHECK (month BETWEEN 1 AND 12),
  rate_pct             numeric(5,2)   CHECK (rate_pct IS NULL OR (rate_pct >= 0 AND rate_pct <= 100)),
  perdas_estoque       numeric(18,2),
  sobras_estoque       numeric(18,2),
  sobras_caixa         numeric(18,2),
  furos_caixa          numeric(18,2),
  updated_at           timestamptz    NOT NULL DEFAULT now(),
  updated_by           text,
  PRIMARY KEY (id_empresa, id_filial, year, month)
);

CREATE INDEX IF NOT EXISTS ix_manager_commission_override_scope
  ON app.manager_commission_period_override (id_empresa, year, month, id_filial);

COMMENT ON TABLE app.manager_commission_rule_config IS
  'Config de comissão de gerente (taxa default + escopo). Chave ativa: empresa+filial.';
COMMENT ON TABLE app.manager_commission_rule_group IS
  'Grupos elegíveis: sales_base (CFOP venda) e stock_loss (CFOP 5927).';
COMMENT ON TABLE app.manager_commission_period_override IS
  'Overrides editáveis do grid mensal de comissão de gerente. NULL = default mart/config.';

COMMIT;
