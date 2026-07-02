-- Migration 085: Commission configuration tables
-- Creates app.commission_config, app.commission_config_group, app.commission_config_tier

BEGIN;

-- Main configuration (one active config per empresa/filial)
CREATE TABLE IF NOT EXISTS app.commission_config (
  id              bigserial PRIMARY KEY,
  id_empresa      integer NOT NULL,
  id_filial       integer NOT NULL,
  name            text NOT NULL DEFAULT 'Comissao padrao',
  is_active       boolean NOT NULL DEFAULT true,
  default_payment_mode text NOT NULL DEFAULT 'team_total'
    CHECK (default_payment_mode IN ('team_total', 'equal_split', 'individual_sales')),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  created_by      integer,
  updated_by      integer
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_commission_config_active
  ON app.commission_config (id_empresa, id_filial)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_commission_config_empresa
  ON app.commission_config (id_empresa, id_filial);

-- Groups participating in commission
CREATE TABLE IF NOT EXISTS app.commission_config_group (
  id              bigserial PRIMARY KEY,
  config_id       bigint NOT NULL REFERENCES app.commission_config(id) ON DELETE CASCADE,
  id_grupo_produto integer NOT NULL,
  nome_grupo_produto_snapshot text NOT NULL DEFAULT '',
  is_active       boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_commission_group_config
  ON app.commission_config_group (config_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_commission_group_unique
  ON app.commission_config_group (config_id, id_grupo_produto)
  WHERE is_active = true;

-- Tiers (levels) for commission
CREATE TABLE IF NOT EXISTS app.commission_config_tier (
  id              bigserial PRIMARY KEY,
  config_id       bigint NOT NULL REFERENCES app.commission_config(id) ON DELETE CASCADE,
  tier_key        text NOT NULL CHECK (tier_key IN ('bronze', 'silver', 'gold', 'diamond')),
  tier_name       text NOT NULL,
  min_sales_amount numeric(18,2) NOT NULL CHECK (min_sales_amount >= 0),
  commission_percent numeric(5,2) NOT NULL CHECK (commission_percent >= 0),
  sort_order      integer NOT NULL,
  is_active       boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_commission_tier_config
  ON app.commission_config_tier (config_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_commission_tier_unique
  ON app.commission_config_tier (config_id, tier_key);

-- View for commission-eligible sales monthly by group and employee
-- Uses canonical source: fact_venda + fact_venda_item (from comprovantes)
CREATE OR REPLACE VIEW mart.commission_sales_monthly AS
SELECT
  v.id_empresa,
  v.id_filial,
  (EXTRACT(YEAR FROM v.data) * 100 + EXTRACT(MONTH FROM v.data))::integer AS ano_mes,
  COALESCE(i.id_grupo_produto, -1) AS id_grupo_produto,
  COALESCE(g.nome, '(Sem grupo)') AS nome_grupo_produto,
  COALESCE(i.id_funcionario, -1) AS id_funcionario,
  COALESCE(f.nome, '(Sem vendedor)') AS nome_vendedor,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS venda_total,
  COUNT(DISTINCT v.id_comprovante)::integer AS quantidade_vendas,
  SUM(1)::integer AS quantidade_itens,
  CASE WHEN COUNT(DISTINCT v.id_comprovante) > 0
    THEN (COALESCE(SUM(i.total), 0) / COUNT(DISTINCT v.id_comprovante))::numeric(18,2)
    ELSE 0
  END AS ticket_medio
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
  AND i.id_filial = v.id_filial
  AND i.id_db = v.id_db
  AND i.id_comprovante = v.id_comprovante
LEFT JOIN dw.dim_grupo_produto g
  ON g.id_empresa = i.id_empresa
  AND g.id_filial = i.id_filial
  AND g.id_grupo_produto = i.id_grupo_produto
LEFT JOIN dw.dim_funcionario f
  ON f.id_empresa = i.id_empresa
  AND f.id_filial = i.id_filial
  AND f.id_funcionario = i.id_funcionario
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) >= 5000
GROUP BY
  v.id_empresa,
  v.id_filial,
  (EXTRACT(YEAR FROM v.data) * 100 + EXTRACT(MONTH FROM v.data))::integer,
  COALESCE(i.id_grupo_produto, -1),
  COALESCE(g.nome, '(Sem grupo)'),
  COALESCE(i.id_funcionario, -1),
  COALESCE(f.nome, '(Sem vendedor)');

COMMIT;
