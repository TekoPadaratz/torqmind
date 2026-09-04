-- Migration 153: Comissão — opção para incluir vendas espelhadas da Central (paridade Xpert LSC)
BEGIN;

ALTER TABLE app.commission_config
  ADD COLUMN IF NOT EXISTS include_central_mirror boolean NOT NULL DEFAULT false;

ALTER TABLE app.manager_commission_rule_config
  ADD COLUMN IF NOT EXISTS include_central_mirror boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN app.commission_config.include_central_mirror IS
  'Quando true, base de comissão de vendedores inclui vendas espelhadas da filial Central.';

COMMENT ON COLUMN app.manager_commission_rule_config.include_central_mirror IS
  'Quando true, base LSC de gerente inclui vendas espelhadas da filial Central.';

COMMIT;
