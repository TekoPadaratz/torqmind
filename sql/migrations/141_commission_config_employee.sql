-- Migration 141: Commission employee inclusion (função Xpert + flag vendedor)
-- Permite excluir funcionários do relatório de comissão por filial/config.

BEGIN;

CREATE TABLE IF NOT EXISTS app.commission_config_employee (
  id                      bigserial PRIMARY KEY,
  config_id               bigint NOT NULL REFERENCES app.commission_config(id) ON DELETE CASCADE,
  id_funcionario          integer NOT NULL,
  nome_funcionario_snapshot text NOT NULL DEFAULT '',
  funcao_snapshot         text NOT NULL DEFAULT '',
  include_in_commission   boolean NOT NULL DEFAULT true,
  is_active               boolean NOT NULL DEFAULT true,
  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_commission_employee_config
  ON app.commission_config_employee (config_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_commission_employee_active
  ON app.commission_config_employee (config_id, id_funcionario)
  WHERE is_active = true;

COMMENT ON TABLE app.commission_config_employee IS
  'Funcionários da filial com função (Xpert) e flag para incluir no cálculo de comissão de vendedor.';

COMMENT ON COLUMN app.commission_config_employee.funcao_snapshot IS
  'Função/cargo do funcionário no Xpert (payload FUNCAO/CARGO) no momento da configuração.';

COMMIT;
