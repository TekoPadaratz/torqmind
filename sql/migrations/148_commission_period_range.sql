-- Comissão gerente: overrides por intervalo (dt_ini/dt_fim), não só mês calendário.
BEGIN;

ALTER TABLE app.manager_commission_period_override
  ADD COLUMN IF NOT EXISTS dt_ini date,
  ADD COLUMN IF NOT EXISTS dt_fim date;

UPDATE app.manager_commission_period_override
SET
  dt_ini = make_date(year, month, 1),
  dt_fim = (make_date(year, month, 1) + interval '1 month' - interval '1 day')::date
WHERE dt_ini IS NULL OR dt_fim IS NULL;

ALTER TABLE app.manager_commission_period_override
  ALTER COLUMN dt_ini SET NOT NULL,
  ALTER COLUMN dt_fim SET NOT NULL;

ALTER TABLE app.manager_commission_period_override
  DROP CONSTRAINT IF EXISTS manager_commission_period_override_pkey;

ALTER TABLE app.manager_commission_period_override
  ADD PRIMARY KEY (id_empresa, id_filial, dt_ini, dt_fim);

COMMENT ON TABLE app.manager_commission_period_override IS
  'Overrides editáveis do grid de comissão de gerente por intervalo dt_ini..dt_fim.';

COMMIT;
