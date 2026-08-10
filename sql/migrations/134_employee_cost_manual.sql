-- Migration 134: Vale / Hora extra manuais por funcionário e competência (YYYYMM)
-- Idempotente. App-owned (OLTP). Não destrutivo.
--
-- Origem Xpert (stg.funcionarios VALES/HORASEXTRAS) costuma vir zerada; o dono
-- informa o valor do mês na tela Equipe → Custo do funcionário. Persistimos por
-- (empresa, filial, funcionário, ano_mes) para o filtro de mês recuperar o valor.

CREATE TABLE IF NOT EXISTS app.employee_cost_manual (
  id_empresa      integer        NOT NULL,
  id_filial       integer        NOT NULL,
  id_funcionario  bigint         NOT NULL,
  ano_mes         integer        NOT NULL,  -- YYYYMM
  vales           numeric(18,2)  NOT NULL DEFAULT 0,
  horas_extras    numeric(18,2)  NOT NULL DEFAULT 0,
  updated_at      timestamptz    NOT NULL DEFAULT now(),
  updated_by      text,
  PRIMARY KEY (id_empresa, id_filial, id_funcionario, ano_mes),
  CONSTRAINT ck_employee_cost_manual_ano_mes
    CHECK (ano_mes >= 200001 AND ano_mes <= 210012 AND (ano_mes % 100) BETWEEN 1 AND 12),
  CONSTRAINT ck_employee_cost_manual_vales_nonneg
    CHECK (vales >= 0),
  CONSTRAINT ck_employee_cost_manual_he_nonneg
    CHECK (horas_extras >= 0)
);

CREATE INDEX IF NOT EXISTS ix_employee_cost_manual_scope_mes
  ON app.employee_cost_manual (id_empresa, ano_mes, id_filial);

COMMENT ON TABLE app.employee_cost_manual IS
  'Override mensal de Vale e Hora extra por funcionário (tela Equipe / Custo). Chave: empresa+filial+funcionário+ano_mes.';
