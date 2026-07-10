-- Migration 097: Gestao Orcamentaria (orcamento de despesas por conta gerencial)
-- Idempotente. Nao destrutivo.
--
-- Objetivo (linguagem do cliente): deixar o dono definir um TETO de gasto por
-- conta (ex.: "Alugueis", "Agua e Esgoto") em cada posto e avisar quando a
-- despesa do mes chega perto do teto. No Financeiro aparece o realizado x
-- orcado; no Dashboard, um alerta quando uma filial esta chegando no limite.
--
-- Fonte (Xpert): plano de contas = dbo.PLANODECONTAS; despesa realizada =
-- dbo.CONTASPAGAR (SUM(VALOR) por conta e mes, data de lancamento DTACONTA).
-- Contas gerenciais = folhas (CONTAMAE=0) efetivamente usadas em contas a pagar.

-- 1) Contas gerenciais sincronizadas do Xpert (catalogo por filial)
CREATE TABLE IF NOT EXISTS mart.plano_contas_gerencial (
  id_empresa      integer NOT NULL,
  id_filial       integer NOT NULL,
  id_plano_conta  integer NOT NULL,     -- ID_PLANODECONTAS
  codigo          text    NOT NULL DEFAULT '',
  nome_conta      text    NOT NULL DEFAULT '',
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_plano_conta)
);

-- 2) Configuracao do orcamento (teto por conta + % de alerta). App-owned.
CREATE TABLE IF NOT EXISTS app.budget_conta (
  id_empresa      integer NOT NULL,
  id_filial       integer NOT NULL,
  id_plano_conta  integer NOT NULL,
  valor_max       numeric(18,2) NOT NULL DEFAULT 0,   -- teto mensal
  alerta_pct      smallint NOT NULL DEFAULT 90,        -- alerta quando realizado >= alerta_pct% do teto
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_plano_conta)
);

-- 3) Despesa realizada por conta e mes (camada rapida). Sincronizada do Xpert.
CREATE TABLE IF NOT EXISTS mart.despesa_conta_mensal (
  id_empresa      integer NOT NULL,
  id_filial       integer NOT NULL,
  id_plano_conta  integer NOT NULL,
  ano             smallint NOT NULL,
  mes             smallint NOT NULL,
  valor_realizado numeric(18,2) NOT NULL DEFAULT 0,
  qtd             integer NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_plano_conta, ano, mes)
);

CREATE INDEX IF NOT EXISTS ix_despesa_conta_mensal_scope
  ON mart.despesa_conta_mensal (id_empresa, id_filial, ano, mes);

COMMENT ON TABLE app.budget_conta IS
  'Orcamento por conta gerencial: teto mensal (valor_max) e % de alerta. Definido em Metas & Equipe (1 filial por vez).';
COMMENT ON TABLE mart.despesa_conta_mensal IS
  'Despesa realizada por conta e mes (Xpert dbo.CONTASPAGAR por DTACONTA). Camada rapida da tela de orcamento.';
