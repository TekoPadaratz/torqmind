-- Migration 103: Lancamentos de credito de clientes (Antifraude)
-- Idempotente. Nao destrutivo.
--
-- Padrao de golpe (linguagem do dono): o operador INJETA um credito no cliente
-- (MOVCREDITOENTIDADES.ENTRADAS) e depois vai APLICANDO esse credito para dar
-- baixa em vendas/pagamentos (MOVCREDITOENTIDADES.SAIDAS). A injecao (ENTRADAS)
-- e o sinal de risco: credito que nasce sem contrapartida e some em vendas.
--
-- Fonte canonica (Xpert):
--   dbo.MOVCREDITOENTIDADES: ID_ENTIDADE(cliente), ID_USUARIOS(quem lancou),
--     ENTRADAS(credito injetado), SAIDAS(credito aplicado), DATA, HISTORICO,
--     REFERENCIA(documento). dbo.CREDITO: SALDO atual por (filial,cliente,produto).

CREATE TABLE IF NOT EXISTS stg.movcreditoentidades (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  id_db        integer NOT NULL DEFAULT 0,
  id_movcredito integer NOT NULL,          -- ID_MOVCREDITOENTIDADES
  payload      jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento    timestamptz,
  id_db_shadow bigint,
  id_chave_natural text,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movcredito)
);
CREATE INDEX IF NOT EXISTS ix_stg_movcredito_evento
  ON stg.movcreditoentidades (id_empresa, id_filial, dt_evento);

CREATE TABLE IF NOT EXISTS stg.credito (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  id_credito   integer NOT NULL,           -- ID_CREDITO
  id_db        integer NOT NULL DEFAULT 0,
  payload      jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento    timestamptz,
  id_db_shadow bigint,
  id_chave_natural text,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_credito)
);

COMMENT ON TABLE stg.movcreditoentidades IS 'Movimento de credito de clientes (Xpert dbo.MOVCREDITOENTIDADES). ENTRADAS=injecao, SAIDAS=aplicacao. Antifraude.';
COMMENT ON TABLE stg.credito IS 'Saldo de credito por cliente/produto (Xpert dbo.CREDITO). SALDO atual.';
