-- Migration: 012 - Contas a Receber/Pagar Baixa (partial payments)
-- Creates STG tables for tracking partial payments on receivables and payables.

CREATE TABLE IF NOT EXISTS stg.contasreceberbaixa (
  id_empresa              integer NOT NULL,
  id_filial               integer NOT NULL,
  id_db                   integer NOT NULL,
  id_contasreceberbaixa   integer NOT NULL,
  payload                 jsonb NOT NULL,
  ingested_at             timestamptz NOT NULL DEFAULT now(),
  dt_evento               timestamptz,
  id_db_shadow            bigint,
  id_chave_natural        text,
  received_at             timestamptz,
  PRIMARY KEY (id_empresa, id_filial, id_db, id_contasreceberbaixa)
);
CREATE INDEX IF NOT EXISTS ix_stg_contasreceberbaixa_ing ON stg.contasreceberbaixa (id_empresa, ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_contasreceberbaixa_conta ON stg.contasreceberbaixa (id_empresa, id_filial, id_db, (payload->>'ID_CONTASRECEBER'));

CREATE TABLE IF NOT EXISTS stg.contaspagarbaixa (
  id_empresa              integer NOT NULL,
  id_filial               integer NOT NULL,
  id_db                   integer NOT NULL,
  id_contaspagarbaixa     integer NOT NULL,
  payload                 jsonb NOT NULL,
  ingested_at             timestamptz NOT NULL DEFAULT now(),
  dt_evento               timestamptz,
  id_db_shadow            bigint,
  id_chave_natural        text,
  received_at             timestamptz,
  PRIMARY KEY (id_empresa, id_filial, id_db, id_contaspagarbaixa)
);
CREATE INDEX IF NOT EXISTS ix_stg_contaspagarbaixa_ing ON stg.contaspagarbaixa (id_empresa, ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_contaspagarbaixa_conta ON stg.contaspagarbaixa (id_empresa, id_filial, id_db, (payload->>'ID_CONTASPAGAR'));
