-- 055_mart_fraud_devolucao_entrada.sql
-- Antifraude: notas de devolução de entrada (devolução de venda).
-- CFOP canônicos etl.cfop_commercial_class = devolucao_saida:
--   1202, 1411, 2202, 2411
-- NÃO inclui compra (nfe_entrada) nem devolucao_entrada (5202/5411/…).
--
-- Grain: 1 linha por comprovante (empresa/filial/db/comprovante).
-- Fonte: stg_itenscomprovantes_slim + stg_comprovantes_slim + nfe + usuarios.
-- Idempotente.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_fraud_devolucao_entrada_rt (
    id_empresa      Int32 NOT NULL,
    id_filial       Int32 NOT NULL,
    filial_nome     String NOT NULL DEFAULT '',
    data_key        Int32 NOT NULL,
    dt              Date NOT NULL,
    id_db           Int32 NOT NULL DEFAULT 0,
    id_comprovante  Int64 NOT NULL,
    id_nfe          Int64 NOT NULL DEFAULT 0,
    documento       String NOT NULL DEFAULT '',
    id_turno        Int32 NOT NULL DEFAULT 0,
    id_usuario      Int32 NOT NULL DEFAULT 0,
    nome_operador   String NOT NULL DEFAULT '',
    cfop_principal  Int32 NOT NULL DEFAULT 0,
    qtd_itens       UInt32 NOT NULL DEFAULT 0,
    valor           Decimal(18, 2) NOT NULL DEFAULT 0,
    published_at    DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, id_db, id_comprovante)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;
