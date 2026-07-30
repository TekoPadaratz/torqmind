-- 059_mart_finance_titles.sql
-- Títulos operacionais de contas a pagar/receber, publicados do STG PostgreSQL.
-- Grão: empresa / tipo / filial / banco de dados de origem / título.
-- Sem PARTITION BY mês: títulos históricos atravessam muitos YYYYMM e o INSERT
-- estoura max_partitions_per_insert_block.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_finance_titles_rt (
    id_empresa       Int32,
    id_filial        Int32,
    tipo_titulo      Int8 COMMENT '0=pagar, 1=receber',
    id_titulo        Int64,
    id_db            Int32,
    id_entidade      Int64 DEFAULT 0,
    entidade_nome    String DEFAULT '',
    dt_lancamento    Nullable(Date),
    dt_vencimento    Date,
    valor            Decimal(18, 2) DEFAULT 0,
    valor_pago       Decimal(18, 2) DEFAULT 0,
    valor_aberto     Decimal(18, 2) DEFAULT 0,
    status           LowCardinality(String),
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, tipo_titulo, id_filial, id_db, id_titulo)
SETTINGS index_granularity = 8192;
