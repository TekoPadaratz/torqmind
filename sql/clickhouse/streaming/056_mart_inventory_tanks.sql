-- 056_mart_inventory_tanks.sql
-- Estoque de combustíveis (sensor de tanque) — snapshot RT por tanque.
-- Fonte mash: stg.tanques + última stg.movtanques.LEITURA + stg.produtos.CUSTOMEDIO.
-- NÃO usar stg.estoque para combustível (valores mentem).

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_inventory_tanks_rt (
    id_empresa       Int32,
    id_filial        Int32,
    id_tanque        Int32,
    id_produto       Int32 DEFAULT 0,
    produto_nome     String DEFAULT '',
    capacidade_l     Decimal(18, 3) DEFAULT 0,
    estoque_l        Decimal(18, 3) DEFAULT 0,
    custo_unitario   Decimal(18, 6) DEFAULT 0,
    custo_estoque    Decimal(18, 2) DEFAULT 0,
    data_leitura     Nullable(Date),
    leitura_fresca   UInt8 DEFAULT 0,
    ativo            UInt8 DEFAULT 1,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_tanque)
SETTINGS index_granularity = 8192;
