-- 057_mart_inventory_tank_readings.sql
-- Leituras diárias do sensor de tanque (MOVTANQUES.LEITURA / DTACONTA).
-- Grão: 1 linha por empresa/filial/tanque/dia (última LEITURA do dia).
-- Vendas diárias dos produtos de tanque (mash PG → CH) para cobertura/perda
-- sem depender do slim CDC (que pode estar atrasado).

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_inventory_tank_readings_rt (
    id_empresa       Int32,
    id_filial        Int32,
    id_tanque        Int32,
    id_produto       Int32 DEFAULT 0,
    produto_nome     String DEFAULT '',
    capacidade_l     Decimal(18, 3) DEFAULT 0,
    dia              Date,
    leitura_l        Decimal(18, 3) DEFAULT 0,
    ativo            UInt8 DEFAULT 1,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_tanque, dia)
PARTITION BY toYYYYMM(dia)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_inventory_fuel_sales_daily_rt (
    id_empresa       Int32,
    id_filial        Int32,
    id_produto       Int32,
    dia              Date,
    litros           Decimal(18, 3) DEFAULT 0,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_produto, dia)
PARTITION BY toYYYYMM(dia)
SETTINGS index_granularity = 8192;
