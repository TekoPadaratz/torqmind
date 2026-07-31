-- 062_mart_inventory_fuel_entries_daily.sql
-- Entradas diárias de combustível (NFe compra) para conciliação com leitura do tanque.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_inventory_fuel_entries_daily_rt (
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
