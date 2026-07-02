-- 044_mart_competitor_pricing.sql
-- Realtime marts for competitor pricing feature.
-- Fed from PostgreSQL app.competitor_* tables via Mart Builder.

-- ============================================================
-- COMPETITOR PRICING: Lancamentos (individual captures per day)
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_preco_concorrente_lancamentos_dia (
    id_empresa              Int32 NOT NULL,
    id_filial               Int32 NOT NULL,
    data_key                Int32 NOT NULL,
    dt                      Date NOT NULL,
    station_id              String NOT NULL,
    station_name            String NOT NULL DEFAULT '',
    id_produto              Int32 NOT NULL,
    product_name            String NOT NULL DEFAULT '',
    fuel_type               LowCardinality(String) NOT NULL DEFAULT '',
    current_price           Decimal(12,4) NOT NULL DEFAULT 0,
    original_price          Decimal(12,4) NOT NULL DEFAULT 0,
    revision_number         UInt16 NOT NULL DEFAULT 1,
    registered_by_name      String NOT NULL DEFAULT '',
    registered_at           DateTime64(6, 'UTC') NOT NULL,
    last_updated_by_name    String NOT NULL DEFAULT '',
    last_updated_at         Nullable(DateTime64(6, 'UTC')),
    capture_status          LowCardinality(String) NOT NULL DEFAULT 'CONFIRMED',
    published_at            DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, station_id, id_produto)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- COMPETITOR PRICING: Resumo diario por concorrente
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_preco_concorrente_resumo_dia (
    id_empresa              Int32 NOT NULL,
    id_filial               Int32 NOT NULL,
    data_key                Int32 NOT NULL,
    dt                      Date NOT NULL,
    station_id              String NOT NULL,
    station_name            String NOT NULL DEFAULT '',
    qtd_produtos            UInt16 NOT NULL DEFAULT 0,
    preco_medio             Decimal(12,4) NOT NULL DEFAULT 0,
    capture_date            Date NOT NULL,
    last_capture_at         DateTime64(6, 'UTC') NOT NULL,
    published_at            DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, station_id)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- COMPETITOR PRICING: Comparativo (us vs competitors per product)
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_preco_concorrente_comparativo_dia (
    id_empresa              Int32 NOT NULL,
    id_filial               Int32 NOT NULL,
    data_key                Int32 NOT NULL,
    dt                      Date NOT NULL,
    id_produto              Int32 NOT NULL,
    product_name            String NOT NULL DEFAULT '',
    fuel_type               LowCardinality(String) NOT NULL DEFAULT '',
    nosso_preco             Decimal(12,4) NOT NULL DEFAULT 0,
    menor_concorrente       Decimal(12,4) NOT NULL DEFAULT 0,
    maior_concorrente       Decimal(12,4) NOT NULL DEFAULT 0,
    media_concorrente       Decimal(12,4) NOT NULL DEFAULT 0,
    qtd_concorrentes        UInt16 NOT NULL DEFAULT 0,
    station_menor_nome      String NOT NULL DEFAULT '',
    station_maior_nome      String NOT NULL DEFAULT '',
    delta_menor_pct         Decimal(8,2) NOT NULL DEFAULT 0,
    delta_media_pct         Decimal(8,2) NOT NULL DEFAULT 0,
    published_at            DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, id_produto)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- FUEL PRICING: Our fuel prices per product per day
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_preco_cliente_combustivel_dia (
    id_empresa              Int32 NOT NULL,
    id_filial               Int32 NOT NULL,
    data_key                Int32 NOT NULL,
    dt                      Date NOT NULL,
    id_produto              Int32 NOT NULL,
    product_name            String NOT NULL DEFAULT '',
    fuel_type               LowCardinality(String) NOT NULL DEFAULT '',
    preco_venda             Decimal(12,4) NOT NULL DEFAULT 0,
    preco_custo             Decimal(12,4) NOT NULL DEFAULT 0,
    margem                  Decimal(12,4) NOT NULL DEFAULT 0,
    volume_litros           Decimal(18,3) NOT NULL DEFAULT 0,
    faturamento             Decimal(18,2) NOT NULL DEFAULT 0,
    published_at            DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, id_produto)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;
