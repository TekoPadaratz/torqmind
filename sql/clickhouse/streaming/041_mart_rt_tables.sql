-- TorqMind Event-Driven Streaming: Realtime Mart Tables
-- Aggregation tables for BI dashboards, fed from torqmind_current via Mart Builder.
-- ReplacingMergeTree by (published_at) ensures idempotent refresh: re-running
-- the builder for the same grain overwrites previous aggregation.

-- ============================================================
-- SALES DOMAIN
-- ============================================================

-- Daily sales aggregation by filial
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.sales_daily_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    faturamento       Decimal(18,2) NOT NULL DEFAULT 0,
    ticket_medio      Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_vendas        UInt32 NOT NULL DEFAULT 0,
    qtd_itens         UInt32 NOT NULL DEFAULT 0,
    qtd_canceladas    UInt32 NOT NULL DEFAULT 0,
    valor_cancelado   Decimal(18,2) NOT NULL DEFAULT 0,
    desconto_total    Decimal(18,2) NOT NULL DEFAULT 0,
    custo_total       Decimal(18,2) NOT NULL DEFAULT 0,
    margem_total      Decimal(18,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- Hourly sales aggregation
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.sales_hourly_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    hora              UInt8 NOT NULL,
    faturamento       Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_vendas        UInt32 NOT NULL DEFAULT 0,
    qtd_itens         UInt32 NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, hora)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- Top products daily
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.sales_products_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    id_produto        Int32 NOT NULL,
    nome_produto      String NOT NULL DEFAULT '',
    id_grupo_produto  Nullable(Int32),
    nome_grupo        String NOT NULL DEFAULT '',
    qtd               Decimal(18,3) NOT NULL DEFAULT 0,
    faturamento       Decimal(18,2) NOT NULL DEFAULT 0,
    custo_total       Decimal(18,2) NOT NULL DEFAULT 0,
    margem            Decimal(18,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, id_produto)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- Top groups daily
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.sales_groups_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    id_grupo_produto  Int32 NOT NULL,
    nome_grupo        String NOT NULL DEFAULT '',
    qtd_itens         UInt32 NOT NULL DEFAULT 0,
    faturamento       Decimal(18,2) NOT NULL DEFAULT 0,
    custo_total       Decimal(18,2) NOT NULL DEFAULT 0,
    margem            Decimal(18,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, id_grupo_produto)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- PAYMENTS DOMAIN
-- ============================================================

-- Payments aggregation by type and day
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.payments_by_type_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    tipo_forma        Int32 NOT NULL,
    label             String NOT NULL DEFAULT '',
    category          String NOT NULL DEFAULT '',
    valor_total       Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_transacoes    UInt32 NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, tipo_forma)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- CASH / CAIXA DOMAIN
-- ============================================================

-- Cash shift overview
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.cash_overview_rt (
    id_empresa              Int32 NOT NULL,
    id_filial               Int32 NOT NULL,
    id_turno                Int32 NOT NULL,
    id_usuario              Nullable(Int32),
    nome_operador           String NOT NULL DEFAULT '',
    abertura_ts             Nullable(DateTime64(6, 'UTC')),
    fechamento_ts           Nullable(DateTime64(6, 'UTC')),
    data_key_abertura       Nullable(Int32),
    is_aberto               UInt8 NOT NULL DEFAULT 0,
    faturamento_turno       Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_vendas_turno        UInt32 NOT NULL DEFAULT 0,
    published_at            DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_turno)
SETTINGS index_granularity = 8192;

-- ============================================================
-- FRAUD / RISK DOMAIN
-- ============================================================

-- Risk events aggregation by day
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.fraud_daily_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    event_type        LowCardinality(String) NOT NULL,
    qtd_eventos       UInt32 NOT NULL DEFAULT 0,
    impacto_total     Decimal(18,2) NOT NULL DEFAULT 0,
    score_medio       Decimal(10,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, event_type)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- Recent risk events (last N events per filial)
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.risk_recent_events_rt (
    id                Int64 NOT NULL,
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    event_type        LowCardinality(String) NOT NULL,
    source            LowCardinality(String) NOT NULL DEFAULT 'STG',
    id_usuario        Nullable(Int32),
    nome_operador     String NOT NULL DEFAULT '',
    id_funcionario    Nullable(Int32),
    nome_funcionario  String NOT NULL DEFAULT '',
    valor_total       Nullable(Decimal(18,2)),
    impacto_estimado  Decimal(18,2) NOT NULL DEFAULT 0,
    score_risco       Int32 NOT NULL DEFAULT 0,
    score_level       LowCardinality(String) NOT NULL DEFAULT '',
    reasons           String NOT NULL DEFAULT '{}',
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id)
SETTINGS index_granularity = 8192;

-- ============================================================
-- FINANCE DOMAIN
-- ============================================================

-- Finance aging overview
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.finance_overview_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    tipo_titulo       Int32 NOT NULL,
    faixa             LowCardinality(String) NOT NULL,
    qtd_titulos       UInt32 NOT NULL DEFAULT 0,
    valor_total       Decimal(18,2) NOT NULL DEFAULT 0,
    valor_pago_total  Decimal(18,2) NOT NULL DEFAULT 0,
    valor_em_aberto   Decimal(18,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, tipo_titulo, faixa)
SETTINGS index_granularity = 8192;

-- ============================================================
-- DASHBOARD HOME (aggregated KPIs)
-- ============================================================

-- Dashboard home daily KPIs
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.dashboard_home_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    faturamento       Decimal(18,2) NOT NULL DEFAULT 0,
    ticket_medio      Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_vendas        UInt32 NOT NULL DEFAULT 0,
    qtd_clientes      UInt32 NOT NULL DEFAULT 0,
    qtd_cancelamentos UInt32 NOT NULL DEFAULT 0,
    valor_cancelado   Decimal(18,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- CUSTOMERS DOMAIN
-- ============================================================

-- Customer churn risk snapshot
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.customers_churn_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_cliente        Int32 NOT NULL,
    nome_cliente      String NOT NULL DEFAULT '',
    segment           LowCardinality(String) NOT NULL DEFAULT '',
    risk_level        LowCardinality(String) NOT NULL DEFAULT '',
    last_purchase_key Int32 NOT NULL DEFAULT 0,
    recency_days      UInt32 NOT NULL DEFAULT 0,
    frequency_30d     UInt32 NOT NULL DEFAULT 0,
    monetary_30d      Decimal(18,2) NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_cliente)
SETTINGS index_granularity = 8192;

-- ============================================================
-- HEALTH / OPS (for Platform page)
-- ============================================================

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.source_freshness (
    id_empresa        Int32 NOT NULL,
    domain            LowCardinality(String) NOT NULL,
    last_event_ts     DateTime64(6, 'UTC') NOT NULL,
    lag_seconds       Float64 NOT NULL DEFAULT 0,
    status            LowCardinality(String) NOT NULL DEFAULT 'ok',
    checked_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (id_empresa, domain)
SETTINGS index_granularity = 8192;
