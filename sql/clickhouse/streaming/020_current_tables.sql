-- TorqMind Event-Driven Streaming: Current state layer
-- ReplacingMergeTree tables that hold the latest version of each row.
-- Version column = source_ts_ms (Debezium source timestamp in millis).
-- is_deleted flag for soft-delete handling.
-- ingested_at DateTime64(6) for dedup and freshness tracking.
-- All columns match exactly what clickhouse_writer.py inserts + DEFAULT-filled meta columns.

-- ============================================================
-- DIMENSIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS torqmind_current.dim_filial (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    cnpj              Nullable(String),
    razao_social      Nullable(String),
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.dim_produto (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_produto        Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    unidade           Nullable(String),
    id_grupo_produto  Nullable(Int32),
    id_local_venda    Nullable(Int32),
    custo_medio       Nullable(Decimal(18,6)),
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_produto)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.dim_grupo_produto (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_grupo_produto  Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_grupo_produto)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.dim_funcionario (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_funcionario    Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_funcionario)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.dim_usuario_caixa (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_usuario        Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    payload           String NOT NULL DEFAULT '{}',
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_usuario)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.dim_local_venda (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_local_venda    Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_local_venda)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.dim_cliente (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_cliente        Int32 NOT NULL,
    nome              String NOT NULL DEFAULT '',
    documento         Nullable(String),
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_cliente)
SETTINGS index_granularity = 8192;

-- ============================================================
-- FACTS
-- ============================================================

CREATE TABLE IF NOT EXISTS torqmind_current.fact_venda (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_db             Int32 NOT NULL,
    id_movprodutos    Int32 NOT NULL,
    data_key          Int32 NOT NULL DEFAULT 0,
    data              Nullable(DateTime64(6, 'UTC')),
    id_usuario        Nullable(Int32),
    id_cliente        Nullable(Int32),
    id_comprovante    Nullable(Int32),
    id_turno          Nullable(Int32),
    saidas_entradas   Nullable(Int32),
    total_venda       Nullable(Decimal(18,2)),
    cancelado         UInt8 NOT NULL DEFAULT 0,
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_movprodutos)
SETTINGS index_granularity = 8192;

ALTER TABLE torqmind_current.fact_venda
    ADD COLUMN IF NOT EXISTS data Nullable(DateTime64(6, 'UTC')) AFTER data_key;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_venda_item (
    id_empresa         Int32 NOT NULL,
    id_filial          Int32 NOT NULL,
    id_db              Int32 NOT NULL,
    id_movprodutos     Int32 NOT NULL,
    id_itensmovprodutos Int32 NOT NULL,
    data_key           Int32 NOT NULL DEFAULT 0,
    id_produto         Int32 NOT NULL,
    id_grupo_produto   Nullable(Int32),
    id_local_venda     Nullable(Int32),
    id_funcionario     Nullable(Int32),
    cfop               Nullable(Int32),
    qtd                Nullable(Decimal(18,3)),
    valor_unitario     Nullable(Decimal(18,4)),
    total              Nullable(Decimal(18,2)),
    desconto           Nullable(Decimal(18,2)),
    custo_total        Nullable(Decimal(18,2)),
    margem             Nullable(Decimal(18,2)),
    is_deleted         UInt8 NOT NULL DEFAULT 0,
    source_ts_ms       Int64 NOT NULL,
    ingested_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at         DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at         DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_comprovante (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_db             Int32 NOT NULL,
    id_comprovante    Int32 NOT NULL,
    data_key          Int32 NOT NULL DEFAULT 0,
    id_usuario        Nullable(Int32),
    id_turno          Nullable(Int32),
    id_cliente        Nullable(Int32),
    valor_total       Nullable(Decimal(18,2)),
    cancelado         UInt8 NOT NULL DEFAULT 0,
    situacao          Nullable(Int32),
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_comprovante)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_pagamento_comprovante (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    referencia        Int64 NOT NULL,
    id_db             Nullable(Int32),
    id_comprovante    Nullable(Int32),
    id_turno          Nullable(Int32),
    id_usuario        Nullable(Int32),
    tipo_forma        Int32 NOT NULL,
    valor             Decimal(18,2) NOT NULL DEFAULT 0,
    dt_evento         DateTime64(6, 'UTC') NOT NULL,
    data_key          Int32 NOT NULL,
    nsu               Nullable(String),
    autorizacao       Nullable(String),
    bandeira          Nullable(String),
    rede              Nullable(String),
    tef               Nullable(String),
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, referencia, tipo_forma)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_caixa_turno (
    id_empresa              Int32 NOT NULL,
    id_filial               Int32 NOT NULL,
    id_turno                Int32 NOT NULL,
    id_db                   Nullable(Int32),
    id_usuario              Nullable(Int32),
    abertura_ts             Nullable(DateTime64(6, 'UTC')),
    fechamento_ts           Nullable(DateTime64(6, 'UTC')),
    data_key_abertura       Nullable(Int32),
    data_key_fechamento     Nullable(Int32),
    encerrante_fechamento   Nullable(Int32),
    is_aberto               UInt8 NOT NULL DEFAULT 0,
    status_raw              Nullable(String),
    is_deleted              UInt8 NOT NULL DEFAULT 0,
    source_ts_ms            Int64 NOT NULL,
    ingested_at             DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at              DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_turno)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_financeiro (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_db             Int32 NOT NULL,
    tipo_titulo       Int32 NOT NULL,
    id_titulo         Int32 NOT NULL,
    id_entidade       Nullable(Int32),
    data_emissao      Nullable(Date),
    data_key_emissao  Nullable(Int32),
    vencimento        Nullable(Date),
    data_key_venc     Nullable(Int32),
    data_pagamento    Nullable(Date),
    data_key_pgto     Nullable(Int32),
    valor             Nullable(Decimal(18,2)),
    valor_pago        Nullable(Decimal(18,2)),
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, tipo_titulo, id_titulo)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_risco_evento (
    id                Int64 NOT NULL,
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    event_type        LowCardinality(String) NOT NULL,
    source            LowCardinality(String) NOT NULL DEFAULT 'DW',
    id_db             Nullable(Int32),
    id_comprovante    Nullable(Int32),
    id_movprodutos    Nullable(Int32),
    id_usuario        Nullable(Int32),
    id_funcionario    Nullable(Int32),
    id_turno          Nullable(Int32),
    id_cliente        Nullable(Int32),
    valor_total       Nullable(Decimal(18,2)),
    impacto_estimado  Decimal(18,2) NOT NULL DEFAULT 0,
    score_risco       Int32 NOT NULL,
    score_level       LowCardinality(String) NOT NULL,
    reasons           String NOT NULL DEFAULT '{}',
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    created_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id)
SETTINGS index_granularity = 8192;

-- ============================================================
-- APP / CONFIG
-- ============================================================

CREATE TABLE IF NOT EXISTS torqmind_current.payment_type_map (
    id                Int64 NOT NULL,
    id_empresa        Nullable(Int32),
    tipo_forma        Int32 NOT NULL,
    label             String NOT NULL,
    category          String NOT NULL,
    severity_hint     LowCardinality(String) NOT NULL DEFAULT 'INFO',
    active            UInt8 NOT NULL DEFAULT 1,
    is_deleted        UInt8 NOT NULL DEFAULT 0,
    source_ts_ms      Int64 NOT NULL,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id)
SETTINGS index_granularity = 8192;
