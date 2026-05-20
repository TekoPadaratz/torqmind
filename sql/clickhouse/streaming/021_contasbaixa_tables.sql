-- ClickHouse tables for partial payment tracking (baixas parciais)

CREATE TABLE IF NOT EXISTS torqmind_current.stg_contasreceberbaixa (
    id_empresa            Int32 NOT NULL,
    id_filial             Int32 NOT NULL,
    id_db                 Int32 NOT NULL,
    id_contasreceberbaixa Int32 NOT NULL,
    payload               String NOT NULL DEFAULT '{}',
    ingested_at           Nullable(DateTime64(6, 'UTC')),
    dt_evento             Nullable(DateTime64(6, 'UTC')),
    id_db_shadow          Nullable(Int64),
    id_chave_natural      Nullable(String),
    received_at           Nullable(DateTime64(6, 'UTC')),
    is_deleted            UInt8 NOT NULL DEFAULT 0,
    source_ts_ms          Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_contasreceberbaixa)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.stg_contaspagarbaixa (
    id_empresa            Int32 NOT NULL,
    id_filial             Int32 NOT NULL,
    id_db                 Int32 NOT NULL,
    id_contaspagarbaixa   Int32 NOT NULL,
    payload               String NOT NULL DEFAULT '{}',
    ingested_at           Nullable(DateTime64(6, 'UTC')),
    dt_evento             Nullable(DateTime64(6, 'UTC')),
    id_db_shadow          Nullable(Int64),
    id_chave_natural      Nullable(String),
    received_at           Nullable(DateTime64(6, 'UTC')),
    is_deleted            UInt8 NOT NULL DEFAULT 0,
    source_ts_ms          Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_contaspagarbaixa)
SETTINGS index_granularity = 8192;
