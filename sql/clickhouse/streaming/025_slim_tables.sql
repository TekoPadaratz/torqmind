-- 025_slim_tables.sql
-- Pre-materialized typed tables without payload for low-memory mart queries.
-- These are populated by the CDC consumer / MartBuilder before mart aggregation.
-- No payload column = 20-30x smaller row size, safe with FINAL on 8 GB RAM.

CREATE TABLE IF NOT EXISTS torqmind_current.stg_comprovantes_slim (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    id_db               Int32 NOT NULL,
    id_comprovante      Int32 NOT NULL,
    data_key            Int32 NOT NULL,
    hora                UInt8 NOT NULL DEFAULT 0,
    dt_evento_local     DateTime64(6, 'America/Sao_Paulo') NOT NULL DEFAULT '1970-01-01 00:00:00',
    valor_total         Decimal(18,2) NOT NULL DEFAULT 0,
    cancelado           UInt8 NOT NULL DEFAULT 0,
    situacao            Int32 NOT NULL DEFAULT 0,
    id_turno            Int32 NOT NULL DEFAULT 0,
    id_usuario          Int32 NOT NULL DEFAULT 0,
    id_cliente          Int32 NOT NULL DEFAULT 0,
    referencia          Int64 NOT NULL DEFAULT 0,
    is_deleted          UInt8 NOT NULL DEFAULT 0,
    source_ts_ms        Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_comprovante)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.stg_itenscomprovantes_slim (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    id_db               Int32 NOT NULL,
    id_comprovante      Int32 NOT NULL,
    id_itemcomprovante  Int32 NOT NULL,
    data_key            Int32 NOT NULL,
    id_produto          Int32 NOT NULL DEFAULT 0,
    id_grupo_produto    Int32 NOT NULL DEFAULT 0,
    cfop                Int32 NOT NULL DEFAULT 0,
    qtd                 Decimal(18,3) NOT NULL DEFAULT 0,
    total               Decimal(18,2) NOT NULL DEFAULT 0,
    desconto            Decimal(18,2) NOT NULL DEFAULT 0,
    custo_total         Decimal(18,6) NOT NULL DEFAULT 0,
    is_deleted          UInt8 NOT NULL DEFAULT 0,
    source_ts_ms        Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.stg_formas_pgto_slim (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    id_referencia       Int64 NOT NULL,
    tipo_forma          Int32 NOT NULL,
    data_key            Int32 NOT NULL,
    valor               Decimal(18,2) NOT NULL DEFAULT 0,
    is_deleted          UInt8 NOT NULL DEFAULT 0,
    source_ts_ms        Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_referencia, tipo_forma)
SETTINGS index_granularity = 8192;
