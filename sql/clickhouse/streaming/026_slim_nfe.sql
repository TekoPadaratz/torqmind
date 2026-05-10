-- 026_slim_nfe.sql
-- Slim typed NFE table for fiscal classification in mart queries.
-- Populated by MartBuilder from stg.nfe via postgresql() or CDC.
-- No payload column = small row size, safe FINAL on 8 GB RAM.

CREATE TABLE IF NOT EXISTS torqmind_current.stg_nfe_slim (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    id_db               Int32 NOT NULL,
    id_comprovante      Int32 NOT NULL,
    id_nfe              Int32 NOT NULL,
    status              Int16 NOT NULL DEFAULT 0,   -- 3=authorized, 4=cancelled, 5=voided
    numero_nfe          String NOT NULL DEFAULT '',
    serie               String NOT NULL DEFAULT '',
    chave_nfe           String NOT NULL DEFAULT '',
    modelo              String NOT NULL DEFAULT '',
    data_emissao        Nullable(DateTime64(6, 'America/Sao_Paulo')),
    valor_nfe           Decimal(18,2) NOT NULL DEFAULT 0,
    is_deleted          UInt8 NOT NULL DEFAULT 0,
    source_ts_ms        Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_db, id_comprovante, id_nfe)
SETTINGS index_granularity = 8192;
