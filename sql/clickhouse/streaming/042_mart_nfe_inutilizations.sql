-- 042_mart_nfe_inutilizations.sql
-- Dedicated mart for NFE inutilizations (status=5).
-- Shows voided fiscal documents linked to comprovantes without treating them as real cancellations.
-- Used by Caixa screen for operational/fiscal audit.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.nfe_inutilizations_rt (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    filial_nome         String NOT NULL DEFAULT '',
    id_db               Int32 NOT NULL,
    id_comprovante      Int32 NOT NULL,
    data_key            Int32 NOT NULL,
    dt                  Date NOT NULL,
    hora                UInt8 NOT NULL DEFAULT 0,
    id_turno            Int32 NOT NULL DEFAULT 0,
    turno_abertura_ts   Nullable(DateTime64(6, 'UTC')),
    turno_fechamento_ts Nullable(DateTime64(6, 'UTC')),
    id_usuario          Int32 NOT NULL DEFAULT 0,
    nome_operador       String NOT NULL DEFAULT '',
    id_nfe              Int32 NOT NULL DEFAULT 0,
    nfe_status          Int16 NOT NULL DEFAULT 5,
    nfe_status_label    LowCardinality(String) NOT NULL DEFAULT 'Inutilizada',
    numero_nfe          String NOT NULL DEFAULT '',
    serie_nfe           String NOT NULL DEFAULT '',
    chave_nfe           String NOT NULL DEFAULT '',
    protocolo           String NOT NULL DEFAULT '',
    modelo_nfe          String NOT NULL DEFAULT '',
    data_emissao_nfe    Nullable(DateTime64(6, 'America/Sao_Paulo')),
    valor_comprovante   Decimal(18,2) NOT NULL DEFAULT 0,
    referencia          Int64 NOT NULL DEFAULT 0,
    published_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, id_comprovante, id_nfe)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;
