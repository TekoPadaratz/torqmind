-- ClickHouse PRODUÇÃO: snapshots mensais de posição patrimonial (dia 1 00:00 SP).
-- Homolog espelha produção — aplicar este DDL no cluster analytics (172.30.0.9).
--
-- Objetivo: eliminar reconstrução on-the-fly (reverter MOV) para datas retroativas.
-- Consumo: API lê torqmind_mart.solvencia_snapshot_mensal por (empresa, filial, ano_mes).

CREATE DATABASE IF NOT EXISTS torqmind_mart;

CREATE TABLE IF NOT EXISTS torqmind_mart.solvencia_snapshot_mensal
(
    id_empresa Int32,
    id_filial Int32,
    ano_mes Int32,                          -- YYYYMM; posição = dia 1 00:00 America/Sao_Paulo
    as_of_ts DateTime64(3, 'America/Sao_Paulo'),
    ativo_caixa Decimal(18, 2) DEFAULT 0,
    ativo_banco Decimal(18, 2) DEFAULT 0,
    ativo_cartoes Decimal(18, 2) DEFAULT 0,
    ativo_cartoes_credito Decimal(18, 2) DEFAULT 0,
    ativo_cartoes_debito Decimal(18, 2) DEFAULT 0,
    ativo_cheques Decimal(18, 2) DEFAULT 0,
    ativo_aprazo Decimal(18, 2) DEFAULT 0,
    ativo_estoque_loja Decimal(18, 2) DEFAULT 0,
    ativo_estoque_combustivel Decimal(18, 2) DEFAULT 0,
    ativo_estoque Decimal(18, 2) DEFAULT 0,
    qtd_estoque_loja Decimal(18, 4) DEFAULT 0,
    qtd_estoque_combustivel Decimal(18, 4) DEFAULT 0,
    passivo_contas_pagar Decimal(18, 2) DEFAULT 0,
    saldo_bancos_json String DEFAULT '[]',   -- [{id_conta, descricao, saldo}]
    cheques_abertos_json String DEFAULT '[]',
    source LowCardinality(String) DEFAULT 'backfill',
    published_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(published_at)
PARTITION BY intDiv(ano_mes, 100)
ORDER BY (id_empresa, id_filial, ano_mes);

CREATE TABLE IF NOT EXISTS torqmind_ops.solvencia_snapshot_job_log
(
    job_id UUID DEFAULT generateUUIDv4(),
    id_empresa Int32,
    from_ano_mes Int32,
    to_ano_mes Int32,
    status LowCardinality(String),
    rows_written UInt64 DEFAULT 0,
    message String DEFAULT '',
    started_at DateTime64(3) DEFAULT now64(3),
    finished_at Nullable(DateTime64(3))
)
ENGINE = MergeTree
ORDER BY (started_at, id_empresa);
