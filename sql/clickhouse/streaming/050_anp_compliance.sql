-- 050_anp_compliance.sql
-- Compliance ANP: facts + mart de variação de margem (lastro NFe entrada x preço bomba).

CREATE TABLE IF NOT EXISTS torqmind_current.fact_nfe_entrada (
    id_empresa      Int32,
    id_filial       Int32,
    id_db           Int32,
    id_nota         Int32,
    id_item         Int32,
    id_produto      Int32,
    dt_entrada      DateTime64(3, 'America/Sao_Paulo'),
    chave_acesso    String DEFAULT '',
    numero_nota     String DEFAULT '',
    cnpj_emitente   String DEFAULT '',
    nome_emitente   String DEFAULT '',
    custo_unitario  Decimal(18, 6),
    qtd             Decimal(18, 6),
    eh_combustivel  UInt8 DEFAULT 0,
    source_ts_ms    Int64
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_produto, dt_entrada, id_nota, id_item)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_current.fact_preco_bomba (
    id_empresa         Int32,
    id_filial          Int32,
    id_db              Int32,
    id_produto         Int32,
    dt_alteracao       DateTime64(3, 'America/Sao_Paulo'),
    preco_venda        Decimal(18, 4),
    preco_anterior     Decimal(18, 4),
    preco_anterior_ts  DateTime64(3, 'America/Sao_Paulo'),
    id_bico            Nullable(Int32),
    id_evento          Int64,
    nome_produto       String DEFAULT '',
    nome_resumido      LowCardinality(String) DEFAULT '',
    source_ts_ms       Int64
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_produto, dt_alteracao, id_evento)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_anp_compliance (
    id_empresa              Int32,
    id_filial               Int32,
    id_produto              Int32,
    nome_resumido           LowCardinality(String) DEFAULT '',
    nome_produto            String DEFAULT '',
    dt_alteracao_preco      DateTime64(3, 'America/Sao_Paulo'),
    preco_venda_anterior    Decimal(18, 4),
    preco_venda_novo        Decimal(18, 4),
    custo_nfe_anterior      Decimal(18, 6),
    custo_nfe_novo          Decimal(18, 6),
    margem_anterior         Decimal(18, 6),
    margem_nova             Decimal(18, 6),
    variacao_margem_pct     Nullable(Decimal(12, 4)),
    limite_alerta_perc      Decimal(8, 2),
    limite_abusivo_perc     Decimal(8, 2),
    status                  LowCardinality(String),
    chave_nfe_anterior      String DEFAULT '',
    chave_nfe_nova          String DEFAULT '',
    cnpj_emitente_nova      String DEFAULT '',
    numero_nota_nova        String DEFAULT '',
    dt_entrada_nfe_nova     Nullable(DateTime64(3, 'America/Sao_Paulo')),
    origem                  LowCardinality(String) DEFAULT 'nfe_asof',
    published_at            DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_produto, dt_alteracao_preco)
PARTITION BY toYYYYMM(dt_alteracao_preco)
SETTINGS index_granularity = 8192;
