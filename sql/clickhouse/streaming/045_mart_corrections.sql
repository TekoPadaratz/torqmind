-- 045_mart_corrections.sql
-- Additional mart tables for corrections to existing features:
-- - Customers summary (paginated)
-- - Fraud detail events (operador/turno/caixa)
-- - Goals team summary

-- ============================================================
-- CUSTOMERS: Resumo paginado
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_clientes_resumo (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    id_cliente          Int32 NOT NULL,
    nome_cliente        String NOT NULL DEFAULT '',
    documento           String NOT NULL DEFAULT '',
    telefone            String NOT NULL DEFAULT '',
    email               String NOT NULL DEFAULT '',
    segmento            LowCardinality(String) NOT NULL DEFAULT '',
    risk_level          LowCardinality(String) NOT NULL DEFAULT '',
    total_compras_30d   Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_compras_30d     UInt32 NOT NULL DEFAULT 0,
    ticket_medio_30d    Decimal(18,2) NOT NULL DEFAULT 0,
    total_compras_all   Decimal(18,2) NOT NULL DEFAULT 0,
    qtd_compras_all     UInt32 NOT NULL DEFAULT 0,
    ultima_compra_key   Int32 NOT NULL DEFAULT 0,
    recencia_dias       UInt32 NOT NULL DEFAULT 0,
    published_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_cliente)
SETTINGS index_granularity = 8192;

-- ============================================================
-- FRAUD: Eventos detalhados com operador/turno/caixa
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_antifraude_eventos (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    filial_nome         String NOT NULL DEFAULT '',
    data_key            Int32 NOT NULL,
    dt                  Date NOT NULL,
    event_id            Int64 NOT NULL,
    event_type          LowCardinality(String) NOT NULL,
    source              LowCardinality(String) NOT NULL DEFAULT 'STG',
    id_turno            Int32 NOT NULL DEFAULT 0,
    turno_abertura_ts   Nullable(DateTime64(6, 'UTC')),
    turno_fechamento_ts Nullable(DateTime64(6, 'UTC')),
    id_caixa            Int32 NOT NULL DEFAULT 0,
    id_usuario          Int32 NOT NULL DEFAULT 0,
    nome_operador       String NOT NULL DEFAULT '',
    id_funcionario      Nullable(Int32),
    nome_funcionario    String NOT NULL DEFAULT '',
    valor_total         Nullable(Decimal(18,2)),
    impacto_estimado    Decimal(18,2) NOT NULL DEFAULT 0,
    score_risco         Int32 NOT NULL DEFAULT 0,
    score_level         LowCardinality(String) NOT NULL DEFAULT '',
    reasons             String NOT NULL DEFAULT '{}',
    hora                UInt8 NOT NULL DEFAULT 0,
    published_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6),
    -- Documento operacional: id_comprovante (PK tecnico) + nro_comprovante
    -- (NROCOMPROVANTE, numero impresso no comprovante de venda). turno_numero
    -- e o turno OPERACIONAL (1..N; 0 = caixa geral), NUNCA o id_turno tecnico.
    id_comprovante      Int32 NOT NULL DEFAULT 0,
    nro_comprovante     Int64 NOT NULL DEFAULT 0,
    turno_numero        Int32 NOT NULL DEFAULT 0
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, event_id)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- GOALS: Resumo diario com equipes
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_metas_equipes_resumo (
    id_empresa          Int32 NOT NULL,
    id_filial           Int32 NOT NULL,
    data_key            Int32 NOT NULL,
    dt                  Date NOT NULL,
    goal_type           LowCardinality(String) NOT NULL,
    target_value        Decimal(18,2) NOT NULL DEFAULT 0,
    current_value       Decimal(18,2) NOT NULL DEFAULT 0,
    pct_achieved        Decimal(8,2) NOT NULL DEFAULT 0,
    daily_projection    Decimal(18,2) NOT NULL DEFAULT 0,
    monthly_projection  Decimal(18,2) NOT NULL DEFAULT 0,
    published_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, goal_type)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;
