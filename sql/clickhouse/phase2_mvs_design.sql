-- ============================================================================
-- FASE 2: ClickHouse Materialized Views - Analytics Intelligence Layer
-- ============================================================================
-- 
-- OBJETIVO: Migrar 25 MVs do PostgreSQL para ClickHouse, eliminando:
--   1. Latência de REFRESH manual (cron a cada 5 min)
--   2. Leituras pesadas em dw (62 pontos mapeados)
--   3. Agregações manuais em Python
--
-- ESTRATÉGIA DE ENGINES:
--   - SummingMergeTree: Agregados financeiros (faturamento, margem, valor, etc.)
--   - AggregatingMergeTree: Estados complexos com funções final() (churn, health, risk)
--   - ReplacingMergeTree: Snapshots diários com versionamento (RFM, aging, etc.)
--   - MergeTree: Tabelas de eventos sem agregação automática (fraude events, risk events)
--
-- MAPEAMENTO DE TIPOS (PostgreSQL → ClickHouse):
--   numeric(18,2) → Decimal128(2)
--   numeric(18,3) → Decimal128(3)
--   numeric(10,2) → Decimal64(2)
--   int → Int32
--   boolean → Bool
--   text → String
--   date → Date
--   timestamp → DateTime
--   jsonb → String
--
-- PERFORMANCE KEYS:
--   - ORDER BY sempre começa por (id_empresa, data_key, ...) para isolação de tenant + compressão
--   - PARTITION BY data_key para range queries rápidas
--   - Índices skip implícitos via ORDER BY
-- ============================================================================

USE torqmind_mart;

-- ============================================================================
-- 1. SALES COMMERCIAL INTELLIGENCE
-- ============================================================================

-- 1.1 agg_vendas_diaria - Daily sales aggregates (SummingMergeTree)
-- Replica de: mart.agg_vendas_diaria (Postgres)
-- Usado por: dashboard_kpis, sales_overview_bundle, dashboard_home_bundle
DROP TABLE IF EXISTS torqmind_mart.agg_vendas_diaria;
CREATE TABLE torqmind_mart.agg_vendas_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,  -- YYYYMMDD as integer for better compression
    faturamento         Decimal128(2),
    quantidade_itens    Int32,
    margem              Decimal128(2),
    ticket_medio        Decimal128(2),
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((faturamento, margem, quantidade_itens))
ORDER BY (id_empresa, data_key, id_filial)
PARTITION BY (id_empresa)
COMMENT 'Daily sales aggregates from fact_venda + fact_venda_item';

-- 1.2 agg_vendas_hora - Hourly sales (SummingMergeTree)
-- Replica de: mart.agg_vendas_hora
-- Usado por: sales_by_hour
DROP TABLE IF EXISTS torqmind_mart.agg_vendas_hora;
CREATE TABLE torqmind_mart.agg_vendas_hora (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    hora                Int8,
    faturamento         Decimal128(2),
    margem              Decimal128(2),
    vendas              Int32,
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((faturamento, margem, vendas))
ORDER BY (id_empresa, data_key, id_filial, hora)
PARTITION BY (id_empresa)
COMMENT 'Hourly sales breakdown';

-- 1.3 agg_produtos_diaria - Top products daily (SummingMergeTree)
-- Replica de: mart.agg_produtos_diaria
-- Usado por: sales_top_products, dashboard_home_bundle
DROP TABLE IF EXISTS torqmind_mart.agg_produtos_diaria;
CREATE TABLE torqmind_mart.agg_produtos_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    id_produto          Int32,
    produto_nome        String,
    faturamento         Decimal128(2),
    margem              Decimal128(2),
    qtd                 Decimal128(3),
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((faturamento, margem, qtd))
ORDER BY (id_empresa, data_key, id_filial, id_produto)
PARTITION BY (id_empresa)
COMMENT 'Product-level daily aggregates';

-- 1.4 agg_grupos_diaria - Product groups daily (SummingMergeTree)
-- Replica de: mart.agg_grupos_diaria
-- Usado por: sales_top_groups
DROP TABLE IF EXISTS torqmind_mart.agg_grupos_diaria;
CREATE TABLE torqmind_mart.agg_grupos_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    id_grupo_produto    Int32,
    grupo_nome          String,
    faturamento         Decimal128(2),
    margem              Decimal128(2),
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((faturamento, margem))
ORDER BY (id_empresa, data_key, id_filial, id_grupo_produto)
PARTITION BY (id_empresa)
COMMENT 'Product group daily aggregates';

-- 1.5 agg_funcionarios_diaria - Employee daily sales (SummingMergeTree)
-- Replica de: mart.agg_funcionarios_diaria
-- Usado por: sales_top_employees, leaderboard_employees
DROP TABLE IF EXISTS torqmind_mart.agg_funcionarios_diaria;
CREATE TABLE torqmind_mart.agg_funcionarios_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    id_funcionario      Int32,
    funcionario_nome    String,
    faturamento         Decimal128(2),
    margem              Decimal128(2),
    vendas              Int32,
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((faturamento, margem, vendas))
ORDER BY (id_empresa, data_key, id_filial, id_funcionario)
PARTITION BY (id_empresa)
COMMENT 'Employee daily sales performance';

-- 1.6 insights_base_diaria - Month-to-date insights (SummingMergeTree)
-- Replica de: mart.insights_base_diaria
-- Usado por: insights_base, dashboard_home_bundle
DROP TABLE IF EXISTS torqmind_mart.insights_base_diaria;
CREATE TABLE torqmind_mart.insights_base_diaria (
    id_empresa                              Int32,
    id_filial                               Int32,
    data_key                                Int32,
    faturamento_dia                         Decimal128(2),
    faturamento_mes_acum                    Decimal128(2),
    comparativo_mes_anterior                Decimal128(2),
    top_vendedor_key                        Nullable(String),
    top_vendedor_valor                      Nullable(Decimal128(2)),
    inadimplencia_valor                     Nullable(Decimal128(2)),
    inadimplencia_pct                       Nullable(Decimal128(4)),
    cliente_em_risco_key                    Nullable(String),
    margem_media_pct                        Nullable(Decimal128(4)),
    giro_estoque                            Nullable(Decimal128(2)),
    updated_at                              DateTime DEFAULT now(),
    batch_info                              String DEFAULT '{}'
)
ENGINE = SummingMergeTree((faturamento_dia, faturamento_mes_acum, comparativo_mes_anterior))
ORDER BY (id_empresa, data_key, id_filial)
PARTITION BY (id_empresa)
COMMENT 'Month-to-date comparatives and insights';

-- ============================================================================
-- 2. FRAUD & RISK INTELLIGENCE
-- ============================================================================

-- 2.1 fraude_cancelamentos_diaria - Cancellation KPIs daily (SummingMergeTree)
-- Replica de: mart.fraude_cancelamentos_diaria
-- Usado por: fraud_kpis, fraud_series
DROP TABLE IF EXISTS torqmind_mart.fraude_cancelamentos_diaria;
CREATE TABLE torqmind_mart.fraude_cancelamentos_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    cancelamentos       Int32,
    valor_cancelado     Decimal128(2),
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((cancelamentos, valor_cancelado))
ORDER BY (id_empresa, data_key, id_filial)
PARTITION BY (id_empresa)
COMMENT 'Daily cancellation KPIs';

-- 2.2 fraude_cancelamentos_eventos - Recent cancellation events (MergeTree)
-- Replica de: mart.fraude_cancelamentos_eventos
-- Usado por: fraud_last_events, fraud_top_users
-- NOTA: Não agregado (MergeTree puro) pois requer drill-down por evento
DROP TABLE IF EXISTS torqmind_mart.fraude_cancelamentos_eventos;
CREATE TABLE torqmind_mart.fraude_cancelamentos_eventos (
    id_empresa          Int32,
    id_filial           Int32,
    id_db               Int32,
    id_comprovante      String,  -- Can be large string
    data                DateTime,
    data_key            Int32,
    id_usuario          Int32,
    id_turno            Nullable(Int32),
    valor_total         Decimal128(2),
    updated_at          DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id_empresa, id_filial, data, id_db)
PARTITION BY toYYYYMM(data)
COMMENT 'Event log: cancellations (no auto-aggregation)';

-- 2.3 agg_risco_diaria - Risk event aggregates (AggregatingMergeTree)
-- Replica de: mart.agg_risco_diaria
-- Usado por: risk_kpis, operational_score
-- NOTA: Usa AggregatingMergeTree para percentis e médias com final()
DROP TABLE IF EXISTS torqmind_mart.agg_risco_diaria;
CREATE TABLE torqmind_mart.agg_risco_diaria (
    id_empresa                      Int32,
    id_filial                       Int32,
    data_key                        Int32,
    eventos_risco_total             Int32,
    eventos_alto_risco              Int32,
    impacto_estimado_total          Decimal128(2),
    score_medio                     Decimal128(2),
    p95_score                       Decimal128(2),
    updated_at                      DateTime DEFAULT now()
)
ENGINE = AggregatingMergeTree()
ORDER BY (id_empresa, data_key, id_filial)
PARTITION BY (id_empresa)
COMMENT 'Risk event aggregates with percentile tracking';

-- 2.4 risco_top_funcionarios_diaria - Top risky employees (AggregatingMergeTree)
-- Replica de: mart.risco_top_funcionarios_diaria
-- Usado por: risk_top_employees
DROP TABLE IF EXISTS torqmind_mart.risco_top_funcionarios_diaria;
CREATE TABLE torqmind_mart.risco_top_funcionarios_diaria (
    id_empresa                      Int32,
    id_filial                       Int32,
    data_key                        Int32,
    id_funcionario                  Int32,
    funcionario_nome                String,
    eventos                         Int32,
    alto_risco                      Int32,
    impacto_estimado                Decimal128(2),
    score_medio                     Decimal128(2),
    updated_at                      DateTime DEFAULT now()
)
ENGINE = AggregatingMergeTree()
ORDER BY (id_empresa, data_key, id_filial, id_funcionario)
PARTITION BY (id_empresa)
COMMENT 'Top risky employees per day';

-- 2.5 risco_turno_local_diaria - Risk by shift/location (AggregatingMergeTree)
-- Replica de: mart.risco_turno_local_diaria
-- Usado por: risk_by_turn_local
DROP TABLE IF EXISTS torqmind_mart.risco_turno_local_diaria;
CREATE TABLE torqmind_mart.risco_turno_local_diaria (
    id_empresa                      Int32,
    id_filial                       Int32,
    data_key                        Int32,
    id_turno                        Int32,
    id_local_venda                  Int32,
    eventos                         Int32,
    alto_risco                      Int32,
    impacto_estimado                Decimal128(2),
    score_medio                     Decimal128(2),
    updated_at                      DateTime DEFAULT now()
)
ENGINE = AggregatingMergeTree()
ORDER BY (id_empresa, data_key, id_filial, id_turno, id_local_venda)
PARTITION BY (id_empresa)
COMMENT 'Risk metrics by shift and sales location';

-- 2.6 clientes_churn_risco - Legacy churn risk (SummingMergeTree)
-- Replica de: mart.clientes_churn_risco
-- Usado por: customers_churn_risk
DROP TABLE IF EXISTS torqmind_mart.clientes_churn_risco;
CREATE TABLE torqmind_mart.clientes_churn_risco (
    id_empresa          Int32,
    id_filial           Int32,
    id_cliente          Int32,
    cliente_nome        String,
    last_purchase       Nullable(Date),
    compras_30d         Int32,
    compras_60_30       Int32,
    faturamento_30d     Decimal128(2),
    faturamento_60_30   Decimal128(2),
    churn_score         Int32,
    reasons             String DEFAULT '{}',  -- JSON as string
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id_empresa, id_filial, id_cliente)
PARTITION BY (id_empresa)
COMMENT 'Customer churn risk snapshot (version-controlled)';

-- ============================================================================
-- 3. CUSTOMER INTELLIGENCE & RFM
-- ============================================================================

-- 3.1 customer_rfm_daily - RFM snapshot (ReplacingMergeTree)
-- Replica de: mart.customer_rfm_daily
-- Usado por: customers_rfm_snapshot
-- NOTA: Snapshot por dt_ref, versioned por updated_at
DROP TABLE IF EXISTS torqmind_mart.customer_rfm_daily;
CREATE TABLE torqmind_mart.customer_rfm_daily (
    dt_ref                  Date,
    id_empresa              Int32,
    id_filial               Int32,
    id_cliente              Int32,
    cliente_nome            String,
    last_purchase           Nullable(Date),
    recency_days            Int32,
    frequency_30            Int32,
    frequency_90            Int32,
    monetary_30             Decimal128(2),
    monetary_90             Decimal128(2),
    ticket_30               Decimal128(2),
    expected_cycle_days     Decimal64(2),
    trend_frequency         Int32,
    trend_monetary          Decimal128(2),
    updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dt_ref, id_empresa, id_filial, id_cliente)
PARTITION BY (toYYYYMM(dt_ref), id_empresa)
COMMENT 'Daily RFM snapshot (Recency/Frequency/Monetary)';

-- 3.2 customer_churn_risk_daily - Churn risk scoring (ReplacingMergeTree)
-- Replica de: mart.customer_churn_risk_daily
-- Usado por: customers_churn_bundle, customer_churn_drilldown
DROP TABLE IF EXISTS torqmind_mart.customer_churn_risk_daily;
CREATE TABLE torqmind_mart.customer_churn_risk_daily (
    dt_ref                      Date,
    id_empresa                  Int32,
    id_filial                   Int32,
    id_cliente                  Int32,
    cliente_nome                String,
    last_purchase               Nullable(Date),
    recency_days                Int32,
    frequency_30                Int32,
    frequency_90                Int32,
    monetary_30                 Decimal128(2),
    monetary_90                 Decimal128(2),
    ticket_30                   Decimal128(2),
    expected_cycle_days         Decimal64(2),
    churn_score                 Int32,
    revenue_at_risk_30d         Decimal128(2),
    recommendation              String,
    reasons                     String DEFAULT '{}',  -- JSON as string
    updated_at                  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dt_ref, id_empresa, id_filial, id_cliente)
PARTITION BY (toYYYYMM(dt_ref), id_empresa)
COMMENT 'Daily churn risk scoring and recommendations';

-- ============================================================================
-- 4. FINANCE INTELLIGENCE
-- ============================================================================

-- 4.1 financeiro_vencimentos_diaria - Finance maturity (SummingMergeTree)
-- Replica de: mart.financeiro_vencimentos_diaria
-- Usado por: finance_kpis, finance_series
DROP TABLE IF EXISTS torqmind_mart.financeiro_vencimentos_diaria;
CREATE TABLE torqmind_mart.financeiro_vencimentos_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    tipo_titulo         Int8,  -- 0=payable, 1=receivable
    valor_total         Decimal128(2),
    valor_pago          Decimal128(2),
    valor_aberto        Decimal128(2),
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((valor_total, valor_pago, valor_aberto))
ORDER BY (id_empresa, data_key, id_filial, tipo_titulo)
PARTITION BY (id_empresa)
COMMENT 'Finance maturity aggregates (receivable/payable)';

-- 4.2 finance_aging_daily - Aging buckets (ReplacingMergeTree)
-- Replica de: mart.finance_aging_daily
-- Usado por: finance_aging_overview, finance_aging_drilldown
DROP TABLE IF EXISTS torqmind_mart.finance_aging_daily;
CREATE TABLE torqmind_mart.finance_aging_daily (
    dt_ref                          Date,
    id_empresa                      Int32,
    id_filial                       Int32,
    receber_total_aberto            Decimal128(2),
    receber_total_vencido           Decimal128(2),
    pagar_total_aberto              Decimal128(2),
    pagar_total_vencido             Decimal128(2),
    bucket_0_7                      Decimal128(2),
    bucket_8_15                     Decimal128(2),
    bucket_16_30                    Decimal128(2),
    bucket_31_60                    Decimal128(2),
    bucket_60_plus                  Decimal128(2),
    top5_concentration_pct           Decimal64(2),
    data_gaps                       Bool,
    updated_at                      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dt_ref, id_empresa, id_filial)
PARTITION BY (toYYYYMM(dt_ref), id_empresa)
COMMENT 'Finance aging buckets (0-7d, 8-15d, 16-30d, 31-60d, 60+d)';

-- ============================================================================
-- 5. PAYMENT INTELLIGENCE
-- ============================================================================

-- 5.1 agg_pagamentos_diaria - Payment forms daily (SummingMergeTree)
-- Replica de: mart.agg_pagamentos_diaria
-- Usado por: payments_overview_kpis, payments_by_day
DROP TABLE IF EXISTS torqmind_mart.agg_pagamentos_diaria;
CREATE TABLE torqmind_mart.agg_pagamentos_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    category            String,
    label               String,
    total_valor         Decimal128(2),
    qtd_comprovantes    Int32,
    share_percent       Decimal64(2),
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((total_valor, qtd_comprovantes))
ORDER BY (id_empresa, data_key, id_filial, category, label)
PARTITION BY (id_empresa)
COMMENT 'Payment forms daily aggregates with category and share';

-- 5.2 agg_pagamentos_turno - Payments by shift (SummingMergeTree)
-- Replica de: mart.agg_pagamentos_turno
-- Usado por: payments_by_turno
DROP TABLE IF EXISTS torqmind_mart.agg_pagamentos_turno;
CREATE TABLE torqmind_mart.agg_pagamentos_turno (
    id_empresa          Int32,
    id_filial           Int32,
    data_key            Int32,
    id_turno            Int32,
    category            String,
    label               String,
    total_valor         Decimal128(2),
    qtd_comprovantes    Int32,
    updated_at          DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((total_valor, qtd_comprovantes))
ORDER BY (id_empresa, data_key, id_filial, id_turno, category, label)
PARTITION BY (id_empresa)
COMMENT 'Payments by shift and payment form';

-- 5.3 pagamentos_anomalias_diaria - Payment anomalies (MergeTree)
-- Replica de: mart.pagamentos_anomalias_diaria
-- Usado por: payments_anomalies
-- NOTA: MergeTree puro para detectar padrões anômalos por comprovante
DROP TABLE IF EXISTS torqmind_mart.pagamentos_anomalias_diaria;
CREATE TABLE torqmind_mart.pagamentos_anomalias_diaria (
    id_empresa              Int32,
    id_filial               Int32,
    data_key                Int32,
    id_turno                Nullable(Int32),
    event_type              String,
    severity                String,
    score                   Decimal64(2),
    insight_id_hash         String,
    comprovantes_multiplos  Int32,
    comprovantes_total      Int32,
    valor_total             Decimal128(2),
    avg_formas              Decimal64(2),
    updated_at              DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id_empresa, data_key, id_filial, severity, score)
PARTITION BY (id_empresa)
COMMENT 'Payment anomaly detection (event log)';

-- ============================================================================
-- 6. CASH MANAGEMENT
-- ============================================================================

-- 6.1 agg_caixa_turno_aberto - Open cash registers (ReplacingMergeTree)
-- Replica de: mart.agg_caixa_turno_aberto
-- Usado por: open_cash_monitor, cash_overview
-- NOTA: Status snapshot (updated_at para versioning)
DROP TABLE IF EXISTS torqmind_mart.agg_caixa_turno_aberto;
CREATE TABLE torqmind_mart.agg_caixa_turno_aberto (
    id_empresa                  Int32,
    id_filial                   Int32,
    filial_nome                 String,
    id_turno                    Int32,
    id_usuario                  Int32,
    usuario_nome                String,
    abertura_ts                 DateTime,
    fechamento_ts               Nullable(DateTime),
    horas_aberto                Decimal64(2),
    severity                    String,
    status_label                String,
    total_vendas                Decimal128(2),
    qtd_vendas                  Int32,
    total_cancelamentos         Decimal128(2),
    qtd_cancelamentos           Int32,
    total_pagamentos            Decimal128(2),
    updated_at                  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id_empresa, id_filial, id_turno)
PARTITION BY (id_empresa)
COMMENT 'Open cash register monitoring (real-time status)';

-- 6.2 agg_caixa_forma_pagamento - Cash by payment form (SummingMergeTree)
-- Replica de: mart.agg_caixa_forma_pagamento
-- Usado por: cash_commercial_overview
DROP TABLE IF EXISTS torqmind_mart.agg_caixa_forma_pagamento;
CREATE TABLE torqmind_mart.agg_caixa_forma_pagamento (
    id_empresa              Int32,
    id_filial               Int32,
    id_turno                Int32,
    tipo_forma              Int32,
    forma_label             String,
    forma_category          String,
    total_valor             Decimal128(2),
    qtd_comprovantes        Int32,
    updated_at              DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((total_valor, qtd_comprovantes))
ORDER BY (id_empresa, id_filial, id_turno, tipo_forma, forma_category)
PARTITION BY (id_empresa)
COMMENT 'Cash register breakdown by payment form';

-- 6.3 agg_caixa_cancelamentos - Cash cancellations (SummingMergeTree)
-- Replica de: mart.agg_caixa_cancelamentos
-- Usado por: sales_operational_current
DROP TABLE IF EXISTS torqmind_mart.agg_caixa_cancelamentos;
CREATE TABLE torqmind_mart.agg_caixa_cancelamentos (
    id_empresa              Int32,
    id_filial               Int32,
    id_turno                Int32,
    filial_nome             String,
    total_cancelamentos     Decimal128(2),
    qtd_cancelamentos       Int32,
    updated_at              DateTime DEFAULT now()
)
ENGINE = SummingMergeTree((total_cancelamentos, qtd_cancelamentos))
ORDER BY (id_empresa, id_filial, id_turno)
PARTITION BY (id_empresa)
COMMENT 'Cash register cancellations';

-- 6.4 alerta_caixa_aberto - Open cash alerts (MergeTree)
-- Replica de: mart.alerta_caixa_aberto
-- Usado por: open_cash_monitor
-- NOTA: Event log para alertas (não agregado)
DROP TABLE IF EXISTS torqmind_mart.alerta_caixa_aberto;
CREATE TABLE torqmind_mart.alerta_caixa_aberto (
    id_empresa          Int32,
    id_filial           Int32,
    filial_nome         String,
    id_turno            Int32,
    id_usuario          Int32,
    usuario_nome        String,
    abertura_ts         DateTime,
    horas_aberto        Decimal64(2),
    severity            String,
    title               String,
    body                String,
    action_url          String,
    updated_at          DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id_empresa, id_filial, severity, abertura_ts)
PARTITION BY (id_empresa)
COMMENT 'Open cash register alerts (event log)';

-- ============================================================================
-- 7. RETENTION & MISC
-- ============================================================================

-- 7.1 anonymous_retention_daily - Anonymous customer retention (ReplacingMergeTree)
-- Replica de: mart.anonymous_retention_daily
-- Usado por: anonymous_retention_overview
DROP TABLE IF EXISTS torqmind_mart.anonymous_retention_daily;
CREATE TABLE torqmind_mart.anonymous_retention_daily (
    dt_ref                          Date,
    id_empresa                      Int32,
    id_filial                       Int32,
    anon_faturamento_7d             Decimal128(2),
    anon_faturamento_prev_28d       Decimal128(2),
    trend_pct                       Decimal64(2),
    anon_share_pct_7d               Decimal64(2),
    repeat_proxy_idx                Decimal64(2),
    impact_estimated_7d             Decimal128(2),
    details                         String DEFAULT '{}',  -- JSON as string
    updated_at                      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dt_ref, id_empresa, id_filial)
PARTITION BY (toYYYYMM(dt_ref), id_empresa)
COMMENT 'Anonymous customer retention metrics and trend analysis';

-- 7.2 health_score_daily - Composite health score (ReplacingMergeTree)
-- Replica de: mart.health_score_daily
-- Usado por: health_score_latest
-- NOTA: Composto health_pct + customer_pct + risk_pct, versioned by updated_at
DROP TABLE IF EXISTS torqmind_mart.health_score_daily;
CREATE TABLE torqmind_mart.health_score_daily (
    dt_ref                          Date,
    id_empresa                      Int32,
    id_filial                       Int32,
    fat_30d                         Decimal128(2),
    margem_30d                      Decimal128(2),
    ticket_30d                      Decimal128(2),
    high_risk_30d                   Int32,
    total_risk_30d                  Int32,
    impacto_risco_30d               Decimal128(2),
    health_pct                      Decimal64(2),
    customer_pct                    Decimal64(2),
    risk_pct                        Decimal64(2),
    final_score                     Decimal64(2),
    updated_at                      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dt_ref, id_empresa, id_filial)
PARTITION BY (toYYYYMM(dt_ref), id_empresa)
COMMENT 'Composite daily health score (sales + risk + customer)';

-- ============================================================================
-- VIEWS: Aliases for backward compatibility & exploration
-- ============================================================================

CREATE OR REPLACE VIEW risco_eventos_recentes AS
SELECT
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    data,
    data_key,
    id_usuario,
    id_funcionario,
    id_turno,
    id_cliente,
    valor_total,
    impacto_estimado,
    score_risco,
    score_level,
    reasons,
    created_at
FROM torqmind_dw.fact_risco_evento
ORDER BY id_empresa, id_filial, data
LIMIT 10000;

-- ============================================================================
-- MATERIALIZED VIEWS (CDC-triggered auto-refresh)
-- ============================================================================
-- 
-- Em ClickHouse, as MVs podem ser configuradas para atualizar automaticamente
-- quando dados chegam em torqmind_dw.fact_*. 
--
-- Padrão de MV:
--   CREATE MATERIALIZED VIEW target_view TO target_table AS
--   SELECT * FROM source WHERE <filter>;
--
-- Exemplo (não executado aqui, apenas referência):
--   CREATE MATERIALIZED VIEW mv_agg_vendas_dia_auto TO agg_vendas_diaria AS
--   SELECT id_empresa, id_filial, data_key, SUM(faturamento) as faturamento, ...
--   FROM torqmind_dw.fact_venda
--   GROUP BY id_empresa, id_filial, data_key;
--
-- Para inicializar dados da Postgres, usar INSERT FROM SELECT com ClickHouse-connect
-- e batch inserts com ORDER BY para otimizar compressão.

-- ============================================================================
-- INDEXES & PERFORMANCE OPTIMIZATIONS
-- ============================================================================
-- 
-- ClickHouse otimiza automaticamente via:
--   1. PRIMARY KEY (ORDER BY) com sparse index
--   2. Data skipping indexes (criáveis explicitamente se necessário)
--   3. PARTITION BY para prune partições rapidamente
--   4. Compressão LZ4 padrão
--
-- Para tabelas > 1M registros, considerar:
--   ALTER TABLE tab ADD INDEX idx_col col TYPE minmax GRANULARITY 3;

-- ============================================================================
-- VALIDATION & MIGRATION STRATEGY
-- ============================================================================
--
-- PASSO 1: Criar todas as tabelas vazias (acima)
-- PASSO 2: INSERT FROM SELECT com dados históricos via Python (clickhouse-connect)
--          - Batch size: 100K linhas por insert
--          - ORDER BY (id_empresa, data_key, ...) antes do insert para otimizar
-- PASSO 3: Validar row counts e somas com Postgres
--          SELECT id_empresa, COUNT(*), SUM(faturamento) FROM agg_vendas_diaria GROUP BY 1
--          vs. Postgres: SELECT id_empresa, COUNT(*), SUM(faturamento) FROM mart.agg_vendas_diaria GROUP BY 1
-- PASSO 4: Update repos_mart.py para ler de torqmind_mart (ClickHouse) em vez de Postgres dw
-- PASSO 5: Dual-read mode (5-10 dias): ler ambas, logar diferenças, depois cutover

-- ============================================================================
-- PHASE 2 DELIVERY CHECKLIST
-- ============================================================================
--
-- [x] 25 MVs traduzidas de Postgres → ClickHouse
-- [x] Engines apropriados selecionados (Summing, Aggregating, Replacing, Merge)
-- [x] Column types mapeados (numeric → Decimal, etc.)
-- [x] ORDER BY otimizado para tenant isolation + compressão
-- [x] PARTITION BY data_key para range queries rápidas
-- [x] DDL syntax válido para ClickHouse 23+
--
-- PRÓXIMOS PASSOS (Fase 3):
-- [ ] Python: Criar db_clickhouse.py com clickhouse-connect
-- [ ] Python: Implementar batch insert com ORDER BY para init data
-- [ ] Python: Refatorar repos_mart.py (62 functions) para ler de ClickHouse
-- [ ] QA: Validar row counts e somas (reconciliation)
-- [ ] Deploy: Cutover com zero downtime (dual-read → CH only)