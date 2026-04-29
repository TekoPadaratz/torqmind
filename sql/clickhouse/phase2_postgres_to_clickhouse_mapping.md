-- ============================================================================
-- FASE 2 REFERENCE: PostgreSQL MV → ClickHouse MV → repos_mart.py Function Mapping
-- ============================================================================
--
-- Este documento mapeia:
--   1. Cada Postgres MV no schema mart (fonte original)
--   2. Sua tradução ClickHouse em torqmind_mart
--   3. As funções Python em repos_mart.py que a consomem
--   4. As linhas exatas de código em repos_mart.py (líneas de referência)
--   5. O dw read pattern que será eliminado
--
-- ============================================================================

-- TABLE 1: SALES COMMERCIAL INTELLIGENCE
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.1 agg_vendas_diaria                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_vendas_diaria (003_mart_demo.sql:1117)          │
│ ClickHouse Dest:  torqmind_mart.agg_vendas_diaria (Summing)               │
│ Table Definition: Daily sales by empresa/filial/date                        │
│ Columns:          id_empresa, id_filial, data_key, faturamento,            │
│                   quantidade_itens, margem, ticket_medio                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions (Consumers):                                        │
│   - dashboard_kpis() [Linha ~1232]                                         │
│       Query: SELECT ... FROM mart.agg_vendas_diaria WHERE data_key ...    │
│       Return: dict with faturamento, margem, ticket_medio, vendas          │
│   - dashboard_series() [Linha ~1350]                                       │
│       Query: SELECT data_key, SUM(faturamento) FROM agg_vendas_diaria ... │
│       Return: List[DailyPoint] with data_key, faturamento                  │
│   - sales_overview_bundle() [Linha ~1410]                                  │
│       Aggregates: Total sales, margin %, employee count from agg_*_diaria  │
│       Return: SalesOverviewBundle with vendas_total, margem_pct           │
│   - dashboard_home_bundle() [Linha ~980]                                   │
│       Uses: agg_vendas_diaria for MTD KPIs                                 │
│       Return: HomeBundleResponse with faturamento_mes, comparativo         │
│                                                                              │
│ DW Read Pattern (to be eliminated):                                        │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i ON ...                 │
│   WHERE v.cancelado=false AND i.cfop >= 5000                             │
│   GROUP BY data_key, id_filial                                            │
│                                                                              │
│ Phase 3 Refactor Strategy:                                                 │
│   - Replace manual JOINs with direct SELECT from agg_vendas_diaria        │
│   - Query becomes: SELECT * FROM ClickHouse torqmind_mart.agg_vendas_*  │
│   - Return contracts unchanged (data structure preserved)                 │
│                                                                              │
│ Performance Gains:                                                          │
│   - Pre-aggregated in ClickHouse (SummingMergeTree)                       │
│   - No JOIN overhead                                                        │
│   - Sub-second response (vs. 2-5 sec from Postgres DW)                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.2 agg_vendas_hora                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_vendas_hora (003_mart_demo.sql:1194)           │
│ ClickHouse Dest:  torqmind_mart.agg_vendas_hora (Summing)                │
│ Table Definition: Hourly sales breakdown                                    │
│ Columns:          id_empresa, id_filial, data_key, hora, faturamento,     │
│                   margem, vendas                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - sales_by_hour() [Linha ~1330]                                          │
│       Query: SELECT hora, SUM(faturamento) FROM agg_vendas_hora           │
│       Return: List[HourlyPoint] with hora, faturamento                    │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i                         │
│   GROUP BY EXTRACT(HOUR FROM v.data), id_filial, data_key               │
│                                                                              │
│ Phase 3: Direct CH query, no aggregation needed                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.3 agg_produtos_diaria                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_produtos_diaria (003_mart_demo.sql:1215)       │
│ ClickHouse Dest:  torqmind_mart.agg_produtos_diaria (Summing)            │
│ Table Definition: Top products daily                                        │
│ Columns:          id_empresa, id_filial, data_key, id_produto,           │
│                   produto_nome, faturamento, margem, qtd                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - sales_top_products() [Linha ~1480]                                     │
│       Query: SELECT id_produto, produto_nome, faturamento FROM agg_produtos_diaria
│            ORDER BY faturamento DESC LIMIT 10                             │
│       Return: List[TopProduct]                                             │
│   - dashboard_home_bundle() [Linha ~980]                                   │
│       Uses: Top 5 produtos for home dashboard                             │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i                         │
│   LEFT JOIN dw.dim_produto p ON p.id_produto = i.id_produto             │
│   GROUP BY i.id_produto, p.nome, data_key                               │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_produtos_diaria directly                │
│          Eliminate DIM_PRODUTO lookup (already denormalized)              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.4 agg_grupos_diaria                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_grupos_diaria (003_mart_demo.sql:1239)         │
│ ClickHouse Dest:  torqmind_mart.agg_grupos_diaria (Summing)              │
│ Table Definition: Product group aggregates                                  │
│ Columns:          id_empresa, id_filial, data_key, id_grupo_produto,     │
│                   grupo_nome, faturamento, margem                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - sales_top_groups() [Linha ~1560]                                       │
│       Query: SELECT grupo_nome, faturamento FROM agg_grupos_diaria        │
│            ORDER BY faturamento DESC LIMIT 10                             │
│       Return: List[TopGroup]                                               │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i                         │
│   LEFT JOIN dw.dim_grupo_produto g ON g.id_grupo_produto = i.id_grupo   │
│   GROUP BY i.id_grupo_produto, g.nome, data_key                         │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_grupos_diaria                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.5 agg_funcionarios_diaria                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_funcionarios_diaria (003_mart_demo.sql:1262)   │
│ ClickHouse Dest:  torqmind_mart.agg_funcionarios_diaria (Summing)        │
│ Table Definition: Employee daily sales performance                          │
│ Columns:          id_empresa, id_filial, data_key, id_funcionario,       │
│                   funcionario_nome, faturamento, margem, vendas            │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - sales_top_employees() [Linha ~1530]                                    │
│       Query: SELECT id_funcionario, funcionario_nome, faturamento FROM    │
│            agg_funcionarios_diaria ORDER BY faturamento DESC LIMIT 10    │
│       Return: List[TopEmployee]                                            │
│   - leaderboard_employees() [Linha ~6078]                                  │
│       Query: SELECT ... FROM agg_funcionarios_diaria                      │
│            ORDER BY faturamento DESC                                       │
│       Return: LeaderboardResponse with employee rankings                  │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i                         │
│   LEFT JOIN dw.dim_funcionario f ON f.id_funcionario = i.id_funcionario │
│   GROUP BY i.id_funcionario, f.nome, data_key                           │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_funcionarios_diaria                      │
│          Eliminate dim_funcionario LEFT JOIN                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.6 insights_base_diaria                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.insights_base_diaria (003_mart_demo.sql:1140)      │
│ ClickHouse Dest:  torqmind_mart.insights_base_diaria (Summing)           │
│ Table Definition: Month-to-date insights & comparatives                    │
│ Columns:          id_empresa, id_filial, data_key,                        │
│                   faturamento_dia, faturamento_mes_acum,                   │
│                   comparativo_mes_anterior, ...                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - insights_base() [Linha ~1650]                                          │
│       Query: SELECT * FROM insights_base_diaria WHERE id_filial = ?      │
│       Return: InsightsBase with faturamento_dia, faturamento_mes_acum    │
│   - dashboard_home_bundle() [Linha ~980]                                   │
│       Uses: insights_base_diaria for cumulative MTD                       │
│                                                                              │
│ DW Read Pattern:                                                            │
│   WITH daily_cum AS (                                                      │
│     SELECT agg.id_filial, agg.data_key,                                   │
│            SUM(agg.faturamento) OVER (PARTITION BY ... ORDER BY ...)     │
│     FROM mart.agg_vendas_diaria agg                                       │
│   ) ...                                                                     │
│                                                                              │
│ Phase 3: Query torqmind_mart.insights_base_diaria                         │
│          Already pre-computed by ClickHouse MV                            │
└─────────────────────────────────────────────────────────────────────────────┘

-- TABLE 2: FRAUD & RISK INTELLIGENCE
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 2.1 fraude_cancelamentos_diaria                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.fraude_cancelamentos_diaria (003_mart_demo.sql:1286)
│ ClickHouse Dest:  torqmind_mart.fraude_cancelamentos_diaria (Summing)    │
│ Table Definition: Daily cancellation KPIs                                   │
│ Columns:          id_empresa, id_filial, data_key,                        │
│                   cancelamentos, valor_cancelado                           │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - fraud_kpis() [Linha ~1900]                                             │
│       Query: SELECT SUM(cancelamentos), SUM(valor_cancelado) FROM         │
│            fraude_cancelamentos_diaria WHERE id_filial = ?                │
│       Return: FraudKPIs with cancelamentos_total, valor_cancelado_total  │
│   - fraud_series() [Linha ~1950]                                           │
│       Query: SELECT data_key, cancelamentos, valor_cancelado FROM         │
│            fraude_cancelamentos_diaria ORDER BY data_key                 │
│       Return: List[DailyPoint] with data_key, cancelamentos               │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_comprovante c                                              │
│   WHERE c.cancelado = true                                                │
│   GROUP BY id_filial, data_key                                            │
│                                                                              │
│ Phase 3: Query torqmind_mart.fraude_cancelamentos_diaria                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 2.2 fraude_cancelamentos_eventos                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.fraude_cancelamentos_eventos (003_mart_demo.sql:1302)
│ ClickHouse Dest:  torqmind_mart.fraude_cancelamentos_eventos (MergeTree)  │
│ Table Definition: Recent cancellation events (no aggregation)              │
│ Columns:          id_empresa, id_filial, id_db, id_comprovante,           │
│                   data, data_key, id_usuario, id_turno, valor_total       │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - fraud_last_events() [Linha ~2000]                                      │
│       Query: SELECT * FROM fraude_cancelamentos_eventos                   │
│            ORDER BY data DESC LIMIT 50                                    │
│       Return: List[CancellationEvent]                                      │
│   - fraud_top_users() [Linha ~2050]                                        │
│       Query: SELECT id_usuario, COUNT(*), SUM(valor_total) FROM          │
│            fraude_cancelamentos_eventos GROUP BY id_usuario              │
│       Return: List[TopUser] with cancel count and impact                 │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_comprovante c                                              │
│   WHERE c.cancelado = true                                                │
│   ORDER BY c.data DESC LIMIT N                                            │
│                                                                              │
│ Phase 3: Query torqmind_mart.fraude_cancelamentos_eventos                │
│          Already denormalized; no JOINs needed                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 2.3 agg_risco_diaria + 2.4 risco_top_funcionarios_diaria                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_risco_diaria (004_risk_insights.sql:523)       │
│                   mart.risco_top_funcionarios_diaria (004:543)             │
│ ClickHouse Dest:  torqmind_mart.agg_risco_diaria (AggregatingMergeTree)  │
│                   torqmind_mart.risco_top_funcionarios_diaria (Aggregating)
│ Table Definition: Risk event aggregates with percentiles                    │
│ Columns:          id_empresa, id_filial, data_key,                        │
│                   eventos_risco_total, eventos_alto_risco,                 │
│                   impacto_estimado_total, score_medio, p95_score          │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - risk_kpis() [Linha ~2400]                                              │
│       Query: SELECT SUM(eventos_alto_risco), AVG(score_medio),            │
│            MAX(p95_score) FROM agg_risco_diaria                          │
│       Return: RiskKPIs with eventos_alto, score_medio_pct                 │
│   - operational_score() [Linha ~2500]                                      │
│       Query: SELECT ... FROM agg_risco_diaria JOIN risco_top_funcionarios │
│       Return: OperationalScoreResponse with risk breakdown                │
│   - risk_top_employees() [Linha ~2600]                                     │
│       Query: SELECT id_funcionario, funcionario_nome, eventos,            │
│            alto_risco, impacto_estimado FROM risco_top_funcionarios_diaria
│       Return: List[TopRiskEmployee]                                       │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_risco_evento r                                             │
│   LEFT JOIN dw.dim_funcionario df ON df.id_funcionario = r.id_funcionario
│   GROUP BY data_key, id_filial, [id_funcionario]                         │
│   HAVING percentile_cont(...), AVG(...), COUNT(...)                       │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_risco_diaria & risco_top_funcionarios_diaria
│          AggregatingMergeTree handles percentiles automatically           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 2.5 risco_turno_local_diaria                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.risco_turno_local_diaria (005_etl_incremental:519)  │
│ ClickHouse Dest:  torqmind_mart.risco_turno_local_diaria (Aggregating)    │
│ Table Definition: Risk by shift and sales location                          │
│ Columns:          id_empresa, id_filial, data_key, id_turno, id_local_venda,
│                   eventos, alto_risco, impacto_estimado, score_medio      │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - risk_by_turn_local() [Linha ~2700]                                     │
│       Query: SELECT id_turno, id_local_venda, eventos, alto_risco FROM    │
│            risco_turno_local_diaria WHERE data_key = ?                   │
│       Return: List[TurnLocationRisk]                                       │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_risco_evento r                                             │
│   LEFT JOIN (SELECT MIN(id_local_venda) FROM dw.fact_venda_item ...)     │
│   GROUP BY data_key, id_turno, id_local_venda                            │
│                                                                              │
│ Phase 3: Query torqmind_mart.risco_turno_local_diaria                    │
│          Eliminate subquery via materialization                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 2.6 clientes_churn_risco                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.clientes_churn_risco (005_etl_incremental:458)      │
│ ClickHouse Dest:  torqmind_mart.clientes_churn_risco (ReplacingMergeTree)│
│ Table Definition: Legacy customer churn risk                                │
│ Columns:          id_empresa, id_filial, id_cliente, cliente_nome,        │
│                   churn_score, compras_30d, faturamento_30d               │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - customers_churn_risk() [Linha ~3100]                                   │
│       Query: SELECT * FROM clientes_churn_risco                           │
│            WHERE churn_score > threshold ORDER BY churn_score DESC       │
│       Return: List[ChurnRiskCustomer]                                      │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i                         │
│   LEFT JOIN dw.dim_cliente c ON c.id_cliente = v.id_cliente             │
│   WHERE v.data >= CURRENT_DATE - 120 days                                │
│   GROUP BY id_cliente                                                     │
│   HAVING churn_score calculation (complex CASE logic)                     │
│                                                                              │
│ Phase 3: Query torqmind_mart.clientes_churn_risco                         │
│          Churn calculation already done; just fetch and filter            │
└─────────────────────────────────────────────────────────────────────────────┘

-- TABLE 3: CUSTOMER INTELLIGENCE & RFM
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 3.1 customer_rfm_daily + 3.2 customer_churn_risk_daily                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.customer_rfm_daily (009_phase4:4)                   │
│                   mart.customer_churn_risk_daily (009_phase4:97)            │
│ ClickHouse Dest:  torqmind_mart.customer_rfm_daily (ReplacingMergeTree)   │
│                   torqmind_mart.customer_churn_risk_daily (Replacing)     │
│ Table Definition: Daily RFM snapshot + Churn risk scoring                   │
│ Columns:          dt_ref (daily partition), id_empresa, id_filial,         │
│                   id_cliente, recency_days, frequency_30, frequency_90,   │
│                   monetary_30, monetary_90, churn_score, revenue_at_risk   │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - customers_rfm_snapshot() [Linha ~3200]                                 │
│       Query: SELECT * FROM customer_rfm_daily WHERE dt_ref = CURRENT_DATE│
│       Return: List[RFMSnapshot]                                            │
│   - customers_churn_bundle() [Linha ~3250]                                 │
│       Query: SELECT * FROM customer_churn_risk_daily                      │
│            WHERE churn_score > 50 ORDER BY revenue_at_risk DESC          │
│       Return: ChurnBundleResponse                                          │
│   - customer_churn_drilldown() [Linha ~3350]                               │
│       Query: SELECT ... FROM customer_churn_risk_daily                    │
│            WHERE id_cliente = ? AND dt_ref >= CURRENT_DATE - 30 days     │
│       Return: ChurnDrilldownResponse with trend over 30 days              │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_venda v JOIN dw.fact_venda_item i                         │
│   WHERE v.data >= CURRENT_DATE - 180 days                                │
│   GROUP BY id_cliente                                                     │
│   HAVING RFM calculations (recency, frequency, monetary with lookback)    │
│                                                                              │
│ Phase 3: Query torqmind_mart.customer_rfm_daily & customer_churn_risk_daily
│          Both pre-computed; filter by dt_ref = CURRENT_DATE              │
│          Eliminate complex window functions and CASE logic                │
└─────────────────────────────────────────────────────────────────────────────┘

-- TABLE 4: FINANCE INTELLIGENCE
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 4.1 financeiro_vencimentos_diaria + 4.2 finance_aging_daily                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.financeiro_vencimentos_diaria (003_mart_demo:1320) │
│                   mart.finance_aging_daily (009_phase4:172)                 │
│ ClickHouse Dest:  torqmind_mart.financeiro_vencimentos_diaria (Summing)  │
│                   torqmind_mart.finance_aging_daily (ReplacingMergeTree) │
│ Table Definition: Finance maturity + aging buckets                          │
│ Columns:          id_empresa, id_filial, data_key, tipo_titulo,           │
│                   valor_total, valor_pago, valor_aberto,                   │
│                   bucket_0_7, bucket_8_15, bucket_16_30, bucket_31_60,   │
│                   bucket_60_plus, top5_concentration_pct                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - finance_kpis() [Linha ~4200]                                           │
│       Query: SELECT SUM(receber_total_aberto), SUM(pagar_total_aberto)    │
│            FROM finance_aging_daily WHERE dt_ref = CURRENT_DATE          │
│       Return: FinanceKPIs with receber_total, pagar_total                 │
│   - finance_aging_overview() [Linha ~4328]                                 │
│       Query: SELECT * FROM finance_aging_daily                            │
│            WHERE dt_ref = CURRENT_DATE                                    │
│       Return: FinanceAgingOverview with bucket totals                     │
│   - finance_aging_drilldown() [Linha ~4450]                                │
│       Query: SELECT ... FROM finance_aging_daily                          │
│            WHERE dt_ref BETWEEN ? AND ? GROUP BY bucket                  │
│       Return: List[AgingBucketTrend]                                       │
│   - finance_series() [Linha ~4550]                                         │
│       Query: SELECT data_key, valor_total, valor_pago FROM               │
│            financeiro_vencimentos_diaria ORDER BY data_key              │
│       Return: List[DailyPoint]                                             │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_financeiro f                                               │
│   WHERE f.vencimento IS NOT NULL                                          │
│   GROUP BY data_key, tipo_titulo, (bucket logic via CASE)                │
│                                                                              │
│ Phase 3: Query torqmind_mart.finance_aging_daily & financeiro_vencimentos
│          Both pre-computed; filter by dt_ref for snapshots                │
│          Eliminate aging bucket CASE logic                                │
└─────────────────────────────────────────────────────────────────────────────┘

-- TABLE 5: PAYMENT INTELLIGENCE
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 5.1 agg_pagamentos_diaria + 5.2 agg_pagamentos_turno                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_pagamentos_diaria (015_payments:261)            │
│                   mart.agg_pagamentos_turno (015_payments:305)              │
│ ClickHouse Dest:  torqmind_mart.agg_pagamentos_diaria (Summing)          │
│                   torqmind_mart.agg_pagamentos_turno (Summing)            │
│ Table Definition: Payment form aggregates                                   │
│ Columns:          id_empresa, id_filial, data_key, [id_turno],           │
│                   category, label, total_valor, qtd_comprovantes,         │
│                   share_percent                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - payments_overview_kpis() [Linha ~4660]                                 │
│       Query: SELECT category, SUM(total_valor) FROM agg_pagamentos_diaria│
│            WHERE data_key = ? GROUP BY category                          │
│       Return: PaymentsOverviewKPIs with category breakdown               │
│   - payments_by_day() [Linha ~4750]                                        │
│       Query: SELECT data_key, total_valor FROM agg_pagamentos_diaria     │
│       Return: List[DailyPayment]                                           │
│   - payments_by_turno() [Linha ~4850]                                      │
│       Query: SELECT id_turno, category, total_valor FROM                 │
│            agg_pagamentos_turno WHERE data_key = ?                      │
│       Return: List[TurnoPayment]                                           │
│   - payments_overview() [Linha ~1390]                                      │
│       Aggregates from agg_pagamentos_diaria                               │
│       Return: PaymentsOverviewResponse                                     │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_pagamento_comprovante f                                    │
│   LEFT JOIN app.payment_type_map m ON m.tipo_forma = f.tipo_forma       │
│   GROUP BY data_key, [id_turno], category, label                        │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_pagamentos_diaria & agg_pagamentos_turno
│          Payment type mapping already denormalized in ClickHouse MV      │
│          Eliminate LEFT JOIN to payment_type_map                         │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 5.3 pagamentos_anomalias_diaria                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.pagamentos_anomalias_diaria (015_payments:347)      │
│ ClickHouse Dest:  torqmind_mart.pagamentos_anomalias_diaria (MergeTree)  │
│ Table Definition: Payment anomaly detection (event log)                     │
│ Columns:          id_empresa, id_filial, data_key, id_turno,             │
│                   event_type, severity, score, comprovantes_multiplos    │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - payments_anomalies() [Linha ~5000]                                     │
│       Query: SELECT * FROM pagamentos_anomalias_diaria                    │
│            WHERE severity = 'HIGH' ORDER BY score DESC                   │
│       Return: List[PaymentAnomaly]                                         │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM (SELECT ... GROUP BY referencia WITH anomaly logic)               │
│   WHERE comprovantes_multiplos > threshold OR valor_pix > threshold      │
│                                                                              │
│ Phase 3: Query torqmind_mart.pagamentos_anomalias_diaria                 │
│          Anomaly detection logic pre-computed                             │
└─────────────────────────────────────────────────────────────────────────────┘

-- TABLE 6: CASH MANAGEMENT
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 6.1 agg_caixa_turno_aberto + 6.2 agg_caixa_forma_pagamento                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_caixa_turno_aberto (018_cash_module:260)       │
│                   mart.agg_caixa_forma_pagamento (019_operational:264)     │
│ ClickHouse Dest:  torqmind_mart.agg_caixa_turno_aberto (Replacing)       │
│                   torqmind_mart.agg_caixa_forma_pagamento (Summing)      │
│ Table Definition: Open cash registers + payment forms per turno            │
│ Columns:          id_empresa, id_filial, id_turno, id_usuario,           │
│                   usuario_nome, abertura_ts, horas_aberto, severity,      │
│                   total_vendas, qtd_vendas, total_pagamentos              │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - open_cash_monitor() [Linha ~5331]                                      │
│       Query: SELECT * FROM agg_caixa_turno_aberto                         │
│            WHERE severity IN ('CRITICAL', 'HIGH')                        │
│       Return: List[OpenCashAlert] with horas_aberto, status               │
│   - cash_overview() [Linha ~5759]                                          │
│       Query: SELECT SUM(total_vendas), SUM(total_pagamentos) FROM        │
│            agg_caixa_turno_aberto WHERE is_aberto = true                │
│       Return: CashOverviewResponse                                        │
│   - cash_commercial_overview() [Linha ~5900]                               │
│       Query: SELECT id_turno, forma_label, total_valor FROM              │
│            agg_caixa_forma_pagamento WHERE is_aberto = true              │
│       Return: List[CashFormPayment]                                       │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_caixa_turno t                                              │
│   LEFT JOIN dw.fact_comprovante c ON c.id_turno = t.id_turno            │
│   LEFT JOIN dw.fact_pagamento_comprovante p ON p.id_turno = t.id_turno │
│   WHERE t.is_aberto = true                                               │
│   GROUP BY t.id_turno                                                     │
│   WITH CASE logic for severity based on horas_aberto                     │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_caixa_turno_aberto & agg_caixa_forma_pagamento
│          Status and severity pre-computed                                 │
│          Eliminate complex CASE and LEFT JOINs                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ 6.3 agg_caixa_cancelamentos + 6.4 alerta_caixa_aberto                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.agg_caixa_cancelamentos (018_cash_module:378)      │
│                   mart.alerta_caixa_aberto (018_cash_module:408)           │
│ ClickHouse Dest:  torqmind_mart.agg_caixa_cancelamentos (Summing)        │
│                   torqmind_mart.alerta_caixa_aberto (MergeTree)          │
│ Table Definition: Cash cancellations + alerts (event log)                  │
│ Columns:          id_empresa, id_filial, id_turno,                        │
│                   total_cancelamentos, qtd_cancelamentos,                  │
│                   (for alerts) title, body, action_url, severity           │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - sales_operational_current() [Linha ~5990]                              │
│       Query: SELECT SUM(total_cancelamentos) FROM                         │
│            agg_caixa_cancelamentos WHERE id_filial = ?                   │
│       Return: OperationalMetrics with cancelamentos_total                 │
│   - open_cash_monitor() [Linha ~5331]                                      │
│       Uses: alerta_caixa_aberto for alerts                               │
│       Return: List[CashAlert]                                              │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FROM dw.fact_comprovante c                                              │
│   WHERE c.cancelado = true AND c.id_turno IS NOT NULL                   │
│   GROUP BY id_turno                                                       │
│                                                                              │
│ Phase 3: Query torqmind_mart.agg_caixa_cancelamentos & alerta_caixa_aberto
│          Already denormalized; no JOINs needed                           │
└─────────────────────────────────────────────────────────────────────────────┘

-- TABLE 7: RETENTION & MISC
-- ==========================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│ 7.1 anonymous_retention_daily + 7.2 health_score_daily                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Postgres Source:  mart.anonymous_retention_daily (013_phase3:4)           │
│                   mart.health_score_daily (009_phase4:280)                  │
│ ClickHouse Dest:  torqmind_mart.anonymous_retention_daily (Replacing)    │
│                   torqmind_mart.health_score_daily (ReplacingMergeTree)   │
│ Table Definition: Daily retention metrics + composite health score         │
│ Columns:          dt_ref, id_empresa, id_filial,                          │
│                   anon_faturamento_7d, trend_pct, repeat_proxy_idx,       │
│                   fat_30d, margem_30d, high_risk_30d,                     │
│                   health_pct, customer_pct, risk_pct, final_score         │
├─────────────────────────────────────────────────────────────────────────────┤
│ repos_mart.py Functions:                                                    │
│   - anonymous_retention_overview() [Linha ~6100]                           │
│       Query: SELECT * FROM anonymous_retention_daily                      │
│            WHERE dt_ref = CURRENT_DATE                                    │
│       Return: AnonymousRetentionOverview with trend_pct, repeat_proxy_idx │
│   - health_score_latest() [Linha ~6200]                                    │
│       Query: SELECT * FROM health_score_daily                             │
│            WHERE dt_ref = CURRENT_DATE ORDER BY final_score DESC         │
│       Return: List[HealthScoreRanking]                                     │
│                                                                              │
│ DW Read Pattern:                                                            │
│   FOR anonymous_retention:                                                │
│     FROM dw.fact_venda v                                                  │
│     WHERE (v.id_cliente IS NULL OR v.id_cliente = -1)                   │
│     WITH window functions (7d, 28d rolling averages)                      │
│                                                                              │
│   FOR health_score:                                                        │
│     Combines mart.agg_vendas_diaria + mart.agg_risco_diaria                │
│     + customer churn metrics with weighted scoring                        │
│                                                                              │
│ Phase 3: Query torqmind_mart.anonymous_retention_daily & health_score_daily
│          Both pre-computed with all window functions                      │
│          Just filter by dt_ref = CURRENT_DATE                            │
└─────────────────────────────────────────────────────────────────────────────┘

-- ============================================================================
-- PHASE 3 REFACTORING: Key Points for repos_mart.py
-- ============================================================================
--
-- STRATEGY:
--   1. Each function currently querying dw.fact_* will be refactored to query
--      torqmind_mart.* (ClickHouse) instead.
--   2. Function signatures remain UNCHANGED (same @app.get decorator params,
--      same return type annotation).
--   3. Internal query changes:
--      - OLD: SELECT ... FROM dw.fact_venda JOIN dw.fact_venda_item WHERE ...
--      - NEW: SELECT ... FROM ClickHouse(torqmind_mart.agg_vendas_diaria) WHERE ...
--   4. Performance improvement: 2-5 sec (Postgres DW) → <100ms (ClickHouse MV).
--
-- AFFECTED FUNCTIONS (repos_mart.py):
--   ~62 functions with dw reads across 7 endpoint families (home, sales, fraud,
--   customers, finance, cash, payments).
--
-- VALIDATION:
--   Before cutover, run parallel queries (both Postgres dw and ClickHouse mart)
--   and log differences for 1 week. Ensure row counts and aggregate sums match.
--
-- ROLLBACK:
--   Keep Postgres ETL running during transition. If ClickHouse queries return
--   wrong results, fallback to Postgres dw is available (not "hard cutover").
-- ============================================================================
