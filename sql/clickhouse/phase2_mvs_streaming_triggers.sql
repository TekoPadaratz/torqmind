--   CREATE MATERIALIZED VIEW mv_<nome> TO torqmind_mart.<tabela_destino> AS
--   SELECT ... FROM torqmind_dw.fact_* ...
--
-- NOTA IMPORTANTE:
--   Este arquivo cria os 25 gatilhos de streaming solicitados.
--   Execute apos phase2_mvs_design.sql (criacao das tabelas destino).
-- ============================================================================

USE torqmind_mart;

-- ============================================================================
-- 1) SALES COMMERCIAL INTELLIGENCE
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_agg_vendas_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_vendas_diaria
TO torqmind_mart.agg_vendas_diaria
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    v.data_key AS data_key,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento,
    toInt32(count()) AS quantidade_itens,
    toDecimal128(sum(ifNull(i.margem, 0)), 2) AS margem,
    toDecimal128(if(countDistinct(v.id_comprovante) = 0, 0, sum(ifNull(i.total, 0)) / countDistinct(v.id_comprovante)), 2) AS ticket_medio,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
WHERE v.data_key IS NOT NULL
  AND ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key;

DROP VIEW IF EXISTS torqmind_mart.mv_agg_vendas_hora;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_vendas_hora
TO torqmind_mart.agg_vendas_hora
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    v.data_key AS data_key,
    toInt8(toHour(v.data)) AS hora,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento,
    toDecimal128(sum(ifNull(i.margem, 0)), 2) AS margem,
    toInt32(countDistinct(v.id_comprovante)) AS vendas,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
WHERE v.data_key IS NOT NULL
  AND v.data IS NOT NULL
  AND ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt8(toHour(v.data));

DROP VIEW IF EXISTS torqmind_mart.mv_agg_produtos_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_produtos_diaria
TO torqmind_mart.agg_produtos_diaria
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    v.data_key AS data_key,
    toInt32(i.id_produto) AS id_produto,
    ifNull(p.nome, '') AS produto_nome,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento,
    toDecimal128(sum(ifNull(i.margem, 0)), 2) AS margem,
    toDecimal128(sum(ifNull(i.qtd, 0)), 3) AS qtd,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_produto p
    ON p.id_empresa = i.id_empresa
   AND p.id_filial = i.id_filial
   AND p.id_produto = i.id_produto
WHERE v.data_key IS NOT NULL
  AND ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt32(i.id_produto), ifNull(p.nome, '');

DROP VIEW IF EXISTS torqmind_mart.mv_agg_grupos_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_grupos_diaria
TO torqmind_mart.agg_grupos_diaria
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    v.data_key AS data_key,
    toInt32(ifNull(i.id_grupo_produto, -1)) AS id_grupo_produto,
    ifNull(g.nome, '(Sem grupo)') AS grupo_nome,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento,
    toDecimal128(sum(ifNull(i.margem, 0)), 2) AS margem,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_grupo_produto g
    ON g.id_empresa = i.id_empresa
   AND g.id_filial = i.id_filial
   AND g.id_grupo_produto = i.id_grupo_produto
WHERE v.data_key IS NOT NULL
  AND ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt32(ifNull(i.id_grupo_produto, -1)), ifNull(g.nome, '(Sem grupo)');

DROP VIEW IF EXISTS torqmind_mart.mv_agg_funcionarios_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_funcionarios_diaria
TO torqmind_mart.agg_funcionarios_diaria
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    v.data_key AS data_key,
    toInt32(ifNull(i.id_funcionario, -1)) AS id_funcionario,
    ifNull(f.nome, '(Sem funcionario)') AS funcionario_nome,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento,
    toDecimal128(sum(ifNull(i.margem, 0)), 2) AS margem,
    toInt32(countDistinct(v.id_comprovante)) AS vendas,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_funcionario f
    ON f.id_empresa = i.id_empresa
   AND f.id_filial = i.id_filial
   AND f.id_funcionario = i.id_funcionario
WHERE v.data_key IS NOT NULL
  AND ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, toInt32(ifNull(i.id_funcionario, -1)), ifNull(f.nome, '(Sem funcionario)');

DROP VIEW IF EXISTS torqmind_mart.mv_insights_base_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_insights_base_diaria
TO torqmind_mart.insights_base_diaria
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    v.data_key AS data_key,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento_dia,
    toDecimal128(sum(ifNull(i.total, 0)), 2) AS faturamento_mes_acum,
    toDecimal128(0, 2) AS comparativo_mes_anterior,
    CAST(NULL, 'Nullable(String)') AS top_vendedor_key,
    CAST(NULL, 'Nullable(Decimal(38,2))') AS top_vendedor_valor,
    CAST(NULL, 'Nullable(Decimal(38,2))') AS inadimplencia_valor,
    CAST(NULL, 'Nullable(Decimal(38,4))') AS inadimplencia_pct,
    CAST(NULL, 'Nullable(String)') AS cliente_em_risco_key,
    CAST(NULL, 'Nullable(Decimal(38,4))') AS margem_media_pct,
    CAST(NULL, 'Nullable(Decimal(38,2))') AS giro_estoque,
    now() AS updated_at,
    '{}' AS batch_info
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
WHERE v.data_key IS NOT NULL
  AND ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key;

-- ============================================================================
-- 2) FRAUD & RISK INTELLIGENCE
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_fraude_cancelamentos_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_fraude_cancelamentos_diaria
TO torqmind_mart.fraude_cancelamentos_diaria
AS
SELECT
    c.id_empresa AS id_empresa,
    c.id_filial AS id_filial,
    c.data_key AS data_key,
    toInt32(count()) AS cancelamentos,
    toDecimal128(sum(ifNull(c.valor_total, 0)), 2) AS valor_cancelado,
    now() AS updated_at
FROM torqmind_dw.fact_comprovante c
WHERE c.data_key IS NOT NULL
  AND ifNull(c.cancelado, 0) = 1
GROUP BY c.id_empresa, c.id_filial, c.data_key;

DROP VIEW IF EXISTS torqmind_mart.mv_fraude_cancelamentos_eventos;
CREATE MATERIALIZED VIEW torqmind_mart.mv_fraude_cancelamentos_eventos
TO torqmind_mart.fraude_cancelamentos_eventos
AS
SELECT
    c.id_empresa AS id_empresa,
    c.id_filial AS id_filial,
    c.id_db AS id_db,
    toString(c.id_comprovante) AS id_comprovante,
    c.data AS data,
    c.data_key AS data_key,
    c.id_usuario AS id_usuario,
    c.id_turno AS id_turno,
    toDecimal128(ifNull(c.valor_total, 0), 2) AS valor_total,
    now() AS updated_at
FROM torqmind_dw.fact_comprovante c
WHERE ifNull(c.cancelado, 0) = 1;

DROP VIEW IF EXISTS torqmind_mart.mv_agg_risco_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_risco_diaria
TO torqmind_mart.agg_risco_diaria
AS
SELECT
    r.id_empresa AS id_empresa,
    r.id_filial AS id_filial,
    r.data_key AS data_key,
    toInt32(count()) AS eventos_risco_total,
    toInt32(countIf(ifNull(r.score_risco, 0) >= 80)) AS eventos_alto_risco,
    toDecimal128(sum(ifNull(r.impacto_estimado, 0)), 2) AS impacto_estimado_total,
    toDecimal128(avg(ifNull(r.score_risco, 0)), 2) AS score_medio,
    toDecimal128(quantileExact(0.95)(ifNull(r.score_risco, 0)), 2) AS p95_score,
    now() AS updated_at
FROM torqmind_dw.fact_risco_evento r
GROUP BY r.id_empresa, r.id_filial, r.data_key;

DROP VIEW IF EXISTS torqmind_mart.mv_risco_top_funcionarios_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_risco_top_funcionarios_diaria
TO torqmind_mart.risco_top_funcionarios_diaria
AS
SELECT
    r.id_empresa AS id_empresa,
    r.id_filial AS id_filial,
    r.data_key AS data_key,
    toInt32(ifNull(r.id_funcionario, -1)) AS id_funcionario,
    ifNull(df.nome, '(Sem funcionario)') AS funcionario_nome,
    toInt32(count()) AS eventos,
    toInt32(countIf(ifNull(r.score_risco, 0) >= 80)) AS alto_risco,
    toDecimal128(sum(ifNull(r.impacto_estimado, 0)), 2) AS impacto_estimado,
    toDecimal128(avg(ifNull(r.score_risco, 0)), 2) AS score_medio,
    now() AS updated_at
FROM torqmind_dw.fact_risco_evento r
LEFT JOIN torqmind_dw.dim_funcionario df
    ON df.id_empresa = r.id_empresa
   AND df.id_filial = r.id_filial
   AND df.id_funcionario = r.id_funcionario
GROUP BY r.id_empresa, r.id_filial, r.data_key, toInt32(ifNull(r.id_funcionario, -1)), ifNull(df.nome, '(Sem funcionario)');

DROP VIEW IF EXISTS torqmind_mart.mv_risco_turno_local_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_risco_turno_local_diaria
TO torqmind_mart.risco_turno_local_diaria
AS
SELECT
    r.id_empresa AS id_empresa,
    r.id_filial AS id_filial,
    r.data_key AS data_key,
    toInt32(ifNull(r.id_turno, -1)) AS id_turno,
    toInt32(ifNull(i.id_local_venda, -1)) AS id_local_venda,
    toInt32(count()) AS eventos,
    toInt32(countIf(ifNull(r.score_risco, 0) >= 80)) AS alto_risco,
    toDecimal128(sum(ifNull(r.impacto_estimado, 0)), 2) AS impacto_estimado,
    toDecimal128(avg(ifNull(r.score_risco, 0)), 2) AS score_medio,
    now() AS updated_at
FROM torqmind_dw.fact_risco_evento r
LEFT JOIN torqmind_dw.fact_venda_item i
    ON i.id_empresa = r.id_empresa
   AND i.id_filial = r.id_filial
   AND i.id_db = r.id_db
   AND i.id_movprodutos = r.id_movprodutos
GROUP BY r.id_empresa, r.id_filial, r.data_key, toInt32(ifNull(r.id_turno, -1)), toInt32(ifNull(i.id_local_venda, -1));

DROP VIEW IF EXISTS torqmind_mart.mv_clientes_churn_risco;
CREATE MATERIALIZED VIEW torqmind_mart.mv_clientes_churn_risco
TO torqmind_mart.clientes_churn_risco
AS
SELECT
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    toInt32(ifNull(v.id_cliente, -1)) AS id_cliente,
    ifNull(c.nome, concat('#ID ', toString(ifNull(v.id_cliente, -1)))) AS cliente_nome,
    max(toDate(v.data)) AS last_purchase,
    toInt32(countIf(toDate(v.data) >= today() - 30)) AS compras_30d,
    toInt32(countIf(toDate(v.data) >= today() - 60 AND toDate(v.data) < today() - 30)) AS compras_60_30,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30), 2) AS faturamento_30d,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 60 AND toDate(v.data) < today() - 30), 2) AS faturamento_60_30,
    toInt32(
        least(100,
            greatest(0,
                if(max(toDate(v.data)) < today() - 60, 95, if(max(toDate(v.data)) < today() - 30, 80, 40))
                + if(countIf(toDate(v.data) >= today() - 60 AND toDate(v.data) < today() - 30) > 0 AND countIf(toDate(v.data) >= today() - 30) = 0, 20, 0)
            )
        )
    ) AS churn_score,
    '{}' AS reasons,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_cliente c
    ON c.id_empresa = v.id_empresa
   AND c.id_filial = v.id_filial
   AND c.id_cliente = v.id_cliente
WHERE ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
  AND v.data IS NOT NULL
GROUP BY v.id_empresa, v.id_filial, toInt32(ifNull(v.id_cliente, -1)), ifNull(c.nome, concat('#ID ', toString(ifNull(v.id_cliente, -1))));

-- ============================================================================
-- 3) CUSTOMER INTELLIGENCE & RFM
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_customer_rfm_daily;
CREATE MATERIALIZED VIEW torqmind_mart.mv_customer_rfm_daily
TO torqmind_mart.customer_rfm_daily
AS
SELECT
    today() AS dt_ref,
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    toInt32(v.id_cliente) AS id_cliente,
    ifNull(c.nome, concat('#ID ', toString(v.id_cliente))) AS cliente_nome,
    max(toDate(v.data)) AS last_purchase,
    toInt32(dateDiff('day', max(toDate(v.data)), today())) AS recency_days,
    toInt32(countIf(toDate(v.data) >= today() - 30)) AS frequency_30,
    toInt32(countIf(toDate(v.data) >= today() - 90)) AS frequency_90,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30), 2) AS monetary_30,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 90), 2) AS monetary_90,
    toDecimal128(if(countIf(toDate(v.data) >= today() - 30) = 0, 0, sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30) / countIf(toDate(v.data) >= today() - 30)), 2) AS ticket_30,
    toDecimal64(30, 2) AS expected_cycle_days,
    toInt32(greatest(0, countIf(toDate(v.data) >= today() - 30) - countIf(toDate(v.data) >= today() - 90 AND toDate(v.data) < today() - 30))) AS trend_frequency,
    toDecimal128(
        sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30)
        - sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 90 AND toDate(v.data) < today() - 30),
        2
    ) AS trend_monetary,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_cliente c
    ON c.id_empresa = v.id_empresa
   AND c.id_filial = v.id_filial
   AND c.id_cliente = v.id_cliente
WHERE ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
  AND v.id_cliente IS NOT NULL
  AND v.data IS NOT NULL
GROUP BY v.id_empresa, v.id_filial, toInt32(v.id_cliente), ifNull(c.nome, concat('#ID ', toString(v.id_cliente)));

DROP VIEW IF EXISTS torqmind_mart.mv_customer_churn_risk_daily;
CREATE MATERIALIZED VIEW torqmind_mart.mv_customer_churn_risk_daily
TO torqmind_mart.customer_churn_risk_daily
AS
SELECT
    today() AS dt_ref,
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    toInt32(v.id_cliente) AS id_cliente,
    ifNull(c.nome, concat('#ID ', toString(v.id_cliente))) AS cliente_nome,
    max(toDate(v.data)) AS last_purchase,
    toInt32(dateDiff('day', max(toDate(v.data)), today())) AS recency_days,
    toInt32(countIf(toDate(v.data) >= today() - 30)) AS frequency_30,
    toInt32(countIf(toDate(v.data) >= today() - 90)) AS frequency_90,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30), 2) AS monetary_30,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 90), 2) AS monetary_90,
    toDecimal128(if(countIf(toDate(v.data) >= today() - 30) = 0, 0, sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30) / countIf(toDate(v.data) >= today() - 30)), 2) AS ticket_30,
    toDecimal64(30, 2) AS expected_cycle_days,
    toInt32(
        least(100,
            greatest(
                0,
                (dateDiff('day', max(toDate(v.data)), today()) * 1.2)
                + if(countIf(toDate(v.data) >= today() - 30) = 0, 25, 0)
            )
        )
    ) AS churn_score,
    toDecimal128(
        greatest(0,
            sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 90 AND toDate(v.data) < today() - 30)
            - sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30)
        ),
        2
    ) AS revenue_at_risk_30d,
    if(
        dateDiff('day', max(toDate(v.data)), today()) >= 60,
        'Contato imediato + oferta de recuperacao em 24h',
        if(dateDiff('day', max(toDate(v.data)), today()) >= 30,
           'Campanha personalizada e follow-up comercial em 7 dias',
           'Monitorar jornada e reforcar frequencia com beneficios')
    ) AS recommendation,
    '{}' AS reasons,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
LEFT JOIN torqmind_dw.dim_cliente c
    ON c.id_empresa = v.id_empresa
   AND c.id_filial = v.id_filial
   AND c.id_cliente = v.id_cliente
WHERE ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
  AND v.id_cliente IS NOT NULL
  AND v.data IS NOT NULL
GROUP BY v.id_empresa, v.id_filial, toInt32(v.id_cliente), ifNull(c.nome, concat('#ID ', toString(v.id_cliente)));

-- ============================================================================
-- 4) FINANCE INTELLIGENCE
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_financeiro_vencimentos_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_financeiro_vencimentos_diaria
TO torqmind_mart.financeiro_vencimentos_diaria
AS
SELECT
    f.id_empresa AS id_empresa,
    f.id_filial AS id_filial,
    f.data_key_venc AS data_key,
    toInt8(f.tipo_titulo) AS tipo_titulo,
    toDecimal128(sum(ifNull(f.valor, 0)), 2) AS valor_total,
    toDecimal128(sum(ifNull(f.valor_pago, 0)), 2) AS valor_pago,
    toDecimal128(sum(if(isNull(f.data_pagamento), greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), 0)), 2) AS valor_aberto,
    now() AS updated_at
FROM torqmind_dw.fact_financeiro f
WHERE f.data_key_venc IS NOT NULL
GROUP BY f.id_empresa, f.id_filial, f.data_key_venc, toInt8(f.tipo_titulo);

DROP VIEW IF EXISTS torqmind_mart.mv_finance_aging_daily;
CREATE MATERIALIZED VIEW torqmind_mart.mv_finance_aging_daily
TO torqmind_mart.finance_aging_daily
AS
SELECT
    today() AS dt_ref,
    f.id_empresa AS id_empresa,
    f.id_filial AS id_filial,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1), 2) AS receber_total_aberto,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today()), 2) AS receber_total_vencido,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 0), 2) AS pagar_total_aberto,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 0 AND f.vencimento < today()), 2) AS pagar_total_vencido,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 0 AND 7), 2) AS bucket_0_7,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 8 AND 15), 2) AS bucket_8_15,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 16 AND 30), 2) AS bucket_16_30,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) BETWEEN 31 AND 60), 2) AS bucket_31_60,
    toDecimal128(sumIf(greatest(ifNull(f.valor, 0) - ifNull(f.valor_pago, 0), 0), f.tipo_titulo = 1 AND f.vencimento < today() AND dateDiff('day', f.vencimento, today()) > 60), 2) AS bucket_60_plus,
    toDecimal64(0, 2) AS top5_concentration_pct,
    toUInt8(if(count() = 0, 1, 0)) AS data_gaps,
    now() AS updated_at
FROM torqmind_dw.fact_financeiro f
WHERE f.vencimento IS NOT NULL
GROUP BY f.id_empresa, f.id_filial;

-- ============================================================================
-- 5) PAYMENT INTELLIGENCE
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_agg_pagamentos_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_pagamentos_diaria
TO torqmind_mart.agg_pagamentos_diaria
AS
SELECT
    p.id_empresa AS id_empresa,
    p.id_filial AS id_filial,
    p.data_key AS data_key,
    multiIf(
        p.tipo_forma IN (3, 13, 23), 'PIX',
        p.tipo_forma IN (4, 5, 6), 'CARTAO',
        p.tipo_forma IN (1), 'DINHEIRO',
        'NAO_IDENTIFICADO'
    ) AS category,
    concat('FORMA_', toString(p.tipo_forma)) AS label,
    toDecimal128(sum(ifNull(p.valor, 0)), 2) AS total_valor,
    toInt32(countDistinct(p.referencia)) AS qtd_comprovantes,
    toDecimal64(0, 2) AS share_percent,
    now() AS updated_at
FROM torqmind_dw.fact_pagamento_comprovante p
GROUP BY
    p.id_empresa,
    p.id_filial,
    p.data_key,
    multiIf(
        p.tipo_forma IN (3, 13, 23), 'PIX',
        p.tipo_forma IN (4, 5, 6), 'CARTAO',
        p.tipo_forma IN (1), 'DINHEIRO',
        'NAO_IDENTIFICADO'
    ),
    concat('FORMA_', toString(p.tipo_forma));

DROP VIEW IF EXISTS torqmind_mart.mv_agg_pagamentos_turno;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_pagamentos_turno
TO torqmind_mart.agg_pagamentos_turno
AS
SELECT
    p.id_empresa AS id_empresa,
    p.id_filial AS id_filial,
    p.data_key AS data_key,
    toInt32(ifNull(p.id_turno, -1)) AS id_turno,
    multiIf(
        p.tipo_forma IN (3, 13, 23), 'PIX',
        p.tipo_forma IN (4, 5, 6), 'CARTAO',
        p.tipo_forma IN (1), 'DINHEIRO',
        'NAO_IDENTIFICADO'
    ) AS category,
    concat('FORMA_', toString(p.tipo_forma)) AS label,
    toDecimal128(sum(ifNull(p.valor, 0)), 2) AS total_valor,
    toInt32(countDistinct(p.referencia)) AS qtd_comprovantes,
    now() AS updated_at
FROM torqmind_dw.fact_pagamento_comprovante p
GROUP BY
    p.id_empresa,
    p.id_filial,
    p.data_key,
    toInt32(ifNull(p.id_turno, -1)),
    multiIf(
        p.tipo_forma IN (3, 13, 23), 'PIX',
        p.tipo_forma IN (4, 5, 6), 'CARTAO',
        p.tipo_forma IN (1), 'DINHEIRO',
        'NAO_IDENTIFICADO'
    ),
    concat('FORMA_', toString(p.tipo_forma));

DROP VIEW IF EXISTS torqmind_mart.mv_pagamentos_anomalias_diaria;
CREATE MATERIALIZED VIEW torqmind_mart.mv_pagamentos_anomalias_diaria
TO torqmind_mart.pagamentos_anomalias_diaria
AS
SELECT
    p.id_empresa AS id_empresa,
    p.id_filial AS id_filial,
    p.data_key AS data_key,
    toInt32(ifNull(p.id_turno, -1)) AS id_turno,
    'PAYMENT_PATTERN' AS event_type,
    multiIf(sum(ifNull(p.valor, 0)) >= 100000, 'CRITICAL', sum(ifNull(p.valor, 0)) >= 30000, 'WARN', 'INFO') AS severity,
    toDecimal64(least(100, greatest(0, sum(ifNull(p.valor, 0)) / 1000)), 2) AS score,
    concat('PAY|', toString(p.id_empresa), '|', toString(p.id_filial), '|', toString(p.data_key), '|', toString(toInt32(ifNull(p.id_turno, -1)))) AS insight_id_hash,
    toInt32(if(countDistinct(p.tipo_forma) >= 3, countDistinct(p.referencia), 0)) AS comprovantes_multiplos,
    toInt32(countDistinct(p.referencia)) AS comprovantes_total,
    toDecimal128(sum(ifNull(p.valor, 0)), 2) AS valor_total,
    toDecimal64(avg(toFloat64(ifNull(p.tipo_forma, 0))), 2) AS avg_formas,
    now() AS updated_at
FROM torqmind_dw.fact_pagamento_comprovante p
GROUP BY p.id_empresa, p.id_filial, p.data_key, toInt32(ifNull(p.id_turno, -1));

-- ============================================================================
-- 6) CASH MANAGEMENT
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_agg_caixa_turno_aberto;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_caixa_turno_aberto
TO torqmind_mart.agg_caixa_turno_aberto
AS
SELECT
    t.id_empresa AS id_empresa,
    t.id_filial AS id_filial,
    ifNull(df.nome, '') AS filial_nome,
    t.id_turno AS id_turno,
    toInt32(ifNull(t.id_usuario, -1)) AS id_usuario,
    ifNull(u.nome, concat('Usuario ', toString(ifNull(t.id_usuario, -1)))) AS usuario_nome,
    t.abertura_ts AS abertura_ts,
    t.fechamento_ts AS fechamento_ts,
    toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2) AS horas_aberto,
    multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'CRITICAL', dateDiff('hour', t.abertura_ts, now()) >= 12, 'HIGH', dateDiff('hour', t.abertura_ts, now()) >= 6, 'WARN', 'OK') AS severity,
    multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'Critico', dateDiff('hour', t.abertura_ts, now()) >= 12, 'Atencao alta', dateDiff('hour', t.abertura_ts, now()) >= 6, 'Monitorar', 'Dentro da janela') AS status_label,
    toDecimal128(sumIf(ifNull(c.valor_total, 0), ifNull(c.cancelado, 0) = 0), 2) AS total_vendas,
    toInt32(countIf(ifNull(c.cancelado, 0) = 0)) AS qtd_vendas,
    toDecimal128(sumIf(ifNull(c.valor_total, 0), ifNull(c.cancelado, 0) = 1), 2) AS total_cancelamentos,
    toInt32(countIf(ifNull(c.cancelado, 0) = 1)) AS qtd_cancelamentos,
    toDecimal128(sum(ifNull(p.valor, 0)), 2) AS total_pagamentos,
    now() AS updated_at
FROM torqmind_dw.fact_caixa_turno t
LEFT JOIN torqmind_dw.dim_filial df
    ON df.id_empresa = t.id_empresa
   AND df.id_filial = t.id_filial
LEFT JOIN torqmind_dw.dim_usuario_caixa u
    ON u.id_empresa = t.id_empresa
   AND u.id_filial = t.id_filial
   AND u.id_usuario = t.id_usuario
LEFT JOIN torqmind_dw.fact_comprovante c
    ON c.id_empresa = t.id_empresa
   AND c.id_filial = t.id_filial
   AND c.id_turno = t.id_turno
LEFT JOIN torqmind_dw.fact_pagamento_comprovante p
    ON p.id_empresa = t.id_empresa
   AND p.id_filial = t.id_filial
   AND p.id_turno = t.id_turno
WHERE ifNull(t.is_aberto, 0) = 1
  AND t.abertura_ts IS NOT NULL
GROUP BY
    t.id_empresa,
    t.id_filial,
    ifNull(df.nome, ''),
    t.id_turno,
    toInt32(ifNull(t.id_usuario, -1)),
    ifNull(u.nome, concat('Usuario ', toString(ifNull(t.id_usuario, -1)))),
    t.abertura_ts,
    t.fechamento_ts,
    multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'CRITICAL', dateDiff('hour', t.abertura_ts, now()) >= 12, 'HIGH', dateDiff('hour', t.abertura_ts, now()) >= 6, 'WARN', 'OK'),
    multiIf(dateDiff('hour', t.abertura_ts, now()) >= 24, 'Critico', dateDiff('hour', t.abertura_ts, now()) >= 12, 'Atencao alta', dateDiff('hour', t.abertura_ts, now()) >= 6, 'Monitorar', 'Dentro da janela');

DROP VIEW IF EXISTS torqmind_mart.mv_agg_caixa_forma_pagamento;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_caixa_forma_pagamento
TO torqmind_mart.agg_caixa_forma_pagamento
AS
SELECT
    p.id_empresa AS id_empresa,
    p.id_filial AS id_filial,
    toInt32(ifNull(p.id_turno, -1)) AS id_turno,
    toInt32(p.tipo_forma) AS tipo_forma,
    concat('FORMA_', toString(p.tipo_forma)) AS forma_label,
    multiIf(
        p.tipo_forma IN (3, 13, 23), 'PIX',
        p.tipo_forma IN (4, 5, 6), 'CARTAO',
        p.tipo_forma IN (1), 'DINHEIRO',
        'NAO_IDENTIFICADO'
    ) AS forma_category,
    toDecimal128(sum(ifNull(p.valor, 0)), 2) AS total_valor,
    toInt32(countDistinct(p.referencia)) AS qtd_comprovantes,
    now() AS updated_at
FROM torqmind_dw.fact_pagamento_comprovante p
WHERE p.id_turno IS NOT NULL
GROUP BY
    p.id_empresa,
    p.id_filial,
    toInt32(ifNull(p.id_turno, -1)),
    toInt32(p.tipo_forma),
    concat('FORMA_', toString(p.tipo_forma)),
    multiIf(
        p.tipo_forma IN (3, 13, 23), 'PIX',
        p.tipo_forma IN (4, 5, 6), 'CARTAO',
        p.tipo_forma IN (1), 'DINHEIRO',
        'NAO_IDENTIFICADO'
    );

DROP VIEW IF EXISTS torqmind_mart.mv_agg_caixa_cancelamentos;
CREATE MATERIALIZED VIEW torqmind_mart.mv_agg_caixa_cancelamentos
TO torqmind_mart.agg_caixa_cancelamentos
AS
SELECT
    c.id_empresa AS id_empresa,
    c.id_filial AS id_filial,
    toInt32(ifNull(c.id_turno, -1)) AS id_turno,
    ifNull(df.nome, '') AS filial_nome,
    toDecimal128(sum(ifNull(c.valor_total, 0)), 2) AS total_cancelamentos,
    toInt32(count()) AS qtd_cancelamentos,
    now() AS updated_at
FROM torqmind_dw.fact_comprovante c
LEFT JOIN torqmind_dw.dim_filial df
    ON df.id_empresa = c.id_empresa
   AND df.id_filial = c.id_filial
WHERE ifNull(c.cancelado, 0) = 1
  AND c.id_turno IS NOT NULL
GROUP BY c.id_empresa, c.id_filial, toInt32(ifNull(c.id_turno, -1)), ifNull(df.nome, '');

DROP VIEW IF EXISTS torqmind_mart.mv_alerta_caixa_aberto;
CREATE MATERIALIZED VIEW torqmind_mart.mv_alerta_caixa_aberto
TO torqmind_mart.alerta_caixa_aberto
AS
SELECT
    t.id_empresa AS id_empresa,
    t.id_filial AS id_filial,
    ifNull(df.nome, '') AS filial_nome,
    t.id_turno AS id_turno,
    toInt32(ifNull(t.id_usuario, -1)) AS id_usuario,
    ifNull(u.nome, concat('Usuario ', toString(ifNull(t.id_usuario, -1)))) AS usuario_nome,
    t.abertura_ts AS abertura_ts,
    toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2) AS horas_aberto,
    'CRITICAL' AS severity,
    concat('Caixa ', toString(t.id_turno), ' aberto ha ', toString(toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2)), ' horas') AS title,
    concat(
        'O caixa ', toString(t.id_turno),
        ' da filial ', ifNull(df.nome, toString(t.id_filial)),
        ' esta aberto ha ', toString(toDecimal64(dateDiff('minute', t.abertura_ts, now()) / 60.0, 2)),
        ' horas. Operador: ', ifNull(u.nome, 'nao identificado'), '.'
    ) AS body,
    '/cash' AS action_url,
    now() AS updated_at
FROM torqmind_dw.fact_caixa_turno t
LEFT JOIN torqmind_dw.dim_filial df
    ON df.id_empresa = t.id_empresa
   AND df.id_filial = t.id_filial
LEFT JOIN torqmind_dw.dim_usuario_caixa u
    ON u.id_empresa = t.id_empresa
   AND u.id_filial = t.id_filial
   AND u.id_usuario = t.id_usuario
WHERE ifNull(t.is_aberto, 0) = 1
  AND t.abertura_ts IS NOT NULL
  AND dateDiff('hour', t.abertura_ts, now()) >= 24;

-- ============================================================================
-- 7) RETENTION & MISC
-- ============================================================================

DROP VIEW IF EXISTS torqmind_mart.mv_anonymous_retention_daily;
CREATE MATERIALIZED VIEW torqmind_mart.mv_anonymous_retention_daily
TO torqmind_mart.anonymous_retention_daily
AS
SELECT
    toDate(v.data) AS dt_ref,
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    toDecimal128(sumIf(ifNull(v.total_venda, 0), isNull(v.id_cliente) OR v.id_cliente = -1), 2) AS anon_faturamento_7d,
    toDecimal128(0, 2) AS anon_faturamento_prev_28d,
    toDecimal64(0, 2) AS trend_pct,
    toDecimal64(
        if(sum(ifNull(v.total_venda, 0)) = 0, 0,
           (sumIf(ifNull(v.total_venda, 0), isNull(v.id_cliente) OR v.id_cliente = -1) / sum(ifNull(v.total_venda, 0))) * 100),
        2
    ) AS anon_share_pct_7d,
    toDecimal64(0, 2) AS repeat_proxy_idx,
    toDecimal128(0, 2) AS impact_estimated_7d,
    '{}' AS details,
    now() AS updated_at
FROM torqmind_dw.fact_venda v
WHERE ifNull(v.cancelado, 0) = 0
  AND v.data IS NOT NULL
GROUP BY toDate(v.data), v.id_empresa, v.id_filial;

DROP VIEW IF EXISTS torqmind_mart.mv_health_score_daily;
CREATE MATERIALIZED VIEW torqmind_mart.mv_health_score_daily
TO torqmind_mart.health_score_daily
AS
SELECT
    today() AS dt_ref,
    v.id_empresa AS id_empresa,
    v.id_filial AS id_filial,
    toDecimal128(sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30), 2) AS fat_30d,
    toDecimal128(sumIf(ifNull(i.margem, 0), toDate(v.data) >= today() - 30), 2) AS margem_30d,
    toDecimal128(if(countIf(toDate(v.data) >= today() - 30) = 0, 0, sumIf(ifNull(i.total, 0), toDate(v.data) >= today() - 30) / countIf(toDate(v.data) >= today() - 30)), 2) AS ticket_30d,
    toInt32(0) AS high_risk_30d,
    toInt32(0) AS total_risk_30d,
    toDecimal128(0, 2) AS impacto_risco_30d,
    toDecimal64(80, 2) AS health_pct,
    toDecimal64(80, 2) AS customer_pct,
    toDecimal64(80, 2) AS risk_pct,
    toDecimal64(80, 2) AS final_score,
    now() AS updated_at
FROM torqmind_dw.fact_venda_item i
INNER JOIN torqmind_dw.fact_venda v
    ON v.id_empresa = i.id_empresa
   AND v.id_filial = i.id_filial
   AND v.id_db = i.id_db
   AND v.id_movprodutos = i.id_movprodutos
WHERE ifNull(v.cancelado, 0) = 0
  AND ifNull(i.cfop, 0) >= 5000
  AND v.data IS NOT NULL
GROUP BY v.id_empresa, v.id_filial;