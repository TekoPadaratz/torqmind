BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mart.customer_rfm_daily CASCADE;
CREATE MATERIALIZED VIEW mart.customer_rfm_daily AS
WITH sales AS (
  SELECT
    v.id_empresa,
    v.id_filial,
    COALESCE(v.id_cliente, -1) AS id_cliente,
    v.data::date AS dt_compra,
    COUNT(DISTINCT v.id_comprovante)::int AS compras_dia,
    COALESCE(SUM(i.total),0)::numeric(18,2) AS valor_dia
  FROM dw.fact_venda v
  JOIN dw.fact_venda_item i
    ON i.id_empresa=v.id_empresa
   AND i.id_filial=v.id_filial
   AND i.id_db=v.id_db
   AND i.id_movprodutos=v.id_movprodutos
  WHERE COALESCE(v.cancelado,false)=false
    AND COALESCE(i.cfop,0) >= 5000
    AND v.data::date >= current_date - interval '180 day'
    AND v.id_cliente IS NOT NULL
  GROUP BY 1,2,3,4
),
purchase_history AS (
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_cliente,
    s.dt_compra,
    LAG(s.dt_compra) OVER (PARTITION BY s.id_empresa, s.id_filial, s.id_cliente ORDER BY s.dt_compra) AS prev_dt
  FROM sales s
),
expected_cycle AS (
  SELECT
    id_empresa,
    id_filial,
    id_cliente,
    COALESCE(
      percentile_cont(0.5) WITHIN GROUP (ORDER BY GREATEST(1, (dt_compra - prev_dt))),
      30
    )::numeric(10,2) AS expected_cycle_days
  FROM purchase_history
  WHERE prev_dt IS NOT NULL
  GROUP BY 1,2,3
),
agg AS (
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_cliente,
    MAX(s.dt_compra) AS last_purchase,
    SUM(CASE WHEN s.dt_compra >= current_date - interval '30 day' THEN s.compras_dia ELSE 0 END)::int AS frequency_30,
    SUM(CASE WHEN s.dt_compra >= current_date - interval '90 day' THEN s.compras_dia ELSE 0 END)::int AS frequency_90,
    SUM(CASE WHEN s.dt_compra >= current_date - interval '30 day' THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_30,
    SUM(CASE WHEN s.dt_compra >= current_date - interval '90 day' THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_90
  FROM sales s
  GROUP BY 1,2,3
)
SELECT
  current_date AS dt_ref,
  a.id_empresa,
  a.id_filial,
  a.id_cliente,
  COALESCE(NULLIF(c.nome,''), '#ID ' || a.id_cliente::text) AS cliente_nome,
  a.last_purchase,
  (current_date - a.last_purchase)::int AS recency_days,
  a.frequency_30,
  a.frequency_90,
  a.monetary_30,
  a.monetary_90,
  CASE WHEN a.frequency_30 > 0 THEN (a.monetary_30 / a.frequency_30)::numeric(18,2) ELSE 0::numeric(18,2) END AS ticket_30,
  COALESCE(ec.expected_cycle_days, 30)::numeric(10,2) AS expected_cycle_days,
  GREATEST(0, a.frequency_30 - (a.frequency_90 - a.frequency_30))::int AS trend_frequency,
  (a.monetary_30 - GREATEST(0::numeric, a.monetary_90 - a.monetary_30))::numeric(18,2) AS trend_monetary,
  now() AS updated_at
FROM agg a
LEFT JOIN expected_cycle ec
  ON ec.id_empresa=a.id_empresa
 AND ec.id_filial=a.id_filial
 AND ec.id_cliente=a.id_cliente
LEFT JOIN LATERAL (
  SELECT d.nome
  FROM dw.dim_cliente d
  WHERE d.id_empresa = a.id_empresa
    AND d.id_cliente = a.id_cliente
  ORDER BY (d.id_filial = a.id_filial) DESC, d.updated_at DESC
  LIMIT 1
) c ON true;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_customer_rfm_daily
  ON mart.customer_rfm_daily (dt_ref, id_empresa, id_filial, id_cliente);
CREATE INDEX IF NOT EXISTS ix_mart_customer_rfm_lookup
  ON mart.customer_rfm_daily (id_empresa, id_filial, recency_days DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.customer_churn_risk_daily CASCADE;
CREATE MATERIALIZED VIEW mart.customer_churn_risk_daily AS
WITH base AS (
  SELECT
    r.*,
    GREATEST(0, r.frequency_90 - r.frequency_30)::int AS frequency_prev_60,
    GREATEST(0::numeric, r.monetary_90 - r.monetary_30)::numeric(18,2) AS monetary_prev_60
  FROM mart.customer_rfm_daily r
),
scored AS (
  SELECT
    b.*,
    LEAST(45,
      CASE
        WHEN b.expected_cycle_days > 0 THEN GREATEST(0, ((b.recency_days - b.expected_cycle_days) / NULLIF(b.expected_cycle_days,0)::numeric) * 25)
        ELSE 0
      END
    ) AS p_cycle_break,
    LEAST(30,
      CASE
        WHEN b.frequency_prev_60 > 0 THEN GREATEST(0, ((b.frequency_prev_60 - b.frequency_30)::numeric / b.frequency_prev_60) * 30)
        ELSE CASE WHEN b.frequency_30 = 0 THEN 10 ELSE 0 END
      END
    ) AS p_freq_drop,
    LEAST(25,
      CASE
        WHEN b.monetary_prev_60 > 0 THEN GREATEST(0, ((b.monetary_prev_60 - b.monetary_30) / b.monetary_prev_60) * 25)
        ELSE 0
      END
    ) AS p_monetary_drop
  FROM base b
),
final AS (
  SELECT
    s.dt_ref,
    s.id_empresa,
    s.id_filial,
    s.id_cliente,
    s.cliente_nome,
    s.last_purchase,
    s.recency_days,
    s.frequency_30,
    s.frequency_90,
    s.monetary_30,
    s.monetary_90,
    s.ticket_30,
    s.expected_cycle_days,
    LEAST(100, GREATEST(0, s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop + LEAST(15, LN(1 + s.monetary_90 / 1000.0) * 4)))::int AS churn_score,
    (GREATEST(0::numeric, s.monetary_prev_60) * LEAST(1.0, (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop)/100.0))::numeric(18,2) AS revenue_at_risk_30d,
    jsonb_build_object(
      'cycle_break', round(s.p_cycle_break::numeric,2),
      'frequency_drop', round(s.p_freq_drop::numeric,2),
      'monetary_drop', round(s.p_monetary_drop::numeric,2),
      'recency_days', s.recency_days,
      'expected_cycle_days', s.expected_cycle_days,
      'frequency_30', s.frequency_30,
      'frequency_90', s.frequency_90,
      'monetary_30', s.monetary_30,
      'monetary_90', s.monetary_90
    ) AS reasons,
    CASE
      WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 70 THEN 'Contato imediato + oferta de recuperação em 24h'
      WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 50 THEN 'Campanha personalizada e follow-up comercial em 7 dias'
      ELSE 'Monitorar jornada e reforçar frequência com benefícios'
    END AS recommendation,
    now() AS updated_at
  FROM scored s
)
SELECT * FROM final;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_customer_churn_risk_daily
  ON mart.customer_churn_risk_daily (dt_ref, id_empresa, id_filial, id_cliente);
CREATE INDEX IF NOT EXISTS ix_mart_customer_churn_risk_score
  ON mart.customer_churn_risk_daily (id_empresa, id_filial, churn_score DESC, revenue_at_risk_30d DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.finance_aging_daily CASCADE;
CREATE MATERIALIZED VIEW mart.finance_aging_daily AS
WITH base AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.tipo_titulo,
    f.id_entidade,
    f.vencimento,
    GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))::numeric(18,2) AS valor_aberto
  FROM dw.fact_financeiro f
  WHERE f.vencimento IS NOT NULL
),
open_titles AS (
  SELECT *
  FROM base
  WHERE valor_aberto > 0
),
receber AS (
  SELECT
    id_empresa,
    id_filial,
    SUM(valor_aberto)::numeric(18,2) AS receber_total_aberto,
    SUM(CASE WHEN vencimento < current_date THEN valor_aberto ELSE 0 END)::numeric(18,2) AS receber_total_vencido
  FROM open_titles
  WHERE tipo_titulo = 1
  GROUP BY 1,2
),
pagar AS (
  SELECT
    id_empresa,
    id_filial,
    SUM(valor_aberto)::numeric(18,2) AS pagar_total_aberto,
    SUM(CASE WHEN vencimento < current_date THEN valor_aberto ELSE 0 END)::numeric(18,2) AS pagar_total_vencido
  FROM open_titles
  WHERE tipo_titulo = 0
  GROUP BY 1,2
),
buckets AS (
  SELECT
    id_empresa,
    id_filial,
    SUM(CASE WHEN (current_date - vencimento) BETWEEN 0 AND 7 THEN valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_0_7,
    SUM(CASE WHEN (current_date - vencimento) BETWEEN 8 AND 15 THEN valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_8_15,
    SUM(CASE WHEN (current_date - vencimento) BETWEEN 16 AND 30 THEN valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_16_30,
    SUM(CASE WHEN (current_date - vencimento) BETWEEN 31 AND 60 THEN valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_31_60,
    SUM(CASE WHEN (current_date - vencimento) > 60 THEN valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_60_plus
  FROM open_titles
  WHERE tipo_titulo = 1
    AND vencimento < current_date
  GROUP BY 1,2
),
top5 AS (
  SELECT
    id_empresa,
    id_filial,
    SUM(valor_aberto)::numeric(18,2) AS total_vencido,
    COALESCE(
      (
        SELECT SUM(t.valor_aberto)::numeric(18,2)
        FROM (
          SELECT valor_aberto
          FROM open_titles o2
          WHERE o2.id_empresa=o.id_empresa
            AND o2.id_filial=o.id_filial
            AND o2.tipo_titulo=1
            AND o2.vencimento < current_date
          ORDER BY valor_aberto DESC
          LIMIT 5
        ) t
      ),
      0::numeric
    ) AS top5_vencido
  FROM open_titles o
  WHERE tipo_titulo = 1
    AND vencimento < current_date
  GROUP BY 1,2
),
keys AS (
  SELECT DISTINCT id_empresa, id_filial FROM dw.fact_financeiro
  UNION
  SELECT DISTINCT id_empresa, id_filial FROM dw.fact_venda
)
SELECT
  current_date AS dt_ref,
  k.id_empresa,
  k.id_filial,
  COALESCE(r.receber_total_aberto,0)::numeric(18,2) AS receber_total_aberto,
  COALESCE(r.receber_total_vencido,0)::numeric(18,2) AS receber_total_vencido,
  COALESCE(p.pagar_total_aberto,0)::numeric(18,2) AS pagar_total_aberto,
  COALESCE(p.pagar_total_vencido,0)::numeric(18,2) AS pagar_total_vencido,
  COALESCE(b.bucket_0_7,0)::numeric(18,2) AS bucket_0_7,
  COALESCE(b.bucket_8_15,0)::numeric(18,2) AS bucket_8_15,
  COALESCE(b.bucket_16_30,0)::numeric(18,2) AS bucket_16_30,
  COALESCE(b.bucket_31_60,0)::numeric(18,2) AS bucket_31_60,
  COALESCE(b.bucket_60_plus,0)::numeric(18,2) AS bucket_60_plus,
  CASE WHEN COALESCE(t.total_vencido,0) > 0 THEN (t.top5_vencido / t.total_vencido * 100)::numeric(10,2) ELSE 0::numeric(10,2) END AS top5_concentration_pct,
  (COALESCE(r.receber_total_aberto,0)=0 AND COALESCE(p.pagar_total_aberto,0)=0) AS data_gaps,
  now() AS updated_at
FROM keys k
LEFT JOIN receber r ON r.id_empresa=k.id_empresa AND r.id_filial=k.id_filial
LEFT JOIN pagar p ON p.id_empresa=k.id_empresa AND p.id_filial=k.id_filial
LEFT JOIN buckets b ON b.id_empresa=k.id_empresa AND b.id_filial=k.id_filial
LEFT JOIN top5 t ON t.id_empresa=k.id_empresa AND t.id_filial=k.id_filial;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_finance_aging_daily
  ON mart.finance_aging_daily (dt_ref, id_empresa, id_filial);

DROP MATERIALIZED VIEW IF EXISTS mart.health_score_daily CASCADE;
CREATE MATERIALIZED VIEW mart.health_score_daily AS
WITH sales AS (
  SELECT
    id_empresa,
    id_filial,
    COALESCE(SUM(faturamento),0)::numeric(18,2) AS fat_30d,
    COALESCE(SUM(margem),0)::numeric(18,2) AS margem_30d,
    COALESCE(AVG(ticket_medio),0)::numeric(18,2) AS ticket_30d
  FROM mart.agg_vendas_diaria
  WHERE data_key >= to_char(current_date - interval '30 day','YYYYMMDD')::int
  GROUP BY 1,2
),
risk AS (
  SELECT
    id_empresa,
    id_filial,
    COALESCE(SUM(eventos_alto_risco),0)::int AS high_risk_30d,
    COALESCE(SUM(eventos_risco_total),0)::int AS total_risk_30d,
    COALESCE(SUM(impacto_estimado_total),0)::numeric(18,2) AS impacto_risco_30d
  FROM mart.agg_risco_diaria
  WHERE data_key >= to_char(current_date - interval '30 day','YYYYMMDD')::int
  GROUP BY 1,2
),
churn AS (
  SELECT
    id_empresa,
    id_filial,
    COALESCE(AVG(churn_score),0)::numeric(10,2) AS churn_score_avg,
    COALESCE(SUM(revenue_at_risk_30d),0)::numeric(18,2) AS revenue_at_risk_30d
  FROM mart.customer_churn_risk_daily
  GROUP BY 1,2
),
fin AS (
  SELECT
    id_empresa,
    id_filial,
    receber_total_aberto,
    receber_total_vencido,
    pagar_total_aberto,
    pagar_total_vencido,
    data_gaps
  FROM mart.finance_aging_daily
),
keys AS (
  SELECT DISTINCT id_empresa, id_filial FROM mart.agg_vendas_diaria
  UNION
  SELECT DISTINCT id_empresa, id_filial FROM mart.agg_risco_diaria
  UNION
  SELECT DISTINCT id_empresa, id_filial FROM mart.finance_aging_daily
),
comp AS (
  SELECT
    current_date AS dt_ref,
    k.id_empresa,
    k.id_filial,
    COALESCE(s.fat_30d,0) AS fat_30d,
    COALESCE(s.margem_30d,0) AS margem_30d,
    COALESCE(s.ticket_30d,0) AS ticket_30d,
    COALESCE(r.high_risk_30d,0) AS high_risk_30d,
    COALESCE(r.total_risk_30d,0) AS total_risk_30d,
    COALESCE(r.impacto_risco_30d,0) AS impacto_risco_30d,
    COALESCE(c.churn_score_avg,0) AS churn_score_avg,
    COALESCE(c.revenue_at_risk_30d,0) AS revenue_at_risk_30d,
    COALESCE(f.receber_total_aberto,0) AS receber_total_aberto,
    COALESCE(f.receber_total_vencido,0) AS receber_total_vencido,
    COALESCE(f.pagar_total_aberto,0) AS pagar_total_aberto,
    COALESCE(f.pagar_total_vencido,0) AS pagar_total_vencido,
    COALESCE(f.data_gaps, true) AS finance_data_gaps
  FROM keys k
  LEFT JOIN sales s ON s.id_empresa=k.id_empresa AND s.id_filial=k.id_filial
  LEFT JOIN risk r ON r.id_empresa=k.id_empresa AND r.id_filial=k.id_filial
  LEFT JOIN churn c ON c.id_empresa=k.id_empresa AND c.id_filial=k.id_filial
  LEFT JOIN fin f ON f.id_empresa=k.id_empresa AND f.id_filial=k.id_filial
),
scored AS (
  SELECT
    c.*,
    LEAST(100, GREATEST(0, CASE WHEN c.fat_30d > 0 THEN (c.margem_30d / c.fat_30d) * 500 ELSE 0 END))::numeric(10,2) AS comp_margem,
    LEAST(100, GREATEST(0, 100 - (CASE WHEN c.total_risk_30d > 0 THEN (c.high_risk_30d::numeric / c.total_risk_30d) * 120 ELSE 0 END) - (c.impacto_risco_30d / GREATEST(c.fat_30d,1)) * 100))::numeric(10,2) AS comp_fraude,
    LEAST(100, GREATEST(0, 100 - c.churn_score_avg))::numeric(10,2) AS comp_churn,
    LEAST(100, GREATEST(0, 100 - (CASE WHEN c.receber_total_aberto > 0 THEN (c.receber_total_vencido / c.receber_total_aberto) * 120 ELSE 0 END)))::numeric(10,2) AS comp_finance,
    LEAST(100, GREATEST(0, (c.ticket_30d / 120) * 100))::numeric(10,2) AS comp_operacao,
    CASE WHEN c.finance_data_gaps THEN 45::numeric(10,2) ELSE 90::numeric(10,2) END AS comp_dados
  FROM comp c
),
final AS (
  SELECT
    s.dt_ref,
    s.id_empresa,
    s.id_filial,
    s.comp_margem,
    s.comp_fraude,
    s.comp_churn,
    s.comp_finance,
    s.comp_operacao,
    s.comp_dados,
    LEAST(
      CASE
        WHEN s.receber_total_aberto > 0 AND (s.receber_total_vencido / s.receber_total_aberto) > 0.60 THEN 55
        WHEN s.receber_total_aberto > 0 AND (s.receber_total_vencido / s.receber_total_aberto) > 0.40 THEN 65
        ELSE 100
      END,
      CASE
        WHEN s.comp_dados < 60 THEN 75
        ELSE 100
      END,
      GREATEST(0, (s.comp_margem*0.22 + s.comp_fraude*0.23 + s.comp_churn*0.20 + s.comp_finance*0.20 + s.comp_operacao*0.10 + s.comp_dados*0.05))
    )::numeric(10,2) AS score_total,
    jsonb_build_object(
      'margem', s.comp_margem,
      'fraude', s.comp_fraude,
      'churn', s.comp_churn,
      'finance', s.comp_finance,
      'operacao', s.comp_operacao,
      'dados', s.comp_dados
    ) AS components,
    jsonb_build_object(
      'finance_overdue_ratio', CASE WHEN s.receber_total_aberto > 0 THEN round((s.receber_total_vencido/s.receber_total_aberto)::numeric,4) ELSE 0 END,
      'finance_data_gaps', s.finance_data_gaps,
      'revenue_at_risk_30d', s.revenue_at_risk_30d,
      'high_risk_30d', s.high_risk_30d
    ) AS reasons,
    now() AS updated_at
  FROM scored s
)
SELECT * FROM final;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_health_score_daily
  ON mart.health_score_daily (dt_ref, id_empresa, id_filial);

CREATE OR REPLACE FUNCTION etl.refresh_marts(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
DECLARE
  v_meta jsonb := '{}'::jsonb;
  v_sales_changed boolean := COALESCE((p_changed->>'fact_venda')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_venda_item')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_fin_changed boolean := COALESCE((p_changed->>'fact_financeiro')::int,0) > 0;
  v_risk_changed boolean := COALESCE((p_changed->>'risk_events')::int,0) > 0;
BEGIN
  IF v_sales_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria;
    REFRESH MATERIALIZED VIEW mart.insights_base_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_vendas_hora;
    REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
    REFRESH MATERIALIZED VIEW mart.clientes_churn_risco;
    REFRESH MATERIALIZED VIEW mart.customer_rfm_daily;
    REFRESH MATERIALIZED VIEW mart.customer_churn_risk_daily;
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', true, 'churn_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', false, 'churn_marts_refreshed', false);
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    REFRESH MATERIALIZED VIEW mart.finance_aging_daily;
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', true, 'finance_aging_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', false, 'finance_aging_refreshed', false);
  END IF;

  IF v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', false);
  END IF;

  IF v_sales_changed OR v_fin_changed OR v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.health_score_daily;
    v_meta := v_meta || jsonb_build_object('health_score_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('health_score_refreshed', false);
  END IF;

  RETURN v_meta;
END;
$$ LANGUAGE plpgsql;

COMMIT;
