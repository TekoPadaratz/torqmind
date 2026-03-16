BEGIN;

-- ==========================================
-- Official Xpert payment taxonomy
-- ==========================================

INSERT INTO app.payment_type_map (id_empresa, tipo_forma, label, category, severity_hint, active)
VALUES
  (NULL, 0,  'DINHEIRO',            'DINHEIRO',        'INFO', true),
  (NULL, 1,  'PRAZO',               'PRAZO',           'WARN', true),
  (NULL, 2,  'CHEQUE PRE',          'CHEQUE_PRE',      'WARN', true),
  (NULL, 3,  'CARTÃO DE CRÉDITO',   'CARTAO_CREDITO',  'INFO', true),
  (NULL, 4,  'CARTÃO DE DÉBITO',    'CARTAO_DEBITO',   'INFO', true),
  (NULL, 5,  'CARTA FRETE',         'CARTA_FRETE',     'WARN', true),
  (NULL, 6,  'CHEQUE A PAGAR',      'CHEQUE_A_PAGAR',  'WARN', true),
  (NULL, 7,  'CHEQUE A VISTA',      'CHEQUE_A_VISTA',  'WARN', true),
  (NULL, 8,  'MOEDAS DIFERESAS',    'MOEDAS_DIFERESAS','WARN', true),
  (NULL, 9,  'OUTROS PAGOS',        'OUTROS_PAGOS',    'WARN', true),
  (NULL, 10, 'CHEQUE PRÓPRIO',      'CHEQUE_PROPRIO',  'WARN', true),
  (NULL, 28, 'PIX',                 'PIX',             'INFO', true)
ON CONFLICT (id_empresa_nk, tipo_forma)
DO UPDATE SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  severity_hint = EXCLUDED.severity_hint,
  active = EXCLUDED.active,
  updated_at = now();

UPDATE app.payment_type_map
SET active = false,
    updated_at = now()
WHERE id_empresa IS NULL
  AND tipo_forma NOT IN (0,1,2,3,4,5,6,7,8,9,10,28)
  AND active = true;

DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.agg_pagamentos_diaria AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, 'NÃO IDENTIFICADO') AS label,
    COALESCE(m.category, 'NAO_IDENTIFICADO') AS category,
    COALESCE(m.severity_hint, 'WARN') AS severity_hint
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category, severity_hint
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  tipo_forma,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  CASE WHEN COALESCE(SUM(SUM(valor)) OVER (PARTITION BY id_empresa,id_filial,data_key),0) = 0 THEN 0
       ELSE ((SUM(valor) / NULLIF(SUM(SUM(valor)) OVER (PARTITION BY id_empresa,id_filial,data_key),0)) * 100)
  END::numeric(10,2) AS share_percent,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, tipo_forma, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_diaria
  ON mart.agg_pagamentos_diaria (id_empresa, id_filial, data_key, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_diaria_lookup
  ON mart.agg_pagamentos_diaria (id_empresa, data_key, id_filial, total_valor DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_turno CASCADE;
CREATE MATERIALIZED VIEW mart.agg_pagamentos_turno AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    COALESCE(f.id_turno, -1) AS id_turno,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, 'NÃO IDENTIFICADO') AS label,
    COALESCE(m.category, 'NAO_IDENTIFICADO') AS category
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  id_turno,
  tipo_forma,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, id_turno, tipo_forma, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_turno
  ON mart.agg_pagamentos_turno (id_empresa, id_filial, data_key, id_turno, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_turno_lookup
  ON mart.agg_pagamentos_turno (id_empresa, data_key, id_filial, id_turno);

DROP MATERIALIZED VIEW IF EXISTS mart.pagamentos_anomalias_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.pagamentos_anomalias_diaria AS
WITH base_ref AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    COUNT(*)::int AS qtd_formas,
    COUNT(*) FILTER (WHERE COALESCE(m.category, 'NAO_IDENTIFICADO') = 'NAO_IDENTIFICADO')::int AS qtd_desconhecido,
    COALESCE(SUM(f.valor),0)::numeric(18,2) AS valor_total,
    COALESCE(SUM(CASE WHEN COALESCE(m.category, 'NAO_IDENTIFICADO') = 'PIX' THEN f.valor ELSE 0 END),0)::numeric(18,2) AS valor_pix,
    COALESCE(MIN(f.id_turno), -1) AS id_turno
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
  GROUP BY 1,2,3,4
), split_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    AVG(qtd_formas)::numeric(10,2) AS avg_formas,
    COUNT(*) FILTER (WHERE qtd_formas >= 3)::int AS comprovantes_multiplos,
    COUNT(*)::int AS comprovantes_total,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
  FROM base_ref
  GROUP BY 1,2,3
), split_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'SPLIT_INCOMUM'::text AS event_type,
    CASE WHEN avg_formas >= 2.4 THEN 'CRITICAL' WHEN avg_formas >= 1.8 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, ROUND((avg_formas - 1.4) * 55 + comprovantes_multiplos * 0.8)))::int AS score,
    COALESCE(valor_total,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'avg_formas_por_comprovante', avg_formas,
      'comprovantes_multiplos', comprovantes_multiplos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM split_daily
  WHERE comprovantes_total >= 20
), unknown_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    'FORMA_NAO_IDENTIFICADA'::text AS event_type,
    CASE WHEN qtd_desconhecido >= 4 THEN 'CRITICAL' WHEN qtd_desconhecido >= 2 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, qtd_desconhecido * 18 + (valor_total / 500)::int))::int AS score,
    valor_total AS impacto_estimado,
    jsonb_build_object(
      'qtd_formas_nao_identificadas', qtd_desconhecido,
      'valor_total', valor_total
    ) AS reasons
  FROM (
    SELECT
      id_empresa,
      id_filial,
      data_key,
      id_turno,
      SUM(qtd_desconhecido)::int AS qtd_desconhecido,
      COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
    FROM base_ref
    WHERE qtd_desconhecido > 0
    GROUP BY 1,2,3,4
  ) u
), pix_signal AS (
  SELECT
    b.id_empresa,
    b.id_filial,
    b.data_key,
    b.id_turno,
    'PIX_DESVIO_TURNO'::text AS event_type,
    CASE WHEN (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.80 THEN 'CRITICAL'
         WHEN (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.60 THEN 'WARN'
         ELSE 'INFO'
    END AS severity,
    LEAST(100, GREATEST(0, ROUND((b.valor_pix / NULLIF(b.valor_total, 0)) * 100)))::int AS score,
    b.valor_pix::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'valor_pix', b.valor_pix,
      'valor_total', b.valor_total,
      'share_pix', ROUND((b.valor_pix / NULLIF(b.valor_total, 0))::numeric, 4)
    ) AS reasons
  FROM (
    SELECT
      id_empresa,
      id_filial,
      data_key,
      id_turno,
      COALESCE(SUM(valor_pix),0)::numeric(18,2) AS valor_pix,
      COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
    FROM base_ref
    GROUP BY 1,2,3,4
  ) b
  WHERE b.valor_pix > 0
    AND b.valor_total > 0
    AND (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.60
)
SELECT
  s.id_empresa,
  s.id_filial,
  s.data_key,
  s.id_turno,
  s.event_type,
  s.severity,
  s.score,
  s.impacto_estimado,
  s.reasons,
  md5(concat_ws('|', s.id_empresa::text, s.id_filial::text, s.data_key::text, COALESCE(s.id_turno,-1)::text, s.event_type)) AS insight_id_hash,
  NULL::bigint AS insight_id,
  now() AS updated_at
FROM (
  SELECT * FROM split_signal
  UNION ALL
  SELECT * FROM unknown_signal
  UNION ALL
  SELECT * FROM pix_signal
) s;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_pagamentos_anomalias_diaria
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, event_type, COALESCE(id_turno,-1));
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_lookup
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, severity, score DESC);
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_insight
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, insight_id_hash);

DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_forma_pagamento CASCADE;
CREATE MATERIALIZED VIEW mart.agg_caixa_forma_pagamento AS
SELECT
  t.id_empresa,
  t.id_filial,
  t.id_turno,
  p.tipo_forma,
  COALESCE(m.label, 'NÃO IDENTIFICADO') AS forma_label,
  COALESCE(m.category, 'NAO_IDENTIFICADO') AS forma_category,
  COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM dw.fact_caixa_turno t
JOIN dw.fact_pagamento_comprovante p
  ON p.id_empresa = t.id_empresa
 AND p.id_filial = t.id_filial
 AND p.id_turno = t.id_turno
LEFT JOIN LATERAL (
  SELECT label, category
  FROM app.payment_type_map m
  WHERE m.tipo_forma = p.tipo_forma
    AND m.active = true
    AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
  ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
  LIMIT 1
) m ON true
WHERE t.is_aberto = true
GROUP BY t.id_empresa, t.id_filial, t.id_turno, p.tipo_forma, COALESCE(m.label, 'NÃO IDENTIFICADO'), COALESCE(m.category, 'NAO_IDENTIFICADO');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_forma_pagamento
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, id_turno, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_forma_pagamento_lookup
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, total_valor DESC);

-- ==========================================
-- Historical executive snapshots by dt_ref
-- ==========================================

DROP MATERIALIZED VIEW IF EXISTS mart.health_score_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.finance_aging_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.customer_churn_risk_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.customer_rfm_daily CASCADE;

CREATE TABLE IF NOT EXISTS mart.customer_rfm_daily (
  dt_ref              date NOT NULL,
  id_empresa          integer NOT NULL,
  id_filial           integer NOT NULL,
  id_cliente          integer NOT NULL,
  cliente_nome        text NOT NULL,
  last_purchase       date NULL,
  recency_days        integer NOT NULL,
  frequency_30        integer NOT NULL,
  frequency_90        integer NOT NULL,
  monetary_30         numeric(18,2) NOT NULL,
  monetary_90         numeric(18,2) NOT NULL,
  ticket_30           numeric(18,2) NOT NULL,
  expected_cycle_days numeric(10,2) NOT NULL,
  trend_frequency     integer NOT NULL,
  trend_monetary      numeric(18,2) NOT NULL,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dt_ref, id_empresa, id_filial, id_cliente)
);

CREATE INDEX IF NOT EXISTS ix_mart_customer_rfm_lookup
  ON mart.customer_rfm_daily (id_empresa, id_filial, recency_days DESC);

CREATE TABLE IF NOT EXISTS mart.customer_churn_risk_daily (
  dt_ref              date NOT NULL,
  id_empresa          integer NOT NULL,
  id_filial           integer NOT NULL,
  id_cliente          integer NOT NULL,
  cliente_nome        text NOT NULL,
  last_purchase       date NULL,
  recency_days        integer NOT NULL,
  frequency_30        integer NOT NULL,
  frequency_90        integer NOT NULL,
  monetary_30         numeric(18,2) NOT NULL,
  monetary_90         numeric(18,2) NOT NULL,
  ticket_30           numeric(18,2) NOT NULL,
  expected_cycle_days numeric(10,2) NOT NULL,
  churn_score         integer NOT NULL,
  revenue_at_risk_30d numeric(18,2) NOT NULL,
  recommendation      text NOT NULL,
  reasons             jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dt_ref, id_empresa, id_filial, id_cliente)
);

CREATE INDEX IF NOT EXISTS ix_mart_customer_churn_risk_score
  ON mart.customer_churn_risk_daily (id_empresa, id_filial, dt_ref, churn_score DESC, revenue_at_risk_30d DESC);

CREATE TABLE IF NOT EXISTS mart.finance_aging_daily (
  dt_ref                   date NOT NULL,
  id_empresa               integer NOT NULL,
  id_filial                integer NOT NULL,
  receber_total_aberto     numeric(18,2) NOT NULL,
  receber_total_vencido    numeric(18,2) NOT NULL,
  pagar_total_aberto       numeric(18,2) NOT NULL,
  pagar_total_vencido      numeric(18,2) NOT NULL,
  bucket_0_7               numeric(18,2) NOT NULL,
  bucket_8_15              numeric(18,2) NOT NULL,
  bucket_16_30             numeric(18,2) NOT NULL,
  bucket_31_60             numeric(18,2) NOT NULL,
  bucket_60_plus           numeric(18,2) NOT NULL,
  top5_concentration_pct   numeric(10,2) NOT NULL,
  data_gaps                boolean NOT NULL,
  updated_at               timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dt_ref, id_empresa, id_filial)
);

CREATE TABLE IF NOT EXISTS mart.health_score_daily (
  dt_ref         date NOT NULL,
  id_empresa     integer NOT NULL,
  id_filial      integer NOT NULL,
  comp_margem    numeric(10,2) NOT NULL,
  comp_fraude    numeric(10,2) NOT NULL,
  comp_churn     numeric(10,2) NOT NULL,
  comp_finance   numeric(10,2) NOT NULL,
  comp_operacao  numeric(10,2) NOT NULL,
  comp_dados     numeric(10,2) NOT NULL,
  score_total    numeric(10,2) NOT NULL,
  components     jsonb NOT NULL DEFAULT '{}'::jsonb,
  reasons        jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dt_ref, id_empresa, id_filial)
);

CREATE OR REPLACE FUNCTION etl.rebuild_customer_rfm_daily(
  p_start_date date DEFAULT NULL,
  p_end_date date DEFAULT NULL
)
RETURNS integer AS $$
DECLARE
  v_start_date date;
  v_end_date date;
  v_rows integer := 0;
BEGIN
  SELECT COALESCE(MAX(v.data::date), current_date) INTO v_end_date
  FROM dw.fact_venda v
  WHERE v.data IS NOT NULL;

  v_end_date := COALESCE(p_end_date, v_end_date, current_date);
  v_start_date := COALESCE(p_start_date, v_end_date - 180);

  DELETE FROM mart.customer_rfm_daily
  WHERE dt_ref BETWEEN v_start_date AND v_end_date;

  WITH refs AS (
    SELECT d::date AS dt_ref
    FROM generate_series(v_start_date, v_end_date, interval '1 day') d
  ), sales AS (
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
      AND v.id_cliente IS NOT NULL
      AND v.data IS NOT NULL
      AND v.data::date BETWEEN v_start_date - 180 AND v_end_date
    GROUP BY 1,2,3,4
  ), purchase_history AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      s.dt_compra,
      LAG(s.dt_compra) OVER (
        PARTITION BY s.id_empresa, s.id_filial, s.id_cliente
        ORDER BY s.dt_compra
      ) AS prev_dt
    FROM sales s
  ), agg AS (
    SELECT
      r.dt_ref,
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      MAX(s.dt_compra) AS last_purchase,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 29 AND r.dt_ref THEN s.compras_dia ELSE 0 END)::int AS frequency_30,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 89 AND r.dt_ref THEN s.compras_dia ELSE 0 END)::int AS frequency_90,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 29 AND r.dt_ref THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_30,
      SUM(CASE WHEN s.dt_compra BETWEEN r.dt_ref - 89 AND r.dt_ref THEN s.valor_dia ELSE 0 END)::numeric(18,2) AS monetary_90
    FROM refs r
    JOIN sales s
      ON s.dt_compra BETWEEN r.dt_ref - 179 AND r.dt_ref
    GROUP BY 1,2,3,4
  ), expected_cycle AS (
    SELECT
      r.dt_ref,
      ph.id_empresa,
      ph.id_filial,
      ph.id_cliente,
      COALESCE(
        percentile_cont(0.5) WITHIN GROUP (
          ORDER BY GREATEST(1, (ph.dt_compra - ph.prev_dt))
        ),
        30
      )::numeric(10,2) AS expected_cycle_days
    FROM refs r
    JOIN purchase_history ph
      ON ph.prev_dt IS NOT NULL
     AND ph.dt_compra BETWEEN r.dt_ref - 179 AND r.dt_ref
    GROUP BY 1,2,3,4
  ), final AS (
    SELECT
      a.dt_ref,
      a.id_empresa,
      a.id_filial,
      a.id_cliente,
      COALESCE(NULLIF(c.nome,''), '#ID ' || a.id_cliente::text) AS cliente_nome,
      a.last_purchase,
      GREATEST(0, (a.dt_ref - a.last_purchase))::int AS recency_days,
      a.frequency_30,
      a.frequency_90,
      a.monetary_30,
      a.monetary_90,
      CASE WHEN a.frequency_30 > 0 THEN (a.monetary_30 / a.frequency_30)::numeric(18,2) ELSE 0::numeric(18,2) END AS ticket_30,
      COALESCE(ec.expected_cycle_days, 30)::numeric(10,2) AS expected_cycle_days,
      GREATEST(0, a.frequency_30 - GREATEST(0, a.frequency_90 - a.frequency_30))::int AS trend_frequency,
      (a.monetary_30 - GREATEST(0::numeric, a.monetary_90 - a.monetary_30))::numeric(18,2) AS trend_monetary
    FROM agg a
    LEFT JOIN expected_cycle ec
      ON ec.dt_ref = a.dt_ref
     AND ec.id_empresa = a.id_empresa
     AND ec.id_filial = a.id_filial
     AND ec.id_cliente = a.id_cliente
    LEFT JOIN LATERAL (
      SELECT d.nome
      FROM dw.dim_cliente d
      WHERE d.id_empresa = a.id_empresa
        AND d.id_cliente = a.id_cliente
      ORDER BY (d.id_filial = a.id_filial) DESC, d.updated_at DESC
      LIMIT 1
    ) c ON true
  ), inserted AS (
    INSERT INTO mart.customer_rfm_daily (
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      trend_frequency,trend_monetary,updated_at
    )
    SELECT
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      trend_frequency,trend_monetary,now()
    FROM final
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.rebuild_customer_churn_risk_daily(
  p_start_date date DEFAULT NULL,
  p_end_date date DEFAULT NULL
)
RETURNS integer AS $$
DECLARE
  v_start_date date;
  v_end_date date;
  v_rows integer := 0;
BEGIN
  SELECT COALESCE(MAX(dt_ref), current_date) INTO v_end_date
  FROM mart.customer_rfm_daily;

  v_end_date := COALESCE(p_end_date, v_end_date, current_date);
  v_start_date := COALESCE(p_start_date, v_end_date - 180);

  DELETE FROM mart.customer_churn_risk_daily
  WHERE dt_ref BETWEEN v_start_date AND v_end_date;

  WITH base AS (
    SELECT
      r.*,
      GREATEST(0, r.frequency_90 - r.frequency_30)::int AS frequency_prev_60,
      GREATEST(0::numeric, r.monetary_90 - r.monetary_30)::numeric(18,2) AS monetary_prev_60
    FROM mart.customer_rfm_daily r
    WHERE r.dt_ref BETWEEN v_start_date AND v_end_date
  ), scored AS (
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
  ), inserted AS (
    INSERT INTO mart.customer_churn_risk_daily (
      dt_ref,id_empresa,id_filial,id_cliente,cliente_nome,last_purchase,recency_days,
      frequency_30,frequency_90,monetary_30,monetary_90,ticket_30,expected_cycle_days,
      churn_score,revenue_at_risk_30d,recommendation,reasons,updated_at
    )
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
      CASE
        WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 70 THEN 'Contato imediato + oferta de recuperação em 24h'
        WHEN (s.p_cycle_break + s.p_freq_drop + s.p_monetary_drop) >= 50 THEN 'Campanha personalizada e follow-up comercial em 7 dias'
        ELSE 'Monitorar jornada e reforçar frequência com benefícios'
      END AS recommendation,
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
      now()
    FROM scored s
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.rebuild_finance_aging_daily(
  p_start_date date DEFAULT NULL,
  p_end_date date DEFAULT NULL
)
RETURNS integer AS $$
DECLARE
  v_start_date date;
  v_end_date date;
  v_rows integer := 0;
BEGIN
  SELECT COALESCE(MAX(COALESCE(vencimento, data_emissao)), current_date) INTO v_end_date
  FROM dw.fact_financeiro;

  v_end_date := COALESCE(p_end_date, v_end_date, current_date);
  v_start_date := COALESCE(p_start_date, v_end_date - 180);

  DELETE FROM mart.finance_aging_daily
  WHERE dt_ref BETWEEN v_start_date AND v_end_date;

  WITH refs AS (
    SELECT d::date AS dt_ref
    FROM generate_series(v_start_date, v_end_date, interval '1 day') d
  ), base AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.tipo_titulo,
      f.id_entidade,
      COALESCE(f.vencimento, f.data_emissao) AS vencimento,
      COALESCE(f.valor,0)::numeric(18,2) AS valor_total,
      COALESCE(f.valor_pago,0)::numeric(18,2) AS valor_pago,
      f.data_pagamento
    FROM dw.fact_financeiro f
    WHERE COALESCE(f.vencimento, f.data_emissao) IS NOT NULL
      AND COALESCE(f.vencimento, f.data_emissao) <= v_end_date
  ), titles_by_ref AS (
    SELECT
      r.dt_ref,
      b.id_empresa,
      b.id_filial,
      b.tipo_titulo,
      b.id_entidade,
      b.vencimento,
      CASE
        WHEN b.data_pagamento IS NULL THEN GREATEST(0::numeric, b.valor_total - b.valor_pago)
        WHEN b.data_pagamento > r.dt_ref THEN GREATEST(0::numeric, b.valor_total)
        ELSE GREATEST(0::numeric, b.valor_total - b.valor_pago)
      END::numeric(18,2) AS valor_aberto
    FROM refs r
    JOIN base b
      ON b.vencimento <= r.dt_ref
     AND (b.data_pagamento IS NULL OR b.data_pagamento > r.dt_ref OR (b.valor_total - b.valor_pago) > 0)
  ), open_titles AS (
    SELECT *
    FROM titles_by_ref
    WHERE valor_aberto > 0
  ), overdue_rank AS (
    SELECT
      o.dt_ref,
      o.id_empresa,
      o.id_filial,
      o.valor_aberto,
      ROW_NUMBER() OVER (
        PARTITION BY o.dt_ref, o.id_empresa, o.id_filial
        ORDER BY o.valor_aberto DESC
      ) AS rn
    FROM open_titles o
    WHERE o.tipo_titulo = 1
      AND o.vencimento < o.dt_ref
  ), top5 AS (
    SELECT
      dt_ref,
      id_empresa,
      id_filial,
      COALESCE(SUM(valor_aberto),0)::numeric(18,2) AS top5_vencido
    FROM overdue_rank
    WHERE rn <= 5
    GROUP BY 1,2,3
  ), totals AS (
    SELECT
      o.dt_ref,
      o.id_empresa,
      o.id_filial,
      SUM(CASE WHEN o.tipo_titulo = 1 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS receber_total_aberto,
      SUM(CASE WHEN o.tipo_titulo = 1 AND o.vencimento < o.dt_ref THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS receber_total_vencido,
      SUM(CASE WHEN o.tipo_titulo = 0 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS pagar_total_aberto,
      SUM(CASE WHEN o.tipo_titulo = 0 AND o.vencimento < o.dt_ref THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS pagar_total_vencido,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 0 AND 7 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_0_7,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 8 AND 15 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_8_15,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 16 AND 30 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_16_30,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) BETWEEN 31 AND 60 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_31_60,
      SUM(CASE WHEN o.tipo_titulo = 1 AND (o.dt_ref - o.vencimento) > 60 THEN o.valor_aberto ELSE 0 END)::numeric(18,2) AS bucket_60_plus
    FROM open_titles o
    GROUP BY 1,2,3
  ), keys AS (
    SELECT DISTINCT r.dt_ref, k.id_empresa, k.id_filial
    FROM refs r
    CROSS JOIN (
      SELECT DISTINCT id_empresa, id_filial FROM dw.fact_financeiro
      UNION
      SELECT DISTINCT id_empresa, id_filial FROM dw.fact_venda
    ) k
  ), inserted AS (
    INSERT INTO mart.finance_aging_daily (
      dt_ref,id_empresa,id_filial,receber_total_aberto,receber_total_vencido,pagar_total_aberto,pagar_total_vencido,
      bucket_0_7,bucket_8_15,bucket_16_30,bucket_31_60,bucket_60_plus,top5_concentration_pct,data_gaps,updated_at
    )
    SELECT
      k.dt_ref,
      k.id_empresa,
      k.id_filial,
      COALESCE(t.receber_total_aberto,0)::numeric(18,2),
      COALESCE(t.receber_total_vencido,0)::numeric(18,2),
      COALESCE(t.pagar_total_aberto,0)::numeric(18,2),
      COALESCE(t.pagar_total_vencido,0)::numeric(18,2),
      COALESCE(t.bucket_0_7,0)::numeric(18,2),
      COALESCE(t.bucket_8_15,0)::numeric(18,2),
      COALESCE(t.bucket_16_30,0)::numeric(18,2),
      COALESCE(t.bucket_31_60,0)::numeric(18,2),
      COALESCE(t.bucket_60_plus,0)::numeric(18,2),
      CASE WHEN COALESCE(t.receber_total_vencido,0) > 0
        THEN (COALESCE(tp.top5_vencido,0) / NULLIF(t.receber_total_vencido,0) * 100)::numeric(10,2)
        ELSE 0::numeric(10,2)
      END AS top5_concentration_pct,
      (COALESCE(t.receber_total_aberto,0)=0 AND COALESCE(t.pagar_total_aberto,0)=0) AS data_gaps,
      now()
    FROM keys k
    LEFT JOIN totals t
      ON t.dt_ref = k.dt_ref
     AND t.id_empresa = k.id_empresa
     AND t.id_filial = k.id_filial
    LEFT JOIN top5 tp
      ON tp.dt_ref = k.dt_ref
     AND tp.id_empresa = k.id_empresa
     AND tp.id_filial = k.id_filial
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.rebuild_health_score_daily(
  p_start_date date DEFAULT NULL,
  p_end_date date DEFAULT NULL
)
RETURNS integer AS $$
DECLARE
  v_start_date date;
  v_end_date date;
  v_rows integer := 0;
BEGIN
  SELECT GREATEST(
    COALESCE((SELECT MAX(to_date(data_key::text, 'YYYYMMDD')) FROM mart.agg_vendas_diaria), current_date),
    COALESCE((SELECT MAX(to_date(data_key::text, 'YYYYMMDD')) FROM mart.agg_risco_diaria), current_date),
    COALESCE((SELECT MAX(dt_ref) FROM mart.finance_aging_daily), current_date)
  )
  INTO v_end_date;

  v_end_date := COALESCE(p_end_date, v_end_date, current_date);
  v_start_date := COALESCE(p_start_date, v_end_date - 180);

  DELETE FROM mart.health_score_daily
  WHERE dt_ref BETWEEN v_start_date AND v_end_date;

  WITH refs AS (
    SELECT d::date AS dt_ref
    FROM generate_series(v_start_date, v_end_date, interval '1 day') d
  ), keys AS (
    SELECT DISTINCT r.dt_ref, k.id_empresa, k.id_filial
    FROM refs r
    CROSS JOIN (
      SELECT DISTINCT id_empresa, id_filial FROM mart.agg_vendas_diaria
      UNION
      SELECT DISTINCT id_empresa, id_filial FROM mart.agg_risco_diaria
      UNION
      SELECT DISTINCT id_empresa, id_filial FROM mart.finance_aging_daily
    ) k
  ), sales AS (
    SELECT
      r.dt_ref,
      v.id_empresa,
      v.id_filial,
      COALESCE(SUM(v.faturamento),0)::numeric(18,2) AS fat_30d,
      COALESCE(SUM(v.margem),0)::numeric(18,2) AS margem_30d,
      COALESCE(AVG(v.ticket_medio),0)::numeric(18,2) AS ticket_30d
    FROM refs r
    JOIN mart.agg_vendas_diaria v
      ON to_date(v.data_key::text, 'YYYYMMDD') BETWEEN r.dt_ref - 29 AND r.dt_ref
    GROUP BY 1,2,3
  ), risk AS (
    SELECT
      r.dt_ref,
      x.id_empresa,
      x.id_filial,
      COALESCE(SUM(x.eventos_alto_risco),0)::int AS high_risk_30d,
      COALESCE(SUM(x.eventos_risco_total),0)::int AS total_risk_30d,
      COALESCE(SUM(x.impacto_estimado_total),0)::numeric(18,2) AS impacto_risco_30d
    FROM refs r
    JOIN mart.agg_risco_diaria x
      ON to_date(x.data_key::text, 'YYYYMMDD') BETWEEN r.dt_ref - 29 AND r.dt_ref
    GROUP BY 1,2,3
  ), churn AS (
    SELECT
      dt_ref,
      id_empresa,
      id_filial,
      COALESCE(AVG(churn_score),0)::numeric(10,2) AS churn_score_avg,
      COALESCE(SUM(revenue_at_risk_30d),0)::numeric(18,2) AS revenue_at_risk_30d
    FROM mart.customer_churn_risk_daily
    WHERE dt_ref BETWEEN v_start_date AND v_end_date
    GROUP BY 1,2,3
  ), fin AS (
    SELECT
      dt_ref,
      id_empresa,
      id_filial,
      receber_total_aberto,
      receber_total_vencido,
      pagar_total_aberto,
      pagar_total_vencido,
      data_gaps
    FROM mart.finance_aging_daily
    WHERE dt_ref BETWEEN v_start_date AND v_end_date
  ), comp AS (
    SELECT
      k.dt_ref,
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
    LEFT JOIN sales s
      ON s.dt_ref = k.dt_ref
     AND s.id_empresa = k.id_empresa
     AND s.id_filial = k.id_filial
    LEFT JOIN risk r
      ON r.dt_ref = k.dt_ref
     AND r.id_empresa = k.id_empresa
     AND r.id_filial = k.id_filial
    LEFT JOIN churn c
      ON c.dt_ref = k.dt_ref
     AND c.id_empresa = k.id_empresa
     AND c.id_filial = k.id_filial
    LEFT JOIN fin f
      ON f.dt_ref = k.dt_ref
     AND f.id_empresa = k.id_empresa
     AND f.id_filial = k.id_filial
  ), scored AS (
    SELECT
      c.*,
      LEAST(100, GREATEST(0, CASE WHEN c.fat_30d > 0 THEN (c.margem_30d / c.fat_30d) * 500 ELSE 0 END))::numeric(10,2) AS comp_margem,
      LEAST(100, GREATEST(0, 100 - (CASE WHEN c.total_risk_30d > 0 THEN (c.high_risk_30d::numeric / c.total_risk_30d) * 120 ELSE 0 END) - (c.impacto_risco_30d / GREATEST(c.fat_30d,1)) * 100))::numeric(10,2) AS comp_fraude,
      LEAST(100, GREATEST(0, 100 - c.churn_score_avg))::numeric(10,2) AS comp_churn,
      LEAST(100, GREATEST(0, 100 - (CASE WHEN c.receber_total_aberto > 0 THEN (c.receber_total_vencido / c.receber_total_aberto) * 120 ELSE 0 END)))::numeric(10,2) AS comp_finance,
      LEAST(100, GREATEST(0, (c.ticket_30d / 120) * 100))::numeric(10,2) AS comp_operacao,
      CASE WHEN c.finance_data_gaps THEN 45::numeric(10,2) ELSE 90::numeric(10,2) END AS comp_dados
    FROM comp c
  ), inserted AS (
    INSERT INTO mart.health_score_daily (
      dt_ref,id_empresa,id_filial,comp_margem,comp_fraude,comp_churn,comp_finance,comp_operacao,comp_dados,
      score_total,components,reasons,updated_at
    )
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
      now()
    FROM scored s
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.refresh_marts(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
DECLARE
  v_meta jsonb := '{}'::jsonb;
  v_sales_changed boolean := COALESCE((p_changed->>'fact_venda')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_venda_item')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_fin_changed boolean := COALESCE((p_changed->>'fact_financeiro')::int,0) > 0;
  v_risk_changed boolean := COALESCE((p_changed->>'risk_events')::int,0) > 0;
  v_payment_changed boolean := COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
                            OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
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
    PERFORM etl.rebuild_customer_rfm_daily();
    PERFORM etl.rebuild_customer_churn_risk_daily();
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', true, 'churn_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', false, 'churn_marts_refreshed', false);
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    PERFORM etl.rebuild_finance_aging_daily();
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

  IF v_payment_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
    REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', false);
  END IF;

  IF v_sales_changed OR v_fin_changed OR v_risk_changed THEN
    PERFORM etl.rebuild_health_score_daily();
    v_meta := v_meta || jsonb_build_object('health_score_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('health_score_refreshed', false);
  END IF;

  RETURN v_meta;
END;
$$ LANGUAGE plpgsql;

COMMIT;
