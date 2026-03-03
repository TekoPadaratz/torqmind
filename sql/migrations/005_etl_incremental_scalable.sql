BEGIN;

-- ==========================================
-- ETL control tables hardening
-- ==========================================

ALTER TABLE etl.watermark
  ADD COLUMN IF NOT EXISTS last_ts timestamptz,
  ADD COLUMN IF NOT EXISTS last_id bigint;

UPDATE etl.watermark
SET last_ts = COALESCE(last_ts, last_ingested_at)
WHERE last_ts IS NULL;

CREATE INDEX IF NOT EXISTS ix_etl_watermark_empresa_updated
  ON etl.watermark (id_empresa, updated_at DESC);

ALTER TABLE etl.run_log
  ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'running',
  ADD COLUMN IF NOT EXISTS step_name text,
  ADD COLUMN IF NOT EXISTS rows_processed bigint,
  ADD COLUMN IF NOT EXISTS error text,
  ADD COLUMN IF NOT EXISTS duration_ms integer;

CREATE INDEX IF NOT EXISTS ix_etl_run_log_empresa_started
  ON etl.run_log (id_empresa, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_etl_run_log_empresa_step
  ON etl.run_log (id_empresa, step_name, started_at DESC);

-- ==========================================
-- Helpers
-- ==========================================

CREATE OR REPLACE FUNCTION etl.hot_window_days()
RETURNS integer AS $$
  SELECT COALESCE(NULLIF(current_setting('etl.hot_window_days', true), '')::int, 3);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.get_watermark(p_id_empresa int, p_dataset text)
RETURNS timestamptz AS $$
  SELECT COALESCE(last_ts, last_ingested_at)
  FROM etl.watermark
  WHERE id_empresa = p_id_empresa AND dataset = p_dataset;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION etl.set_watermark(
  p_id_empresa int,
  p_dataset text,
  p_ts timestamptz,
  p_last_id bigint DEFAULT NULL
)
RETURNS void AS $$
BEGIN
  INSERT INTO etl.watermark (id_empresa, dataset, last_ingested_at, last_ts, last_id)
  VALUES (
    p_id_empresa,
    p_dataset,
    COALESCE(p_ts, '1970-01-01'::timestamptz),
    COALESCE(p_ts, '1970-01-01'::timestamptz),
    p_last_id
  )
  ON CONFLICT (id_empresa, dataset)
  DO UPDATE SET
    last_ingested_at = EXCLUDED.last_ingested_at,
    last_ts = EXCLUDED.last_ts,
    last_id = COALESCE(EXCLUDED.last_id, etl.watermark.last_id),
    updated_at = now();
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS etl.set_watermark(integer, text, timestamptz);

CREATE OR REPLACE FUNCTION etl.log_step(
  p_id_empresa int,
  p_step_name text,
  p_started_at timestamptz,
  p_finished_at timestamptz,
  p_status text,
  p_rows_processed bigint,
  p_error text DEFAULT NULL,
  p_meta jsonb DEFAULT '{}'::jsonb
)
RETURNS void AS $$
BEGIN
  INSERT INTO etl.run_log (
    id_empresa,
    started_at,
    finished_at,
    status,
    step_name,
    rows_processed,
    error,
    duration_ms,
    meta
  )
  VALUES (
    p_id_empresa,
    p_started_at,
    p_finished_at,
    p_status,
    p_step_name,
    p_rows_processed,
    p_error,
    GREATEST(0, FLOOR(EXTRACT(epoch FROM (p_finished_at - p_started_at)) * 1000)::int),
    COALESCE(p_meta, '{}'::jsonb)
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.effective_from_ts(p_id_empresa int, p_dataset text)
RETURNS timestamptz AS $$
DECLARE
  v_wm timestamptz;
  v_hot timestamptz;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, p_dataset), '1970-01-01'::timestamptz);
  v_hot := now() - make_interval(days => etl.hot_window_days());
  RETURN LEAST(v_wm, v_hot);
END;
$$ LANGUAGE plpgsql STABLE;

-- ==========================================
-- Facts with hot-window incremental
-- ==========================================

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_from_ts timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);
  v_from_ts := etl.effective_from_ts(p_id_empresa, 'comprovantes');

  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      etl.safe_timestamp(payload->>'DATA') AS data,
      etl.date_key(etl.safe_timestamp(payload->>'DATA')) AS data_key,
      etl.safe_int(payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_numeric(payload->>'VLRTOTAL')::numeric(18,2) AS valor_total,
      etl.to_bool(payload->>'CANCELADO') AS cancelado,
      etl.safe_int(payload->>'SITUACAO') AS situacao,
      payload
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND ingested_at > v_from_ts
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_turno=EXCLUDED.id_turno,
      id_cliente=EXCLUDED.id_cliente,
      valor_total=EXCLUDED.valor_total,
      cancelado=EXCLUDED.cancelado,
      situacao=EXCLUDED.situacao,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_from_ts timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz);
  v_from_ts := etl.effective_from_ts(p_id_empresa, 'movprodutos');

  WITH src AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      etl.safe_timestamp(m.payload->>'DATA') AS data,
      etl.date_key(etl.safe_timestamp(m.payload->>'DATA')) AS data_key,
      etl.safe_int(m.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(m.payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_int(m.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(m.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(m.payload->>'SAIDAS_ENTRADAS') AS saidas_entradas,
      etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2) AS total_venda,
      m.payload
    FROM stg.movprodutos m
    WHERE m.id_empresa = p_id_empresa
      AND m.ingested_at > v_from_ts
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_cliente=EXCLUDED.id_cliente,
      id_comprovante=EXCLUDED.id_comprovante,
      id_turno=EXCLUDED.id_turno,
      saidas_entradas=EXCLUDED.saidas_entradas,
      total_venda=EXCLUDED.total_venda,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  UPDATE dw.fact_venda v
  SET cancelado = c.cancelado
  FROM dw.fact_comprovante c
  WHERE v.id_empresa = p_id_empresa
    AND v.id_empresa = c.id_empresa
    AND v.id_filial = c.id_filial
    AND v.id_db = c.id_db
    AND v.id_comprovante IS NOT NULL
    AND v.id_comprovante = c.id_comprovante;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.movprodutos
  WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;

  PERFORM etl.set_watermark(p_id_empresa, 'movprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_from_ts timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itensmovprodutos'), '1970-01-01'::timestamptz);
  v_from_ts := etl.effective_from_ts(p_id_empresa, 'itensmovprodutos');

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      i.id_itensmovprodutos,
      v.data_key,
      etl.safe_int(i.payload->>'ID_PRODUTOS') AS id_produto,
      etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(i.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_int(i.payload->>'ID_FUNCIONARIOS') AS id_funcionario,
      etl.safe_int(i.payload->>'CFOP') AS cfop,
      etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3) AS qtd,
      etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4) AS valor_unitario,
      etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2) AS total,
      etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2) AS desconto,
      COALESCE(
        (etl.safe_numeric(i.payload->>'VLRCUSTO')::numeric(18,6) * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2),
        (dp.custo_medio * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2)
      ) AS custo_total,
      i.payload
    FROM stg.itensmovprodutos i
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa=i.id_empresa AND v.id_filial=i.id_filial AND v.id_db=i.id_db AND v.id_movprodutos=i.id_movprodutos
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa=i.id_empresa AND dp.id_filial=i.id_filial AND dp.id_produto=etl.safe_int(i.payload->>'ID_PRODUTOS')
    WHERE i.id_empresa = p_id_empresa
      AND i.ingested_at > v_from_ts
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,margem,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,
      (COALESCE(total,0) - COALESCE(custo_total,0))::numeric(18,2) AS margem,
      payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
    DO UPDATE SET
      data_key=EXCLUDED.data_key,
      id_produto=EXCLUDED.id_produto,
      id_grupo_produto=EXCLUDED.id_grupo_produto,
      id_local_venda=EXCLUDED.id_local_venda,
      id_funcionario=EXCLUDED.id_funcionario,
      cfop=EXCLUDED.cfop,
      qtd=EXCLUDED.qtd,
      valor_unitario=EXCLUDED.valor_unitario,
      total=EXCLUDED.total,
      desconto=EXCLUDED.desconto,
      custo_total=EXCLUDED.custo_total,
      margem=EXCLUDED.margem,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.itensmovprodutos
  WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;

  PERFORM etl.set_watermark(p_id_empresa, 'itensmovprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_financeiro(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_from_ts timestamptz;
  v_max_a timestamptz;
  v_max_b timestamptz;
  v_max_c timestamptz;
  v_max_final timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'financeiro'), '1970-01-01'::timestamptz);
  v_from_ts := etl.effective_from_ts(p_id_empresa, 'financeiro');

  WITH src AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_db,
      f.tipo_titulo,
      f.id_titulo,
      etl.safe_int(f.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(f.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(f.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(f.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(f.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(f.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      f.payload
    FROM stg.financeiro f
    WHERE f.id_empresa = p_id_empresa
      AND f.ingested_at > v_from_ts

    UNION ALL

    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_db,
      0 AS tipo_titulo,
      p.id_contaspagar AS id_titulo,
      etl.safe_int(p.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(p.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(p.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(p.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(p.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      p.payload
    FROM stg.contaspagar p
    WHERE p.id_empresa = p_id_empresa
      AND p.ingested_at > v_from_ts

    UNION ALL

    SELECT
      r.id_empresa,
      r.id_filial,
      r.id_db,
      1 AS tipo_titulo,
      r.id_contasreceber AS id_titulo,
      etl.safe_int(r.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(r.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(r.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(r.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(r.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(r.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      r.payload
    FROM stg.contasreceber r
    WHERE r.id_empresa = p_id_empresa
      AND r.ingested_at > v_from_ts
  ), upserted AS (
    INSERT INTO dw.fact_financeiro (
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    )
    SELECT
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,tipo_titulo,id_titulo)
    DO UPDATE SET
      id_entidade=EXCLUDED.id_entidade,
      data_emissao=EXCLUDED.data_emissao,
      data_key_emissao=EXCLUDED.data_key_emissao,
      vencimento=EXCLUDED.vencimento,
      data_key_venc=EXCLUDED.data_key_venc,
      data_pagamento=EXCLUDED.data_pagamento,
      data_key_pgto=EXCLUDED.data_key_pgto,
      valor=EXCLUDED.valor,
      valor_pago=EXCLUDED.valor_pago,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max_a FROM stg.financeiro WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;
  SELECT MAX(ingested_at) INTO v_max_b FROM stg.contaspagar WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;
  SELECT MAX(ingested_at) INTO v_max_c FROM stg.contasreceber WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;

  v_max_final := GREATEST(COALESCE(v_max_a, '1970-01-01'::timestamptz), COALESCE(v_max_b, '1970-01-01'::timestamptz), COALESCE(v_max_c, '1970-01-01'::timestamptz));
  PERFORM etl.set_watermark(p_id_empresa, 'financeiro', COALESCE(v_max_final, v_wm), NULL::bigint);

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Product improvement: churn mart + risk by shift/local
-- ==========================================

DROP MATERIALIZED VIEW IF EXISTS mart.clientes_churn_risco CASCADE;
CREATE MATERIALIZED VIEW mart.clientes_churn_risco AS
WITH sales AS (
  SELECT
    v.id_empresa,
    v.id_filial,
    COALESCE(v.id_cliente, -1) AS id_cliente,
    MAX(v.data::date) AS last_purchase,
    COUNT(DISTINCT CASE WHEN v.data::date >= current_date - interval '30 day' THEN v.id_comprovante END)::int AS compras_30d,
    COUNT(DISTINCT CASE WHEN v.data::date >= current_date - interval '60 day' AND v.data::date < current_date - interval '30 day' THEN v.id_comprovante END)::int AS compras_60_30,
    COALESCE(SUM(CASE WHEN v.data::date >= current_date - interval '30 day' THEN i.total ELSE 0 END),0)::numeric(18,2) AS faturamento_30d,
    COALESCE(SUM(CASE WHEN v.data::date >= current_date - interval '60 day' AND v.data::date < current_date - interval '30 day' THEN i.total ELSE 0 END),0)::numeric(18,2) AS faturamento_60_30
  FROM dw.fact_venda v
  JOIN dw.fact_venda_item i
    ON i.id_empresa=v.id_empresa AND i.id_filial=v.id_filial AND i.id_db=v.id_db AND i.id_movprodutos=v.id_movprodutos
  WHERE COALESCE(v.cancelado,false) = false
    AND COALESCE(i.cfop,0) >= 5000
    AND v.data::date >= current_date - interval '120 day'
  GROUP BY 1,2,3
), churn AS (
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_cliente,
    c.nome AS cliente_nome,
    s.last_purchase,
    s.compras_30d,
    s.compras_60_30,
    s.faturamento_30d,
    s.faturamento_60_30,
    LEAST(100,
      GREATEST(0,
        CASE
          WHEN s.id_cliente = -1 THEN 0
          WHEN s.last_purchase < current_date - interval '60 day' THEN 95
          WHEN s.last_purchase < current_date - interval '30 day' THEN 80
          ELSE 40
        END
        + CASE WHEN s.compras_60_30 > 0 AND s.compras_30d = 0 THEN 20 ELSE 0 END
        + CASE WHEN s.faturamento_60_30 > 0 AND s.faturamento_30d < (s.faturamento_60_30 * 0.60) THEN 20 ELSE 0 END
      )
    )::int AS churn_score,
    jsonb_build_object(
      'last_purchase', s.last_purchase,
      'compras_30d', s.compras_30d,
      'compras_60_30', s.compras_60_30,
      'faturamento_30d', s.faturamento_30d,
      'faturamento_60_30', s.faturamento_60_30
    ) AS reasons,
    now() AS updated_at
  FROM sales s
  LEFT JOIN dw.dim_cliente c
    ON c.id_empresa=s.id_empresa AND c.id_filial=s.id_filial AND c.id_cliente=s.id_cliente
)
SELECT * FROM churn;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_clientes_churn_risco
  ON mart.clientes_churn_risco (id_empresa, id_filial, id_cliente);
CREATE INDEX IF NOT EXISTS ix_mart_clientes_churn_risco_score
  ON mart.clientes_churn_risco (id_empresa, id_filial, churn_score DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.risco_turno_local_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.risco_turno_local_diaria AS
SELECT
  r.id_empresa,
  r.id_filial,
  r.data_key,
  COALESCE(r.id_turno, -1) AS id_turno,
  COALESCE(lv.id_local_venda, -1) AS id_local_venda,
  COUNT(*)::int AS eventos,
  COUNT(*) FILTER (WHERE r.score_risco >= 80)::int AS alto_risco,
  COALESCE(SUM(r.impacto_estimado),0)::numeric(18,2) AS impacto_estimado,
  COALESCE(AVG(r.score_risco),0)::numeric(10,2) AS score_medio,
  now() AS updated_at
FROM dw.fact_risco_evento r
LEFT JOIN LATERAL (
  SELECT MIN(i.id_local_venda) AS id_local_venda
  FROM dw.fact_venda_item i
  WHERE i.id_empresa = r.id_empresa
    AND i.id_filial = r.id_filial
    AND i.id_db = r.id_db
    AND i.id_movprodutos = r.id_movprodutos
) lv ON true
GROUP BY 1,2,3,4,5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_risco_turno_local_diaria
  ON mart.risco_turno_local_diaria (id_empresa, id_filial, data_key, id_turno, id_local_venda);

-- ==========================================
-- Selective mart refresh
-- ==========================================

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
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', false);
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', false);
  END IF;

  IF v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', false);
  END IF;

  RETURN v_meta;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- run_all with step log + timings
-- ==========================================

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := now();
  v_meta jsonb := '{}'::jsonb;
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
  v_refresh_meta jsonb := '{}'::jsonb;
BEGIN
  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_step_started := now();
  v_rows := etl.load_dim_filial(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_filial', v_rows, 'dim_filial_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_filial', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_dim_grupos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_grupos', v_rows, 'dim_grupos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_grupos', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_dim_localvendas(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_localvendas', v_rows, 'dim_localvendas_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_localvendas', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_dim_produtos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_produtos', v_rows, 'dim_produtos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_produtos', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_dim_funcionarios(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', v_rows, 'dim_funcionarios_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_funcionarios', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_dim_clientes(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_clientes', v_rows, 'dim_clientes_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_clientes', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_fact_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_comprovante', v_rows, 'fact_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_comprovante', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_fact_venda(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda', v_rows, 'fact_venda_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_fact_venda_item(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda_item', v_rows, 'fact_venda_item_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda_item', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := now();
  v_rows := etl.load_fact_financeiro(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_financeiro', v_rows, 'fact_financeiro_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_financeiro', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  IF p_force_full
     OR COALESCE((v_meta->>'fact_comprovante')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda_item')::int,0) > 0
  THEN
    v_step_started := now();
    v_rows := etl.compute_risk_events(p_id_empresa, p_force_full, 14);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('risk_events', v_rows, 'risk_events_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'risk_events', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_rows := 0;
    v_meta := v_meta || jsonb_build_object('risk_events', 0, 'risk_events_skipped', true, 'risk_events_skip_reason', 'no_fact_changes');
    PERFORM etl.log_step(p_id_empresa, 'risk_events', now(), now(), 'ok', 0, NULL, jsonb_build_object('skipped', true, 'reason', 'no_fact_changes'));
  END IF;

  IF p_refresh_mart THEN
    v_step_started := now();
    v_refresh_meta := etl.refresh_marts(v_meta);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true, 'mart_refresh', v_refresh_meta, 'mart_refresh_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'refresh_marts', v_step_started, now(), 'ok', 1, NULL, jsonb_build_object('ms', v_step_ms, 'refresh', v_refresh_meta));
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false);
  END IF;

  v_step_started := now();
  v_rows := etl.generate_insights(p_id_empresa, CURRENT_DATE, 7);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (now() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('insights_generated', v_rows, 'insights_generated_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'insights_generated', v_step_started, now(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    now(),
    'ok',
    1,
    NULL,
    jsonb_build_object('force_full', p_force_full, 'refresh_mart', p_refresh_mart, 'meta', v_meta)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'hot_window_days', etl.hot_window_days(),
    'started_at', v_started,
    'finished_at', now(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    now(),
    'failed',
    0,
    SQLERRM,
    jsonb_build_object('meta_partial', v_meta)
  );
  RAISE;
END;
$$ LANGUAGE plpgsql;

COMMIT;
