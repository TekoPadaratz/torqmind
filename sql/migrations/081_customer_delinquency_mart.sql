BEGIN;

-- ==========================================
-- Mart dedicada: Prioridades de Cobrança (clientes inadimplentes)
-- Grain: (id_empresa, id_filial, id_cliente)
-- Apenas clientes com ao menos 1 título vencido
-- Inclui títulos a vencer do MESMO cliente inadimplente
-- ==========================================

CREATE TABLE IF NOT EXISTS mart.customer_delinquency_summary (
  id_empresa           integer NOT NULL,
  id_filial            integer NOT NULL,
  id_cliente           integer NOT NULL,
  cliente_nome         text NOT NULL DEFAULT '',
  titulos_ate_30d      integer NOT NULL DEFAULT 0,
  valor_ate_30d        numeric(18,2) NOT NULL DEFAULT 0,
  titulos_acima_30d    integer NOT NULL DEFAULT 0,
  valor_acima_30d      numeric(18,2) NOT NULL DEFAULT 0,
  titulos_a_vencer     integer NOT NULL DEFAULT 0,
  valor_a_vencer       numeric(18,2) NOT NULL DEFAULT 0,
  max_dias_atraso      integer NOT NULL DEFAULT 0,
  valor_total_vencido  numeric(18,2) NOT NULL DEFAULT 0,
  dt_ref               date NOT NULL,
  updated_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_cliente)
);

-- Sort por gravidade (default): primeiro quem tem mais vencidos 30+, depois até 30d
CREATE INDEX IF NOT EXISTS ix_delinq_gravity
  ON mart.customer_delinquency_summary (id_empresa, id_filial, titulos_acima_30d DESC, titulos_ate_30d DESC, valor_total_vencido DESC);

-- Sort por valor total vencido
CREATE INDEX IF NOT EXISTS ix_delinq_valor
  ON mart.customer_delinquency_summary (id_empresa, id_filial, valor_total_vencido DESC);

-- Sort por maior atraso
CREATE INDEX IF NOT EXISTS ix_delinq_atraso
  ON mart.customer_delinquency_summary (id_empresa, id_filial, max_dias_atraso DESC);

-- Busca por nome
CREATE INDEX IF NOT EXISTS ix_delinq_nome
  ON mart.customer_delinquency_summary (id_empresa, id_filial, cliente_nome text_pattern_ops);

-- ==========================================
-- ETL function: refresh_customer_delinquency_summary
-- Fonte: dw.fact_financeiro + stg.contasreceberbaixa + dw.dim_cliente
-- Idempotente (DELETE + INSERT)
-- ==========================================

CREATE OR REPLACE FUNCTION etl.refresh_customer_delinquency_summary(
  p_id_empresa integer DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_ref_date date;
BEGIN
  v_ref_date := COALESCE(etl.runtime_ref_date(), CURRENT_DATE);

  -- Delete existing rows for the empresa (or all if NULL)
  DELETE FROM mart.customer_delinquency_summary
  WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH open_financeiro AS (
    -- All open receivables (unpaid)
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_titulo,
      COALESCE(f.id_entidade, -1) AS id_cliente,
      COALESCE(f.valor, 0)::numeric(18,2) AS valor,
      COALESCE(f.valor_pago, 0)::numeric(18,2) AS valor_pago,
      COALESCE(f.vencimento, f.data_emissao) AS vencimento
    FROM dw.fact_financeiro f
    WHERE (p_id_empresa IS NULL OR f.id_empresa = p_id_empresa)
      AND f.tipo_titulo = 1
      AND f.data_pagamento IS NULL
      AND COALESCE(f.id_entidade, -1) <> -1
  ), baixa_agg AS (
    -- Partial payments from stg
    SELECT
      b.id_empresa,
      b.id_filial,
      (b.payload->>'ID_CONTASRECEBER')::int AS id_contasreceber,
      SUM((b.payload->>'VALORBAIXA')::numeric) AS total_baixa
    FROM stg.contasreceberbaixa b
    WHERE (p_id_empresa IS NULL OR b.id_empresa = p_id_empresa)
    GROUP BY b.id_empresa, b.id_filial, (b.payload->>'ID_CONTASRECEBER')::int
  ), titulos_com_saldo AS (
    -- Only titles with positive open balance
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_cliente,
      f.vencimento,
      GREATEST(0::numeric, f.valor - GREATEST(f.valor_pago, COALESCE(bx.total_baixa, 0)))::numeric(18,2) AS valor_aberto,
      CASE
        WHEN f.vencimento < v_ref_date THEN GREATEST(0, v_ref_date - f.vencimento)::int
        ELSE 0
      END AS dias_atraso,
      (f.vencimento < v_ref_date) AS is_vencido
    FROM open_financeiro f
    LEFT JOIN baixa_agg bx
      ON bx.id_empresa = f.id_empresa
     AND bx.id_filial = f.id_filial
     AND bx.id_contasreceber = f.id_titulo
    WHERE GREATEST(0::numeric, f.valor - GREATEST(f.valor_pago, COALESCE(bx.total_baixa, 0))) > 0
  ), clientes_inadimplentes AS (
    -- Only customers that have at least 1 overdue title
    SELECT DISTINCT id_empresa, id_filial, id_cliente
    FROM titulos_com_saldo
    WHERE is_vencido = true
  ), per_customer AS (
    SELECT
      t.id_empresa,
      t.id_filial,
      t.id_cliente,
      -- Títulos vencidos até 30 dias
      COUNT(*) FILTER (WHERE t.is_vencido AND t.dias_atraso BETWEEN 1 AND 30)::int AS titulos_ate_30d,
      COALESCE(SUM(t.valor_aberto) FILTER (WHERE t.is_vencido AND t.dias_atraso BETWEEN 1 AND 30), 0)::numeric(18,2) AS valor_ate_30d,
      -- Títulos vencidos acima de 30 dias
      COUNT(*) FILTER (WHERE t.is_vencido AND t.dias_atraso > 30)::int AS titulos_acima_30d,
      COALESCE(SUM(t.valor_aberto) FILTER (WHERE t.is_vencido AND t.dias_atraso > 30), 0)::numeric(18,2) AS valor_acima_30d,
      -- Títulos a vencer (ainda não venceram mas cliente é inadimplente)
      COUNT(*) FILTER (WHERE NOT t.is_vencido)::int AS titulos_a_vencer,
      COALESCE(SUM(t.valor_aberto) FILTER (WHERE NOT t.is_vencido), 0)::numeric(18,2) AS valor_a_vencer,
      -- Métricas gerais
      COALESCE(MAX(t.dias_atraso) FILTER (WHERE t.is_vencido), 0)::int AS max_dias_atraso,
      COALESCE(SUM(t.valor_aberto) FILTER (WHERE t.is_vencido), 0)::numeric(18,2) AS valor_total_vencido
    FROM titulos_com_saldo t
    INNER JOIN clientes_inadimplentes ci
      ON ci.id_empresa = t.id_empresa
     AND ci.id_filial = t.id_filial
     AND ci.id_cliente = t.id_cliente
    GROUP BY t.id_empresa, t.id_filial, t.id_cliente
  ), dim AS (
    SELECT DISTINCT ON (d.id_empresa, d.id_cliente)
      d.id_empresa,
      d.id_cliente,
      d.nome
    FROM dw.dim_cliente d
    WHERE (p_id_empresa IS NULL OR d.id_empresa = p_id_empresa)
    ORDER BY d.id_empresa, d.id_cliente, d.updated_at DESC, d.id_filial
  ), inserted AS (
    INSERT INTO mart.customer_delinquency_summary (
      id_empresa, id_filial, id_cliente, cliente_nome,
      titulos_ate_30d, valor_ate_30d,
      titulos_acima_30d, valor_acima_30d,
      titulos_a_vencer, valor_a_vencer,
      max_dias_atraso, valor_total_vencido,
      dt_ref, updated_at
    )
    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_cliente,
      COALESCE(NULLIF(d.nome, ''), '#ID ' || p.id_cliente::text),
      p.titulos_ate_30d,
      p.valor_ate_30d,
      p.titulos_acima_30d,
      p.valor_acima_30d,
      p.titulos_a_vencer,
      p.valor_a_vencer,
      p.max_dias_atraso,
      p.valor_total_vencido,
      v_ref_date,
      now()
    FROM per_customer p
    LEFT JOIN dim d
      ON d.id_empresa = p.id_empresa
     AND d.id_cliente = p.id_cliente
    WHERE (p.titulos_ate_30d + p.titulos_acima_30d) > 0
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$;

COMMIT;
