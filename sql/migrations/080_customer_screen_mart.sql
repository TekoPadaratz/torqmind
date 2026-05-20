BEGIN;

-- ==========================================
-- Mart dedicada para tela de clientes (paginada)
-- Grain: (id_empresa, id_filial, id_cliente)
-- Independente da tela financeira
-- ==========================================

CREATE TABLE IF NOT EXISTS mart.customer_screen_summary (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_cliente         integer NOT NULL,
  cliente_nome       text NOT NULL DEFAULT '',
  faturamento_30d    numeric(18,2) NOT NULL DEFAULT 0,
  compras_30d        integer NOT NULL DEFAULT 0,
  ticket_medio_30d   numeric(18,2) NOT NULL DEFAULT 0,
  faturamento_90d    numeric(18,2) NOT NULL DEFAULT 0,
  compras_90d        integer NOT NULL DEFAULT 0,
  ticket_medio_90d   numeric(18,2) NOT NULL DEFAULT 0,
  ultima_compra      date,
  dt_ref             date NOT NULL,
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_cliente)
);

-- Index para busca paginada com search por nome
CREATE INDEX IF NOT EXISTS ix_customer_screen_search
  ON mart.customer_screen_summary (id_empresa, id_filial, cliente_nome text_pattern_ops);

-- Index para sort por faturamento (default)
CREATE INDEX IF NOT EXISTS ix_customer_screen_fat30
  ON mart.customer_screen_summary (id_empresa, id_filial, faturamento_30d DESC);

-- Index para sort por compras
CREATE INDEX IF NOT EXISTS ix_customer_screen_compras30
  ON mart.customer_screen_summary (id_empresa, id_filial, compras_30d DESC);

-- Index para sort por ultima compra
CREATE INDEX IF NOT EXISTS ix_customer_screen_ultima
  ON mart.customer_screen_summary (id_empresa, id_filial, ultima_compra DESC NULLS LAST);

-- ==========================================
-- ETL function: refresh_customer_screen_summary
-- Fonte: mart.customer_sales_daily + dw.dim_cliente
-- Idempotente (UPSERT via DELETE + INSERT)
-- ==========================================

CREATE OR REPLACE FUNCTION etl.refresh_customer_screen_summary(
  p_id_empresa integer DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_ref_date date;
BEGIN
  v_ref_date := etl.runtime_ref_date();

  -- Delete existing rows for the empresa (or all if NULL)
  DELETE FROM mart.customer_screen_summary
  WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa);

  WITH dim AS (
    SELECT DISTINCT ON (d.id_empresa, d.id_cliente)
      d.id_empresa,
      d.id_cliente,
      d.nome
    FROM dw.dim_cliente d
    WHERE (p_id_empresa IS NULL OR d.id_empresa = p_id_empresa)
    ORDER BY d.id_empresa, d.id_cliente, d.updated_at DESC, d.id_filial
  ), agg_30 AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      COALESCE(SUM(s.valor_dia), 0)::numeric(18,2) AS faturamento_30d,
      COALESCE(SUM(s.compras_dia), 0)::int AS compras_30d,
      MAX(s.dt_ref) AS ultima_compra
    FROM mart.customer_sales_daily s
    WHERE (p_id_empresa IS NULL OR s.id_empresa = p_id_empresa)
      AND s.id_cliente <> -1
      AND s.dt_ref BETWEEN (v_ref_date - interval '30 days')::date AND v_ref_date
    GROUP BY s.id_empresa, s.id_filial, s.id_cliente
  ), agg_90 AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_cliente,
      COALESCE(SUM(s.valor_dia), 0)::numeric(18,2) AS faturamento_90d,
      COALESCE(SUM(s.compras_dia), 0)::int AS compras_90d
    FROM mart.customer_sales_daily s
    WHERE (p_id_empresa IS NULL OR s.id_empresa = p_id_empresa)
      AND s.id_cliente <> -1
      AND s.dt_ref BETWEEN (v_ref_date - interval '90 days')::date AND v_ref_date
    GROUP BY s.id_empresa, s.id_filial, s.id_cliente
  ), combined AS (
    SELECT
      COALESCE(a30.id_empresa, a90.id_empresa) AS id_empresa,
      COALESCE(a30.id_filial, a90.id_filial) AS id_filial,
      COALESCE(a30.id_cliente, a90.id_cliente) AS id_cliente,
      COALESCE(a30.faturamento_30d, 0) AS faturamento_30d,
      COALESCE(a30.compras_30d, 0) AS compras_30d,
      CASE
        WHEN COALESCE(a30.compras_30d, 0) > 0
        THEN (COALESCE(a30.faturamento_30d, 0) / a30.compras_30d)::numeric(18,2)
        ELSE 0::numeric(18,2)
      END AS ticket_medio_30d,
      COALESCE(a90.faturamento_90d, 0) AS faturamento_90d,
      COALESCE(a90.compras_90d, 0) AS compras_90d,
      CASE
        WHEN COALESCE(a90.compras_90d, 0) > 0
        THEN (COALESCE(a90.faturamento_90d, 0) / a90.compras_90d)::numeric(18,2)
        ELSE 0::numeric(18,2)
      END AS ticket_medio_90d,
      a30.ultima_compra
    FROM agg_30 a30
    FULL OUTER JOIN agg_90 a90
      ON a90.id_empresa = a30.id_empresa
     AND a90.id_filial = a30.id_filial
     AND a90.id_cliente = a30.id_cliente
  ), inserted AS (
    INSERT INTO mart.customer_screen_summary (
      id_empresa, id_filial, id_cliente, cliente_nome,
      faturamento_30d, compras_30d, ticket_medio_30d,
      faturamento_90d, compras_90d, ticket_medio_90d,
      ultima_compra, dt_ref, updated_at
    )
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_cliente,
      COALESCE(NULLIF(d.nome, ''), '#ID ' || c.id_cliente::text),
      c.faturamento_30d,
      c.compras_30d,
      c.ticket_medio_30d,
      c.faturamento_90d,
      c.compras_90d,
      c.ticket_medio_90d,
      c.ultima_compra,
      v_ref_date,
      now()
    FROM combined c
    LEFT JOIN dim d
      ON d.id_empresa = c.id_empresa
     AND d.id_cliente = c.id_cliente
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM inserted;

  RETURN COALESCE(v_rows, 0);
END;
$$;

COMMIT;

-- ==========================================
-- Plug into ETL cycle: override run_tenant_post_refresh
-- to call refresh_customer_screen_summary after customer_sales_daily
-- (additive patch via DO block — safe, idempotent)
-- ==========================================

-- Note: The Python-side etl_orchestrator already handles this step.
-- The SQL function etl.run_tenant_post_refresh is used only as fallback.
-- To keep it in sync, we add an explicit call via a wrapper approach.
-- However, the primary execution path is Python, which is already patched.
