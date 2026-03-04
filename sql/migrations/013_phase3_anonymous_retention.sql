-- Phase 3: Anonymous retention (operational churn proxy)

DROP MATERIALIZED VIEW IF EXISTS mart.anonymous_retention_daily CASCADE;
CREATE MATERIALIZED VIEW mart.anonymous_retention_daily AS
WITH daily AS (
  SELECT
    v.id_empresa,
    v.id_filial,
    (v.data)::date AS dt_ref,
    COALESCE(SUM(CASE WHEN v.id_cliente IS NULL OR v.id_cliente = -1 THEN v.total_venda ELSE 0 END), 0)::numeric(18,2) AS anon_faturamento_dia,
    COALESCE(SUM(v.total_venda), 0)::numeric(18,2) AS faturamento_total_dia,
    COALESCE(COUNT(DISTINCT CASE WHEN v.id_cliente IS NULL OR v.id_cliente = -1 THEN v.id_comprovante END), 0)::int AS anon_comprovantes_dia
  FROM dw.fact_venda v
  WHERE COALESCE(v.cancelado, false) = false
    AND v.data IS NOT NULL
  GROUP BY v.id_empresa, v.id_filial, (v.data)::date
),
roll AS (
  SELECT
    d.*,
    COALESCE(SUM(d.anon_faturamento_dia) OVER (
      PARTITION BY d.id_empresa, d.id_filial
      ORDER BY d.dt_ref
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0)::numeric(18,2) AS anon_faturamento_7d,
    COALESCE(SUM(d.anon_faturamento_dia) OVER (
      PARTITION BY d.id_empresa, d.id_filial
      ORDER BY d.dt_ref
      ROWS BETWEEN 34 PRECEDING AND 7 PRECEDING
    ), 0)::numeric(18,2) AS anon_faturamento_prev_28d,
    COALESCE(SUM(d.faturamento_total_dia) OVER (
      PARTITION BY d.id_empresa, d.id_filial
      ORDER BY d.dt_ref
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0)::numeric(18,2) AS faturamento_total_7d,
    COALESCE(COUNT(*) FILTER (WHERE d.anon_comprovantes_dia > 0) OVER (
      PARTITION BY d.id_empresa, d.id_filial
      ORDER BY d.dt_ref
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0)::int AS dias_anon_ativos_7d,
    COALESCE(COUNT(*) FILTER (WHERE d.anon_comprovantes_dia > 0) OVER (
      PARTITION BY d.id_empresa, d.id_filial
      ORDER BY d.dt_ref
      ROWS BETWEEN 34 PRECEDING AND 7 PRECEDING
    ), 0)::int AS dias_anon_ativos_prev_28d
  FROM daily d
)
SELECT
  r.dt_ref,
  r.id_empresa,
  r.id_filial,
  r.anon_faturamento_7d,
  r.anon_faturamento_prev_28d,
  CASE
    WHEN r.anon_faturamento_prev_28d > 0
      THEN ROUND((((r.anon_faturamento_7d - (r.anon_faturamento_prev_28d / 4.0)) / (r.anon_faturamento_prev_28d / 4.0)) * 100.0)::numeric, 2)
    ELSE 0
  END AS trend_pct,
  CASE
    WHEN r.faturamento_total_7d > 0
      THEN ROUND(((r.anon_faturamento_7d / r.faturamento_total_7d) * 100.0)::numeric, 2)
    ELSE 0
  END AS anon_share_pct_7d,
  CASE
    WHEN r.dias_anon_ativos_prev_28d > 0
      THEN ROUND((((r.dias_anon_ativos_7d::numeric / 7.0) / (r.dias_anon_ativos_prev_28d::numeric / 28.0)) * 100.0)::numeric, 2)
    ELSE 0
  END AS repeat_proxy_idx,
  GREATEST(0, ROUND(((r.anon_faturamento_prev_28d / 4.0) - r.anon_faturamento_7d)::numeric, 2)) AS impact_estimated_7d,
  jsonb_build_object(
    'dias_anon_ativos_7d', r.dias_anon_ativos_7d,
    'dias_anon_ativos_prev_28d', r.dias_anon_ativos_prev_28d
  ) AS details,
  now() AS updated_at
FROM roll r;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_anonymous_retention_daily
  ON mart.anonymous_retention_daily (dt_ref, id_empresa, id_filial);

CREATE INDEX IF NOT EXISTS ix_mart_anonymous_retention_lookup
  ON mart.anonymous_retention_daily (id_empresa, id_filial, dt_ref DESC);

CREATE OR REPLACE FUNCTION etl.refresh_anonymous_retention()
RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW mart.anonymous_retention_daily;
  RETURN 1;
END;
$$;
