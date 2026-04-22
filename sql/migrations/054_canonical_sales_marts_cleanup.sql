BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mart.agg_vendas_hora CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.insights_base_diaria CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_funcionarios_diaria CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_grupos_diaria CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_produtos_diaria CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_vendas_diaria CASCADE;

CREATE MATERIALIZED VIEW mart.agg_vendas_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS faturamento,
  COALESCE(COUNT(*), 0)::int AS quantidade_itens,
  COALESCE(SUM(i.margem), 0)::numeric(18,2) AS margem,
  CASE
    WHEN COUNT(DISTINCT v.id_comprovante) = 0 THEN 0::numeric(18,2)
    ELSE (SUM(i.total) / COUNT(DISTINCT v.id_comprovante)::numeric)::numeric(18,2)
  END AS ticket_medio,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
 AND i.id_filial = v.id_filial
 AND i.id_db = v.id_db
 AND i.id_comprovante = v.id_comprovante
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_vendas_diaria
  ON mart.agg_vendas_diaria (id_empresa, id_filial, data_key);

CREATE INDEX IF NOT EXISTS ix_mart_agg_vendas_diaria_empresa_data
  ON mart.agg_vendas_diaria (id_empresa, data_key);

CREATE MATERIALIZED VIEW mart.insights_base_diaria AS
WITH daily AS (
  SELECT
    a.id_empresa,
    a.id_filial,
    a.data_key,
    to_date(a.data_key::text, 'YYYYMMDD') AS dt,
    a.faturamento AS faturamento_dia
  FROM mart.agg_vendas_diaria a
), daily_cum AS (
  SELECT
    d.*,
    SUM(d.faturamento_dia) OVER (
      PARTITION BY d.id_empresa, d.id_filial, date_trunc('month', d.dt)
      ORDER BY d.dt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::numeric(18,2) AS faturamento_mes_acum
  FROM daily d
), prev_join AS (
  SELECT
    cur.id_empresa,
    cur.id_filial,
    cur.data_key,
    cur.faturamento_dia,
    cur.faturamento_mes_acum,
    COALESCE(prev.faturamento_mes_acum, 0)::numeric(18,2) AS faturamento_mes_anterior_acum
  FROM daily_cum cur
  LEFT JOIN daily_cum prev
    ON prev.id_empresa = cur.id_empresa
   AND prev.id_filial = cur.id_filial
   AND prev.dt = (cur.dt - interval '1 month')::date
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  faturamento_dia,
  faturamento_mes_acum,
  (faturamento_mes_acum - faturamento_mes_anterior_acum)::numeric(18,2) AS comparativo_mes_anterior,
  NULL::text AS top_vendedor_key,
  NULL::numeric(18,2) AS top_vendedor_valor,
  NULL::numeric(18,2) AS inadimplencia_valor,
  NULL::numeric(9,4) AS inadimplencia_pct,
  NULL::text AS cliente_em_risco_key,
  NULL::numeric(9,4) AS margem_media_pct,
  NULL::numeric(18,2) AS giro_estoque,
  now() AS updated_at,
  '{}'::jsonb AS batch_info
FROM prev_join;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_insights_base_diaria
  ON mart.insights_base_diaria (id_empresa, id_filial, data_key);

CREATE INDEX IF NOT EXISTS ix_mart_insights_base_diaria_empresa_data
  ON mart.insights_base_diaria (id_empresa, data_key);

CREATE MATERIALIZED VIEW mart.agg_produtos_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  i.id_produto,
  COALESCE(p.nome, '') AS produto_nome,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.custo_total), 0)::numeric(18,2) AS custo_total,
  COALESCE(SUM(i.margem), 0)::numeric(18,2) AS margem,
  COALESCE(SUM(i.qtd), 0)::numeric(18,3) AS qtd,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
 AND i.id_filial = v.id_filial
 AND i.id_db = v.id_db
 AND i.id_comprovante = v.id_comprovante
LEFT JOIN dw.dim_produto p
  ON p.id_empresa = i.id_empresa
 AND p.id_filial = i.id_filial
 AND p.id_produto = i.id_produto
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, i.id_produto, COALESCE(p.nome, '');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_produtos_diaria
  ON mart.agg_produtos_diaria (id_empresa, id_filial, data_key, COALESCE(id_produto, -1));

CREATE INDEX IF NOT EXISTS ix_mart_agg_produtos_diaria_lookup
  ON mart.agg_produtos_diaria (id_empresa, data_key, faturamento DESC);

CREATE MATERIALIZED VIEW mart.agg_grupos_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  COALESCE(i.id_grupo_produto, -1) AS id_grupo_produto,
  COALESCE(g.nome, '(Sem grupo)') AS grupo_nome,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem), 0)::numeric(18,2) AS margem,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
 AND i.id_filial = v.id_filial
 AND i.id_db = v.id_db
 AND i.id_comprovante = v.id_comprovante
LEFT JOIN dw.dim_grupo_produto g
  ON g.id_empresa = i.id_empresa
 AND g.id_filial = i.id_filial
 AND g.id_grupo_produto = i.id_grupo_produto
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, COALESCE(i.id_grupo_produto, -1), COALESCE(g.nome, '(Sem grupo)');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_grupos_diaria
  ON mart.agg_grupos_diaria (id_empresa, id_filial, data_key, COALESCE(id_grupo_produto, -1));

CREATE INDEX IF NOT EXISTS ix_mart_agg_grupos_diaria_lookup
  ON mart.agg_grupos_diaria (id_empresa, data_key, faturamento DESC);

CREATE MATERIALIZED VIEW mart.agg_funcionarios_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  COALESCE(i.id_funcionario, -1) AS id_funcionario,
  COALESCE(f.nome, '(Sem funcionário)') AS funcionario_nome,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem), 0)::numeric(18,2) AS margem,
  COALESCE(COUNT(DISTINCT v.id_comprovante), 0)::int AS vendas,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
 AND i.id_filial = v.id_filial
 AND i.id_db = v.id_db
 AND i.id_comprovante = v.id_comprovante
LEFT JOIN dw.dim_funcionario f
  ON f.id_empresa = i.id_empresa
 AND f.id_filial = i.id_filial
 AND f.id_funcionario = i.id_funcionario
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, COALESCE(i.id_funcionario, -1), COALESCE(f.nome, '(Sem funcionário)');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_funcionarios_diaria
  ON mart.agg_funcionarios_diaria (id_empresa, id_filial, data_key, COALESCE(id_funcionario, -1));

CREATE INDEX IF NOT EXISTS ix_mart_agg_funcionarios_diaria_lookup
  ON mart.agg_funcionarios_diaria (id_empresa, data_key, faturamento DESC);

CREATE MATERIALIZED VIEW mart.agg_vendas_hora AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  EXTRACT(HOUR FROM v.data)::int AS hora,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS faturamento,
  COALESCE(SUM(i.margem), 0)::numeric(18,2) AS margem,
  COALESCE(COUNT(DISTINCT v.id_comprovante), 0)::int AS vendas,
  now() AS updated_at
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
 AND i.id_filial = v.id_filial
 AND i.id_db = v.id_db
 AND i.id_comprovante = v.id_comprovante
WHERE v.data IS NOT NULL
  AND v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) >= 5000
GROUP BY v.id_empresa, v.id_filial, v.data_key, EXTRACT(HOUR FROM v.data)::int;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_vendas_hora
  ON mart.agg_vendas_hora (id_empresa, id_filial, data_key, hora);

ANALYZE mart.agg_vendas_diaria;
ANALYZE mart.insights_base_diaria;
ANALYZE mart.agg_produtos_diaria;
ANALYZE mart.agg_grupos_diaria;
ANALYZE mart.agg_funcionarios_diaria;
ANALYZE mart.agg_vendas_hora;

COMMIT;
