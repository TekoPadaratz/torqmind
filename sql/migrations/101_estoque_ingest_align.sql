-- Migration 101: Estoque — alinhar STG ao ingest (shadow columns) + ETL robusto
-- Idempotente. Nao destrutivo. Complementa a 100.
--
-- O ingest (routes_ingest.py) grava payload + colunas "shadow" (pk + id_db_shadow,
-- id_chave_natural, dt_evento + campos especificos). As STG de tanque (100) e a
-- stg.estoque (legado) precisam dessas colunas para o INSERT do ingest nao falhar
-- em producao. O ETL passa a ler o valor de forma robusta: shadow -> payload ->
-- coluna dedicada, funcionando tanto com o seed manual de homolog quanto com o
-- que o Agent grava em producao.

-- 1) Shadow columns exigidas pelo ingest.
ALTER TABLE stg.tanques
  ADD COLUMN IF NOT EXISTS id_db_shadow     bigint,
  ADD COLUMN IF NOT EXISTS id_chave_natural text;

ALTER TABLE stg.movtanques
  ADD COLUMN IF NOT EXISTS id_db_shadow     bigint,
  ADD COLUMN IF NOT EXISTS id_chave_natural text;

ALTER TABLE stg.estoque
  ADD COLUMN IF NOT EXISTS id_db_shadow          bigint,
  ADD COLUMN IF NOT EXISTS id_chave_natural      text,
  ADD COLUMN IF NOT EXISTS id_produto_shadow     integer,
  ADD COLUMN IF NOT EXISTS id_local_venda_shadow integer,
  ADD COLUMN IF NOT EXISTS qtd_atual_shadow      numeric;

-- 2) ETL robusto: le shadow/payload/coluna (funciona em homolog e producao).
CREATE OR REPLACE FUNCTION etl.refresh_liquidez_estoque(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows    integer := 0;
  v_ref     date    := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
  v_cur_ym  integer := (EXTRACT(YEAR FROM v_ref)::int * 100 + EXTRACT(MONTH FROM v_ref)::int);
  v_fresh   integer := 7;
BEGIN
  DROP TABLE IF EXISTS _liq_est;
  CREATE TEMP TABLE _liq_est AS
  WITH ult AS (
    SELECT DISTINCT ON (m.id_empresa, m.id_filial, (m.payload->>'ID_TANQUES')::int)
      m.id_empresa,
      m.id_filial,
      (m.payload->>'ID_TANQUES')::int AS id_tanque,
      etl.safe_numeric(m.payload->>'LEITURA') AS litros,
      (etl.safe_timestamp(m.payload->>'DTACONTA'))::date AS data_leitura
    FROM stg.movtanques m
    WHERE m.id_empresa = p_id_empresa
      AND etl.safe_numeric(m.payload->>'LEITURA') > 0
      AND etl.safe_timestamp(m.payload->>'DTACONTA') IS NOT NULL
    ORDER BY m.id_empresa, m.id_filial, (m.payload->>'ID_TANQUES')::int,
             etl.safe_timestamp(m.payload->>'DTACONTA') DESC
  ),
  comb AS (
    SELECT
      ul.id_empresa,
      ul.id_filial,
      SUM(ul.litros * COALESCE(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'), 0))::numeric(18,2) AS valor,
      MAX(ul.data_leitura) AS data_leitura
    FROM ult ul
    JOIN stg.tanques t
      ON t.id_empresa = ul.id_empresa AND t.id_filial = ul.id_filial AND t.id_tanque = ul.id_tanque
    LEFT JOIN stg.produtos pr
      ON pr.id_empresa = ul.id_empresa AND pr.id_filial = ul.id_filial
     AND pr.id_produto = (t.payload->>'ID_PRODUTOS')::int
    WHERE ul.data_leitura >= v_ref - v_fresh
    GROUP BY ul.id_empresa, ul.id_filial
  ),
  est AS (
    -- resolve id_produto e quantidade de forma robusta (shadow -> payload -> coluna)
    SELECT
      e.id_empresa,
      e.id_filial,
      COALESCE(e.id_produto, e.id_produto_shadow, (e.payload->>'ID_PRODUTOS')::int) AS id_produto,
      GREATEST(
        COALESCE(e.qtd_atual_shadow, etl.safe_numeric(e.payload->>'QTDEATUAL'), e.quantidade, 0), 0
      ) AS qtde,
      e.custo_medio
    FROM stg.estoque e
    WHERE e.id_empresa = p_id_empresa
  ),
  loja AS (
    SELECT
      es.id_empresa,
      es.id_filial,
      SUM(es.qtde * COALESCE(NULLIF(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'), 0), es.custo_medio, 0))::numeric(18,2) AS valor
    FROM est es
    JOIN stg.produtos pr
      ON pr.id_empresa = es.id_empresa AND pr.id_filial = es.id_filial AND pr.id_produto = es.id_produto
    WHERE (pr.payload->>'ID_GRUPOPRODUTOS')::int = ANY (
        ARRAY[2,4,7,8,9,16,39, 10, 14,15,18,12,13,11,17,21,37,40,41,19,20]
      )
      AND NOT EXISTS (
        SELECT 1 FROM stg.tanques t
        WHERE t.id_empresa = es.id_empresa AND t.id_filial = es.id_filial
          AND (t.payload->>'ID_PRODUTOS')::int = es.id_produto
      )
    GROUP BY es.id_empresa, es.id_filial
  )
  SELECT
    COALESCE(c.id_empresa, l.id_empresa) AS id_empresa,
    COALESCE(c.id_filial,  l.id_filial)  AS id_filial,
    COALESCE(c.valor, 0)::numeric(18,2)  AS valor_combustivel,
    COALESCE(l.valor, 0)::numeric(18,2)  AS valor_loja,
    (c.id_filial IS NOT NULL)            AS combustivel_medido,
    c.data_leitura
  FROM comb c
  FULL OUTER JOIN loja l ON l.id_empresa = c.id_empresa AND l.id_filial = c.id_filial;

  INSERT INTO mart.liquidez_solvencia (id_empresa, id_filial, ano_mes, updated_at)
  SELECT id_empresa, id_filial, v_cur_ym, now() FROM _liq_est
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO NOTHING;

  UPDATE mart.liquidez_solvencia m
  SET ativo_estoque             = v.valor_combustivel + v.valor_loja,
      ativo_estoque_combustivel = v.valor_combustivel,
      ativo_estoque_loja        = v.valor_loja,
      estoque_combustivel_medido = v.combustivel_medido,
      estoque_data_leitura      = v.data_leitura,
      tem_ativo_dados           = true,
      updated_at                = now()
  FROM _liq_est v
  WHERE m.id_empresa = v.id_empresa AND m.id_filial = v.id_filial;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  DROP TABLE IF EXISTS _liq_est;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;
