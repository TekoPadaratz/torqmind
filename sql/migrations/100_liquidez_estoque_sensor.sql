-- Migration 100: Estoque para Solvencia — fonte correta (sensor de tanque + loja curada)
-- Idempotente. Nao destrutivo. Reescreve etl.refresh_liquidez_estoque (que na 099
-- lia dbo.ESTOQUE direto — ERRADO para combustivel).
--
-- Descoberta (validada na fonte, 2026-07-09):
--   - COMBUSTIVEL: as tabelas "de estoque" (dbo.ESTOQUE, QTDESTANQUES, QTDESISTEMA)
--     mentem — ficam negativas ou com valores absurdos (5,2 milhoes de litros num
--     tanque de 30k). A UNICA fonte real e o SENSOR DE NIVEL DO TANQUE
--     (dbo.MOVTANQUES.LEITURA), lido diariamente na abertura. So 5/16 postos tem o
--     sensor sincronizando hoje; os demais estao com o sensor parado (questao
--     operacional). O pipeline usa o sensor onde ha e escala sozinho.
--   - LOJA: dbo.ESTOQUE esta contaminada com IMOBILIZADO (grupo 28: veiculos de
--     R$800k cadastrados como "produto"). So entram grupos de MERCADORIA de revenda
--     (conveniencia + automotivo + cigarro, a mesma classificacao do DRE).
--
-- Estoque valorizado = combustivel (litros do sensor x custo) + loja (qtde x custo).
--   Custo = stg.produtos.CUSTOMEDIO (dim_produto.custo_medio esta zerado).

-- 1) STG das fontes de tanque (coletadas pelo Agent: dbo.TANQUES, dbo.MOVTANQUES).
CREATE TABLE IF NOT EXISTS stg.tanques (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  id_tanque    integer NOT NULL,          -- ID_TANQUES
  id_db        integer NOT NULL DEFAULT 0,
  payload      jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento    timestamptz,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_tanque)
);

CREATE TABLE IF NOT EXISTS stg.movtanques (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  id_movtanque integer NOT NULL,          -- ID_MOVTANQUES
  id_db        integer NOT NULL DEFAULT 0,
  payload      jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento    timestamptz,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_movtanque)
);

CREATE INDEX IF NOT EXISTS ix_movtanques_scope
  ON stg.movtanques (id_empresa, id_filial, id_movtanque);

COMMENT ON TABLE stg.tanques IS 'Cadastro de tanques (Xpert dbo.TANQUES): payload liga id_tanque->ID_PRODUTOS e CAPACIDADE.';
COMMENT ON TABLE stg.movtanques IS 'Movimentacao/leitura de tanque (Xpert dbo.MOVTANQUES): payload LEITURA (sensor de nivel), DTACONTA. Fonte real do estoque de combustivel.';

-- 2) Colunas de detalhe/cobertura do estoque na mart de solvencia (aditivo).
ALTER TABLE mart.liquidez_solvencia
  ADD COLUMN IF NOT EXISTS ativo_estoque_combustivel  numeric(18,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS ativo_estoque_loja         numeric(18,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS estoque_combustivel_medido boolean       NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS estoque_data_leitura       date;

COMMENT ON COLUMN mart.liquidez_solvencia.estoque_combustivel_medido IS
  'true = o combustivel deste posto veio do sensor de tanque com leitura fresca. false = sem medicao (sensor parado): estoque de combustivel nao entra.';

-- 3) ETL do ATIVO ESTOQUE: combustivel via sensor + loja curada.
CREATE OR REPLACE FUNCTION etl.refresh_liquidez_estoque(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows    integer := 0;
  v_ref     date    := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
  v_cur_ym  integer := (EXTRACT(YEAR FROM v_ref)::int * 100 + EXTRACT(MONTH FROM v_ref)::int);
  v_fresh   integer := 7;   -- sensor considerado fresco se a leitura for dos ultimos 7 dias
BEGIN
  DROP TABLE IF EXISTS _liq_est;
  CREATE TEMP TABLE _liq_est AS
  WITH ult AS (
    -- ultima leitura do sensor por tanque (positiva)
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
    -- combustivel valorizado: so tanques com leitura fresca
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
  loja AS (
    -- loja: dbo.ESTOQUE dos grupos de MERCADORIA (conveniencia+automotivo+cigarro),
    -- produtos SEM tanque, quantidade positiva. Exclui imobilizado/servico/combustivel.
    SELECT
      e.id_empresa,
      e.id_filial,
      SUM(
        GREATEST(COALESCE(etl.safe_numeric(e.payload->>'QTDEATUAL'), e.quantidade, 0), 0)
        * COALESCE(NULLIF(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'), 0), e.custo_medio, 0)
      )::numeric(18,2) AS valor
    FROM stg.estoque e
    JOIN stg.produtos pr
      ON pr.id_empresa = e.id_empresa AND pr.id_filial = e.id_filial AND pr.id_produto = e.id_produto
    WHERE e.id_empresa = p_id_empresa
      AND (pr.payload->>'ID_GRUPOPRODUTOS')::int = ANY (
        ARRAY[2,4,7,8,9,16,39, 10, 14,15,18,12,13,11,17,21,37,40,41,19,20]
      )
      AND NOT EXISTS (
        SELECT 1 FROM stg.tanques t
        WHERE t.id_empresa = e.id_empresa AND t.id_filial = e.id_filial
          AND (t.payload->>'ID_PRODUTOS')::int = e.id_produto
      )
    GROUP BY e.id_empresa, e.id_filial
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

  -- garante linha no mes corrente para filiais com estoque
  INSERT INTO mart.liquidez_solvencia (id_empresa, id_filial, ano_mes, updated_at)
  SELECT id_empresa, id_filial, v_cur_ym, now() FROM _liq_est
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO NOTHING;

  -- aplica o snapshot de estoque a todas as linhas de mes da filial
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

COMMENT ON FUNCTION etl.refresh_liquidez_estoque(integer) IS
  'Preenche ativo_estoque da mart.liquidez_solvencia: combustivel = ultima leitura do sensor de tanque (stg.movtanques.LEITURA) fresca x custo; loja = stg.estoque grupos de mercadoria x custo (exclui imobilizado). Marca estoque_combustivel_medido por posto.';
