-- Migration 099: Solvencia — componente ATIVO ESTOQUE
-- Idempotente. Nao destrutivo. Complementa a 098 (nao altera passivo nem DRE).
--
-- Objetivo: preencher o componente ativo_estoque da mart.liquidez_solvencia com
-- o estoque VALORIZADO A CUSTO de cada posto, para entrar no cruzamento
-- "ativos cobrem o passivo do mes?".
--
-- Fonte (Xpert): dbo.ESTOQUE (QTDEATUAL por filial/produto/local de venda) —
--   NAO dbo.SALDOSPRODUTOS, que vem zerado nesta base. Custo unitario =
--   dbo.PRODUTOS.CUSTOMEDIO (espelhado em stg.produtos.payload->>'CUSTOMEDIO';
--   dw.dim_produto.custo_medio esta zerado, nao usar).
--
-- Regras:
--   valor_estoque = SUM_por_filial( GREATEST(qtde_liquida_do_produto, 0) x custo )
--   - qtde_liquida = soma dos locais de venda do produto na filial;
--   - GREATEST(.,0): estoque negativo e anomalia de cadastro, nao reduz o ativo;
--   - custo: stg.produtos.CUSTOMEDIO; fallback stg.estoque.custo_medio (quando o
--     dataset ja traz o custo do join na origem).
--
-- O estoque e um SNAPSHOT ATUAL (ponto no tempo): o mesmo valor e aplicado a
--   todas as linhas de mes da filial na mart. A leitura cruza "ativo disponivel
--   hoje" x "contas a pagar do mes selecionado" (deixado claro no disclaimer).

CREATE OR REPLACE FUNCTION etl.refresh_liquidez_estoque(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows   integer := 0;
  v_ref    date    := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
  v_cur_ym integer := (EXTRACT(YEAR FROM v_ref)::int * 100 + EXTRACT(MONTH FROM v_ref)::int);
BEGIN
  DROP TABLE IF EXISTS _liq_est_val;
  CREATE TEMP TABLE _liq_est_val AS
  WITH est AS (
    SELECT
      e.id_empresa,
      e.id_filial,
      e.id_produto,
      SUM(e.quantidade)  AS qtde,        -- saldo liquido do produto (soma dos locais)
      MAX(e.custo_medio) AS custo_stg     -- custo trazido no dataset (fallback)
    FROM stg.estoque e
    WHERE e.id_empresa = p_id_empresa
    GROUP BY e.id_empresa, e.id_filial, e.id_produto
  )
  SELECT
    est.id_empresa,
    est.id_filial,
    SUM(
      GREATEST(est.qtde, 0)
      * COALESCE(NULLIF(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'), 0), est.custo_stg, 0)
    )::numeric(18,2) AS valor_estoque
  FROM est
  LEFT JOIN stg.produtos pr
    ON pr.id_empresa = est.id_empresa
   AND pr.id_filial  = est.id_filial
   AND pr.id_produto = est.id_produto
  GROUP BY est.id_empresa, est.id_filial;

  -- Garante linha no mes corrente para filiais com estoque mas sem passivo.
  INSERT INTO mart.liquidez_solvencia (id_empresa, id_filial, ano_mes, updated_at)
  SELECT id_empresa, id_filial, v_cur_ym, now()
  FROM _liq_est_val
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO NOTHING;

  -- Aplica o snapshot de estoque a todas as linhas de mes da filial.
  UPDATE mart.liquidez_solvencia m
  SET ativo_estoque   = v.valor_estoque,
      tem_ativo_dados = true,
      updated_at      = now()
  FROM _liq_est_val v
  WHERE m.id_empresa = v.id_empresa
    AND m.id_filial  = v.id_filial;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  DROP TABLE IF EXISTS _liq_est_val;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION etl.refresh_liquidez_estoque(integer) IS
  'Preenche ativo_estoque (estoque valorizado a custo, snapshot atual) da mart.liquidez_solvencia a partir de stg.estoque x stg.produtos.CUSTOMEDIO. GREATEST(qtde,0) por produto. Marca tem_ativo_dados=true.';
