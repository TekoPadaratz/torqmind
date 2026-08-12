-- Migration 137: níveis de premiação por QUANTIDADE (não valor).
-- Fonte: planilha LSC aba METASCOMISSOES (bloco ALTERAÇÃO NOV.2025).
-- min_sales_amount permanece a coluna física; o significado passa a ser qtd mínima.

BEGIN;

COMMENT ON COLUMN app.commission_config_tier.min_sales_amount IS
  'Quantidade mínima de produtos vendidos (ativos) para o nível. Nome histórico: não é R$.';

-- View: SUM(qtd) + comprovantes ativos (sem cancel/situacao 3/devolução 14).
-- DROP necessário: quantidade_vendas muda de integer (COUNT) para numeric (SUM qtd).
DROP VIEW IF EXISTS mart.commission_sales_monthly;
CREATE VIEW mart.commission_sales_monthly AS
SELECT
  v.id_empresa,
  v.id_filial,
  (EXTRACT(YEAR FROM v.data) * 100 + EXTRACT(MONTH FROM v.data))::integer AS ano_mes,
  COALESCE(i.id_grupo_produto, -1) AS id_grupo_produto,
  COALESCE(g.nome, '(Sem grupo)') AS nome_grupo_produto,
  COALESCE(i.id_funcionario, -1) AS id_funcionario,
  COALESCE(f.nome, '(Sem vendedor)') AS nome_vendedor,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS venda_total,
  COALESCE(SUM(i.qtd), 0)::numeric(18,4) AS quantidade_vendas,
  COUNT(*)::integer AS quantidade_itens,
  CASE WHEN COALESCE(SUM(i.qtd), 0) > 0
    THEN (COALESCE(SUM(i.total), 0) / SUM(i.qtd))::numeric(18,2)
    ELSE 0
  END AS ticket_medio
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
LEFT JOIN dw.dim_funcionario f
  ON f.id_empresa = i.id_empresa
  AND f.id_filial = i.id_filial
  AND f.id_funcionario = i.id_funcionario
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(v.commercial_eligible, true) = true
  AND COALESCE(v.situacao, 0) NOT IN (2, 3, 14)
  AND COALESCE(i.cfop, 0) IN (5102, 5405, 5656, 5667, 5929)
GROUP BY
  v.id_empresa,
  v.id_filial,
  (EXTRACT(YEAR FROM v.data) * 100 + EXTRACT(MONTH FROM v.data))::integer,
  COALESCE(i.id_grupo_produto, -1),
  COALESCE(g.nome, '(Sem grupo)'),
  COALESCE(i.id_funcionario, -1),
  COALESCE(f.nome, '(Sem vendedor)');

-- Seed/overwrite LSC (id_empresa=1) a partir da planilha NOV.2025.
-- IDs explícitos: apelido operacional + CNPJ antigo do mesmo posto.
DO $$
DECLARE
  rec RECORD;
  v_config_id bigint;
BEGIN
  FOR rec IN
    SELECT * FROM (VALUES
      -- codigo, id_filial, bronze_q, bronze_p, prata_q, prata_p, ouro_q, ouro_p, diam_q, diam_p
      ('VR01',  14458, 50, 2.0, 110, 3.0, 160, 5.0, 300, 7.0),
      ('VR02',  17337, 40, 2.0,  90, 3.0, 130, 5.0, 240, 7.0),
      ('VR02',  14388, 40, 2.0,  90, 3.0, 130, 5.0, 240, 7.0),
      ('VR04',  16305, 40, 2.0,  60, 3.0,  90, 5.0, 190, 7.0),
      ('VR04',  14417, 40, 2.0,  60, 3.0,  90, 5.0, 190, 7.0),
      ('VR05',  14122, 50, 2.0,  80, 3.0, 120, 5.0, 210, 7.0),
      ('VR06',  11621, 40, 2.0,  50, 3.0,  80, 5.0, 165, 7.0),
      ('VR07',  10169, 30, 2.0,  45, 3.0,  70, 5.0, 110, 7.0),
      ('VR08',  15383, 40, 2.0,  60, 3.0,  90, 5.0, 190, 7.0),
      ('PVR01', 18176, 40, 2.0,  90, 3.0, 130, 5.0, 240, 7.0)
    ) AS t(codigo, id_filial, bq, bp, pq, pp, oq, op, dq, dp)
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM auth.filiais f
      WHERE f.id_empresa = 1 AND f.id_filial = rec.id_filial
    ) THEN
      CONTINUE;
    END IF;

    SELECT c.id INTO v_config_id
    FROM app.commission_config c
    WHERE c.id_empresa = 1 AND c.id_filial = rec.id_filial AND c.is_active = true
    LIMIT 1;

    IF v_config_id IS NULL THEN
      INSERT INTO app.commission_config (
        id_empresa, id_filial, name, is_active, default_payment_mode,
        manager_commission_mode, manager_commission_percent
      ) VALUES (
        1, rec.id_filial, 'Comissao padrao', true, 'team_total', 'use_tiers', 0
      )
      RETURNING id INTO v_config_id;
    END IF;

    DELETE FROM app.commission_config_tier WHERE config_id = v_config_id;
    INSERT INTO app.commission_config_tier
      (config_id, tier_key, tier_name, min_sales_amount, commission_percent, sort_order, is_active)
    VALUES
      (v_config_id, 'bronze',  'Bronze',   rec.bq, rec.bp, 1, true),
      (v_config_id, 'silver',  'Prata',    rec.pq, rec.pp, 2, true),
      (v_config_id, 'gold',    'Ouro',     rec.oq, rec.op, 3, true),
      (v_config_id, 'diamond', 'Diamante', rec.dq, rec.dp, 4, true);
  END LOOP;
END $$;

COMMIT;
