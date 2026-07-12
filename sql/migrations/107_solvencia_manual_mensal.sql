-- Migration 107: entradas manuais da Solvencia POR MES + tipos.
-- Branch vs001.002-DRE. Idempotente. Nao destrutivo (tabela manual ainda vazia).
--
-- Refina o modelo da 106: o dono cadastra bancos/investimentos (e outros que
-- precisem de preenchimento manual) ITEMIZADOS e POR MES/ANO. Cada painel manual
-- (banco, investimento...) e um "tipo" (id_tipo) com grupo/ordem. A tela mostra
-- o total do painel e, no hover/clique, abre os itens (nome + valor) editaveis.
--
-- Armazenamento no Postgres app (owner-maintained, editavel) — padrao TorqMind
-- para config do dono (app.budget_conta, app.company_branding). A camada rapida
-- da tela (mart.solvencia_item) continua com os itens AUTO; o repo mescla os
-- manuais do mes selecionado na leitura.

-- 1) Tipos de entrada manual (definem cada painel manual e onde ele entra no DRE)
CREATE TABLE IF NOT EXISTS app.solvencia_tipo_manual (
  id_tipo   smallint PRIMARY KEY,
  chave     text NOT NULL UNIQUE,      -- 'banco' | 'investimento' | 'ativo_outro' | 'passivo_outro'
  nome      text NOT NULL,             -- 'Bancos' | 'Investimentos'
  grupo     text NOT NULL,             -- 'ativo_circulante' | 'ativo_nao_circulante' | 'passivo_circulante'
  secao     text NOT NULL,             -- secao usada em mart.solvencia_item/render
  ordem     integer NOT NULL DEFAULT 0
);

INSERT INTO app.solvencia_tipo_manual (id_tipo, chave, nome, grupo, secao, ordem) VALUES
  (1, 'banco',         'Bancos',         'ativo_circulante',     'banco',        60),
  (2, 'investimento',  'Investimentos',  'ativo_nao_circulante', 'investimento', 210),
  (3, 'ativo_outro',   'Outros Ativos',  'ativo_circulante',     'outro',        80),
  (4, 'passivo_outro', 'Outros Passivos','passivo_circulante',   'outro',        95)
ON CONFLICT (id_tipo) DO UPDATE
  SET chave=EXCLUDED.chave, nome=EXCLUDED.nome, grupo=EXCLUDED.grupo, secao=EXCLUDED.secao, ordem=EXCLUDED.ordem;

COMMENT ON TABLE app.solvencia_tipo_manual IS
  'Tipos de entrada manual da Solvencia (banco, investimento, outros). Cada tipo vira um painel na tela com total + itens editaveis.';

-- 2) Entradas manuais POR MES (ano, mes) + tipo. Estrutura evoluida da 106
--    (tabela ainda vazia em prod; ALTER nao-destrutivo).
ALTER TABLE app.solvencia_entrada_manual DROP CONSTRAINT IF EXISTS ck_solv_manual_secao;
ALTER TABLE app.solvencia_entrada_manual
  ADD COLUMN IF NOT EXISTS ano     smallint,
  ADD COLUMN IF NOT EXISTS mes     smallint,
  ADD COLUMN IF NOT EXISTS id_tipo smallint;
ALTER TABLE app.solvencia_entrada_manual DROP COLUMN IF EXISTS secao;

-- Preenche defaults e trava as chaves obrigatorias (tabela vazia -> seguro).
UPDATE app.solvencia_entrada_manual SET id_tipo = 1 WHERE id_tipo IS NULL;
UPDATE app.solvencia_entrada_manual
  SET ano = EXTRACT(YEAR FROM now())::smallint, mes = EXTRACT(MONTH FROM now())::smallint
  WHERE ano IS NULL OR mes IS NULL;

ALTER TABLE app.solvencia_entrada_manual ALTER COLUMN ano     SET NOT NULL;
ALTER TABLE app.solvencia_entrada_manual ALTER COLUMN mes     SET NOT NULL;
ALTER TABLE app.solvencia_entrada_manual ALTER COLUMN id_tipo SET NOT NULL;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solv_manual_tipo') THEN
    ALTER TABLE app.solvencia_entrada_manual
      ADD CONSTRAINT fk_solv_manual_tipo FOREIGN KEY (id_tipo)
      REFERENCES app.solvencia_tipo_manual(id_tipo);
  END IF;
END $$;

DROP INDEX IF EXISTS app.ix_solv_manual_scope;
CREATE INDEX IF NOT EXISTS ix_solv_manual_scope
  ON app.solvencia_entrada_manual (id_empresa, id_filial, ano, mes, id_tipo) WHERE ativo;

COMMENT ON TABLE app.solvencia_entrada_manual IS
  'Entradas manuais itemizadas da Solvencia POR MES: (id_empresa,id_filial,ano,mes,id_tipo) + descricao + valor. Ex.: bancos (Banco do Brasil R$x, Caixa R$y) e investimentos, editados no hover/popup da tela. Servidas pelo repo no mes selecionado.';

-- 3) ETL auto-only: remove o merge manual (agora servido por mes pelo repo).
--    Reconstroi apenas os itens AUTO da STG. Manuais nao entram na mart.
CREATE OR REPLACE FUNCTION etl.refresh_solvencia_itens(p_id_empresa integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_ref  date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_solvencia_itens'), p_id_empresa);

  DELETE FROM mart.solvencia_item WHERE id_empresa = p_id_empresa;

  -- 3.1 CHEQUES por banco (a compensar)
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, qtd, origem, ordem, updated_at)
  SELECT c.id_empresa, c.id_filial, 'ativo_circulante', 'cheques',
    'Banco ' || COALESCE(NULLIF(c.payload->>'CODIGOBANCOSPADRAO',''),'?'),
    SUM(etl.safe_numeric(c.payload->>'VALOR'))::numeric(18,2),
    COUNT(*)::numeric, 'auto', 50, now()
  FROM stg.cheques c
  WHERE c.id_empresa = p_id_empresa
    AND c.payload->>'DTACOMPENSADO' IS NULL
    AND COALESCE(c.payload->>'SITUACAOCHEQUE','0') <> '2'
    AND etl.safe_numeric(c.payload->>'VALOR') > 0
  GROUP BY c.id_empresa, c.id_filial, c.payload->>'CODIGOBANCOSPADRAO';

  -- 3.2 APRAZO (contas a receber em aberto)
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, origem, ordem, updated_at)
  SELECT r.id_empresa, r.id_filial, 'ativo_circulante', 'aprazo', 'A Prazo',
    SUM(GREATEST(etl.safe_numeric(r.payload->>'VALOR') - COALESCE(etl.safe_numeric(r.payload->>'VLRPAGO'),0), 0))::numeric(18,2),
    'auto', 30, now()
  FROM stg.contasreceber r
  WHERE r.id_empresa = p_id_empresa AND r.payload->>'DTAPGTO' IS NULL
  GROUP BY r.id_empresa, r.id_filial
  HAVING SUM(GREATEST(etl.safe_numeric(r.payload->>'VALOR') - COALESCE(etl.safe_numeric(r.payload->>'VLRPAGO'),0), 0)) > 0;

  -- 3.3 ESTOQUE por grupo: combustivel (tanque) / imobilizado (investimento) / loja
  WITH grp AS (
    SELECT DISTINCT ON (g.id_filial, (g.payload->>'ID_GRUPOPRODUTOS'))
      g.id_filial, (g.payload->>'ID_GRUPOPRODUTOS') AS id_grupo,
      UPPER(COALESCE(g.payload->>'NOMEGRUPOPRODUTOS','')) AS nome_grupo
    FROM stg.grupoprodutos g WHERE g.id_empresa = p_id_empresa
    ORDER BY g.id_filial, (g.payload->>'ID_GRUPOPRODUTOS')
  ),
  prod AS (
    SELECT DISTINCT ON (p.id_filial, (p.payload->>'ID_PRODUTOS'))
      p.id_filial, (p.payload->>'ID_PRODUTOS') AS id_produto,
      COALESCE(NULLIF(p.payload->>'NOMEPRODUTO',''), p.payload->>'PRODUTO', '?') AS nome,
      (p.payload->>'ID_GRUPOPRODUTOS') AS id_grupo,
      COALESCE(NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
               NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
               NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),0) AS custo
    FROM stg.produtos p WHERE p.id_empresa = p_id_empresa
    ORDER BY p.id_filial, (p.payload->>'ID_PRODUTOS')
  ),
  prodc AS (
    SELECT pr.id_filial, pr.id_produto, pr.nome, pr.custo,
      CASE WHEN gr.nome_grupo LIKE '%COMBUST%' THEN 'combustivel'
           WHEN gr.nome_grupo LIKE '%IMOBIL%' OR gr.nome_grupo LIKE '%INVEST%' THEN 'imobilizado'
           ELSE 'loja' END AS classe
    FROM prod pr LEFT JOIN grp gr ON gr.id_filial = pr.id_filial AND gr.id_grupo = pr.id_grupo
  ),
  tanq AS (
    SELECT t.id_filial, (t.payload->>'ID_PRODUTOS') AS id_produto, SUM(mt.qtde)::numeric AS litros
    FROM stg.tanques t
    JOIN LATERAL (
      SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'), 0) AS qtde
      FROM stg.movtanques m
      WHERE m.id_empresa = t.id_empresa AND m.id_filial = t.id_filial
        AND m.payload->>'ID_TANQUES' = t.payload->>'ID_TANQUES'
      ORDER BY etl.safe_timestamp(m.payload->>'DTACONTA') DESC NULLS LAST LIMIT 1
    ) mt ON true
    WHERE t.id_empresa = p_id_empresa
    GROUP BY t.id_filial, (t.payload->>'ID_PRODUTOS')
  ),
  est AS (
    SELECT e.id_filial, (e.payload->>'ID_PRODUTOS') AS id_produto,
           SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde
    FROM stg.estoque e WHERE e.id_empresa = p_id_empresa
    GROUP BY e.id_filial, (e.payload->>'ID_PRODUTOS')
  ),
  itens AS (
    SELECT tq.id_filial, 'ativo_circulante' AS grupo, 'combustivel' AS secao, pc.nome AS item_label,
           (tq.litros * pc.custo)::numeric(18,2) AS valor, tq.litros AS qtd, 10 AS ordem
    FROM tanq tq JOIN prodc pc ON pc.id_filial = tq.id_filial AND pc.id_produto = tq.id_produto
    WHERE pc.classe = 'combustivel' AND tq.litros * pc.custo > 0
    UNION ALL
    SELECT e.id_filial, 'ativo_circulante', 'estoque', 'Estoque Loja',
           SUM(e.qtde * pc.custo)::numeric(18,2), NULL::numeric, 20
    FROM est e JOIN prodc pc ON pc.id_filial = e.id_filial AND pc.id_produto = e.id_produto
    WHERE pc.classe = 'loja' AND e.qtde <> 0 AND pc.custo > 0
    GROUP BY e.id_filial HAVING SUM(e.qtde * pc.custo) > 0
    UNION ALL
    SELECT e.id_filial, 'ativo_nao_circulante', 'investimento', pc.nome,
           SUM(e.qtde * pc.custo)::numeric(18,2), NULL::numeric, 200
    FROM est e JOIN prodc pc ON pc.id_filial = e.id_filial AND pc.id_produto = e.id_produto
    WHERE pc.classe = 'imobilizado' AND e.qtde <> 0 AND pc.custo > 0
    GROUP BY e.id_filial, pc.nome HAVING SUM(e.qtde * pc.custo) > 0
  )
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, qtd, origem, ordem, updated_at)
  SELECT p_id_empresa, i.id_filial, i.grupo, i.secao, i.item_label, i.valor, i.qtd, 'auto', i.ordem, now()
  FROM itens i WHERE i.valor > 0
  ON CONFLICT (id_empresa, id_filial, grupo, secao, item_label) DO UPDATE
    SET valor = EXCLUDED.valor, qtd = EXCLUDED.qtd, updated_at = now();

  -- 3.4 PASSIVO: contas a pagar em aberto vencendo ate o fim do mes
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, origem, ordem, updated_at)
  SELECT pg.id_empresa, pg.id_filial, 'passivo_circulante', 'boleto', 'Contas a Pagar',
    SUM(GREATEST(etl.safe_numeric(pg.payload->>'VALOR') - COALESCE(etl.safe_numeric(pg.payload->>'VLRPAGO'),0), 0))::numeric(18,2),
    'auto', 90, now()
  FROM stg.contaspagar pg
  WHERE pg.id_empresa = p_id_empresa AND pg.payload->>'DTAPGTO' IS NULL
    AND etl.safe_timestamp(pg.payload->>'DTAVCTO') IS NOT NULL
    AND (etl.safe_timestamp(pg.payload->>'DTAVCTO'))::date <= (date_trunc('month', v_ref) + interval '1 month - 1 day')::date
  GROUP BY pg.id_empresa, pg.id_filial
  HAVING SUM(GREATEST(etl.safe_numeric(pg.payload->>'VALOR') - COALESCE(etl.safe_numeric(pg.payload->>'VLRPAGO'),0), 0)) > 0;

  SELECT COUNT(*) INTO v_rows FROM mart.solvencia_item WHERE id_empresa = p_id_empresa;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_solvencia_itens(integer) IS
  'Reconstroi os itens AUTO da mart.solvencia_item (cheques por banco, aprazo, combustivel por tanque, estoque loja, investimentos=imobilizado, boletos). Entradas manuais (bancos/investimentos) sao servidas por mes pelo repo, nao entram na mart.';
