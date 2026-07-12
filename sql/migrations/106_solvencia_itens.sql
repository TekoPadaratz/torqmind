-- Migration 106: DRE Solvencia detalhada (itens/linhas) + entradas manuais.
-- Branch vs001.002-DRE. Idempotente. Nao destrutivo.
--
-- Objetivo (dono): aba Solvencia do DRE Gerencial no formato do "Fechamento de
-- Caixa Geral" do Xpert — detalhada por item (combustivel por tipo, estoque,
-- aprazo, cartoes por operadora, cheques por banco, dinheiro, boletos), com
-- Ativo Circulante / Ativo Nao-Circulante / Passivo totalizados.
--
-- Os ativos financeiros (caixa/banco/cartoes/cheques) nunca foram preenchidos
-- (colunas zeradas na 098). Aqui materializamos os ITENS reais por posto a
-- partir da STG, e o que nao existe na fonte do cliente (bancos com saldo,
-- investimentos) entra como ENTRADA MANUAL mantida pelo dono.

-- ===========================================================================
-- 1) Entradas manuais (dono cadastra: bancos com saldo, investimentos, outros).
--    App-owned. Itemizada (descricao + valor), por filial.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS app.solvencia_entrada_manual (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  secao        text    NOT NULL,               -- 'banco' | 'investimento' | 'ativo_outro' | 'passivo_outro'
  descricao    text    NOT NULL,               -- 'Banco do Brasil', 'Fazenda', 'Caminhao'...
  valor        numeric(18,2) NOT NULL DEFAULT 0,
  ordem        integer NOT NULL DEFAULT 0,
  ativo        boolean NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_solv_manual_secao
    CHECK (secao IN ('banco','investimento','ativo_outro','passivo_outro'))
);

CREATE INDEX IF NOT EXISTS ix_solv_manual_scope
  ON app.solvencia_entrada_manual (id_empresa, id_filial, secao) WHERE ativo;

COMMENT ON TABLE app.solvencia_entrada_manual IS
  'Entradas manuais da aba Solvencia (bancos com saldo, investimentos, outros ativos/passivos) mantidas pelo dono, por filial. Fonte para itens origem=manual na mart.solvencia_item.';

-- ===========================================================================
-- 2) Itens detalhados da Solvencia (posicao ATUAL por filial). Snapshot:
--    reconstruido a cada refresh. Cada linha = uma linha da tela.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS mart.solvencia_item (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  grupo        text    NOT NULL,   -- 'ativo_circulante' | 'ativo_nao_circulante' | 'passivo_circulante'
  secao        text    NOT NULL,   -- 'combustivel'|'estoque'|'aprazo'|'cartoes'|'cheques'|'dinheiro'|'banco'|'boleto'|'investimento'|'outro'
  item_label   text    NOT NULL,   -- 'S10','GC','Cartao Baratao','Banco Brasil','Fazenda'...
  valor        numeric(18,2) NOT NULL DEFAULT 0,
  qtd          numeric(18,3),      -- opcional (ex.: litros de combustivel)
  origem       text    NOT NULL DEFAULT 'auto',  -- 'auto' | 'manual'
  ordem        integer NOT NULL DEFAULT 0,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, grupo, secao, item_label)
);

CREATE INDEX IF NOT EXISTS ix_solvencia_item_scope
  ON mart.solvencia_item (id_empresa, id_filial, grupo, secao);

COMMENT ON TABLE mart.solvencia_item IS
  'Itens/linhas da aba Solvencia do DRE Gerencial (posicao atual por filial): ativo circulante (combustivel/estoque/aprazo/cartoes/cheques/dinheiro/banco), ativo nao-circulante (investimentos), passivo circulante (boletos). origem auto=STG, manual=app.solvencia_entrada_manual. Camada rapida da tela.';

-- ===========================================================================
-- 3) ETL: reconstroi os itens AUTO da STG + faz merge das entradas manuais.
--    Idempotente, advisory-lock por empresa. Horizonte = posicao atual.
-- ===========================================================================
CREATE OR REPLACE FUNCTION etl.refresh_solvencia_itens(p_id_empresa integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_ref  date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_solvencia_itens'), p_id_empresa);

  -- Zera apenas os itens AUTO (preserva manuais, que vem da app).
  DELETE FROM mart.solvencia_item
  WHERE id_empresa = p_id_empresa AND origem = 'auto';

  -- ---- 3.1 ATIVO CIRCULANTE: CHEQUES por banco (a compensar) --------------
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, qtd, origem, ordem, updated_at)
  SELECT
    c.id_empresa, c.id_filial, 'ativo_circulante', 'cheques',
    'Banco ' || COALESCE(NULLIF(c.payload->>'CODIGOBANCOSPADRAO',''),'?'),
    SUM(etl.safe_numeric(c.payload->>'VALOR'))::numeric(18,2),
    COUNT(*)::numeric, 'auto', 50, now()
  FROM stg.cheques c
  WHERE c.id_empresa = p_id_empresa
    AND c.payload->>'DTACOMPENSADO' IS NULL
    AND COALESCE(c.payload->>'SITUACAOCHEQUE','0') <> '2'   -- exclui devolvido
    AND etl.safe_numeric(c.payload->>'VALOR') > 0
  GROUP BY c.id_empresa, c.id_filial, c.payload->>'CODIGOBANCOSPADRAO';

  -- ---- 3.2 ATIVO CIRCULANTE: APRAZO (contas a receber em aberto) ----------
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, origem, ordem, updated_at)
  SELECT
    r.id_empresa, r.id_filial, 'ativo_circulante', 'aprazo', 'A Prazo',
    SUM(GREATEST(etl.safe_numeric(r.payload->>'VALOR') - COALESCE(etl.safe_numeric(r.payload->>'VLRPAGO'),0), 0))::numeric(18,2),
    'auto', 30, now()
  FROM stg.contasreceber r
  WHERE r.id_empresa = p_id_empresa
    AND r.payload->>'DTAPGTO' IS NULL
  GROUP BY r.id_empresa, r.id_filial
  HAVING SUM(GREATEST(etl.safe_numeric(r.payload->>'VALOR') - COALESCE(etl.safe_numeric(r.payload->>'VLRPAGO'),0), 0)) > 0;

  -- ---- 3.3 ESTOQUE valorizado, classificado por GRUPO (NOMEGRUPOPRODUTOS):
  --      COMBUSTIVEIS -> combustivel por tipo (volume do TANQUE x custo);
  --      IMOBILIZADO  -> investimentos (ativo nao-circulante, por item);
  --      demais grupos -> "Estoque Loja" (QTDEATUAL x custo, consolidado).
  --      Custo = CUSTOGERENCIAL, fallback CUSTOMEDIO/ULTVLRENTRADA.
  WITH grp AS (
    SELECT DISTINCT ON (g.id_filial, (g.payload->>'ID_GRUPOPRODUTOS'))
      g.id_filial, (g.payload->>'ID_GRUPOPRODUTOS') AS id_grupo,
      UPPER(COALESCE(g.payload->>'NOMEGRUPOPRODUTOS','')) AS nome_grupo
    FROM stg.grupoprodutos g
    WHERE g.id_empresa = p_id_empresa
    ORDER BY g.id_filial, (g.payload->>'ID_GRUPOPRODUTOS')
  ),
  prod AS (
    SELECT DISTINCT ON (p.id_filial, (p.payload->>'ID_PRODUTOS'))
      p.id_filial, (p.payload->>'ID_PRODUTOS') AS id_produto,
      COALESCE(NULLIF(p.payload->>'NOMEPRODUTO',''), p.payload->>'PRODUTO', '?') AS nome,
      (p.payload->>'ID_GRUPOPRODUTOS') AS id_grupo,
      COALESCE(
        NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
        NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
        NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
        0) AS custo
    FROM stg.produtos p
    WHERE p.id_empresa = p_id_empresa
    ORDER BY p.id_filial, (p.payload->>'ID_PRODUTOS')
  ),
  prodc AS (
    SELECT pr.id_filial, pr.id_produto, pr.nome, pr.custo,
      CASE
        WHEN gr.nome_grupo LIKE '%COMBUST%' THEN 'combustivel'
        WHEN gr.nome_grupo LIKE '%IMOBIL%' OR gr.nome_grupo LIKE '%INVEST%' THEN 'imobilizado'
        ELSE 'loja'
      END AS classe
    FROM prod pr
    LEFT JOIN grp gr ON gr.id_filial = pr.id_filial AND gr.id_grupo = pr.id_grupo
  ),
  tanq AS (   -- volume atual de combustivel por produto (ultima leitura do tanque)
    SELECT t.id_filial, (t.payload->>'ID_PRODUTOS') AS id_produto,
           SUM(mt.qtde)::numeric AS litros
    FROM stg.tanques t
    JOIN LATERAL (
      SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'), 0) AS qtde
      FROM stg.movtanques m
      WHERE m.id_empresa = t.id_empresa AND m.id_filial = t.id_filial
        AND m.payload->>'ID_TANQUES' = t.payload->>'ID_TANQUES'
      ORDER BY etl.safe_timestamp(m.payload->>'DTACONTA') DESC NULLS LAST
      LIMIT 1
    ) mt ON true
    WHERE t.id_empresa = p_id_empresa
    GROUP BY t.id_filial, (t.payload->>'ID_PRODUTOS')
  ),
  est AS (
    SELECT e.id_filial, (e.payload->>'ID_PRODUTOS') AS id_produto,
           SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde
    FROM stg.estoque e
    WHERE e.id_empresa = p_id_empresa
    GROUP BY e.id_filial, (e.payload->>'ID_PRODUTOS')
  ),
  itens AS (
    -- Combustivel: volume do tanque x custo, por produto
    SELECT tq.id_filial, 'ativo_circulante' AS grupo, 'combustivel' AS secao,
           pc.nome AS item_label, (tq.litros * pc.custo)::numeric(18,2) AS valor,
           tq.litros AS qtd, 10 AS ordem
    FROM tanq tq
    JOIN prodc pc ON pc.id_filial = tq.id_filial AND pc.id_produto = tq.id_produto
    WHERE pc.classe = 'combustivel' AND tq.litros * pc.custo > 0
    UNION ALL
    -- Estoque loja: consolidado (produtos que nao sao combustivel nem imobilizado)
    SELECT e.id_filial, 'ativo_circulante', 'estoque', 'Estoque Loja',
           SUM(e.qtde * pc.custo)::numeric(18,2), NULL::numeric, 20
    FROM est e
    JOIN prodc pc ON pc.id_filial = e.id_filial AND pc.id_produto = e.id_produto
    WHERE pc.classe = 'loja' AND e.qtde <> 0 AND pc.custo > 0
    GROUP BY e.id_filial
    HAVING SUM(e.qtde * pc.custo) > 0
    UNION ALL
    -- Imobilizado: investimentos por item (ativo nao-circulante)
    SELECT e.id_filial, 'ativo_nao_circulante', 'investimento', pc.nome,
           SUM(e.qtde * pc.custo)::numeric(18,2), NULL::numeric, 200
    FROM est e
    JOIN prodc pc ON pc.id_filial = e.id_filial AND pc.id_produto = e.id_produto
    WHERE pc.classe = 'imobilizado' AND e.qtde <> 0 AND pc.custo > 0
    GROUP BY e.id_filial, pc.nome
    HAVING SUM(e.qtde * pc.custo) > 0
  )
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, qtd, origem, ordem, updated_at)
  SELECT p_id_empresa, i.id_filial, i.grupo, i.secao, i.item_label, i.valor, i.qtd, 'auto', i.ordem, now()
  FROM itens i
  WHERE i.valor > 0
  ON CONFLICT (id_empresa, id_filial, grupo, secao, item_label) DO UPDATE
    SET valor = EXCLUDED.valor, qtd = EXCLUDED.qtd, updated_at = now();

  -- ---- 3.4 PASSIVO CIRCULANTE: BOLETOS / contas a pagar em aberto ---------
  --      Posicao: titulos em aberto vencendo ate o fim do mes atual.
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, origem, ordem, updated_at)
  SELECT
    pg.id_empresa, pg.id_filial, 'passivo_circulante', 'boleto', 'Contas a Pagar',
    SUM(GREATEST(etl.safe_numeric(pg.payload->>'VALOR') - COALESCE(etl.safe_numeric(pg.payload->>'VLRPAGO'),0), 0))::numeric(18,2),
    'auto', 90, now()
  FROM stg.contaspagar pg
  WHERE pg.id_empresa = p_id_empresa
    AND pg.payload->>'DTAPGTO' IS NULL
    AND etl.safe_timestamp(pg.payload->>'DTAVCTO') IS NOT NULL
    AND (etl.safe_timestamp(pg.payload->>'DTAVCTO'))::date
        <= (date_trunc('month', v_ref) + interval '1 month - 1 day')::date
  GROUP BY pg.id_empresa, pg.id_filial
  HAVING SUM(GREATEST(etl.safe_numeric(pg.payload->>'VALOR') - COALESCE(etl.safe_numeric(pg.payload->>'VLRPAGO'),0), 0)) > 0;

  -- ---- 3.5 MERGE das ENTRADAS MANUAIS (bancos, investimentos, outros) -----
  DELETE FROM mart.solvencia_item
  WHERE id_empresa = p_id_empresa AND origem = 'manual';

  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, origem, ordem, updated_at)
  SELECT
    m.id_empresa, m.id_filial,
    CASE
      WHEN m.secao = 'investimento'   THEN 'ativo_nao_circulante'
      WHEN m.secao = 'passivo_outro'  THEN 'passivo_circulante'
      ELSE 'ativo_circulante'
    END AS grupo,
    CASE WHEN m.secao IN ('ativo_outro','passivo_outro') THEN 'outro' ELSE m.secao END AS secao,
    m.descricao, m.valor, 'manual',
    CASE m.secao WHEN 'banco' THEN 60 WHEN 'investimento' THEN 200 WHEN 'passivo_outro' THEN 95 ELSE 70 END + COALESCE(m.ordem,0),
    now()
  FROM app.solvencia_entrada_manual m
  WHERE m.id_empresa = p_id_empresa AND m.ativo
  ON CONFLICT (id_empresa, id_filial, grupo, secao, item_label) DO UPDATE
    SET valor = EXCLUDED.valor, origem = 'manual', ordem = EXCLUDED.ordem, updated_at = now();

  SELECT COUNT(*) INTO v_rows FROM mart.solvencia_item WHERE id_empresa = p_id_empresa;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_solvencia_itens(integer) IS
  'Reconstroi mart.solvencia_item por filial: cheques por banco, aprazo, estoque valorizado (combustivel por tipo + loja), boletos (passivo) da STG + merge das entradas manuais (bancos/investimentos). Idempotente, advisory-lock. Cartoes por operadora entram quando stg.movcartaodebito for coletado.';
