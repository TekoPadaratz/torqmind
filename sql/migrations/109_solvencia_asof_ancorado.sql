-- Migration 109: Solvência as-of confiável — estoque loja ancorado + combustível via movtanques.
-- Branch vs001.002-DRE. Idempotente. Não destrutivo.
--
-- Problema (conferência VR01 14458):
--   * refresh_liquidez_estoque_loja_asof (108) reconstruía qtde só com MOV desde
--     bootstrap (sem saldo de abertura) → ~R$ 9M, vs ESTOQUE Xpert ~R$ 535k.
--   * ativo_estoque_combustivel vinha do sensor atual copiado em todos os meses
--     (migration 100 UPDATE sem filtro de ano_mes) → Jun=Jul=471k falso.
--   * Overlay da API zerava cheques (coluna sem ETL as-of).
--
-- Correção:
--   1) Estoque loja as-of = QTDE atual (stg.estoque) − movimentos após o corte,
--      só grupos de mercadoria (whitelist da 100), exclusão de produto de tanque.
--   2) Combustível as-of = última LEITURA de stg.movtanques com
--      tanque.id_tanque = mov.ID_TANQUES, antes do corte, × CUSTOMEDIO.
--      Se não houver leitura antes do corte, NÃO sobrescreve (mantém valor prévio).

CREATE OR REPLACE FUNCTION etl.refresh_liquidez_estoque_loja_asof(
  p_id_empresa integer,
  p_id_filial integer DEFAULT NULL,
  p_ano_mes integer DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_tz   text := 'America/Sao_Paulo';
  -- Mesmos grupos de mercadoria da migration 100 (conveniência/automotivo/cigarro…).
  v_grupos_loja int[] := ARRAY[2,4,7,8,9,16,39,10,14,15,18,12,13,11,17,21,37,40,41,19,20];
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_liquidez_estoque_loja_asof'), p_id_empresa);

  WITH cortes AS (
    SELECT p_ano_mes AS ano_mes,
           make_date(p_ano_mes / 100, p_ano_mes % 100, 1) AS corte
    WHERE p_ano_mes IS NOT NULL
    UNION ALL
    SELECT (EXTRACT(YEAR FROM d)::int * 100 + EXTRACT(MONTH FROM d)::int) AS ano_mes,
           (d)::date AS corte
    FROM generate_series(
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) - interval '3 months',
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) + interval '1 month',
      interval '1 month'
    ) AS g(d)
    WHERE p_ano_mes IS NULL
  ),
  base AS (
    SELECT
      e.id_empresa,
      e.id_filial,
      e.id_produto,
      GREATEST(COALESCE(e.quantidade, 0), 0)::numeric(18,6) AS q_hoje,
      COALESCE(
        NULLIF(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'), 0),
        NULLIF(e.custo_medio, 0),
        NULLIF(etl.safe_numeric(pr.payload->>'CUSTOGERENCIAL'), 0),
        0
      ) AS custo
    FROM stg.estoque e
    JOIN stg.produtos pr
      ON pr.id_empresa = e.id_empresa
     AND pr.id_filial = e.id_filial
     AND pr.id_produto = e.id_produto
    WHERE e.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR e.id_filial = p_id_filial)
      AND etl.safe_int(pr.payload->>'ID_GRUPOPRODUTOS') = ANY (v_grupos_loja)
      AND NOT EXISTS (
        SELECT 1 FROM stg.tanques t
        WHERE t.id_empresa = e.id_empresa AND t.id_filial = e.id_filial
          AND etl.safe_int(t.payload->>'ID_PRODUTOS') = e.id_produto
      )
  ),
  delta AS (
    SELECT
      ct.ano_mes,
      i.id_empresa,
      i.id_filial,
      COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
      SUM(
        CASE
          WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 1
            THEN COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
          WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 0
            THEN -COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
          ELSE 0
        END
      )::numeric(18,6) AS q_pos_corte
    FROM cortes ct
    JOIN stg.itensmovprodutos i
      ON i.id_empresa = p_id_empresa
     AND (p_id_filial IS NULL OR i.id_filial = p_id_filial)
    JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db
     AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
     AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) >= ct.corte
     AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA'))
           < (now() AT TIME ZONE v_tz)
    GROUP BY ct.ano_mes, i.id_empresa, i.id_filial,
             COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
  ),
  loja AS (
    SELECT
      b.id_empresa,
      b.id_filial,
      ct.ano_mes,
      SUM(
        GREATEST(b.q_hoje - COALESCE(d.q_pos_corte, 0), 0) * b.custo
      )::numeric(18,2) AS ativo_estoque_loja
    FROM cortes ct
    CROSS JOIN base b
    LEFT JOIN delta d
      ON d.ano_mes = ct.ano_mes
     AND d.id_empresa = b.id_empresa
     AND d.id_filial = b.id_filial
     AND d.id_produto = b.id_produto
    WHERE b.custo > 0
    GROUP BY b.id_empresa, b.id_filial, ct.ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS m
    (id_empresa, id_filial, ano_mes, ativo_estoque_loja, ativo_estoque, tem_ativo_dados, updated_at)
  SELECT
    l.id_empresa, l.id_filial, l.ano_mes,
    l.ativo_estoque_loja,
    COALESCE(m0.ativo_estoque_combustivel, 0) + l.ativo_estoque_loja,
    true, now()
  FROM loja l
  LEFT JOIN mart.liquidez_solvencia m0
    ON m0.id_empresa = l.id_empresa AND m0.id_filial = l.id_filial AND m0.ano_mes = l.ano_mes
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
    ativo_estoque_loja = EXCLUDED.ativo_estoque_loja,
    ativo_estoque      = COALESCE(m.ativo_estoque_combustivel, 0)
                         + EXCLUDED.ativo_estoque_loja,
    tem_ativo_dados    = true,
    updated_at         = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_estoque_loja_asof(integer, integer, integer) IS
  'Solvência: ativo_estoque_loja as-of = stg.estoque (grupos mercadoria) − MOV após o corte. Ancorado no ESTOQUE Xpert; não reconstrói desde zero.';


CREATE OR REPLACE FUNCTION etl.refresh_liquidez_combustivel_asof(
  p_id_empresa integer,
  p_id_filial integer DEFAULT NULL,
  p_ano_mes integer DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_tz   text := 'America/Sao_Paulo';
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_liquidez_combustivel_asof'), p_id_empresa);

  WITH cortes AS (
    SELECT p_ano_mes AS ano_mes,
           make_date(p_ano_mes / 100, p_ano_mes % 100, 1) AS corte
    WHERE p_ano_mes IS NOT NULL
    UNION ALL
    SELECT (EXTRACT(YEAR FROM d)::int * 100 + EXTRACT(MONTH FROM d)::int) AS ano_mes,
           (d)::date AS corte
    FROM generate_series(
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) - interval '3 months',
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) + interval '1 month',
      interval '1 month'
    ) AS g(d)
    WHERE p_ano_mes IS NULL
  ),
  tanques AS (
    SELECT t.id_empresa, t.id_filial, t.id_tanque,
           etl.safe_int(t.payload->>'ID_PRODUTOS') AS id_produto
    FROM stg.tanques t
    WHERE t.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR t.id_filial = p_id_filial)
  ),
  leituras AS (
    SELECT ct.ano_mes, tn.id_empresa, tn.id_filial, tn.id_produto, tn.id_tanque,
      (
        SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'), 0)
        FROM stg.movtanques m
        WHERE m.id_empresa = tn.id_empresa AND m.id_filial = tn.id_filial
          AND etl.safe_int(m.payload->>'ID_TANQUES') = tn.id_tanque
          AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DTACONTA')) < ct.corte
        ORDER BY COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DTACONTA')) DESC NULLS LAST
        LIMIT 1
      ) AS litros
    FROM cortes ct
    CROSS JOIN tanques tn
  ),
  agg AS (
    SELECT
      l.id_empresa,
      l.id_filial,
      l.ano_mes,
      SUM(COALESCE(l.litros, 0) * COALESCE(NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'), 0), 0))
        ::numeric(18,2) AS ativo_estoque_combustivel,
      COUNT(*) FILTER (WHERE l.litros IS NOT NULL) AS tanques_com_leitura
    FROM leituras l
    LEFT JOIN stg.produtos p
      ON p.id_empresa = l.id_empresa AND p.id_filial = l.id_filial AND p.id_produto = l.id_produto
    GROUP BY l.id_empresa, l.id_filial, l.ano_mes
  ),
  upserted AS (
    INSERT INTO mart.liquidez_solvencia AS m
      (id_empresa, id_filial, ano_mes, ativo_estoque_combustivel, ativo_estoque, tem_ativo_dados, updated_at)
    SELECT
      a.id_empresa, a.id_filial, a.ano_mes,
      a.ativo_estoque_combustivel,
      a.ativo_estoque_combustivel + COALESCE(m0.ativo_estoque_loja, 0),
      true, now()
    FROM agg a
    LEFT JOIN mart.liquidez_solvencia m0
      ON m0.id_empresa = a.id_empresa AND m0.id_filial = a.id_filial AND m0.ano_mes = a.ano_mes
    WHERE a.tanques_com_leitura > 0
    ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
      ativo_estoque_combustivel = EXCLUDED.ativo_estoque_combustivel,
      ativo_estoque = EXCLUDED.ativo_estoque_combustivel
                      + COALESCE(m.ativo_estoque_loja, 0),
      tem_ativo_dados = true,
      updated_at = now()
    RETURNING m.id_empresa
  ),
  cleared AS (
    UPDATE mart.liquidez_solvencia m
    SET ativo_estoque_combustivel = 0,
        ativo_estoque = COALESCE(m.ativo_estoque_loja, 0),
        updated_at = now()
    FROM agg a
    WHERE a.tanques_com_leitura = 0
      AND m.id_empresa = a.id_empresa
      AND m.id_filial = a.id_filial
      AND m.ano_mes = a.ano_mes
      AND m.ativo_estoque_combustivel <> 0
    RETURNING m.id_empresa
  )
  SELECT COUNT(*) INTO v_rows FROM (
    SELECT 1 FROM upserted
    UNION ALL
    SELECT 1 FROM cleared
  ) x;

  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_combustivel_asof(integer, integer, integer) IS
  'Solvência: combustível as-of via última leitura stg.movtanques antes do corte (join tipado id_tanque). Sem leitura no período, não sobrescreve.';

-- Snapshot AUTO: tipado + A Prazo + Havel via CREDITO
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

  -- 3.2b HAVEL: crédito antecipado de pista (stg.credito.SALDO > 0 = saldo
  -- disponível do cliente para abastecer). Fonte = dbo.CREDITO, NÃO saldoclientes.
  -- Na planilha entra como linha negativa sob Cartões; valor gravado negativo.
  INSERT INTO mart.solvencia_item
    (id_empresa, id_filial, grupo, secao, item_label, valor, qtd, origem, ordem, updated_at)
  SELECT
    c.id_empresa, c.id_filial, 'ativo_circulante', 'havel', 'Havel clientes',
    (-SUM(GREATEST(etl.safe_numeric(c.payload->>'SALDO'), 0)))::numeric(18,2),
    COUNT(*) FILTER (WHERE etl.safe_numeric(c.payload->>'SALDO') > 0)::numeric,
    'auto', 41, now()
  FROM stg.credito c
  WHERE c.id_empresa = p_id_empresa
  GROUP BY c.id_empresa, c.id_filial
  HAVING SUM(GREATEST(etl.safe_numeric(c.payload->>'SALDO'), 0)) > 0;

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
      CASE
        WHEN EXISTS (
          SELECT 1 FROM stg.tanques t
          WHERE t.id_filial = pr.id_filial
            AND etl.safe_int(t.payload->>'ID_PRODUTOS')::text = pr.id_produto
        ) OR COALESCE(gr.nome_grupo, '') LIKE '%COMBUST%' THEN 'combustivel'
           WHEN COALESCE(gr.nome_grupo, '') LIKE '%IMOBIL%'
             OR COALESCE(gr.nome_grupo, '') LIKE '%INVEST%' THEN 'imobilizado'
           ELSE 'loja' END AS classe
    FROM prod pr LEFT JOIN grp gr ON gr.id_filial = pr.id_filial AND gr.id_grupo = pr.id_grupo
  ),
  tanq AS (
    -- Join tipado: stg.tanques.id_tanque = movtanques.payload.ID_TANQUES
    -- (payload do tanque NAO carrega ID_TANQUES).
    SELECT t.id_filial, etl.safe_int(t.payload->>'ID_PRODUTOS')::text AS id_produto,
           SUM(mt.qtde)::numeric AS litros
    FROM stg.tanques t
    JOIN LATERAL (
      SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'), 0) AS qtde
      FROM stg.movtanques m
      WHERE m.id_empresa = t.id_empresa AND m.id_filial = t.id_filial
        AND etl.safe_int(m.payload->>'ID_TANQUES') = t.id_tanque
      ORDER BY COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DTACONTA')) DESC NULLS LAST
      LIMIT 1
    ) mt ON true
    WHERE t.id_empresa = p_id_empresa
    GROUP BY t.id_filial, etl.safe_int(t.payload->>'ID_PRODUTOS')
  ),
  est AS (
    -- Colunas tipadas (quantidade/id_produto); fallback payload legado.
    SELECT e.id_filial,
           COALESCE(e.id_produto, etl.safe_int(e.payload->>'ID_PRODUTOS'))::text AS id_produto,
           SUM(COALESCE(e.quantidade, etl.safe_numeric(e.payload->>'QTDEATUAL'), 0))::numeric AS qtde
    FROM stg.estoque e WHERE e.id_empresa = p_id_empresa
    GROUP BY e.id_filial, COALESCE(e.id_produto, etl.safe_int(e.payload->>'ID_PRODUTOS'))
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
