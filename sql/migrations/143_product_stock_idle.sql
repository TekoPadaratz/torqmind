-- 143: Gestão de Produtos — estoque parado + últimas compras (mart PG → publish CH)
-- Idempotente. Homolog first.

BEGIN;

CREATE TABLE IF NOT EXISTS mart.product_stock_idle (
  id_empresa          integer        NOT NULL,
  id_filial           integer        NOT NULL,
  id_produto          integer        NOT NULL,
  nome_produto        text           NOT NULL DEFAULT '',
  setor_gerencial     text           NOT NULL DEFAULT 'outros',
  qtd_estoque         numeric(18, 3) NOT NULL DEFAULT 0,
  last_sale_date      date,
  dias_sem_venda      integer        NOT NULL DEFAULT 0,
  custo_medio_compra  numeric(18, 4) NOT NULL DEFAULT 0,
  preco_venda         numeric(18, 4) NOT NULL DEFAULT 0,
  updated_at          timestamptz    NOT NULL DEFAULT now(),
  CONSTRAINT pk_mart_product_stock_idle
    PRIMARY KEY (id_empresa, id_filial, id_produto)
);

CREATE INDEX IF NOT EXISTS ix_mart_product_stock_idle_filial
  ON mart.product_stock_idle (id_empresa, id_filial, dias_sem_venda DESC);

COMMENT ON TABLE mart.product_stock_idle IS
  'Produtos com estoque > 0 e dias sem venda; mash para torqmind_mart_rt.product_stock_idle.';

CREATE TABLE IF NOT EXISTS mart.product_purchase_recent (
  id_empresa        integer        NOT NULL,
  id_filial         integer        NOT NULL,
  id_produto        integer        NOT NULL,
  rank              smallint       NOT NULL,
  numero_documento  text           NOT NULL DEFAULT '',
  data_compra       date           NOT NULL,
  qtd               numeric(18, 3) NOT NULL DEFAULT 0,
  valor_unitario    numeric(18, 4) NOT NULL DEFAULT 0,
  valor_total       numeric(18, 2) NOT NULL DEFAULT 0,
  updated_at        timestamptz    NOT NULL DEFAULT now(),
  CONSTRAINT pk_mart_product_purchase_recent
    PRIMARY KEY (id_empresa, id_filial, id_produto, rank),
  CONSTRAINT ck_mart_product_purchase_recent_rank CHECK (rank BETWEEN 1 AND 3)
);

CREATE INDEX IF NOT EXISTS ix_mart_product_purchase_recent_prod
  ON mart.product_purchase_recent (id_empresa, id_filial, id_produto);

COMMENT ON TABLE mart.product_purchase_recent IS
  'Últimas 3 notas de compra por produto (entrada Xpert / nfe_entrada).';

CREATE OR REPLACE FUNCTION etl.map_setor_gerencial(p_id_grupo integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    CASE
      WHEN p_id_grupo = 1 THEN 'combustivel'
      WHEN p_id_grupo IN (2, 4, 7, 8, 9, 16, 39) THEN 'automotivo'
      WHEN p_id_grupo = 10 THEN 'cigarro'
      WHEN p_id_grupo IN (5, 35) THEN 'servico'
      WHEN p_id_grupo IN (6, 28, 32, 38, 42) THEN 'interno'
      WHEN p_id_grupo IN (14, 15, 18, 12, 13, 11, 17, 21, 37, 40, 41, 19, 20) THEN 'conveniencia'
      ELSE 'outros'
    END,
    'outros'
  );
$$;

CREATE OR REPLACE FUNCTION etl.refresh_product_stock_idle(p_id_empresa integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_today date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_product_stock_idle'), p_id_empresa);

  DELETE FROM mart.product_purchase_recent WHERE id_empresa = p_id_empresa;
  DELETE FROM mart.product_stock_idle WHERE id_empresa = p_id_empresa;

  WITH stock AS (
    SELECT
      e.id_empresa,
      e.id_filial,
      COALESCE(
        e.id_produto,
        e.id_produto_shadow,
        etl.safe_int(e.payload->>'ID_PRODUTOS')
      ) AS id_produto,
      GREATEST(
        COALESCE(
          e.qtd_atual_shadow,
          etl.safe_numeric(e.payload->>'QTDEATUAL'),
          e.quantidade,
          0
        ),
        0
      )::numeric(18, 3) AS qtd_estoque
    FROM stg.estoque e
    WHERE e.id_empresa = p_id_empresa
  ),
  stock_pos AS (
    SELECT s.*
    FROM stock s
    WHERE s.id_produto > 0
      AND s.qtd_estoque > 0
      AND NOT EXISTS (
        SELECT 1
        FROM stg.tanques t
        WHERE t.id_empresa = s.id_empresa
          AND t.id_filial = s.id_filial
          AND etl.safe_int(t.payload->>'ID_PRODUTOS') = s.id_produto
      )
  ),
  last_sale AS (
    SELECT
      fvi.id_empresa,
      fvi.id_filial,
      fvi.id_produto,
      MAX(to_date(fvi.data_key::text, 'YYYYMMDD')) AS last_sale_date
    FROM dw.fact_venda_item fvi
    WHERE fvi.id_empresa = p_id_empresa
      AND fvi.data_key >= 20190101
      AND fvi.data_key < 20310101
      AND COALESCE(fvi.qtd, 0) > 0
      AND COALESCE(fvi.total, 0) > 0
    GROUP BY fvi.id_empresa, fvi.id_filial, fvi.id_produto
  ),
  prod AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_produto,
      COALESCE(
        NULLIF(trim(both FROM p.payload->>'NOME'), ''),
        NULLIF(trim(both FROM p.payload->>'DESCRICAO'), ''),
        'Produto ' || p.id_produto::text
      ) AS nome_produto,
      etl.map_setor_gerencial(
        COALESCE(
          etl.safe_int(p.payload->>'ID_GRUPOPRODUTOS'),
          etl.safe_int(p.payload->>'ID_GRUPO_PRODUTO'),
          0
        )
      ) AS setor_gerencial,
      COALESCE(
        etl.safe_numeric(p.payload->>'PRECO'),
        etl.safe_numeric(p.payload->>'PRECOVENDA'),
        etl.safe_numeric(p.payload->>'PPL'),
        0
      )::numeric(18, 4) AS preco_venda
    FROM stg.produtos p
    WHERE p.id_empresa = p_id_empresa
  ),
  purchase_lines AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      COALESCE(
        NULLIF(trim(both FROM n.numero_nota_shadow), ''),
        NULLIF(trim(both FROM n.payload->>'NUMERO'), ''),
        NULLIF(trim(both FROM n.payload->>'NROCOMPROVANTE'), ''),
        '—'
      ) AS numero_documento,
      COALESCE(
        n.dt_entrada_shadow::date,
        n.dt_evento::date,
        etl.safe_timestamp(n.payload->>'DATAENTRADA')::date,
        etl.safe_timestamp(n.payload->>'DATA')::date
      ) AS data_compra,
      GREATEST(
        COALESCE(
          i.qtd_shadow,
          etl.safe_numeric(i.payload->>'QUANTIDADE'),
          etl.safe_numeric(i.payload->>'QTDE'),
          0
        ),
        0
      )::numeric(18, 3) AS qtd,
      GREATEST(
        COALESCE(
          i.custo_unitario_shadow,
          etl.safe_numeric(i.payload->>'VLRCUSTO'),
          etl.safe_numeric(i.payload->>'VLRUNITARIO'),
          etl.safe_numeric(i.payload->>'CUSTO_UNITARIO'),
          0
        ),
        0
      )::numeric(18, 4) AS valor_unitario,
      GREATEST(
        COALESCE(
          i.custo_total_shadow,
          etl.safe_numeric(i.payload->>'VLRTOTALITEM'),
          etl.safe_numeric(i.payload->>'VALORTOTAL'),
          0
        ),
        0
      )::numeric(18, 2) AS valor_total,
      COALESCE(
        n.dt_entrada_shadow,
        n.dt_evento,
        etl.safe_timestamp(n.payload->>'DATAENTRADA'),
        etl.safe_timestamp(n.payload->>'DATA')
      ) AS sort_ts
    FROM stg.itens_nfe_entrada i
    INNER JOIN stg.nfe_entrada n
      ON n.id_empresa = i.id_empresa
     AND n.id_filial = i.id_filial
     AND n.id_db = i.id_db
     AND n.id_nota = i.id_nota
    WHERE i.id_empresa = p_id_empresa
      AND COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) > 0
  ),
  purchase_ranked AS (
    SELECT
      pl.*,
      ROW_NUMBER() OVER (
        PARTITION BY pl.id_empresa, pl.id_filial, pl.id_produto
        ORDER BY pl.sort_ts DESC NULLS LAST, pl.data_compra DESC, pl.numero_documento DESC
      ) AS rn
    FROM purchase_lines pl
    WHERE pl.data_compra IS NOT NULL
      AND pl.qtd > 0
  ),
  purchase_top3 AS (
    SELECT * FROM purchase_ranked WHERE rn <= 3
  ),
  custo_avg AS (
    SELECT
      id_empresa,
      id_filial,
      id_produto,
      COALESCE(
        AVG(valor_unitario) FILTER (WHERE valor_unitario > 0),
        0
      )::numeric(18, 4) AS custo_medio_compra
    FROM purchase_top3
    GROUP BY id_empresa, id_filial, id_produto
  ),
  merged AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_produto,
      COALESCE(pr.nome_produto, 'Produto ' || s.id_produto::text) AS nome_produto,
      COALESCE(pr.setor_gerencial, 'outros') AS setor_gerencial,
      s.qtd_estoque,
      ls.last_sale_date,
      CASE
        WHEN ls.last_sale_date IS NULL THEN 9999
        ELSE GREATEST((v_today - ls.last_sale_date), 0)
      END AS dias_sem_venda,
      COALESCE(ca.custo_medio_compra, 0) AS custo_medio_compra,
      COALESCE(pr.preco_venda, 0) AS preco_venda
    FROM stock_pos s
    LEFT JOIN prod pr
      ON pr.id_empresa = s.id_empresa
     AND pr.id_filial = s.id_filial
     AND pr.id_produto = s.id_produto
    LEFT JOIN last_sale ls
      ON ls.id_empresa = s.id_empresa
     AND ls.id_filial = s.id_filial
     AND ls.id_produto = s.id_produto
    LEFT JOIN custo_avg ca
      ON ca.id_empresa = s.id_empresa
     AND ca.id_filial = s.id_filial
     AND ca.id_produto = s.id_produto
    WHERE COALESCE(pr.setor_gerencial, 'outros') <> 'interno'
  )
  INSERT INTO mart.product_stock_idle (
    id_empresa, id_filial, id_produto, nome_produto, setor_gerencial,
    qtd_estoque, last_sale_date, dias_sem_venda, custo_medio_compra, preco_venda, updated_at
  )
  SELECT
    id_empresa, id_filial, id_produto, nome_produto, setor_gerencial,
    qtd_estoque, last_sale_date, dias_sem_venda, custo_medio_compra, preco_venda, now()
  FROM merged;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO mart.product_purchase_recent (
    id_empresa, id_filial, id_produto, rank,
    numero_documento, data_compra, qtd, valor_unitario, valor_total, updated_at
  )
  SELECT
    id_empresa,
    id_filial,
    id_produto,
    rn::smallint,
    numero_documento,
    data_compra,
    qtd,
    valor_unitario,
    CASE
      WHEN valor_total > 0 THEN valor_total
      WHEN valor_unitario > 0 AND qtd > 0 THEN ROUND(valor_unitario * qtd, 2)
      ELSE 0
    END,
    now()
  FROM purchase_top3;

  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_product_stock_idle(integer) IS
  'Refresh mash Gestão de Produtos: estoque atual (stg.estoque) + última venda (DW) + compras (nfe_entrada).';

COMMIT;
