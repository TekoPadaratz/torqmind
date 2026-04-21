BEGIN;

ALTER TABLE dw.dim_produto
  ADD COLUMN IF NOT EXISTS situacao integer;

CREATE INDEX IF NOT EXISTS ix_dim_produto_active_lookup
  ON dw.dim_produto (id_empresa, id_filial, situacao, id_grupo_produto);

WITH latest_status AS (
  SELECT DISTINCT ON (p.id_empresa, p.id_filial, p.id_produto)
    p.id_empresa,
    p.id_filial,
    p.id_produto,
    COALESCE(
      etl.safe_int(p.payload->>'SITUACAO'),
      etl.safe_int(p.payload->>'situacao'),
      etl.safe_int(p.payload->>'STATUS'),
      etl.safe_int(p.payload->>'status')
    ) AS situacao
  FROM stg.produtos p
  ORDER BY p.id_empresa, p.id_filial, p.id_produto, p.ingested_at DESC
)
UPDATE dw.dim_produto d
SET situacao = latest_status.situacao
FROM latest_status
WHERE d.id_empresa = latest_status.id_empresa
  AND d.id_filial = latest_status.id_filial
  AND d.id_produto = latest_status.id_produto
  AND d.situacao IS DISTINCT FROM latest_status.situacao;

CREATE OR REPLACE FUNCTION etl.load_dim_produtos(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'produtos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_produto,
      COALESCE(p.payload->>'NOMEPRODUTO', p.payload->>'NOME', '') AS nome,
      NULLIF(p.payload->>'UNIDADE', '') AS unidade,
      etl.safe_int(p.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(p.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_numeric(p.payload->>'customedio') AS custo_medio,
      COALESCE(
        etl.safe_int(p.payload->>'SITUACAO'),
        etl.safe_int(p.payload->>'situacao'),
        etl.safe_int(p.payload->>'STATUS'),
        etl.safe_int(p.payload->>'status')
      ) AS situacao
    FROM stg.produtos p
    WHERE p.id_empresa = p_id_empresa
      AND p.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.dim_produto (
      id_empresa,
      id_filial,
      id_produto,
      nome,
      unidade,
      id_grupo_produto,
      id_local_venda,
      custo_medio,
      situacao
    )
    SELECT
      id_empresa,
      id_filial,
      id_produto,
      nome,
      unidade,
      id_grupo_produto,
      id_local_venda,
      custo_medio,
      situacao
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_produto)
    DO UPDATE SET
      nome = EXCLUDED.nome,
      unidade = EXCLUDED.unidade,
      id_grupo_produto = EXCLUDED.id_grupo_produto,
      id_local_venda = EXCLUDED.id_local_venda,
      custo_medio = EXCLUDED.custo_medio,
      situacao = EXCLUDED.situacao
    WHERE
      dw.dim_produto.nome IS DISTINCT FROM EXCLUDED.nome
      OR dw.dim_produto.unidade IS DISTINCT FROM EXCLUDED.unidade
      OR dw.dim_produto.id_grupo_produto IS DISTINCT FROM EXCLUDED.id_grupo_produto
      OR dw.dim_produto.id_local_venda IS DISTINCT FROM EXCLUDED.id_local_venda
      OR dw.dim_produto.custo_medio IS DISTINCT FROM EXCLUDED.custo_medio
      OR dw.dim_produto.situacao IS DISTINCT FROM EXCLUDED.situacao
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.produtos
  WHERE id_empresa = p_id_empresa
    AND ingested_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'produtos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
