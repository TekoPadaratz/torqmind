-- Fix race in etl.load_dim_clientes watermark advancement.
-- The watermark must advance only to the max ingested_at from the same snapshot
-- that fed the upsert; otherwise concurrent inserts can be skipped permanently.

CREATE OR REPLACE FUNCTION etl.load_dim_clientes(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'entidades'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      e.id_empresa,
      e.id_filial,
      e.id_entidade AS id_cliente,
      COALESCE(e.payload->>'NOMEENTIDADE', e.payload->>'NOME', '') AS nome,
      COALESCE(e.payload->>'CNPJCPF', e.payload->>'DOCUMENTO', NULL) AS documento,
      e.ingested_at
    FROM stg.entidades e
    WHERE e.id_empresa = p_id_empresa
      AND e.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome, documento)
    SELECT id_empresa, id_filial, id_cliente, nome, documento
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_cliente)
    DO UPDATE SET
      nome = EXCLUDED.nome,
      documento = EXCLUDED.documento
    WHERE
      dw.dim_cliente.nome IS DISTINCT FROM EXCLUDED.nome
      OR dw.dim_cliente.documento IS DISTINCT FROM EXCLUDED.documento
    RETURNING 1
  )
  SELECT
    COALESCE((SELECT COUNT(*)::int FROM upserted), 0),
    (SELECT MAX(ingested_at) FROM src)
  INTO v_rows, v_max;

  PERFORM etl.set_watermark(p_id_empresa, 'entidades', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;