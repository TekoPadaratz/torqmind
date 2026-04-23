BEGIN;

CREATE OR REPLACE FUNCTION etl.resolve_item_total(
  p_total_shadow numeric,
  p_payload jsonb
)
RETURNS numeric
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    p_total_shadow,
    etl.safe_numeric(p_payload->>'VLRTOTALITEM')::numeric(18,2),
    etl.safe_numeric(p_payload->>'TOTAL')::numeric(18,2),
    etl.safe_numeric(p_payload->>'VLRTOTAL')::numeric(18,2)
  )
$$;

CREATE OR REPLACE FUNCTION etl.repair_fact_venda_item_from_stg(
  p_id_empresa integer,
  p_id_filial integer DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_stg_updated integer := 0;
  v_dw_updated integer := 0;
BEGIN
  WITH patched_stg AS (
    UPDATE stg.itenscomprovantes i
       SET total_shadow = etl.resolve_item_total(i.total_shadow, i.payload)
     WHERE i.id_empresa = p_id_empresa
       AND (p_id_filial IS NULL OR i.id_filial = p_id_filial)
       AND etl.resolve_item_total(i.total_shadow, i.payload) IS NOT NULL
       AND i.total_shadow IS DISTINCT FROM etl.resolve_item_total(i.total_shadow, i.payload)
    RETURNING 1
  )
  SELECT COUNT(*)::int
    INTO v_stg_updated
  FROM patched_stg;

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_comprovante,
      i.id_itemcomprovante,
      COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)) AS qtd,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS valor_unitario,
      etl.resolve_item_total(i.total_shadow, i.payload)::numeric(18,2) AS total,
      COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2)) AS desconto,
      COALESCE(
        (
          etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6)
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2),
        (
          dp.custo_medio
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2)
      ) AS custo_total
    FROM stg.itenscomprovantes i
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
    WHERE i.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR i.id_filial = p_id_filial)
  ),
  repaired AS (
    UPDATE dw.fact_venda_item f
       SET qtd = s.qtd,
           valor_unitario = s.valor_unitario,
           total = s.total,
           desconto = s.desconto,
           custo_total = s.custo_total,
           margem = (COALESCE(s.total, 0) - COALESCE(s.custo_total, 0))::numeric(18,2),
           preco_praticado_unitario = s.valor_unitario,
           desconto_unitario = CASE
             WHEN COALESCE(s.qtd, 0) = 0 THEN NULL
             ELSE (COALESCE(s.desconto, 0) / NULLIF(s.qtd, 0))::numeric(18,4)
           END,
           desconto_total = s.desconto,
           discount_source = CASE
             WHEN COALESCE(s.desconto, 0) > 0 THEN 'payload_explicit_discount'
             ELSE NULL
           END,
           updated_at = now()
      FROM src s
     WHERE f.id_empresa = s.id_empresa
       AND f.id_filial = s.id_filial
       AND f.id_db = s.id_db
       AND f.id_comprovante = s.id_comprovante
       AND f.id_itemcomprovante = s.id_itemcomprovante
       AND (
         f.qtd IS DISTINCT FROM s.qtd
         OR f.valor_unitario IS DISTINCT FROM s.valor_unitario
         OR f.total IS DISTINCT FROM s.total
         OR f.desconto IS DISTINCT FROM s.desconto
         OR f.custo_total IS DISTINCT FROM s.custo_total
         OR f.margem IS DISTINCT FROM (COALESCE(s.total, 0) - COALESCE(s.custo_total, 0))::numeric(18,2)
         OR f.preco_praticado_unitario IS DISTINCT FROM s.valor_unitario
         OR f.desconto_total IS DISTINCT FROM s.desconto
       )
    RETURNING 1
  )
  SELECT COUNT(*)::int
    INTO v_dw_updated
  FROM repaired;

  RETURN jsonb_build_object(
    'id_empresa', p_id_empresa,
    'id_filial', p_id_filial,
    'stg_total_shadow_updated', COALESCE(v_stg_updated, 0),
    'dw_fact_rows_repaired', COALESCE(v_dw_updated, 0)
  );
END;
$$;

COMMIT;
