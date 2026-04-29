-- Enable ClickHouse MaterializedPostgreSQL CDC for DW tables.
-- Fresh resets already create primary keys for these tables. This migration
-- repairs older homolog/dev databases that lost constraints before enabling the
-- ClickHouse replica, and fails loudly if duplicates make a key unsafe.

DO $$
DECLARE
  spec record;
  table_oid oid;
  cols_sql text;
  has_pk boolean;
  has_duplicates boolean;
BEGIN
  FOR spec IN
    SELECT *
    FROM (
      VALUES
        ('dw.dim_cliente', ARRAY['id_empresa', 'id_filial', 'id_cliente'], 'pk_dw_dim_cliente_ch'),
        ('dw.dim_filial', ARRAY['id_empresa', 'id_filial'], 'pk_dw_dim_filial_ch'),
        ('dw.dim_funcionario', ARRAY['id_empresa', 'id_filial', 'id_funcionario'], 'pk_dw_dim_funcionario_ch'),
        ('dw.dim_grupo_produto', ARRAY['id_empresa', 'id_filial', 'id_grupo_produto'], 'pk_dw_dim_grupo_produto_ch'),
        ('dw.dim_local_venda', ARRAY['id_empresa', 'id_filial', 'id_local_venda'], 'pk_dw_dim_local_venda_ch'),
        ('dw.dim_produto', ARRAY['id_empresa', 'id_filial', 'id_produto'], 'pk_dw_dim_produto_ch'),
        ('dw.dim_usuario_caixa', ARRAY['id_empresa', 'id_filial', 'id_usuario'], 'pk_dw_dim_usuario_caixa_ch'),
        ('dw.fact_caixa_turno', ARRAY['id_empresa', 'id_filial', 'id_turno'], 'pk_dw_fact_caixa_turno_ch'),
        ('dw.fact_comprovante', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_comprovante'], 'pk_dw_fact_comprovante_ch'),
        ('dw.fact_financeiro', ARRAY['id_empresa', 'id_filial', 'id_db', 'tipo_titulo', 'id_titulo'], 'pk_dw_fact_financeiro_ch'),
        ('dw.fact_pagamento_comprovante', ARRAY['id_empresa', 'id_filial', 'referencia', 'tipo_forma'], 'pk_dw_fact_pagamento_comprovante_ch'),
        ('dw.fact_risco_evento', ARRAY['id'], 'pk_dw_fact_risco_evento_ch'),
        ('dw.fact_venda', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_movprodutos'], 'pk_dw_fact_venda_ch'),
        ('dw.fact_venda_item', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_movprodutos', 'id_itensmovprodutos'], 'pk_dw_fact_venda_item_ch')
    ) AS s(table_name, columns, constraint_name)
  LOOP
    table_oid := to_regclass(spec.table_name);
    IF table_oid IS NULL THEN
      CONTINUE;
    END IF;

    SELECT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = table_oid
        AND contype = 'p'
    )
    INTO has_pk;

    IF NOT has_pk THEN
      SELECT string_agg(quote_ident(col), ', ')
      FROM unnest(spec.columns) AS col
      INTO cols_sql;

      EXECUTE format(
        'SELECT EXISTS (
           SELECT 1
           FROM (
             SELECT %1$s
             FROM %2$s
             GROUP BY %1$s
             HAVING COUNT(*) > 1
             LIMIT 1
           ) duplicated_key
         )',
        cols_sql,
        spec.table_name
      )
      INTO has_duplicates;

      IF has_duplicates THEN
        RAISE EXCEPTION 'Cannot add primary key % on %: duplicate key rows exist', spec.constraint_name, spec.table_name;
      END IF;

      EXECUTE format(
        'ALTER TABLE %s ADD CONSTRAINT %I PRIMARY KEY (%s)',
        spec.table_name,
        spec.constraint_name,
        cols_sql
      );
    END IF;

    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY DEFAULT', spec.table_name);
  END LOOP;
END;
$$;
