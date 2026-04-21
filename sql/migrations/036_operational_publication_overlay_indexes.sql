-- @nontransactional

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_live_overlay
  ON dw.fact_venda (id_empresa, id_filial, data_key, id_db, id_movprodutos)
  INCLUDE (id_comprovante, data, updated_at, created_at)
  WHERE COALESCE(cancelado, false) = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_item_live_overlay
  ON dw.fact_venda_item (id_empresa, id_filial, id_db, id_movprodutos)
  INCLUDE (cfop, total, margem, qtd, id_produto, id_grupo_produto, id_funcionario, updated_at, created_at)
  WHERE COALESCE(cfop, 0) >= 5000;
