-- @nontransactional

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_itenscomprovantes_emp_comp_range_cover
  ON stg.itenscomprovantes (
    id_empresa,
    id_comprovante,
    id_filial,
    id_db,
    id_itemcomprovante
  )
  INCLUDE (received_at, dt_evento);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_stg_formas_pgto_comp_emp_ref_range_cover
  ON stg.formas_pgto_comprovantes (
    id_empresa,
    id_referencia,
    id_filial,
    tipo_forma
  )
  INCLUDE (
    received_at,
    dt_evento,
    id_db_shadow,
    valor_shadow,
    nsu_shadow,
    autorizacao_shadow,
    bandeira_shadow,
    rede_shadow,
    tef_shadow
  );

REINDEX TABLE CONCURRENTLY stg.itenscomprovantes;
REINDEX TABLE CONCURRENTLY dw.fact_venda_item;
REINDEX TABLE CONCURRENTLY dw.fact_pagamento_comprovante;

ANALYZE stg.itenscomprovantes;
ANALYZE stg.formas_pgto_comprovantes;
ANALYZE dw.fact_venda_item;
ANALYZE dw.fact_pagamento_comprovante;
