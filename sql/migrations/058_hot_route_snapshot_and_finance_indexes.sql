-- @nontransactional

CREATE TABLE IF NOT EXISTS app.snapshot_cache (
    snapshot_key text NOT NULL,
    id_empresa integer NOT NULL,
    id_filial integer NULL,
    scope_signature text NOT NULL,
    scope_context jsonb NOT NULL,
    snapshot_data jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_snapshot_cache_scope
    ON app.snapshot_cache (snapshot_key, id_empresa, scope_signature);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_cache_branch_updated
    ON app.snapshot_cache (snapshot_key, id_empresa, id_filial, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_financeiro_receber_aging_hotpath
    ON dw.fact_financeiro (
        id_empresa,
        id_filial,
        tipo_titulo,
        COALESCE(vencimento, data_emissao),
        id_entidade
    )
    INCLUDE (valor, valor_pago, data_pagamento)
    WHERE tipo_titulo = 1;
