-- 074_stg_nfe.sql
-- NFE (Nota Fiscal Eletrônica) staging table for fiscal classification.
-- Stores raw NFE records from the client SQL Server via Agent ingest.
-- Used to classify comprovantes as: authorized (3), cancelled (4), or voided/inutilized (5).

BEGIN;

-- Create stg.nfe table if not exists (idempotent)
CREATE TABLE IF NOT EXISTS stg.nfe (
    id_empresa          int         NOT NULL,
    id_filial           int         NOT NULL,
    id_db               int         NOT NULL,
    id_comprovante      int         NOT NULL,
    id_nfe              int         NOT NULL,

    -- Shadow columns (pre-extracted for fast queries)
    status_shadow       smallint,                 -- 3=authorized, 4=cancelled, 5=voided/inutilized
    numero_nfe_shadow   varchar(64),
    serie_shadow        varchar(16),
    chave_nfe_shadow    varchar(64),
    modelo_shadow       varchar(4),               -- 55=NF-e, 65=NFC-e
    protocolo_shadow    varchar(64),
    data_emissao_shadow timestamptz,
    data_autorizacao_shadow   timestamptz,
    data_cancelamento_shadow  timestamptz,
    data_inutilizacao_shadow  timestamptz,
    valor_nfe_shadow    numeric(18,2),

    -- Standard ingest columns
    id_db_shadow        int,
    id_chave_natural    text,
    dt_evento           timestamptz,
    payload             jsonb       NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT stg_nfe_pk PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante, id_nfe)
);

-- Indexes for common access patterns
CREATE INDEX IF NOT EXISTS idx_stg_nfe_emp_ingested
    ON stg.nfe (id_empresa, ingested_at);

CREATE INDEX IF NOT EXISTS idx_stg_nfe_emp_filial_db_comprovante
    ON stg.nfe (id_empresa, id_filial, id_db, id_comprovante);

CREATE INDEX IF NOT EXISTS idx_stg_nfe_status
    ON stg.nfe (id_empresa, status_shadow);

CREATE INDEX IF NOT EXISTS idx_stg_nfe_dt_evento
    ON stg.nfe (id_empresa, dt_evento);

-- Enable RLS if the policy pattern exists (matches other STG tables)
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'comprovantes' AND schemaname = 'stg'
    ) THEN
        ALTER TABLE stg.nfe ENABLE ROW LEVEL SECURITY;
        -- Policy: tenant can only see their own data
        IF NOT EXISTS (
            SELECT 1 FROM pg_policies WHERE tablename = 'nfe' AND schemaname = 'stg' AND policyname = 'tenant_isolation_nfe'
        ) THEN
            EXECUTE 'CREATE POLICY tenant_isolation_nfe ON stg.nfe USING (id_empresa = current_setting(''app.current_tenant'')::int)';
        END IF;
    END IF;
END $$;

COMMENT ON TABLE stg.nfe IS 'Raw NFE (Nota Fiscal Eletrônica) records from client SQL Server. status_shadow: 3=authorized, 4=cancelled_real, 5=voided/inutilized.';

COMMIT;
