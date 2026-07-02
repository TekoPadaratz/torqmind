-- 088_stg_troca_forma_pgto.sql
-- Antifraude: troca de forma de pagamento (Xpert SQL Server).
--
-- Estrutura de ingestao para reconstruir a trilha DE -> PARA de trocas de forma
-- de pagamento (sinal forte de fraude quando uma forma JA recebida vira A receber).
--
-- Tabelas:
--   stg.controle_troca_pgto  -> auditoria da troca (quem/quando). 1 linha por troca.
--   stg.movlctoscancelados   -> lancamento financeiro CANCELADO (forma DE da troca).
--   app.payment_account_category -> de-para opcional ID_PLANODECONTAS -> familia
--                                   (RECEBIDA / A_RECEBER) para override manual.
--
-- Origem confirmada (ATXDADOS):
--   CONTROLE_TROCA_PGTO PK=[ID, ID_FILIAL, ID_DB]
--   MOVLCTOSCANCELADOS  PK=[ID_MOVLCTOSCANCELADOS, ID_FILIAL, ID_DB]
--
-- Idempotente e seguro (CREATE TABLE IF NOT EXISTS, sem DROP/TRUNCATE).

BEGIN;

-- ============================================================
-- stg.controle_troca_pgto  (auditoria: quem trocou e quando)
-- ============================================================
CREATE TABLE IF NOT EXISTS stg.controle_troca_pgto (
    id_empresa                  int         NOT NULL,
    id_filial                   int         NOT NULL,
    id_db                       int         NOT NULL,
    id                          int         NOT NULL,

    -- Shadow columns (pre-extraidas para consultas rapidas)
    id_movlctoscancelados_shadow int,                   -- FK p/ MOVLCTOSCANCELADOS (forma DE)
    id_usuario_shadow            int,                   -- quem efetuou a troca
    data_troca_shadow            timestamptz,           -- quando a troca ocorreu (DATA)

    -- Standard ingest columns
    id_db_shadow        int,
    id_chave_natural    text,
    dt_evento           timestamptz,
    payload             jsonb       NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    received_at         timestamptz DEFAULT now(),

    CONSTRAINT stg_controle_troca_pgto_pk PRIMARY KEY (id_empresa, id_filial, id_db, id)
);

CREATE INDEX IF NOT EXISTS idx_stg_controle_troca_pgto_emp_ingested
    ON stg.controle_troca_pgto (id_empresa, ingested_at);

CREATE INDEX IF NOT EXISTS idx_stg_controle_troca_pgto_movlcto
    ON stg.controle_troca_pgto (id_empresa, id_filial, id_db, id_movlctoscancelados_shadow);

CREATE INDEX IF NOT EXISTS idx_stg_controle_troca_pgto_usuario
    ON stg.controle_troca_pgto (id_empresa, id_filial, id_usuario_shadow);

CREATE INDEX IF NOT EXISTS idx_stg_controle_troca_pgto_dt_evento
    ON stg.controle_troca_pgto (id_empresa, dt_evento);

-- ============================================================
-- stg.movlctoscancelados  (lancamento CANCELADO = forma DE)
-- ============================================================
CREATE TABLE IF NOT EXISTS stg.movlctoscancelados (
    id_empresa                  int         NOT NULL,
    id_filial                   int         NOT NULL,
    id_db                       int         NOT NULL,
    id_movlctoscancelados       int         NOT NULL,

    -- Shadow columns
    id_planodecontas_shadow     int,                    -- conta/forma DE (nome em PLANODECONTAS)
    referencia_shadow           int,                    -- liga a venda/comprovante (REFERENCIA)
    tipo_shadow                 smallint,               -- lado contabil (0/1 debito/credito)
    valor_shadow                numeric(18,2),
    dtaconta_shadow             timestamptz,            -- data do lancamento (DTACONTA)
    id_turno_shadow             int,
    ref_operacao_shadow         int,
    documento_shadow            text,

    -- Standard ingest columns
    id_db_shadow        int,
    id_chave_natural    text,
    dt_evento           timestamptz,
    payload             jsonb       NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    received_at         timestamptz DEFAULT now(),

    CONSTRAINT stg_movlctoscancelados_pk PRIMARY KEY (id_empresa, id_filial, id_db, id_movlctoscancelados)
);

CREATE INDEX IF NOT EXISTS idx_stg_movlctoscancelados_emp_ingested
    ON stg.movlctoscancelados (id_empresa, ingested_at);

CREATE INDEX IF NOT EXISTS idx_stg_movlctoscancelados_referencia
    ON stg.movlctoscancelados (id_empresa, id_filial, referencia_shadow);

CREATE INDEX IF NOT EXISTS idx_stg_movlctoscancelados_plano
    ON stg.movlctoscancelados (id_empresa, id_filial, id_planodecontas_shadow);

CREATE INDEX IF NOT EXISTS idx_stg_movlctoscancelados_turno
    ON stg.movlctoscancelados (id_empresa, id_filial, id_turno_shadow);

CREATE INDEX IF NOT EXISTS idx_stg_movlctoscancelados_dt_evento
    ON stg.movlctoscancelados (id_empresa, dt_evento);

-- ============================================================
-- app.payment_account_category  (de-para opcional: conta -> familia)
-- Override manual do classificador automatico de familia. LEFT JOIN
-- no mart; quando presente, vence a heuristica por nome.
--   categoria: 'RECEBIDA' (caixa imediato) | 'A_RECEBER' (risco)
-- ============================================================
CREATE TABLE IF NOT EXISTS app.payment_account_category (
    id_empresa          int          NOT NULL,
    id_filial           int          NOT NULL,
    id_planodecontas    int          NOT NULL,
    categoria           varchar(16)  NOT NULL,          -- RECEBIDA | A_RECEBER
    forma_label         varchar(120) NOT NULL DEFAULT '',
    updated_at          timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT pk_payment_account_category PRIMARY KEY (id_empresa, id_filial, id_planodecontas),
    CONSTRAINT ck_payment_account_category_cat CHECK (categoria IN ('RECEBIDA', 'A_RECEBER'))
);

-- ============================================================
-- RLS (segue o mesmo padrao das demais tabelas STG)
-- ============================================================
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'comprovantes' AND schemaname = 'stg'
    ) THEN
        ALTER TABLE stg.controle_troca_pgto ENABLE ROW LEVEL SECURITY;
        IF NOT EXISTS (
            SELECT 1 FROM pg_policies WHERE tablename = 'controle_troca_pgto' AND schemaname = 'stg' AND policyname = 'tenant_isolation_controle_troca_pgto'
        ) THEN
            EXECUTE 'CREATE POLICY tenant_isolation_controle_troca_pgto ON stg.controle_troca_pgto USING (id_empresa = current_setting(''app.current_tenant'')::int)';
        END IF;

        ALTER TABLE stg.movlctoscancelados ENABLE ROW LEVEL SECURITY;
        IF NOT EXISTS (
            SELECT 1 FROM pg_policies WHERE tablename = 'movlctoscancelados' AND schemaname = 'stg' AND policyname = 'tenant_isolation_movlctoscancelados'
        ) THEN
            EXECUTE 'CREATE POLICY tenant_isolation_movlctoscancelados ON stg.movlctoscancelados USING (id_empresa = current_setting(''app.current_tenant'')::int)';
        END IF;
    END IF;
END $$;

COMMENT ON TABLE stg.controle_troca_pgto IS 'Antifraude: auditoria de troca de forma de pagamento (quem/quando). 1 linha por troca. Origem: dbo.CONTROLE_TROCA_PGTO.';
COMMENT ON TABLE stg.movlctoscancelados IS 'Antifraude: lancamento financeiro CANCELADO (forma DE da troca). Origem: dbo.MOVLCTOSCANCELADOS. TIPO=lado contabil (deb/cred), nao tipo_forma.';
COMMENT ON TABLE app.payment_account_category IS 'De-para opcional ID_PLANODECONTAS -> familia (RECEBIDA/A_RECEBER) para override do classificador de antifraude.';

COMMIT;
