-- ============================================================
-- TorqMind DW/MART (Enterprise baseline)
-- Staging JSONB (raw payload) + DW facts/dims + MART MVs
-- ============================================================

BEGIN;

-- Schemas
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS auth;

-- ============================================================
-- AUTH (baseline para MASTER/OWNER/MANAGER)
-- ============================================================

CREATE TABLE IF NOT EXISTS auth.users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Um usuário pode ter múltiplos escopos (ex.: dono com várias empresas)
-- Regra:
--  MASTER  => id_empresa NULL, id_filial NULL
--  OWNER   => id_empresa NOT NULL, id_filial NULL
--  MANAGER => id_empresa NOT NULL, id_filial NOT NULL
CREATE TABLE IF NOT EXISTS auth.user_scopes (
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('MASTER','OWNER','MANAGER')),
  id_empresa BIGINT,
  id_filial BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, role, COALESCE(id_empresa,0), COALESCE(id_filial,0)),
  CONSTRAINT ck_user_scopes_scope CHECK (
    (role = 'MASTER'  AND id_empresa IS NULL AND id_filial IS NULL)
 OR (role = 'OWNER'   AND id_empresa IS NOT NULL AND id_filial IS NULL)
 OR (role = 'MANAGER' AND id_empresa IS NOT NULL AND id_filial IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS ix_user_scopes_user ON auth.user_scopes(user_id);
CREATE INDEX IF NOT EXISTS ix_user_scopes_empresa ON auth.user_scopes(id_empresa) WHERE id_empresa IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_user_scopes_filial ON auth.user_scopes(id_empresa, id_filial) WHERE id_filial IS NOT NULL;

-- ============================================================
-- STAGING (payload JSONB)
-- ============================================================

CREATE TABLE IF NOT EXISTS stg.movprodutos (
  id_empresa BIGINT NOT NULL,
  xpert_id_filial BIGINT NOT NULL,
  id_db BIGINT NOT NULL,
  id_movprodutos BIGINT NOT NULL,
  payload JSONB NOT NULL,
  datarepl TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, xpert_id_filial, id_db, id_movprodutos)
);

CREATE TABLE IF NOT EXISTS stg.itensmovprodutos (
  id_empresa BIGINT NOT NULL,
  xpert_id_filial BIGINT NOT NULL,
  id_db BIGINT NOT NULL,
  id_movprodutos BIGINT NOT NULL,
  id_itensmovprodutos BIGINT NOT NULL,
  payload JSONB NOT NULL,
  datarepl TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, xpert_id_filial, id_db, id_movprodutos, id_itensmovprodutos)
);

CREATE TABLE IF NOT EXISTS stg.comprovantes (
  id_empresa BIGINT NOT NULL,
  xpert_id_filial BIGINT NOT NULL,
  id_db BIGINT NOT NULL,
  id_comprovante BIGINT NOT NULL,
  payload JSONB NOT NULL,
  datarepl TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, xpert_id_filial, id_db, id_comprovante)
);

CREATE INDEX IF NOT EXISTS ix_stg_movprodutos_datarepl ON stg.movprodutos (id_empresa, xpert_id_filial, datarepl);
CREATE INDEX IF NOT EXISTS ix_stg_itens_datarepl ON stg.itensmovprodutos (id_empresa, xpert_id_filial, datarepl);
CREATE INDEX IF NOT EXISTS ix_stg_comp_datarepl ON stg.comprovantes (id_empresa, xpert_id_filial, datarepl);

-- ============================================================
-- DW DIMENSIONS (mínimo)
-- ============================================================

-- Dim Filial (você já inseriu filiais manualmente: ótimo)
CREATE TABLE IF NOT EXISTS dw.dim_filial (
  id_empresa BIGINT NOT NULL,
  id_filial  BIGINT NOT NULL,
  nome TEXT,
  cnpj TEXT,
  cidade TEXT,
  uf TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial)
);

-- Dim Data (opcional, mas útil para BI)
CREATE TABLE IF NOT EXISTS dw.dim_data (
  data_key INT PRIMARY KEY,            -- YYYYMMDD
  "date" DATE NOT NULL,
  ano INT NOT NULL,
  mes INT NOT NULL,
  dia INT NOT NULL,
  semana INT NOT NULL,
  trimestre INT NOT NULL
);

-- ============================================================
-- DW FACTS (vendas / itens / comprovante)
-- ============================================================

CREATE TABLE IF NOT EXISTS dw.fact_venda (
  id_empresa BIGINT NOT NULL,
  id_filial  BIGINT NOT NULL,
  id_db BIGINT NOT NULL,
  id_movprodutos BIGINT NOT NULL,
  id_comprovante BIGINT,
  data DATE,
  data_key INT,                         -- YYYYMMDD
  totalvenda NUMERIC(18,2),
  datarepl TIMESTAMPTZ,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos)
);

CREATE INDEX IF NOT EXISTS ix_fact_venda_empresa_filial_data ON dw.fact_venda (id_empresa, id_filial, data_key);
CREATE INDEX IF NOT EXISTS ix_fact_venda_comp ON dw.fact_venda (id_empresa, id_filial, id_db, id_comprovante) WHERE id_comprovante IS NOT NULL;

CREATE TABLE IF NOT EXISTS dw.fact_venda_item (
  id_empresa BIGINT NOT NULL,
  id_filial  BIGINT NOT NULL,
  id_db BIGINT NOT NULL,
  id_movprodutos BIGINT NOT NULL,
  id_itensmovprodutos BIGINT NOT NULL,
  id_produtos BIGINT,
  id_funcionarios BIGINT,
  qtd NUMERIC(18,3),
  total NUMERIC(18,2),
  margem NUMERIC(18,2),
  datarepl TIMESTAMPTZ,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
);

CREATE INDEX IF NOT EXISTS ix_fact_item_empresa_filial_mov ON dw.fact_venda_item (id_empresa, id_filial, id_db, id_movprodutos);
CREATE INDEX IF NOT EXISTS ix_fact_item_produto ON dw.fact_venda_item (id_empresa, id_filial, id_produtos) WHERE id_produtos IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_fact_item_func ON dw.fact_venda_item (id_empresa, id_filial, id_funcionarios) WHERE id_funcionarios IS NOT NULL;

CREATE TABLE IF NOT EXISTS dw.fact_comprovante (
  id_empresa BIGINT NOT NULL,
  id_filial  BIGINT NOT NULL,
  id_db BIGINT NOT NULL,
  id_comprovante BIGINT NOT NULL,
  data DATE,
  data_key INT,
  vlrtotal NUMERIC(18,2),
  situacao TEXT,
  cancelado BOOLEAN,
  formapgto TEXT,
  datarepl TIMESTAMPTZ,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
);

CREATE INDEX IF NOT EXISTS ix_fact_comp_empresa_filial_data ON dw.fact_comprovante (id_empresa, id_filial, data_key);

-- ============================================================
-- MART MATERIALIZED VIEWS
-- ============================================================

-- Agregado diário (base p/ dashboard)
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.agg_vendas_diaria AS
SELECT
  v.id_empresa,
  v.id_filial,
  v.data_key,
  MIN(v.data) AS data,
  SUM(COALESCE(i.total,0)) AS faturamento,
  SUM(COALESCE(i.margem,0)) AS margem,
  COUNT(DISTINCT v.id_movprodutos) AS qtd_vendas,
  CASE WHEN COUNT(DISTINCT v.id_movprodutos) > 0
       THEN SUM(COALESCE(i.total,0)) / COUNT(DISTINCT v.id_movprodutos)
       ELSE 0 END AS ticket_medio,
  SUM(COALESCE(i.qtd,0)) AS quantidade_itens
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa=v.id_empresa
 AND i.id_filial=v.id_filial
 AND i.id_db=v.id_db
 AND i.id_movprodutos=v.id_movprodutos
GROUP BY 1,2,3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_agg_vendas_diaria
ON mart.agg_vendas_diaria (id_empresa, id_filial, data_key);

CREATE INDEX IF NOT EXISTS ix_agg_vendas_diaria_empresa_data
ON mart.agg_vendas_diaria (id_empresa, data_key);

-- Insights base diária (Jarvis lê daqui)
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.insights_base_diaria AS
WITH base AS (
  SELECT
    a.id_empresa,
    a.id_filial,
    a.data_key,
    a.data,
    a.faturamento,
    a.margem,
    a.ticket_medio,
    a.qtd_vendas
  FROM mart.agg_vendas_diaria a
),
mes AS (
  SELECT
    id_empresa,
    id_filial,
    (data_key/100) AS ym, -- YYYYMM
    SUM(faturamento) AS faturamento_mes
  FROM base
  GROUP BY 1,2,3
),
mes_anterior AS (
  SELECT
    m.id_empresa,
    m.id_filial,
    m.ym,
    LAG(m.faturamento_mes) OVER (PARTITION BY m.id_empresa, m.id_filial ORDER BY m.ym) AS faturamento_mes_anterior
  FROM mes m
)
SELECT
  b.id_empresa,
  b.id_filial,
  b.data_key,
  b.data,
  b.faturamento AS faturamento_dia,
  (SELECT m.faturamento_mes
     FROM mes m
    WHERE m.id_empresa=b.id_empresa AND m.id_filial=b.id_filial AND m.ym=(b.data_key/100)
    LIMIT 1) AS faturamento_mes_acum,
  (SELECT
      CASE
        WHEN ma.faturamento_mes_anterior IS NULL OR ma.faturamento_mes_anterior=0 THEN NULL
        ELSE (( (SELECT m2.faturamento_mes
                  FROM mes m2
                 WHERE m2.id_empresa=b.id_empresa AND m2.id_filial=b.id_filial AND m2.ym=(b.data_key/100)
                 LIMIT 1) - ma.faturamento_mes_anterior) / ma.faturamento_mes_anterior) * 100
      END
    FROM mes_anterior ma
   WHERE ma.id_empresa=b.id_empresa AND ma.id_filial=b.id_filial AND ma.ym=(b.data_key/100)
   LIMIT 1) AS comparativo_mes_anterior_pct,
  b.margem,
  b.ticket_medio,
  b.qtd_vendas,
  now() AS updated_at
FROM base b;

CREATE UNIQUE INDEX IF NOT EXISTS ux_insights_base_diaria
ON mart.insights_base_diaria (id_empresa, id_filial, data_key);

COMMIT;