-- ============================================================================
-- Migration 060: Row-Level Security on DW Fact & Dimension Tables
-- ============================================================================
-- PT-BR: Habilita RLS nas tabelas de fato e dimensão do DW para garantir
--        isolamento multi-tenant no nível do banco de dados.
--        A API já filtra por tenant_id, mas RLS é a defesa em profundidade.
-- EN:    Enables RLS on DW fact/dimension tables as defense-in-depth
--        for multi-tenant isolation. The API already filters by tenant,
--        but RLS prevents accidental cross-tenant data leakage.
--
-- POLICY LOGIC:
--   - If app.current_tenant() IS NOT NULL → filter by id_empresa = current_tenant()
--   - If app.current_tenant() IS NULL AND app.current_role() = 'MASTER' → allow all (platform admin)
--   - If neither → deny (fail-safe)
--
-- NOTE: The table owner (postgres) bypasses RLS by default.
--       ETL runs with SET app.tenant_id = N, so it is scoped correctly.
-- ============================================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- Helper: Reusable tenant isolation check
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION app.rls_tenant_check(row_empresa integer)
RETURNS boolean
LANGUAGE sql STABLE
AS $$
  SELECT
    CASE
      -- Tenant is set → strict match
      WHEN app.current_tenant() IS NOT NULL
        THEN row_empresa = app.current_tenant()
      -- MASTER with no tenant scope → platform-wide access
      WHEN app.current_role() = 'MASTER'
        THEN true
      -- Deny by default
      ELSE false
    END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Enable RLS on DW Fact Tables
-- ─────────────────────────────────────────────────────────────────────────────

-- fact_venda
ALTER TABLE dw.fact_venda ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_venda FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_fact_venda ON dw.fact_venda;
CREATE POLICY rls_tenant_fact_venda ON dw.fact_venda
  USING (app.rls_tenant_check(id_empresa));

-- fact_comprovante
ALTER TABLE dw.fact_comprovante ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_comprovante FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_fact_comprovante ON dw.fact_comprovante;
CREATE POLICY rls_tenant_fact_comprovante ON dw.fact_comprovante
  USING (app.rls_tenant_check(id_empresa));

-- fact_venda_item
ALTER TABLE dw.fact_venda_item ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_venda_item FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_fact_venda_item ON dw.fact_venda_item;
CREATE POLICY rls_tenant_fact_venda_item ON dw.fact_venda_item
  USING (app.rls_tenant_check(id_empresa));

-- fact_financeiro
ALTER TABLE dw.fact_financeiro ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_financeiro FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_fact_financeiro ON dw.fact_financeiro;
CREATE POLICY rls_tenant_fact_financeiro ON dw.fact_financeiro
  USING (app.rls_tenant_check(id_empresa));

-- fact_pagamento_comprovante
ALTER TABLE dw.fact_pagamento_comprovante ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_pagamento_comprovante FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_fact_pagamento ON dw.fact_pagamento_comprovante;
CREATE POLICY rls_tenant_fact_pagamento ON dw.fact_pagamento_comprovante
  USING (app.rls_tenant_check(id_empresa));

-- fact_risco_evento
ALTER TABLE dw.fact_risco_evento ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_risco_evento FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_fact_risco ON dw.fact_risco_evento;
CREATE POLICY rls_tenant_fact_risco ON dw.fact_risco_evento
  USING (app.rls_tenant_check(id_empresa));

-- fact_caixa_turno
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'fact_caixa_turno') THEN
    EXECUTE 'ALTER TABLE dw.fact_caixa_turno ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE dw.fact_caixa_turno FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS rls_tenant_fact_caixa ON dw.fact_caixa_turno';
    EXECUTE 'CREATE POLICY rls_tenant_fact_caixa ON dw.fact_caixa_turno USING (app.rls_tenant_check(id_empresa))';
  END IF;
END$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Enable RLS on DW Dimension Tables
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
DECLARE
  dim_tables text[] := ARRAY[
    'dim_filial', 'dim_produto', 'dim_grupo_produto',
    'dim_cliente', 'dim_funcionario', 'dim_local_venda', 'dim_usuario_caixa'
  ];
  t text;
BEGIN
  FOREACH t IN ARRAY dim_tables
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = t) THEN
      EXECUTE format('ALTER TABLE dw.%I ENABLE ROW LEVEL SECURITY', t);
      EXECUTE format('ALTER TABLE dw.%I FORCE ROW LEVEL SECURITY', t);
      EXECUTE format('DROP POLICY IF EXISTS rls_tenant_%I ON dw.%I', t, t);
      EXECUTE format('CREATE POLICY rls_tenant_%I ON dw.%I USING (app.rls_tenant_check(id_empresa))', t, t);
    END IF;
  END LOOP;
END$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Enable RLS on STG Tables (defense-in-depth for ingest layer)
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
DECLARE
  stg_tables text[] := ARRAY[
    'comprovantes', 'movprodutos', 'itensmovprodutos',
    'formas_pgto_comprovantes', 'turnos', 'usuarios',
    'financeiro', 'filiais'
  ];
  t text;
BEGIN
  FOREACH t IN ARRAY stg_tables
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'stg' AND table_name = t) THEN
      EXECUTE format('ALTER TABLE stg.%I ENABLE ROW LEVEL SECURITY', t);
      EXECUTE format('ALTER TABLE stg.%I FORCE ROW LEVEL SECURITY', t);
      EXECUTE format('DROP POLICY IF EXISTS rls_tenant_%I ON stg.%I', t, t);
      EXECUTE format('CREATE POLICY rls_tenant_%I ON stg.%I USING (app.rls_tenant_check(id_empresa))', t, t);
    END IF;
  END LOOP;
END$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Enable RLS on App Tables with tenant data
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
DECLARE
  app_tables text[] := ARRAY[
    'insights_gerados', 'notifications', 'alert_comprovante_cancelado',
    'snapshot_cache'
  ];
  t text;
BEGIN
  FOREACH t IN ARRAY app_tables
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'app' AND table_name = t) THEN
      EXECUTE format('ALTER TABLE app.%I ENABLE ROW LEVEL SECURITY', t);
      EXECUTE format('ALTER TABLE app.%I FORCE ROW LEVEL SECURITY', t);
      EXECUTE format('DROP POLICY IF EXISTS rls_tenant_%I ON app.%I', t, t);
      EXECUTE format('CREATE POLICY rls_tenant_%I ON app.%I USING (app.rls_tenant_check(id_empresa))', t, t);
    END IF;
  END LOOP;
END$$;

COMMIT;

-- ============================================================================
-- END OF MIGRATION 060
-- ============================================================================
 