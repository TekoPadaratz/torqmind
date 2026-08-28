-- Migration 142: Pacote modular por empresa/filial + flag margem/custo por usuário

BEGIN;

ALTER TABLE app.tenants
  ADD COLUMN IF NOT EXISTS module_tier text NOT NULL DEFAULT 'essencial';

ALTER TABLE auth.filiais
  ADD COLUMN IF NOT EXISTS module_tier text;

ALTER TABLE auth.users
  ADD COLUMN IF NOT EXISTS can_view_sensitive_financials boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN app.tenants.module_tier IS
  'Pacote modular contratado (essencial|profissional|gestao|intelligence) — padrão para filiais novas.';

COMMENT ON COLUMN auth.filiais.module_tier IS
  'Pacote modular da filial; NULL = herda app.tenants.module_tier na sincronização.';

COMMENT ON COLUMN auth.users.can_view_sensitive_financials IS
  'Permite ver margem, custo, lucro e CMV (gerente/visualizador); owner/master sempre true.';

-- Owner existente: mantém visão financeira
UPDATE auth.users
SET can_view_sensitive_financials = true
WHERE role IN ('platform_master', 'platform_admin', 'product_global', 'tenant_admin');

-- Filiais sem tier: espelha empresa
UPDATE auth.filiais f
SET module_tier = t.module_tier
FROM app.tenants t
WHERE f.id_empresa = t.id_empresa
  AND f.module_tier IS NULL;

-- Novas filiais via ETL: tier da empresa
CREATE OR REPLACE FUNCTION etl.load_dim_filiais(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows integer;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      COALESCE(payload->>'NOMEFILIAL', payload->>'NOME', payload->>'RAZAOSOCIALFILIAL', '') AS nome,
      COALESCE(payload->>'CNPJ', payload->>'cnpj', payload->>'CNPJCPF', NULL) AS cnpj,
      COALESCE(payload->>'RAZAOSOCIALFILIAL', NULL) AS razao_social
    FROM stg.filiais
    WHERE id_empresa = p_id_empresa
  )
  INSERT INTO dw.dim_filial (id_empresa,id_filial,nome,cnpj,razao_social)
  SELECT id_empresa,id_filial,nome,cnpj,razao_social FROM src
  ON CONFLICT (id_empresa,id_filial)
  DO UPDATE SET
    nome = EXCLUDED.nome,
    cnpj = EXCLUDED.cnpj,
    razao_social = EXCLUDED.razao_social;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO auth.filiais (id_empresa,id_filial,nome,cnpj,is_active,valid_from,module_tier)
  SELECT f.id_empresa, f.id_filial, f.nome, f.cnpj, true, CURRENT_DATE, t.module_tier
  FROM dw.dim_filial f
  JOIN app.tenants t ON t.id_empresa = f.id_empresa
  WHERE f.id_empresa = p_id_empresa
  ON CONFLICT (id_empresa,id_filial) DO NOTHING;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
