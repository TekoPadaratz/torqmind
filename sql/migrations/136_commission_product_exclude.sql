-- Migration 136: Commission product exclusions + CFOP allowlist (Power BI parity)
-- Source remains dw.fact_venda / fact_venda_item (itenscomprovantes).

BEGIN;

CREATE TABLE IF NOT EXISTS app.commission_config_product_exclude (
  id                    bigserial PRIMARY KEY,
  config_id             bigint NOT NULL REFERENCES app.commission_config(id) ON DELETE CASCADE,
  id_produto            integer NOT NULL,
  nome_produto_snapshot text NOT NULL DEFAULT '',
  is_active             boolean NOT NULL DEFAULT true,
  created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_commission_product_exclude_config
  ON app.commission_config_product_exclude (config_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_commission_product_exclude_active
  ON app.commission_config_product_exclude (config_id, id_produto)
  WHERE is_active = true;

COMMENT ON TABLE app.commission_config_product_exclude IS
  'Produtos excluídos da base elegível de comissão de funcionários (grupo ativo − produtos off).';

-- Eligible CFOP allowlist aligned with Power BI (saídas comerciais LSC).
CREATE OR REPLACE VIEW mart.commission_sales_monthly AS
SELECT
  v.id_empresa,
  v.id_filial,
  (EXTRACT(YEAR FROM v.data) * 100 + EXTRACT(MONTH FROM v.data))::integer AS ano_mes,
  COALESCE(i.id_grupo_produto, -1) AS id_grupo_produto,
  COALESCE(g.nome, '(Sem grupo)') AS nome_grupo_produto,
  COALESCE(i.id_funcionario, -1) AS id_funcionario,
  COALESCE(f.nome, '(Sem vendedor)') AS nome_vendedor,
  COALESCE(SUM(i.total), 0)::numeric(18,2) AS venda_total,
  COUNT(DISTINCT v.id_comprovante)::integer AS quantidade_vendas,
  SUM(1)::integer AS quantidade_itens,
  CASE WHEN COUNT(DISTINCT v.id_comprovante) > 0
    THEN (COALESCE(SUM(i.total), 0) / COUNT(DISTINCT v.id_comprovante))::numeric(18,2)
    ELSE 0
  END AS ticket_medio
FROM dw.fact_venda v
JOIN dw.fact_venda_item i
  ON i.id_empresa = v.id_empresa
  AND i.id_filial = v.id_filial
  AND i.id_db = v.id_db
  AND i.id_comprovante = v.id_comprovante
LEFT JOIN dw.dim_grupo_produto g
  ON g.id_empresa = i.id_empresa
  AND g.id_filial = i.id_filial
  AND g.id_grupo_produto = i.id_grupo_produto
LEFT JOIN dw.dim_funcionario f
  ON f.id_empresa = i.id_empresa
  AND f.id_filial = i.id_filial
  AND f.id_funcionario = i.id_funcionario
WHERE v.data_key IS NOT NULL
  AND COALESCE(v.cancelado, false) = false
  AND COALESCE(i.cfop, 0) IN (5102, 5405, 5656, 5667, 5929)
GROUP BY
  v.id_empresa,
  v.id_filial,
  (EXTRACT(YEAR FROM v.data) * 100 + EXTRACT(MONTH FROM v.data))::integer,
  COALESCE(i.id_grupo_produto, -1),
  COALESCE(g.nome, '(Sem grupo)'),
  COALESCE(i.id_funcionario, -1),
  COALESCE(f.nome, '(Sem vendedor)');

-- Seed LSC (id_empresa = 1): grupos + exclusões de produto em todas as filiais ativas.
-- IDs são seed inicial deste cliente — a UI lista grupos/produtos da base dinamicamente.
DO $$
DECLARE
  v_filial integer;
  v_config_id bigint;
  v_gid integer;
  v_gname text;
  v_pid integer;
  v_pname text;
  v_group_ids integer[] := ARRAY[2, 3, 4, 7, 8, 9, 16, 40];
  v_product_ids integer[] := ARRAY[3434, 3435, 3437, 3438, 3440, 5752];
BEGIN
  FOR v_filial IN
    SELECT f.id_filial
    FROM auth.filiais f
    WHERE f.id_empresa = 1
      AND COALESCE(f.is_active, true) = true
      AND f.id_filial > 1
    ORDER BY f.id_filial
  LOOP
    SELECT c.id INTO v_config_id
    FROM app.commission_config c
    WHERE c.id_empresa = 1 AND c.id_filial = v_filial AND c.is_active = true
    LIMIT 1;

    IF v_config_id IS NULL THEN
      INSERT INTO app.commission_config (
        id_empresa, id_filial, name, is_active, default_payment_mode,
        manager_commission_mode, manager_commission_percent
      )
      VALUES (
        1, v_filial, 'Comissao padrao', true, 'team_total', 'use_tiers', 0
      )
      RETURNING id INTO v_config_id;
    END IF;

    IF v_config_id IS NULL THEN
      CONTINUE;
    END IF;

    -- Default tiers if missing
    IF NOT EXISTS (
      SELECT 1 FROM app.commission_config_tier t WHERE t.config_id = v_config_id
    ) THEN
      INSERT INTO app.commission_config_tier
        (config_id, tier_key, tier_name, min_sales_amount, commission_percent, sort_order, is_active)
      VALUES
        (v_config_id, 'bronze', 'Bronze', 30000, 0.5, 1, true),
        (v_config_id, 'silver', 'Prata', 50000, 1.0, 2, true),
        (v_config_id, 'gold', 'Ouro', 80000, 1.5, 3, true),
        (v_config_id, 'diamond', 'Diamante', 120000, 2.0, 4, true);
    END IF;

    -- Seed groups only when none are active yet (não sobrescreve config manual)
    IF NOT EXISTS (
      SELECT 1 FROM app.commission_config_group g
      WHERE g.config_id = v_config_id AND g.is_active = true
    ) THEN
      FOREACH v_gid IN ARRAY v_group_ids
      LOOP
        SELECT COALESCE(MAX(dg.nome), 'Grupo ' || v_gid::text)
          INTO v_gname
        FROM dw.dim_grupo_produto dg
        WHERE dg.id_empresa = 1
          AND dg.id_filial = v_filial
          AND dg.id_grupo_produto = v_gid;

        INSERT INTO app.commission_config_group
          (config_id, id_grupo_produto, nome_grupo_produto_snapshot, is_active)
        VALUES (v_config_id, v_gid, v_gname, true);
      END LOOP;
    END IF;

    -- Seed product excludes only when none active yet
    IF NOT EXISTS (
      SELECT 1 FROM app.commission_config_product_exclude pe
      WHERE pe.config_id = v_config_id AND pe.is_active = true
    ) THEN
      FOREACH v_pid IN ARRAY v_product_ids
      LOOP
        SELECT COALESCE(MAX(dp.nome), 'Produto ' || v_pid::text)
          INTO v_pname
        FROM dw.dim_produto dp
        WHERE dp.id_empresa = 1
          AND dp.id_filial = v_filial
          AND dp.id_produto = v_pid;

        INSERT INTO app.commission_config_product_exclude
          (config_id, id_produto, nome_produto_snapshot, is_active)
        VALUES (v_config_id, v_pid, v_pname, true);
      END LOOP;
    END IF;
  END LOOP;
END $$;

COMMIT;
