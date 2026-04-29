-- ============================================================================
-- Migration 068: Operational compatibility repair
-- ============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION etl.bigint_hash64(p_input bigint)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT etl.bigint_hash64(p_input::text);
$$;

DO $$
BEGIN
  IF to_regclass('dw.fact_venda') IS NOT NULL
     AND to_regclass('dw.ux_fact_venda_canonical_doc') IS NULL THEN
    IF EXISTS (
      SELECT 1
      FROM dw.fact_venda
      GROUP BY id_empresa, id_filial, id_db, id_comprovante
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot create unique index dw.fact_venda(id_empresa, id_filial, id_db, id_comprovante): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX ux_fact_venda_canonical_doc
      ON dw.fact_venda (id_empresa, id_filial, id_db, id_comprovante);
  END IF;

  IF to_regclass('app.competitor_fuel_prices') IS NOT NULL
     AND to_regclass('app.uq_competitor_fuel_prices_product') IS NULL THEN
    IF EXISTS (
      SELECT 1
      FROM app.competitor_fuel_prices
      GROUP BY id_empresa, id_filial, id_produto
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot create unique index app.competitor_fuel_prices(id_empresa, id_filial, id_produto): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX uq_competitor_fuel_prices_product
      ON app.competitor_fuel_prices (id_empresa, id_filial, id_produto);
  END IF;
END $$;

COMMIT;
