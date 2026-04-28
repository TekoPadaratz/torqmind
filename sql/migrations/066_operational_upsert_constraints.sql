-- ============================================================================
-- Migration 066: Operational UPSERT constraints
-- ============================================================================

BEGIN;

DO $$
BEGIN
  IF to_regclass('etl.pagamento_comprovante_bridge') IS NOT NULL
     AND to_regclass('etl.uq_pagamento_comprovante_bridge_ref') IS NULL THEN
    IF EXISTS (
      SELECT 1
      FROM etl.pagamento_comprovante_bridge
      GROUP BY id_empresa, id_filial, referencia
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot create unique index etl.pagamento_comprovante_bridge(id_empresa, id_filial, referencia): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX uq_pagamento_comprovante_bridge_ref
      ON etl.pagamento_comprovante_bridge (id_empresa, id_filial, referencia);
  END IF;

  IF to_regclass('app.telegram_settings') IS NOT NULL
     AND to_regclass('app.uq_telegram_settings_empresa') IS NULL THEN
    IF EXISTS (
      SELECT 1
      FROM app.telegram_settings
      GROUP BY id_empresa
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot create unique index app.telegram_settings(id_empresa): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX uq_telegram_settings_empresa
      ON app.telegram_settings (id_empresa);
  END IF;

  IF to_regclass('mart.finance_aging_daily') IS NOT NULL
     AND to_regclass('mart.ux_mart_finance_aging_daily') IS NULL THEN
    IF EXISTS (
      SELECT 1
      FROM mart.finance_aging_daily
      GROUP BY dt_ref, id_empresa, id_filial
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot create unique index mart.finance_aging_daily(dt_ref, id_empresa, id_filial): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX ux_mart_finance_aging_daily
      ON mart.finance_aging_daily (dt_ref, id_empresa, id_filial);
  END IF;
END $$;

COMMIT;
