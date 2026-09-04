-- Migration 154: espelho Central sempre ligado (paridade Xpert LSC confirmada)
BEGIN;

UPDATE app.commission_config
   SET include_central_mirror = true
 WHERE include_central_mirror = false;

UPDATE app.manager_commission_rule_config
   SET include_central_mirror = true
 WHERE include_central_mirror = false;

ALTER TABLE app.commission_config
  ALTER COLUMN include_central_mirror SET DEFAULT true;

ALTER TABLE app.manager_commission_rule_config
  ALTER COLUMN include_central_mirror SET DEFAULT true;

COMMIT;
