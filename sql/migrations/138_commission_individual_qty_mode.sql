-- Migration 138: nível de premiação é por quantidade do VENDEDOR, não da equipe.
-- team_total somava ~1.8k un. e todo mundo caía em Diamante 7% sobre o R$.

BEGIN;

UPDATE app.commission_config
SET default_payment_mode = 'individual_sales',
    updated_at = now()
WHERE is_active = true
  AND default_payment_mode IS DISTINCT FROM 'individual_sales';

COMMIT;
