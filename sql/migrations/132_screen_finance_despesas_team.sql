-- 132_screen_finance_despesas_team.sql
-- Novas telas: finance.despesas + team / team.custos.
-- Backfill para usuários que já tinham finance / goals_team.

BEGIN;

INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT DISTINCT usp.user_id, v.screen_key
FROM auth.user_screen_permissions usp
CROSS JOIN (VALUES
  ('finance.despesas'),
  ('team'),
  ('team.custos')
) AS v(screen_key)
WHERE usp.screen_key IN ('finance', 'finance.overview', 'finance.payable', 'finance.budget', 'goals_team')
ON CONFLICT (user_id, screen_key) DO NOTHING;

INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT DISTINCT usp.user_id, 'finance.budget'
FROM auth.user_screen_permissions usp
WHERE usp.screen_key = 'goals_team.orcamento'
ON CONFLICT (user_id, screen_key) DO NOTHING;

COMMIT;
