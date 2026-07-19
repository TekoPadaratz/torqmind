-- 116: ACL por painel — backfill dos filhos quando só o menu estava gravado.
-- Não altera schema; só materializa as chaves filhas para quem já tinha o menu.
-- Runtime também expande (permissions.expand_screen_permissions); isto alinha o DB.

BEGIN;

-- fraud
INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT DISTINCT usp.user_id, v.child_key
FROM auth.user_screen_permissions usp
CROSS JOIN (VALUES
  ('fraud', 'fraud.core'),
  ('fraud', 'fraud.risco_financeiro')
) AS v(parent_key, child_key)
WHERE usp.screen_key = v.parent_key
ON CONFLICT (user_id, screen_key) DO NOTHING;

-- competitor_pricing
INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT DISTINCT usp.user_id, v.child_key
FROM auth.user_screen_permissions usp
CROSS JOIN (VALUES
  ('competitor_pricing', 'competitor_pricing.register'),
  ('competitor_pricing', 'competitor_pricing.history'),
  ('competitor_pricing', 'competitor_pricing.comparison')
) AS v(parent_key, child_key)
WHERE usp.screen_key = v.parent_key
ON CONFLICT (user_id, screen_key) DO NOTHING;

-- goals_team
INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT DISTINCT usp.user_id, v.child_key
FROM auth.user_screen_permissions usp
CROSS JOIN (VALUES
  ('goals_team', 'goals_team.metas'),
  ('goals_team', 'goals_team.comissoes'),
  ('goals_team', 'goals_team.config'),
  ('goals_team', 'goals_team.orcamento')
) AS v(parent_key, child_key)
WHERE usp.screen_key = v.parent_key
ON CONFLICT (user_id, screen_key) DO NOTHING;

-- profit_management (inclui quem ainda não tinha o menu no checkbox legado)
INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT DISTINCT usp.user_id, v.child_key
FROM auth.user_screen_permissions usp
CROSS JOIN (VALUES
  ('profit_management', 'profit_management.overview'),
  ('profit_management', 'profit_management.products'),
  ('profit_management', 'profit_management.repricing'),
  ('profit_management', 'profit_management.solvencia'),
  ('profit_management', 'profit_management.anp')
) AS v(parent_key, child_key)
WHERE usp.screen_key = v.parent_key
ON CONFLICT (user_id, screen_key) DO NOTHING;

COMMIT;
