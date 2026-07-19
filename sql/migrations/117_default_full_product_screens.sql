-- 117: usuários pré-cadastrados (gerente/visualizador) com produto completo.
-- Roles altas (master/admin/owner) não usam esta tabela — ROLE_DEFAULT_SCREENS.
-- Idempotente: ON CONFLICT DO NOTHING.

BEGIN;

INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT u.id, s.screen_key
FROM auth.users u
CROSS JOIN (
  VALUES
    ('dashboard_home'),
    ('sales'),
    ('cash'),
    ('fraud'),
    ('fraud.core'),
    ('fraud.risco_financeiro'),
    ('finance'),
    ('customers'),
    ('competitor_pricing'),
    ('competitor_pricing.register'),
    ('competitor_pricing.history'),
    ('competitor_pricing.comparison'),
    ('goals_team'),
    ('goals_team.metas'),
    ('goals_team.comissoes'),
    ('goals_team.config'),
    ('goals_team.orcamento'),
    ('profit_management'),
    ('profit_management.overview'),
    ('profit_management.products'),
    ('profit_management.repricing'),
    ('profit_management.solvencia'),
    ('profit_management.anp')
) AS s(screen_key)
WHERE u.role IN ('tenant_manager', 'tenant_viewer')
ON CONFLICT (user_id, screen_key) DO NOTHING;

COMMIT;
