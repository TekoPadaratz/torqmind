BEGIN;

CREATE OR REPLACE FUNCTION etl.change_domains(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
WITH params AS (
  SELECT
    LOWER(COALESCE(NULLIF(p_changed->>'track', ''), 'full')) AS track,
    COALESCE((p_changed->>'force_full')::boolean, false) AS force_full,
    COALESCE((p_changed->>'dim_grupos')::int, 0) AS dim_grupos,
    COALESCE((p_changed->>'dim_produtos')::int, 0) AS dim_produtos,
    COALESCE((p_changed->>'dim_funcionarios')::int, 0) AS dim_funcionarios,
    COALESCE((p_changed->>'dim_clientes')::int, 0) AS dim_clientes,
    COALESCE((p_changed->>'fact_comprovante')::int, 0) AS fact_comprovante,
    COALESCE((p_changed->>'fact_venda')::int, 0) AS fact_venda,
    COALESCE((p_changed->>'fact_venda_item')::int, 0) AS fact_venda_item,
    COALESCE((p_changed->>'fact_financeiro')::int, 0) AS fact_financeiro,
    COALESCE((p_changed->>'risk_events')::int, 0) AS risk_events,
    COALESCE((p_changed->>'fact_pagamento_comprovante')::int, 0) AS fact_pagamento_comprovante,
    COALESCE((p_changed->>'fact_caixa_turno')::int, 0) AS fact_caixa_turno,
    COALESCE((p_changed->>'dim_usuario_caixa')::int, 0) AS dim_usuario_caixa
)
SELECT jsonb_build_object(
  'sales',
    track IN ('full', 'operational')
    AND (
      force_full
      OR dim_grupos > 0
      OR dim_produtos > 0
      OR dim_funcionarios > 0
      OR dim_clientes > 0
      OR fact_comprovante > 0
      OR fact_venda > 0
      OR fact_venda_item > 0
    ),
  'finance',
    track IN ('full', 'operational')
    AND (
      force_full
      OR fact_financeiro > 0
    ),
  'risk',
    track IN ('full', 'risk')
    AND (
      force_full
      OR risk_events > 0
    ),
  'payments',
    track IN ('full', 'operational')
    AND (
      force_full
      OR fact_pagamento_comprovante > 0
      OR fact_comprovante > 0
    ),
  'cash',
    track IN ('full', 'operational')
    AND (
      force_full
      OR fact_caixa_turno > 0
      OR fact_pagamento_comprovante > 0
      OR fact_comprovante > 0
      OR dim_usuario_caixa > 0
    )
)
FROM params;
$$ LANGUAGE sql STABLE;

COMMIT;
