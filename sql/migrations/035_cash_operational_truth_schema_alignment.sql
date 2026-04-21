BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_forma_pagamento CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_turno_aberto CASCADE;

CREATE MATERIALIZED VIEW mart.agg_caixa_turno_aberto AS
WITH runtime AS (
  SELECT etl.runtime_now() AS clock_ts
), comprovantes_caixa AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_turno,
    MAX(c.data) AS last_sale_ts,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool), 0)::numeric(18,2) AS total_vendas,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool)::int AS qtd_vendas,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS qtd_cancelamentos
  FROM (
    SELECT
      fc.id_empresa,
      fc.id_filial,
      fc.id_turno,
      fc.data,
      fc.valor_total,
      COALESCE(fc.cancelado, false) AS cancelado_bool,
      etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
    FROM dw.fact_comprovante fc
    WHERE fc.id_turno IS NOT NULL
  ) c
  GROUP BY c.id_empresa, c.id_filial, c.id_turno
), pagamentos_turno AS (
  SELECT
    p.id_empresa,
    p.id_filial,
    p.id_turno,
    MAX(p.dt_evento) AS last_payment_ts,
    COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
  FROM dw.fact_pagamento_comprovante p
  WHERE p.id_turno IS NOT NULL
  GROUP BY p.id_empresa, p.id_filial, p.id_turno
)
SELECT
  t.id_empresa,
  t.id_filial,
  COALESCE(f.nome, '') AS filial_nome,
  t.id_turno,
  t.id_usuario,
  COALESCE(
    NULLIF(u.nome, ''),
    NULLIF(t.payload->>'NOMEUSUARIOS', ''),
    NULLIF(t.payload->>'NOME_USUARIOS', ''),
    NULLIF(t.payload->>'NOMEUSUARIO', ''),
    NULLIF(t.payload->>'NOME_USUARIO', ''),
    CASE WHEN t.id_usuario IS NOT NULL THEN format('Operador %s', t.id_usuario) ELSE NULL END
  ) AS usuario_nome,
  CASE
    WHEN NULLIF(u.nome, '') IS NOT NULL THEN 'usuarios'
    WHEN COALESCE(
      NULLIF(t.payload->>'NOMEUSUARIOS', ''),
      NULLIF(t.payload->>'NOME_USUARIOS', ''),
      NULLIF(t.payload->>'NOMEUSUARIO', ''),
      NULLIF(t.payload->>'NOME_USUARIO', '')
    ) IS NOT NULL THEN 'turnos_payload'
    WHEN t.id_usuario IS NOT NULL THEN 'turno_id'
    ELSE 'indefinido'
  END AS usuario_source,
  t.abertura_ts,
  t.fechamento_ts,
  GREATEST(
    COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
    COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
    COALESCE(t.abertura_ts, '-infinity'::timestamptz)
  ) AS last_activity_ts,
  ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2)::numeric(10,2) AS horas_aberto,
  ROUND(
    EXTRACT(
      EPOCH FROM (
        runtime.clock_ts
        - GREATEST(
            COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
            COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
            COALESCE(t.abertura_ts, '-infinity'::timestamptz)
          )
      )
    ) / 3600.0,
    2
  )::numeric(10,2) AS horas_sem_movimento,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) < runtime.clock_ts - interval '96 hour' THEN true
    ELSE false
  END AS is_stale,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) >= runtime.clock_ts - interval '96 hour' THEN true
    ELSE false
  END AS is_operational_live,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) < runtime.clock_ts - interval '96 hour' THEN 'STALE'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'CRITICAL'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'HIGH'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) < runtime.clock_ts - interval '96 hour' THEN 'Stale'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'Crítico'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'Atenção alta'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'Monitorar'
    ELSE 'Dentro da janela'
  END AS status_label,
  COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
  COALESCE(c.qtd_vendas, 0)::int AS qtd_vendas,
  COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
  COALESCE(c.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
  COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos,
  runtime.clock_ts AS snapshot_ts,
  runtime.clock_ts AS updated_at
FROM dw.fact_caixa_turno t
CROSS JOIN runtime
LEFT JOIN auth.filiais f
  ON f.id_empresa = t.id_empresa
 AND f.id_filial = t.id_filial
LEFT JOIN dw.dim_usuario_caixa u
  ON u.id_empresa = t.id_empresa
 AND u.id_filial = t.id_filial
 AND u.id_usuario = t.id_usuario
LEFT JOIN comprovantes_caixa c
  ON c.id_empresa = t.id_empresa
 AND c.id_filial = t.id_filial
 AND c.id_turno = t.id_turno
LEFT JOIN pagamentos_turno p
  ON p.id_empresa = t.id_empresa
 AND p.id_filial = t.id_filial
 AND p.id_turno = t.id_turno
WHERE t.is_aberto = true
  AND t.abertura_ts IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_turno_aberto
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_turno_aberto_lookup
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, is_operational_live, severity, horas_aberto DESC);

CREATE MATERIALIZED VIEW mart.agg_caixa_forma_pagamento AS
SELECT
  a.id_empresa,
  a.id_filial,
  a.id_turno,
  p.tipo_forma,
  COALESCE(m.label, 'NÃO IDENTIFICADO') AS forma_label,
  COALESCE(m.category, 'NAO_IDENTIFICADO') AS forma_category,
  COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM mart.agg_caixa_turno_aberto a
JOIN dw.fact_pagamento_comprovante p
  ON p.id_empresa = a.id_empresa
 AND p.id_filial = a.id_filial
 AND p.id_turno = a.id_turno
LEFT JOIN LATERAL (
  SELECT label, category
  FROM app.payment_type_map m
  WHERE m.tipo_forma = p.tipo_forma
    AND m.active = true
    AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
  ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
  LIMIT 1
) m ON true
WHERE a.is_operational_live = true
GROUP BY a.id_empresa, a.id_filial, a.id_turno, p.tipo_forma, COALESCE(m.label, 'NÃO IDENTIFICADO'), COALESCE(m.category, 'NAO_IDENTIFICADO');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_forma_pagamento
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, id_turno, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_forma_pagamento_lookup
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, total_valor DESC);

CREATE MATERIALIZED VIEW mart.alerta_caixa_aberto AS
SELECT
  a.id_empresa,
  a.id_filial,
  a.filial_nome,
  a.id_turno,
  a.id_usuario,
  a.usuario_nome,
  a.abertura_ts,
  a.last_activity_ts,
  a.horas_aberto,
  'CRITICAL'::text AS severity,
  format('Caixa %s aberto há %s horas', a.id_turno, trim(to_char(a.horas_aberto, 'FM999999990D00'))) AS title,
  format(
    'O caixa %s da filial %s segue aberto na última leitura operacional. Operador: %s. Última atividade observada: %s.',
    a.id_turno,
    COALESCE(NULLIF(a.filial_nome, ''), format('Filial %s', a.id_filial)),
    COALESCE(NULLIF(a.usuario_nome, ''), 'não identificado'),
    COALESCE(to_char(a.last_activity_ts, 'DD/MM/YYYY HH24:MI'), 'sem atividade observada')
  ) AS body,
  '/cash'::text AS url,
  (
    ('x' || substr(md5(
      'CASH_OPEN_OVER_24H|' || a.id_empresa::text || '|' || a.id_filial::text || '|' || a.id_turno::text
    ), 1, 16))::bit(64)::bigint
  ) AS insight_id_hash,
  now() AS updated_at
FROM mart.agg_caixa_turno_aberto a
WHERE a.is_operational_live = true
  AND a.horas_aberto >= 24;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_alerta_caixa_aberto
  ON mart.alerta_caixa_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_alerta_caixa_aberto_lookup
  ON mart.alerta_caixa_aberto (id_empresa, severity, horas_aberto DESC);

COMMIT;
