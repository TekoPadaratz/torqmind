BEGIN;

CREATE INDEX IF NOT EXISTS ix_fact_comprovante_cash_turn_activity
  ON dw.fact_comprovante (id_empresa, id_filial, id_turno, data DESC)
  INCLUDE (id_db, id_comprovante, id_usuario, valor_total, cancelado, payload)
  WHERE id_turno IS NOT NULL AND data IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_cash_turn_activity
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, id_turno, dt_evento DESC)
  INCLUDE (referencia, id_comprovante, id_usuario, tipo_forma, valor)
  WHERE id_turno IS NOT NULL AND dt_evento IS NOT NULL;

CREATE OR REPLACE FUNCTION etl.load_dim_usuario_caixa(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'usuarios'), '1970-01-01'::timestamptz);

  WITH src_usuarios AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_usuario,
      COALESCE(
        NULLIF(trim(s.payload->>'NOMEUSUARIOS'), ''),
        NULLIF(trim(s.payload->>'NOME_USUARIOS'), ''),
        NULLIF(trim(s.payload->>'NOMEUSUARIO'), ''),
        NULLIF(trim(s.payload->>'NOME_USUARIO'), ''),
        NULLIF(trim(s.payload->>'NOME'), ''),
        format('Operador %s', s.id_usuario)
      ) AS nome,
      s.payload,
      s.received_at,
      0 AS source_priority
    FROM stg.usuarios s
    WHERE s.id_empresa = p_id_empresa
      AND (
        s.received_at > v_wm
        OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), src_turnos AS (
    SELECT
      t.id_empresa,
      t.id_filial,
      COALESCE(
        etl.safe_int(t.payload->>'ID_USUARIOS'),
        etl.safe_int(t.payload->>'ID_USUARIO')
      ) AS id_usuario,
      COALESCE(
        NULLIF(trim(t.payload->>'NOMEUSUARIOS'), ''),
        NULLIF(trim(t.payload->>'NOME_USUARIOS'), ''),
        NULLIF(trim(t.payload->>'NOMEUSUARIO'), ''),
        NULLIF(trim(t.payload->>'NOME_USUARIO'), ''),
        NULLIF(trim(t.payload->>'USUARIO'), '')
      ) AS nome,
      jsonb_strip_nulls(
        jsonb_build_object(
          'source', 'turnos_payload',
          'id_turno', t.id_turno,
          'id_usuario', COALESCE(
            etl.safe_int(t.payload->>'ID_USUARIOS'),
            etl.safe_int(t.payload->>'ID_USUARIO')
          )
        )
      ) AS payload,
      t.received_at,
      1 AS source_priority
    FROM stg.turnos t
    WHERE t.id_empresa = p_id_empresa
      AND (
        t.received_at > v_wm
        OR COALESCE(etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO'), 0) = 0
      )
  ), ranked AS (
    SELECT
      src.id_empresa,
      src.id_filial,
      src.id_usuario,
      src.nome,
      src.payload,
      ROW_NUMBER() OVER (
        PARTITION BY src.id_empresa, src.id_filial, src.id_usuario
        ORDER BY src.source_priority ASC, src.received_at DESC
      ) AS rn
    FROM (
      SELECT * FROM src_usuarios
      UNION ALL
      SELECT * FROM src_turnos
    ) src
    WHERE src.id_usuario IS NOT NULL
      AND COALESCE(NULLIF(trim(src.nome), ''), '') <> ''
  ), upserted AS (
    INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
    SELECT id_empresa, id_filial, id_usuario, nome, payload
    FROM ranked
    WHERE rn = 1
    ON CONFLICT (id_empresa, id_filial, id_usuario)
    DO UPDATE SET
      nome = EXCLUDED.nome,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.dim_usuario_caixa.nome IS DISTINCT FROM EXCLUDED.nome
      OR dw.dim_usuario_caixa.payload IS DISTINCT FROM EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT GREATEST(
           COALESCE(
             (
               SELECT MAX(received_at)
               FROM stg.usuarios
               WHERE id_empresa = p_id_empresa
                 AND received_at > v_wm
             ),
             v_wm
           ),
           COALESCE(
             (
               SELECT MAX(received_at)
               FROM stg.turnos
               WHERE id_empresa = p_id_empresa
                 AND received_at > v_wm
                 AND COALESCE(
                       NULLIF(trim(payload->>'NOMEUSUARIOS'), ''),
                       NULLIF(trim(payload->>'NOME_USUARIOS'), ''),
                       NULLIF(trim(payload->>'NOMEUSUARIO'), ''),
                       NULLIF(trim(payload->>'NOME_USUARIO'), ''),
                       NULLIF(trim(payload->>'USUARIO'), '')
                     ) IS NOT NULL
             ),
             v_wm
           )
         )
  INTO v_max;

  PERFORM etl.set_watermark(p_id_empresa, 'usuarios', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_caixa_turno(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'turnos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      t.id_empresa,
      t.id_filial,
      t.id_turno,
      COALESCE(
        etl.safe_int(t.payload->>'ID_DB'),
        etl.safe_int(t.id_db_shadow::text)
      ) AS id_db,
      COALESCE(
        etl.safe_int(t.payload->>'ID_USUARIOS'),
        etl.safe_int(t.payload->>'ID_USUARIO')
      ) AS id_usuario,
      COALESCE(
        etl.safe_timestamp(t.payload->>'DATA'),
        etl.safe_timestamp(t.payload->>'DTABERTURA'),
        etl.safe_timestamp(t.payload->>'DATAABERTURA'),
        etl.safe_timestamp(t.payload->>'DTHRABERTURA'),
        etl.safe_timestamp(t.payload->>'DTHR_ABERTURA'),
        etl.safe_timestamp(t.payload->>'ABERTURA'),
        etl.safe_timestamp(t.payload->>'INICIO')
      ) AS abertura_ts,
      COALESCE(
        etl.safe_timestamp(t.payload->>'DTFECHAMENTO'),
        etl.safe_timestamp(t.payload->>'DATAFECHAMENTO'),
        etl.safe_timestamp(t.payload->>'DTHRFECHAMENTO'),
        etl.safe_timestamp(t.payload->>'DTHR_FECHAMENTO'),
        etl.safe_timestamp(t.payload->>'FECHAMENTO'),
        etl.safe_timestamp(t.payload->>'FIM')
      ) AS fechamento_ts,
      etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO') AS encerrante_fechamento,
      UPPER(COALESCE(
        NULLIF(t.payload->>'STATUS', ''),
        NULLIF(t.payload->>'STATUSTURNO', ''),
        NULLIF(t.payload->>'STATUS_TURNO_WEB', ''),
        NULLIF(t.payload->>'SITUACAO', ''),
        NULLIF(t.payload->>'SITUACAO_TURNO', ''),
        NULLIF(t.payload->>'ST', '')
      )) AS status_raw,
      t.payload,
      t.received_at
    FROM stg.turnos t
    WHERE t.id_empresa = p_id_empresa
      AND (
        t.received_at > v_wm
        OR COALESCE(etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO'), 0) = 0
      )
  ), normalized AS (
    SELECT
      id_empresa,
      id_filial,
      id_turno,
      id_db,
      id_usuario,
      abertura_ts,
      fechamento_ts,
      CASE WHEN abertura_ts IS NULL THEN NULL ELSE etl.date_key(abertura_ts::timestamp) END AS data_key_abertura,
      CASE WHEN fechamento_ts IS NULL THEN NULL ELSE etl.date_key(fechamento_ts::timestamp) END AS data_key_fechamento,
      encerrante_fechamento,
      CASE
        WHEN encerrante_fechamento = 0 THEN true
        ELSE false
      END AS is_aberto,
      status_raw,
      payload
    FROM src
  ), upserted AS (
    INSERT INTO dw.fact_caixa_turno (
      id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts, fechamento_ts,
      data_key_abertura, data_key_fechamento, encerrante_fechamento, is_aberto, status_raw, payload
    )
    SELECT
      id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts, fechamento_ts,
      data_key_abertura, data_key_fechamento, encerrante_fechamento, is_aberto, status_raw, payload
    FROM normalized
    ON CONFLICT (id_empresa, id_filial, id_turno)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_usuario = EXCLUDED.id_usuario,
      abertura_ts = EXCLUDED.abertura_ts,
      fechamento_ts = EXCLUDED.fechamento_ts,
      data_key_abertura = EXCLUDED.data_key_abertura,
      data_key_fechamento = EXCLUDED.data_key_fechamento,
      encerrante_fechamento = EXCLUDED.encerrante_fechamento,
      is_aberto = EXCLUDED.is_aberto,
      status_raw = EXCLUDED.status_raw,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.fact_caixa_turno.id_usuario IS DISTINCT FROM EXCLUDED.id_usuario
      OR dw.fact_caixa_turno.abertura_ts IS DISTINCT FROM EXCLUDED.abertura_ts
      OR dw.fact_caixa_turno.fechamento_ts IS DISTINCT FROM EXCLUDED.fechamento_ts
      OR dw.fact_caixa_turno.encerrante_fechamento IS DISTINCT FROM EXCLUDED.encerrante_fechamento
      OR dw.fact_caixa_turno.is_aberto IS DISTINCT FROM EXCLUDED.is_aberto
      OR dw.fact_caixa_turno.status_raw IS DISTINCT FROM EXCLUDED.status_raw
      OR dw.fact_caixa_turno.payload IS DISTINCT FROM EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.turnos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'turnos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS mart.fraude_cancelamentos_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.fraude_cancelamentos_diaria AS
WITH cancelamentos_operacionais AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.data_key,
    c.valor_total,
    etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num,
    COALESCE(t.id_turno, c.id_turno) AS id_turno
  FROM dw.fact_comprovante c
  LEFT JOIN dw.fact_caixa_turno t
    ON t.id_empresa = c.id_empresa
   AND t.id_filial = c.id_filial
   AND t.id_turno = c.id_turno
   AND (t.data_key_abertura IS NULL OR t.data_key_abertura <= c.data_key)
   AND (
         t.data_key_fechamento IS NULL
         OR t.data_key_fechamento >= c.data_key
         OR t.is_aberto = true
       )
  WHERE c.cancelado = true
    AND c.data_key IS NOT NULL
    AND COALESCE(t.id_turno, c.id_turno) IS NOT NULL
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  COUNT(*)::int AS cancelamentos,
  COALESCE(SUM(valor_total), 0)::numeric(18,2) AS valor_cancelado,
  now() AS updated_at
FROM cancelamentos_operacionais
WHERE cfop_num > 5000
GROUP BY 1,2,3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_fraude_cancelamentos_diaria
  ON mart.fraude_cancelamentos_diaria (id_empresa, id_filial, data_key);
CREATE INDEX IF NOT EXISTS ix_mart_fraude_cancelamentos_diaria_lookup
  ON mart.fraude_cancelamentos_diaria (id_empresa, data_key, id_filial);

DROP MATERIALIZED VIEW IF EXISTS mart.fraude_cancelamentos_eventos CASCADE;
CREATE MATERIALIZED VIEW mart.fraude_cancelamentos_eventos AS
WITH cancelamentos_operacionais AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante,
    c.data,
    c.data_key,
    COALESCE(t.id_turno, c.id_turno) AS id_turno,
    COALESCE(t.id_usuario, c.id_usuario) AS id_usuario,
    c.id_usuario AS id_usuario_documento,
    CASE
      WHEN t.id_usuario IS NOT NULL THEN 'turno'
      WHEN c.id_usuario IS NOT NULL THEN 'comprovante'
      ELSE 'indefinido'
    END AS usuario_source,
    COALESCE(
      NULLIF(u.nome, ''),
      NULLIF(t.payload->>'NOMEUSUARIOS', ''),
      NULLIF(t.payload->>'NOME_USUARIOS', ''),
      NULLIF(t.payload->>'NOMEUSUARIO', ''),
      NULLIF(t.payload->>'NOME_USUARIO', ''),
      CASE
        WHEN COALESCE(t.id_usuario, c.id_usuario) IS NOT NULL THEN format('Operador %s', COALESCE(t.id_usuario, c.id_usuario))
        ELSE NULL
      END
    ) AS usuario_nome,
    c.valor_total,
    etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num,
    now() AS updated_at
  FROM dw.fact_comprovante c
  LEFT JOIN dw.fact_caixa_turno t
    ON t.id_empresa = c.id_empresa
   AND t.id_filial = c.id_filial
   AND t.id_turno = c.id_turno
   AND (t.data_key_abertura IS NULL OR t.data_key_abertura <= c.data_key)
   AND (
         t.data_key_fechamento IS NULL
         OR t.data_key_fechamento >= c.data_key
         OR t.is_aberto = true
       )
  LEFT JOIN dw.dim_usuario_caixa u
    ON u.id_empresa = c.id_empresa
   AND u.id_filial = c.id_filial
   AND u.id_usuario = COALESCE(t.id_usuario, c.id_usuario)
  WHERE c.cancelado = true
    AND COALESCE(t.id_turno, c.id_turno) IS NOT NULL
)
SELECT
  id_empresa,
  id_filial,
  id_db,
  id_comprovante,
  data,
  data_key,
  id_turno,
  id_usuario,
  id_usuario_documento,
  usuario_source,
  usuario_nome,
  valor_total,
  updated_at
FROM cancelamentos_operacionais
WHERE cfop_num > 5000;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_fraude_cancelamentos_eventos
  ON mart.fraude_cancelamentos_eventos (id_empresa, id_filial, id_db, id_comprovante);
CREATE INDEX IF NOT EXISTS ix_mart_fraude_eventos_dt
  ON mart.fraude_cancelamentos_eventos (id_empresa, data DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;
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

DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_forma_pagamento CASCADE;
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
