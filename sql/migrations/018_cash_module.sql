-- ==========================================
-- Caixa module: STG + DW + MART + alerts
-- ==========================================

CREATE TABLE IF NOT EXISTS stg.usuarios (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_usuario         integer NOT NULL,
  payload            jsonb NOT NULL,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  dt_evento          timestamptz NULL,
  id_db_shadow       integer NULL,
  id_chave_natural   text NULL,
  received_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_usuario)
);

CREATE INDEX IF NOT EXISTS ix_stg_usuarios_ing
  ON stg.usuarios (id_empresa, ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_usuarios_received
  ON stg.usuarios (id_empresa, received_at);

CREATE TABLE IF NOT EXISTS stg.movlctos (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_db              integer NOT NULL,
  id_movlctos        integer NOT NULL,
  payload            jsonb NOT NULL,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  dt_evento          timestamptz NULL,
  id_db_shadow       integer NULL,
  id_chave_natural   text NULL,
  received_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movlctos)
);

CREATE INDEX IF NOT EXISTS ix_stg_movlctos_ing
  ON stg.movlctos (id_empresa, ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_movlctos_received
  ON stg.movlctos (id_empresa, received_at);

CREATE TABLE IF NOT EXISTS dw.dim_usuario_caixa (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_usuario         integer NOT NULL,
  nome               text NOT NULL,
  payload            jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_usuario)
);

DROP TRIGGER IF EXISTS trg_dim_usuario_caixa_updated_at ON dw.dim_usuario_caixa;
CREATE TRIGGER trg_dim_usuario_caixa_updated_at
BEFORE UPDATE ON dw.dim_usuario_caixa
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE TABLE IF NOT EXISTS dw.fact_caixa_turno (
  id_empresa              integer NOT NULL,
  id_filial               integer NOT NULL,
  id_turno                integer NOT NULL,
  id_db                   integer NULL,
  id_usuario              integer NULL,
  abertura_ts             timestamptz NULL,
  fechamento_ts           timestamptz NULL,
  data_key_abertura       integer NULL,
  data_key_fechamento     integer NULL,
  encerrante_fechamento   integer NULL,
  is_aberto               boolean NOT NULL DEFAULT false,
  status_raw              text NULL,
  payload                 jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at              timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_turno)
);

CREATE INDEX IF NOT EXISTS ix_fact_caixa_turno_status
  ON dw.fact_caixa_turno (id_empresa, id_filial, is_aberto, abertura_ts);
CREATE INDEX IF NOT EXISTS ix_fact_caixa_turno_usuario
  ON dw.fact_caixa_turno (id_empresa, id_filial, id_usuario);

DROP TRIGGER IF EXISTS trg_fact_caixa_turno_updated_at ON dw.fact_caixa_turno;
CREATE TRIGGER trg_fact_caixa_turno_updated_at
BEFORE UPDATE ON dw.fact_caixa_turno
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

CREATE OR REPLACE FUNCTION etl.load_dim_usuario_caixa(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'usuarios'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_usuario,
      COALESCE(
        NULLIF(trim(s.payload->>'NOMEUSUARIOS'), ''),
        NULLIF(trim(s.payload->>'NOME_USUARIOS'), ''),
        NULLIF(trim(s.payload->>'NOME'), ''),
        format('Usuário %s', s.id_usuario)
      ) AS nome,
      s.payload
    FROM stg.usuarios s
    WHERE s.id_empresa = p_id_empresa
      AND (
        s.received_at > v_wm
        OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), upserted AS (
    INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
    SELECT id_empresa, id_filial, id_usuario, nome, payload
    FROM src
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

  SELECT MAX(received_at) INTO v_max
  FROM stg.usuarios
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

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
      COALESCE(
        etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO'),
        0
      ) AS encerrante_fechamento,
      UPPER(COALESCE(
        NULLIF(t.payload->>'STATUS', ''),
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
        WHEN status_raw IN ('ABERTO', 'OPEN') AND fechamento_ts IS NULL THEN true
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

DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_turno_aberto CASCADE;
CREATE MATERIALIZED VIEW mart.agg_caixa_turno_aberto AS
WITH comprovantes_caixa AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_turno,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool), 0)::numeric(18,2) AS total_vendas,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool)::int AS qtd_vendas,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS qtd_cancelamentos
  FROM (
    SELECT
      fc.id_empresa,
      fc.id_filial,
      fc.id_turno,
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
  COALESCE(NULLIF(u.nome, ''), format('Usuário %s', t.id_usuario)) AS usuario_nome,
  t.abertura_ts,
  t.fechamento_ts,
  ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2)::numeric(10,2) AS horas_aberto,
  CASE
    WHEN ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'CRITICAL'
    WHEN ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'HIGH'
    WHEN ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  CASE
    WHEN ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'Crítico'
    WHEN ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'Atenção alta'
    WHEN ROUND(EXTRACT(EPOCH FROM (now() - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'Monitorar'
    ELSE 'Dentro da janela'
  END AS status_label,
  COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
  COALESCE(c.qtd_vendas, 0)::int AS qtd_vendas,
  COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
  COALESCE(c.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
  COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos,
  now() AS updated_at
FROM dw.fact_caixa_turno t
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
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, severity, horas_aberto DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_forma_pagamento CASCADE;
CREATE MATERIALIZED VIEW mart.agg_caixa_forma_pagamento AS
SELECT
  t.id_empresa,
  t.id_filial,
  t.id_turno,
  p.tipo_forma,
  COALESCE(m.label, 'NÃO IDENTIFICADO') AS forma_label,
  COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM dw.fact_caixa_turno t
JOIN dw.fact_pagamento_comprovante p
  ON p.id_empresa = t.id_empresa
 AND p.id_filial = t.id_filial
 AND p.id_turno = t.id_turno
LEFT JOIN LATERAL (
  SELECT label
  FROM app.payment_type_map m
  WHERE m.tipo_forma = p.tipo_forma
    AND m.active = true
    AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
  ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
  LIMIT 1
) m ON true
WHERE t.is_aberto = true
GROUP BY t.id_empresa, t.id_filial, t.id_turno, p.tipo_forma, COALESCE(m.label, 'NÃO IDENTIFICADO');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_forma_pagamento
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, id_turno, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_forma_pagamento_lookup
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, total_valor DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_cancelamentos CASCADE;
CREATE MATERIALIZED VIEW mart.agg_caixa_cancelamentos AS
SELECT
  c.id_empresa,
  c.id_filial,
  c.id_turno,
  COALESCE(f.nome, '') AS filial_nome,
  COALESCE(SUM(c.valor_total), 0)::numeric(18,2) AS total_cancelamentos,
  COUNT(*)::int AS qtd_cancelamentos,
  now() AS updated_at
FROM (
  SELECT
    fc.id_empresa,
    fc.id_filial,
    fc.id_turno,
    fc.valor_total,
    etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
  FROM dw.fact_comprovante fc
  WHERE COALESCE(fc.cancelado, false) = true
    AND fc.id_turno IS NOT NULL
) c
LEFT JOIN auth.filiais f
  ON f.id_empresa = c.id_empresa
 AND f.id_filial = c.id_filial
WHERE c.cfop_num > 5000
GROUP BY c.id_empresa, c.id_filial, c.id_turno, f.nome;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_cancelamentos
  ON mart.agg_caixa_cancelamentos (id_empresa, id_filial, id_turno);

DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;
CREATE MATERIALIZED VIEW mart.alerta_caixa_aberto AS
SELECT
  a.id_empresa,
  a.id_filial,
  a.filial_nome,
  a.id_turno,
  a.id_usuario,
  a.usuario_nome,
  a.abertura_ts,
  a.horas_aberto,
  'CRITICAL'::text AS severity,
  format('Caixa %s aberto há %s horas', a.id_turno, trim(to_char(a.horas_aberto, 'FM999999990D00'))) AS title,
  format(
    'O caixa %s da filial %s está aberto há %s horas. Operador: %s.',
    a.id_turno,
    COALESCE(NULLIF(a.filial_nome, ''), format('Filial %s', a.id_filial)),
    trim(to_char(a.horas_aberto, 'FM999999990D00')),
    COALESCE(NULLIF(a.usuario_nome, ''), 'não identificado')
  ) AS body,
  '/cash'::text AS url,
  (
    ('x' || substr(md5(
      'CASH_OPEN_OVER_24H|' || a.id_empresa::text || '|' || a.id_filial::text || '|' || a.id_turno::text
    ), 1, 16))::bit(64)::bigint
  ) AS insight_id_hash,
  now() AS updated_at
FROM mart.agg_caixa_turno_aberto a
WHERE a.horas_aberto >= 24;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_alerta_caixa_aberto
  ON mart.alerta_caixa_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_alerta_caixa_aberto_lookup
  ON mart.alerta_caixa_aberto (id_empresa, severity, horas_aberto DESC);

CREATE OR REPLACE FUNCTION etl.sync_cash_open_notifications(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      insight_id_hash AS insight_id,
      severity,
      title,
      body,
      url
    FROM mart.alerta_caixa_aberto
    WHERE id_empresa = p_id_empresa
      AND insight_id_hash IS NOT NULL
  ), upserted AS (
    INSERT INTO app.notifications (id_empresa, id_filial, insight_id, severity, title, body, url)
    SELECT id_empresa, id_filial, insight_id, severity, title, body, url
    FROM src
    ON CONFLICT (id_empresa, id_filial, insight_id)
    WHERE insight_id IS NOT NULL
    DO UPDATE SET
      severity = EXCLUDED.severity,
      title = EXCLUDED.title,
      body = EXCLUDED.body,
      url = EXCLUDED.url,
      created_at = now(),
      read_at = NULL
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS etl.run_all(integer, boolean, boolean, date);

CREATE OR REPLACE FUNCTION etl.refresh_marts(p_changed jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS $$
DECLARE
  v_meta jsonb := '{}'::jsonb;
  v_sales_changed boolean := COALESCE((p_changed->>'fact_venda')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_venda_item')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_fin_changed boolean := COALESCE((p_changed->>'fact_financeiro')::int,0) > 0;
  v_risk_changed boolean := COALESCE((p_changed->>'risk_events')::int,0) > 0;
  v_payment_changed boolean := COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
                            OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
  v_cash_changed boolean := COALESCE((p_changed->>'fact_caixa_turno')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_pagamento_comprovante')::int,0) > 0
                         OR COALESCE((p_changed->>'fact_comprovante')::int,0) > 0;
BEGIN
  IF v_sales_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria;
    REFRESH MATERIALIZED VIEW mart.insights_base_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_vendas_hora;
    REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
    REFRESH MATERIALIZED VIEW mart.clientes_churn_risco;
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('sales_marts_refreshed', false);
  END IF;

  IF v_fin_changed THEN
    REFRESH MATERIALIZED VIEW mart.financeiro_vencimentos_diaria;
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('finance_mart_refreshed', false);
  END IF;

  IF v_risk_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_risco_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria;
    REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria;
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('risk_marts_refreshed', false);
  END IF;

  IF v_payment_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
    REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
    REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('payments_marts_refreshed', false);
  END IF;

  IF v_cash_changed THEN
    REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
    REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
    REFRESH MATERIALIZED VIEW mart.agg_caixa_cancelamentos;
    REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;
    v_meta := v_meta || jsonb_build_object('cash_marts_refreshed', true);
  ELSE
    v_meta := v_meta || jsonb_build_object('cash_marts_refreshed', false);
  END IF;

  RETURN v_meta;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.run_all(
  p_id_empresa int,
  p_force_full boolean DEFAULT false,
  p_refresh_mart boolean DEFAULT true,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS jsonb AS $$
DECLARE
  v_started timestamptz := clock_timestamp();
  v_meta jsonb := '{}'::jsonb;
  v_step_started timestamptz;
  v_rows integer;
  v_step_ms integer;
  v_refresh_meta jsonb := '{}'::jsonb;
BEGIN
  IF p_force_full THEN
    DELETE FROM etl.watermark WHERE id_empresa = p_id_empresa;
    v_meta := v_meta || jsonb_build_object('watermark_reset', true);
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_filial(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_filial', v_rows, 'dim_filial_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_filial', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_grupos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_grupos', v_rows, 'dim_grupos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_grupos', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_localvendas(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_localvendas', v_rows, 'dim_localvendas_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_localvendas', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_produtos(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_produtos', v_rows, 'dim_produtos_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_produtos', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_funcionarios(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_funcionarios', v_rows, 'dim_funcionarios_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_funcionarios', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_usuario_caixa(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_usuario_caixa', v_rows, 'dim_usuario_caixa_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_usuario_caixa', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_dim_clientes(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('dim_clientes', v_rows, 'dim_clientes_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'dim_clientes', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_comprovante', v_rows, 'fact_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_comprovante', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_caixa_turno(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_caixa_turno', v_rows, 'fact_caixa_turno_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_caixa_turno', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_pagamento_comprovante(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_pagamento_comprovante', v_rows, 'fact_pagamento_comprovante_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_pagamento_comprovante', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_venda(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda', v_rows, 'fact_venda_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_venda_item(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_venda_item', v_rows, 'fact_venda_item_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_venda_item', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  v_step_started := clock_timestamp();
  v_rows := etl.load_fact_financeiro(p_id_empresa);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('fact_financeiro', v_rows, 'fact_financeiro_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'fact_financeiro', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  IF p_force_full
     OR COALESCE((v_meta->>'fact_comprovante')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda')::int,0) > 0
     OR COALESCE((v_meta->>'fact_venda_item')::int,0) > 0
     OR COALESCE((v_meta->>'fact_pagamento_comprovante')::int,0) > 0
  THEN
    v_step_started := clock_timestamp();
    v_rows := etl.compute_risk_events(p_id_empresa, p_force_full, 14);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('risk_events', v_rows, 'risk_events_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'risk_events', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_rows := 0;
    v_meta := v_meta || jsonb_build_object('risk_events', 0, 'risk_events_skipped', true, 'risk_events_skip_reason', 'no_fact_changes');
    PERFORM etl.log_step(p_id_empresa, 'risk_events', clock_timestamp(), clock_timestamp(), 'ok', 0, NULL, jsonb_build_object('skipped', true, 'reason', 'no_fact_changes'));
  END IF;

  IF p_refresh_mart THEN
    v_step_started := clock_timestamp();
    v_refresh_meta := etl.refresh_marts(v_meta);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('mart_refreshed', true, 'mart_refresh', v_refresh_meta, 'mart_refresh_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'refresh_marts', v_step_started, clock_timestamp(), 'ok', 1, NULL, jsonb_build_object('ms', v_step_ms, 'refresh', v_refresh_meta));

    v_step_started := clock_timestamp();
    v_rows := etl.sync_payment_anomaly_notifications(p_id_empresa, p_ref_date);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('payment_notifications', v_rows, 'payment_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'payment_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

    v_step_started := clock_timestamp();
    v_rows := etl.sync_cash_open_notifications(p_id_empresa);
    v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
    v_meta := v_meta || jsonb_build_object('cash_notifications', v_rows, 'cash_notifications_ms', v_step_ms);
    PERFORM etl.log_step(p_id_empresa, 'cash_notifications', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));
  ELSE
    v_meta := v_meta || jsonb_build_object(
      'mart_refreshed', false,
      'payment_notifications', 0,
      'payment_notifications_skipped', true,
      'cash_notifications', 0,
      'cash_notifications_skipped', true
    );
  END IF;

  v_step_started := clock_timestamp();
  v_rows := etl.generate_insights(p_id_empresa, p_ref_date, 7);
  v_step_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_step_started)) * 1000)::int;
  v_meta := v_meta || jsonb_build_object('insights_generated', v_rows, 'insights_generated_ms', v_step_ms);
  PERFORM etl.log_step(p_id_empresa, 'insights_generated', v_step_started, clock_timestamp(), 'ok', v_rows, NULL, jsonb_build_object('ms', v_step_ms));

  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    clock_timestamp(),
    'ok',
    1,
    NULL,
    jsonb_build_object('force_full', p_force_full, 'refresh_mart', p_refresh_mart, 'meta', v_meta)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'id_empresa', p_id_empresa,
    'force_full', p_force_full,
    'ref_date', p_ref_date,
    'hot_window_days', etl.hot_window_days(),
    'started_at', v_started,
    'finished_at', clock_timestamp(),
    'meta', v_meta
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM etl.log_step(
    p_id_empresa,
    'run_all',
    v_started,
    clock_timestamp(),
    'failed',
    0,
    SQLERRM,
    jsonb_build_object('meta_partial', v_meta)
  );
  RAISE;
END;
$$ LANGUAGE plpgsql;
