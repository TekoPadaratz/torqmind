BEGIN;

-- ==========================================
-- Configurable mapping for payment types
-- ==========================================

CREATE TABLE IF NOT EXISTS app.payment_type_map (
  id                bigserial PRIMARY KEY,
  id_empresa        integer NULL,
  id_empresa_nk     integer GENERATED ALWAYS AS (COALESCE(id_empresa, -1)) STORED,
  tipo_forma        integer NOT NULL,
  label             text NOT NULL,
  category          text NOT NULL,
  severity_hint     text NOT NULL DEFAULT 'INFO' CHECK (severity_hint IN ('INFO','WARN','CRITICAL')),
  active            boolean NOT NULL DEFAULT true,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id_empresa_nk, tipo_forma)
);

CREATE INDEX IF NOT EXISTS ix_payment_type_map_lookup
  ON app.payment_type_map (id_empresa_nk, tipo_forma, active, updated_at DESC);

INSERT INTO app.payment_type_map (id_empresa, tipo_forma, label, category, severity_hint, active)
VALUES
  (NULL, 0, 'CAIXA_LOCAL', 'DINHEIRO', 'INFO', true),
  (NULL, 1, 'DINHEIRO', 'DINHEIRO', 'INFO', true),
  (NULL, 2, 'CARTAO_CREDITO', 'CARTAO', 'INFO', true),
  (NULL, 3, 'CARTAO_DEBITO', 'CARTAO', 'INFO', true),
  (NULL, 4, 'PIX', 'PIX', 'INFO', true),
  (NULL, 5, 'CHEQUE', 'CHEQUE', 'WARN', true),
  (NULL, 6, 'CONVENIO/FROTA', 'CONVENIO_FROTA', 'WARN', true),
  (NULL, 999, 'OUTROS', 'OUTROS', 'WARN', true)
ON CONFLICT (id_empresa_nk, tipo_forma)
DO UPDATE SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  severity_hint = EXCLUDED.severity_hint,
  active = EXCLUDED.active,
  updated_at = now();

-- ==========================================
-- STG: formas de pagamento por comprovante
-- ==========================================

CREATE TABLE IF NOT EXISTS stg.formas_pgto_comprovantes (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  id_referencia     bigint NOT NULL,
  tipo_forma        integer NOT NULL,
  id_db_shadow      bigint NULL,
  id_chave_natural  text NULL,
  dt_evento         timestamptz NULL,
  payload           jsonb NOT NULL,
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  received_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_referencia, tipo_forma)
);

CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_received
  ON stg.formas_pgto_comprovantes (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_evento
  ON stg.formas_pgto_comprovantes (id_empresa, dt_evento);
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_filial_evento
  ON stg.formas_pgto_comprovantes (id_empresa, id_filial, dt_evento);
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_comp_emp_ref
  ON stg.formas_pgto_comprovantes (id_empresa, id_filial, id_referencia);

-- ==========================================
-- DW fact: pagamento por comprovante
-- ==========================================

CREATE TABLE IF NOT EXISTS dw.fact_pagamento_comprovante (
  id_empresa        integer NOT NULL,
  id_filial         integer NOT NULL,
  referencia        bigint NOT NULL,
  id_db             integer NULL,
  id_comprovante    integer NULL,
  id_turno          integer NULL,
  id_usuario        integer NULL,
  tipo_forma        integer NOT NULL,
  valor             numeric(18,2) NOT NULL DEFAULT 0,
  dt_evento         timestamptz NOT NULL,
  data_key          integer NOT NULL,
  nsu               text NULL,
  autorizacao       text NULL,
  bandeira          text NULL,
  rede              text NULL,
  tef               text NULL,
  payload           jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, referencia, tipo_forma)
);

CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_lookup
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, data_key, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_turno
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, id_turno, data_key);
CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_ref
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, referencia);

DROP TRIGGER IF EXISTS trg_dw_fact_pagamento_comprovante_updated_at ON dw.fact_pagamento_comprovante;
CREATE TRIGGER trg_dw_fact_pagamento_comprovante_updated_at
BEFORE UPDATE ON dw.fact_pagamento_comprovante
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- ==========================================
-- ETL incremental: STG -> DW
-- ==========================================

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);

  WITH src_raw AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_referencia AS referencia,
      etl.safe_int(s.payload->>'TIPO_FORMA') AS tipo_forma,
      COALESCE(
        etl.safe_int(s.payload->>'ID_DB'),
        etl.safe_int(s.payload->>'id_db'),
        etl.safe_int(s.id_db_shadow::text)
      ) AS id_db,
      COALESCE(
        etl.safe_numeric(s.payload->>'VALOR'),
        etl.safe_numeric(s.payload->>'VALOR_PAGO'),
        etl.safe_numeric(s.payload->>'VALORPAGO'),
        etl.safe_numeric(s.payload->>'VLR'),
        etl.safe_numeric(s.payload->>'VLR_PAGO'),
        etl.safe_numeric(s.payload->>'VLRPAGO'),
        0
      )::numeric(18,2) AS valor,
      COALESCE(
        s.dt_evento,
        etl.safe_timestamp(s.payload->>'DATAREPL'),
        etl.safe_timestamp(s.payload->>'DATAHORA'),
        etl.safe_timestamp(s.payload->>'DATA')
      ) AS dt_evento_src,
      COALESCE(s.payload->>'NSU', s.payload->>'nsu') AS nsu,
      COALESCE(s.payload->>'AUTORIZACAO', s.payload->>'autorizacao') AS autorizacao,
      COALESCE(s.payload->>'BANDEIRA', s.payload->>'bandeira') AS bandeira,
      COALESCE(s.payload->>'REDE', s.payload->>'rede') AS rede,
      COALESCE(s.payload->>'TEF', s.payload->>'tef') AS tef,
      s.payload,
      s.received_at
    FROM stg.formas_pgto_comprovantes s
    WHERE s.id_empresa = p_id_empresa
      AND (
        s.received_at > v_wm
        OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), src_refs AS (
    SELECT DISTINCT id_empresa, id_filial, referencia
    FROM src_raw
    WHERE referencia IS NOT NULL
  ), comp_ref AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      etl.safe_int(c.payload->>'REFERENCIA') AS referencia,
      etl.safe_int(c.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(c.payload->>'ID_DB') AS id_db,
      etl.safe_int(c.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(c.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_timestamp(c.payload->>'DATA') AS data_comp,
      row_number() OVER (
        PARTITION BY c.id_empresa, c.id_filial, etl.safe_int(c.payload->>'REFERENCIA')
        ORDER BY c.received_at DESC
      ) AS rn
    FROM stg.comprovantes c
    JOIN src_refs r
      ON r.id_empresa = c.id_empresa
     AND r.id_filial = c.id_filial
     AND r.referencia = etl.safe_int(c.payload->>'REFERENCIA')
    WHERE c.id_empresa = p_id_empresa
      AND etl.safe_int(c.payload->>'REFERENCIA') IS NOT NULL
  ), src AS (
    SELECT
      r.id_empresa,
      r.id_filial,
      r.referencia,
      r.id_db,
      cr.id_comprovante,
      cr.id_turno,
      cr.id_usuario,
      r.tipo_forma,
      r.valor,
      COALESCE(r.dt_evento_src, cr.data_comp, r.received_at) AS dt_evento,
      etl.date_key(COALESCE(r.dt_evento_src, cr.data_comp, r.received_at)::timestamp) AS data_key,
      r.nsu,
      r.autorizacao,
      r.bandeira,
      r.rede,
      r.tef,
      r.payload
    FROM src_raw r
    LEFT JOIN comp_ref cr
      ON cr.id_empresa = r.id_empresa
     AND cr.id_filial = r.id_filial
     AND cr.referencia = r.referencia
     AND cr.rn = 1
    WHERE r.tipo_forma IS NOT NULL
  ), upserted AS (
    INSERT INTO dw.fact_pagamento_comprovante (
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      nsu,autorizacao,bandeira,rede,tef,payload
    )
    SELECT
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      nsu,autorizacao,bandeira,rede,tef,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,referencia,tipo_forma)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      valor = EXCLUDED.valor,
      dt_evento = EXCLUDED.dt_evento,
      data_key = EXCLUDED.data_key,
      nsu = EXCLUDED.nsu,
      autorizacao = EXCLUDED.autorizacao,
      bandeira = EXCLUDED.bandeira,
      rede = EXCLUDED.rede,
      tef = EXCLUDED.tef,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.fact_pagamento_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_pagamento_comprovante.valor IS DISTINCT FROM EXCLUDED.valor
      OR dw.fact_pagamento_comprovante.dt_evento IS DISTINCT FROM EXCLUDED.dt_evento
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.formas_pgto_comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'formas_pgto_comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- MART: payment mix and payment risk/anomalies
-- ==========================================

DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.agg_pagamentos_diaria AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, format('DESCONHECIDO (TIPO_FORMA=%s)', f.tipo_forma)) AS label,
    COALESCE(m.category, 'DESCONHECIDO') AS category,
    COALESCE(m.severity_hint, 'WARN') AS severity_hint
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category, severity_hint
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  CASE WHEN COALESCE(SUM(SUM(valor)) OVER (PARTITION BY id_empresa,id_filial,data_key),0) = 0 THEN 0
       ELSE ((SUM(valor) / NULLIF(SUM(SUM(valor)) OVER (PARTITION BY id_empresa,id_filial,data_key),0)) * 100)
  END::numeric(10,2) AS share_percent,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_diaria
  ON mart.agg_pagamentos_diaria (id_empresa, id_filial, data_key, category, label);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_diaria_lookup
  ON mart.agg_pagamentos_diaria (id_empresa, data_key, id_filial, total_valor DESC);

DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_turno CASCADE;
CREATE MATERIALIZED VIEW mart.agg_pagamentos_turno AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    COALESCE(f.id_turno, -1) AS id_turno,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, format('DESCONHECIDO (TIPO_FORMA=%s)', f.tipo_forma)) AS label,
    COALESCE(m.category, 'DESCONHECIDO') AS category
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  id_turno,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, id_turno, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_turno
  ON mart.agg_pagamentos_turno (id_empresa, id_filial, data_key, id_turno, category, label);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_turno_lookup
  ON mart.agg_pagamentos_turno (id_empresa, data_key, id_filial, id_turno);

DROP MATERIALIZED VIEW IF EXISTS mart.pagamentos_anomalias_diaria CASCADE;
CREATE MATERIALIZED VIEW mart.pagamentos_anomalias_diaria AS
WITH base_ref AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    COUNT(*)::int AS qtd_formas,
    COUNT(*) FILTER (WHERE UPPER(COALESCE(m.category, 'DESCONHECIDO')) = 'DESCONHECIDO')::int AS qtd_desconhecido,
    COALESCE(SUM(f.valor),0)::numeric(18,2) AS valor_total,
    COALESCE(SUM(CASE WHEN UPPER(COALESCE(m.category, 'DESCONHECIDO')) = 'PIX' THEN f.valor ELSE 0 END),0)::numeric(18,2) AS valor_pix,
    COALESCE(MIN(f.id_turno), -1) AS id_turno
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
  GROUP BY 1,2,3,4
), split_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    AVG(qtd_formas)::numeric(10,2) AS avg_formas,
    COUNT(*) FILTER (WHERE qtd_formas >= 3)::int AS comprovantes_multiplos,
    COUNT(*)::int AS comprovantes_total,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
  FROM base_ref
  GROUP BY 1,2,3
), split_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'SPLIT_INCOMUM'::text AS event_type,
    CASE WHEN avg_formas >= 2.4 THEN 'CRITICAL' WHEN avg_formas >= 1.8 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, ROUND((avg_formas - 1.4) * 55 + comprovantes_multiplos * 0.8)))::int AS score,
    COALESCE(valor_total,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'avg_formas_por_comprovante', avg_formas,
      'comprovantes_multiplos', comprovantes_multiplos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM split_daily
  WHERE comprovantes_total >= 20
    AND avg_formas >= 1.8
), unknown_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    COALESCE(SUM(CASE WHEN qtd_desconhecido > 0 THEN valor_total ELSE 0 END),0)::numeric(18,2) AS valor_desconhecido,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total,
    COUNT(*) FILTER (WHERE qtd_desconhecido > 0)::int AS comprovantes_desconhecidos,
    COUNT(*)::int AS comprovantes_total
  FROM base_ref
  GROUP BY 1,2,3
), unknown_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'DESCONHECIDO_EXCESSO'::text AS event_type,
    CASE
      WHEN (valor_desconhecido / NULLIF(valor_total,0)) >= 0.22 THEN 'CRITICAL'
      WHEN (valor_desconhecido / NULLIF(valor_total,0)) >= 0.12 THEN 'WARN'
      ELSE 'INFO'
    END AS severity,
    LEAST(100, ROUND((valor_desconhecido / NULLIF(valor_total,0)) * 280))::int AS score,
    COALESCE(valor_desconhecido,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'share_desconhecido_pct', ROUND((valor_desconhecido / NULLIF(valor_total,0)) * 100, 2),
      'valor_desconhecido', valor_desconhecido,
      'valor_total', valor_total,
      'comprovantes_desconhecidos', comprovantes_desconhecidos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM unknown_daily
  WHERE valor_total > 0
    AND (valor_desconhecido / NULLIF(valor_total,0)) >= 0.12
), turno_pix AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    COALESCE(SUM(valor_pix),0)::numeric(18,2) AS valor_pix,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total,
    CASE WHEN COALESCE(SUM(valor_total),0) = 0 THEN 0
         ELSE COALESCE(SUM(valor_pix),0) / NULLIF(COALESCE(SUM(valor_total),0),0)
    END AS pix_share
  FROM base_ref
  GROUP BY 1,2,3,4
), turno_pix_sig AS (
  SELECT
    t.*,
    AVG(pix_share) OVER (
      PARTITION BY id_empresa,id_filial,id_turno
      ORDER BY data_key
      ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS pix_share_prev_7
  FROM turno_pix t
), turno_pix_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    'PIX_DESVIO_TURNO'::text AS event_type,
    CASE
      WHEN pix_share_prev_7 IS NOT NULL AND pix_share > pix_share_prev_7 * 2.2 THEN 'CRITICAL'
      WHEN pix_share_prev_7 IS NOT NULL AND pix_share > pix_share_prev_7 * 1.7 THEN 'WARN'
      ELSE 'INFO'
    END AS severity,
    LEAST(100, GREATEST(0, ROUND((pix_share - COALESCE(pix_share_prev_7,0)) * 250)))::int AS score,
    GREATEST(0, (pix_share - COALESCE(pix_share_prev_7,0)) * valor_total)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'pix_share_pct', ROUND(pix_share * 100, 2),
      'pix_share_prev_7_pct', ROUND(COALESCE(pix_share_prev_7,0) * 100, 2),
      'valor_pix', valor_pix,
      'valor_total', valor_total
    ) AS reasons
  FROM turno_pix_sig
  WHERE pix_share_prev_7 IS NOT NULL
    AND valor_total >= 1000
    AND pix_share > pix_share_prev_7 * 1.7
), unioned AS (
  SELECT * FROM split_signal
  UNION ALL
  SELECT * FROM unknown_signal
  UNION ALL
  SELECT * FROM turno_pix_signal
)
SELECT
  u.id_empresa,
  u.id_filial,
  u.data_key,
  u.id_turno,
  u.event_type,
  u.severity,
  u.score,
  u.impacto_estimado,
  u.reasons,
  (
    u.event_type || '|' || u.id_empresa::text || '|' || u.id_filial::text || '|' || u.data_key::text || '|' || COALESCE(u.id_turno::text, '-')
  ) AS insight_id,
  (('x' || substr(md5(u.event_type || '|' || u.id_empresa::text || '|' || u.id_filial::text || '|' || u.data_key::text || '|' || COALESCE(u.id_turno::text, '-')), 1, 16))::bit(64)::bigint) AS insight_id_hash,
  now() AS updated_at
FROM unioned u
WHERE u.severity IN ('WARN','CRITICAL');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_pagamentos_anomalias_diaria
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, event_type, COALESCE(id_turno,-1));
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_lookup
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, severity, score DESC);
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_insight
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, insight_id_hash);

-- ==========================================
-- Notifications from CRITICAL payment anomalies
-- ==========================================

CREATE OR REPLACE FUNCTION etl.sync_payment_anomaly_notifications(
  p_id_empresa int,
  p_ref_date date DEFAULT CURRENT_DATE
)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.insight_id_hash AS insight_id,
      'CRITICAL'::text AS severity,
      format('Anomalia de pagamento (%s)', p.event_type) AS title,
      format('Score %s | Impacto estimado R$ %s', p.score, to_char(COALESCE(p.impacto_estimado,0), 'FM999G999G990D00')) AS body,
      '/fraud'::text AS url
    FROM mart.pagamentos_anomalias_diaria p
    WHERE p.id_empresa = p_id_empresa
      AND p.severity = 'CRITICAL'
      AND p.data_key >= to_char((p_ref_date - interval '2 day')::date, 'YYYYMMDD')::int
      AND p.insight_id_hash IS NOT NULL
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
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Integrate with mart refresh + run_all
-- ==========================================

DROP FUNCTION IF EXISTS etl.run_all(integer, boolean, boolean);

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
  ELSE
    v_meta := v_meta || jsonb_build_object('mart_refreshed', false, 'payment_notifications', 0, 'payment_notifications_skipped', true);
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

COMMIT;
