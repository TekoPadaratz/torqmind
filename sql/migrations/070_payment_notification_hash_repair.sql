-- ============================================================================
-- Migration 070: Payment notification hash repair
-- ============================================================================

BEGIN;

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
      COALESCE(
        p.insight_id_hash,
        CASE WHEN p.insight_id IS NOT NULL THEN etl.bigint_hash64(p.insight_id) END
      ) AS insight_id,
      'CRITICAL'::text AS severity,
      format('Anomalia de pagamento (%s)', p.event_type) AS title,
      format('Score %s | Impacto estimado R$ %s', p.score, to_char(COALESCE(p.impacto_estimado,0), 'FM999G999G990D00')) AS body,
      '/fraud'::text AS url
    FROM mart.pagamentos_anomalias_diaria p
    WHERE p.id_empresa = p_id_empresa
      AND p.severity = 'CRITICAL'
      AND p.data_key >= to_char((COALESCE(p_ref_date, CURRENT_DATE) - interval '2 day')::date, 'YYYYMMDD')::int
      AND COALESCE(
        p.insight_id_hash,
        CASE WHEN p.insight_id IS NOT NULL THEN etl.bigint_hash64(p.insight_id) END
      ) IS NOT NULL
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

COMMIT;
