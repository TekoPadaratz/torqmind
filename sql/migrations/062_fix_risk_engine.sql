-- ============================================================================
-- Migration 062: Fix etl.compute_risk_events_v2 (grain + scans + field names)
-- ============================================================================
-- PT-BR: Corrige a função v2 do motor de risco quebrada em produção:
--   1) GRÃO: outliers de funcionário recebem IDs sintéticos negativos
--      (id_db=-1, id_comprovante=-id_usuario, id_movprodutos=-data_key)
--      para evitar violação de uq_fact_risco_evento_nk quando múltiplos
--      usuários disparam outlier no mesmo dia/filial. As colunas *_nk
--      são GENERATED ALWAYS AS COALESCE(col, -1) STORED — qualquer NULL
--      colapsava a tripla (id_db_nk,id_comprovante_nk,id_movprodutos_nk)
--      para (-1,-1,-1), violando a unique.
--   2) SCANS: consolida 3 scans em fact_comprovante em UMA materialização
--      CTE base (cte_base) reutilizada por p90_cancel, user_stats e
--      cancel_events. CTE MATERIALIZED força o planner a não inline.
--   3) CAMPOS: mantém dw.fact_comprovante.valor_total (correto, grão de
--      comprovante). NÃO referencia dw.fact_venda — função opera apenas
--      no grão de comprovante. A coluna real em fact_venda é "total_venda"
--      (singular), conforme migration 003 — irrelevante aqui.
--
-- Assinatura preservada: (int, boolean, int, timestamptz) RETURNS integer.
-- Compatível com routes_etl.py e etl_orchestrator.py existentes.
-- ============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION etl.compute_risk_events_v2(
  p_id_empresa     int,
  p_force_full     boolean    DEFAULT false,
  p_lookback_days  int        DEFAULT 14,
  p_end_ts         timestamptz DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_from_date  date;
  v_to_date    date;
  v_from_key   integer;
  v_to_key     integer;
  v_rows       integer := 0;
BEGIN
  v_to_date   := COALESCE(p_end_ts, now())::date;
  v_from_date := v_to_date - make_interval(days => p_lookback_days);
  v_from_key  := to_char(v_from_date, 'YYYYMMDD')::int;
  v_to_key    := to_char(v_to_date,   'YYYYMMDD')::int;

  -- ────────────────────────────────────────────────────────────────────────
  -- 1) Limpeza idempotente da janela alvo
  -- ────────────────────────────────────────────────────────────────────────
  IF p_force_full THEN
    DELETE FROM dw.fact_risco_evento
    WHERE id_empresa = p_id_empresa;
  ELSE
    DELETE FROM dw.fact_risco_evento
    WHERE id_empresa = p_id_empresa
      AND data_key BETWEEN v_from_key AND v_to_key;
  END IF;

  -- ────────────────────────────────────────────────────────────────────────
  -- 2) Insere eventos pré-agregados (single-scan via CTE base)
  -- ────────────────────────────────────────────────────────────────────────
  WITH
  cte_base AS MATERIALIZED (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      c.id_comprovante,
      c.id_usuario,
      c.id_turno,
      c.id_cliente,
      c.data,
      c.data_key,
      c.valor_total,
      c.cancelado
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = p_id_empresa
      AND c.data_key BETWEEN v_from_key AND v_to_key
  ),
  p90_cancel AS (
    SELECT
      b.id_filial,
      b.data_key,
      percentile_cont(0.90) WITHIN GROUP (ORDER BY b.valor_total) AS p90_valor
    FROM cte_base b
    WHERE b.cancelado = true
      AND b.valor_total IS NOT NULL
    GROUP BY b.id_filial, b.data_key
  ),
  user_stats AS (
    SELECT
      b.id_filial,
      b.id_usuario,
      b.data_key,
      COUNT(*) FILTER (WHERE b.cancelado) AS user_cancels,
      COUNT(*)                            AS user_total,
      CASE WHEN COUNT(*) > 0
        THEN ROUND(
               COUNT(*) FILTER (WHERE b.cancelado)::numeric
               / COUNT(*)::numeric, 4)
        ELSE 0
      END                                 AS user_cancel_rate
    FROM cte_base b
    WHERE b.id_usuario IS NOT NULL
    GROUP BY b.id_filial, b.id_usuario, b.data_key
  ),
  cancel_events AS (
    SELECT
      p_id_empresa                              AS id_empresa,
      b.id_filial,
      b.data_key,
      b.data,
      'CANCELAMENTO'::text                      AS event_type,
      'DW'::text                                AS source,
      b.id_db,
      b.id_comprovante,
      NULL::integer                             AS id_movprodutos,
      b.id_usuario,
      NULL::integer                             AS id_funcionario,
      b.id_turno,
      b.id_cliente,
      b.valor_total,
      COALESCE(b.valor_total, 0)                AS impacto_estimado,
      LEAST(100, GREATEST(0,
        40
        + CASE
            WHEN p90.p90_valor IS NOT NULL AND b.valor_total >  p90.p90_valor       THEN 30
            WHEN p90.p90_valor IS NOT NULL AND b.valor_total >  p90.p90_valor * 0.5 THEN 15
            ELSE 0
          END
        + CASE
            WHEN us.user_cancel_rate > 0.30 THEN 30
            WHEN us.user_cancel_rate > 0.15 THEN 15
            ELSE 0
          END
      ))::integer                               AS score_risco,
      jsonb_build_object(
        'p90_valor',        p90.p90_valor,
        'user_cancel_rate', us.user_cancel_rate,
        'user_cancels',     us.user_cancels,
        'user_total',       us.user_total
      )                                         AS reasons
    FROM cte_base b
    LEFT JOIN p90_cancel p90
      ON p90.id_filial = b.id_filial
     AND p90.data_key  = b.data_key
    LEFT JOIN user_stats us
      ON us.id_filial  = b.id_filial
     AND us.id_usuario = b.id_usuario
     AND us.data_key   = b.data_key
    WHERE b.cancelado = true
  ),
  outlier_events AS (
    -- Grão sintético: (id_db=-1, id_comprovante=-id_usuario, id_movprodutos=-data_key)
    -- garante unicidade em uq_fact_risco_evento_nk para múltiplos outliers/dia.
    SELECT
      p_id_empresa                              AS id_empresa,
      us.id_filial,
      us.data_key,
      NULL::timestamptz                         AS data,
      'FUNCIONARIO_OUTLIER'::text               AS event_type,
      'DW'::text                                AS source,
      -1::integer                               AS id_db,
      (- us.id_usuario)::integer                AS id_comprovante,
      (- us.data_key)::integer                  AS id_movprodutos,
      us.id_usuario,
      NULL::integer                             AS id_funcionario,
      NULL::integer                             AS id_turno,
      NULL::integer                             AS id_cliente,
      NULL::numeric                             AS valor_total,
      0::numeric                                AS impacto_estimado,
      LEAST(100, GREATEST(0,
        CASE
          WHEN us.user_cancel_rate > 0.40 THEN 80
          WHEN us.user_cancel_rate > 0.25 THEN 60
          WHEN us.user_cancel_rate > 0.15 THEN 40
          ELSE 20
        END
      ))::integer                               AS score_risco,
      jsonb_build_object(
        'user_cancel_rate', us.user_cancel_rate,
        'user_cancels',     us.user_cancels,
        'user_total',       us.user_total,
        'synthetic_grain',  true
      )                                         AS reasons
    FROM user_stats us
    WHERE us.user_cancel_rate > 0.10
      AND us.user_total       >= 5
  ),
  all_events AS (
    SELECT * FROM cancel_events
    UNION ALL
    SELECT * FROM outlier_events
  ),
  upserted AS (
    INSERT INTO dw.fact_risco_evento (
      id_empresa, id_filial, data_key, data,
      event_type, source,
      id_db, id_comprovante, id_movprodutos,
      id_usuario, id_funcionario, id_turno, id_cliente,
      valor_total, impacto_estimado, score_risco, score_level, reasons
    )
    SELECT
      e.id_empresa, e.id_filial, e.data_key, e.data,
      e.event_type, e.source,
      e.id_db, e.id_comprovante, e.id_movprodutos,
      e.id_usuario, e.id_funcionario, e.id_turno, e.id_cliente,
      e.valor_total, e.impacto_estimado, e.score_risco,
      CASE
        WHEN e.score_risco >= 80 THEN 'ALTO'
        WHEN e.score_risco >= 60 THEN 'SUSPEITO'
        WHEN e.score_risco >= 40 THEN 'ATENCAO'
        ELSE                          'NORMAL'
      END,
      e.reasons
    FROM all_events e
    ON CONFLICT ON CONSTRAINT uq_fact_risco_evento_nk
    DO UPDATE SET
      data             = EXCLUDED.data,
      data_key         = EXCLUDED.data_key,
      id_usuario       = EXCLUDED.id_usuario,
      id_funcionario   = EXCLUDED.id_funcionario,
      id_turno         = EXCLUDED.id_turno,
      id_cliente       = EXCLUDED.id_cliente,
      valor_total      = EXCLUDED.valor_total,
      impacto_estimado = EXCLUDED.impacto_estimado,
      score_risco      = EXCLUDED.score_risco,
      score_level      = EXCLUDED.score_level,
      reasons          = EXCLUDED.reasons
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  PERFORM etl.set_watermark(p_id_empresa, 'risk_events', now());

  RETURN v_rows;
END;
$$;

COMMIT;

-- ============================================================================
-- END OF MIGRATION 062
-- ============================================================================
