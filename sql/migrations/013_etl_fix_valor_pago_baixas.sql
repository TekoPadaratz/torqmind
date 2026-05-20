-- Migration: 013 - Fix valor_pago in fact_financeiro to use partial payments (baixas parciais)
-- The original ETL only uses VLRPAGO from the main record, which is only populated on full payment.
-- This update uses GREATEST(VLRPAGO, SUM(baixas)) to reflect partial payments from CONTASRECEBERBAIXA/CONTASPAGARBAIXA.

CREATE OR REPLACE FUNCTION etl.load_fact_financeiro(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_from_ts timestamptz;
  v_max_a timestamptz;
  v_max_b timestamptz;
  v_max_c timestamptz;
  v_max_d timestamptz;
  v_max_e timestamptz;
  v_max_final timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'financeiro'), '1970-01-01'::timestamptz);
  v_from_ts := etl.effective_from_ts(p_id_empresa, 'financeiro');

  WITH baixa_receber_agg AS (
    SELECT id_empresa, id_filial,
           (payload->>'ID_CONTASRECEBER')::int AS id_contasreceber,
           SUM(COALESCE(etl.safe_numeric(payload->>'VALORBAIXA'), 0))::numeric(18,2) AS total_baixa
    FROM stg.contasreceberbaixa
    WHERE id_empresa = p_id_empresa
    GROUP BY id_empresa, id_filial, (payload->>'ID_CONTASRECEBER')::int
  ),
  baixa_pagar_agg AS (
    SELECT id_empresa, id_filial,
           (payload->>'ID_CONTASPAGAR')::int AS id_contaspagar,
           SUM(COALESCE(etl.safe_numeric(payload->>'VALORBAIXA'), 0))::numeric(18,2) AS total_baixa
    FROM stg.contaspagarbaixa
    WHERE id_empresa = p_id_empresa
    GROUP BY id_empresa, id_filial, (payload->>'ID_CONTASPAGAR')::int
  ),
  src AS (
    -- stg.financeiro (unified table) - apply baixa correction based on tipo_titulo
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_db,
      f.tipo_titulo,
      f.id_titulo,
      etl.safe_int(f.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(f.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(f.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(f.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(f.payload->>'VALOR')::numeric(18,2) AS valor,
      GREATEST(
        COALESCE(etl.safe_numeric(f.payload->>'VLRPAGO'), 0),
        COALESCE(
          CASE f.tipo_titulo
            WHEN 1 THEN br.total_baixa
            WHEN 0 THEN bp.total_baixa
            ELSE 0
          END, 0)
      )::numeric(18,2) AS valor_pago,
      f.payload
    FROM stg.financeiro f
    LEFT JOIN baixa_receber_agg br
      ON br.id_empresa = f.id_empresa AND br.id_filial = f.id_filial
      AND br.id_contasreceber = f.id_titulo AND f.tipo_titulo = 1
    LEFT JOIN baixa_pagar_agg bp
      ON bp.id_empresa = f.id_empresa AND bp.id_filial = f.id_filial
      AND bp.id_contaspagar = f.id_titulo AND f.tipo_titulo = 0
    WHERE f.id_empresa = p_id_empresa
      AND f.ingested_at > v_from_ts

    UNION ALL

    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_db,
      0 AS tipo_titulo,
      p.id_contaspagar AS id_titulo,
      etl.safe_int(p.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(p.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(p.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(p.payload->>'VALOR')::numeric(18,2) AS valor,
      GREATEST(
        COALESCE(etl.safe_numeric(p.payload->>'VLRPAGO'), 0),
        COALESCE(bp.total_baixa, 0)
      )::numeric(18,2) AS valor_pago,
      p.payload
    FROM stg.contaspagar p
    LEFT JOIN baixa_pagar_agg bp
      ON bp.id_empresa = p.id_empresa AND bp.id_filial = p.id_filial
      AND bp.id_contaspagar = p.id_contaspagar
    WHERE p.id_empresa = p_id_empresa
      AND p.ingested_at > v_from_ts

    UNION ALL

    SELECT
      r.id_empresa,
      r.id_filial,
      r.id_db,
      1 AS tipo_titulo,
      r.id_contasreceber AS id_titulo,
      etl.safe_int(r.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(r.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(r.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(r.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(r.payload->>'VALOR')::numeric(18,2) AS valor,
      GREATEST(
        COALESCE(etl.safe_numeric(r.payload->>'VLRPAGO'), 0),
        COALESCE(br.total_baixa, 0)
      )::numeric(18,2) AS valor_pago,
      r.payload
    FROM stg.contasreceber r
    LEFT JOIN baixa_receber_agg br
      ON br.id_empresa = r.id_empresa AND br.id_filial = r.id_filial
      AND br.id_contasreceber = r.id_contasreceber
    WHERE r.id_empresa = p_id_empresa
      AND r.ingested_at > v_from_ts
  ), upserted AS (
    INSERT INTO dw.fact_financeiro (
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    )
    SELECT
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,tipo_titulo,id_titulo)
    DO UPDATE SET
      id_entidade=EXCLUDED.id_entidade,
      data_emissao=EXCLUDED.data_emissao,
      data_key_emissao=EXCLUDED.data_key_emissao,
      vencimento=EXCLUDED.vencimento,
      data_key_venc=EXCLUDED.data_key_venc,
      data_pagamento=EXCLUDED.data_pagamento,
      data_key_pgto=EXCLUDED.data_key_pgto,
      valor=EXCLUDED.valor,
      valor_pago=EXCLUDED.valor_pago,
      payload=EXCLUDED.payload
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max_a FROM stg.financeiro WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;
  SELECT MAX(ingested_at) INTO v_max_b FROM stg.contaspagar WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;
  SELECT MAX(ingested_at) INTO v_max_c FROM stg.contasreceber WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;
  SELECT MAX(ingested_at) INTO v_max_d FROM stg.contasreceberbaixa WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;
  SELECT MAX(ingested_at) INTO v_max_e FROM stg.contaspagarbaixa WHERE id_empresa = p_id_empresa AND ingested_at > v_from_ts;

  v_max_final := GREATEST(
    COALESCE(v_max_a, '1970-01-01'::timestamptz),
    COALESCE(v_max_b, '1970-01-01'::timestamptz),
    COALESCE(v_max_c, '1970-01-01'::timestamptz),
    COALESCE(v_max_d, '1970-01-01'::timestamptz),
    COALESCE(v_max_e, '1970-01-01'::timestamptz)
  );
  PERFORM etl.set_watermark(p_id_empresa, 'financeiro', COALESCE(v_max_final, v_wm), NULL::bigint);

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;
