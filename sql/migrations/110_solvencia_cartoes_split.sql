-- 110: split as-of de cartões (crédito TIPO_FORMA=3 / débito=4) em mart.liquidez_solvencia
-- para hint da Solvência sem scan STG na API.

ALTER TABLE mart.liquidez_solvencia
  ADD COLUMN IF NOT EXISTS ativo_cartoes_credito numeric(18,2),
  ADD COLUMN IF NOT EXISTS ativo_cartoes_debito  numeric(18,2);

COMMENT ON COLUMN mart.liquidez_solvencia.ativo_cartoes_credito IS
  'Solvencia as-of: cartões TIPO_FORMA=3 (crédito) ainda a receber no corte.';
COMMENT ON COLUMN mart.liquidez_solvencia.ativo_cartoes_debito IS
  'Solvencia as-of: cartões TIPO_FORMA=4 (débito) ainda a receber no corte.';

CREATE OR REPLACE FUNCTION etl.refresh_liquidez_cartoes(
  p_id_empresa integer,
  p_id_filial integer DEFAULT NULL,
  p_ano_mes integer DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
  v_tz   text := 'America/Sao_Paulo';
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_liquidez_cartoes'), p_id_empresa);

  WITH cortes AS (
    SELECT p_ano_mes AS ano_mes,
           make_date(p_ano_mes / 100, p_ano_mes % 100, 1) AS corte
    WHERE p_ano_mes IS NOT NULL
    UNION ALL
    SELECT (EXTRACT(YEAR FROM d)::int * 100 + EXTRACT(MONTH FROM d)::int) AS ano_mes,
           (d)::date AS corte
    FROM generate_series(
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) - interval '3 months',
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) + interval '1 month',
      interval '1 month'
    ) AS g(d)
    WHERE p_ano_mes IS NULL
  ),
  conv AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_convenios,
      COALESCE(etl.safe_int(c.payload->>'VENCIMENTO'), 0) AS dias_repasse
    FROM stg.convenios c
    WHERE c.id_empresa = p_id_empresa
  ),
  cartoes AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      ct.ano_mes,
      SUM(COALESCE(
        etl.safe_numeric(f.payload->>'VALOR_PAGO'),
        etl.safe_numeric(f.payload->>'VALOR'),
        0
      ))::numeric(18,2) AS ativo_cartoes,
      SUM(CASE WHEN (f.payload->>'TIPO_FORMA') = '3' THEN COALESCE(
        etl.safe_numeric(f.payload->>'VALOR_PAGO'),
        etl.safe_numeric(f.payload->>'VALOR'), 0
      ) ELSE 0 END)::numeric(18,2) AS ativo_cartoes_credito,
      SUM(CASE WHEN (f.payload->>'TIPO_FORMA') = '4' THEN COALESCE(
        etl.safe_numeric(f.payload->>'VALOR_PAGO'),
        etl.safe_numeric(f.payload->>'VALOR'), 0
      ) ELSE 0 END)::numeric(18,2) AS ativo_cartoes_debito
    FROM cortes ct
    JOIN stg.formas_pgto_comprovantes f
      ON f.id_empresa = p_id_empresa
     AND (p_id_filial IS NULL OR f.id_filial = p_id_filial)
     AND (f.payload->>'TIPO_FORMA') IN ('3', '4')
    JOIN stg.comprovantes cp
      ON cp.id_empresa = f.id_empresa
     AND cp.id_filial = f.id_filial
     AND (
          cp.referencia_shadow = etl.safe_int(f.payload->>'ID_REFERENCIA')
       OR cp.id_comprovante = etl.safe_int(f.payload->>'ID_REFERENCIA')
         )
     AND COALESCE(etl.safe_int(cp.payload->>'SITUACAO'), cp.situacao_shadow, 0) <> 3
     AND COALESCE(
           (etl.safe_timestamp(cp.payload->>'DATA'))::date,
           cp.dt_evento::date
         ) < ct.corte
     AND COALESCE(
           (etl.safe_timestamp(cp.payload->>'DATA'))::date,
           cp.dt_evento::date
         ) >= (ct.corte - interval '120 days')
    LEFT JOIN conv cv
      ON cv.id_empresa = f.id_empresa
     AND cv.id_filial = f.id_filial
     AND cv.id_convenios = COALESCE(
           etl.safe_int(f.payload->>'ID_CARTAO'),
           etl.safe_int(f.payload->>'ID_CONVENIOS')
         )
    WHERE (
      COALESCE(
        (etl.safe_timestamp(cp.payload->>'DATA'))::date,
        cp.dt_evento::date
      )
      + (
          CASE
            WHEN COALESCE(cv.dias_repasse, 0) > 0 THEN cv.dias_repasse
            WHEN (f.payload->>'TIPO_FORMA') = '4' THEN 1
            ELSE 30
          END
        ) * interval '1 day'
    )::date >= ct.corte
    GROUP BY f.id_empresa, f.id_filial, ct.ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS m
    (id_empresa, id_filial, ano_mes,
     ativo_cartoes, ativo_cartoes_credito, ativo_cartoes_debito,
     tem_ativo_dados, updated_at)
  SELECT id_empresa, id_filial, ano_mes,
         ativo_cartoes, ativo_cartoes_credito, ativo_cartoes_debito,
         true, now()
  FROM cartoes
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
    ativo_cartoes          = EXCLUDED.ativo_cartoes,
    ativo_cartoes_credito  = EXCLUDED.ativo_cartoes_credito,
    ativo_cartoes_debito   = EXCLUDED.ativo_cartoes_debito,
    tem_ativo_dados        = true,
    updated_at             = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_cartoes(integer, integer, integer) IS
  'Solvencia: ativo_cartoes as-of + split crédito(3)/débito(4) por data de repasse CONVENIOS.VENCIMENTO.';
