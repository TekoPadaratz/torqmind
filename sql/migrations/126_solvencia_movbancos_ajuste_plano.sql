-- Migration 126: Solvência bancos — ajuste de plano (TRANSF AJUSTE*) fora de MOVBANCOS.
--
-- Prova viva ATXDADOS (filial 14458, conta 11 Banrisul, 2026-07-24):
--   SUM(MOVBANCOS sinal ID_DB=ID_FILIAL) = 152.833,35
--   MOVLCTOS DOCUMENTO='TRANSF AJUSTE PIX' plano 414 TIPO=1 VALOR=24.869,45
--     (contrapartida TIPO=0 no plano 815 / conta 35 Sicoob, mesma REFERENCIA)
--   152.833,35 - 24.869,45 = 127.963,90  ← saldo alvo da tela Xpert
--
-- Esses lançamentos NÃO geram linha em MOVBANCOS. Filtro ID_DB=ID_FILIAL no
-- movimento bancário permanece; o ajuste entra pelo plano ligado à conta
-- (CONTASBANCARIA.ID_PLANODECONTAS), sem filtrar ID_DB do MOVLCTOS (costuma
-- ser a matriz 14126 com ID_FILIAL do posto).
--
-- Sinal do ajuste (conta patrimonial bancária):
--   TIPO=0 → +VALOR (entrada no plano)
--   TIPO=1 → −VALOR (saída do plano)
--   ESTORNO=1 → ignorar

CREATE TABLE IF NOT EXISTS stg.movbancos_ajuste_plano (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_db              integer NOT NULL,
  id_movlctos        integer NOT NULL,
  payload            jsonb NOT NULL DEFAULT '{}'::jsonb,
  id_db_shadow       integer,
  id_chave_natural   text,
  dt_evento          timestamptz,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  received_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movlctos)
);

CREATE INDEX IF NOT EXISTS ix_stg_movbancos_ajuste_plano_emp_fil
  ON stg.movbancos_ajuste_plano (id_empresa, id_filial);

CREATE INDEX IF NOT EXISTS ix_stg_movbancos_ajuste_plano_plano
  ON stg.movbancos_ajuste_plano (
    id_empresa,
    id_filial,
    ((payload->>'ID_PLANODECONTAS')),
    dt_evento
  );

COMMENT ON TABLE stg.movbancos_ajuste_plano IS
  'Ajustes de plano (MOVLCTOS DOCUMENTO TRANSF AJUSTE*) que alteram saldo bancário sem espelho em MOVBANCOS. Ver docs/product/XPERT_BANCOS_MAP.md.';

CREATE OR REPLACE FUNCTION etl.movbancos_ajuste_plano_sinal(payload jsonb)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(NULLIF(payload->>'VALOR','')::numeric, 0) * CASE
    WHEN COALESCE(round(NULLIF(payload->>'TIPO','')::numeric)::int, -1) = 0 THEN 1
    WHEN COALESCE(round(NULLIF(payload->>'TIPO','')::numeric)::int, -1) = 1 THEN -1
    ELSE 0
  END;
$$;

COMMENT ON FUNCTION etl.movbancos_ajuste_plano_sinal(jsonb) IS
  'Sinal do ajuste TRANSF AJUSTE* no plano bancário: TIPO 0 = +, TIPO 1 = −.';

CREATE OR REPLACE FUNCTION etl.refresh_liquidez_banco(
  p_id_empresa integer,
  p_id_filial integer DEFAULT NULL,
  p_ano_mes integer DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_liquidez_banco'), p_id_empresa);

  DROP TABLE IF EXISTS pg_temp._solvencia_banco_enriched;
  CREATE TEMP TABLE _solvencia_banco_enriched ON COMMIT DROP AS
  WITH cortes AS (
    SELECT p_ano_mes AS ano_mes,
           make_date(p_ano_mes / 100, p_ano_mes % 100, 1) AS corte
    WHERE p_ano_mes IS NOT NULL
    UNION ALL
    SELECT (EXTRACT(YEAR FROM d)::int * 100 + EXTRACT(MONTH FROM d)::int),
           d::date
    FROM generate_series(
      date_trunc('month', now() AT TIME ZONE 'America/Sao_Paulo') - interval '18 months',
      date_trunc('month', now() AT TIME ZONE 'America/Sao_Paulo'),
      interval '1 month'
    ) AS g(d)
    WHERE p_ano_mes IS NULL
  ),
  mov_ok AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      COALESCE(round(NULLIF(m.payload->>'ID_CONTASBANCARIAS','')::numeric)::int, 0) AS id_conta,
      c.ano_mes,
      etl.movbancos_sinal(m.payload) AS sinal
    FROM stg.movbancos m
    CROSS JOIN cortes c
    WHERE m.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR m.id_filial = p_id_filial)
      AND NOT COALESCE(
            CASE
              WHEN lower(coalesce(m.payload->>'DELETAR', '0')) IN ('1','true','t','yes') THEN true
              WHEN lower(coalesce(m.payload->>'DELETAR', '0')) IN ('0','false','f','no','') THEN false
              ELSE (NULLIF(m.payload->>'DELETAR','')::numeric) <> 0
            END,
            false
          )
      AND m.id_db = m.id_filial
      AND COALESCE(m.dt_evento, (NULLIF(m.payload->>'DTACONTA',''))::timestamptz) < c.corte
      AND COALESCE(round(NULLIF(m.payload->>'ID_CONTASBANCARIAS','')::numeric)::int, 0) > 0
  ),
  por_conta_mov AS (
    SELECT
      mo.id_empresa,
      mo.id_filial,
      mo.ano_mes,
      mo.id_conta AS id_contasbancarias,
      SUM(mo.sinal)::numeric(18,2) AS saldo_mov
    FROM mov_ok mo
    GROUP BY mo.id_empresa, mo.id_filial, mo.ano_mes, mo.id_conta
  ),
  ajuste_por_plano AS (
    SELECT
      a.id_empresa,
      a.id_filial,
      c.ano_mes,
      COALESCE(round(NULLIF(a.payload->>'ID_PLANODECONTAS','')::numeric)::int, 0) AS id_planodecontas,
      SUM(etl.movbancos_ajuste_plano_sinal(a.payload))::numeric(18,2) AS saldo_ajuste
    FROM stg.movbancos_ajuste_plano a
    CROSS JOIN cortes c
    WHERE a.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR a.id_filial = p_id_filial)
      AND COALESCE(round(NULLIF(a.payload->>'ID_PLANODECONTAS','')::numeric)::int, 0) > 0
      AND COALESCE(
            a.dt_evento,
            (NULLIF(a.payload->>'DTACONTA',''))::timestamptz
          ) < c.corte
      AND upper(btrim(coalesce(a.payload->>'DOCUMENTO', ''))) LIKE 'TRANSF AJUSTE%'
      AND NOT COALESCE(
            CASE
              WHEN lower(coalesce(a.payload->>'ESTORNO', '0')) IN ('1','true','t','yes') THEN true
              WHEN lower(coalesce(a.payload->>'ESTORNO', '0')) IN ('0','false','f','no','') THEN false
              ELSE (NULLIF(a.payload->>'ESTORNO','')::numeric) <> 0
            END,
            false
          )
    GROUP BY
      a.id_empresa,
      a.id_filial,
      c.ano_mes,
      COALESCE(round(NULLIF(a.payload->>'ID_PLANODECONTAS','')::numeric)::int, 0)
  ),
  contas_meta AS (
    SELECT
      cb.id_empresa,
      cb.id_filial,
      cb.id_contasbancarias,
      COALESCE(round(NULLIF(cb.payload->>'ID_PLANODECONTAS','')::numeric)::int, 0) AS id_planodecontas,
      COALESCE(
        NULLIF(TRIM(cb.payload->>'DESCRICAO'), ''),
        'Conta #' || cb.id_contasbancarias::text
      ) AS descricao,
      NULLIF(TRIM(cb.payload->>'AGENCIA'), '') AS agencia,
      NULLIF(TRIM(cb.payload->>'NROCONTA'), '') AS nro_conta,
      COALESCE(
        CASE
          WHEN lower(coalesce(cb.payload->>'ATIVO', 'true')) IN ('1','true','t','yes') THEN true
          WHEN lower(coalesce(cb.payload->>'ATIVO', 'true')) IN ('0','false','f','no') THEN false
          ELSE true
        END,
        true
      ) AS ativo,
      COALESCE(round(NULLIF(cb.payload->>'CODIGOBANCOSPADRAO','')::numeric)::int, -1) AS codigo_banco
    FROM stg.contasbancaria cb
    WHERE cb.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR cb.id_filial = p_id_filial)
  ),
  chaves AS (
    SELECT DISTINCT id_empresa, id_filial, ano_mes, id_contasbancarias
    FROM (
      SELECT id_empresa, id_filial, ano_mes, id_contasbancarias FROM por_conta_mov
      UNION
      SELECT cm.id_empresa, cm.id_filial, ap.ano_mes, cm.id_contasbancarias
      FROM ajuste_por_plano ap
      JOIN contas_meta cm
        ON cm.id_empresa = ap.id_empresa
       AND cm.id_filial = ap.id_filial
       AND cm.id_planodecontas = ap.id_planodecontas
       AND cm.id_planodecontas > 0
    ) u
  ),
  por_conta AS (
    SELECT
      k.id_empresa,
      k.id_filial,
      k.ano_mes,
      k.id_contasbancarias,
      (
        COALESCE(pm.saldo_mov, 0) + COALESCE(ap.saldo_ajuste, 0)
      )::numeric(18,2) AS saldo
    FROM chaves k
    LEFT JOIN por_conta_mov pm
      ON pm.id_empresa = k.id_empresa
     AND pm.id_filial = k.id_filial
     AND pm.ano_mes = k.ano_mes
     AND pm.id_contasbancarias = k.id_contasbancarias
    LEFT JOIN contas_meta cm
      ON cm.id_empresa = k.id_empresa
     AND cm.id_filial = k.id_filial
     AND cm.id_contasbancarias = k.id_contasbancarias
    LEFT JOIN ajuste_por_plano ap
      ON ap.id_empresa = k.id_empresa
     AND ap.id_filial = k.id_filial
     AND ap.ano_mes = k.ano_mes
     AND ap.id_planodecontas = cm.id_planodecontas
     AND cm.id_planodecontas > 0
  )
  SELECT
    p.id_empresa,
    p.id_filial,
    p.ano_mes,
    p.id_contasbancarias,
    p.saldo,
    COALESCE(cm.descricao, 'Conta #' || p.id_contasbancarias::text) AS descricao,
    cm.agencia,
    cm.nro_conta,
    COALESCE(cm.ativo, true) AS ativo,
    COALESCE(
      NULLIF(TRIM(bp.payload->>'NOMEBANCOSPADRAO'), ''),
      'Banco ' || COALESCE(cm.codigo_banco::text, '?')
    ) AS banco_nome
  FROM por_conta p
  LEFT JOIN contas_meta cm
    ON cm.id_empresa = p.id_empresa
   AND cm.id_filial = p.id_filial
   AND cm.id_contasbancarias = p.id_contasbancarias
  LEFT JOIN LATERAL (
    SELECT b.payload
    FROM stg.bancospadrao b
    WHERE b.id_empresa = p.id_empresa
      AND COALESCE(round(NULLIF(b.payload->>'CODIGOBANCOSPADRAO','')::numeric)::int, -1)
          = COALESCE(cm.codigo_banco, -2)
    ORDER BY CASE WHEN b.id_filial = p.id_filial THEN 0 ELSE 1 END, b.id_bancospadrao
    LIMIT 1
  ) bp ON true;

  INSERT INTO mart.liquidez_solvencia AS t
    (id_empresa, id_filial, ano_mes, ativo_banco, tem_ativo_dados, updated_at)
  SELECT
    id_empresa,
    id_filial,
    ano_mes,
    COALESCE(SUM(saldo) FILTER (WHERE ativo IS DISTINCT FROM false), 0)::numeric(18,2),
    true,
    now()
  FROM _solvencia_banco_enriched
  GROUP BY id_empresa, id_filial, ano_mes
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE
    SET ativo_banco = EXCLUDED.ativo_banco,
        tem_ativo_dados = true,
        updated_at = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  DELETE FROM mart.solvencia_banco_conta x
  WHERE x.id_empresa = p_id_empresa
    AND (p_id_filial IS NULL OR x.id_filial = p_id_filial)
    AND (p_ano_mes IS NULL OR x.ano_mes = p_ano_mes);

  INSERT INTO mart.solvencia_banco_conta AS d
    (id_empresa, id_filial, ano_mes, id_contasbancarias,
     banco_nome, agencia, nro_conta, descricao, ativo, saldo, updated_at)
  SELECT
    e.id_empresa, e.id_filial, e.ano_mes, e.id_contasbancarias,
    e.banco_nome, e.agencia, e.nro_conta, e.descricao, e.ativo, e.saldo, now()
  FROM _solvencia_banco_enriched e;

  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_banco(integer, integer, integer) IS
  'Atualiza ativo_banco (contas ativas) e mart.solvencia_banco_conta as-of dia 1. MOVBANCOS (id_db=id_filial) + ajuste TRANSF AJUSTE* do plano ligado à conta.';
