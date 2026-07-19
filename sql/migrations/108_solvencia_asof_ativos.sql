-- Migration 108: Solvencia as-of — STG novas + ativos (dinheiro/estoque loja/cartões).
-- Branch vs001.002-DRE. Idempotente. Nao destrutivo. Nao altera STG existente.
--
-- Objetivo: materializar posicao de ABERTURA do mes (corte dia 1 00:00 America/Sao_Paulo)
-- para os blocos pendentes da Solvencia, em mart.liquidez_solvencia (UPSERT por componente).
--
-- Fontes (Xpert -> Agent -> stg):
--   dinheiro  : stg.formas_pgto_comprovantes (TIPO_FORMA=0) no dia D-1 dos turnos,
--               via stg.comprovantes.DATA; TURNOS nao tem valor de especie.
--   estoque   : reconstrucao por stg.movprodutos/itensmovprodutos ate o corte,
--               custo = VLRCUSTOCOMICMS da ultima NF de entrada (CFOP 1.xxx/2.xxx).
--   cartoes   : stg.formas_pgto_comprovantes (TIPO_FORMA 3/4) x stg.convenios
--               (ID_CARTAO = ID_CONVENIOS; prazo = VENCIMENTO dias; 0 => D+1 debito / D+30 credito).
--
-- Throttle do Agent permanece em runtime.batch_delay_seconds (nao mexer aqui).

-- ===========================================================================
-- 1) STG aditivas (payload jsonb padrao TorqMind)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS stg.convenios (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  id_convenios integer NOT NULL,
  id_db        integer NOT NULL DEFAULT 0,
  payload      jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento    timestamptz,
  id_db_shadow bigint,
  id_chave_natural text,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_convenios)
);
CREATE INDEX IF NOT EXISTS ix_stg_convenios_filial
  ON stg.convenios (id_empresa, id_filial);

COMMENT ON TABLE stg.convenios IS
  'Cadastro de convenios/operadoras (Xpert dbo.CONVENIOS). VENCIMENTO=dias de repasse; join FORMAS_PGTO.ID_CARTAO.';

CREATE TABLE IF NOT EXISTS stg.movbancos (
  id_empresa    integer NOT NULL,
  id_filial     integer NOT NULL,
  id_db         integer NOT NULL DEFAULT 0,
  id_movbancos  integer NOT NULL,
  payload       jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento     timestamptz,
  id_db_shadow  bigint,
  id_chave_natural text,
  ingested_at   timestamptz NOT NULL DEFAULT now(),
  received_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_movbancos)
);
CREATE INDEX IF NOT EXISTS ix_stg_movbancos_evento
  ON stg.movbancos (id_empresa, id_filial, dt_evento);

COMMENT ON TABLE stg.movbancos IS
  'Movimento bancario (Xpert dbo.MOVBANCOS). TIPO/OPERACAO exigem validacao operacional antes de saldo.';

CREATE TABLE IF NOT EXISTS stg.saldoclientes (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  id_db              integer NOT NULL DEFAULT 0,
  id_saldoclientes   integer NOT NULL,
  payload            jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento          timestamptz,
  id_db_shadow       bigint,
  id_chave_natural   text,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  received_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_saldoclientes)
);
CREATE INDEX IF NOT EXISTS ix_stg_saldoclientes_filial
  ON stg.saldoclientes (id_empresa, id_filial);

COMMENT ON TABLE stg.saldoclientes IS
  'Snapshot de saldo de clientes (Xpert dbo.SALDOCLIENTES). VALOR<0 = posto credor (Havel). Sem serie temporal.';

-- Indices de suporte as agregacoes as-of (payload JSON) — so se tabelas ja existirem
CREATE INDEX IF NOT EXISTS ix_stg_formas_pgto_tipo_filial
  ON stg.formas_pgto_comprovantes (id_empresa, id_filial, ((payload->>'TIPO_FORMA')));

CREATE INDEX IF NOT EXISTS ix_stg_movprodutos_filial_data
  ON stg.movprodutos (id_empresa, id_filial, dt_evento);

CREATE INDEX IF NOT EXISTS ix_stg_itensmov_filial_mov
  ON stg.itensmovprodutos (id_empresa, id_filial, id_db, ((payload->>'ID_MOVPRODUTOS')));

-- ===========================================================================
-- 2) ETL: dinheiro em especie na virada do mes (fechamento D-1)
-- ===========================================================================
CREATE OR REPLACE FUNCTION etl.refresh_liquidez_dinheiro(
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
  PERFORM pg_advisory_xact_lock(hashtext('refresh_liquidez_dinheiro'), p_id_empresa);

  -- Cortes: NUNCA usar COALESCE(p_ano_mes, mes_da_serie) dentro do generate_series
  -- (multiplicava o mesmo ano_mes N vezes e inflava SUM).
  WITH cortes AS (
    SELECT p_ano_mes AS ano_mes,
           make_date(p_ano_mes / 100, p_ano_mes % 100, 1) AS corte,
           (make_date(p_ano_mes / 100, p_ano_mes % 100, 1) - 1) AS d_menos_1
    WHERE p_ano_mes IS NOT NULL
    UNION ALL
    SELECT (EXTRACT(YEAR FROM d)::int * 100 + EXTRACT(MONTH FROM d)::int) AS ano_mes,
           (d)::date AS corte,
           ((d)::date - 1) AS d_menos_1
    FROM generate_series(
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) - interval '3 months',
      date_trunc('month', (now() AT TIME ZONE v_tz)::date) + interval '1 month',
      interval '1 month'
    ) AS g(d)
    WHERE p_ano_mes IS NULL
  ),
  dinheiro AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      c.ano_mes,
      SUM(COALESCE(
        etl.safe_numeric(f.payload->>'VALOR_PAGO'),
        etl.safe_numeric(f.payload->>'VALOR'),
        0
      ))::numeric(18,2) AS ativo_caixa
    FROM cortes c
    JOIN stg.formas_pgto_comprovantes f
      ON f.id_empresa = p_id_empresa
     AND (p_id_filial IS NULL OR f.id_filial = p_id_filial)
     AND COALESCE(etl.safe_int(f.payload->>'TIPO_FORMA'), -1) = 0
    JOIN stg.comprovantes cp
      ON cp.id_empresa = f.id_empresa
     AND cp.id_filial = f.id_filial
     AND (
          cp.referencia_shadow = etl.safe_int(f.payload->>'ID_REFERENCIA')
       OR (cp.id_db = COALESCE(etl.safe_int(f.payload->>'ID_DB'), cp.id_db)
           AND cp.id_comprovante = etl.safe_int(f.payload->>'ID_REFERENCIA'))
         )
     AND COALESCE(etl.safe_int(cp.payload->>'SITUACAO'), cp.situacao_shadow, 0) <> 3
     AND COALESCE(
           (etl.safe_timestamp(cp.payload->>'DATA'))::date,
           cp.dt_evento::date
         ) = c.d_menos_1
    GROUP BY f.id_empresa, f.id_filial, c.ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS m
    (id_empresa, id_filial, ano_mes, ativo_caixa, tem_ativo_dados, updated_at)
  SELECT id_empresa, id_filial, ano_mes, ativo_caixa, true, now()
  FROM dinheiro
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
    ativo_caixa     = EXCLUDED.ativo_caixa,
    tem_ativo_dados = true,
    updated_at      = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_dinheiro(integer, integer, integer) IS
  'Solvencia: ativo_caixa = dinheiro (TIPO_FORMA=0) apurado nas vendas do dia anterior ao corte (dia 1 00:00). UPSERT em mart.liquidez_solvencia.';

-- ===========================================================================
-- 3) ETL: cartoes a receber as-of (formas x convenios)
-- ===========================================================================
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
      ))::numeric(18,2) AS ativo_cartoes
    FROM cortes ct
    JOIN stg.formas_pgto_comprovantes f
      ON f.id_empresa = p_id_empresa
     AND (p_id_filial IS NULL OR f.id_filial = p_id_filial)
     AND COALESCE(etl.safe_int(f.payload->>'TIPO_FORMA'), -1) IN (3, 4)
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
            WHEN COALESCE(etl.safe_int(f.payload->>'TIPO_FORMA'), 0) = 4 THEN 1
            ELSE 30
          END
        ) * interval '1 day'
    )::date >= ct.corte
    GROUP BY f.id_empresa, f.id_filial, ct.ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS m
    (id_empresa, id_filial, ano_mes, ativo_cartoes, tem_ativo_dados, updated_at)
  SELECT id_empresa, id_filial, ano_mes, ativo_cartoes, true, now()
  FROM cartoes
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
    ativo_cartoes   = EXCLUDED.ativo_cartoes,
    tem_ativo_dados = true,
    updated_at      = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_cartoes(integer, integer, integer) IS
  'Solvencia: ativo_cartoes as-of = vendas cartao (tipo 3/4) com data de repasse (CONVENIOS.VENCIMENTO) ainda nao vencida no corte.';

-- ===========================================================================
-- 4) ETL: estoque loja as-of valorizado a VLRCUSTOCOMICMS (ultima NF entrada)
-- ===========================================================================
CREATE OR REPLACE FUNCTION etl.refresh_liquidez_estoque_loja_asof(
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
  PERFORM pg_advisory_xact_lock(hashtext('refresh_liquidez_estoque_loja_asof'), p_id_empresa);

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
  -- Sinal: SAIDAS_ENTRADAS=1 entrada (+), 0 saida (-); ignora combustivel (produto em tanque)
  mov AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      ct.ano_mes,
      COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
      SUM(
        CASE
          WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 1
            THEN COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
          WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 0
            THEN -COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
          ELSE 0
        END
      )::numeric(18,6) AS qtde_asof
    FROM cortes ct
    JOIN stg.itensmovprodutos i
      ON i.id_empresa = p_id_empresa
     AND (p_id_filial IS NULL OR i.id_filial = p_id_filial)
    JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa
     AND m.id_filial = i.id_filial
     AND m.id_db = i.id_db
     AND m.id_movprodutos = COALESCE(
           etl.safe_int(i.payload->>'ID_MOVPRODUTOS'),
           NULL
         )
     AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) < ct.corte
    WHERE NOT EXISTS (
      SELECT 1 FROM stg.tanques t
      WHERE t.id_empresa = i.id_empresa AND t.id_filial = i.id_filial
        AND etl.safe_int(t.payload->>'ID_PRODUTOS') = COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
    )
    GROUP BY i.id_empresa, i.id_filial, ct.ano_mes,
             COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
  ),
  custo AS (
    SELECT DISTINCT ON (i.id_empresa, i.id_filial, ct.ano_mes,
                        COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow))
      i.id_empresa,
      i.id_filial,
      ct.ano_mes,
      COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
      etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS') AS custo_unit
    FROM cortes ct
    JOIN stg.itensmovprodutos i
      ON i.id_empresa = p_id_empresa
     AND (p_id_filial IS NULL OR i.id_filial = p_id_filial)
    JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa
     AND m.id_filial = i.id_filial
     AND m.id_db = i.id_db
     AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
     AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) < ct.corte
    WHERE (
        COALESCE(i.payload->>'CFOP','') LIKE '1.%'
     OR COALESCE(i.payload->>'CFOP','') LIKE '2.%'
     OR COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow) = 1
    )
      AND etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS') > 0
    ORDER BY i.id_empresa, i.id_filial, ct.ano_mes,
             COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow),
             COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) DESC
  ),
  loja AS (
    SELECT
      mov.id_empresa,
      mov.id_filial,
      mov.ano_mes,
      SUM(GREATEST(mov.qtde_asof, 0) * COALESCE(custo.custo_unit, 0))::numeric(18,2) AS ativo_estoque_loja
    FROM mov
    LEFT JOIN custo
      ON custo.id_empresa = mov.id_empresa
     AND custo.id_filial = mov.id_filial
     AND custo.ano_mes = mov.ano_mes
     AND custo.id_produto = mov.id_produto
    WHERE mov.qtde_asof > 0 AND COALESCE(custo.custo_unit, 0) > 0
    GROUP BY mov.id_empresa, mov.id_filial, mov.ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS m
    (id_empresa, id_filial, ano_mes, ativo_estoque_loja, ativo_estoque, tem_ativo_dados, updated_at)
  SELECT
    l.id_empresa, l.id_filial, l.ano_mes,
    l.ativo_estoque_loja,
    COALESCE(m0.ativo_estoque_combustivel, 0) + l.ativo_estoque_loja,
    true, now()
  FROM loja l
  LEFT JOIN mart.liquidez_solvencia m0
    ON m0.id_empresa = l.id_empresa AND m0.id_filial = l.id_filial AND m0.ano_mes = l.ano_mes
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
    ativo_estoque_loja = EXCLUDED.ativo_estoque_loja,
    ativo_estoque      = COALESCE(m.ativo_estoque_combustivel, 0)
                         + EXCLUDED.ativo_estoque_loja,
    tem_ativo_dados    = true,
    updated_at         = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_estoque_loja_asof(integer, integer, integer) IS
  'Solvencia: estoque loja as-of reconstruido de movprodutos ate o corte, valorizado com VLRCUSTOCOMICMS da ultima NF de entrada (CFOP 1.xxx/2.xxx). Exclui produtos de tanque.';
