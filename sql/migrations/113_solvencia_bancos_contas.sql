-- Migration 113: Solvência — contas bancárias + saldo as-of por conta.
-- Fonte Xpert (validado 2026-07-17 em ATXDADOS):
--   dbo.CONTASBANCARIA  PK (ID_CONTASBANCARIAS, ID_FILIAL) — cadastro da conta
--   dbo.BANCOSPADRAO    domínio FEBRABAN (CODIGOBANCOSPADRAO + NOME)
--   dbo.MOVBANCOS       movimentos; PK (ID_FILIAL, ID_MOVBANCOS, ID_DB)
--   dbo.SALDOSBANCARIOS vazia neste cliente — NÃO usar
--
-- Regra canônica de saldo (abertura do mês = dia 1 00:00 America/Sao_Paulo):
--   1) DELETAR = 0
--   2) ID_DB = ID_FILIAL  (evita réplica cross-DB distorcer o saldo)
--   3) sinal via etl.movbancos_sinal (TIPO 1/8 crédito, 3/5 débito; OPERACAO=1 estorna)
--   4) DTACONTA < make_date(ano, mes, 1)
--   5) preferir contas ATIVO = 1 no total operacional
-- Join conta: (ID_CONTASBANCARIAS, ID_FILIAL) — ID da conta NÃO é global.

CREATE TABLE IF NOT EXISTS stg.contasbancaria (
  id_empresa           integer NOT NULL,
  id_filial            integer NOT NULL,
  id_contasbancarias   integer NOT NULL,
  payload              jsonb NOT NULL,
  id_db_shadow         integer,
  id_chave_natural     text,
  dt_evento            timestamptz,
  ingested_at          timestamptz NOT NULL DEFAULT now(),
  received_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_contasbancarias)
);

CREATE INDEX IF NOT EXISTS ix_stg_contasbancaria_ativo
  ON stg.contasbancaria (id_empresa, id_filial, ((payload->>'ATIVO')));

COMMENT ON TABLE stg.contasbancaria IS
  'Cadastro de contas bancárias (Xpert dbo.CONTASBANCARIA). NK=(id_filial,id_contasbancarias).';

CREATE TABLE IF NOT EXISTS stg.bancospadrao (
  id_empresa            integer NOT NULL,
  id_filial             integer NOT NULL,
  id_bancospadrao       integer NOT NULL,
  payload               jsonb NOT NULL,
  id_db_shadow          integer,
  id_chave_natural      text,
  dt_evento             timestamptz,
  ingested_at           timestamptz NOT NULL DEFAULT now(),
  received_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_bancospadrao)
);

CREATE INDEX IF NOT EXISTS ix_stg_bancospadrao_codigo
  ON stg.bancospadrao (id_empresa, ((payload->>'CODIGOBANCOSPADRAO')));

COMMENT ON TABLE stg.bancospadrao IS
  'Domínio FEBRABAN (Xpert dbo.BANCOSPADRAO). Nome do banco por CODIGOBANCOSPADRAO (+ réplica por filial).';

CREATE TABLE IF NOT EXISTS mart.solvencia_banco_conta (
  id_empresa           integer NOT NULL,
  id_filial            integer NOT NULL,
  ano_mes              integer NOT NULL,
  id_contasbancarias   integer NOT NULL,
  banco_nome           text,
  agencia              text,
  nro_conta            text,
  descricao            text,
  ativo                boolean NOT NULL DEFAULT true,
  saldo                numeric(18,2) NOT NULL DEFAULT 0,
  updated_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, ano_mes, id_contasbancarias)
);

CREATE INDEX IF NOT EXISTS ix_mart_solvencia_banco_conta_mes
  ON mart.solvencia_banco_conta (id_empresa, ano_mes, id_filial);

COMMENT ON TABLE mart.solvencia_banco_conta IS
  'Saldo bancário as-of abertura do mês (dia 1 00:00 SP) por conta corrente, para a seção Bancos da Solvência.';

-- Casts robustos (payload pode trazer "10.0" / boolean JSON).
CREATE OR REPLACE FUNCTION etl.movbancos_sinal(payload jsonb)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(NULLIF(payload->>'VALOR','')::numeric, 0) * CASE
    WHEN COALESCE(round(NULLIF(payload->>'TIPO','')::numeric)::int, -1) IN (1, 8)
      THEN CASE WHEN COALESCE(round(NULLIF(payload->>'OPERACAO','')::numeric)::int, 0) = 1 THEN -1 ELSE 1 END
    WHEN COALESCE(round(NULLIF(payload->>'TIPO','')::numeric)::int, -1) IN (3, 5)
      THEN CASE WHEN COALESCE(round(NULLIF(payload->>'OPERACAO','')::numeric)::int, 0) = 1 THEN 1 ELSE -1 END
    ELSE 0
  END;
$$;

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
  por_conta AS (
    SELECT
      mo.id_empresa,
      mo.id_filial,
      mo.ano_mes,
      mo.id_conta AS id_contasbancarias,
      SUM(mo.sinal)::numeric(18,2) AS saldo
    FROM mov_ok mo
    GROUP BY mo.id_empresa, mo.id_filial, mo.ano_mes, mo.id_conta
  )
  SELECT
    p.id_empresa,
    p.id_filial,
    p.ano_mes,
    p.id_contasbancarias,
    p.saldo,
    COALESCE(
      NULLIF(TRIM(cb.payload->>'DESCRICAO'), ''),
      'Conta #' || p.id_contasbancarias::text
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
    COALESCE(
      NULLIF(TRIM(bp.payload->>'NOMEBANCOSPADRAO'), ''),
      'Banco ' || COALESCE(cb.payload->>'CODIGOBANCOSPADRAO', '?')
    ) AS banco_nome
  FROM por_conta p
  LEFT JOIN stg.contasbancaria cb
    ON cb.id_empresa = p.id_empresa
   AND cb.id_filial = p.id_filial
   AND cb.id_contasbancarias = p.id_contasbancarias
  LEFT JOIN LATERAL (
    SELECT b.payload
    FROM stg.bancospadrao b
    WHERE b.id_empresa = p.id_empresa
      AND COALESCE(round(NULLIF(b.payload->>'CODIGOBANCOSPADRAO','')::numeric)::int, -1)
          = COALESCE(round(NULLIF(cb.payload->>'CODIGOBANCOSPADRAO','')::numeric)::int, -2)
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
  'Atualiza ativo_banco (contas ativas) e mart.solvencia_banco_conta as-of dia 1. Filtro critico: id_db=id_filial + DELETAR=0 + sinal TIPO/OPERACAO.';
