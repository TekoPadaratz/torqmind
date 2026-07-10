-- Migration 102: Controle de Cheques — reformulacao completa (Financeiro)
-- Idempotente. Nao destrutivo. Estende a 096 (que era so o stub da tabela).
--
-- Objetivo (linguagem do dono): a tela de cheques deve trazer os cheques a VISTA
-- e a PRAZO, com TODOS os status (a compensar, depositado, compensado, devolvido
-- com o MOTIVO), filtro de status multi-selecao e paginacao. Compensados saem da
-- visao padrao mas continuam disponiveis no filtro.
--
-- Fonte canonica (Xpert):
--   dbo.CHEQUESRECEBIDOS: VALOR, DTACONTA(recebido), DTABOM(bom para/vencimento),
--     DTACOMPENSADO(compensado quando preenchido), SITUACAOCHEQUE, ID_SITUACOES,
--     NOME(cliente), CODIGOBANCOSPADRAO, AGENCIA, NROCONTA, NUMERO, CPF.
--   dbo.SITUACOES: ID_SITUACOES -> DESCRICAO = motivo de devolucao (codigos de
--     retorno bancario: 11/12 sem fundos, 21/22/28 sustado/divergencia, etc).
-- Regra de status:
--   devolvido    = ID_SITUACOES >= 1 (tem motivo de devolucao) e nao compensado
--   compensado   = DTACOMPENSADO preenchida (dinheiro caiu)
--   depositado   = SITUACAOCHEQUE = 1 e nao compensado/devolvido
--   a_compensar  = em carteira, ainda nao depositado
--   a vista      = DTABOM ate 2 dias apos DTACONTA; a prazo = pre-datado

-- 1) STG das fontes (coletadas pelo Agent: dbo.CHEQUESRECEBIDOS, dbo.SITUACOES).
CREATE TABLE IF NOT EXISTS stg.cheques (
  id_empresa      integer NOT NULL,
  id_filial       integer NOT NULL,
  id_db           integer NOT NULL DEFAULT 0,
  id_cheque       integer NOT NULL,          -- ID_CHEQUESRECEBIDOS
  payload         jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento       timestamptz,
  id_db_shadow    bigint,
  id_chave_natural text,
  ingested_at     timestamptz NOT NULL DEFAULT now(),
  received_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_cheque)
);
CREATE INDEX IF NOT EXISTS ix_stg_cheques_scope ON stg.cheques (id_empresa, id_filial);

CREATE TABLE IF NOT EXISTS stg.situacoes (
  id_empresa      integer NOT NULL,
  id_filial       integer NOT NULL,
  id_situacao     integer NOT NULL,          -- ID_SITUACOES
  id_db           integer NOT NULL DEFAULT 0,
  payload         jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento       timestamptz,
  id_db_shadow    bigint,
  id_chave_natural text,
  ingested_at     timestamptz NOT NULL DEFAULT now(),
  received_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_situacao)
);

COMMENT ON TABLE stg.cheques IS 'Cheques recebidos (Xpert dbo.CHEQUESRECEBIDOS). payload: VALOR, DTACONTA, DTABOM, DTACOMPENSADO, SITUACAOCHEQUE, ID_SITUACOES, NOME, CODIGOBANCOSPADRAO, AGENCIA, NROCONTA, NUMERO, CPF.';
COMMENT ON TABLE stg.situacoes IS 'Situacoes/motivos (Xpert dbo.SITUACOES). ID_SITUACOES -> DESCRICAO = motivo de devolucao de cheque.';

-- 2) Colunas novas na mart de cheques (aditivo sobre a 096).
ALTER TABLE mart.cheques_pendentes
  ADD COLUMN IF NOT EXISTS dt_compensado    date,
  ADD COLUMN IF NOT EXISTS avista           boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS motivo_devolucao text    NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS status_cheque    text    NOT NULL DEFAULT 'a_compensar';

CREATE INDEX IF NOT EXISTS ix_cheques_status
  ON mart.cheques_pendentes (id_empresa, id_filial, status_cheque);

COMMENT ON COLUMN mart.cheques_pendentes.status_cheque IS
  'a_compensar | depositado | compensado | devolvido. Camada rapida da tela de cheques.';

-- 3) ETL: reconstroi a mart de cheques a partir da STG.
--    DELETE+INSERT por empresa com advisory lock (evita corrida entre agendadores).
--    Horizonte: mantem todos os NAO compensados + compensados dos ultimos 180 dias.
CREATE OR REPLACE FUNCTION etl.refresh_cheques(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
  v_ref  date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_cheques'), p_id_empresa);

  DELETE FROM mart.cheques_pendentes WHERE id_empresa = p_id_empresa;

  INSERT INTO mart.cheques_pendentes (
    id_empresa, id_filial, id_db, id_cheque, id_entidade, cliente_nome, cpf,
    valor, dt_recebido, dt_vencimento, situacao_cheque, banco, agencia, nroconta,
    numero, dt_compensado, avista, motivo_devolucao, status_cheque, updated_at
  )
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_cheque,
    NULLIF(c.payload->>'ID_ENTIDADE','')::int                                   AS id_entidade,
    COALESCE(NULLIF(regexp_replace(TRIM(c.payload->>'NOME'), '[\s\-]+$', ''),''), '') AS cliente_nome,
    COALESCE(NULLIF(TRIM(c.payload->>'CPF'),''), '')                            AS cpf,
    COALESCE(NULLIF(c.payload->>'VALOR','')::numeric, 0)::numeric(18,2)         AS valor,
    NULLIF(LEFT(c.payload->>'DTACONTA',10),'')::date                            AS dt_recebido,
    NULLIF(LEFT(c.payload->>'DTABOM',10),'')::date                             AS dt_vencimento,
    NULLIF(c.payload->>'SITUACAOCHEQUE','')::int                                AS situacao_cheque,
    COALESCE(NULLIF(TRIM(c.payload->>'CODIGOBANCOSPADRAO'),''), '')             AS banco,
    COALESCE(NULLIF(TRIM(c.payload->>'AGENCIA'),''), '')                        AS agencia,
    COALESCE(NULLIF(TRIM(c.payload->>'NROCONTA'),''), '')                       AS nroconta,
    COALESCE(NULLIF(TRIM(c.payload->>'NUMERO'),''), '')                         AS numero,
    dc.dt_compensado,
    (dc.dt_vencimento IS NOT NULL AND dc.dt_recebido IS NOT NULL
       AND (dc.dt_vencimento - dc.dt_recebido) <= 2)                           AS avista,
    COALESCE(sit.motivo, '')                                                    AS motivo_devolucao,
    CASE
      WHEN COALESCE(NULLIF(c.payload->>'ID_SITUACOES','')::int, 0) >= 1
           AND dc.dt_compensado IS NULL                                         THEN 'devolvido'
      WHEN dc.dt_compensado IS NOT NULL                                         THEN 'compensado'
      WHEN NULLIF(c.payload->>'SITUACAOCHEQUE','')::int = 1                     THEN 'depositado'
      ELSE 'a_compensar'
    END                                                                         AS status_cheque,
    now()
  FROM stg.cheques c
  CROSS JOIN LATERAL (
    SELECT
      NULLIF(LEFT(c.payload->>'DTACONTA',10),'')::date AS dt_recebido,
      NULLIF(LEFT(c.payload->>'DTABOM',10),'')::date   AS dt_vencimento,
      CASE WHEN NULLIF(LEFT(c.payload->>'DTACOMPENSADO',10),'')::date >= DATE '1990-01-01'
           THEN NULLIF(LEFT(c.payload->>'DTACOMPENSADO',10),'')::date END       AS dt_compensado
  ) dc
  LEFT JOIN LATERAL (
    SELECT NULLIF(TRIM(s.payload->>'DESCRICAO'),'') AS motivo
    FROM stg.situacoes s
    WHERE s.id_empresa = c.id_empresa
      AND s.id_filial  = c.id_filial
      AND s.id_situacao = NULLIF(c.payload->>'ID_SITUACOES','')::int
    LIMIT 1
  ) sit ON true
  WHERE c.id_empresa = p_id_empresa
    AND (
      dc.dt_compensado IS NULL
      OR dc.dt_compensado >= (v_ref - INTERVAL '180 days')
    );

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION etl.refresh_cheques(integer) IS
  'Reconstroi mart.cheques_pendentes a partir de stg.cheques + stg.situacoes (motivo). Mantem nao compensados + compensados <=180d. Advisory lock por empresa.';
