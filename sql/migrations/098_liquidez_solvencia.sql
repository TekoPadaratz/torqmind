-- Migration 098: Solvencia / Capital de Giro (DRE Gerencial afinado)
-- Idempotente. Nao destrutivo. NAO altera o DRE existente (profit_dre_mensal).
--
-- Objetivo (linguagem do cliente): "meus ativos cobrem meu passivo?" — cruzar o
-- que o posto tem disponivel (caixa, banco, cartoes/cheques a compensar) e em
-- estoque com as contas a pagar que vencem no mes-alvo, para saber se aguenta o
-- mes sem depender de vender. Aba "Solvencia" no DRE Gerencial, com filtro de mes.
--
-- Conceito contabil (padrao BR):
--   Ativo Circulante   = Disponivel (caixa + banco) + Realizavel CP (cartoes +
--                        cheques) + Estoque (a custo)
--   Passivo Circulante = contas a pagar em ABERTO vencendo no mes-alvo
--   Indice de Liquidez Corrente = Ativo Circulante / Passivo Circulante
--   Capital de Giro Liquido     = Ativo Circulante - Passivo Circulante
--   (Liquidez >= 1  => os ativos cobrem o passivo do mes.)
--
-- Fonte do PASSIVO (disponivel hoje): stg.contaspagar, titulos em ABERTO
--   (DTAPGTO nulo), agrupados por mes de vencimento (DTAVCTO). Valor em aberto =
--   VALOR - COALESCE(VLRPAGO, 0). Lido DIRETO da STG de proposito: a
--   dw.fact_financeiro nao e confiavel para "pagar" (o watermark 'financeiro'
--   compartilhado com contas a receber avanca e pula os titulos a pagar). A
--   gestao orcamentaria (097) e a despesa operacional (083) tambem leem
--   contaspagar direto da STG pelo mesmo motivo.
--
-- Os ATIVOS (caixa/banco/cartoes/cheques/estoque) sao preenchidos por ETLs das
--   fases seguintes conforme a coleta via Agent for habilitada; as colunas ja
--   nascem aqui (zeradas) para nao quebrar o contrato da mart/tela. A flag
--   tem_ativo_dados distingue "ativo = 0 real" de "ativo ainda nao coletado",
--   evitando exibir "nao cobre" enganoso.

-- 1) Mart de solvencia por filial e mes (camada rapida da aba "Solvencia").
CREATE TABLE IF NOT EXISTS mart.liquidez_solvencia (
  id_empresa            integer NOT NULL,
  id_filial             integer NOT NULL,
  ano_mes               integer NOT NULL,            -- YYYYMM do mes-alvo (vencimento)

  -- Passivo circulante (contas a pagar em aberto vencendo no mes)
  passivo_contas_pagar  numeric(18,2) NOT NULL DEFAULT 0,
  passivo_qtd_titulos   integer       NOT NULL DEFAULT 0,
  passivo_vencido       numeric(18,2) NOT NULL DEFAULT 0,   -- ja vencido (DTAVCTO < hoje) e ainda aberto

  -- Ativo circulante (componentes; preenchidos por fase)
  ativo_caixa           numeric(18,2) NOT NULL DEFAULT 0,   -- dinheiro em caixa
  ativo_banco           numeric(18,2) NOT NULL DEFAULT 0,   -- saldo em bancos
  ativo_cartoes         numeric(18,2) NOT NULL DEFAULT 0,   -- cartoes a compensar (curto prazo)
  ativo_cheques         numeric(18,2) NOT NULL DEFAULT 0,   -- cheques a receber
  ativo_estoque         numeric(18,2) NOT NULL DEFAULT 0,   -- estoque valorizado a custo

  -- Indica se algum componente de ativo ja foi coletado (fonte disponivel)
  tem_ativo_dados       boolean       NOT NULL DEFAULT false,

  -- Derivados (colunas geradas: sempre consistentes com os componentes)
  ativo_disponivel numeric(18,2)
    GENERATED ALWAYS AS (ativo_caixa + ativo_banco) STORED,
  ativo_circulante numeric(18,2)
    GENERATED ALWAYS AS (ativo_caixa + ativo_banco + ativo_cartoes + ativo_cheques + ativo_estoque) STORED,
  capital_giro_liquido numeric(18,2)
    GENERATED ALWAYS AS ((ativo_caixa + ativo_banco + ativo_cartoes + ativo_cheques + ativo_estoque) - passivo_contas_pagar) STORED,
  liquidez_corrente numeric(12,4)
    GENERATED ALWAYS AS (
      CASE WHEN passivo_contas_pagar > 0
           THEN ((ativo_caixa + ativo_banco + ativo_cartoes + ativo_cheques + ativo_estoque) / passivo_contas_pagar)
           ELSE 0 END
    ) STORED,
  cobre_passivo boolean
    GENERATED ALWAYS AS (
      (ativo_caixa + ativo_banco + ativo_cartoes + ativo_cheques + ativo_estoque) >= passivo_contas_pagar
    ) STORED,

  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, ano_mes)
);

CREATE INDEX IF NOT EXISTS ix_liquidez_solvencia_scope
  ON mart.liquidez_solvencia (id_empresa, ano_mes, id_filial);

COMMENT ON TABLE mart.liquidez_solvencia IS
  'Solvencia de curto prazo por filial e mes: ativo circulante (caixa/banco/cartoes/cheques/estoque) x passivo (contas a pagar vencendo no mes). Indice de liquidez corrente e capital de giro liquido. Camada rapida da aba Solvencia do DRE Gerencial. Derivados sao colunas geradas; componentes preenchidos por ETLs (passivo=etl.refresh_liquidez_solvencia; ativos por fase).';

-- 2) ETL do PASSIVO: contas a pagar em aberto por mes de vencimento.
--    Materializa apenas o horizonte relevante (-3 a +18 meses) para nao poluir
--    a mart com provisoes de despesa fixa lancadas anos a frente.
--    Faz UPSERT so dos campos de passivo (preserva ativos ja carregados) e zera
--    passivo de meses cujos titulos foram todos pagos (reconciliacao).
--
--    VALOR EM ABERTO desconta BAIXAS PARCIAIS: valor_aberto =
--    GREATEST(VALOR - VLRPAGO - SUM(VALORBAIXA), 0). Titulos com DTAPGTO nulo
--    podem ter VLRPAGO=0 e ja terem baixas parciais em stg.contaspagarbaixa
--    (validado na fonte: VALOR = VLRPAGO + SUM(VALORBAIXA), sem sobreposicao).
--    Join por (id_empresa, id_db, ID_CONTASPAGAR) — ID_CONTASPAGAR NAO e unico
--    global, so por id_db. Sem descontar a baixa o passivo fica superestimado.
CREATE OR REPLACE FUNCTION etl.refresh_liquidez_solvencia(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows   integer := 0;
  v_ref    date    := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
  v_min    date    := (date_trunc('month', (now() AT TIME ZONE 'America/Sao_Paulo')::date) - interval '3 months')::date;
  v_max    date    := (date_trunc('month', (now() AT TIME ZONE 'America/Sao_Paulo')::date) + interval '18 months')::date;
  v_min_ym integer := (EXTRACT(YEAR FROM v_min)::int * 100 + EXTRACT(MONTH FROM v_min)::int);
  v_max_ym integer := (EXTRACT(YEAR FROM v_max)::int * 100 + EXTRACT(MONTH FROM v_max)::int);
BEGIN
  WITH baixas AS (
    SELECT
      b.id_empresa,
      b.id_db,
      (b.payload->>'ID_CONTASPAGAR')::bigint AS id_contaspagar,
      SUM(etl.safe_numeric(b.payload->>'VALORBAIXA'))::numeric(18,2) AS total_baixa
    FROM stg.contaspagarbaixa b
    WHERE b.id_empresa = p_id_empresa
      AND b.payload->>'ID_CONTASPAGAR' IS NOT NULL
    GROUP BY b.id_empresa, b.id_db, (b.payload->>'ID_CONTASPAGAR')::bigint
  ),
  titulos AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      (etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) / 100) AS ano_mes,
      (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date AS vencimento,
      GREATEST(
        etl.safe_numeric(p.payload->>'VALOR')
          - COALESCE(etl.safe_numeric(p.payload->>'VLRPAGO'), 0)
          - COALESCE(bx.total_baixa, 0), 0)::numeric(18,2) AS valor_aberto
    FROM stg.contaspagar p
    LEFT JOIN baixas bx
      ON bx.id_empresa = p.id_empresa
     AND bx.id_db = p.id_db
     AND bx.id_contaspagar = p.id_contaspagar
    WHERE p.id_empresa = p_id_empresa
      AND p.payload->>'DTAPGTO' IS NULL
      AND etl.safe_timestamp(p.payload->>'DTAVCTO') IS NOT NULL
      AND (etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) / 100) BETWEEN v_min_ym AND v_max_ym
  ),
  passivo AS (
    SELECT
      id_empresa,
      id_filial,
      ano_mes,
      SUM(valor_aberto)::numeric(18,2) AS passivo_contas_pagar,
      COUNT(*) FILTER (WHERE valor_aberto > 0)::int AS passivo_qtd_titulos,
      SUM(CASE WHEN vencimento < v_ref THEN valor_aberto ELSE 0 END)::numeric(18,2) AS passivo_vencido
    FROM titulos
    GROUP BY id_empresa, id_filial, ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS m
    (id_empresa, id_filial, ano_mes, passivo_contas_pagar, passivo_qtd_titulos, passivo_vencido, updated_at)
  SELECT id_empresa, id_filial, ano_mes, passivo_contas_pagar, passivo_qtd_titulos, passivo_vencido, now()
  FROM passivo
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE SET
    passivo_contas_pagar = EXCLUDED.passivo_contas_pagar,
    passivo_qtd_titulos  = EXCLUDED.passivo_qtd_titulos,
    passivo_vencido      = EXCLUDED.passivo_vencido,
    updated_at           = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  -- Reconciliacao: zera passivo de meses no horizonte que nao tem mais titulo
  -- aberto (todos pagos), sem apagar os ativos ja carregados na linha.
  UPDATE mart.liquidez_solvencia m
  SET passivo_contas_pagar = 0,
      passivo_qtd_titulos  = 0,
      passivo_vencido      = 0,
      updated_at           = now()
  WHERE m.id_empresa = p_id_empresa
    AND m.ano_mes BETWEEN v_min_ym AND v_max_ym
    AND (m.passivo_contas_pagar > 0 OR m.passivo_qtd_titulos > 0)
    AND NOT EXISTS (
      SELECT 1 FROM stg.contaspagar p
      WHERE p.id_empresa = m.id_empresa
        AND p.id_filial = m.id_filial
        AND p.payload->>'DTAPGTO' IS NULL
        AND etl.safe_timestamp(p.payload->>'DTAVCTO') IS NOT NULL
        AND (etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) / 100) = m.ano_mes
    );

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION etl.refresh_liquidez_solvencia(integer) IS
  'Popula o PASSIVO (contas a pagar em aberto por mes de vencimento) da mart.liquidez_solvencia, horizonte -3..+18 meses. UPSERT preserva os componentes de ativo; reconcilia meses quitados. Ativos sao preenchidos por ETLs de fases seguintes.';
