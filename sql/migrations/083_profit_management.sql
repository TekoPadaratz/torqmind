-- Migration 083: Profit Management Module (Gestão de Lucro)
-- Creates STG, DW dimensions/facts, and ETL functions for the profit module.

BEGIN;

-- ??????????????????????????????????????????????????????????????????????
-- 1. STG: planodecontas
-- ??????????????????????????????????????????????????????????????????????

CREATE TABLE IF NOT EXISTS stg.planodecontas (
    id_empresa          int         NOT NULL,
    id_filial           int         NOT NULL,
    id_planodecontas    int         NOT NULL,
    payload             jsonb       NOT NULL DEFAULT '{}',
    received_at         timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_planodecontas PRIMARY KEY (id_empresa, id_filial, id_planodecontas)
);

CREATE INDEX IF NOT EXISTS idx_stg_planodecontas_filial
    ON stg.planodecontas (id_empresa, id_filial);

-- ??????????????????????????????????????????????????????????????????????
-- 2. DW: dim_plano_contas_gerencial
-- ??????????????????????????????????????????????????????????????????????

CREATE TABLE IF NOT EXISTS dw.dim_plano_contas_gerencial (
    id_empresa                  int         NOT NULL,
    id_filial                   int         NOT NULL,
    id_planodecontas            int         NOT NULL,
    codigo_plano                text,
    nome_plano                  text,
    nivel_1                     text,
    nivel_2                     text,
    nivel_3                     text,
    nivel_4                     text,
    is_conta_mae                boolean     NOT NULL DEFAULT false,
    is_conta_folha              boolean     NOT NULL DEFAULT true,
    classificacao_gerencial     text        NOT NULL DEFAULT 'nao_classificado',
    centro_custo_gerencial      text        NOT NULL DEFAULT 'geral',
    entra_dre                   boolean     NOT NULL DEFAULT true,
    entra_custo_operacional     boolean     NOT NULL DEFAULT false,
    entra_rateio_produto        boolean     NOT NULL DEFAULT false,
    regra_rateio                text,
    is_excepcional              boolean     NOT NULL DEFAULT false,
    is_financeiro               boolean     NOT NULL DEFAULT false,
    is_tributo_sobre_venda      boolean     NOT NULL DEFAULT false,
    is_tributo_operacional      boolean     NOT NULL DEFAULT false,
    is_imposto_sobre_lucro      boolean     NOT NULL DEFAULT false,
    flag_revisar                boolean     NOT NULL DEFAULT false,
    motivo_classificacao        text,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_dw_dim_plano_contas_gerencial
        PRIMARY KEY (id_empresa, id_filial, id_planodecontas)
);

-- ??????????????????????????????????????????????????????????????????????
-- 3. DW: fact_despesa_operacional
-- ??????????????????????????????????????????????????????????????????????

CREATE TABLE IF NOT EXISTS dw.fact_despesa_operacional (
    id_empresa                  int             NOT NULL,
    id_filial                   int             NOT NULL,
    id_db_origem                int             NOT NULL DEFAULT 0,
    id_contaspagar              int             NOT NULL,
    id_entidade                 int,
    id_planodecontas            int,
    dt_vencimento               date,
    dt_emissao                  date,
    dt_pagamento                date,
    ano_mes_competencia         int             NOT NULL,  -- YYYYMM
    valor                       numeric(18,2)   NOT NULL DEFAULT 0,
    juros                       numeric(18,2)   NOT NULL DEFAULT 0,
    desconto                    numeric(18,2)   NOT NULL DEFAULT 0,
    vlr_pago                    numeric(18,2)   NOT NULL DEFAULT 0,
    tipo_conta                  smallint        NOT NULL DEFAULT 0,
    origem_caixa                boolean         NOT NULL DEFAULT false,
    historico                   text,
    documento                   text,
    codigo_plano                text,
    nome_plano                  text,
    classificacao_gerencial     text            NOT NULL DEFAULT 'nao_classificado',
    centro_custo_gerencial      text            NOT NULL DEFAULT 'geral',
    entra_dre                   boolean         NOT NULL DEFAULT true,
    entra_custo_operacional     boolean         NOT NULL DEFAULT false,
    entra_rateio_produto        boolean         NOT NULL DEFAULT false,
    regra_rateio                text,
    is_excepcional              boolean         NOT NULL DEFAULT false,
    is_financeiro               boolean         NOT NULL DEFAULT false,
    is_despesa_operacional      boolean         NOT NULL DEFAULT false,
    is_tributo_sobre_venda      boolean         NOT NULL DEFAULT false,
    is_tributo_operacional      boolean         NOT NULL DEFAULT false,
    is_imposto_sobre_lucro      boolean         NOT NULL DEFAULT false,
    flag_revisar                boolean         NOT NULL DEFAULT false,
    created_at                  timestamptz     NOT NULL DEFAULT now(),
    updated_at                  timestamptz     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dw_fact_despesa_operacional
        PRIMARY KEY (id_empresa, id_filial, id_contaspagar, id_db_origem)
);

CREATE INDEX IF NOT EXISTS idx_fact_despesa_op_competencia
    ON dw.fact_despesa_operacional (id_empresa, id_filial, ano_mes_competencia);

CREATE INDEX IF NOT EXISTS idx_fact_despesa_op_classificacao
    ON dw.fact_despesa_operacional (id_empresa, id_filial, classificacao_gerencial, ano_mes_competencia);

-- ??????????????????????????????????????????????????????????????????????
-- 4. ETL: load_dim_plano_contas_gerencial
-- ??????????????????????????????????????????????????????????????????????

CREATE OR REPLACE FUNCTION etl.load_dim_plano_contas_gerencial(
    p_id_empresa int DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO dw.dim_plano_contas_gerencial (
        id_empresa, id_filial, id_planodecontas,
        codigo_plano, nome_plano,
        nivel_1, nivel_2, nivel_3, nivel_4,
        is_conta_mae, is_conta_folha,
        classificacao_gerencial, centro_custo_gerencial,
        entra_dre, entra_custo_operacional, entra_rateio_produto,
        regra_rateio, is_excepcional, is_financeiro,
        is_tributo_sobre_venda, is_tributo_operacional, is_imposto_sobre_lucro,
        flag_revisar, motivo_classificacao, updated_at
    )
    SELECT
        s.id_empresa,
        s.id_filial,
        s.id_planodecontas,
        COALESCE(s.payload->>'CODIGOPLANODECONTAS', '')::text AS codigo_plano,
        COALESCE(s.payload->>'NOMEPLANODECONTAS', '')::text AS nome_plano,
        -- Hierarchy levels by code prefix
        SUBSTRING(COALESCE(s.payload->>'CODIGOPLANODECONTAS',''), 1, 1) AS nivel_1,
        SUBSTRING(COALESCE(s.payload->>'CODIGOPLANODECONTAS',''), 1, 3) AS nivel_2,
        SUBSTRING(COALESCE(s.payload->>'CODIGOPLANODECONTAS',''), 1, 6) AS nivel_3,
        COALESCE(s.payload->>'CODIGOPLANODECONTAS', '')::text AS nivel_4,
        -- Conta mae flag
        COALESCE((s.payload->>'CONTAMAE')::boolean, false) AS is_conta_mae,
        NOT COALESCE((s.payload->>'CONTAMAE')::boolean, false) AS is_conta_folha,
        -- Classification rules
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN 'pessoal'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN 'comercial'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN 'administrativo'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' THEN 'financeiro'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN 'tributos'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.09%' THEN 'perdas'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2%' THEN 'nao_classificado'
            ELSE 'outros'
        END AS classificacao_gerencial,
        -- Centro de custo gerencial
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN 'geral'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN 'comercial'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN 'administrativo'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' THEN 'financeiro'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN 'tributos'
            ELSE 'geral'
        END AS centro_custo_gerencial,
        -- Entra DRE: everything 3.x
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3%' AS entra_dre,
        -- Entra custo operacional
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04.12' THEN true  -- tarifa bancaria
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.07' THEN true  -- IPTU
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.09' THEN true  -- IPVA
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.11' THEN true  -- Taxas Municipais
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.13' THEN true  -- INMETRO
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.14' THEN true  -- IBAMA
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.09%' THEN true   -- perdas
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2%' THEN true      -- fallback 3.2
            ELSE false
        END AS entra_custo_operacional,
        -- Entra rateio produto
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') != '3.2.01.05' THEN true  -- pessoal exceto rescisao
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') != '3.2.02.23' THEN true  -- comercial exceto cortesia
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN true      -- administrativo
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.07' THEN true    -- IPTU
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.09' THEN true    -- IPVA
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.11' THEN true    -- Taxas
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.13' THEN true    -- INMETRO
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.14' THEN true    -- IBAMA
            ELSE false
        END AS entra_rateio_produto,
        -- Regra rateio
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN 'proporcional_receita_setor'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02.07' THEN 'proporcional_receita_cartao'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02.09' THEN 'atribuivel_setor'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN 'proporcional_receita'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN 'proporcional_receita'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN 'proporcional_receita'
            ELSE NULL
        END AS regra_rateio,
        -- is_excepcional
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') IN ('3.2.01.05', '3.2.04.29') AS is_excepcional,
        -- is_financeiro
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' AS is_financeiro,
        -- is_tributo_sobre_venda
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') IN ('3.2.05.01', '3.2.05.02') AS is_tributo_sobre_venda,
        -- is_tributo_operacional
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') IN ('3.2.05.07','3.2.05.09','3.2.05.11','3.2.05.13','3.2.05.14') AS is_tributo_operacional,
        -- is_imposto_sobre_lucro
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') = '3.2.05.05' AS is_imposto_sobre_lucro,
        -- flag_revisar: unknown accounts under 3.2
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.01%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.02%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.03%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.04%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.05%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.09%'
            THEN true
            ELSE false
        END AS flag_revisar,
        NULL AS motivo_classificacao,
        now() AS updated_at
    FROM stg.planodecontas s
    WHERE (p_id_empresa IS NULL OR s.id_empresa = p_id_empresa)
      AND NOT COALESCE((s.payload->>'CONTAMAE')::boolean, false)  -- Only leaf accounts
    ON CONFLICT (id_empresa, id_filial, id_planodecontas) DO UPDATE SET
        codigo_plano = EXCLUDED.codigo_plano,
        nome_plano = EXCLUDED.nome_plano,
        nivel_1 = EXCLUDED.nivel_1,
        nivel_2 = EXCLUDED.nivel_2,
        nivel_3 = EXCLUDED.nivel_3,
        nivel_4 = EXCLUDED.nivel_4,
        is_conta_mae = EXCLUDED.is_conta_mae,
        is_conta_folha = EXCLUDED.is_conta_folha,
        classificacao_gerencial = EXCLUDED.classificacao_gerencial,
        centro_custo_gerencial = EXCLUDED.centro_custo_gerencial,
        entra_dre = EXCLUDED.entra_dre,
        entra_custo_operacional = EXCLUDED.entra_custo_operacional,
        entra_rateio_produto = EXCLUDED.entra_rateio_produto,
        regra_rateio = EXCLUDED.regra_rateio,
        is_excepcional = EXCLUDED.is_excepcional,
        is_financeiro = EXCLUDED.is_financeiro,
        is_tributo_sobre_venda = EXCLUDED.is_tributo_sobre_venda,
        is_tributo_operacional = EXCLUDED.is_tributo_operacional,
        is_imposto_sobre_lucro = EXCLUDED.is_imposto_sobre_lucro,
        flag_revisar = EXCLUDED.flag_revisar,
        motivo_classificacao = EXCLUDED.motivo_classificacao,
        updated_at = EXCLUDED.updated_at;
END;
$$;

-- ??????????????????????????????????????????????????????????????????????
-- 5. ETL: load_fact_despesa_operacional
-- ??????????????????????????????????????????????????????????????????????

CREATE OR REPLACE FUNCTION etl.load_fact_despesa_operacional(
    p_id_empresa int DEFAULT NULL,
    p_from_date  date DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_from date;
BEGIN
    v_from := COALESCE(p_from_date, '2024-01-01'::date);

    -- Delete and reinsert for idempotency within window
    DELETE FROM dw.fact_despesa_operacional
    WHERE (p_id_empresa IS NULL OR id_empresa = p_id_empresa)
      AND dt_vencimento >= v_from;

    INSERT INTO dw.fact_despesa_operacional (
        id_empresa, id_filial, id_db_origem, id_contaspagar,
        id_entidade, id_planodecontas,
        dt_vencimento, dt_emissao, dt_pagamento,
        ano_mes_competencia,
        valor, juros, desconto, vlr_pago,
        tipo_conta, origem_caixa,
        historico, documento,
        codigo_plano, nome_plano,
        classificacao_gerencial, centro_custo_gerencial,
        entra_dre, entra_custo_operacional, entra_rateio_produto,
        regra_rateio, is_excepcional, is_financeiro, is_despesa_operacional,
        is_tributo_sobre_venda, is_tributo_operacional, is_imposto_sobre_lucro,
        flag_revisar
    )
    SELECT
        cp.id_empresa,
        cp.id_filial,
        COALESCE((cp.payload->>'ID_DB')::int, 0) AS id_db_origem,
        (cp.payload->>'ID_CONTASPAGAR')::int AS id_contaspagar,
        (cp.payload->>'ID_ENTIDADE')::int AS id_entidade,
        (cp.payload->>'ID_PLANODECONTAS')::int AS id_planodecontas,
        (cp.payload->>'DTAVCTO')::date AS dt_vencimento,
        (cp.payload->>'DTACONTA')::date AS dt_emissao,
        (cp.payload->>'DTAPGTO')::date AS dt_pagamento,
        EXTRACT(YEAR FROM (cp.payload->>'DTAVCTO')::date)::int * 100
            + EXTRACT(MONTH FROM (cp.payload->>'DTAVCTO')::date)::int AS ano_mes_competencia,
        COALESCE((cp.payload->>'VALOR')::numeric, 0) AS valor,
        COALESCE((cp.payload->>'JUROS')::numeric, 0) AS juros,
        COALESCE((cp.payload->>'DESCONTO')::numeric, 0) AS desconto,
        COALESCE((cp.payload->>'VLRPAGO')::numeric, 0) AS vlr_pago,
        COALESCE((cp.payload->>'TIPO_CONTA')::smallint, 0) AS tipo_conta,
        COALESCE((cp.payload->>'TIPO_CONTA')::smallint, 0) = 1 AS origem_caixa,
        cp.payload->>'HISTORICO' AS historico,
        cp.payload->>'DOCUMENTO' AS documento,
        COALESCE(d.codigo_plano, '') AS codigo_plano,
        COALESCE(d.nome_plano, '') AS nome_plano,
        COALESCE(d.classificacao_gerencial, 'nao_classificado') AS classificacao_gerencial,
        COALESCE(d.centro_custo_gerencial, 'geral') AS centro_custo_gerencial,
        COALESCE(d.entra_dre, true) AS entra_dre,
        COALESCE(d.entra_custo_operacional, false) AS entra_custo_operacional,
        COALESCE(d.entra_rateio_produto, false) AS entra_rateio_produto,
        d.regra_rateio,
        COALESCE(d.is_excepcional, false) AS is_excepcional,
        COALESCE(d.is_financeiro, false) AS is_financeiro,
        -- is_despesa_operacional = entra_custo_operacional AND NOT financeiro/excepcional
        COALESCE(d.entra_custo_operacional, false)
            AND NOT COALESCE(d.is_financeiro, false)
            AND NOT COALESCE(d.is_excepcional, false) AS is_despesa_operacional,
        COALESCE(d.is_tributo_sobre_venda, false) AS is_tributo_sobre_venda,
        COALESCE(d.is_tributo_operacional, false) AS is_tributo_operacional,
        COALESCE(d.is_imposto_sobre_lucro, false) AS is_imposto_sobre_lucro,
        COALESCE(d.flag_revisar, false) AS flag_revisar
    FROM stg.contaspagar cp
    LEFT JOIN dw.dim_plano_contas_gerencial d
        ON d.id_empresa = cp.id_empresa
       AND d.id_filial = cp.id_filial
       AND d.id_planodecontas = (cp.payload->>'ID_PLANODECONTAS')::int
    WHERE (p_id_empresa IS NULL OR cp.id_empresa = p_id_empresa)
      AND (cp.payload->>'DTAVCTO') IS NOT NULL
      AND (cp.payload->>'DTAVCTO')::date >= v_from
      -- Include only expense accounts (prefix 3)
      AND COALESCE(d.codigo_plano, cp.payload->>'CODIGOPLANODECONTAS', '3') LIKE '3%';
END;
$$;

COMMIT;
