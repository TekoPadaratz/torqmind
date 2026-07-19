-- Migration 114: DRE despesas alinhadas ao plano de contas Xpert (3.2*/3.3*)
--
-- Causa: stg.planodecontas vazio ⇒ dim_plano_contas_gerencial vazia ⇒ fact
-- com classificacao_gerencial='nao_classificado' e inclusão indevida de 1.*/3.1.*.
--
-- Fonte canônica: docs/product/XPERT_DRE_DESPESAS_MAP.md

CREATE OR REPLACE FUNCTION etl.load_dim_plano_contas_gerencial(p_id_empresa integer DEFAULT NULL::integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
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
        SUBSTRING(COALESCE(s.payload->>'CODIGOPLANODECONTAS',''), 1, 1) AS nivel_1,
        SUBSTRING(COALESCE(s.payload->>'CODIGOPLANODECONTAS',''), 1, 3) AS nivel_2,
        SUBSTRING(COALESCE(s.payload->>'CODIGOPLANODECONTAS',''), 1, 6) AS nivel_3,
        COALESCE(s.payload->>'CODIGOPLANODECONTAS', '')::text AS nivel_4,
        COALESCE(
          NULLIF(lower(COALESCE(s.payload->>'CONTAMAE','')), '') IN ('1','true','t','yes','on'),
          false
        ) AS is_conta_mae,
        NOT COALESCE(
          NULLIF(lower(COALESCE(s.payload->>'CONTAMAE','')), '') IN ('1','true','t','yes','on'),
          false
        ) AS is_conta_folha,
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN 'pessoal'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN 'comercial'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN 'administrativo'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' THEN 'financeiro'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN 'tributos'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.07%' THEN 'administrativo'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.08%' THEN 'comercial'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.09%' THEN 'excepcional'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.3%' THEN 'excepcional'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2%' THEN 'nao_classificado'
            ELSE 'outros'
        END AS classificacao_gerencial,
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN 'geral'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN 'comercial'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN 'administrativo'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.07%' THEN 'administrativo'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' THEN 'financeiro'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN 'tributos'
            ELSE 'geral'
        END AS centro_custo_gerencial,
        -- Só despesas operacionais / não operacionais (nunca CMV 3.1 nem ativo/passivo)
        (
          COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2%'
          OR COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.3%'
        ) AS entra_dre,
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.07%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.08%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.09%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.3%' THEN true
            ELSE false
        END AS entra_custo_operacional,
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') <> '3.2.01.05' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') <> '3.2.02.23' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.07%' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.07' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.09' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.11' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.13' THEN true
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05.14' THEN true
            ELSE false
        END AS entra_rateio_produto,
        CASE
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.01%' THEN 'proporcional_receita_setor'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02.07%' THEN 'proporcional_receita_cartao'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02.08%' THEN 'proporcional_receita_cartao'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02.09%' THEN 'atribuivel_setor'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.02%' THEN 'proporcional_receita'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.03%' THEN 'proporcional_receita'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.07%' THEN 'proporcional_receita'
            WHEN COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%' THEN 'proporcional_receita'
            ELSE NULL
        END AS regra_rateio,
        (
          COALESCE(s.payload->>'CODIGOPLANODECONTAS','') IN ('3.2.01.05', '3.2.04.29')
          OR COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.09%'
          OR COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.3%'
        ) AS is_excepcional,
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.04%' AS is_financeiro,
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') IN ('3.2.05.01', '3.2.05.02') AS is_tributo_sobre_venda,
        (
          COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2.05%'
          AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT IN ('3.2.05.01', '3.2.05.02', '3.2.05.05')
        ) AS is_tributo_operacional,
        COALESCE(s.payload->>'CODIGOPLANODECONTAS','') = '3.2.05.05' AS is_imposto_sobre_lucro,
        CASE
            WHEN (
              COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.2%'
              OR COALESCE(s.payload->>'CODIGOPLANODECONTAS','') LIKE '3.3%'
            )
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.01%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.02%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.03%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.04%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.05%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.07%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.08%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.2.09%'
                 AND COALESCE(s.payload->>'CODIGOPLANODECONTAS','') NOT LIKE '3.3%'
            THEN true
            ELSE false
        END AS flag_revisar,
        'xpert_plano_3.2_3.3'::text AS motivo_classificacao,
        now() AS updated_at
    FROM stg.planodecontas s
    WHERE (p_id_empresa IS NULL OR s.id_empresa = p_id_empresa)
      AND NOT COALESCE(
        NULLIF(lower(COALESCE(s.payload->>'CONTAMAE','')), '') IN ('1','true','t','yes','on'),
        false
      )
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
$function$;

COMMENT ON FUNCTION etl.load_dim_plano_contas_gerencial(integer) IS
  'Classifica folhas do plano Xpert (stg.planodecontas) por CODIGOPLANODECONTAS 3.2*/3.3* → buckets DRE.';

CREATE OR REPLACE FUNCTION etl.load_fact_despesa_operacional(p_id_empresa integer DEFAULT NULL::integer, p_from_date date DEFAULT NULL::date)
 RETURNS void
 LANGUAGE plpgsql
 SET statement_timeout TO '600s'
AS $function$
DECLARE
    v_from date;
BEGIN
    v_from := COALESCE(p_from_date, '2024-01-01'::date);

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
        COALESCE(etl.safe_int(cp.payload->>'ID_DB'), 0) AS id_db_origem,
        etl.safe_int(cp.payload->>'ID_CONTASPAGAR') AS id_contaspagar,
        etl.safe_int(cp.payload->>'ID_ENTIDADE') AS id_entidade,
        etl.safe_int(cp.payload->>'ID_PLANODECONTAS') AS id_planodecontas,
        (cp.payload->>'DTAVCTO')::date AS dt_vencimento,
        (cp.payload->>'DTACONTA')::date AS dt_emissao,
        (cp.payload->>'DTAPGTO')::date AS dt_pagamento,
        EXTRACT(YEAR FROM (cp.payload->>'DTAVCTO')::date)::int * 100
            + EXTRACT(MONTH FROM (cp.payload->>'DTAVCTO')::date)::int AS ano_mes_competencia,
        COALESCE(etl.safe_numeric(cp.payload->>'VALOR'), 0) AS valor,
        COALESCE(etl.safe_numeric(cp.payload->>'JUROS'), 0) AS juros,
        COALESCE(etl.safe_numeric(cp.payload->>'DESCONTO'), 0) AS desconto,
        COALESCE(etl.safe_numeric(cp.payload->>'VLRPAGO'), 0) AS vlr_pago,
        COALESCE(etl.safe_int(cp.payload->>'TIPO_CONTA'), 0)::smallint AS tipo_conta,
        COALESCE(etl.safe_int(cp.payload->>'TIPO_CONTA'), 0) = 1 AS origem_caixa,
        cp.payload->>'HISTORICO' AS historico,
        cp.payload->>'DOCUMENTO' AS documento,
        COALESCE(d.codigo_plano, '') AS codigo_plano,
        COALESCE(d.nome_plano, '') AS nome_plano,
        COALESCE(d.classificacao_gerencial, 'nao_classificado') AS classificacao_gerencial,
        COALESCE(d.centro_custo_gerencial, 'geral') AS centro_custo_gerencial,
        COALESCE(d.entra_dre, false) AS entra_dre,
        COALESCE(d.entra_custo_operacional, false) AS entra_custo_operacional,
        COALESCE(d.entra_rateio_produto, false) AS entra_rateio_produto,
        d.regra_rateio,
        COALESCE(d.is_excepcional, false) AS is_excepcional,
        COALESCE(d.is_financeiro, false) AS is_financeiro,
        COALESCE(d.entra_custo_operacional, false)
            AND NOT COALESCE(d.is_financeiro, false)
            AND NOT COALESCE(d.is_excepcional, false) AS is_despesa_operacional,
        COALESCE(d.is_tributo_sobre_venda, false) AS is_tributo_sobre_venda,
        COALESCE(d.is_tributo_operacional, false) AS is_tributo_operacional,
        COALESCE(d.is_imposto_sobre_lucro, false) AS is_imposto_sobre_lucro,
        COALESCE(d.flag_revisar, false) AS flag_revisar
    FROM stg.contaspagar cp
    INNER JOIN dw.dim_plano_contas_gerencial d
        ON d.id_empresa = cp.id_empresa
       AND d.id_filial = cp.id_filial
       AND d.id_planodecontas = etl.safe_int(cp.payload->>'ID_PLANODECONTAS')
    WHERE (p_id_empresa IS NULL OR cp.id_empresa = p_id_empresa)
      AND (cp.payload->>'DTAVCTO') IS NOT NULL
      AND (cp.payload->>'DTAVCTO')::date >= v_from
      AND COALESCE(d.entra_dre, false) = true
      AND etl.safe_int(cp.payload->>'ID_CONTASPAGAR') IS NOT NULL;
END;
$function$;

COMMENT ON FUNCTION etl.load_fact_despesa_operacional(integer, date) IS
  'Fact de despesa DRE a partir de stg.contaspagar × dim_plano_contas_gerencial (só entra_dre 3.2*/3.3*).';
