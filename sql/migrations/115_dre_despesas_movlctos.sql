-- Migration 115: DRE despesas canônicas via MOVLCTOS + DTACONTA (nível 3)
--
-- Referência: docs/product/dre_referencia_xpert.md (VR01 jun/2026)
-- Descoberta: o Demonstrativo Xpert NÃO usa CONTASPAGAR/DTAVCTO.
-- Fonte: dbo.MOVLCTOS.DTACONTA; TIPO 0/2 = débito (+), TIPO 1 = crédito (−).
-- Inclui ESTORNO (o relatório soma esses lançamentos).
-- Totalização gerencial: prefixo nível 3 (3.2.01, 3.2.02, … / 3.3).

COMMENT ON COLUMN dw.fact_despesa_operacional.id_contaspagar IS
  'ID natural do lançamento de origem. A partir da 115 = ID_MOVLCTOS (não CONTASPAGAR).';

COMMENT ON COLUMN dw.fact_despesa_operacional.dt_vencimento IS
  'Data de competência DRE = MOVLCTOS.DTACONTA (não DTAVCTO de contas a pagar).';

COMMENT ON COLUMN dw.fact_despesa_operacional.ano_mes_competencia IS
  'YYYYMM de MOVLCTOS.DTACONTA — alinhado ao Demonstrativo de Resultado Xpert.';

CREATE OR REPLACE FUNCTION etl.load_fact_despesa_operacional(
    p_id_empresa integer DEFAULT NULL::integer,
    p_from_date date DEFAULT NULL::date
)
 RETURNS void
 LANGUAGE plpgsql
 SET statement_timeout TO '900s'
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
        m.id_empresa,
        m.id_filial,
        COALESCE(etl.safe_int(m.payload->>'ID_DB'), m.id_db, 0) AS id_db_origem,
        -- PK legado: grava ID_MOVLCTOS neste campo (ver COMMENT).
        etl.safe_int(m.payload->>'ID_MOVLCTOS') AS id_contaspagar,
        NULL::int AS id_entidade,
        etl.safe_int(m.payload->>'ID_PLANODECONTAS') AS id_planodecontas,
        (m.payload->>'DTACONTA')::date AS dt_vencimento,
        (m.payload->>'DTACONTA')::date AS dt_emissao,
        (m.payload->>'DTACONTA')::date AS dt_pagamento,
        EXTRACT(YEAR FROM (m.payload->>'DTACONTA')::date)::int * 100
            + EXTRACT(MONTH FROM (m.payload->>'DTACONTA')::date)::int AS ano_mes_competencia,
        (
          CASE
            WHEN COALESCE(etl.safe_int(m.payload->>'TIPO'), 0) = 1
              THEN -COALESCE(etl.safe_numeric(m.payload->>'VALOR'), 0)
            ELSE COALESCE(etl.safe_numeric(m.payload->>'VALOR'), 0)
          END
        ) AS valor,
        0::numeric AS juros,
        0::numeric AS desconto,
        (
          CASE
            WHEN COALESCE(etl.safe_int(m.payload->>'TIPO'), 0) = 1
              THEN -COALESCE(etl.safe_numeric(m.payload->>'VALOR'), 0)
            ELSE COALESCE(etl.safe_numeric(m.payload->>'VALOR'), 0)
          END
        ) AS vlr_pago,
        COALESCE(etl.safe_int(m.payload->>'TIPO'), 0)::smallint AS tipo_conta,
        COALESCE((m.payload->>'CAIXAGERAL')::boolean, false) AS origem_caixa,
        NULLIF(m.payload->>'DOCUMENTO', '') AS historico,
        NULLIF(m.payload->>'DOCUMENTO', '') AS documento,
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
    FROM stg.movlctos m
    INNER JOIN dw.dim_plano_contas_gerencial d
        ON d.id_empresa = m.id_empresa
       AND d.id_filial = m.id_filial
       AND d.id_planodecontas = etl.safe_int(m.payload->>'ID_PLANODECONTAS')
    WHERE (p_id_empresa IS NULL OR m.id_empresa = p_id_empresa)
      AND (m.payload->>'DTACONTA') IS NOT NULL
      AND (m.payload->>'DTACONTA')::date >= v_from
      AND COALESCE(d.entra_dre, false) = true
      AND etl.safe_int(m.payload->>'ID_MOVLCTOS') IS NOT NULL;
END;
$function$;

COMMENT ON FUNCTION etl.load_fact_despesa_operacional(integer, date) IS
  'Fact DRE a partir de stg.movlctos × dim_plano (DTACONTA; TIPO1 negativo). Nível 3 via classificacao_gerencial.';
