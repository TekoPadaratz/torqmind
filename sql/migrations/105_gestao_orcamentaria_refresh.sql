-- Migration 105: popular as marts de Gestao Orcamentaria (097).
-- Idempotente. Nao destrutivo (recria funcao + reconstroi as marts derivadas).
--
-- Problema: a migration 097 criou mart.plano_contas_gerencial e
-- mart.despesa_conta_mensal, e o repo budget_overview LE dessas marts, mas
-- nenhum ETL as populava. Resultado: tela de Gestao Orcamentaria mostrava
-- "realizado" sempre 0 e o config nao listava contas.
--
-- Fonte canonica: dw.fact_despesa_operacional (carregada de dbo.CONTASPAGAR por
-- etl.load_fact_despesa_operacional). Agregacao por conta gerencial + mes de
-- competencia (ano_mes_competencia = YYYYMM), espelhando profit_despesas_mensal.

CREATE OR REPLACE FUNCTION etl.refresh_gestao_orcamentaria(p_id_empresa integer)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Serializa por empresa para evitar corrida com outros refresh (DELETE+INSERT).
    PERFORM pg_advisory_xact_lock(hashtext('refresh_gestao_orcamentaria'), p_id_empresa);

    -- 1) Catalogo de contas gerenciais efetivamente usadas em despesa (upsert).
    INSERT INTO mart.plano_contas_gerencial
        (id_empresa, id_filial, id_plano_conta, codigo, nome_conta, updated_at)
    SELECT
        f.id_empresa,
        f.id_filial,
        f.id_planodecontas,
        COALESCE(MAX(NULLIF(f.codigo_plano, '')), ''),
        COALESCE(MAX(NULLIF(f.nome_plano, '')), ''),
        now()
    FROM dw.fact_despesa_operacional f
    WHERE f.id_empresa = p_id_empresa
      AND f.id_planodecontas IS NOT NULL
    GROUP BY f.id_empresa, f.id_filial, f.id_planodecontas
    ON CONFLICT (id_empresa, id_filial, id_plano_conta)
    DO UPDATE SET
        codigo     = EXCLUDED.codigo,
        nome_conta = EXCLUDED.nome_conta,
        updated_at = now();

    -- 2) Despesa realizada por conta e mes (competencia). Rebuild do escopo.
    DELETE FROM mart.despesa_conta_mensal WHERE id_empresa = p_id_empresa;

    INSERT INTO mart.despesa_conta_mensal
        (id_empresa, id_filial, id_plano_conta, ano, mes, valor_realizado, qtd, updated_at)
    SELECT
        f.id_empresa,
        f.id_filial,
        f.id_planodecontas,
        (f.ano_mes_competencia / 100)::smallint AS ano,
        (f.ano_mes_competencia % 100)::smallint AS mes,
        SUM(f.valor)::numeric(18,2),
        COUNT(*)::int,
        now()
    FROM dw.fact_despesa_operacional f
    WHERE f.id_empresa = p_id_empresa
      AND f.id_planodecontas IS NOT NULL
      AND f.ano_mes_competencia IS NOT NULL
      AND f.ano_mes_competencia >= 200001
    GROUP BY f.id_empresa, f.id_filial, f.id_planodecontas, f.ano_mes_competencia;
END;
$$;

COMMENT ON FUNCTION etl.refresh_gestao_orcamentaria(integer) IS
  'Popula mart.plano_contas_gerencial + mart.despesa_conta_mensal a partir de dw.fact_despesa_operacional (competencia). Camada rapida da tela de Gestao Orcamentaria (097). Idempotente, advisory-lock por empresa.';
