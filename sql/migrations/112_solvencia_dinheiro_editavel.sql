-- Migration 112: Solvência — dinheiro editável + sinal canônico MOVBANCOS.
-- Homolog/produção PostgreSQL.

INSERT INTO app.solvencia_tipo_manual (id_tipo, chave, nome, grupo, secao, ordem) VALUES
  (5, 'dinheiro', 'Dinheiro em Espécie', 'ativo_circulante', 'dinheiro', 55)
ON CONFLICT (id_tipo) DO UPDATE
  SET chave = EXCLUDED.chave,
      nome = EXCLUDED.nome,
      grupo = EXCLUDED.grupo,
      secao = EXCLUDED.secao,
      ordem = EXCLUDED.ordem;

COMMENT ON TABLE app.solvencia_tipo_manual IS
  'Tipos manuais da Solvência. id_tipo=5 (dinheiro) permite override do valor as-of de sistema; a API marca editado_humano quando houver entrada.';

-- Semântica canônica MOVBANCOS (Xpert VR01):
--   TIPO IN (1,8) = entrada (crédito); TIPO IN (3,5) = saída (débito);
--   OPERACAO = 1 = estorno (inverte o sinal natural);
--   filtrar DELETAR = 0. Contas ativas: CONTASBANCARIA.ATIVO = 1.
CREATE OR REPLACE FUNCTION etl.movbancos_sinal(payload jsonb)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE((payload->>'VALOR')::numeric, 0) * CASE
    WHEN COALESCE((payload->>'TIPO')::int, -1) IN (1, 8)
      THEN CASE WHEN COALESCE((payload->>'OPERACAO')::int, 0) = 1 THEN -1 ELSE 1 END
    WHEN COALESCE((payload->>'TIPO')::int, -1) IN (3, 5)
      THEN CASE WHEN COALESCE((payload->>'OPERACAO')::int, 0) = 1 THEN 1 ELSE -1 END
    ELSE 0
  END;
$$;

COMMENT ON FUNCTION etl.movbancos_sinal(jsonb) IS
  'Sinal canônico MOVBANCOS: TIPO 1/8 crédito, 3/5 débito; OPERACAO=1 estorna. Caller filtra DELETAR=0.';

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
  saldos AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      c.ano_mes,
      SUM(etl.movbancos_sinal(m.payload))::numeric(18,2) AS ativo_banco
    FROM stg.movbancos m
    CROSS JOIN cortes c
    WHERE m.id_empresa = p_id_empresa
      AND (p_id_filial IS NULL OR m.id_filial = p_id_filial)
      AND COALESCE((m.payload->>'DELETAR')::int, 0) = 0
      AND COALESCE(m.dt_evento, (NULLIF(m.payload->>'DTACONTA',''))::timestamptz) < c.corte
    GROUP BY m.id_empresa, m.id_filial, c.ano_mes
  )
  INSERT INTO mart.liquidez_solvencia AS t
    (id_empresa, id_filial, ano_mes, ativo_banco, tem_ativo_dados, updated_at)
  SELECT id_empresa, id_filial, ano_mes, ativo_banco, true, now()
  FROM saldos
  ON CONFLICT (id_empresa, id_filial, ano_mes) DO UPDATE
    SET ativo_banco = EXCLUDED.ativo_banco,
        tem_ativo_dados = true,
        updated_at = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION etl.refresh_liquidez_banco(integer, integer, integer) IS
  'Atualiza ativo_banco em mart.liquidez_solvencia a partir de stg.movbancos com sinal canônico TIPO/OPERACAO.';
