-- 124: perf refresh crédito funcionário — CR via dt_evento indexado
-- Baseado em 123: Crédito funcionário — origem ENTIDADES grupo 12 (sem join FUNCIONARIOS/CPF).
-- Motivo: cadastro de funcionários incompleto no Xpert; Grupo Entidade 12 = Funcionários.
-- Limite/uso são por empresa (entidade compartilhada entre filiais).
-- id_funcionario na mart = id_entidade (grão canônico).
-- id_filial_ref = 0 (não amarra a uma filial). Detalhe de uso traz id_filial do gasto.
-- CONTASRECEBER: join só por id_empresa + ID_ENTIDADE (sem filtro de HISTORICO/nome).

BEGIN;

CREATE OR REPLACE FUNCTION etl.refresh_fraud_credito_funcionario(
  p_id_empresa integer,
  p_ano_mes integer DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_ano_mes integer;
  v_ini date;
  v_fim date;
  v_hist_ini date;
  v_count integer;
BEGIN
  IF p_id_empresa IS NULL OR p_id_empresa <= 0 THEN
    RAISE EXCEPTION 'id_empresa obrigatório';
  END IF;

  v_ano_mes := COALESCE(
    p_ano_mes,
    (EXTRACT(YEAR FROM (now() AT TIME ZONE 'America/Sao_Paulo'))::int * 100
     + EXTRACT(MONTH FROM (now() AT TIME ZONE 'America/Sao_Paulo'))::int)
  );
  v_ini := make_date(v_ano_mes / 100, v_ano_mes % 100, 1);
  v_fim := (v_ini + interval '1 month')::date;
  v_hist_ini := (v_ini - interval '90 days')::date;

  PERFORM pg_advisory_xact_lock(88118001, p_id_empresa);
  PERFORM pg_advisory_xact_lock(88118002, v_ano_mes);

  DELETE FROM mart.fraud_credito_funcionario_uso
   WHERE id_empresa = p_id_empresa AND ano_mes = v_ano_mes;
  DELETE FROM mart.fraud_credito_funcionario_resumo
   WHERE id_empresa = p_id_empresa AND ano_mes = v_ano_mes;

  CREATE TEMP TABLE _fcf_base ON COMMIT DROP AS
  SELECT
    e.id_empresa,
    e.id_entidade AS id_funcionario,
    0 AS id_filial_ref,
    e.nome_funcionario,
    e.cpf,
    e.ativo,
    0::numeric(18,2) AS vales_cadastro,
    e.id_entidade,
    e.limite_prazo,
    e.limite_vale,
    (e.limite_prazo + e.limite_vale) AS limite_total
  FROM (
    SELECT DISTINCT ON ((e.payload->>'ID_ENTIDADE')::int)
      e.id_empresa,
      (e.payload->>'ID_ENTIDADE')::int AS id_entidade,
      COALESCE(
        NULLIF(TRIM(e.payload->>'NOMEENTIDADE'), ''),
        NULLIF(TRIM(e.payload->>'RAZAOSOCIALENTIDADE'), ''),
        'Funcionário'
      ) AS nome_funcionario,
      regexp_replace(COALESCE(e.payload->>'CNPJCPF', e.payload->>'CPF', ''), '[^0-9]', '', 'g') AS cpf,
      COALESCE(e.payload->>'ATIVO', 'true') IN ('true', '1', 't', 'True') AS ativo,
      COALESCE(NULLIF(e.payload->>'LIMITE', '')::numeric, 0) AS limite_prazo,
      COALESCE(NULLIF(e.payload->>'LIMITE_VALE', '')::numeric, 0) AS limite_vale
    FROM stg.entidades e
    WHERE e.id_empresa = p_id_empresa
      AND NULLIF(e.payload->>'ID_ENTIDADE', '') IS NOT NULL
      AND e.payload->>'ID_GRUPOENTIDADES' = '12'
      AND (
        COALESCE(NULLIF(e.payload->>'LIMITE', '')::numeric, 0) > 0
        OR COALESCE(NULLIF(e.payload->>'LIMITE_VALE', '')::numeric, 0) > 0
      )
    ORDER BY
      (e.payload->>'ID_ENTIDADE')::int,
      CASE WHEN COALESCE(e.payload->>'ATIVO', 'true') IN ('true', '1', 't', 'True') THEN 0 ELSE 1 END,
      e.id_filial
  ) e;

  CREATE INDEX ON _fcf_base (id_entidade) WHERE id_entidade IS NOT NULL;

  -- CR filtrado por dt_evento indexado (id_empresa, dt_evento) — evita full scan.
  CREATE TEMP TABLE _fcf_cr ON COMMIT DROP AS
  SELECT
    b.id_empresa,
    b.id_funcionario,
    b.nome_funcionario,
    b.id_entidade,
    cr.id_filial,
    (cr.payload->>'ID_CONTASRECEBER')::int AS id_contasreceber,
    COALESCE((cr.payload->>'VALOR')::numeric, 0) AS valor,
    COALESCE(cr.payload->>'HISTORICO', '') AS historico,
    CASE
      WHEN COALESCE(cr.payload->>'HISTORICO', '') ILIKE '%vale%' THEN 'vale'
      ELSE 'prazo'
    END AS tipo_uso,
    NULLIF(substring(COALESCE(cr.payload->>'HISTORICO', '') from 'Cupom:\s*([0-9]+)'), '') AS nro_cupom,
    NULLIF(substring(COALESCE(cr.payload->>'HISTORICO', '') from 'NFC-[eE]\s*[#:]?\s*([0-9]+)'), '') AS nro_nfce,
    NULLIF(substring(COALESCE(cr.payload->>'HISTORICO', '') from 'NF-[eE]\s*[#:]?\s*([0-9]+)'), '') AS nro_nfe,
    NULLIF(cr.payload->>'NRODOC', '') AS nro_documento_raw,
    COALESCE(
      cr.dt_evento,
      NULLIF(cr.payload->>'DTACONTA', '')::timestamptz,
      NULLIF(cr.payload->>'DTAVCTO', '')::timestamptz,
      NULLIF(cr.payload->>'DATAREPL', '')::timestamptz
    ) AS dt_cr
  FROM stg.contasreceber cr
  JOIN _fcf_base b
    ON b.id_empresa = cr.id_empresa
   AND b.id_entidade = (cr.payload->>'ID_ENTIDADE')::int
  WHERE cr.id_empresa = p_id_empresa
    AND cr.dt_evento IS NOT NULL
    AND cr.dt_evento >= v_hist_ini
    AND cr.dt_evento < (v_fim + interval '3 days')
    AND NULLIF(cr.payload->>'ID_ENTIDADE', '') IS NOT NULL;

  -- Janela ampla: cupom/NF do mês + margem (título pode casar comprovante vizinho).
  CREATE TEMP TABLE _fcf_comp ON COMMIT DROP AS
  SELECT
    c.id_filial,
    c.id_comprovante,
    COALESCE(c.dt_evento, NULLIF(c.payload->>'DATA', '')::timestamptz) AS dt_evento,
    COALESCE(c.id_usuario_shadow, NULLIF(c.payload->>'ID_USUARIOS', '')::int) AS id_usuario_caixa,
    NULLIF(c.payload->>'NROCUPOMFISCAL', '') AS nro_cupom_fiscal,
    NULLIF(c.payload->>'NROCOMPROVANTE', '') AS nro_comprovante,
    NULLIF(c.payload->>'NRODOC', '') AS nro_doc
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND COALESCE(c.situacao_shadow, COALESCE(NULLIF(c.payload->>'SITUACAO', '')::int, 0)) <> 3
    AND c.dt_evento >= (v_ini - interval '7 days')
    AND c.dt_evento < (v_fim + interval '3 days');

  CREATE INDEX ON _fcf_comp (id_filial, nro_cupom_fiscal);
  CREATE INDEX ON _fcf_comp (id_filial, nro_comprovante);
  CREATE INDEX ON _fcf_comp (id_filial, nro_doc);
  CREATE INDEX ON _fcf_comp (id_filial, id_comprovante);

  CREATE TEMP TABLE _fcf_usos ON COMMIT DROP AS
  SELECT
    r.id_empresa,
    r.id_funcionario,
    r.id_filial,
    r.id_entidade,
    r.id_contasreceber,
    r.valor,
    r.historico,
    r.tipo_uso,
    r.nro_cupom,
    COALESCE(r.nro_nfce, r.nro_nfe, '') AS nro_documento,
    COALESCE(c_cupom.id_comprovante, nfe.id_comprovante) AS id_comprovante,
    COALESCE(c_cupom.dt_evento, c_nfe.dt_evento, r.dt_cr) AS dt_evento,
    COALESCE(
      c_cupom.id_usuario_caixa,
      c_nfe.id_usuario_caixa
    ) AS id_usuario_caixa,
    COALESCE(
      NULLIF(TRIM(u.payload->>'NOMEUSUARIOS'), ''),
      NULLIF(TRIM(u.payload->>'NOME'), ''),
      ''
    ) AS operador_caixa
  FROM _fcf_cr r
  -- 1) Cupom / número no comprovante
  LEFT JOIN LATERAL (
    SELECT *
    FROM _fcf_comp c
    WHERE c.id_filial = r.id_filial
      AND (
        (r.nro_cupom IS NOT NULL AND (c.nro_cupom_fiscal = r.nro_cupom OR c.nro_comprovante = r.nro_cupom))
        OR (r.nro_nfce IS NOT NULL AND (c.nro_cupom_fiscal = r.nro_nfce OR c.nro_comprovante = r.nro_nfce OR c.nro_doc = r.nro_nfce))
        OR (r.nro_nfe IS NOT NULL AND (c.nro_cupom_fiscal = r.nro_nfe OR c.nro_comprovante = r.nro_nfe OR c.nro_doc = r.nro_nfe))
      )
    ORDER BY c.dt_evento DESC NULLS LAST
    LIMIT 1
  ) c_cupom ON true
  -- 2) NF-e/NFC-e → stg.nfe → id_comprovante (quando cupom não resolveu)
  LEFT JOIN LATERAL (
    SELECT
      n.id_comprovante,
      COALESCE(n.dt_evento, n.data_emissao_shadow) AS dt_evento
    FROM stg.nfe n
    WHERE n.id_empresa = r.id_empresa
      AND n.id_filial = r.id_filial
      AND COALESCE(n.status_shadow, 0) <> 5
      AND c_cupom.id_comprovante IS NULL
      AND COALESCE(r.nro_nfce, r.nro_nfe) IS NOT NULL
      AND (
        NULLIF(TRIM(n.numero_nfe_shadow), '') = COALESCE(r.nro_nfce, r.nro_nfe)
        OR NULLIF(TRIM(n.payload->>'NRONF'), '') = COALESCE(r.nro_nfce, r.nro_nfe)
      )
    ORDER BY n.dt_evento DESC NULLS LAST
    LIMIT 1
  ) nfe ON true
  LEFT JOIN LATERAL (
    SELECT
      c.id_comprovante,
      c.dt_evento,
      c.id_usuario_caixa
    FROM _fcf_comp c
    WHERE nfe.id_comprovante IS NOT NULL
      AND c.id_filial = r.id_filial
      AND c.id_comprovante = nfe.id_comprovante
    LIMIT 1
  ) c_nfe ON true
  LEFT JOIN stg.comprovantes c_fallback
    ON c_fallback.id_empresa = r.id_empresa
   AND c_fallback.id_filial = r.id_filial
   AND c_fallback.id_comprovante = COALESCE(c_cupom.id_comprovante, nfe.id_comprovante)
   AND COALESCE(c_cupom.id_usuario_caixa, c_nfe.id_usuario_caixa) IS NULL
  LEFT JOIN stg.usuarios u
    ON u.id_empresa = r.id_empresa
   AND u.id_usuario = COALESCE(
     c_cupom.id_usuario_caixa,
     c_nfe.id_usuario_caixa,
     c_fallback.id_usuario_shadow,
     NULLIF(c_fallback.payload->>'ID_USUARIOS', '')::int
   );

  CREATE TEMP TABLE _fcf_hist ON COMMIT DROP AS
  SELECT
    id_funcionario,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY valor) AS mediana,
    avg(valor) AS media,
    COALESCE(stddev_pop(valor), 0) AS desvio
  FROM _fcf_usos
  WHERE dt_evento IS NOT NULL AND dt_evento >= v_hist_ini AND dt_evento < v_fim AND valor > 0
  GROUP BY id_funcionario;

  CREATE TEMP TABLE _fcf_usos_mes ON COMMIT DROP AS
  SELECT
    u.*,
    CASE
      WHEN COALESCE(h.mediana, 0) > 0
           AND u.valor >= GREATEST(h.mediana * 2.5, h.mediana + 2 * COALESCE(h.desvio, 0))
        THEN true
      WHEN COALESCE(h.mediana, 0) <= 0 AND COALESCE(h.media, 0) > 0 AND u.valor >= h.media * 2.5
        THEN true
      ELSE false
    END AS atipico
  FROM _fcf_usos u
  LEFT JOIN _fcf_hist h ON h.id_funcionario = u.id_funcionario
  WHERE u.dt_evento IS NOT NULL AND u.dt_evento >= v_ini AND u.dt_evento < v_fim;

  INSERT INTO mart.fraud_credito_funcionario_uso (
    id_empresa, id_funcionario, ano_mes, id_filial, id_entidade,
    id_contasreceber, id_comprovante, nro_cupom, nro_documento,
    dt_evento, valor, id_usuario_caixa, operador_caixa, historico, atipico, tipo_uso, refreshed_at
  )
  SELECT
    id_empresa, id_funcionario, v_ano_mes, id_filial, id_entidade,
    id_contasreceber, id_comprovante, nro_cupom, nro_documento,
    dt_evento, valor, id_usuario_caixa, COALESCE(operador_caixa, ''), historico, atipico,
    COALESCE(tipo_uso, 'prazo'), now()
  FROM _fcf_usos_mes
  WHERE id_contasreceber IS NOT NULL
  ON CONFLICT DO NOTHING;

  INSERT INTO mart.fraud_credito_funcionario_resumo (
    id_empresa, id_funcionario, ano_mes, id_filial_ref, id_entidade,
    nome_funcionario, cpf, ativo,
    limite_prazo, limite_vale, limite_total, vales_cadastro,
    usado_prazo, usado_vale, usado_mes,
    saldo_prazo, saldo_vale, saldo_restante,
    qtd_usos_mes, max_usos_mesmo_dia,
    status, motivos, refreshed_at
  )
  SELECT
    b.id_empresa, b.id_funcionario, v_ano_mes, b.id_filial_ref, b.id_entidade,
    b.nome_funcionario, b.cpf, b.ativo,
    b.limite_prazo, b.limite_vale, b.limite_total, b.vales_cadastro,
    COALESCE(x.usado_prazo, 0),
    COALESCE(x.usado_vale, 0),
    COALESCE(x.usado_mes, 0),
    GREATEST(b.limite_prazo - COALESCE(x.usado_prazo, 0), 0),
    GREATEST(b.limite_vale - COALESCE(x.usado_vale, 0), 0),
    GREATEST(b.limite_total - COALESCE(x.usado_mes, 0), 0),
    COALESCE(x.qtd_usos_mes, 0),
    COALESCE(x.max_usos_mesmo_dia, 0),
    CASE WHEN cardinality(COALESCE(x.motivos, '{}'::text[])) > 0 THEN 'Suspeito' ELSE 'Normal' END,
    COALESCE(x.motivos, '{}'::text[]),
    now()
  FROM _fcf_base b
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(SUM(u.valor) FILTER (WHERE u.tipo_uso = 'prazo'), 0)::numeric(18,2) AS usado_prazo,
      COALESCE(SUM(u.valor) FILTER (WHERE u.tipo_uso = 'vale'), 0)::numeric(18,2) AS usado_vale,
      COALESCE(SUM(u.valor), 0)::numeric(18,2) AS usado_mes,
      COUNT(*)::int AS qtd_usos_mes,
      COALESCE((
        SELECT MAX(cnt) FROM (
          SELECT count(*)::int AS cnt
          FROM _fcf_usos_mes u2
          WHERE u2.id_funcionario = b.id_funcionario
          GROUP BY (u2.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date
        ) d
      ), 0)::int AS max_usos_mesmo_dia,
      ARRAY_REMOVE(ARRAY[
        CASE
          WHEN b.limite_prazo > 0
               AND COALESCE(SUM(u.valor) FILTER (WHERE u.tipo_uso = 'prazo'), 0) > b.limite_prazo
            THEN 'Limite a prazo extrapolado'
        END,
        CASE
          WHEN b.limite_vale > 0
               AND COALESCE(SUM(u.valor) FILTER (WHERE u.tipo_uso = 'vale'), 0) > b.limite_vale
            THEN 'Limite de vale extrapolado'
        END,
        CASE
          WHEN b.limite_total > 0
               AND COALESCE(SUM(u.valor), 0) > b.limite_total
            THEN 'Limite total extrapolado'
        END,
        CASE WHEN COALESCE((
          SELECT MAX(cnt) FROM (
            SELECT count(*)::int AS cnt
            FROM _fcf_usos_mes u2
            WHERE u2.id_funcionario = b.id_funcionario
            GROUP BY (u2.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date
          ) d
        ), 0) >= 2 THEN 'Frequência Anômala' END,
        CASE WHEN COALESCE(BOOL_OR(u.atipico), false) THEN 'Valor Atípico' END
      ], NULL) AS motivos
    FROM _fcf_usos_mes u
    WHERE u.id_funcionario = b.id_funcionario
  ) x ON true;

  SELECT count(*)::int INTO v_count
  FROM mart.fraud_credito_funcionario_resumo
  WHERE id_empresa = p_id_empresa AND ano_mes = v_ano_mes;
  RETURN v_count;
END;
$$;

COMMENT ON FUNCTION etl.refresh_fraud_credito_funcionario(integer, integer) IS
  'Refresh crédito funcionário: ENTIDADES grupo 12; CR via dt_evento indexado; uso empresa-wide; doc=NF.';

COMMIT;
