-- Migration 118: Antifraude — Crédito / Vale de Funcionário
-- Idempotente. Não destrutivo.
--
-- Fonte Xpert (levantamento):
--   dbo.FUNCIONARIOS.LIMITEVALE  → teto de vale/crédito a prazo do colaborador
--   dbo.FUNCIONARIOS.VALES       → snapshot de consumo no cadastro (cruzamento)
--   dbo.ENTIDADES.CNPJCPF        → vínculo funcionário↔cliente (mesmo CPF)
--   dbo.CONTASRECEBER            → títulos a prazo (HISTORICO traz Cupom/NFC-e)
--   dbo.COMPROVANTES.ID_USUARIOS → operador de caixa que liberou a venda
--
-- Grão: resumo por (empresa, funcionário, ano_mes); usos por título/cupom.

CREATE TABLE IF NOT EXISTS mart.fraud_credito_funcionario_resumo (
  id_empresa           integer NOT NULL,
  id_funcionario       integer NOT NULL,
  ano_mes              integer NOT NULL,
  id_filial_ref        integer,
  id_entidade          integer,
  nome_funcionario     text NOT NULL DEFAULT '',
  cpf                  text,
  ativo                boolean NOT NULL DEFAULT true,
  limite_vale          numeric(18,2) NOT NULL DEFAULT 0,
  vales_cadastro       numeric(18,2) NOT NULL DEFAULT 0,
  usado_mes            numeric(18,2) NOT NULL DEFAULT 0,
  saldo_restante       numeric(18,2) NOT NULL DEFAULT 0,
  qtd_usos_mes         integer NOT NULL DEFAULT 0,
  max_usos_mesmo_dia   integer NOT NULL DEFAULT 0,
  status               text NOT NULL DEFAULT 'Normal',
  motivos              text[] NOT NULL DEFAULT '{}',
  refreshed_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_funcionario, ano_mes)
);

CREATE INDEX IF NOT EXISTS ix_mart_fraud_cred_func_resumo_status
  ON mart.fraud_credito_funcionario_resumo (id_empresa, ano_mes, status);

CREATE TABLE IF NOT EXISTS mart.fraud_credito_funcionario_uso (
  id_empresa           integer NOT NULL,
  id_funcionario       integer NOT NULL,
  ano_mes              integer NOT NULL,
  id_filial            integer NOT NULL,
  id_entidade          integer,
  id_contasreceber     integer NOT NULL,
  id_comprovante       bigint,
  nro_cupom            text,
  nro_documento        text,
  dt_evento            timestamptz,
  valor                numeric(18,2) NOT NULL DEFAULT 0,
  id_usuario_caixa     integer,
  operador_caixa       text NOT NULL DEFAULT '',
  historico            text NOT NULL DEFAULT '',
  atipico              boolean NOT NULL DEFAULT false,
  refreshed_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_funcionario, ano_mes, id_filial, id_contasreceber)
);

CREATE INDEX IF NOT EXISTS ix_mart_fraud_cred_func_uso_func
  ON mart.fraud_credito_funcionario_uso (id_empresa, id_funcionario, ano_mes, dt_evento DESC);

COMMENT ON TABLE mart.fraud_credito_funcionario_resumo IS
  'Antifraude crédito funcionário: limite LIMITEVALE × usos a prazo do mês.';
COMMENT ON TABLE mart.fraud_credito_funcionario_uso IS
  'Detalhe de cada uso a prazo (CONTASRECEBER→cupom→COMPROVANTES/operador).';

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
  WITH funcionarios AS (
    SELECT DISTINCT ON (regexp_replace(COALESCE(f.payload->>'CPF', ''), '[^0-9]', '', 'g'))
      f.id_empresa,
      (f.payload->>'ID_FUNCIONARIOS')::int AS id_funcionario,
      f.id_filial AS id_filial_ref,
      COALESCE(NULLIF(TRIM(f.payload->>'NOMEFUNCIONARIO'), ''), 'Funcionário') AS nome_funcionario,
      regexp_replace(COALESCE(f.payload->>'CPF', ''), '[^0-9]', '', 'g') AS cpf,
      true AS ativo,
      COALESCE(NULLIF(f.payload->>'LIMITEVALE', '')::numeric, 0) AS limite_vale,
      COALESCE(NULLIF(f.payload->>'VALES', '')::numeric, 0) AS vales_cadastro
    FROM stg.funcionarios f
    WHERE f.id_empresa = p_id_empresa
      AND NULLIF(f.payload->>'ID_FUNCIONARIOS', '') IS NOT NULL
      AND length(regexp_replace(COALESCE(f.payload->>'CPF', ''), '[^0-9]', '', 'g')) >= 11
      AND COALESCE(NULLIF(f.payload->>'LIMITEVALE', '')::numeric, 0) > 0
      AND COALESCE(f.payload->>'ATIVO', 'true') IN ('true', '1', 't', 'True')
    ORDER BY
      regexp_replace(COALESCE(f.payload->>'CPF', ''), '[^0-9]', '', 'g'),
      f.id_filial
  ),
  entidades AS (
    SELECT DISTINCT ON (regexp_replace(COALESCE(e.payload->>'CNPJCPF', e.payload->>'CPF', ''), '[^0-9]', '', 'g'))
      e.id_empresa,
      (e.payload->>'ID_ENTIDADE')::int AS id_entidade,
      regexp_replace(COALESCE(e.payload->>'CNPJCPF', e.payload->>'CPF', ''), '[^0-9]', '', 'g') AS cpf
    FROM stg.entidades e
    WHERE e.id_empresa = p_id_empresa
      AND length(regexp_replace(COALESCE(e.payload->>'CNPJCPF', e.payload->>'CPF', ''), '[^0-9]', '', 'g')) >= 11
    ORDER BY regexp_replace(COALESCE(e.payload->>'CNPJCPF', e.payload->>'CPF', ''), '[^0-9]', '', 'g')
  )
  SELECT f.*, e.id_entidade
  FROM funcionarios f
  LEFT JOIN entidades e ON e.id_empresa = f.id_empresa AND e.cpf = f.cpf;

  CREATE INDEX ON _fcf_base (id_entidade) WHERE id_entidade IS NOT NULL;

  -- Títulos a prazo do cliente-espelho (DTACONTA/DATAREPL; cupom resolve data/operador)
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
    NULLIF(substring(COALESCE(cr.payload->>'HISTORICO', '') from 'Cupom:\s*([0-9]+)'), '') AS nro_cupom,
    NULLIF(substring(COALESCE(cr.payload->>'HISTORICO', '') from 'NFC-e\s*([0-9]+)'), '') AS nro_nfce,
    NULLIF(cr.payload->>'NRODOC', '') AS nro_documento,
    COALESCE(
      NULLIF(cr.payload->>'DTACONTA', '')::timestamptz,
      NULLIF(cr.payload->>'DTAVCTO', '')::timestamptz,
      NULLIF(cr.payload->>'DATAREPL', '')::timestamptz
    ) AS dt_cr
  FROM _fcf_base b
  JOIN stg.contasreceber cr
    ON cr.id_empresa = b.id_empresa
   AND (cr.payload->>'ID_ENTIDADE')::int = b.id_entidade
  WHERE b.id_entidade IS NOT NULL
    AND (
      cr.payload->>'HISTORICO' ILIKE ('%' || b.nome_funcionario || '%')
      OR cr.payload->>'HISTORICO' ~ ('Cliente:\s*' || b.id_entidade::text)
    )
    AND COALESCE(
      NULLIF(cr.payload->>'DTACONTA', '')::timestamptz,
      NULLIF(cr.payload->>'DATAREPL', '')::timestamptz,
      NULLIF(cr.payload->>'DTAVCTO', '')::timestamptz
    ) >= v_hist_ini;

  -- Comprovantes só da janela do mês (operador/data real). Histórico atípico usa dt_cr.
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
    AND c.dt_evento >= (v_ini - interval '3 days')
    AND c.dt_evento < v_fim;

  CREATE INDEX ON _fcf_comp (id_filial, nro_cupom_fiscal);
  CREATE INDEX ON _fcf_comp (id_filial, nro_comprovante);
  CREATE INDEX ON _fcf_comp (id_filial, nro_doc);

  CREATE TEMP TABLE _fcf_usos ON COMMIT DROP AS
  SELECT
    r.id_empresa,
    r.id_funcionario,
    r.id_filial,
    r.id_entidade,
    r.id_contasreceber,
    r.valor,
    r.historico,
    r.nro_cupom,
    COALESCE(r.nro_cupom, r.nro_nfce, r.nro_documento) AS nro_documento,
    c.id_comprovante,
    COALESCE(c.dt_evento, r.dt_cr) AS dt_evento,
    c.id_usuario_caixa,
    COALESCE(
      NULLIF(TRIM(u.payload->>'NOMEUSUARIOS'), ''),
      NULLIF(TRIM(u.payload->>'NOME'), ''),
      ''
    ) AS operador_caixa
  FROM _fcf_cr r
  LEFT JOIN LATERAL (
    SELECT *
    FROM _fcf_comp c
    WHERE c.id_filial = r.id_filial
      AND (
        (r.nro_cupom IS NOT NULL AND (c.nro_cupom_fiscal = r.nro_cupom OR c.nro_comprovante = r.nro_cupom))
        OR (r.nro_nfce IS NOT NULL AND (c.nro_cupom_fiscal = r.nro_nfce OR c.nro_comprovante = r.nro_nfce OR c.nro_doc = r.nro_nfce))
      )
    ORDER BY c.dt_evento DESC NULLS LAST
    LIMIT 1
  ) c ON true
  LEFT JOIN stg.usuarios u
    ON u.id_empresa = r.id_empresa
   AND u.id_usuario = c.id_usuario_caixa;

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
    dt_evento, valor, id_usuario_caixa, operador_caixa, historico, atipico, refreshed_at
  )
  SELECT
    id_empresa, id_funcionario, v_ano_mes, id_filial, id_entidade,
    id_contasreceber, id_comprovante, nro_cupom, nro_documento,
    dt_evento, valor, id_usuario_caixa, COALESCE(operador_caixa, ''), historico, atipico, now()
  FROM _fcf_usos_mes
  WHERE id_contasreceber IS NOT NULL
  ON CONFLICT DO NOTHING;

  INSERT INTO mart.fraud_credito_funcionario_resumo (
    id_empresa, id_funcionario, ano_mes, id_filial_ref, id_entidade,
    nome_funcionario, cpf, ativo, limite_vale, vales_cadastro,
    usado_mes, saldo_restante, qtd_usos_mes, max_usos_mesmo_dia,
    status, motivos, refreshed_at
  )
  SELECT
    b.id_empresa, b.id_funcionario, v_ano_mes, b.id_filial_ref, b.id_entidade,
    b.nome_funcionario, b.cpf, b.ativo, b.limite_vale, b.vales_cadastro,
    COALESCE(x.usado_mes, 0),
    GREATEST(b.limite_vale - COALESCE(x.usado_mes, 0), 0),
    COALESCE(x.qtd_usos_mes, 0),
    COALESCE(x.max_usos_mesmo_dia, 0),
    CASE WHEN cardinality(COALESCE(x.motivos, '{}'::text[])) > 0 THEN 'Suspeito' ELSE 'Normal' END,
    COALESCE(x.motivos, '{}'::text[]),
    now()
  FROM _fcf_base b
  LEFT JOIN LATERAL (
    SELECT
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
        CASE WHEN b.limite_vale > 0 AND COALESCE(SUM(u.valor), 0) > b.limite_vale THEN 'Limite Extrapolado' END,
        CASE WHEN b.limite_vale > 0 AND b.vales_cadastro > b.limite_vale THEN 'Limite Extrapolado (cadastro VALES)' END,
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
  'Refresh mart antifraude crédito funcionário (LIMITEVALE × CONTASRECEBER/cupom/operador).';


-- ACL: painel Crédito Funcionário sob Antifraude
-- Roles altas (master/owner) usam ROLE_DEFAULT_SCREENS no código; aqui liberamos
-- manager/viewer quando já têm fraud.risco_financeiro (mesmo perfil de risco).
INSERT INTO auth.user_screen_permissions (user_id, screen_key)
SELECT u.id, 'fraud.credito_funcionario'
FROM auth.users u
WHERE u.role IN ('tenant_manager', 'tenant_viewer')
  AND EXISTS (
    SELECT 1 FROM auth.user_screen_permissions p
    WHERE p.user_id = u.id AND p.screen_key IN ('fraud', 'fraud.risco_financeiro')
  )
ON CONFLICT (user_id, screen_key) DO NOTHING;
