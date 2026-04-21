BEGIN;

-- Canonical helpers for financial and operational semantics.

CREATE OR REPLACE FUNCTION etl.item_cost_unitario(
  p_payload jsonb,
  p_shadow numeric DEFAULT NULL
)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    etl.safe_numeric(p_payload->>'VLRCUSTOCOMICMS')::numeric(18,6),
    etl.safe_numeric(p_payload->>'VLR_CUSTO_COM_ICMS')::numeric(18,6),
    etl.safe_numeric(p_payload->>'VALOR_CUSTO_COM_ICMS')::numeric(18,6),
    etl.safe_numeric(p_payload->>'vlrcustocomicms')::numeric(18,6),
    etl.safe_numeric(p_payload->>'vlr_custo_com_icms')::numeric(18,6),
    etl.safe_numeric(p_payload->>'valor_custo_com_icms')::numeric(18,6),
    p_shadow::numeric(18,6),
    etl.safe_numeric(p_payload->>'VLRCUSTO')::numeric(18,6),
    etl.safe_numeric(p_payload->>'vlrcusto')::numeric(18,6)
  );
$$;

CREATE OR REPLACE FUNCTION etl.comprovante_data_conta(
  p_payload jsonb,
  p_fallback date DEFAULT NULL
)
RETURNS date
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    p_fallback,
    etl.business_date(
      etl.coalesce_operational_timestamptz(
        NULL::timestamptz,
        p_payload->>'DTACONTA',
        p_payload->>'dtaconta',
        p_payload->>'DATA_CONTA',
        p_payload->>'data_conta',
        p_payload->>'DT_CONTA',
        p_payload->>'dt_conta'
      )
    )
  );
$$;

CREATE OR REPLACE FUNCTION etl.comprovante_is_cancelled(
  p_cancelado boolean,
  p_situacao integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_situacao = 2 THEN true
    WHEN p_situacao IS NOT NULL THEN false
    ELSE COALESCE(p_cancelado, false)
  END;
$$;

CREATE OR REPLACE FUNCTION etl.comprovante_cash_eligible(
  p_data timestamp,
  p_data_conta date,
  p_id_turno integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    COALESCE(p_id_turno, 0) > 1
    AND p_data IS NOT NULL
    AND p_data_conta IS NOT NULL
    AND etl.business_date(p_data) = p_data_conta;
$$;

CREATE OR REPLACE FUNCTION etl.comprovante_cash_eligible(
  p_data timestamptz,
  p_data_conta date,
  p_id_turno integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    COALESCE(p_id_turno, 0) > 1
    AND p_data IS NOT NULL
    AND p_data_conta IS NOT NULL
    AND etl.business_date(p_data) = p_data_conta;
$$;

CREATE OR REPLACE FUNCTION etl.resolve_cash_eligible(
  p_cash_eligible boolean,
  p_data timestamp,
  p_data_conta date,
  p_id_turno integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    p_cash_eligible,
    CASE
      WHEN p_data_conta IS NOT NULL THEN etl.comprovante_cash_eligible(p_data, p_data_conta, p_id_turno)
      ELSE COALESCE(p_id_turno, 0) > 1
    END
  );
$$;

CREATE OR REPLACE FUNCTION etl.resolve_cash_eligible(
  p_cash_eligible boolean,
  p_data timestamptz,
  p_data_conta date,
  p_id_turno integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    p_cash_eligible,
    CASE
      WHEN p_data_conta IS NOT NULL THEN etl.comprovante_cash_eligible(p_data, p_data_conta, p_id_turno)
      ELSE COALESCE(p_id_turno, 0) > 1
    END
  );
$$;

ALTER TABLE dw.fact_comprovante
  ADD COLUMN IF NOT EXISTS data_conta date NULL,
  ADD COLUMN IF NOT EXISTS cash_eligible boolean NULL;

ALTER TABLE dw.fact_pagamento_comprovante
  ADD COLUMN IF NOT EXISTS data_conta date NULL,
  ADD COLUMN IF NOT EXISTS cash_eligible boolean NULL;

CREATE INDEX IF NOT EXISTS ix_fact_comprovante_cash_lookup
  ON dw.fact_comprovante (id_empresa, id_filial, data_key, id_turno)
  WHERE cash_eligible = true;

CREATE INDEX IF NOT EXISTS ix_fact_pag_comp_cash_lookup
  ON dw.fact_pagamento_comprovante (id_empresa, id_filial, data_key, id_turno, tipo_forma)
  WHERE cash_eligible = true;

UPDATE app.payment_type_map
SET label = 'Depósito Bancário',
    category = 'DEPOSITO_BANCARIO',
    updated_at = now()
WHERE tipo_forma = 6
  AND (
    label IS DISTINCT FROM 'Depósito Bancário'
    OR category IS DISTINCT FROM 'DEPOSITO_BANCARIO'
  );

UPDATE stg.itensmovprodutos i
SET custo_unitario_shadow = etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6)
WHERE etl.item_cost_unitario(i.payload, i.custo_unitario_shadow) IS NOT NULL
  AND i.custo_unitario_shadow IS DISTINCT FROM etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6);

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_comprovantes;
  CREATE TEMP TABLE tmp_etl_candidate_comprovantes (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_comprovante int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_comprovantes
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) >= v_cutoff
    AND (
      c.received_at > v_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH base AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      c.id_comprovante,
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS valor_total,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS raw_cancelado,
      COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')) AS situacao,
      etl.comprovante_data_conta(c.payload, NULL) AS data_conta,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
  ), src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      data,
      data_key,
      id_usuario,
      id_turno,
      id_cliente,
      valor_total,
      etl.comprovante_is_cancelled(raw_cancelado, situacao) AS cancelado,
      situacao,
      data_conta,
      etl.comprovante_cash_eligible(data, data_conta, id_turno) AS cash_eligible,
      payload
    FROM base
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,data_conta,cash_eligible,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,data_conta,cash_eligible,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
    DO UPDATE SET
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_turno = EXCLUDED.id_turno,
      id_cliente = EXCLUDED.id_cliente,
      valor_total = EXCLUDED.valor_total,
      cancelado = EXCLUDED.cancelado,
      situacao = EXCLUDED.situacao,
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_comprovante.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_comprovante.situacao IS DISTINCT FROM EXCLUDED.situacao
      OR dw.fact_comprovante.valor_total IS DISTINCT FROM EXCLUDED.valor_total
      OR dw.fact_comprovante.data_conta IS DISTINCT FROM EXCLUDED.data_conta
      OR dw.fact_comprovante.cash_eligible IS DISTINCT FROM EXCLUDED.cash_eligible
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itensmovprodutos'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_movimentos;
  CREATE TEMP TABLE tmp_etl_candidate_movimentos (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_movprodutos int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_movimentos
  SELECT
    m.id_empresa,
    m.id_filial,
    m.id_db,
    m.id_movprodutos
  FROM stg.movprodutos m
  WHERE m.id_empresa = p_id_empresa
    AND etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento)) >= v_cutoff
    AND (
      m.received_at > COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz)
      OR (m.dt_evento IS NOT NULL AND m.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  DROP TABLE IF EXISTS tmp_etl_candidate_itens;
  CREATE TEMP TABLE tmp_etl_candidate_itens (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_movprodutos int NOT NULL,
    id_itensmovprodutos int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_itens
  SELECT
    i.id_empresa,
    i.id_filial,
    i.id_db,
    i.id_movprodutos,
    i.id_itensmovprodutos
  FROM stg.itensmovprodutos i
  LEFT JOIN stg.movprodutos m
    ON m.id_empresa = i.id_empresa
   AND m.id_filial = i.id_filial
   AND m.id_db = i.id_db
   AND m.id_movprodutos = i.id_movprodutos
  WHERE i.id_empresa = p_id_empresa
    AND COALESCE(
      etl.business_date(i.dt_evento),
      etl.business_date(etl.sales_event_timestamptz(m.payload, m.dt_evento))
    ) >= v_cutoff
    AND (
      i.received_at > v_wm
      OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  INSERT INTO tmp_etl_candidate_itens
  SELECT
    i.id_empresa,
    i.id_filial,
    i.id_db,
    i.id_movprodutos,
    i.id_itensmovprodutos
  FROM stg.itensmovprodutos i
  JOIN tmp_etl_candidate_movimentos tm
    ON tm.id_empresa = i.id_empresa
   AND tm.id_filial = i.id_filial
   AND tm.id_db = i.id_db
   AND tm.id_movprodutos = i.id_movprodutos
  ON CONFLICT DO NOTHING;

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      i.id_itensmovprodutos,
      COALESCE(
        v.data_key,
        etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento)))
      ) AS data_key,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      COALESCE(i.id_grupo_produto_shadow, etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS')) AS id_grupo_produto,
      COALESCE(i.id_local_venda_shadow, etl.safe_int(i.payload->>'ID_LOCALVENDAS')) AS id_local_venda,
      COALESCE(i.id_funcionario_shadow, etl.safe_int(i.payload->>'ID_FUNCIONARIOS')) AS id_funcionario,
      COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP')) AS cfop,
      COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)) AS qtd,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS valor_unitario,
      COALESCE(i.total_shadow, etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2)) AS total,
      COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2)) AS desconto,
      COALESCE(
        (
          etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6)
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2),
        (dp.custo_medio * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6)))::numeric(18,2)
      ) AS custo_total,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS preco_praticado_unitario,
      NULL::numeric(18,4) AS preco_lista_unitario,
      CASE
        WHEN COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')) > 0
          THEN (
            COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2))
            / NULLIF(COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)), 0)
          )::numeric(18,4)
        ELSE NULL::numeric(18,4)
      END AS desconto_unitario,
      COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2)) AS desconto_total,
      CASE
        WHEN COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2), 0) > 0
          THEN 'payload_explicit_discount'
        ELSE NULL
      END AS discount_source,
      i.payload
    FROM stg.itensmovprodutos i
    JOIN tmp_etl_candidate_itens ti
      ON ti.id_empresa = i.id_empresa
     AND ti.id_filial = i.id_filial
     AND ti.id_db = i.id_db
     AND ti.id_movprodutos = i.id_movprodutos
     AND ti.id_itensmovprodutos = i.id_itensmovprodutos
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_movprodutos = i.id_movprodutos
    LEFT JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa
     AND m.id_filial = i.id_filial
     AND m.id_db = i.id_db
     AND m.id_movprodutos = i.id_movprodutos
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,margem,
      preco_lista_unitario,preco_praticado_unitario,desconto_unitario,desconto_total,discount_source,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,(COALESCE(total,0) - COALESCE(custo_total,0))::numeric(18,2),
      preco_lista_unitario,preco_praticado_unitario,desconto_unitario,desconto_total,discount_source,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
    DO UPDATE SET
      data_key = EXCLUDED.data_key,
      id_produto = EXCLUDED.id_produto,
      id_grupo_produto = EXCLUDED.id_grupo_produto,
      id_local_venda = EXCLUDED.id_local_venda,
      id_funcionario = EXCLUDED.id_funcionario,
      cfop = EXCLUDED.cfop,
      qtd = EXCLUDED.qtd,
      valor_unitario = EXCLUDED.valor_unitario,
      total = EXCLUDED.total,
      desconto = EXCLUDED.desconto,
      custo_total = EXCLUDED.custo_total,
      margem = EXCLUDED.margem,
      preco_lista_unitario = EXCLUDED.preco_lista_unitario,
      preco_praticado_unitario = EXCLUDED.preco_praticado_unitario,
      desconto_unitario = EXCLUDED.desconto_unitario,
      desconto_total = EXCLUDED.desconto_total,
      discount_source = EXCLUDED.discount_source,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
      OR dw.fact_venda_item.desconto_total IS DISTINCT FROM EXCLUDED.desconto_total
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.itensmovprodutos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'itensmovprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_comp_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'formas_pgto_comprovantes'), '1970-01-01'::timestamptz);
  v_comp_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_pag_refs;
  CREATE TEMP TABLE tmp_etl_candidate_pag_refs (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    referencia bigint NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, referencia)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_pag_refs
  SELECT
    s.id_empresa,
    s.id_filial,
    s.id_referencia
  FROM stg.formas_pgto_comprovantes s
  WHERE s.id_empresa = p_id_empresa
    AND COALESCE(
      etl.business_date(
        etl.coalesce_operational_timestamptz(
          s.dt_evento,
          s.payload->>'TORQMIND_DT_EVENTO',
          s.payload->>'DT_EVENTO',
          s.payload->>'DATAREPL',
          s.payload->>'DATAHORA',
          s.payload->>'DATA'
        )
      ),
      CURRENT_DATE
    ) >= v_cutoff
    AND (
      s.received_at > v_wm
      OR (s.dt_evento IS NOT NULL AND s.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  INSERT INTO tmp_etl_candidate_pag_refs
  SELECT
    c.id_empresa,
    c.id_filial,
    c.referencia_shadow
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND c.referencia_shadow IS NOT NULL
    AND etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)) >= v_cutoff
    AND (
      c.received_at > v_comp_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH src_raw AS (
    SELECT
      s.id_empresa,
      s.id_filial,
      s.id_referencia AS referencia,
      s.tipo_forma AS tipo_forma,
      COALESCE(
        s.id_db_shadow,
        etl.safe_int(s.payload->>'ID_DB'),
        etl.safe_int(s.payload->>'id_db')
      ) AS id_db,
      COALESCE(
        s.valor_shadow,
        etl.safe_numeric(s.payload->>'VALOR')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VALOR_PAGO')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VALORPAGO')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VLR')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VLR_PAGO')::numeric(18,2),
        etl.safe_numeric(s.payload->>'VLRPAGO')::numeric(18,2),
        0::numeric(18,2)
      ) AS valor,
      etl.coalesce_operational_timestamptz(
        s.dt_evento,
        s.payload->>'TORQMIND_DT_EVENTO',
        s.payload->>'DT_EVENTO',
        s.payload->>'DATAREPL',
        s.payload->>'DATAHORA',
        s.payload->>'DATA'
      ) AS dt_evento_src,
      COALESCE(s.nsu_shadow, s.payload->>'NSU', s.payload->>'nsu') AS nsu,
      COALESCE(s.autorizacao_shadow, s.payload->>'AUTORIZACAO', s.payload->>'autorizacao') AS autorizacao,
      COALESCE(s.bandeira_shadow, s.payload->>'BANDEIRA', s.payload->>'bandeira') AS bandeira,
      COALESCE(s.rede_shadow, s.payload->>'REDE', s.payload->>'rede') AS rede,
      COALESCE(s.tef_shadow, s.payload->>'TEF', s.payload->>'tef') AS tef,
      s.payload,
      s.received_at
    FROM stg.formas_pgto_comprovantes s
    JOIN tmp_etl_candidate_pag_refs r
      ON r.id_empresa = s.id_empresa
     AND r.id_filial = s.id_filial
     AND r.referencia = s.id_referencia
    WHERE s.id_empresa = p_id_empresa
  ), comp_ref AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.referencia_shadow AS referencia,
      c.id_comprovante AS id_comprovante,
      c.id_db AS id_db,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      etl.sales_event_timestamptz(c.payload, c.dt_evento) AS data_comp,
      etl.comprovante_data_conta(c.payload, NULL) AS data_conta,
      etl.comprovante_cash_eligible(
        etl.sales_business_ts(c.payload, c.dt_evento),
        etl.comprovante_data_conta(c.payload, NULL),
        COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS'))
      ) AS cash_eligible,
      row_number() OVER (
        PARTITION BY c.id_empresa, c.id_filial, c.referencia_shadow
        ORDER BY c.received_at DESC
      ) AS rn
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_pag_refs r
      ON r.id_empresa = c.id_empresa
     AND r.id_filial = c.id_filial
     AND r.referencia = c.referencia_shadow
    WHERE c.id_empresa = p_id_empresa
      AND c.referencia_shadow IS NOT NULL
  ), src AS (
    SELECT
      r.id_empresa,
      r.id_filial,
      r.referencia,
      COALESCE(cr.id_db, r.id_db) AS id_db,
      cr.id_comprovante,
      cr.id_turno,
      cr.id_usuario,
      r.tipo_forma,
      r.valor,
      COALESCE(cr.data_comp, r.dt_evento_src, r.received_at) AS dt_evento,
      etl.business_date_key(COALESCE(cr.data_comp, r.dt_evento_src, r.received_at)) AS data_key,
      cr.data_conta,
      COALESCE(cr.cash_eligible, false) AS cash_eligible,
      r.nsu,
      r.autorizacao,
      r.bandeira,
      r.rede,
      r.tef,
      r.payload
    FROM src_raw r
    LEFT JOIN comp_ref cr
      ON cr.id_empresa = r.id_empresa
     AND cr.id_filial = r.id_filial
     AND cr.referencia = r.referencia
     AND cr.rn = 1
    WHERE COALESCE(etl.business_date(cr.data_comp), etl.business_date(r.dt_evento_src), CURRENT_DATE) >= v_cutoff
  ), upserted AS (
    INSERT INTO dw.fact_pagamento_comprovante (
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      data_conta,cash_eligible,nsu,autorizacao,bandeira,rede,tef,payload
    )
    SELECT
      id_empresa,id_filial,referencia,id_db,id_comprovante,id_turno,id_usuario,tipo_forma,valor,dt_evento,data_key,
      data_conta,cash_eligible,nsu,autorizacao,bandeira,rede,tef,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,referencia,tipo_forma)
    DO UPDATE SET
      id_db = EXCLUDED.id_db,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      valor = EXCLUDED.valor,
      dt_evento = EXCLUDED.dt_evento,
      data_key = EXCLUDED.data_key,
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      nsu = EXCLUDED.nsu,
      autorizacao = EXCLUDED.autorizacao,
      bandeira = EXCLUDED.bandeira,
      rede = EXCLUDED.rede,
      tef = EXCLUDED.tef,
      payload = EXCLUDED.payload,
      updated_at = now()
    WHERE
      dw.fact_pagamento_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_pagamento_comprovante.valor IS DISTINCT FROM EXCLUDED.valor
      OR dw.fact_pagamento_comprovante.dt_evento IS DISTINCT FROM EXCLUDED.dt_evento
      OR dw.fact_pagamento_comprovante.data_conta IS DISTINCT FROM EXCLUDED.data_conta
      OR dw.fact_pagamento_comprovante.cash_eligible IS DISTINCT FROM EXCLUDED.cash_eligible
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.formas_pgto_comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'formas_pgto_comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

WITH base AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante,
    etl.sales_business_ts(c.payload, c.dt_evento) AS data,
    etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
    COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
    COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
    COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
    COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS valor_total,
    COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS raw_cancelado,
    COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')) AS situacao,
    etl.comprovante_data_conta(c.payload, NULL) AS data_conta,
    c.payload
  FROM stg.comprovantes c
), normalized AS (
  SELECT
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    data,
    data_key,
    id_usuario,
    id_turno,
    id_cliente,
    valor_total,
    etl.comprovante_is_cancelled(raw_cancelado, situacao) AS cancelado,
    situacao,
    data_conta,
    etl.comprovante_cash_eligible(data, data_conta, id_turno) AS cash_eligible,
    payload
  FROM base
)
UPDATE dw.fact_comprovante d
SET data = n.data,
    data_key = n.data_key,
    id_usuario = n.id_usuario,
    id_turno = n.id_turno,
    id_cliente = n.id_cliente,
    valor_total = n.valor_total,
    cancelado = n.cancelado,
    situacao = n.situacao,
    data_conta = n.data_conta,
    cash_eligible = n.cash_eligible,
    payload = n.payload,
    updated_at = now()
FROM normalized n
WHERE d.id_empresa = n.id_empresa
  AND d.id_filial = n.id_filial
  AND d.id_db = n.id_db
  AND d.id_comprovante = n.id_comprovante
  AND (
    d.data IS DISTINCT FROM n.data
    OR d.data_key IS DISTINCT FROM n.data_key
    OR d.id_usuario IS DISTINCT FROM n.id_usuario
    OR d.id_turno IS DISTINCT FROM n.id_turno
    OR d.id_cliente IS DISTINCT FROM n.id_cliente
    OR d.valor_total IS DISTINCT FROM n.valor_total
    OR d.cancelado IS DISTINCT FROM n.cancelado
    OR d.situacao IS DISTINCT FROM n.situacao
    OR d.data_conta IS DISTINCT FROM n.data_conta
    OR d.cash_eligible IS DISTINCT FROM n.cash_eligible
    OR d.payload IS DISTINCT FROM n.payload
  );

UPDATE dw.fact_venda v
SET cancelado = c.cancelado,
    updated_at = now()
FROM dw.fact_comprovante c
WHERE c.id_empresa = v.id_empresa
  AND c.id_filial = v.id_filial
  AND c.id_db = v.id_db
  AND c.id_comprovante = v.id_comprovante
  AND v.cancelado IS DISTINCT FROM c.cancelado;

WITH recalculated AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.id_db,
    f.id_movprodutos,
    f.id_itensmovprodutos,
    COALESCE(
      (
        etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6)
        * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
      )::numeric(18,2),
      (dp.custo_medio * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6)))::numeric(18,2)
    ) AS custo_total
  FROM dw.fact_venda_item f
  JOIN stg.itensmovprodutos i
    ON i.id_empresa = f.id_empresa
   AND i.id_filial = f.id_filial
   AND i.id_db = f.id_db
   AND i.id_movprodutos = f.id_movprodutos
   AND i.id_itensmovprodutos = f.id_itensmovprodutos
  LEFT JOIN dw.dim_produto dp
    ON dp.id_empresa = i.id_empresa
   AND dp.id_filial = i.id_filial
   AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
)
UPDATE dw.fact_venda_item f
SET custo_total = r.custo_total,
    margem = (COALESCE(f.total, 0) - COALESCE(r.custo_total, 0))::numeric(18,2),
    updated_at = now()
FROM recalculated r
WHERE f.id_empresa = r.id_empresa
  AND f.id_filial = r.id_filial
  AND f.id_db = r.id_db
  AND f.id_movprodutos = r.id_movprodutos
  AND f.id_itensmovprodutos = r.id_itensmovprodutos
  AND (
    f.custo_total IS DISTINCT FROM r.custo_total
    OR f.margem IS DISTINCT FROM (COALESCE(f.total, 0) - COALESCE(r.custo_total, 0))::numeric(18,2)
  );

WITH comp_link AS (
  SELECT
    p.id_empresa,
    p.id_filial,
    p.referencia,
    p.tipo_forma,
    c.data_conta,
    c.cash_eligible,
    row_number() OVER (
      PARTITION BY p.id_empresa, p.id_filial, p.referencia, p.tipo_forma
      ORDER BY CASE
        WHEN p.id_db IS NOT NULL AND c.id_db = p.id_db THEN 0
        ELSE 1
      END,
      c.updated_at DESC
    ) AS rn
  FROM dw.fact_pagamento_comprovante p
  JOIN dw.fact_comprovante c
    ON c.id_empresa = p.id_empresa
   AND c.id_filial = p.id_filial
   AND c.id_comprovante = p.id_comprovante
)
UPDATE dw.fact_pagamento_comprovante p
SET data_conta = cl.data_conta,
    cash_eligible = cl.cash_eligible,
    updated_at = now()
FROM comp_link cl
WHERE cl.rn = 1
  AND p.id_empresa = cl.id_empresa
  AND p.id_filial = cl.id_filial
  AND p.referencia = cl.referencia
  AND p.tipo_forma = cl.tipo_forma
  AND (
    p.data_conta IS DISTINCT FROM cl.data_conta
    OR p.cash_eligible IS DISTINCT FROM cl.cash_eligible
  );

DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_forma_pagamento CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_turno_aberto CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_caixa_cancelamentos CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.fraude_cancelamentos_eventos CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.fraude_cancelamentos_diaria CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.pagamentos_anomalias_diaria CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart.agg_pagamentos_turno CASCADE;

CREATE MATERIALIZED VIEW mart.agg_pagamentos_turno AS
WITH labeled AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    COALESCE(f.id_turno, -1) AS id_turno,
    f.referencia,
    f.tipo_forma,
    f.valor,
    COALESCE(m.label, 'NÃO IDENTIFICADO') AS label,
    COALESCE(m.category, 'NAO_IDENTIFICADO') AS category
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT label, category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
  WHERE etl.resolve_cash_eligible(f.cash_eligible, f.dt_evento, f.data_conta, f.id_turno)
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  id_turno,
  tipo_forma,
  category,
  label,
  COALESCE(SUM(valor),0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM labeled
GROUP BY id_empresa, id_filial, data_key, id_turno, tipo_forma, category, label;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_pagamentos_turno
  ON mart.agg_pagamentos_turno (id_empresa, id_filial, data_key, id_turno, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_pagamentos_turno_lookup
  ON mart.agg_pagamentos_turno (id_empresa, data_key, id_filial, id_turno);

CREATE MATERIALIZED VIEW mart.pagamentos_anomalias_diaria AS
WITH base_ref AS (
  SELECT
    f.id_empresa,
    f.id_filial,
    f.data_key,
    f.referencia,
    COUNT(*)::int AS qtd_formas,
    COUNT(*) FILTER (WHERE COALESCE(m.category, 'NAO_IDENTIFICADO') = 'NAO_IDENTIFICADO')::int AS qtd_desconhecido,
    COALESCE(SUM(f.valor),0)::numeric(18,2) AS valor_total,
    COALESCE(SUM(CASE WHEN COALESCE(m.category, 'NAO_IDENTIFICADO') = 'PIX' THEN f.valor ELSE 0 END),0)::numeric(18,2) AS valor_pix,
    COALESCE(MIN(f.id_turno), -1) AS id_turno
  FROM dw.fact_pagamento_comprovante f
  LEFT JOIN LATERAL (
    SELECT category
    FROM app.payment_type_map m
    WHERE m.tipo_forma = f.tipo_forma
      AND m.active = true
      AND (m.id_empresa = f.id_empresa OR m.id_empresa IS NULL)
    ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
    LIMIT 1
  ) m ON true
  WHERE etl.resolve_cash_eligible(f.cash_eligible, f.dt_evento, f.data_conta, f.id_turno)
  GROUP BY 1,2,3,4
), split_daily AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    AVG(qtd_formas)::numeric(10,2) AS avg_formas,
    COUNT(*) FILTER (WHERE qtd_formas >= 3)::int AS comprovantes_multiplos,
    COUNT(*)::int AS comprovantes_total,
    COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
  FROM base_ref
  GROUP BY 1,2,3
), split_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    NULL::int AS id_turno,
    'SPLIT_INCOMUM'::text AS event_type,
    CASE WHEN avg_formas >= 2.4 THEN 'CRITICAL' WHEN avg_formas >= 1.8 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, ROUND((avg_formas - 1.4) * 55 + comprovantes_multiplos * 0.8)))::int AS score,
    COALESCE(valor_total,0)::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'avg_formas_por_comprovante', avg_formas,
      'comprovantes_multiplos', comprovantes_multiplos,
      'comprovantes_total', comprovantes_total
    ) AS reasons
  FROM split_daily
  WHERE comprovantes_total >= 20
), unknown_signal AS (
  SELECT
    id_empresa,
    id_filial,
    data_key,
    id_turno,
    'FORMA_NAO_IDENTIFICADA'::text AS event_type,
    CASE WHEN qtd_desconhecido >= 4 THEN 'CRITICAL' WHEN qtd_desconhecido >= 2 THEN 'WARN' ELSE 'INFO' END AS severity,
    LEAST(100, GREATEST(0, qtd_desconhecido * 18 + (valor_total / 500)::int))::int AS score,
    valor_total AS impacto_estimado,
    jsonb_build_object(
      'qtd_formas_nao_identificadas', qtd_desconhecido,
      'valor_total', valor_total
    ) AS reasons
  FROM (
    SELECT
      id_empresa,
      id_filial,
      data_key,
      id_turno,
      SUM(qtd_desconhecido)::int AS qtd_desconhecido,
      COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
    FROM base_ref
    WHERE qtd_desconhecido > 0
    GROUP BY 1,2,3,4
  ) u
), pix_signal AS (
  SELECT
    b.id_empresa,
    b.id_filial,
    b.data_key,
    b.id_turno,
    'PIX_DESVIO_TURNO'::text AS event_type,
    CASE WHEN (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.80 THEN 'CRITICAL'
         WHEN (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.60 THEN 'WARN'
         ELSE 'INFO'
    END AS severity,
    LEAST(100, GREATEST(0, ROUND((b.valor_pix / NULLIF(b.valor_total, 0)) * 100)))::int AS score,
    b.valor_pix::numeric(18,2) AS impacto_estimado,
    jsonb_build_object(
      'valor_pix', b.valor_pix,
      'valor_total', b.valor_total,
      'share_pix', ROUND((b.valor_pix / NULLIF(b.valor_total, 0))::numeric, 4)
    ) AS reasons
  FROM (
    SELECT
      id_empresa,
      id_filial,
      data_key,
      id_turno,
      COALESCE(SUM(valor_pix),0)::numeric(18,2) AS valor_pix,
      COALESCE(SUM(valor_total),0)::numeric(18,2) AS valor_total
    FROM base_ref
    GROUP BY 1,2,3,4
  ) b
  WHERE b.valor_pix > 0
    AND b.valor_total > 0
    AND (b.valor_pix / NULLIF(b.valor_total, 0)) >= 0.60
)
SELECT
  s.id_empresa,
  s.id_filial,
  s.data_key,
  s.id_turno,
  s.event_type,
  s.severity,
  s.score,
  s.impacto_estimado,
  s.reasons,
  md5(concat_ws('|', s.id_empresa::text, s.id_filial::text, s.data_key::text, COALESCE(s.id_turno,-1)::text, s.event_type)) AS insight_id_hash,
  NULL::bigint AS insight_id,
  now() AS updated_at
FROM (
  SELECT * FROM split_signal
  UNION ALL
  SELECT * FROM unknown_signal
  UNION ALL
  SELECT * FROM pix_signal
) s;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_pagamentos_anomalias_diaria
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, event_type, COALESCE(id_turno,-1));
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_lookup
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, data_key, severity, score DESC);
CREATE INDEX IF NOT EXISTS ix_mart_pagamentos_anomalias_insight
  ON mart.pagamentos_anomalias_diaria (id_empresa, id_filial, insight_id_hash);

CREATE MATERIALIZED VIEW mart.fraude_cancelamentos_diaria AS
WITH cancelamentos_operacionais AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.data_key,
    c.valor_total,
    etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num,
    COALESCE(t.id_turno, c.id_turno) AS id_turno
  FROM dw.fact_comprovante c
  LEFT JOIN dw.fact_caixa_turno t
    ON t.id_empresa = c.id_empresa
   AND t.id_filial = c.id_filial
   AND t.id_turno = c.id_turno
   AND (t.data_key_abertura IS NULL OR t.data_key_abertura <= c.data_key)
   AND (
         t.data_key_fechamento IS NULL
         OR t.data_key_fechamento >= c.data_key
         OR t.is_aberto = true
       )
  WHERE c.cancelado = true
    AND c.data_key IS NOT NULL
    AND etl.resolve_cash_eligible(c.cash_eligible, c.data, c.data_conta, c.id_turno)
    AND COALESCE(t.id_turno, c.id_turno) IS NOT NULL
)
SELECT
  id_empresa,
  id_filial,
  data_key,
  COUNT(*)::int AS cancelamentos,
  COALESCE(SUM(valor_total), 0)::numeric(18,2) AS valor_cancelado,
  now() AS updated_at
FROM cancelamentos_operacionais
WHERE cfop_num > 5000
GROUP BY 1,2,3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_fraude_cancelamentos_diaria
  ON mart.fraude_cancelamentos_diaria (id_empresa, id_filial, data_key);
CREATE INDEX IF NOT EXISTS ix_mart_fraude_cancelamentos_diaria_lookup
  ON mart.fraude_cancelamentos_diaria (id_empresa, data_key, id_filial);

CREATE MATERIALIZED VIEW mart.fraude_cancelamentos_eventos AS
WITH cancelamentos_operacionais AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante,
    c.data,
    c.data_key,
    COALESCE(t.id_turno, c.id_turno) AS id_turno,
    COALESCE(t.id_usuario, c.id_usuario) AS id_usuario,
    c.id_usuario AS id_usuario_documento,
    CASE
      WHEN t.id_usuario IS NOT NULL THEN 'turno'
      WHEN c.id_usuario IS NOT NULL THEN 'comprovante'
      ELSE 'indefinido'
    END AS usuario_source,
    COALESCE(
      NULLIF(u.nome, ''),
      NULLIF(t.payload->>'NOMEUSUARIOS', ''),
      NULLIF(t.payload->>'NOME_USUARIOS', ''),
      NULLIF(t.payload->>'NOMEUSUARIO', ''),
      NULLIF(t.payload->>'NOME_USUARIO', ''),
      CASE
        WHEN COALESCE(t.id_usuario, c.id_usuario) IS NOT NULL THEN format('Operador %s', COALESCE(t.id_usuario, c.id_usuario))
        ELSE NULL
      END
    ) AS usuario_nome,
    c.valor_total,
    etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num,
    now() AS updated_at
  FROM dw.fact_comprovante c
  LEFT JOIN dw.fact_caixa_turno t
    ON t.id_empresa = c.id_empresa
   AND t.id_filial = c.id_filial
   AND t.id_turno = c.id_turno
   AND (t.data_key_abertura IS NULL OR t.data_key_abertura <= c.data_key)
   AND (
         t.data_key_fechamento IS NULL
         OR t.data_key_fechamento >= c.data_key
         OR t.is_aberto = true
       )
  LEFT JOIN dw.dim_usuario_caixa u
    ON u.id_empresa = c.id_empresa
   AND u.id_filial = c.id_filial
   AND u.id_usuario = COALESCE(t.id_usuario, c.id_usuario)
  WHERE c.cancelado = true
    AND etl.resolve_cash_eligible(c.cash_eligible, c.data, c.data_conta, c.id_turno)
    AND COALESCE(t.id_turno, c.id_turno) IS NOT NULL
)
SELECT
  id_empresa,
  id_filial,
  id_db,
  id_comprovante,
  data,
  data_key,
  id_turno,
  id_usuario,
  id_usuario_documento,
  usuario_source,
  usuario_nome,
  valor_total,
  updated_at
FROM cancelamentos_operacionais
WHERE cfop_num > 5000;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_fraude_cancelamentos_eventos
  ON mart.fraude_cancelamentos_eventos (id_empresa, id_filial, id_db, id_comprovante);
CREATE INDEX IF NOT EXISTS ix_mart_fraude_eventos_dt
  ON mart.fraude_cancelamentos_eventos (id_empresa, data DESC);

CREATE MATERIALIZED VIEW mart.agg_caixa_turno_aberto AS
WITH runtime AS (
  SELECT etl.runtime_now() AS clock_ts
), comprovantes_caixa AS (
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_turno,
    MAX(c.data) AS last_sale_ts,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool), 0)::numeric(18,2) AS total_vendas,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND NOT cancelado_bool)::int AS qtd_vendas,
    COALESCE(SUM(c.valor_total) FILTER (WHERE cfop_num > 5000 AND cancelado_bool), 0)::numeric(18,2) AS total_cancelamentos,
    COUNT(*) FILTER (WHERE cfop_num > 5000 AND cancelado_bool)::int AS qtd_cancelamentos
  FROM (
    SELECT
      fc.id_empresa,
      fc.id_filial,
      fc.id_turno,
      fc.data,
      fc.valor_total,
      COALESCE(fc.cancelado, false) AS cancelado_bool,
      etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
    FROM dw.fact_comprovante fc
    WHERE etl.resolve_cash_eligible(fc.cash_eligible, fc.data, fc.data_conta, fc.id_turno)
  ) c
  GROUP BY c.id_empresa, c.id_filial, c.id_turno
), pagamentos_turno AS (
  SELECT
    p.id_empresa,
    p.id_filial,
    p.id_turno,
    MAX(p.dt_evento) AS last_payment_ts,
    COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
  FROM dw.fact_pagamento_comprovante p
  WHERE etl.resolve_cash_eligible(p.cash_eligible, p.dt_evento, p.data_conta, p.id_turno)
  GROUP BY p.id_empresa, p.id_filial, p.id_turno
)
SELECT
  t.id_empresa,
  t.id_filial,
  COALESCE(f.nome, '') AS filial_nome,
  t.id_turno,
  t.id_usuario,
  COALESCE(
    NULLIF(u.nome, ''),
    NULLIF(t.payload->>'NOMEUSUARIOS', ''),
    NULLIF(t.payload->>'NOME_USUARIOS', ''),
    NULLIF(t.payload->>'NOMEUSUARIO', ''),
    NULLIF(t.payload->>'NOME_USUARIO', ''),
    CASE WHEN t.id_usuario IS NOT NULL THEN format('Operador %s', t.id_usuario) ELSE NULL END
  ) AS usuario_nome,
  CASE
    WHEN NULLIF(u.nome, '') IS NOT NULL THEN 'usuarios'
    WHEN COALESCE(
      NULLIF(t.payload->>'NOMEUSUARIOS', ''),
      NULLIF(t.payload->>'NOME_USUARIOS', ''),
      NULLIF(t.payload->>'NOMEUSUARIO', ''),
      NULLIF(t.payload->>'NOME_USUARIO', '')
    ) IS NOT NULL THEN 'turnos_payload'
    WHEN t.id_usuario IS NOT NULL THEN 'turno_id'
    ELSE 'indefinido'
  END AS usuario_source,
  t.abertura_ts,
  t.fechamento_ts,
  GREATEST(
    COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
    COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
    COALESCE(t.abertura_ts, '-infinity'::timestamptz)
  ) AS last_activity_ts,
  ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2)::numeric(10,2) AS horas_aberto,
  ROUND(
    EXTRACT(
      EPOCH FROM (
        runtime.clock_ts
        - GREATEST(
            COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
            COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
            COALESCE(t.abertura_ts, '-infinity'::timestamptz)
          )
      )
    ) / 3600.0,
    2
  )::numeric(10,2) AS horas_sem_movimento,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) < runtime.clock_ts - interval '96 hour' THEN true
    ELSE false
  END AS is_stale,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) >= runtime.clock_ts - interval '96 hour' THEN true
    ELSE false
  END AS is_operational_live,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) < runtime.clock_ts - interval '96 hour' THEN 'STALE'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'CRITICAL'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'HIGH'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  CASE
    WHEN GREATEST(
      COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
      COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
      COALESCE(t.abertura_ts, '-infinity'::timestamptz)
    ) < runtime.clock_ts - interval '96 hour' THEN 'Stale'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'Crítico'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'Atenção alta'
    WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'Monitorar'
    ELSE 'Dentro da janela'
  END AS status_label,
  COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
  COALESCE(c.qtd_vendas, 0)::int AS qtd_vendas,
  COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
  COALESCE(c.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
  COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos,
  runtime.clock_ts AS snapshot_ts,
  runtime.clock_ts AS updated_at
FROM dw.fact_caixa_turno t
CROSS JOIN runtime
LEFT JOIN auth.filiais f
  ON f.id_empresa = t.id_empresa
 AND f.id_filial = t.id_filial
LEFT JOIN dw.dim_usuario_caixa u
  ON u.id_empresa = t.id_empresa
 AND u.id_filial = t.id_filial
 AND u.id_usuario = t.id_usuario
LEFT JOIN comprovantes_caixa c
  ON c.id_empresa = t.id_empresa
 AND c.id_filial = t.id_filial
 AND c.id_turno = t.id_turno
LEFT JOIN pagamentos_turno p
  ON p.id_empresa = t.id_empresa
 AND p.id_filial = t.id_filial
 AND p.id_turno = t.id_turno
WHERE t.is_aberto = true
  AND t.abertura_ts IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_turno_aberto
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_turno_aberto_lookup
  ON mart.agg_caixa_turno_aberto (id_empresa, id_filial, is_operational_live, severity, horas_aberto DESC);

CREATE MATERIALIZED VIEW mart.agg_caixa_forma_pagamento AS
SELECT
  a.id_empresa,
  a.id_filial,
  a.id_turno,
  p.tipo_forma,
  COALESCE(m.label, 'NÃO IDENTIFICADO') AS forma_label,
  COALESCE(m.category, 'NAO_IDENTIFICADO') AS forma_category,
  COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_valor,
  COUNT(DISTINCT p.referencia)::int AS qtd_comprovantes,
  now() AS updated_at
FROM mart.agg_caixa_turno_aberto a
JOIN dw.fact_pagamento_comprovante p
  ON p.id_empresa = a.id_empresa
 AND p.id_filial = a.id_filial
 AND p.id_turno = a.id_turno
 AND etl.resolve_cash_eligible(p.cash_eligible, p.dt_evento, p.data_conta, p.id_turno)
LEFT JOIN LATERAL (
  SELECT label, category
  FROM app.payment_type_map m
  WHERE m.tipo_forma = p.tipo_forma
    AND m.active = true
    AND (m.id_empresa = p.id_empresa OR m.id_empresa IS NULL)
  ORDER BY CASE WHEN m.id_empresa IS NULL THEN 1 ELSE 0 END, m.updated_at DESC
  LIMIT 1
) m ON true
WHERE a.is_operational_live = true
GROUP BY a.id_empresa, a.id_filial, a.id_turno, p.tipo_forma, COALESCE(m.label, 'NÃO IDENTIFICADO'), COALESCE(m.category, 'NAO_IDENTIFICADO');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_forma_pagamento
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, id_turno, tipo_forma);
CREATE INDEX IF NOT EXISTS ix_mart_agg_caixa_forma_pagamento_lookup
  ON mart.agg_caixa_forma_pagamento (id_empresa, id_filial, total_valor DESC);

CREATE MATERIALIZED VIEW mart.agg_caixa_cancelamentos AS
SELECT
  c.id_empresa,
  c.id_filial,
  c.id_turno,
  COALESCE(f.nome, '') AS filial_nome,
  COALESCE(SUM(c.valor_total), 0)::numeric(18,2) AS total_cancelamentos,
  COUNT(*)::int AS qtd_cancelamentos,
  now() AS updated_at
FROM (
  SELECT
    fc.id_empresa,
    fc.id_filial,
    fc.id_turno,
    fc.valor_total,
    etl.safe_int(NULLIF(regexp_replace(COALESCE(fc.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) AS cfop_num
  FROM dw.fact_comprovante fc
  WHERE COALESCE(fc.cancelado, false) = true
    AND etl.resolve_cash_eligible(fc.cash_eligible, fc.data, fc.data_conta, fc.id_turno)
) c
LEFT JOIN auth.filiais f
  ON f.id_empresa = c.id_empresa
 AND f.id_filial = c.id_filial
WHERE c.cfop_num > 5000
GROUP BY c.id_empresa, c.id_filial, c.id_turno, f.nome;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_agg_caixa_cancelamentos
  ON mart.agg_caixa_cancelamentos (id_empresa, id_filial, id_turno);

CREATE MATERIALIZED VIEW mart.alerta_caixa_aberto AS
SELECT
  a.id_empresa,
  a.id_filial,
  a.filial_nome,
  a.id_turno,
  a.id_usuario,
  a.usuario_nome,
  a.abertura_ts,
  a.last_activity_ts,
  a.horas_aberto,
  'CRITICAL'::text AS severity,
  format('Caixa %s aberto há %s horas', a.id_turno, trim(to_char(a.horas_aberto, 'FM999999990D00'))) AS title,
  format(
    'O caixa %s da filial %s segue aberto na última leitura operacional. Operador: %s. Última atividade observada: %s.',
    a.id_turno,
    COALESCE(NULLIF(a.filial_nome, ''), format('Filial %s', a.id_filial)),
    COALESCE(NULLIF(a.usuario_nome, ''), 'não identificado'),
    COALESCE(to_char(a.last_activity_ts, 'DD/MM/YYYY HH24:MI'), 'sem atividade observada')
  ) AS body,
  '/cash'::text AS url,
  (
    ('x' || substr(md5(
      'CASH_OPEN_OVER_24H|' || a.id_empresa::text || '|' || a.id_filial::text || '|' || a.id_turno::text
    ), 1, 16))::bit(64)::bigint
  ) AS insight_id_hash,
  now() AS updated_at
FROM mart.agg_caixa_turno_aberto a
WHERE a.is_operational_live = true
  AND a.horas_aberto >= 24;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_alerta_caixa_aberto
  ON mart.alerta_caixa_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_alerta_caixa_aberto_lookup
  ON mart.alerta_caixa_aberto (id_empresa, severity, horas_aberto DESC);

REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria;
REFRESH MATERIALIZED VIEW mart.insights_base_diaria;
REFRESH MATERIALIZED VIEW mart.agg_vendas_hora;
REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria;
REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria;
REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria;
REFRESH MATERIALIZED VIEW mart.agg_pagamentos_diaria;
REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno;
REFRESH MATERIALIZED VIEW mart.pagamentos_anomalias_diaria;
REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria;
REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos;
REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
REFRESH MATERIALIZED VIEW mart.agg_caixa_cancelamentos;
REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;

DO $$
BEGIN
  PERFORM etl.rebuild_health_score_daily();
END;
$$;

COMMIT;
