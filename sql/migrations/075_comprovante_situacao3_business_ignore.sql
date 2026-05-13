BEGIN;

ALTER TABLE dw.fact_comprovante
  ADD COLUMN IF NOT EXISTS ignored_business boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS commercial_eligible boolean NOT NULL DEFAULT true;

ALTER TABLE dw.fact_venda
  ADD COLUMN IF NOT EXISTS commercial_eligible boolean NOT NULL DEFAULT true;

CREATE OR REPLACE FUNCTION etl.comprovante_situacao(p_situacao integer)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_situacao;
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
    WHEN etl.comprovante_situacao(p_situacao) = 2 THEN true
    ELSE COALESCE(p_cancelado, false)
  END;
$$;

CREATE OR REPLACE FUNCTION etl.comprovante_is_ignored_business(p_situacao integer)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT etl.comprovante_situacao(p_situacao) = 3;
$$;

CREATE OR REPLACE FUNCTION etl.comprovante_is_business_eligible(
  p_cancelado boolean,
  p_situacao integer
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NOT etl.comprovante_is_cancelled(p_cancelado, p_situacao)
    AND NOT etl.comprovante_is_ignored_business(p_situacao);
$$;

UPDATE dw.fact_comprovante fc
SET
  cancelado = src.cancelado,
  ignored_business = src.ignored_business,
  commercial_eligible = src.commercial_eligible,
  cash_eligible = CASE
    WHEN src.commercial_eligible THEN etl.comprovante_cash_eligible(fc.data, fc.data_conta, fc.id_turno)
    ELSE false
  END
FROM (
  SELECT
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    etl.comprovante_is_cancelled(cancelado, situacao) AS cancelado,
    etl.comprovante_is_ignored_business(situacao) AS ignored_business,
    etl.comprovante_is_business_eligible(cancelado, situacao) AS commercial_eligible
  FROM dw.fact_comprovante
) src
WHERE fc.id_empresa = src.id_empresa
  AND fc.id_filial = src.id_filial
  AND fc.id_db = src.id_db
  AND fc.id_comprovante = src.id_comprovante
  AND (
    fc.cancelado IS DISTINCT FROM src.cancelado
    OR fc.ignored_business IS DISTINCT FROM src.ignored_business
    OR fc.commercial_eligible IS DISTINCT FROM src.commercial_eligible
    OR fc.cash_eligible IS DISTINCT FROM CASE
      WHEN src.commercial_eligible THEN etl.comprovante_cash_eligible(fc.data, fc.data_conta, fc.id_turno)
      ELSE false
    END
  );

UPDATE dw.fact_venda fv
SET
  cancelado = src.cancelado,
  commercial_eligible = src.commercial_eligible
FROM (
  SELECT
    id_empresa,
    id_filial,
    id_db,
    id_comprovante,
    etl.comprovante_is_cancelled(cancelado, situacao) AS cancelado,
    etl.comprovante_is_business_eligible(cancelado, situacao) AS commercial_eligible
  FROM dw.fact_venda
) src
WHERE fv.id_empresa = src.id_empresa
  AND fv.id_filial = src.id_filial
  AND fv.id_db = src.id_db
  AND fv.id_comprovante = src.id_comprovante
  AND (
    fv.cancelado IS DISTINCT FROM src.cancelado
    OR fv.commercial_eligible IS DISTINCT FROM src.commercial_eligible
  );

DELETE FROM dw.fact_venda_item i
USING dw.fact_venda v
WHERE v.id_empresa = i.id_empresa
  AND v.id_filial = i.id_filial
  AND v.id_db = i.id_db
  AND v.id_comprovante = i.id_comprovante
  AND COALESCE(v.commercial_eligible, false) = false;

CREATE INDEX IF NOT EXISTS ix_fact_comprovante_commercial_eligible
  ON dw.fact_comprovante (id_empresa, id_filial, data_key)
  WHERE commercial_eligible = true;

CREATE INDEX IF NOT EXISTS ix_fact_venda_commercial_eligible
  ON dw.fact_venda (id_empresa, id_filial, data_key)
  WHERE commercial_eligible = true;

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_bridge_rows integer;
  v_synced integer;
  v_deleted_items integer;
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
    AND etl.runtime_branch_matches(c.id_filial)
    AND etl.runtime_business_date_in_range(
      etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR c.received_at > v_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH base AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      c.id_comprovante,
      c.referencia_shadow AS referencia,
      c.received_at AS source_received_at,
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.sales_event_timestamptz(c.payload, c.dt_evento) AS data_comp,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS valor_total,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS raw_cancelado,
      COALESCE(
        c.situacao_shadow,
        etl.safe_int(c.payload->>'SITUACAO'),
        etl.safe_int(c.payload->>'situacao'),
        etl.safe_int(c.payload->>'STATUS'),
        etl.safe_int(c.payload->>'status')
      ) AS situacao,
      etl.comprovante_data_conta(c.payload, NULL) AS data_conta,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
  ), classified AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      referencia,
      source_received_at,
      data,
      data_comp,
      data_key,
      id_usuario,
      id_turno,
      id_cliente,
      valor_total,
      etl.comprovante_is_cancelled(raw_cancelado, situacao) AS cancelado,
      etl.comprovante_is_ignored_business(situacao) AS ignored_business,
      etl.comprovante_is_business_eligible(raw_cancelado, situacao) AS commercial_eligible,
      situacao,
      data_conta,
      payload
    FROM base
  ), src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      referencia,
      source_received_at,
      data,
      data_comp,
      data_key,
      id_usuario,
      id_turno,
      id_cliente,
      valor_total,
      cancelado,
      ignored_business,
      commercial_eligible,
      situacao,
      data_conta,
      (
        commercial_eligible
        AND etl.comprovante_cash_eligible(data, data_conta, id_turno)
      ) AS cash_eligible,
      etl.pagamento_comprovante_bridge_hash(
        id_comprovante,
        id_db,
        id_turno,
        id_usuario,
        data_comp,
        data_conta,
        (
          commercial_eligible
          AND etl.comprovante_cash_eligible(data, data_conta, id_turno)
        )
      ) AS bridge_source_hash,
      payload
    FROM classified
  ), src_bridge AS (
    SELECT DISTINCT ON (id_empresa, id_filial, referencia)
      id_empresa,
      id_filial,
      referencia,
      id_comprovante,
      id_db,
      id_turno,
      id_usuario,
      data_comp,
      data_conta,
      cash_eligible,
      source_received_at,
      bridge_source_hash
    FROM src
    WHERE referencia IS NOT NULL
    ORDER BY id_empresa, id_filial, referencia, source_received_at DESC, id_db DESC, id_comprovante DESC
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
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
      cancelado,
      ignored_business,
      commercial_eligible,
      situacao,
      data_conta,
      cash_eligible,
      payload
    )
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
      cancelado,
      ignored_business,
      commercial_eligible,
      situacao,
      data_conta,
      cash_eligible,
      payload
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
    DO UPDATE SET
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_turno = EXCLUDED.id_turno,
      id_cliente = EXCLUDED.id_cliente,
      valor_total = EXCLUDED.valor_total,
      cancelado = EXCLUDED.cancelado,
      ignored_business = EXCLUDED.ignored_business,
      commercial_eligible = EXCLUDED.commercial_eligible,
      situacao = EXCLUDED.situacao,
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_comprovante.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_comprovante.ignored_business IS DISTINCT FROM EXCLUDED.ignored_business
      OR dw.fact_comprovante.commercial_eligible IS DISTINCT FROM EXCLUDED.commercial_eligible
      OR dw.fact_comprovante.situacao IS DISTINCT FROM EXCLUDED.situacao
      OR dw.fact_comprovante.valor_total IS DISTINCT FROM EXCLUDED.valor_total
      OR dw.fact_comprovante.data_conta IS DISTINCT FROM EXCLUDED.data_conta
      OR dw.fact_comprovante.cash_eligible IS DISTINCT FROM EXCLUDED.cash_eligible
    RETURNING 1
  ), upserted_bridge AS (
    INSERT INTO etl.pagamento_comprovante_bridge (
      id_empresa,
      id_filial,
      referencia,
      id_comprovante,
      id_db,
      id_turno,
      id_usuario,
      data_comp,
      data_conta,
      cash_eligible,
      source_received_at,
      source_hash,
      updated_at
    )
    SELECT
      id_empresa,
      id_filial,
      referencia,
      id_comprovante,
      id_db,
      id_turno,
      id_usuario,
      data_comp,
      data_conta,
      cash_eligible,
      source_received_at,
      bridge_source_hash,
      now()
    FROM src_bridge
    ON CONFLICT (id_empresa, id_filial, referencia)
    DO UPDATE SET
      id_comprovante = EXCLUDED.id_comprovante,
      id_db = EXCLUDED.id_db,
      id_turno = EXCLUDED.id_turno,
      id_usuario = EXCLUDED.id_usuario,
      data_comp = EXCLUDED.data_comp,
      data_conta = EXCLUDED.data_conta,
      cash_eligible = EXCLUDED.cash_eligible,
      source_received_at = EXCLUDED.source_received_at,
      source_hash = EXCLUDED.source_hash,
      updated_at = now()
    WHERE etl.pagamento_comprovante_bridge.source_hash IS DISTINCT FROM EXCLUDED.source_hash
    RETURNING 1
  ), synced_venda AS (
    UPDATE dw.fact_venda v
    SET
      cancelado = s.cancelado,
      commercial_eligible = s.commercial_eligible
    FROM src s
    WHERE v.id_empresa = s.id_empresa
      AND v.id_filial = s.id_filial
      AND v.id_db = s.id_db
      AND v.id_comprovante = s.id_comprovante
      AND (
        v.cancelado IS DISTINCT FROM s.cancelado
        OR v.commercial_eligible IS DISTINCT FROM s.commercial_eligible
      )
    RETURNING 1
  ), deleted_ineligible_items AS (
    DELETE FROM dw.fact_venda_item i
    USING src s
    WHERE i.id_empresa = s.id_empresa
      AND i.id_filial = s.id_filial
      AND i.id_db = s.id_db
      AND i.id_comprovante = s.id_comprovante
      AND s.commercial_eligible = false
    RETURNING 1
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM upserted), 0),
    COALESCE((SELECT COUNT(*) FROM upserted_bridge), 0),
    COALESCE((SELECT COUNT(*) FROM synced_venda), 0),
    COALESCE((SELECT COUNT(*) FROM deleted_ineligible_items), 0)
  INTO v_rows, v_bridge_rows, v_synced, v_deleted_items;

  IF etl.runtime_watermark_updates_enabled() THEN
    SELECT MAX(received_at) INTO v_max
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND received_at > v_wm;

    PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  END IF;

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes_sales_fact'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  DROP TABLE IF EXISTS tmp_etl_candidate_comprovantes_sales;
  CREATE TEMP TABLE tmp_etl_candidate_comprovantes_sales (
    id_empresa int NOT NULL,
    id_filial int NOT NULL,
    id_db int NOT NULL,
    id_comprovante int NOT NULL,
    PRIMARY KEY (id_empresa, id_filial, id_db, id_comprovante)
  ) ON COMMIT DROP;

  INSERT INTO tmp_etl_candidate_comprovantes_sales
  SELECT
    c.id_empresa,
    c.id_filial,
    c.id_db,
    c.id_comprovante
  FROM stg.comprovantes c
  WHERE c.id_empresa = p_id_empresa
    AND etl.runtime_branch_matches(c.id_filial)
    AND etl.runtime_business_date_in_range(
      etl.business_date(etl.sales_event_timestamptz(c.payload, c.dt_evento)),
      v_cutoff,
      NULL::date
    )
    AND (
      etl.runtime_force_full_scan()
      OR c.received_at > v_wm
      OR (c.dt_evento IS NOT NULL AND c.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH src_base AS (
    SELECT
      c.id_empresa,
      c.id_filial,
      c.id_db,
      c.id_comprovante AS id_movprodutos,
      etl.sales_business_ts(c.payload, c.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(c.payload, c.dt_evento)) AS data_key,
      COALESCE(c.id_usuario_shadow, etl.safe_int(c.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(c.id_cliente_shadow, etl.safe_int(c.payload->>'ID_ENTIDADE')) AS id_cliente,
      c.id_comprovante,
      COALESCE(c.id_turno_shadow, etl.safe_int(c.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(etl.safe_int(c.payload->>'SAIDAS_ENTRADAS'), 0) AS saidas_entradas,
      COALESCE(c.valor_total_shadow, etl.safe_numeric(c.payload->>'VLRTOTAL')::numeric(18,2)) AS total_venda,
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS raw_cancelado,
      COALESCE(
        c.situacao_shadow,
        etl.safe_int(c.payload->>'SITUACAO'),
        etl.safe_int(c.payload->>'situacao'),
        etl.safe_int(c.payload->>'STATUS'),
        etl.safe_int(c.payload->>'status')
      ) AS situacao,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes_sales tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
  ), src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      data,
      data_key,
      id_usuario,
      id_cliente,
      id_comprovante,
      id_turno,
      saidas_entradas,
      total_venda,
      etl.comprovante_is_cancelled(raw_cancelado, situacao) AS cancelado,
      etl.comprovante_is_business_eligible(raw_cancelado, situacao) AS commercial_eligible,
      situacao,
      payload
    FROM src_base
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      data,
      data_key,
      id_usuario,
      id_cliente,
      id_comprovante,
      id_turno,
      saidas_entradas,
      total_venda,
      situacao,
      cancelado,
      commercial_eligible,
      payload
    )
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      data,
      data_key,
      id_usuario,
      id_cliente,
      id_comprovante,
      id_turno,
      saidas_entradas,
      total_venda,
      situacao,
      cancelado,
      commercial_eligible,
      payload
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
    DO UPDATE SET
      id_movprodutos = EXCLUDED.id_movprodutos,
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_cliente = EXCLUDED.id_cliente,
      id_turno = EXCLUDED.id_turno,
      saidas_entradas = EXCLUDED.saidas_entradas,
      total_venda = EXCLUDED.total_venda,
      situacao = EXCLUDED.situacao,
      cancelado = EXCLUDED.cancelado,
      commercial_eligible = EXCLUDED.commercial_eligible,
      payload = EXCLUDED.payload
    WHERE dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
      OR dw.fact_venda.id_movprodutos IS DISTINCT FROM EXCLUDED.id_movprodutos
      OR dw.fact_venda.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_venda.commercial_eligible IS DISTINCT FROM EXCLUDED.commercial_eligible
      OR dw.fact_venda.situacao IS DISTINCT FROM EXCLUDED.situacao
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  IF etl.runtime_watermark_updates_enabled() THEN
    SELECT MAX(received_at) INTO v_max
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND received_at > v_wm;

    PERFORM etl.set_watermark(
      p_id_empresa,
      'comprovantes_sales_fact',
      COALESCE(v_max, v_wm),
      NULL::bigint
    );
  END IF;

  RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item_range_detail(
  p_id_empresa int,
  p_id_comprovante_from int DEFAULT NULL,
  p_id_comprovante_to int DEFAULT NULL,
  p_update_watermark boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_cutoff date;
  v_candidate_count integer := 0;
  v_conflict_count integer := 0;
  v_upsert_inserts integer := 0;
  v_upsert_updates integer := 0;
  v_deleted_ineligible integer := 0;
  v_total_ms integer := 0;
  v_started timestamptz := clock_timestamp();
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itenscomprovantes_sales_fact'), '1970-01-01'::timestamptz);
  v_cutoff := etl.sales_cutoff_date(p_id_empresa);

  WITH src AS MATERIALIZED (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_comprovante AS id_movprodutos,
      i.id_itemcomprovante AS id_itensmovprodutos,
      i.id_comprovante,
      i.id_itemcomprovante,
      COALESCE(
        v.data_key,
        c.data_key,
        etl.business_date_key(i.dt_evento)
      ) AS data_key,
      COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS')) AS id_produto,
      etl.resolve_item_group_produto(
        i.id_grupo_produto_shadow,
        i.payload,
        dp.id_grupo_produto
      ) AS id_grupo_produto,
      COALESCE(i.id_local_venda_shadow, etl.safe_int(i.payload->>'ID_LOCALVENDAS')) AS id_local_venda,
      COALESCE(i.id_funcionario_shadow, etl.safe_int(i.payload->>'ID_FUNCIONARIOS')) AS id_funcionario,
      COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP')) AS cfop,
      COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3)) AS qtd,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS valor_unitario,
      etl.resolve_item_total(i.total_shadow, i.payload)::numeric(18,2) AS total,
      COALESCE(i.desconto_shadow, etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2)) AS desconto,
      COALESCE(
        (
          etl.item_cost_unitario(i.payload, i.custo_unitario_shadow)::numeric(18,6)
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2),
        (
          dp.custo_medio
          * COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))
        )::numeric(18,2)
      ) AS custo_total,
      COALESCE(i.valor_unitario_shadow, etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4)) AS preco_praticado_unitario,
      NULL::numeric(18,4) AS preco_lista_unitario,
      CASE
        WHEN COALESCE(i.qtd_shadow, etl.safe_numeric(i.payload->>'QTDE')) > 0 THEN (
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
      COALESCE(
        v.commercial_eligible,
        c.commercial_eligible,
        etl.comprovante_is_business_eligible(
          COALESCE(sc.cancelado_shadow, etl.to_bool(sc.payload->>'CANCELADO'), false),
          COALESCE(
            sc.situacao_shadow,
            etl.safe_int(sc.payload->>'SITUACAO'),
            etl.safe_int(sc.payload->>'situacao'),
            etl.safe_int(sc.payload->>'STATUS'),
            etl.safe_int(sc.payload->>'status')
          )
        ),
        false
      ) AS commercial_eligible,
      i.payload
    FROM stg.itenscomprovantes i
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa = i.id_empresa
     AND v.id_filial = i.id_filial
     AND v.id_db = i.id_db
     AND v.id_comprovante = i.id_comprovante
    LEFT JOIN dw.fact_comprovante c
      ON c.id_empresa = i.id_empresa
     AND c.id_filial = i.id_filial
     AND c.id_db = i.id_db
     AND c.id_comprovante = i.id_comprovante
    LEFT JOIN stg.comprovantes sc
      ON sc.id_empresa = i.id_empresa
     AND sc.id_filial = i.id_filial
     AND sc.id_db = i.id_db
     AND sc.id_comprovante = i.id_comprovante
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa = i.id_empresa
     AND dp.id_filial = i.id_filial
     AND dp.id_produto = COALESCE(i.id_produto_shadow, etl.safe_int(i.payload->>'ID_PRODUTOS'))
    WHERE i.id_empresa = p_id_empresa
      AND etl.runtime_branch_matches(i.id_filial)
      AND etl.runtime_business_date_in_range(
        COALESCE(etl.business_date(i.dt_evento), v_cutoff),
        v_cutoff,
        NULL::date
      )
      AND (
        etl.runtime_force_full_scan()
        OR i.received_at > v_wm
        OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
      AND (p_id_comprovante_from IS NULL OR i.id_comprovante >= p_id_comprovante_from)
      AND (p_id_comprovante_to IS NULL OR i.id_comprovante <= p_id_comprovante_to)
  ), deleted_ineligible AS (
    DELETE FROM dw.fact_venda_item f
    USING src s
    WHERE f.id_empresa = s.id_empresa
      AND f.id_filial = s.id_filial
      AND f.id_db = s.id_db
      AND f.id_comprovante = s.id_comprovante
      AND f.id_itemcomprovante = s.id_itemcomprovante
      AND s.commercial_eligible = false
    RETURNING 1
  ), eligible_src AS MATERIALIZED (
    SELECT *
    FROM src
    WHERE commercial_eligible = true
  ), prepared AS MATERIALIZED (
    SELECT
      s.*,
      f.id_movprodutos AS current_id_movprodutos,
      f.id_itensmovprodutos AS current_id_itensmovprodutos,
      f.data_key AS current_data_key,
      f.id_produto AS current_id_produto,
      f.id_grupo_produto AS current_id_grupo_produto,
      f.id_local_venda AS current_id_local_venda,
      f.id_funcionario AS current_id_funcionario,
      f.cfop AS current_cfop,
      f.qtd AS current_qtd,
      f.valor_unitario AS current_valor_unitario,
      f.total AS current_total,
      f.desconto AS current_desconto,
      f.custo_total AS current_custo_total,
      f.preco_lista_unitario AS current_preco_lista_unitario,
      f.preco_praticado_unitario AS current_preco_praticado_unitario,
      f.desconto_unitario AS current_desconto_unitario,
      f.desconto_total AS current_desconto_total,
      f.discount_source AS current_discount_source,
      f.payload AS current_payload
    FROM eligible_src s
    LEFT JOIN dw.fact_venda_item f
      ON f.id_empresa = s.id_empresa
     AND f.id_filial = s.id_filial
     AND f.id_db = s.id_db
     AND f.id_comprovante = s.id_comprovante
     AND f.id_itemcomprovante = s.id_itemcomprovante
  ), to_upsert AS MATERIALIZED (
    SELECT *
    FROM prepared
    WHERE ROW(
      current_id_movprodutos,
      current_id_itensmovprodutos,
      current_data_key,
      current_id_produto,
      current_id_grupo_produto,
      current_id_local_venda,
      current_id_funcionario,
      current_cfop,
      current_qtd,
      current_valor_unitario,
      current_total,
      current_desconto,
      current_custo_total,
      current_preco_lista_unitario,
      current_preco_praticado_unitario,
      current_desconto_unitario,
      current_desconto_total,
      current_discount_source,
      current_payload
    ) IS DISTINCT FROM ROW(
      id_movprodutos,
      id_itensmovprodutos,
      data_key,
      id_produto,
      id_grupo_produto,
      id_local_venda,
      id_funcionario,
      cfop,
      qtd,
      valor_unitario,
      total,
      desconto,
      custo_total,
      preco_lista_unitario,
      preco_praticado_unitario,
      desconto_unitario,
      desconto_total,
      discount_source,
      payload
    )
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      id_itensmovprodutos,
      id_comprovante,
      id_itemcomprovante,
      data_key,
      id_produto,
      id_grupo_produto,
      id_local_venda,
      id_funcionario,
      cfop,
      qtd,
      valor_unitario,
      total,
      desconto,
      custo_total,
      margem,
      preco_lista_unitario,
      preco_praticado_unitario,
      desconto_unitario,
      desconto_total,
      discount_source,
      payload
    )
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_movprodutos,
      id_itensmovprodutos,
      id_comprovante,
      id_itemcomprovante,
      data_key,
      id_produto,
      id_grupo_produto,
      id_local_venda,
      id_funcionario,
      cfop,
      qtd,
      valor_unitario,
      total,
      desconto,
      custo_total,
      (COALESCE(total, 0) - COALESCE(custo_total, 0))::numeric(18,2),
      preco_lista_unitario,
      preco_praticado_unitario,
      desconto_unitario,
      desconto_total,
      discount_source,
      payload
    FROM to_upsert
    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
    DO UPDATE SET
      id_movprodutos = EXCLUDED.id_movprodutos,
      id_itensmovprodutos = EXCLUDED.id_itensmovprodutos,
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
    WHERE dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.id_movprodutos IS DISTINCT FROM EXCLUDED.id_movprodutos
      OR dw.fact_venda_item.id_itensmovprodutos IS DISTINCT FROM EXCLUDED.id_itensmovprodutos
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
      OR dw.fact_venda_item.desconto_total IS DISTINCT FROM EXCLUDED.desconto_total
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM src), 0),
    COALESCE((SELECT COUNT(*) FROM prepared WHERE current_payload IS NOT NULL), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE inserted), 0),
    COALESCE((SELECT COUNT(*) FROM upserted WHERE NOT inserted), 0),
    COALESCE((SELECT COUNT(*) FROM deleted_ineligible), 0)
  INTO v_candidate_count, v_conflict_count, v_upsert_inserts, v_upsert_updates, v_deleted_ineligible;

  IF p_update_watermark AND etl.runtime_watermark_updates_enabled() THEN
    SELECT MAX(received_at) INTO v_max
    FROM stg.itenscomprovantes
    WHERE id_empresa = p_id_empresa
      AND received_at > v_wm;

    PERFORM etl.set_watermark(
      p_id_empresa,
      'itenscomprovantes_sales_fact',
      COALESCE(v_max, v_wm),
      NULL::bigint
    );
  END IF;

  v_total_ms := FLOOR(EXTRACT(epoch FROM (clock_timestamp() - v_started)) * 1000)::int;

  RETURN jsonb_build_object(
    'rows', COALESCE(v_upsert_inserts, 0) + COALESCE(v_upsert_updates, 0),
    'candidate_count', COALESCE(v_candidate_count, 0),
    'conflict_count', COALESCE(v_conflict_count, 0),
    'upsert_inserts', COALESCE(v_upsert_inserts, 0),
    'upsert_updates', COALESCE(v_upsert_updates, 0),
    'deleted_ineligible', COALESCE(v_deleted_ineligible, 0),
    'range_from', p_id_comprovante_from,
    'range_to', p_id_comprovante_to,
    'watermark_updated', p_update_watermark AND etl.runtime_watermark_updates_enabled(),
    'total_ms', COALESCE(v_total_ms, 0)
  );
END;
$$;

COMMIT;