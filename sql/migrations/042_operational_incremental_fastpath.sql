-- @nontransactional

-- Tighten the operational fast-path so no-op incremental cycles stop reprocessing
-- full dimensions or rescanning all sales/comprovantes just to sync cancel flags.

CREATE OR REPLACE FUNCTION etl.load_dim_grupos(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'grupoprodutos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      g.id_empresa,
      g.id_filial,
      g.id_grupoprodutos AS id_grupo_produto,
      COALESCE(g.payload->>'NOMEGRUPOPRODUTOS', '') AS nome
    FROM stg.grupoprodutos g
    WHERE g.id_empresa = p_id_empresa
      AND g.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.dim_grupo_produto (id_empresa, id_filial, id_grupo_produto, nome)
    SELECT id_empresa, id_filial, id_grupo_produto, nome
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_grupo_produto)
    DO UPDATE SET
      nome = EXCLUDED.nome
    WHERE dw.dim_grupo_produto.nome IS DISTINCT FROM EXCLUDED.nome
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.grupoprodutos
  WHERE id_empresa = p_id_empresa
    AND ingested_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'grupoprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.load_dim_produtos(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'produtos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_produto,
      COALESCE(p.payload->>'NOMEPRODUTO', p.payload->>'NOME', '') AS nome,
      NULLIF(p.payload->>'UNIDADE', '') AS unidade,
      etl.safe_int(p.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(p.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_numeric(p.payload->>'customedio') AS custo_medio
    FROM stg.produtos p
    WHERE p.id_empresa = p_id_empresa
      AND p.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.dim_produto (
      id_empresa,
      id_filial,
      id_produto,
      nome,
      unidade,
      id_grupo_produto,
      id_local_venda,
      custo_medio
    )
    SELECT
      id_empresa,
      id_filial,
      id_produto,
      nome,
      unidade,
      id_grupo_produto,
      id_local_venda,
      custo_medio
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_produto)
    DO UPDATE SET
      nome = EXCLUDED.nome,
      unidade = EXCLUDED.unidade,
      id_grupo_produto = EXCLUDED.id_grupo_produto,
      id_local_venda = EXCLUDED.id_local_venda,
      custo_medio = EXCLUDED.custo_medio
    WHERE
      dw.dim_produto.nome IS DISTINCT FROM EXCLUDED.nome
      OR dw.dim_produto.unidade IS DISTINCT FROM EXCLUDED.unidade
      OR dw.dim_produto.id_grupo_produto IS DISTINCT FROM EXCLUDED.id_grupo_produto
      OR dw.dim_produto.id_local_venda IS DISTINCT FROM EXCLUDED.id_local_venda
      OR dw.dim_produto.custo_medio IS DISTINCT FROM EXCLUDED.custo_medio
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.produtos
  WHERE id_empresa = p_id_empresa
    AND ingested_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'produtos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.load_dim_funcionarios(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'funcionarios'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_funcionario,
      COALESCE(f.payload->>'NOMEFUNCIONARIO', f.payload->>'NOME', '') AS nome
    FROM stg.funcionarios f
    WHERE f.id_empresa = p_id_empresa
      AND f.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
    SELECT id_empresa, id_filial, id_funcionario, nome
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_funcionario)
    DO UPDATE SET
      nome = EXCLUDED.nome
    WHERE dw.dim_funcionario.nome IS DISTINCT FROM EXCLUDED.nome
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.funcionarios
  WHERE id_empresa = p_id_empresa
    AND ingested_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'funcionarios', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.load_dim_clientes(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'entidades'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      e.id_empresa,
      e.id_filial,
      e.id_entidade AS id_cliente,
      COALESCE(e.payload->>'NOMEENTIDADE', e.payload->>'NOME', '') AS nome,
      COALESCE(e.payload->>'CNPJCPF', e.payload->>'DOCUMENTO', NULL) AS documento
    FROM stg.entidades e
    WHERE e.id_empresa = p_id_empresa
      AND e.ingested_at > v_wm
  ), upserted AS (
    INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome, documento)
    SELECT id_empresa, id_filial, id_cliente, nome, documento
    FROM src
    ON CONFLICT (id_empresa, id_filial, id_cliente)
    DO UPDATE SET
      nome = EXCLUDED.nome,
      documento = EXCLUDED.documento
    WHERE
      dw.dim_cliente.nome IS DISTINCT FROM EXCLUDED.nome
      OR dw.dim_cliente.documento IS DISTINCT FROM EXCLUDED.documento
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(ingested_at) INTO v_max
  FROM stg.entidades
  WHERE id_empresa = p_id_empresa
    AND ingested_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'entidades', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;


CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fact_venda_cancel_sync_lookup
  ON dw.fact_venda (id_empresa, id_filial, id_db, id_comprovante)
  INCLUDE (cancelado)
  WHERE id_comprovante IS NOT NULL;


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

  WITH src AS (
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
      COALESCE(c.cancelado_shadow, etl.to_bool(c.payload->>'CANCELADO'), false) AS cancelado,
      COALESCE(c.situacao_shadow, etl.safe_int(c.payload->>'SITUACAO')) AS situacao,
      c.payload
    FROM stg.comprovantes c
    JOIN tmp_etl_candidate_comprovantes tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
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
      payload = EXCLUDED.payload
    WHERE
      dw.fact_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_comprovante.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_comprovante.valor_total IS DISTINCT FROM EXCLUDED.valor_total
    RETURNING 1
  ), synced_venda_cancel AS (
    UPDATE dw.fact_venda v
    SET cancelado = c.cancelado
    FROM dw.fact_comprovante c
    JOIN tmp_etl_candidate_comprovantes tc
      ON tc.id_empresa = c.id_empresa
     AND tc.id_filial = c.id_filial
     AND tc.id_db = c.id_db
     AND tc.id_comprovante = c.id_comprovante
    WHERE v.id_empresa = c.id_empresa
      AND v.id_filial = c.id_filial
      AND v.id_db = c.id_db
      AND v.id_comprovante = c.id_comprovante
      AND v.cancelado IS DISTINCT FROM c.cancelado
    RETURNING 1
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM upserted), 0)
    + COALESCE((SELECT COUNT(*) FROM synced_venda_cancel), 0)
  INTO v_rows;

  SELECT MAX(received_at) INTO v_max
  FROM stg.comprovantes
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'comprovantes', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
  v_cutoff date;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz);
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
      m.received_at > v_wm
      OR (m.dt_evento IS NOT NULL AND m.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
    )
  ON CONFLICT DO NOTHING;

  WITH src AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      etl.sales_business_ts(m.payload, m.dt_evento) AS data,
      etl.business_date_key(etl.sales_event_timestamptz(m.payload, m.dt_evento)) AS data_key,
      COALESCE(m.id_usuario_shadow, etl.safe_int(m.payload->>'ID_USUARIOS')) AS id_usuario,
      COALESCE(m.id_cliente_shadow, etl.safe_int(m.payload->>'ID_ENTIDADE')) AS id_cliente,
      COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) AS id_comprovante,
      COALESCE(m.id_turno_shadow, etl.safe_int(m.payload->>'ID_TURNOS')) AS id_turno,
      COALESCE(m.saidas_entradas_shadow, etl.safe_int(m.payload->>'SAIDAS_ENTRADAS')) AS saidas_entradas,
      COALESCE(m.total_venda_shadow, etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2)) AS total_venda,
      COALESCE(c.cancelado, false) AS cancelado,
      m.payload
    FROM stg.movprodutos m
    JOIN tmp_etl_candidate_movimentos tm
      ON tm.id_empresa = m.id_empresa
     AND tm.id_filial = m.id_filial
     AND tm.id_db = m.id_db
     AND tm.id_movprodutos = m.id_movprodutos
    LEFT JOIN dw.fact_comprovante c
      ON c.id_empresa = m.id_empresa
     AND c.id_filial = m.id_filial
     AND c.id_db = m.id_db
     AND c.id_comprovante = COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'))
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,cancelado,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,cancelado,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
    DO UPDATE SET
      data = EXCLUDED.data,
      data_key = EXCLUDED.data_key,
      id_usuario = EXCLUDED.id_usuario,
      id_cliente = EXCLUDED.id_cliente,
      id_comprovante = EXCLUDED.id_comprovante,
      id_turno = EXCLUDED.id_turno,
      saidas_entradas = EXCLUDED.saidas_entradas,
      total_venda = EXCLUDED.total_venda,
      cancelado = EXCLUDED.cancelado,
      payload = EXCLUDED.payload
    WHERE
      dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
      OR dw.fact_venda.id_comprovante IS DISTINCT FROM EXCLUDED.id_comprovante
      OR dw.fact_venda.cancelado IS DISTINCT FROM EXCLUDED.cancelado
    RETURNING 1
  )
  SELECT COUNT(*)::int INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max
  FROM stg.movprodutos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'movprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;
