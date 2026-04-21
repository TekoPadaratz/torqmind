BEGIN;

CREATE INDEX IF NOT EXISTS ix_stg_comprovantes_emp_received ON stg.comprovantes (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_movprodutos_emp_received ON stg.movprodutos (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_itensmovprodutos_emp_received ON stg.itensmovprodutos (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_financeiro_emp_received ON stg.financeiro (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_contaspagar_emp_received ON stg.contaspagar (id_empresa, received_at);
CREATE INDEX IF NOT EXISTS ix_stg_contasreceber_emp_received ON stg.contasreceber (id_empresa, received_at);

CREATE OR REPLACE FUNCTION etl.load_fact_comprovante(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'comprovantes'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      id_empresa,
      id_filial,
      id_db,
      id_comprovante,
      etl.safe_timestamp(payload->>'DATA') AS data,
      etl.date_key(etl.safe_timestamp(payload->>'DATA')) AS data_key,
      etl.safe_int(payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_numeric(payload->>'VLRTOTAL')::numeric(18,2) AS valor_total,
      etl.to_bool(payload->>'CANCELADO') AS cancelado,
      etl.safe_int(payload->>'SITUACAO') AS situacao,
      payload
    FROM stg.comprovantes
    WHERE id_empresa = p_id_empresa
      AND (
        received_at > v_wm
        OR (dt_evento IS NOT NULL AND dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), upserted AS (
    INSERT INTO dw.fact_comprovante (
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_comprovante,data,data_key,id_usuario,id_turno,id_cliente,valor_total,cancelado,situacao,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_turno=EXCLUDED.id_turno,
      id_cliente=EXCLUDED.id_cliente,
      valor_total=EXCLUDED.valor_total,
      cancelado=EXCLUDED.cancelado,
      situacao=EXCLUDED.situacao,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_comprovante.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_comprovante.cancelado IS DISTINCT FROM EXCLUDED.cancelado
      OR dw.fact_comprovante.valor_total IS DISTINCT FROM EXCLUDED.valor_total
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

CREATE OR REPLACE FUNCTION etl.load_fact_venda(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'movprodutos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      m.id_empresa,
      m.id_filial,
      m.id_db,
      m.id_movprodutos,
      etl.safe_timestamp(m.payload->>'DATA') AS data,
      etl.date_key(etl.safe_timestamp(m.payload->>'DATA')) AS data_key,
      etl.safe_int(m.payload->>'ID_USUARIOS') AS id_usuario,
      etl.safe_int(m.payload->>'ID_ENTIDADE') AS id_cliente,
      etl.safe_int(m.payload->>'ID_COMPROVANTE') AS id_comprovante,
      etl.safe_int(m.payload->>'ID_TURNOS') AS id_turno,
      etl.safe_int(m.payload->>'SAIDAS_ENTRADAS') AS saidas_entradas,
      etl.safe_numeric(m.payload->>'TOTALVENDA')::numeric(18,2) AS total_venda,
      m.payload
    FROM stg.movprodutos m
    WHERE m.id_empresa = p_id_empresa
      AND (
        m.received_at > v_wm
        OR (m.dt_evento IS NOT NULL AND m.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), upserted AS (
    INSERT INTO dw.fact_venda (
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,data,data_key,id_usuario,id_cliente,id_comprovante,id_turno,saidas_entradas,total_venda,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
    DO UPDATE SET
      data=EXCLUDED.data,
      data_key=EXCLUDED.data_key,
      id_usuario=EXCLUDED.id_usuario,
      id_cliente=EXCLUDED.id_cliente,
      id_comprovante=EXCLUDED.id_comprovante,
      id_turno=EXCLUDED.id_turno,
      saidas_entradas=EXCLUDED.saidas_entradas,
      total_venda=EXCLUDED.total_venda,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_venda.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda.total_venda IS DISTINCT FROM EXCLUDED.total_venda
    RETURNING id_empresa,id_filial,id_db,id_movprodutos,id_comprovante
  ), updated_cancel AS (
    UPDATE dw.fact_venda v
    SET cancelado = c.cancelado
    FROM dw.fact_comprovante c, upserted u
    WHERE v.id_empresa = p_id_empresa
      AND u.id_empresa=v.id_empresa
      AND u.id_filial=v.id_filial
      AND u.id_db=v.id_db
      AND u.id_movprodutos=v.id_movprodutos
      AND v.id_empresa = c.id_empresa
      AND v.id_filial = c.id_filial
      AND v.id_db = c.id_db
      AND v.id_comprovante IS NOT NULL
      AND v.id_comprovante = c.id_comprovante
      AND v.cancelado IS DISTINCT FROM c.cancelado
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM upserted),0) + COALESCE((SELECT COUNT(*) FROM updated_cancel),0)
  INTO v_rows;

  SELECT MAX(received_at) INTO v_max
  FROM stg.movprodutos
  WHERE id_empresa = p_id_empresa
    AND received_at > v_wm;

  PERFORM etl.set_watermark(p_id_empresa, 'movprodutos', COALESCE(v_max, v_wm), NULL::bigint);
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max timestamptz;
  v_rows integer;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'itensmovprodutos'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      i.id_empresa,
      i.id_filial,
      i.id_db,
      i.id_movprodutos,
      i.id_itensmovprodutos,
      v.data_key,
      etl.safe_int(i.payload->>'ID_PRODUTOS') AS id_produto,
      etl.safe_int(i.payload->>'ID_GRUPOPRODUTOS') AS id_grupo_produto,
      etl.safe_int(i.payload->>'ID_LOCALVENDAS') AS id_local_venda,
      etl.safe_int(i.payload->>'ID_FUNCIONARIOS') AS id_funcionario,
      etl.safe_int(i.payload->>'CFOP') AS cfop,
      etl.safe_numeric(i.payload->>'QTDE')::numeric(18,3) AS qtd,
      etl.safe_numeric(i.payload->>'VLRUNITARIO')::numeric(18,4) AS valor_unitario,
      etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2) AS total,
      etl.safe_numeric(i.payload->>'VLRDESCONTO')::numeric(18,2) AS desconto,
      COALESCE(
        (etl.safe_numeric(i.payload->>'VLRCUSTO')::numeric(18,6) * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2),
        (dp.custo_medio * etl.safe_numeric(i.payload->>'QTDE')::numeric(18,6))::numeric(18,2)
      ) AS custo_total,
      i.payload
    FROM stg.itensmovprodutos i
    LEFT JOIN dw.fact_venda v
      ON v.id_empresa=i.id_empresa AND v.id_filial=i.id_filial AND v.id_db=i.id_db AND v.id_movprodutos=i.id_movprodutos
    LEFT JOIN dw.dim_produto dp
      ON dp.id_empresa=i.id_empresa AND dp.id_filial=i.id_filial AND dp.id_produto=etl.safe_int(i.payload->>'ID_PRODUTOS')
    WHERE i.id_empresa = p_id_empresa
      AND (
        i.received_at > v_wm
        OR (i.dt_evento IS NOT NULL AND i.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), upserted AS (
    INSERT INTO dw.fact_venda_item (
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,margem,payload
    )
    SELECT
      id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,data_key,
      id_produto,id_grupo_produto,id_local_venda,id_funcionario,cfop,
      qtd,valor_unitario,total,desconto,custo_total,
      (COALESCE(total,0) - COALESCE(custo_total,0))::numeric(18,2) AS margem,
      payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
    DO UPDATE SET
      data_key=EXCLUDED.data_key,
      id_produto=EXCLUDED.id_produto,
      id_grupo_produto=EXCLUDED.id_grupo_produto,
      id_local_venda=EXCLUDED.id_local_venda,
      id_funcionario=EXCLUDED.id_funcionario,
      cfop=EXCLUDED.cfop,
      qtd=EXCLUDED.qtd,
      valor_unitario=EXCLUDED.valor_unitario,
      total=EXCLUDED.total,
      desconto=EXCLUDED.desconto,
      custo_total=EXCLUDED.custo_total,
      margem=EXCLUDED.margem,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_venda_item.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_venda_item.custo_total IS DISTINCT FROM EXCLUDED.custo_total
      OR dw.fact_venda_item.total IS DISTINCT FROM EXCLUDED.total
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

CREATE OR REPLACE FUNCTION etl.load_fact_financeiro(p_id_empresa int)
RETURNS integer AS $$
DECLARE
  v_wm timestamptz;
  v_max_a timestamptz;
  v_max_b timestamptz;
  v_max_c timestamptz;
  v_max_final timestamptz;
  v_rows integer := 0;
BEGIN
  v_wm := COALESCE(etl.get_watermark(p_id_empresa, 'financeiro'), '1970-01-01'::timestamptz);

  WITH src AS (
    SELECT
      f.id_empresa,
      f.id_filial,
      f.id_db,
      f.tipo_titulo,
      f.id_titulo,
      etl.safe_int(f.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(f.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(f.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(f.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(f.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(f.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(f.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      f.payload
    FROM stg.financeiro f
    WHERE f.id_empresa = p_id_empresa
      AND (
        f.received_at > v_wm
        OR (f.dt_evento IS NOT NULL AND f.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )

    UNION ALL

    SELECT
      p.id_empresa,
      p.id_filial,
      p.id_db,
      0 AS tipo_titulo,
      p.id_contaspagar AS id_titulo,
      etl.safe_int(p.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(p.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(p.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(p.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(p.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(p.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(p.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      p.payload
    FROM stg.contaspagar p
    WHERE p.id_empresa = p_id_empresa
      AND (
        p.received_at > v_wm
        OR (p.dt_evento IS NOT NULL AND p.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )

    UNION ALL

    SELECT
      r.id_empresa,
      r.id_filial,
      r.id_db,
      1 AS tipo_titulo,
      r.id_contasreceber AS id_titulo,
      etl.safe_int(r.payload->>'ID_ENTIDADE') AS id_entidade,
      (etl.safe_timestamp(r.payload->>'DTACONTA'))::date AS data_emissao,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTACONTA')) AS data_key_emissao,
      (etl.safe_timestamp(r.payload->>'DTAVCTO'))::date AS vencimento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAVCTO')) AS data_key_venc,
      (etl.safe_timestamp(r.payload->>'DTAPGTO'))::date AS data_pagamento,
      etl.date_key(etl.safe_timestamp(r.payload->>'DTAPGTO')) AS data_key_pgto,
      etl.safe_numeric(r.payload->>'VALOR')::numeric(18,2) AS valor,
      etl.safe_numeric(r.payload->>'VLRPAGO')::numeric(18,2) AS valor_pago,
      r.payload
    FROM stg.contasreceber r
    WHERE r.id_empresa = p_id_empresa
      AND (
        r.received_at > v_wm
        OR (r.dt_evento IS NOT NULL AND r.dt_evento >= now() - make_interval(days => etl.hot_window_days()))
      )
  ), upserted AS (
    INSERT INTO dw.fact_financeiro (
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    )
    SELECT
      id_empresa,id_filial,id_db,tipo_titulo,id_titulo,id_entidade,
      data_emissao,data_key_emissao,vencimento,data_key_venc,data_pagamento,data_key_pgto,
      valor,valor_pago,payload
    FROM src
    ON CONFLICT (id_empresa,id_filial,id_db,tipo_titulo,id_titulo)
    DO UPDATE SET
      id_entidade=EXCLUDED.id_entidade,
      data_emissao=EXCLUDED.data_emissao,
      data_key_emissao=EXCLUDED.data_key_emissao,
      vencimento=EXCLUDED.vencimento,
      data_key_venc=EXCLUDED.data_key_venc,
      data_pagamento=EXCLUDED.data_pagamento,
      data_key_pgto=EXCLUDED.data_key_pgto,
      valor=EXCLUDED.valor,
      valor_pago=EXCLUDED.valor_pago,
      payload=EXCLUDED.payload
    WHERE
      dw.fact_financeiro.payload IS DISTINCT FROM EXCLUDED.payload
      OR dw.fact_financeiro.valor IS DISTINCT FROM EXCLUDED.valor
      OR dw.fact_financeiro.valor_pago IS DISTINCT FROM EXCLUDED.valor_pago
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM upserted;

  SELECT MAX(received_at) INTO v_max_a FROM stg.financeiro WHERE id_empresa = p_id_empresa AND received_at > v_wm;
  SELECT MAX(received_at) INTO v_max_b FROM stg.contaspagar WHERE id_empresa = p_id_empresa AND received_at > v_wm;
  SELECT MAX(received_at) INTO v_max_c FROM stg.contasreceber WHERE id_empresa = p_id_empresa AND received_at > v_wm;

  v_max_final := GREATEST(COALESCE(v_max_a, '1970-01-01'::timestamptz), COALESCE(v_max_b, '1970-01-01'::timestamptz), COALESCE(v_max_c, '1970-01-01'::timestamptz));
  PERFORM etl.set_watermark(p_id_empresa, 'financeiro', COALESCE(v_max_final, v_wm), NULL::bigint);

  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMIT;
