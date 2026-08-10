-- Migration 133: ticket combustível — não descartar abastecimento com VALOR=0
-- Idempotente. Não destrutivo.
--
-- Antes: etl.refresh_ticket_combustivel filtrava VALOR > 0 e omitia linhas
-- reais de CONSOLEARQUIVO com litros/PPL e VALOR zerado (ex.: ABASTECIMENTO_GERADO=3).
-- Continua excluindo VALOR < 0 (lixo/ajuste anômalo, ex. STATUS=9 com centenas de milhares negativos).

CREATE OR REPLACE FUNCTION etl.refresh_ticket_combustivel(p_id_empresa integer)
RETURNS integer AS $$
DECLARE
  v_rows integer := 0;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('refresh_ticket_combustivel'), p_id_empresa);

  DELETE FROM mart.ticket_combustivel_diaria WHERE id_empresa = p_id_empresa;

  INSERT INTO mart.ticket_combustivel_diaria (
    id_empresa, id_filial, data_ref, valor_total, litros_total, qtd_abastecimentos, updated_at
  )
  SELECT
    c.id_empresa,
    c.id_filial,
    NULLIF(LEFT(c.payload->>'DATA', 10), '')::date AS data_ref,
    COALESCE(SUM((c.payload->>'VALOR')::numeric), 0)::numeric(18,2) AS valor_total,
    COALESCE(SUM((c.payload->>'QTDE')::numeric), 0)::numeric(18,3) AS litros_total,
    COUNT(*)::int AS qtd_abastecimentos,
    now()
  FROM stg.consolearquivo c
  WHERE c.id_empresa = p_id_empresa
    AND COALESCE((c.payload->>'VALOR')::numeric, 0) >= 0
    AND NULLIF(LEFT(c.payload->>'DATA', 10), '')::date IS NOT NULL
  GROUP BY c.id_empresa, c.id_filial, NULLIF(LEFT(c.payload->>'DATA', 10), '')::date;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION etl.refresh_ticket_combustivel(integer) IS
  'Reconstroi mart.ticket_combustivel_diaria a partir de stg.consolearquivo (VALOR>=0; exclui só negativo). ticket = SUM(VALOR)/COUNT.';
