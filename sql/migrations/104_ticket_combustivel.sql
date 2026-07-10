-- Migration 104: Ticket medio de COMBUSTIVEL (Vendas)
-- Idempotente. Nao destrutivo.
--
-- O ticket medio de combustivel real vem do console da bomba (cada abastecimento
-- fisico), NAO do comprovante (que agrega/mistura). Fonte: dbo.CONSOLEARQUIVO
-- (1 linha = 1 abastecimento): VALOR, QTDE(litros), PPL(preco/litro), DATA.
-- Ticket medio combustivel = SUM(VALOR) / COUNT(abastecimentos) no periodo.
-- Mart diaria (grao empresa,filial,dia) mantem baixo volume na tela.

CREATE TABLE IF NOT EXISTS stg.consolearquivo (
  id_empresa       integer NOT NULL,
  id_filial        integer NOT NULL,
  id_db            integer NOT NULL DEFAULT 0,
  id_consolearquivo integer NOT NULL,        -- ID_CONSOLEARQUIVO
  payload          jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento        timestamptz,
  id_db_shadow     bigint,
  id_chave_natural text,
  ingested_at      timestamptz NOT NULL DEFAULT now(),
  received_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_consolearquivo)
);
CREATE INDEX IF NOT EXISTS ix_stg_console_evento
  ON stg.consolearquivo (id_empresa, id_filial, dt_evento);

COMMENT ON TABLE stg.consolearquivo IS 'Abastecimentos do console da bomba (Xpert dbo.CONSOLEARQUIVO). 1 linha=1 abastecimento. payload: VALOR, QTDE(litros), PPL, DATA, ID_BICOS. Fonte do ticket medio de combustivel.';

CREATE TABLE IF NOT EXISTS mart.ticket_combustivel_diaria (
  id_empresa         integer NOT NULL,
  id_filial          integer NOT NULL,
  data_ref           date    NOT NULL,
  valor_total        numeric(18,2) NOT NULL DEFAULT 0,
  litros_total       numeric(18,3) NOT NULL DEFAULT 0,
  qtd_abastecimentos integer NOT NULL DEFAULT 0,
  ticket_medio       numeric(18,2) GENERATED ALWAYS AS (
                        CASE WHEN qtd_abastecimentos > 0 THEN valor_total / qtd_abastecimentos ELSE 0 END
                      ) STORED,
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, data_ref)
);
CREATE INDEX IF NOT EXISTS ix_ticket_comb_scope
  ON mart.ticket_combustivel_diaria (id_empresa, id_filial, data_ref);

COMMENT ON TABLE mart.ticket_combustivel_diaria IS 'Ticket medio de combustivel por dia/filial (Xpert CONSOLEARQUIVO). ticket_medio = valor_total / qtd_abastecimentos. Camada rapida da tela de Vendas.';

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
    AND COALESCE((c.payload->>'VALOR')::numeric, 0) > 0
    AND NULLIF(LEFT(c.payload->>'DATA', 10), '')::date IS NOT NULL
  GROUP BY c.id_empresa, c.id_filial, NULLIF(LEFT(c.payload->>'DATA', 10), '')::date;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION etl.refresh_ticket_combustivel(integer) IS
  'Reconstroi mart.ticket_combustivel_diaria a partir de stg.consolearquivo (SUM VALOR, COUNT abastecimentos por dia/filial). Advisory lock por empresa.';
