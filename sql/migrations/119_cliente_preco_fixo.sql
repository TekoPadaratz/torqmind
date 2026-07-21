-- 119_cliente_preco_fixo.sql
-- Preço fixo de cliente (combustível): cadastro Xpert DESCONTOSENTIDADESITENS.VALORFIXO=1.
-- Desconto econômico = (PPL bomba do dia − VLRUNITARIO pago) × QTDE.
-- Sem histórico de cadastro: o preço pago na venda é a verdade do dia.

CREATE TABLE IF NOT EXISTS stg.descontos_entidades_itens (
  id_empresa                    integer NOT NULL,
  id_filial                     integer NOT NULL,
  id_descontoentidadesitens     integer NOT NULL,
  payload                       jsonb NOT NULL,
  id_db_shadow                  integer,
  id_chave_natural              text,
  dt_evento                     timestamptz,
  ingested_at                   timestamptz NOT NULL DEFAULT now(),
  received_at                   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_descontoentidadesitens)
);

CREATE INDEX IF NOT EXISTS ix_stg_descontos_entidades_itens_ent
  ON stg.descontos_entidades_itens (id_empresa, id_filial, ((payload->>'ID_ENTIDADE')));

CREATE INDEX IF NOT EXISTS ix_stg_descontos_entidades_itens_fixo
  ON stg.descontos_entidades_itens (id_empresa, ((payload->>'VALORFIXO')), ((payload->>'ATIVO')));

COMMENT ON TABLE stg.descontos_entidades_itens IS
  'Xpert dbo.DESCONTOSENTIDADESITENS. VALORFIXO=1 + VALOR = preço fixo unitário do cliente no produto.';
