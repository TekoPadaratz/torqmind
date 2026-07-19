-- 111: Compliance ANP / CDC — config + STG para NFe entrada e hist. preço bomba
-- Homolog first. Sem DROP/TRUNCATE. nome_resumido = auth.filiais.apelido (095).

BEGIN;

-- ---------------------------------------------------------------------------
-- Config (Gestão de Lucros): limites por empresa / filial
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.anp_compliance_config (
  id_empresa                 int            NOT NULL,
  id_filial                  int            NOT NULL DEFAULT 0, -- 0 = default empresa
  limite_alerta_amarelo_perc numeric(8,2)   NOT NULL DEFAULT 50.00,
  limite_abusivo_anp_perc    numeric(8,2)   NOT NULL DEFAULT 70.00,
  ativo                      boolean        NOT NULL DEFAULT true,
  updated_at                 timestamptz    NOT NULL DEFAULT now(),
  updated_by                 text,
  CONSTRAINT pk_anp_compliance_config PRIMARY KEY (id_empresa, id_filial),
  CONSTRAINT ck_anp_limite_alerta
    CHECK (limite_alerta_amarelo_perc >= 0 AND limite_alerta_amarelo_perc <= 500),
  CONSTRAINT ck_anp_limite_abusivo
    CHECK (limite_abusivo_anp_perc >= limite_alerta_amarelo_perc
       AND limite_abusivo_anp_perc <= 1000)
);

COMMENT ON TABLE app.anp_compliance_config IS
  'Limites ANP/CDC de variação de margem bruta (Gestão de Lucros). id_filial=0 = default da empresa.';

INSERT INTO app.anp_compliance_config (id_empresa, id_filial)
SELECT DISTINCT e.id_empresa, 0
FROM auth.filiais e
WHERE NOT EXISTS (
  SELECT 1 FROM app.anp_compliance_config c
  WHERE c.id_empresa = e.id_empresa AND c.id_filial = 0
)
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- STG: NFe de ENTRADA (compra) — distinta de stg.nfe (NFC-e venda)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.nfe_entrada (
  id_empresa         int         NOT NULL,
  id_filial          int         NOT NULL,
  id_db              int         NOT NULL,
  id_nota            int         NOT NULL,
  chave_acesso_shadow varchar(44),
  numero_nota_shadow varchar(20),
  serie_shadow       varchar(8),
  cnpj_emitente_shadow varchar(14),
  dt_entrada_shadow  timestamptz,
  dt_emissao_shadow  timestamptz,
  valor_total_shadow numeric(18,2),
  situacao_shadow    smallint,
  nome_emitente_shadow text,
  payload            jsonb       NOT NULL,
  dt_evento          timestamptz,
  id_db_shadow       int,
  id_chave_natural   text,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  received_at        timestamptz DEFAULT now(),
  CONSTRAINT pk_stg_nfe_entrada
    PRIMARY KEY (id_empresa, id_filial, id_db, id_nota)
);

CREATE INDEX IF NOT EXISTS ix_stg_nfe_entrada_emp_fil_dt
  ON stg.nfe_entrada (id_empresa, id_filial, dt_entrada_shadow DESC NULLS LAST);

COMMENT ON TABLE stg.nfe_entrada IS
  'NFe de entrada/compra (distribuidora). NÃO confundir com stg.nfe (NFC-e de venda).';

CREATE TABLE IF NOT EXISTS stg.itens_nfe_entrada (
  id_empresa         int         NOT NULL,
  id_filial          int         NOT NULL,
  id_db              int         NOT NULL,
  id_nota            int         NOT NULL,
  id_item            int         NOT NULL,
  id_produto_shadow  int,
  qtd_shadow         numeric(18,6),
  custo_unitario_shadow numeric(18,6),
  custo_total_shadow numeric(18,2),
  eh_combustivel_shadow boolean,
  payload            jsonb       NOT NULL,
  dt_evento          timestamptz,
  id_db_shadow       int,
  id_chave_natural   text,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  received_at        timestamptz DEFAULT now(),
  CONSTRAINT pk_stg_itens_nfe_entrada
    PRIMARY KEY (id_empresa, id_filial, id_db, id_nota, id_item)
);

CREATE INDEX IF NOT EXISTS ix_stg_itens_nfe_ent_prod
  ON stg.itens_nfe_entrada (id_empresa, id_filial, id_produto_shadow)
  WHERE id_produto_shadow IS NOT NULL;

COMMENT ON TABLE stg.itens_nfe_entrada IS
  'Itens de NFe de entrada; custo_unitario inclui impostos/frete rateados (regra Agent).';

CREATE TABLE IF NOT EXISTS stg.preco_bomba_hist (
  id_empresa         int         NOT NULL,
  id_filial          int         NOT NULL,
  id_db              int         NOT NULL,
  id_produto         int         NOT NULL,
  id_evento          bigint      NOT NULL,
  dt_alteracao_shadow timestamptz,
  preco_venda_shadow numeric(18,4),
  preco_anterior_shadow numeric(18,4),
  id_bico_shadow     int,
  payload            jsonb       NOT NULL,
  dt_evento          timestamptz,
  id_db_shadow       int,
  id_chave_natural   text,
  ingested_at        timestamptz NOT NULL DEFAULT now(),
  received_at        timestamptz DEFAULT now(),
  CONSTRAINT pk_stg_preco_bomba_hist
    PRIMARY KEY (id_empresa, id_filial, id_db, id_produto, id_evento)
);

CREATE INDEX IF NOT EXISTS ix_stg_preco_bomba_emp_fil_prod_dt
  ON stg.preco_bomba_hist (id_empresa, id_filial, id_produto, dt_alteracao_shadow DESC NULLS LAST);

COMMENT ON TABLE stg.preco_bomba_hist IS
  'Histórico de alteração de preço na bomba (combustível).';

COMMIT;
