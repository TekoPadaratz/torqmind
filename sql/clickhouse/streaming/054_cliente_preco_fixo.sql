-- 054_cliente_preco_fixo.sql
-- Marts RT: desconto econômico de clientes com preço fixo em combustível.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_preco_bomba_dia (
    id_empresa       Int32,
    id_filial        Int32,
    id_produto       Int32,
    dt               Date,
    preco_venda      Decimal(18, 4) DEFAULT 0,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_produto, dt)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_cliente_preco_fixo_cadastro (
    id_empresa       Int32,
    id_filial        Int32,
    id_entidade      Int32,
    id_produto       Int32,
    valor_fixo      Decimal(18, 4) DEFAULT 0,
    ativo            UInt8 DEFAULT 1,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_entidade, id_produto)
SETTINGS index_granularity = 8192;

-- Detalhe por item de venda (grão fino para drill-down + agregação no período)
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_cliente_preco_fixo_item (
    id_empresa           Int32,
    id_filial            Int32,
    id_db                Int32 DEFAULT 0,
    id_entidade          Int32,
    id_comprovante       Int32,
    id_itemcomprovante   Int32,
    id_produto           Int32,
    data_key             Int32,
    dt_venda             Date,
    dt_evento            DateTime64(3, 'America/Sao_Paulo'),
    cliente_nome         String DEFAULT '',
    produto_nome         String DEFAULT '',
    documento_label      String DEFAULT '',
    qtd                  Decimal(18, 3) DEFAULT 0,
    preco_bomba          Decimal(18, 4) DEFAULT 0,
    preco_pago           Decimal(18, 4) DEFAULT 0,
    desconto_unitario    Decimal(18, 4) DEFAULT 0,
    desconto_total       Decimal(18, 2) DEFAULT 0,
    custo_unitario       Nullable(Decimal(18, 6)) DEFAULT NULL,
    margem_unitaria_pct  Nullable(Decimal(18, 4)) DEFAULT NULL,
    margem_bomba_pct     Nullable(Decimal(18, 4)) DEFAULT NULL,
    published_at         DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_entidade, data_key, id_comprovante, id_itemcomprovante)
PARTITION BY toYYYYMM(dt_venda)
SETTINGS index_granularity = 8192;

-- Evolução idempotente (tabelas já existentes em homolog/prod)
ALTER TABLE torqmind_mart_rt.mart_cliente_preco_fixo_item
  ADD COLUMN IF NOT EXISTS custo_unitario Nullable(Decimal(18, 6)) DEFAULT NULL;
ALTER TABLE torqmind_mart_rt.mart_cliente_preco_fixo_item
  ADD COLUMN IF NOT EXISTS margem_unitaria_pct Nullable(Decimal(18, 4)) DEFAULT NULL;
ALTER TABLE torqmind_mart_rt.mart_cliente_preco_fixo_item
  ADD COLUMN IF NOT EXISTS margem_bomba_pct Nullable(Decimal(18, 4)) DEFAULT NULL;
