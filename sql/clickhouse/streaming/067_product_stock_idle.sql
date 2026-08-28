-- Gestão de Produtos — estoque parado + últimas compras (publish PG mart → CH)

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.product_stock_idle (
    id_empresa         Int32 NOT NULL,
    id_filial          Int32 NOT NULL,
    id_produto         Int32 NOT NULL,
    nome_produto       String DEFAULT '',
    setor_gerencial    String DEFAULT 'outros',
    qtd_estoque        Decimal(18, 3) DEFAULT 0,
    last_sale_date     Nullable(Date),
    dias_sem_venda     Int32 DEFAULT 0,
    custo_medio_compra Decimal(18, 4) DEFAULT 0,
    preco_venda        Decimal(18, 4) DEFAULT 0,
    published_at       DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, setor_gerencial, nome_produto, id_produto)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.product_purchase_recent (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    id_produto        Int32 NOT NULL,
    rank              UInt8 NOT NULL,
    numero_documento  String DEFAULT '',
    data_compra       Date NOT NULL,
    qtd               Decimal(18, 3) DEFAULT 0,
    valor_unitario    Decimal(18, 4) DEFAULT 0,
    valor_total       Decimal(18, 2) DEFAULT 0,
    published_at      DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_produto, rank)
SETTINGS index_granularity = 8192;
